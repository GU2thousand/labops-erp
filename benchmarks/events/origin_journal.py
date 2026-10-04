"""Exclusive child journals and conservative composite acceptance accounting.

PostgreSQL and these files are not atomic. Missing commit evidence is never
rollback evidence. The parent may append settled, exact-key DB observations;
it never edits a child's original records or replays a business command.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from uuid import UUID

from benchmarks.events.generation_journal import COUNTERS, numeric_profile
from benchmarks.events.writer_topology import resolve_profile_writer, writer_profile


KINDS = ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')
EXTRA_COUNTERS = ('commit_unknown', 'attempted_unknown', 'integrity_failed',
                  'database_recovered_committed', 'database_identified_events')
CONTEXT_FIELDS = frozenset({
    'actor_id', 'source_id', 'target_id', 'source_warehouse_id', 'target_warehouse_id',
    'item_id', 'project_id', 'task_id', 'order_id', 'order_line_id', 'orderline_id',
    'batch_id', 'cycle_issue_id',
    'source_cluster', 'source_generation', 'topic', 'database_scope_digest',
    'source_context_digest', 'fixture_digest',
})
BASE_RECORD_FIELDS = frozenset({'schema_version', 'sequence', 'run_id', 'origin_id',
                               'plan_digest', 'recorded_at', 'action'})
ACTION_FIELDS = {
    'origin_frozen': frozenset(), 'origin_finalized': frozenset(),
    'batch_started': frozenset({'batch_id'}), 'batch_succeeded': frozenset({'batch_id'}),
    'command_attempted': frozenset({'batch_id', 'ordinal', 'global_index', 'command_key', 'kind'}),
    'command_committed': frozenset({'batch_id', 'ordinal', 'movement_id', 'request_hash'}),
    'event_identified': frozenset({'batch_id', 'ordinal', 'event_id', 'payload_hash'}),
    'command_failed': frozenset({'batch_id', 'ordinal', 'state', 'stage', 'error_type', 'outcome'}),
    'batch_failed': frozenset({'batch_id', 'stage', 'error_type', 'outcome'}),
}


def _json(value):
    def unique(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError('Duplicate origin JSON field')
            result[key] = item
        return result

    def finite(_):
        raise ValueError('Nonfinite origin JSON value')

    return json.loads(value, object_pairs_hook=unique, parse_constant=finite)


def _integer(value, *, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError('Expected an exact nonnegative integer')
    return value


def _name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('Invalid authored identifier')
    return value


def _uuid(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError('Expected a canonical UUID')
    return value


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('Expected a canonical SHA256')
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def _context(value):
    if not isinstance(value, dict) or set(value) - CONTEXT_FIELDS:
        raise ValueError('Unexpected origin context fields')
    for name, item in value.items():
        if name in ('batch_id', 'cycle_issue_id') and item is None:
            continue
        if name.endswith('_id'):
            _uuid(item)
        elif name.endswith('_digest'):
            _hash(item)
        elif not isinstance(item, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,249}', item):
            raise ValueError('Invalid authored source context')
    return deepcopy(value)


def _plan(*, run_id, origin_id, profile, label, lane, indices, rate, context=None):
    if not isinstance(run_id, str) or not re.fullmatch('[a-z0-9][a-z0-9_-]{0,47}', run_id):
        raise ValueError('Invalid run identifier')
    _name(origin_id); _name(label)
    _integer(lane)
    requested = numeric_profile(profile)
    selected = writer_profile(resolve_profile_writer(requested))
    if 'writer_topology' in requested and rate != requested['rate'] / selected['lanes']:
        raise ValueError('Origin nominal rate differs from frozen writer profile')
    if lane >= selected['lanes'] or isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0:
        raise ValueError('Invalid origin lane or rate')
    if not isinstance(indices, (list, tuple)):
        raise ValueError('An origin requires its frozen index list')
    indices = [_integer(index) for index in indices]
    if indices != sorted(set(indices)) or any((index // 4) % selected['lanes'] != lane for index in indices):
        raise ValueError('Frozen index allocation is duplicated or belongs to another lane')
    return {'schema_version': 1, 'run_id': run_id, 'origin_id': origin_id,
            'requested_numeric_profile': requested, 'label': label,
            'lane': lane, 'indices': indices, 'target_rate': rate,
            'context': _context(context or {}),
            'commands': [{'ordinal': ordinal, 'global_index': index,
                          'command_key': f'{run_id}:{index}', 'kind': KINDS[index % 4]}
                         for ordinal, index in enumerate(indices, 1)]}


def _counts(plan, attempts):
    items = list(attempts.values())
    states = [item['state'] for item in items]
    return {'requested': len(plan['indices']), 'attempted': len(items),
            'committed': sum(item['committed'] for item in items),
            'failed_before_commit': states.count('failed_before_commit'),
            'post_commit_observation_failed': states.count('post_commit_observation_failed'),
            'identified_events': sum(item.get('event_id') is not None for item in items),
            'unattempted': len(plan['indices']) - len(items),
            'pending_before_commit': sum(state in ('attempted', 'commit_unknown') for state in states),
            'pending_observation': states.count('committed'),
            'commit_unknown': sum(state in ('attempted', 'commit_unknown') for state in states),
            'attempted_unknown': 0, 'integrity_failed': 0,
            'database_recovered_committed': 0, 'database_identified_events': 0}


class OriginJournal:
    """One process owns one stream; ordinal is bound to a frozen command key."""

    def __init__(self, directory, *, run_id, origin_id, profile, label, lane,
                 indices, rate, context=None):
        self._plan = _plan(run_id=run_id, origin_id=origin_id, profile=profile,
                          label=label, lane=lane, indices=indices, rate=rate, context=context)
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.plan_digest = _digest(self._plan)
        self.batch_id = origin_id + '_batch_000001'
        self._owner = os.getpid()
        self._sequence = 0
        self._closed = False
        self._begun = False
        self._attempts = {}
        self._status = 'planned'
        self._failure = None
        with (self.directory / 'origin-plan.json').open('x', encoding='utf-8') as out:
            json.dump(self._plan, out, sort_keys=True, allow_nan=False)
            out.write('\n'); out.flush(); os.fsync(out.fileno())
        self._stream = (self.directory / 'origin-journal.jsonl').open('x', encoding='utf-8')
        self._append('origin_frozen', sync=True, plan_digest=self.plan_digest)

    @property
    def plan(self):
        return deepcopy(self._plan)

    def _owned(self):
        if os.getpid() != self._owner:
            raise RuntimeError('Origin journal cannot cross a process boundary')
        if self._closed:
            raise RuntimeError('Origin journal is closed')

    def _append(self, action, *, sync=False, **fields):
        self._owned()
        # A flush/fsync failure can occur after a complete line was written.
        # Allocate its sequence first so a later failure annotation cannot
        # duplicate the existing line's sequence.
        self._sequence += 1
        record = {'schema_version': 1, 'sequence': self._sequence,
                  'run_id': self._plan['run_id'], 'origin_id': self._plan['origin_id'],
                  'plan_digest': self.plan_digest, 'recorded_at': time.time(),
                  'action': action, **fields}
        self._stream.write(json.dumps(record, sort_keys=True, allow_nan=False) + '\n')
        self._stream.flush()
        if sync:
            os.fsync(self._stream.fileno())

    def _batch(self, batch):
        self._owned()
        if not self._begun or batch != self.batch_id or self._status != 'running':
            raise ValueError('Origin batch is not running')

    def _attempt(self, batch, ordinal):
        self._batch(batch)
        _integer(ordinal, minimum=1)
        if ordinal not in self._attempts:
            raise ValueError('Unknown origin attempt')
        return self._attempts[ordinal]

    def begin_batch(self, requested, rate, label):
        self._owned()
        if (self._begun or type(requested) is not int or requested != len(self._plan['indices'])
                or isinstance(rate, bool) or rate != self._plan['target_rate'] or label != self._plan['label']):
            raise ValueError('Origin batch must match its frozen reservation')
        self._append('batch_started', sync=True, batch_id=self.batch_id)
        self._begun = True
        self._status = 'running'
        return self.batch_id

    def bind_origin(self, batch, global_index):
        self._batch(batch)
        ordinal = len(self._attempts) + 1
        _integer(global_index)
        if ordinal > len(self._plan['commands']) or self._plan['commands'][ordinal - 1]['global_index'] != global_index:
            raise ValueError('Command does not match the next frozen origin')
        return deepcopy(self._plan['commands'][ordinal - 1])

    def attempt(self, batch, *, global_index=None):
        self._batch(batch)
        ordinal = len(self._attempts) + 1
        if ordinal > len(self._plan['commands']) or (ordinal > 1
                and self._attempts[ordinal - 1]['state'] != 'identified'):
            raise ValueError('Origin has an unfinished attempt or no reserved command')
        command = self._plan['commands'][ordinal - 1]
        if global_index is not None:
            self.bind_origin(batch, global_index)
        self._append('command_attempted', sync=True, batch_id=batch, **command)
        self._attempts[ordinal] = {**command, 'attempt_id': ordinal, 'state': 'attempted',
            'committed': False, 'movement_id': None, 'event_id': None, 'failure': None}
        return ordinal

    def commit(self, batch, ordinal, movement_id=None, *, request_hash=None):
        attempt = self._attempt(batch, ordinal)
        if attempt['state'] != 'attempted':
            raise ValueError('Origin commit requires one uncommitted attempt')
        _uuid(movement_id)
        if request_hash is not None:
            _hash(request_hash)
        # The caller has already returned from its outer business transaction.
        # A subsequent journal I/O error cannot turn that commit into rollback.
        attempt.update(committed=True, state='committed', movement_id=movement_id,
                       request_hash=request_hash)
        self._append('command_committed', sync=True, batch_id=batch, ordinal=ordinal,
                     movement_id=movement_id, request_hash=request_hash)

    def identify_event(self, batch, ordinal, event_id, *, payload_hash=None):
        attempt = self._attempt(batch, ordinal)
        if attempt['state'] != 'committed':
            raise ValueError('Origin event requires a recorded commit')
        _uuid(event_id)
        if payload_hash is not None:
            _hash(payload_hash)
        self._append('event_identified', batch_id=batch, ordinal=ordinal,
                     event_id=event_id, payload_hash=payload_hash)
        attempt.update(state='identified', event_id=event_id, payload_hash=payload_hash)

    def finish_success(self, batch):
        self._batch(batch)
        if len(self._attempts) != len(self._plan['indices']) or any(
                item['state'] != 'identified' for item in self._attempts.values()):
            raise ValueError('Origin success requires every frozen command')
        self._append('batch_succeeded', sync=True, batch_id=batch)
        self._status = 'succeeded'
        return self.summary()['batches'][0]

    def finish_failure(self, batch, stage, error_type, attempt_id=None, *, outcome='commit_unknown'):
        self._batch(batch)
        _name(stage); _name(error_type)
        if outcome not in ('commit_unknown', 'rollback_proven', 'not_entered'):
            raise ValueError('Unknown origin transaction outcome')
        if attempt_id is None and any(item['state'] in ('attempted', 'committed') for item in self._attempts.values()):
            raise ValueError('An active origin requires its attempt ordinal')
        failure = {'stage': stage, 'error_type': error_type, 'outcome': outcome}
        if attempt_id is not None:
            attempt = self._attempt(batch, attempt_id)
            if attempt_id != len(self._attempts) or attempt['failure'] is not None:
                raise ValueError('Only the current origin may fail')
            if attempt['committed'] and outcome != 'commit_unknown':
                raise ValueError('A returned transaction cannot be declared rolled back')
            state = ('post_commit_observation_failed' if attempt['committed'] else
                     'failed_before_commit' if outcome in ('rollback_proven', 'not_entered') else 'commit_unknown')
            self._append('command_failed', batch_id=batch, ordinal=attempt_id, state=state, **failure)
            attempt.update(state=state, failure=deepcopy(failure))
        self._append('batch_failed', sync=True, batch_id=batch, **failure)
        self._status = 'failed'
        self._failure = failure
        return self.summary()['batches'][0]

    def summary(self):
        counts = _counts(self._plan, self._attempts)
        batch = {'batch_id': self.batch_id, 'origin_id': self._plan['origin_id'],
                 'label': self._plan['label'], 'target_rate': self._plan['target_rate'],
                 'status': self._status, 'failure': deepcopy(self._failure), **counts}
        return {'schema_version': 1, 'run_id': self._plan['run_id'],
                'requested_numeric_profile': deepcopy(self._plan['requested_numeric_profile']),
                'journal_database_atomic': False, 'database_reconciliation_required': True,
                'batches': [batch], 'totals': counts}

    def batch_summary(self, batch):
        if batch != self.batch_id:
            raise ValueError('Unknown origin batch')
        return self.summary()['batches'][0]

    def finalize(self):
        if self._closed:
            return self.summary()
        self._append('origin_finalized', sync=True)
        self._stream.close()
        self._closed = True
        return self.summary()


def _read_origin(plan, directory):
    """Keep valid-prefix facts; corrupt evidence never becomes zero attempts."""
    attempts, errors, status, failure = {}, [], 'planned', None
    path = Path(directory)
    journal = path / 'origin-journal.jsonl'
    if not journal.exists():
        return attempts, errors, status, failure, False
    plan_digest = _digest(plan)
    try:
        observed = _json((path / 'origin-plan.json').read_text())
        if _digest(observed) != plan_digest:
            raise ValueError('Origin plan mismatch')
    except (OSError, ValueError, TypeError, UnicodeError) as exc:
        errors.append({'stage': 'origin_plan_read', 'error_type': type(exc).__name__})
    try:
        raw = journal.read_bytes().splitlines(keepends=True)
        if not raw:
            raise ValueError('Empty origin journal')
        began = False
        ended = False
        batch = plan['origin_id'] + '_batch_000001'
        for sequence, line in enumerate(raw, 1):
            if not line.endswith(b'\n'):
                raise ValueError('Incomplete origin record')
            item = _json(line)
            if not isinstance(item, dict):
                raise ValueError('Origin record must be an object')
            action = item.get('action')
            if action not in ACTION_FIELDS or set(item) != BASE_RECORD_FIELDS | ACTION_FIELDS[action]:
                raise ValueError('Unexpected origin record schema')
            if (type(item.get('schema_version')) is not int or item['schema_version'] != 1
                    or type(item.get('sequence')) is not int or item['sequence'] != sequence
                    or item.get('run_id') != plan['run_id'] or item.get('origin_id') != plan['origin_id']
                    or item.get('plan_digest') != plan_digest
                    or isinstance(item.get('recorded_at'), bool)
                    or not isinstance(item.get('recorded_at'), (int, float))
                    or not math.isfinite(item['recorded_at']) or ended):
                raise ValueError('Origin record identity or sequence mismatch')
            if sequence == 1:
                if action != 'origin_frozen':
                    raise ValueError('Missing frozen origin record')
                continue
            if action == 'batch_started':
                if began or item.get('batch_id') != batch:
                    raise ValueError('Duplicate origin batch')
                began, status = True, 'running'
            elif action == 'origin_finalized':
                ended = True
            elif not began or status != 'running' or item.get('batch_id') != batch:
                raise ValueError('Origin transition outside its batch')
            elif action == 'command_attempted':
                ordinal = _integer(item.get('ordinal'), minimum=1)
                if ordinal != len(attempts) + 1 or ordinal > len(plan['commands']) or (ordinal > 1
                        and attempts[ordinal - 1]['state'] != 'identified'):
                    raise ValueError('Duplicated or overlapping origin attempt')
                command = plan['commands'][ordinal - 1]
                if any(item.get(key) != value or type(item.get(key)) is not type(value) for key, value in command.items()):
                    raise ValueError('Origin command mapping changed')
                attempts[ordinal] = {**command, 'attempt_id': ordinal, 'state': 'attempted',
                    'committed': False, 'movement_id': None, 'event_id': None, 'failure': None}
            elif action in ('command_committed', 'event_identified', 'command_failed'):
                ordinal = _integer(item.get('ordinal'), minimum=1)
                if ordinal != len(attempts) or ordinal not in attempts:
                    raise ValueError('Transition has no matching current attempt')
                attempt = attempts[ordinal]
                if action == 'command_committed':
                    if attempt['state'] != 'attempted':
                        raise ValueError('Duplicate origin commit')
                    _uuid(item.get('movement_id'))
                    if item.get('request_hash') is not None:
                        _hash(item['request_hash'])
                    attempt.update(committed=True, state='committed', movement_id=item['movement_id'],
                                   request_hash=item.get('request_hash'))
                elif action == 'event_identified':
                    if attempt['state'] != 'committed':
                        raise ValueError('Identification without recorded commit')
                    _uuid(item.get('event_id'))
                    if item.get('payload_hash') is not None:
                        _hash(item['payload_hash'])
                    attempt.update(state='identified', event_id=item['event_id'], payload_hash=item.get('payload_hash'))
                else:
                    _name(item.get('stage')); _name(item.get('error_type'))
                    outcome = item.get('outcome')
                    expected = ('post_commit_observation_failed' if attempt['committed'] else
                        'failed_before_commit' if outcome in ('rollback_proven', 'not_entered') else 'commit_unknown')
                    if (outcome not in ('commit_unknown', 'rollback_proven', 'not_entered')
                            or attempt['committed'] and outcome != 'commit_unknown'
                            or item.get('state') != expected or attempt['failure']):
                        raise ValueError('Invalid origin failure outcome')
                    failure = {key: item[key] for key in ('stage', 'error_type', 'outcome')}
                    attempt.update(state=expected, failure=deepcopy(failure))
            elif action == 'batch_succeeded':
                if len(attempts) != len(plan['indices']) or any(entry['state'] != 'identified' for entry in attempts.values()):
                    raise ValueError('Incomplete successful origin')
                status = 'succeeded'
            elif action == 'batch_failed':
                _name(item.get('stage')); _name(item.get('error_type'))
                if item.get('outcome') not in ('commit_unknown', 'rollback_proven', 'not_entered'):
                    raise ValueError('Invalid failed origin outcome')
                failure = {key: item[key] for key in ('stage', 'error_type', 'outcome')}
                if attempts:
                    current = attempts[len(attempts)]
                    if current['state'] in ('attempted', 'committed'):
                        raise ValueError('Active command failure transition is missing')
                    if current['failure'] and current['failure'] != failure:
                        raise ValueError('Batch failure contradicts its command failure')
                status = 'failed'
            else:
                raise ValueError('Unknown origin action')
    except (OSError, ValueError, KeyError, TypeError, UnicodeError) as exc:
        errors.append({'stage': 'origin_read', 'error_type': type(exc).__name__})
    return attempts, errors, status, failure, True


def _validate_database(plan, command, attempt, facts):
    """Validate normalized independently queried ORM facts, never broker text."""
    if not isinstance(facts, dict) or facts.get('database_observed') is not True:
        return {'state': 'commit_unknown', 'error_type': 'DatabaseUnavailable'}
    if (facts.get('command_key') != command['command_key'] or facts.get('session_settled') is not True
            or facts.get('database_scope_matches') is not True or facts.get('source_context_matches') is not True):
        return {'state': 'integrity_failed', 'error_type': 'OriginContextConflict'}
    movements, outboxes = facts.get('movement_rows'), facts.get('outbox_rows')
    if not isinstance(movements, list) or not isinstance(outboxes, list):
        return {'state': 'commit_unknown', 'error_type': 'IncompleteDatabaseFacts'}
    if not movements and not outboxes:
        if attempt and attempt['committed']:
            return {'state': 'integrity_failed', 'error_type': 'RecordedCommitMissing'}
        return {'state': 'no_business_commit'}
    try:
        if len(movements) != 1 or len(outboxes) != 1:
            raise ValueError('Inventory movement/outbox cardinality conflict')
        movement, outbox = movements[0], outboxes[0]
        if 'actor_id' not in plan['context']:
            raise ValueError('Recovered movement has no frozen expected actor')
        _uuid(movement['id']); _uuid(outbox['id']); _uuid(movement['actor_id'])
        _integer(movement['version'], minimum=1); _integer(outbox['aggregate_version'], minimum=1)
        _hash(movement['request_hash'])
        if movement.get('expected_request_hash') is not None:
            _hash(movement['expected_request_hash'])
        _hash(outbox['payload_hash']); _hash(outbox['computed_payload_hash'])
        if (movement['idempotency_key'] != command['command_key'] or movement['status'] != 'POSTED'
                or movement['type'] != command['kind'] or movement['context_matches'] is not True
                or movement.get('expected_request_hash', movement['request_hash']) not in (None, movement['request_hash'])
                or plan['context']['actor_id'] != movement['actor_id']
                or outbox['aggregate_type'] != 'stockmovement' or outbox['aggregate_id'] != movement['id']
                or outbox['aggregate_version'] != movement['version']
                or outbox['dedupe_key'] != 'inventory:' + movement['id']
                or outbox['event_type'] != 'inventory.' + command['kind'].lower() + '.posted'
                or outbox['transport'] != 'kafka' or type(outbox['schema_version']) is not int or outbox['schema_version'] != 1
                or outbox['schema_valid'] is not True or outbox['ledger_links_valid'] is not True
                or outbox['payload_hash'] != outbox['computed_payload_hash']):
            raise ValueError('Original inventory identity/content conflict')
        if attempt and (attempt.get('movement_id') not in (None, movement['id'])
                or attempt.get('event_id') not in (None, outbox['id'])
                or attempt.get('request_hash') not in (None, movement['request_hash'])
                or attempt.get('payload_hash') not in (None, outbox['payload_hash'])
                or attempt['state'] == 'failed_before_commit'):
            raise ValueError('Journal contradicts original database content')
        markers = facts.get('marker_hashes', [])
        if not isinstance(markers, list) or any(_hash(value) != outbox['payload_hash'] for value in markers):
            raise ValueError('Processed marker content conflict')
        return {'state': 'committed_recovered', 'movement_id': movement['id'],
                'event_id': outbox['id'], 'request_hash': movement['request_hash'],
                'payload_hash': outbox['payload_hash'],
                'request_hash_independently_verified': movement.get('expected_request_hash') is not None,
                'journal_request_hash_matches_database': bool(attempt and attempt.get('request_hash')),
                'request_hash_reference': ('expected_input_hash' if movement.get('expected_request_hash') is not None
                    else 'child_commit_record' if attempt and attempt.get('request_hash') else 'unavailable'),
                'source_context_verification': 'configuration_only'}
    except (ValueError, KeyError, TypeError, AttributeError):
        return {'state': 'integrity_failed', 'error_type': 'InventoryIdentityConflict'}


class CompositeGenerationJournal:
    """Forward serial writes; read exclusive origin files and append resolutions."""

    def __init__(self, parent_serial_journal, origin_directories=(), *, expected_plans=(), reconciliation_path=None):
        self.parent = parent_serial_journal
        self._plans = {}
        self._transport = {}
        self._resolutions = {}
        self._annotation_sequence = 0
        self.reconciliation_path = Path(reconciliation_path or
            (self.parent.evidence_dir / 'origin-reconciliation.jsonl'))
        if origin_directories and len(origin_directories) != len(expected_plans):
            raise ValueError('Every origin directory requires a frozen parent plan')
        for index, plan in enumerate(expected_plans):
            self.add_origin_plan({**plan, **({'path': origin_directories[index]} if origin_directories else {})})

    def __getattr__(self, name):
        if name in ('begin_batch', 'attempt', 'commit', 'identify_event', 'finish_success', 'finish_failure', 'batch_summary'):
            return getattr(self.parent, name)
        raise AttributeError(name)

    @property
    def profile(self):
        return deepcopy(self.parent.profile)

    @property
    def run_id(self):
        return self.parent.run_id

    @property
    def evidence_dir(self):
        return self.parent.evidence_dir

    def add_origin_plan(self, value):
        value = deepcopy(value)
        context = value.get('context', {key: value[key] for key in CONTEXT_FIELDS if key in value})
        if 'rate' in value and 'target_rate' in value and value['rate'] != value['target_rate']:
            raise ValueError('Conflicting origin rate aliases')
        lanes = writer_profile(resolve_profile_writer(self.parent.profile))['lanes']
        rate = value.get('rate', value.get('target_rate', self.parent.profile['rate'] / lanes))
        if 'requested_numeric_profile' in value and _digest(value['requested_numeric_profile']) != _digest(self.parent.profile):
            raise ValueError('Origin requested profile differs from the parent')
        if 'schema_version' in value and (type(value['schema_version']) is not int or value['schema_version'] != 1):
            raise ValueError('Unexpected origin plan schema')
        plan = _plan(run_id=value['run_id'], origin_id=value['origin_id'], profile=self.parent.profile,
                     label=value['label'], lane=value['lane'], indices=value['indices'],
                     rate=rate, context=context)
        if 'commands' in value and _digest(value['commands']) != _digest(plan['commands']):
            raise ValueError('Origin command mapping differs from the reservation')
        if plan['run_id'] != self.parent.run_id or plan['origin_id'] in self._plans:
            raise ValueError('Origin run or namespace conflict')
        reserved = {index for prior, _ in self._plans.values() for index in prior['indices']}
        if reserved.intersection(plan['indices']):
            raise ValueError('Global origin index already reserved')
        self._plans[plan['origin_id']] = (plan, Path(value['path']))
        return deepcopy(plan)

    def note_transport(self, origin_id, *, started_indices=(), stage='child_transport', error_type='ChildEOF',
                       execution_permitted=True):
        plan, _ = self._plans[origin_id]
        _name(stage); _name(error_type)
        if type(execution_permitted) is not bool:
            raise ValueError('Execution permission must be an actual boolean')
        indices = {_integer(index) for index in started_indices}
        if not indices.issubset(plan['indices']):
            raise ValueError('Transport evidence belongs to another origin')
        old = self._transport.get(origin_id, {})
        self._transport[origin_id] = {'started_indices': sorted(indices | set(old.get('started_indices', []))),
                                     'stage': stage, 'error_type': error_type,
                                     'execution_permitted': execution_permitted or old.get('execution_permitted', False)}

    def reconcile_origin(self, origin_id, *, settled, query_callback):
        if type(settled) is not bool:
            raise ValueError('Session settlement must be an actual boolean')
        plan, directory = self._plans[origin_id]
        attempts, _, _, _, _ = _read_origin(plan, directory)
        annotations = []
        plan_digest = _digest(plan)
        query_identity = {key: deepcopy(value) for key, value in plan.items()
                          if key not in ('commands', 'indices')}
        query_identity['plan_digest'] = plan_digest
        for command in plan['commands']:
            if not settled:
                outcome = {'state': 'commit_unknown', 'error_type': 'OwnedSessionUnsettled'}
            else:
                try:
                    facts = query_callback(deepcopy(query_identity), deepcopy(command))
                    outcome = _validate_database(plan, command, attempts.get(command['ordinal']), facts)
                except Exception as exc:
                    error_type = type(exc).__name__
                    outcome = {'state': 'commit_unknown', 'error_type': error_type
                               if re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,95}', error_type) else 'CallbackError'}
            self._annotation_sequence += 1
            row = {'schema_version': 1, 'sequence': self._annotation_sequence,
                   'run_id': plan['run_id'], 'origin_id': origin_id, 'plan_digest': plan_digest,
                   **command, 'session_settled': settled, 'stage': 'database_reconciliation', **outcome}
            # Only safe authored fields and validated IDs/hashes are appended;
            # callback payloads and exception messages never cross this boundary.
            with self.reconciliation_path.open('a', encoding='utf-8') as out:
                out.write(json.dumps(row, sort_keys=True, allow_nan=False) + '\n')
                out.flush(); os.fsync(out.fileno())
            self._resolutions[(origin_id, command['ordinal'])] = deepcopy(row)
            annotations.append(row)
        return deepcopy(annotations)

    def _origin_snapshot(self, origin_id):
        plan, directory = self._plans[origin_id]
        attempts, errors, status, failure, exists = _read_origin(plan, directory)
        raw = _counts(plan, attempts)
        transport = self._transport.get(origin_id)
        started = set(transport.get('started_indices', [])) if transport else set()
        if not exists and transport and transport['execution_permitted']:
            # EOF can hide a STARTED notification too. An absent origin log
            # cannot prove which permitted commands entered the transaction.
            started.update(plan['indices'])
        items = []
        counts = {name: 0 for name in (*COUNTERS, *EXTRA_COUNTERS)}
        counts['requested'] = len(plan['indices'])
        for command in plan['commands']:
            ordinal = command['ordinal']
            item = deepcopy(attempts.get(ordinal))
            uncertain_attempt = item is None and (bool(errors) or command['global_index'] in started)
            if item is None:
                item = {**command, 'attempt_id': ordinal, 'state': 'commit_unknown' if uncertain_attempt else 'unattempted',
                        'committed': False, 'movement_id': None, 'event_id': None, 'failure': None}
            attempted_known = ordinal in attempts
            resolution = self._resolutions.get((origin_id, ordinal))
            if resolution and resolution['state'] == 'committed_recovered':
                counts['database_recovered_committed'] += int(not item['committed'])
                counts['database_identified_events'] += int(item['event_id'] is None)
                item.update(committed=True, movement_id=resolution['movement_id'], event_id=resolution['event_id'],
                            state='committed_recovered', payload_hash=resolution['payload_hash'],
                            request_hash=resolution['request_hash'])
                attempted_known = True
                uncertain_attempt = False
            elif resolution and resolution['state'] == 'no_business_commit':
                item['state'] = 'failed_before_commit' if attempted_known else (
                    'no_commit_attempt_unknown' if uncertain_attempt else 'unattempted')
            elif resolution and resolution['state'] == 'integrity_failed':
                item['state'] = 'integrity_failed'
                if not attempted_known:
                    uncertain_attempt = True
            elif resolution and resolution['state'] == 'commit_unknown' and not item['committed']:
                item['state'] = 'commit_unknown'
                if not attempted_known:
                    uncertain_attempt = True
            counts['attempted'] += int(attempted_known)
            counts['attempted_unknown'] += int(uncertain_attempt)
            counts['committed'] += int(item['committed'])
            counts['identified_events'] += int(item['event_id'] is not None)
            counts['failed_before_commit'] += int(item['state'] == 'failed_before_commit')
            counts['post_commit_observation_failed'] += int(item['committed'] and bool(item.get('failure')))
            counts['unattempted'] += int(item['state'] == 'unattempted')
            counts['commit_unknown'] += int(item['state'] == 'commit_unknown')
            counts['integrity_failed'] += int(item['state'] == 'integrity_failed')
            counts['pending_before_commit'] += int(item['state'] in ('attempted', 'commit_unknown'))
            counts['pending_observation'] += int(item['committed'] and item['event_id'] is None)
            if item['state'] == 'attempted':
                counts['commit_unknown'] += 1
            item.update(batch_id=origin_id + '_batch_000001', origin_id=origin_id, label=plan['label'])
            if resolution:
                item['reconciliation'] = deepcopy(resolution)
            items.append(item)
        if errors or transport or status != 'succeeded' or counts['commit_unknown'] or counts['integrity_failed']:
            status = 'failed' if status == 'failed' or errors or transport else 'interrupted'
        batch = {'batch_id': origin_id + '_batch_000001', 'origin_id': origin_id,
                 'label': plan['label'], 'target_rate': plan['target_rate'], 'status': status,
                 'failure': deepcopy(failure), 'transport_failure': deepcopy(transport),
                 'evidence_errors': errors, 'journal_present': exists,
                 'raw_totals': raw, **counts}
        batch['attempted_accounting_complete'] = counts['attempted_unknown'] == 0
        batch['requested_partition_complete'] = (counts['requested'] == counts['attempted']
            + counts['attempted_unknown'] + counts['unattempted'])
        if not batch['requested_partition_complete']:
            raise AssertionError('Frozen origin denominator coverage was lost')
        return batch, items

    def summary(self):
        parent = self.parent.summary()
        batches = deepcopy(parent['batches'])
        origin_batches = [self._origin_snapshot(origin)[0] for origin in self._plans]
        batches.extend(origin_batches)
        return {'schema_version': 1, 'run_id': self.parent.run_id,
                'requested_numeric_profile': deepcopy(self.parent.profile),
                'journal_database_atomic': False, 'database_reconciliation_required': True,
                'batches': batches,
                'totals': {name: sum(batch.get(name, 0) for batch in batches) for name in (*COUNTERS, *EXTRA_COUNTERS)}}

    def batch_summary(self, batch_id):
        for origin_id in self._plans:
            if batch_id == origin_id + '_batch_000001':
                return deepcopy(self._origin_snapshot(origin_id)[0])
        return self.parent.batch_summary(batch_id)

    def committed_attempts(self):
        result = deepcopy(self.parent.committed_attempts())
        for origin in self._plans:
            _, items = self._origin_snapshot(origin)
            result.extend(item for item in items if item['committed'])
        return result

    def finalize(self):
        # Children close their own streams. Parent finalization must never
        # rewrite a child record or convert unknown outcomes into rollback.
        self.parent.finalize()
        report = self.summary()
        temporary = self.parent.summary_path.with_suffix('.json.tmp')
        with temporary.open('w', encoding='utf-8') as out:
            json.dump(report, out, sort_keys=True, allow_nan=False)
            out.write('\n'); out.flush(); os.fsync(out.fileno())
        os.replace(temporary, self.parent.summary_path)
        return report
