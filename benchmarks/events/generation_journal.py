"""Durable, incremental accounting for disposable acceptance generation.

Call ``attempt`` before the outer business transaction, ``commit`` immediately
after it returns, and ``identify_event`` only after observing the outbox row.
The journal and PostgreSQL are not atomic: a final database snapshot must still
reconcile actual committed IDs when a process or filesystem fails at a boundary.
Only authored identifiers and numeric data belong here, never exception text,
business payloads, connection strings, environment values, or credentials.
"""
from __future__ import annotations

import json
from functools import wraps
import math
import os
from pathlib import Path
import re
from threading import RLock
import time
from typing import Mapping
from uuid import UUID


PROFILE_FIELDS = (
    'events', 'rate', 'duration', 'fault_repetitions', 'fault_events',
    'duplicate_events', 'poison_events', 'broker_fault_seconds',
    'outage_seconds', 'consumer_outage_seconds', 'drain_timeout',
)
COUNT_FIELDS = frozenset({
    'events', 'fault_repetitions', 'fault_events', 'duplicate_events', 'poison_events',
})
COUNTERS = (
    'requested', 'attempted', 'committed', 'failed_before_commit',
    'post_commit_observation_failed', 'identified_events', 'unattempted',
    'pending_before_commit', 'pending_observation',
)


def _number(value, *, count=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError('Expected a finite nonnegative numeric value')
    if not math.isfinite(value) or value < 0 or (count and not isinstance(value, int)):
        raise ValueError('Expected a finite nonnegative numeric value')
    return value


def numeric_profile(args) -> dict:
    """Copy only the fixed numeric CLI contract, preserving supplied values.

    Do not serialize ``vars(args)``: it also contains paths and may acquire
    private configuration in future harness versions. No full-tier default or
    observed count is substituted for a requested value.
    """
    profile = {name: _number(args[name] if isinstance(args, Mapping) else getattr(args, name),
                          count=name in COUNT_FIELDS)
            for name in PROFILE_FIELDS}
    enabled = (args.get('runtime_diagnostics_enabled', args.get('runtime_diagnostics', False))
               if isinstance(args, Mapping) else getattr(args, 'runtime_diagnostics', False))
    if not isinstance(enabled, bool):
        raise ValueError('Runtime diagnostics request must be a boolean')
    profile['runtime_diagnostics_enabled'] = enabled
    profiling = (args.get('diagnostic_profile_enabled', args.get('diagnostic_profile', False))
        if isinstance(args, Mapping) else getattr(args, 'diagnostic_profile', False))
    if type(profiling) is not bool:
        raise ValueError('Diagnostic profile request must be a boolean')
    profile['diagnostic_profile_enabled'] = profiling
    engine = (args.get('diagnostic_profile_engine', 'cprofile') if isinstance(args, Mapping)
              else getattr(args, 'diagnostic_profile_engine', 'cprofile'))
    from benchmarks.events.diagnostic_profile import request_profile
    request_profile(profiling, engine)
    if engine != 'cprofile':
        profile['diagnostic_profile_engine'] = engine
    from benchmarks.events.writer_topology import resolve_profile_writer, WRITER_TOPOLOGY_VERSION
    preset = resolve_profile_writer(args)
    has_writer = 'writer_topology' in args if isinstance(args, Mapping) else hasattr(args, 'writer_topology')
    if has_writer:
        profile['writer_topology'] = preset
        profile['writer_topology_version'] = WRITER_TOPOLOGY_VERSION
    return profile


def _identifier(value, purpose):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('Expected an authored ' + purpose + ' identifier')
    return value


def _uuid(value):
    return str(UUID(str(value)))


def _synchronized(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return locked


class GenerationJournal:
    """One writer shared by threads; each generation lane has its own batch.

    Append and flush every transition; fsync the committed boundary and batch
    completion. Attempt/identification lines do not incur extra per-command
    fsyncs. Query methods return copies and never modify requested denominators.
    """

    def __init__(self, evidence_dir, run_id, profile):
        self._lock = RLock()
        if not isinstance(run_id, str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', run_id):
            raise ValueError('Invalid validation run identifier')
        self.evidence_dir = Path(evidence_dir)
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.profile = numeric_profile(profile)
        self._batches = {}
        self._sequence = 0
        self._closed = False
        self.profile_path = self.evidence_dir / 'requested-profile.json'
        self.journal_path = self.evidence_dir / 'generation-journal.jsonl'
        self.summary_path = self.evidence_dir / 'generation-summary.json'
        # Exclusive creation prevents a rerun from overwriting or interleaving
        # evidence for an earlier attempt with the same evidence directory.
        with self.profile_path.open('x', encoding='utf-8') as stream:
            json.dump({'schema_version': 1, 'run_id': run_id,
                       'requested_numeric_profile': self.profile}, stream,
                      sort_keys=True, allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        self._stream = self.journal_path.open('x', encoding='utf-8')
        self._append('profile_frozen', sync=True, requested_numeric_profile=self.profile)

    def _append(self, action, *, sync=False, **fields):
        if self._closed:
            raise RuntimeError('Generation journal is closed')
        self._sequence += 1
        item = {'schema_version': 1, 'sequence': self._sequence, 'run_id': self.run_id,
                'action': action, 'recorded_at': time.time(), **fields}
        self._stream.write(json.dumps(item, sort_keys=True, allow_nan=False) + '\n')
        self._stream.flush()
        if sync:
            os.fsync(self._stream.fileno())

    def _batch(self, batch_id, *, running=False):
        if running and self._closed:
            raise RuntimeError('Generation journal is closed')
        batch = self._batches[batch_id]
        if running and batch['status'] != 'running':
            raise ValueError('Generation batch is already finished')
        return batch

    def _attempt(self, batch_id, attempt_id):
        batch = self._batch(batch_id, running=True)
        return batch, batch['attempts'][attempt_id]

    @_synchronized
    def begin_batch(self, requested, rate, label):
        if self._closed:
            raise RuntimeError('Generation journal is closed')
        requested = _number(requested, count=True)
        rate = _number(rate)
        if rate == 0:
            raise ValueError('Generation rate must be positive')
        label = _identifier(label, 'scenario')
        batch_id = f'batch-{len(self._batches) + 1:06d}'
        self._batches[batch_id] = {'batch_id': batch_id, 'label': label,
                                 'requested': requested, 'target_rate': rate,
                                 'status': 'running', 'attempts': {}, 'failure': None,
                                 'active_attempt': None}
        self._append('batch_started', sync=True, batch_id=batch_id, label=label,
                     requested=requested, target_rate=rate)
        return batch_id

    @_synchronized
    def attempt(self, batch_id):
        batch = self._batch(batch_id, running=True)
        if len(batch['attempts']) >= batch['requested']:
            raise ValueError('Attempt would exceed the requested denominator')
        if batch['active_attempt'] is not None:
            raise ValueError('Previous command observation is incomplete')
        attempt_id = len(batch['attempts']) + 1
        batch['attempts'][attempt_id] = {'attempt_id': attempt_id, 'state': 'attempted',
                                        'committed': False, 'movement_id': None,
                                        'event_id': None, 'failure': None}
        batch['active_attempt'] = attempt_id
        self._append('command_attempted', batch_id=batch_id, attempt_id=attempt_id)
        return attempt_id

    @_synchronized
    def commit(self, batch_id, attempt_id, movement_id=None):
        _, attempt = self._attempt(batch_id, attempt_id)
        if attempt['state'] != 'attempted':
            raise ValueError('Command cannot be committed twice')
        movement_id = _uuid(movement_id) if movement_id is not None else None
        attempt.update(state='committed', committed=True, movement_id=movement_id)
        self._append('command_committed', sync=True, batch_id=batch_id,
                     attempt_id=attempt_id, movement_id=movement_id)

    @_synchronized
    def identify_event(self, batch_id, attempt_id, event_id):
        batch, attempt = self._attempt(batch_id, attempt_id)
        if attempt['state'] != 'committed':
            raise ValueError('Event observation requires a committed command')
        event_id = _uuid(event_id)
        attempt.update(state='identified', event_id=event_id)
        batch['active_attempt'] = None
        self._append('event_identified', batch_id=batch_id,
                     attempt_id=attempt_id, event_id=event_id)

    @_synchronized
    def finish_success(self, batch_id):
        batch = self._batch(batch_id, running=True)
        if len(batch['attempts']) != batch['requested'] or any(
                item['state'] != 'identified' for item in batch['attempts'].values()):
            raise ValueError('Success requires all requested commands and identified events')
        batch['status'] = 'succeeded'
        self._append('batch_succeeded', sync=True, **self.batch_summary(batch_id))
        self._write_summary()
        return self.batch_summary(batch_id)

    @_synchronized
    def finish_failure(self, batch_id, stage, error_type, attempt_id=None):
        """Preserve partial counts without deriving them from observed event IDs.

        ``stage`` is a code-authored identifier and ``error_type`` is the class
        name (e.g. ``type(exc).__name__``), never ``str(exc)``. If an exception
        occurs before the next command is attempted, pass no attempt ID.
        """
        batch = self._batch(batch_id, running=True)
        stage = _identifier(stage, 'failure stage')
        error_type = _identifier(error_type, 'exception type')
        failure = {'stage': stage, 'error_type': error_type}
        if attempt_id is not None:
            _, attempt = self._attempt(batch_id, attempt_id)
            if attempt_id != len(batch['attempts']):
                raise ValueError('Only the current command can fail a generation batch')
            if attempt['failure'] is not None:
                raise ValueError('Command failure is already recorded')
            attempt['state'] = ('post_commit_observation_failed' if attempt['committed']
                                else 'failed_before_commit')
            attempt['failure'] = dict(failure)
            batch['active_attempt'] = None
            self._append('command_failed', batch_id=batch_id, attempt_id=attempt_id,
                         state=attempt['state'], **failure)
        elif batch['active_attempt'] is not None:
            raise ValueError('An active command requires its attempt ID on failure')
        batch.update(status='failed', failure=failure)
        self._append('batch_failed', sync=True, **self.batch_summary(batch_id))
        self._write_summary()
        return self.batch_summary(batch_id)

    @_synchronized
    def batch_summary(self, batch_id):
        batch = self._batch(batch_id)
        attempts = list(batch['attempts'].values())
        states = [item['state'] for item in attempts]
        return {'batch_id': batch_id, 'label': batch['label'],
                'target_rate': batch['target_rate'], 'status': batch['status'],
                'requested': batch['requested'], 'attempted': len(attempts),
                'committed': sum(item['committed'] for item in attempts),
                'failed_before_commit': states.count('failed_before_commit'),
                'post_commit_observation_failed': states.count('post_commit_observation_failed'),
                'identified_events': sum(item['event_id'] is not None for item in attempts),
                'unattempted': batch['requested'] - len(attempts),
                'pending_before_commit': states.count('attempted'),
                'pending_observation': states.count('committed'),
                'failure': dict(batch['failure']) if batch['failure'] else None}

    @_synchronized
    def committed_attempts(self):
        """Recorded IDs only; a database snapshot supplies any boundary gap."""
        return [{'batch_id': batch_id, 'label': batch['label'],
                 'attempt_id': attempt['attempt_id'],
                 'movement_id': attempt['movement_id'], 'event_id': attempt['event_id'],
                 'state': attempt['state']}
                for batch_id, batch in self._batches.items()
                for attempt in batch['attempts'].values() if attempt['committed']]

    @_synchronized
    def summary(self):
        batches = [self.batch_summary(batch_id) for batch_id in self._batches]
        return {'schema_version': 1, 'run_id': self.run_id,
                'requested_numeric_profile': dict(self.profile),
                'journal_database_atomic': False,
                'database_reconciliation_required': True,
                'batches': batches,
                'totals': {name: sum(batch[name] for batch in batches) for name in COUNTERS}}

    def _write_summary(self):
        report = self.summary()
        temporary = self.summary_path.with_suffix('.json.tmp')
        with temporary.open('w', encoding='utf-8') as stream:
            json.dump(report, stream, sort_keys=True, allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.summary_path)
        return report

    @_synchronized
    def finalize(self):
        """Write final counts, keeping any unresolved transitions explicitly pending."""
        if self._closed:
            return self.summary()
        self._append('journal_finalized', sync=True, totals=self.summary()['totals'])
        report = self._write_summary()
        self._stream.close()
        self._closed = True
        return report
