"""Acceptance-only inventory command reuse; production services retain all rules.

A fresh child supplies its own Django connection, origin journal and event sink.
The coordinator supplies the same interface for serial fault fixtures. No Kafka
client, Harness instance or parent database connection belongs in a child.
"""
from datetime import timedelta
import copy
import hashlib
import json
import os
from pathlib import Path
import time
from types import SimpleNamespace


def validate_process_bootstrap(lane, bootstrap):
    """Check every parent's lane plan before importing Django or opening paths."""
    from benchmarks.events.writer_topology import resolve_profile_writer, writer_profile
    from benchmarks.events.generation_journal import numeric_profile
    from benchmarks.events.origin_journal import _plan
    if not isinstance(bootstrap, dict) or 'profile' not in bootstrap:
        raise ValueError('Missing parent writer profile')
    preset = resolve_profile_writer(bootstrap['profile'])
    selected = writer_profile(preset,
        diagnostic_profile=bootstrap.get('diagnostic_profile_enabled', False))
    lanes = selected['lanes']
    if type(lane) is not int or lane not in range(lanes):
        raise ValueError('Child lane is outside the selected writer profile')
    if bootstrap.get('writer_topology', preset) != preset:
        raise ValueError('Child writer selection differs from parent profile')
    profile = numeric_profile(bootstrap['profile'])
    for name in ('diagnostic_profile_enabled', 'runtime_diagnostics_enabled'):
        if type(bootstrap.get(name)) is not bool or bootstrap[name] != profile[name]:
            raise ValueError('Child diagnostic request differs from frozen parent')
    rate = bootstrap.get('rate')
    if type(rate) not in (int, float) or rate != profile['rate'] or rate <= 0:
        raise ValueError('Child global rate differs from parent profile')
    plans = bootstrap.get('plans')
    if not isinstance(plans, list) or len(plans) != lanes:
        raise ValueError('Child requires every ordered selected lane plan')
    identifiers, applications, reserved = set(), set(), set()
    context_ids = {key: set() for key in ('project_id', 'task_id', 'order_id', 'order_line_id')}
    shared = None
    for number, plan in enumerate(plans):
        if not isinstance(plan, dict) or type(plan.get('lane')) is not int or plan['lane'] != number:
            raise ValueError('Child lane plans must be ordered and unique')
        required = {'run_id', 'origin_id', 'label', 'indices', 'rate', 'context',
                    'application_name', 'database_name', 'directory', 'path'}
        if not required.issubset(plan):
            raise ValueError('Child lane plan is incomplete')
        if plan.get('rate') != rate / lanes:
            raise ValueError('Child nominal lane rate differs from parent profile')
        canonical = _plan(run_id=plan['run_id'], origin_id=plan['origin_id'], profile=profile,
            label=plan['label'], lane=number, indices=plan['indices'], rate=plan['rate'], context=plan['context'])
        if 'requested_numeric_profile' in plan and numeric_profile(plan['requested_numeric_profile']) != profile:
            raise ValueError('Child lane profile differs from frozen parent')
        if 'commands' in plan and plan['commands'] != canonical['commands']:
            raise ValueError('Child command identities differ from frozen allocation')
        if (plan['origin_id'] in identifiers or plan['application_name'] in applications
                or reserved.intersection(plan['indices'])
                or plan['database_name'] != bootstrap['database_config']['NAME']):
            raise ValueError('Child lane origin or owning namespace conflicts')
        identifiers.add(plan['origin_id']); applications.add(plan['application_name'])
        reserved.update(plan['indices'])
        context = canonical['context']
        if not set(context_ids).union({'actor_id', 'source_warehouse_id', 'target_warehouse_id',
                'source_cluster', 'source_generation', 'topic', 'database_scope_digest',
                'source_context_digest'}).issubset(context):
            raise ValueError('Child lane context is incomplete')
        values = (plan['run_id'], plan['label'], *(context[key] for key in
            ('actor_id', 'source_warehouse_id', 'target_warehouse_id', 'source_cluster',
             'source_generation', 'topic', 'database_scope_digest', 'source_context_digest')))
        if shared is None:
            shared = values
        elif shared != values:
            raise ValueError('Child lane shared context differs from parent scope')
        for key, seen in context_ids.items():
            if context[key] in seen:
                raise ValueError('Child business fixtures are not distinct per lane')
            seen.add(context[key])
    if 'writer_topology' in profile and not {'start_index', 'requested_count'}.issubset(bootstrap):
        raise ValueError('Child explicit writer request lacks input reservation')
    if 'start_index' in bootstrap or 'requested_count' in bootstrap:
        start, count = bootstrap.get('start_index'), bootstrap.get('requested_count')
        if type(start) is not int or type(count) is not int or min(start, count) < 0:
            raise ValueError('Child input reservation is invalid')
        if reserved != set(range(start, start + count)):
            raise ValueError('Child plans omit or add requested command indices')
    return preset, lanes


def execute_inventory_command(context, seq, label, batch, state, data, *, lane, scheduled_at):
    from django.utils import timezone
    from labops.purchasing.services import create_receipt
    from django.db import transaction
    key = f'{context.args.run_id}:{seq}'
    rid = f'acceptance-{seq}'
    before = time.time()
    attempt = context.generation.attempt(batch)
    state.update(attempt=attempt, last_attempt=attempt, stage='business_transaction', committed=False)
    with transaction.atomic():
        with context.connection.cursor() as cursor:
            cursor.execute('SELECT txid_current()::text')
            inserted_xid = cursor.fetchone()[0]
        position = seq % 4
        if position == 0:
            receipt = create_receipt(context.admin, {'order_id': str(data['order'].id), 'lines': [{
                'order_line_id': str(data['order_line'].id), 'warehouse_id': str(context.source.id),
                'qty': '4', 'batch_no': f'ACCEPTANCE-{context.args.run_id[:24]}-{seq}',
                'supplier_lot': 'SYNTHETIC', 'expires_on': str(timezone.localdate() + timedelta(days=365))}]}, rid)
            data['batch'] = receipt.lines.first().batch
            movement = context.services.post_receipt(context.admin, receipt.id,
                {'expected_version': receipt.version, 'receipt_id': str(receipt.id)}, key, rid)
        elif position == 1:
            movement = context.services.issue(context.admin, {'task_id': str(data.get('task', context.task).id), 'lines': [{
                'batch_id': str(data['batch'].id), 'warehouse_id': str(context.source.id), 'qty': '1'}]}, key, rid)
            data['cycle_issue'] = movement
        elif position == 2:
            movement = context.services.transfer(context.admin, {'batch_id': str(data['batch'].id),
                'from_warehouse_id': str(context.source.id), 'to_warehouse_id': str(context.target.id), 'qty': '1'}, key, rid)
        else:
            assert data['cycle_issue'] is not None, 'Business lane lost its original issue before reversal'
            movement = context.services.reverse(context.admin, data['cycle_issue'].id,
                {'reason': 'Synthetic acceptance reversal'}, key, rid)
            data['cycle_issue'] = None
    # Commit accounting precedes every optional timestamp/outbox/log
    # observation, so an observation failure cannot erase a DB commit.
    state['stage'] = 'commit_accounting'
    context.commit_generated_movement(batch, attempt, movement)
    state['committed'] = True
    transaction_return = time.time()
    state['stage'] = 'commit_timestamp_observation'
    with context.connection.cursor() as cursor:
        cursor.execute('SHOW track_commit_timestamp')
        enabled = cursor.fetchone()[0] == 'on'
        if enabled:
            cursor.execute('SELECT pg_xact_commit_timestamp(%s::xid)', [inserted_xid])
            committed_at = cursor.fetchone()[0]
        else:
            committed_at = None
    state['stage'] = 'outbox_observation'
    event = context.models.OutboxEvent.objects.get(aggregate_id=movement.id)
    context.identify_generated_event(batch, attempt, event)
    item = {'event_id': str(event.id), 'movement_id': str(movement.id), 'kind': movement.type,
                'global_index': seq, 'business_lane': lane, 'scheduled_at_monotonic': scheduled_at,
                'scenario': label, 'command_started_at': before,
                'transaction_return_observed_at': transaction_return,
                'insert_transaction_xid': inserted_xid,
                'outbox_transaction_commit_at': committed_at.timestamp() if committed_at else None,
                'outbox_created_at': event.created_at.timestamp(),
                'payload_bytes': len(json.dumps(context.api.envelope(event)).encode())}
    state['stage'] = 'event_log_observation'
    context.record_generated_event(item)
    state['attempt'] = None
    return item


class BusinessExecutionError(RuntimeError):
    """Authored boundary information only; private exception text stays local."""
    def __init__(self, details):
        self.details = details
        self.authored_class = details.get('error_type', type(self).__name__)
        self.stage = details.get('stage', 'child_command')
        self.outcome = {'commit_unknown': 'unknown', 'not_entered': 'not_started'}.get(
            details.get('outcome'), 'unknown')
        super().__init__('Inventory child command failed; inspect origin evidence')


class InventoryProcessWorker:
    """One fresh Django-only client's context, journal and owning connection."""
    def __init__(self, lane, bootstrap):
        from benchmarks.events.diagnostic_profile import request_profile
        diagnostic_profile_enabled = bootstrap.get('diagnostic_profile_enabled', False)
        diagnostic_profile_engine = bootstrap.get('diagnostic_profile_engine', 'cprofile')
        request_profile(diagnostic_profile_enabled, diagnostic_profile_engine)
        if diagnostic_profile_engine != bootstrap['profile'].get('diagnostic_profile_engine', 'cprofile'):
            raise ValueError('Child diagnostic engine differs from frozen requested profile')
        _writer_topology, writer_lanes = validate_process_bootstrap(lane, bootstrap)
        import django
        os.environ['DJANGO_SETTINGS_MODULE'] = 'config.settings'
        os.environ['WORKER_METRICS_ENABLED'] = '0'
        django.setup()
        from django.conf import settings
        from django.db import connections
        from labops import models, events
        from labops.inventory import services
        from benchmarks.events.origin_journal import OriginJournal

        self.lane = lane
        self.plan = bootstrap['plans'][lane]
        self._indices = frozenset(self.plan['indices'])
        self.args = SimpleNamespace(run_id=self.plan['run_id'])
        self.directory = Path(self.plan['directory'])
        self.directory.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._failure = None
        self.state = {'attempt': None, 'stage': 'initialization'}
        self.runtime_diagnostics_enabled = bootstrap['runtime_diagnostics_enabled']
        self.diagnostic_profile_enabled = diagnostic_profile_enabled
        self.diagnostic_profile_engine = diagnostic_profile_engine
        self.cpu_profile = None
        self.models, self.api, self.services = models, events, services
        self.connections = connections
        # This is the actual owning caller's wrapper configuration, including
        # its runner-mutated test database NAME. It travels through private
        # spawn IPC and is never serialized into any evidence or command line.
        config = copy.deepcopy(bootstrap['database_config'])
        if config['ENGINE'] != 'django.db.backends.postgresql':
            raise ValueError('Process acceptance requires PostgreSQL')
        config.setdefault('OPTIONS', {})['application_name'] = self.plan['application_name']
        connections.close_all()
        connections.databases['default'] = config
        self.connection = connections['default']
        self.connection.settings_dict = config
        namespace = bootstrap['runtime_settings']
        allowed_settings = {'EVENT_TRANSPORT', 'KAFKA_TOPIC', 'KAFKA_SOURCE_CLUSTER_ID',
                            'KAFKA_SOURCE_STREAM_GENERATION', 'EVENT_MAX_PAYLOAD_BYTES'}
        if set(namespace) != allowed_settings or namespace['EVENT_TRANSPORT'] != 'kafka':
            connections.close_all()
            raise ValueError('Child event namespace is not the frozen Kafka runtime')
        for name, value in namespace.items():
            setattr(settings, name, value)
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT current_database(), pg_backend_pid(), backend_start, application_name '
                           'FROM pg_stat_activity WHERE pid = pg_backend_pid()')
            database_name, backend_pid, backend_start, application_name = cursor.fetchone()
        if (database_name != config['NAME'] or database_name != self.plan['database_name']
                or application_name != self.plan['application_name']):
            connections.close_all()
            raise ValueError('Child owning database namespace does not match frozen plan')
        self.backend_identity = {'backend_pid': backend_pid,
            'backend_start': backend_start.isoformat(), 'database_name': database_name,
            'application_name': application_name}
        self.runtime_namespace = {name: getattr(settings, name) for name in allowed_settings}

        context = self.plan['context']
        self.admin = models.User.objects.get(pk=context['actor_id'])
        self.source = models.Warehouse.objects.get(pk=context['source_warehouse_id'])
        self.target = models.Warehouse.objects.get(pk=context['target_warehouse_id'])
        self.task = models.Task.objects.get(pk=context['task_id'])
        self.data = {'order': models.PurchaseOrder.objects.get(pk=context['order_id']),
            'order_line': models.OrderLine.objects.get(pk=context['order_line_id']),
            'task': self.task,
            'batch': models.Batch.objects.get(pk=context['batch_id']) if context['batch_id'] else None,
            'cycle_issue': models.StockMovement.objects.get(pk=context['cycle_issue_id'])
                if context['cycle_issue_id'] else None}
        if (self.data['order_line'].order_id != self.data['order'].id
                or str(self.task.project_id) != context['project_id']
                or str(self.data['order_line'].request_line.request.project_id) != context['project_id']):
            connections.close_all()
            raise ValueError('Child order line is outside its frozen lane')
        self.generation = OriginJournal(self.directory, run_id=self.plan['run_id'],
            origin_id=self.plan['origin_id'], profile=bootstrap['profile'],
            label=self.plan['label'], lane=lane, indices=self.plan['indices'],
            rate=bootstrap['rate'] / writer_lanes, context=context)
        self.batch = self.generation.begin_batch(len(self.plan['indices']), bootstrap['rate'] / writer_lanes,
                                               self.plan['label'])
        self._events = (self.directory / 'events.jsonl').open('x')
        if self.diagnostic_profile_enabled:
            from benchmarks.events.diagnostic_profile import CPUProfile
            from labops.purchasing import services as purchasing
            self.cpu_profile = CPUProfile('generator', lane=lane, engine=self.diagnostic_profile_engine)
            self.cpu_profile.hook(purchasing, 'create_receipt', 'receipt_create', expected=purchasing.create_receipt)
            self.cpu_profile.hook(services, 'post_receipt', 'receipt_post', expected=services.post_receipt)
            for name in ('commit_generated_movement', 'identify_generated_event', 'record_generated_event'):
                self.cpu_profile.hook(self, name, 'provenance_journal', expected=getattr(self, name))

    def ready_metadata(self):
        from benchmarks.events.process_resources import own_process_snapshot
        return {'process_snapshot': own_process_snapshot(),
                'backend_identity': self.backend_identity,
                'runtime_namespace': self.runtime_namespace}

    def commit_generated_movement(self, batch, attempt, movement):
        self.generation.commit(batch, attempt, movement_id=str(movement.id),
                               request_hash=movement.request_hash)

    def identify_generated_event(self, batch, attempt, event):
        from django.conf import settings
        from labops.event_schema import canonical_payload_hash, validate_inventory_envelope
        value = self.api.raw_envelope(event)
        validate_inventory_envelope(value, max_bytes=settings.EVENT_MAX_PAYLOAD_BYTES)
        digest = canonical_payload_hash(value)
        if event.payload_hash != digest:
            raise ValueError('Child original outbox immutable hash mismatch')
        self.generation.identify_event(batch, attempt, str(event.id), payload_hash=digest)

    def record_generated_event(self, item):
        item['origin_id'] = self.plan['origin_id']
        self._events.write(json.dumps(item, sort_keys=True) + '\n')
        self._events.flush()

    def execute(self, global_index, target_monotonic, emit_status):
        if global_index not in self._indices:
            raise ValueError('Driver command is outside the child frozen plan')
        self.generation.bind_origin(self.batch, global_index)
        self.state = {'attempt': None, 'stage': 'initialization'}
        original = None
        observer = None
        item = None
        from contextlib import nullcontext
        profiled = self.cpu_profile.call('generator_command', global_index) if self.cpu_profile else nullcontext()
        def invoke():
            if self.cpu_profile and self.cpu_profile.engine == 'python-profile-owned':
                return self.cpu_profile.run('generator_command', global_index, execute_inventory_command,
                    self, global_index, self.plan['label'], self.batch, self.state, self.data,
                    lane=self.lane, scheduled_at=target_monotonic)
            with profiled:
                return execute_inventory_command(self, global_index, self.plan['label'],
                    self.batch, self.state, self.data, lane=self.lane, scheduled_at=target_monotonic)
        try:
            if self.runtime_diagnostics_enabled:
                from benchmarks.events.runtime_diagnostics import CommandDiagnostics
                observer = CommandDiagnostics(self.connection)
                with observer:
                    try:
                        item = invoke()
                    except BaseException as exc:
                        original = exc
                        raise
            else:
                item = invoke()
        except BaseException as exc:
            original = original or exc
        finally:
            if observer is not None:
                try:
                    summary = observer.summary()
                    with (self.directory / 'command-diagnostics.jsonl').open('a') as out:
                        out.write(json.dumps({'global_index': global_index,
                            'business_lane': self.lane, 'scenario': self.plan['label'],
                            'kind': ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')[global_index % 4],
                            'origin_id': self.plan['origin_id'], **summary}, sort_keys=True) + '\n')
                    if summary.get('diagnostic_errors') or summary.get('collection_complete') is not True:
                        raise RuntimeError('Child command diagnostic evidence did not qualify')
                except BaseException as diagnostic_error:
                    original = original or diagnostic_error
        if original is not None:
            self._failure = {'error_type': type(original).__name__,
                'stage': self.state['stage'], 'global_index': global_index,
                'outcome': 'commit_unknown' if self.state.get('last_attempt') is not None else 'not_entered'}
            try:
                self.generation.finish_failure(self.batch, self.state['stage'],
                    type(original).__name__, attempt_id=self.state.get('last_attempt'),
                    outcome=self._failure['outcome'])
            except BaseException as accounting_error:
                self._failure['accounting_error_type'] = type(accounting_error).__name__
            raise BusinessExecutionError(self._failure) from None
        return item

    def close(self):
        from benchmarks.events.process_resources import own_process_snapshot
        original = None
        metadata = {'backend_identity': self.backend_identity,
                    'in_atomic_block': self.connection.in_atomic_block,
                    'lane_state': {'batch_id': str(self.data['batch'].id) if self.data['batch'] else None,
                        'cycle_issue_id': str(self.data['cycle_issue'].id) if self.data['cycle_issue'] else None}}
        try:
            summary = self.generation.batch_summary(self.batch)
            if summary['status'] == 'running':
                if summary['identified_events'] == summary['requested']:
                    self.generation.finish_success(self.batch)
                else:
                    self.generation.finish_failure(self.batch, 'peer_lane_stop', 'PeerLaneFailure',
                        attempt_id=self.state['attempt'], outcome='commit_unknown')
            metadata['origin_summary'] = self.generation.finalize()
            self._events.flush()
            os.fsync(self._events.fileno())
        except BaseException as exc:
            original = exc
        finally:
            if self.cpu_profile is not None:
                try:
                    metadata['diagnostic_profile'] = self.cpu_profile.close(self.directory / 'diagnostic-profile.json')
                except BaseException as error:
                    # Profiling failure is secondary; normal journal and owning
                    # DB cleanup must still complete, even after a body error.
                    metadata['diagnostic_profile'] = {'complete': False,
                        'errors': [{'stage': 'close', 'error_type': type(error).__name__}]}
            try:
                self._events.close()
            except BaseException as exc:
                original = original or exc
            try:
                self.connections.close_all()
                metadata['connection_closed'] = self.connection.connection is None
            except BaseException as exc:
                original = original or exc
                metadata['connection_closed'] = False
            self._closed = metadata.get('connection_closed') is True
            try:
                metadata['process_snapshot'] = own_process_snapshot()
            except BaseException as exc:
                original = original or exc
                metadata['process_snapshot'] = {'status': 'unavailable', 'error_type': type(exc).__name__}
        if original is not None:
            raise BusinessExecutionError({'stage': 'owning_cleanup', 'error_type': type(original).__name__,
                'outcome': 'commit_unknown', 'cleanup_metadata': metadata}) from None
        return metadata


def inventory_process_worker(lane, bootstrap):
    return InventoryProcessWorker(lane, bootstrap)
