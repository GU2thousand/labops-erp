"""Disposable acceptance process wrappers; no production fault switches."""
import argparse
from contextlib import contextmanager
import json
import hashlib
import math
import os
from pathlib import Path
import re
import signal
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def atomic_json(path, value):
    target = Path(path)
    temporary = target.with_suffix(target.suffix + '.tmp')
    temporary.write_text(json.dumps(value, sort_keys=True) + '\n')
    temporary.replace(target)


def identity_error(stage, error):
    name = type(error).__name__
    return {'stage': stage, 'error_type': name if re.fullmatch(
        r'[A-Za-z][A-Za-z0-9_]{0,79}', name) else 'BaseException'}


def worker_process_snapshot(snapshot):
    """Project the child-origin process receipt without arbitrary extra fields."""
    observed = snapshot()
    if not isinstance(observed, dict):
        raise ValueError('Worker process snapshot must be an object')

    def unsigned(value):
        return value if type(value) is int and 0 <= value < 1 << 64 else None

    def number(value):
        try:
            return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
        except (OverflowError, ValueError):
            return None

    result = {'source': 'child_origin' if observed.get('source') == 'child_origin' else None,
        'status': observed.get('status') if observed.get('status') in {'available', 'unavailable'} else 'unavailable',
        **{name: unsigned(observed.get(name)) for name in ('pid', 'start_time_ticks', 'rss_bytes')},
        **{name: number(observed.get(name)) for name in
           ('user_cpu_seconds', 'system_cpu_seconds', 'captured_monotonic')}}
    error_type = observed.get('error_type')
    if isinstance(error_type, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', error_type):
        result['error_type'] = error_type
    return result


def validate_process_identity(observed, pid, start_time_ticks=None):
    if (observed['source'] != 'child_origin' or observed['status'] != 'available'
            or observed['pid'] != pid or observed['start_time_ticks'] is None
            or any(observed[name] is None for name in
                   ('rss_bytes', 'user_cpu_seconds', 'system_cpu_seconds', 'captured_monotonic'))
            or (start_time_ticks is not None and observed['start_time_ticks'] != start_time_ticks)):
        raise ValueError('Worker process identity is unavailable or changed')


@contextmanager
def validation_worker_identity(connection, connections, *, receipt_path, role,
                               application_name, snapshot=None, writer=None):
    """Opt-in receipts for this validation process and its owning DB thread.

    The startup query opens the worker's ordinary connection before delivery.
    Both presets use this same instrumentation; it is not a cold-start sample.
    Closing this thread's wrappers cannot prove that other worker threads have
    closed their sessions. The coordinator separately settles the exact native
    application-name namespace after it has reaped the process.
    """
    if snapshot is None:
        from benchmarks.events.process_resources import own_process_snapshot
        snapshot = own_process_snapshot
    writer = writer or atomic_json
    owner_thread = threading.get_ident()
    safe_role = role if isinstance(role, str) and re.fullmatch(r'[a-z][a-z0-9_.-]{0,63}', role) else None
    safe_application = application_name if isinstance(application_name, str) and re.fullmatch(
        r'[A-Za-z][A-Za-z0-9_.:-]{0,62}', application_name) else None
    base = {'version': 'validation-worker-identity-v1', 'role': safe_role,
            'pid': os.getpid(), 'expected_application_name': safe_application,
            'connection_scope': 'owning_thread', 'all_process_sessions_closed': None}
    started = {**base, 'status': 'failed', 'process_snapshot': None,
               'backend_identity': None, 'autocommit': None, 'in_atomic_block': None,
               'errors': []}
    startup_stage, startup_written, original = 'configuration', False, None
    cleanup_error = None
    try:
        if safe_role is None or safe_application is None:
            raise ValueError('Invalid validation worker namespace')
        if threading.current_thread() is not threading.main_thread():
            raise ValueError('Worker identity must be owned by the main thread')
        if connection.settings_dict['ENGINE'] != 'django.db.backends.postgresql':
            raise ValueError('Worker identity requires PostgreSQL')
        if connection.connection is not None:
            raise ValueError('Worker identity must precede the owning connection')
        # Copy only OPTIONS, retaining binding, timeouts and every other native
        # option. New thread-local wrappers inherit this process's same tag.
        options = dict(connection.settings_dict.get('OPTIONS') or {})
        options['application_name'] = application_name
        connection.settings_dict['OPTIONS'] = options
        startup_stage = 'native_identity'
        connection.ensure_connection()
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_backend_pid(), current_database(), current_setting('application_name')")
            backend_pid, database_name, actual_application = cursor.fetchone()
        if (type(backend_pid) is not int or backend_pid <= 0
                or database_name != connection.settings_dict['NAME']
                or actual_application != application_name):
            raise ValueError('Worker native database identity does not match its namespace')
        started['backend_identity'] = {'backend_pid': backend_pid,
            'database_name': database_name, 'application_name': actual_application}
        autocommit, in_atomic_block = connection.get_autocommit(), connection.in_atomic_block
        if type(autocommit) is not bool or type(in_atomic_block) is not bool:
            raise ValueError('Worker transaction state is unavailable')
        started['autocommit'], started['in_atomic_block'] = autocommit, in_atomic_block
        startup_stage = 'process_identity'
        started['process_snapshot'] = worker_process_snapshot(snapshot)
        validate_process_identity(started['process_snapshot'], base['pid'])
        started['status'] = 'ready'
        startup_stage = 'startup_receipt'
        writer(str(receipt_path) + '.started', started)
        startup_written = True
        yield
    except BaseException as error:
        original = error
        if not startup_written:
            started['status'] = 'failed'
            started['errors'].append(identity_error(startup_stage, error))
            try:
                writer(str(receipt_path) + '.started', started)
            except BaseException as receipt_error:
                started['errors'].append(identity_error('startup_receipt', receipt_error))
        raise
    finally:
        closed = {**base, 'status': 'incomplete',
            'startup_backend_identity': started['backend_identity'],
            'process_snapshot': None, 'closing_backend_pid': None,
            'autocommit': None, 'in_atomic_block': None,
            'connection_closed': False, 'cleanup_complete': False,
            'errors': list(started['errors'])}
        if original is not None:
            closed['operation_error_type'] = identity_error('operation', original)['error_type']
        try:
            if (threading.current_thread() is not threading.main_thread()
                    or threading.get_ident() != owner_thread):
                raise ValueError('Worker cleanup changed its owning thread')
            in_atomic_block = connection.in_atomic_block
            if type(in_atomic_block) is not bool:
                raise ValueError('Final worker transaction state is unavailable')
            closed['in_atomic_block'] = in_atomic_block
            if connection.connection is not None:
                backend_pid = connection.connection.info.backend_pid
                if type(backend_pid) is not int or backend_pid <= 0:
                    raise ValueError('Final owning worker backend identity is unavailable')
                closed['closing_backend_pid'] = backend_pid
                autocommit = connection.get_autocommit()
                if type(autocommit) is not bool:
                    raise ValueError('Final worker autocommit state is unavailable')
                closed['autocommit'] = autocommit
        except BaseException as error:
            if cleanup_error is None:
                cleanup_error = error
            closed['errors'].append(identity_error('final_connection_state', error))
        try:
            if (threading.current_thread() is not threading.main_thread()
                    or threading.get_ident() != owner_thread):
                raise ValueError('Worker cleanup changed its owning thread')
            connections.close_all()
            closed['connection_closed'] = connection.connection is None
            if not closed['connection_closed']:
                raise ValueError('Owning worker connection remains open')
        except BaseException as error:
            if cleanup_error is None:
                cleanup_error = error
            closed['errors'].append(identity_error('owning_connection_close', error))
        try:
            closed['process_snapshot'] = worker_process_snapshot(snapshot)
            startup_process = started['process_snapshot'] or {}
            validate_process_identity(closed['process_snapshot'], base['pid'],
                                      startup_process.get('start_time_ticks'))
        except BaseException as error:
            if cleanup_error is None:
                cleanup_error = error
            closed['errors'].append(identity_error('final_process_identity', error))
        closed['cleanup_complete'] = cleanup_error is None and closed['connection_closed']
        closed['status'] = 'closed' if closed['cleanup_complete'] else 'incomplete'
        try:
            writer(str(receipt_path) + '.closed', closed)
        except BaseException as error:
            if cleanup_error is None:
                cleanup_error = error
        # A receipt or close failure is secondary to the original startup or
        # business BaseException, including an operation deadline or signal.
        if original is None and cleanup_error is not None:
            raise cleanup_error


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('role', choices=['consumer', 'publisher', 'rebuild', 'business', 'restore_check'])
    p.add_argument('--consumer', choices=['notification', 'analytics'])
    p.add_argument('--stage', default='normal')
    p.add_argument('--event')
    p.add_argument('--event-file')
    p.add_argument('--marker')
    p.add_argument('--observations')
    p.add_argument('--max-messages', type=int, default=0)
    p.add_argument('--data-file')
    args = p.parse_args()
    import django
    django.setup()
    receipt_path = os.environ.get('LABOPS_VALIDATION_WORKER_IDENTITY')
    if not receipt_path:
        return _run(args)
    from django.db import connection, connections
    with validation_worker_identity(connection, connections, receipt_path=receipt_path,
            role=os.environ.get('LABOPS_VALIDATION_WORKER_ROLE'),
            application_name=os.environ.get('LABOPS_VALIDATION_WORKER_APPLICATION_NAME')):
        return _run(args)


def _run(args):
    from django.core.management import call_command
    from labops import events

    context = {}

    def pause(stage):
        atomic_json(args.marker, {**context, 'stage': stage, 'pid': os.getpid(),
                                 'at': time.time()})
        if stage in {'before_delivery', 'stale_owner', 'postgres_effect_commit', 'before_business_commit'}:
            while not Path(args.marker + '.release').exists():
                time.sleep(.05)
        else:
            while True:
                signal.pause()

    if args.role == 'rebuild':
        call_command('rebuild_inventory_projection', verbosity=0)
        return
    if args.role == 'business':
        from labops.models import User
        from labops.inventory.services import issue
        data = json.loads(Path(args.data_file).read_text())
        original_emit = events.emit_inventory

        def emit(movement):
            event = original_emit(movement)
            context.update(event_id=str(event.id), movement_id=str(movement.id))
            pause('before_business_commit')
            return event
        events.emit_inventory = emit
        try:
            issue(User.objects.get(id=data['user_id']), data['command'], data['key'], data['request_id'])
        except Exception as exc:
            atomic_json(args.marker + '.error', {**context, 'error_type': type(exc).__name__})
            raise
        atomic_json(args.marker + '.completed', context)
        return
    if args.role == 'restore_check':
        from labops.models import OutboxEvent, ProcessedEvent, Notification, InventoryProjection
        ids = json.loads(Path(args.data_file).read_text())

        def snapshot():
            projection = sorted((str(b), str(w), str(q)) for b, w, q in
                InventoryProjection.objects.values_list('batch_id', 'warehouse_id', 'quantity'))
            return {'processed': ProcessedEvent.objects.count(), 'notifications': Notification.objects.count(),
                    'projection_hash': hashlib.sha256(json.dumps(projection, separators=(',', ':')).encode()).hexdigest()}
        before = snapshot()
        redelivered = 0
        for event in OutboxEvent.objects.filter(id__in=ids):
            for consumer in ('notification', 'analytics'):
                assert events.process_envelope(consumer, events.envelope(event)) is False
                redelivered += 1
        after = snapshot()
        assert before == after, 'Restored DB replay changed an already checkpointed effect'
        atomic_json(args.marker, {'database_effects_before': before, 'database_effects_after': after,
                    'same_id_deliveries': redelivered, 'additional_effects': 0})
        return
    if args.role == 'publisher':
        original_send = events.send
        real = events.producer()

        class ProducerProxy:
            def produce(self, *a, **kw):
                callback = kw.get('on_delivery')

                def observed(error, message):
                    if error is None:
                        context.setdefault('acknowledged_records', []).append({
                            'topic': message.topic(), 'partition': message.partition(),
                            'offset': message.offset(), 'at': time.time()})
                    if callback:
                        callback(error, message)
                kw['on_delivery'] = observed
                return real.produce(*a, **kw)

            def __getattr__(self, name):
                return getattr(real, name)

        def send(client, topic, key, value):
            context.update(event_id=value.get('event_id'), key=str(key))
            if args.event and value.get('event_id') != args.event:
                raise AssertionError('Publisher selected a different outbox row')
            if args.stage in {'before_send', 'stale_owner'}:
                pause(args.stage)
            result = original_send(client, topic, key, value)
            if args.stage == 'after_ack':
                pause('after_ack')
            return result
        events.send = send
        try:
            result = events.publish_one(ProducerProxy())
        except events.LeaseLost:
            if args.stage != 'stale_owner':
                raise
            context['stale_writeback_rejected'] = True
            result = False
        if args.marker:
            atomic_json(args.marker + '.completed', {**context, 'result': result})
        return

    from labops.management.commands import consume_kafka
    base_consumer = consume_kafka.Consumer

    def identified_consumer(config):
        return base_consumer({**config, 'client.id': 'acceptance-' + str(os.getpid())})
    consume_kafka.Consumer = identified_consumer
    if args.event and args.max_messages:
        original_consumer = consume_kafka.Consumer

        class TargetAwareConsumer:
            def __init__(self, config):
                self.client = original_consumer(config)

            def __getattr__(self, name):
                return getattr(self.client, name)

            def commit(self, *a, **kw):
                result = self.client.commit(*a, **kw)
                if context.get('event_id') == args.event:
                    # Trigger the real command's StopController only after its
                    # synchronous target offset commit. Earlier redeliveries
                    # cannot prematurely exhaust a one-message limit.
                    os.kill(os.getpid(), signal.SIGTERM)
                return result
        consume_kafka.Consumer = TargetAwareConsumer
    original_deliver = consume_kafka.deliver
    original_constraints = events.connection.check_constraints
    original_budget = consume_kafka.database_processing_budget
    original_process = events.process_envelope

    def process_envelope(name, event):
        if args.stage == 'transient_failure' and event.get('event_id') == args.event:
            raise ConnectionError('Synthetic temporary dependency failure')
        return original_process(name, event)
    events.process_envelope = process_envelope

    @contextmanager
    def committed_budget():
        try:
            with original_budget():
                yield
        except Exception as exc:
            if args.marker and context.get('event_id') == args.event:
                atomic_json(args.marker + '.error', {**context, 'error_type': type(exc).__name__})
            raise
        # The enclosing consumer transaction is now durably committed, while
        # the real command has not reached its synchronous broker commit.
        if args.stage == 'after_commit' and context.get('event_id') == args.event:
            pause('after_commit')
    consume_kafka.database_processing_budget = committed_budget

    def constraints(*a, **kw):
        result = original_constraints(*a, **kw)
        if args.stage in {'before_commit', 'postgres_effect_commit'} and context.get('event_id') == args.event:
            pause(args.stage)
        return result
    events.connection.check_constraints = constraints

    def deliver(name, event, key, *a, **kw):
        if args.event_file and Path(args.event_file).exists():
            args.event = json.loads(Path(args.event_file).read_text())['event_id']
        context.clear()
        context.update(event_id=event.get('event_id'), consumer=name,
                       delivery_key=key, received_at=time.time())
        target = args.event and context['event_id'] == args.event
        if target and args.marker:
            atomic_json(args.marker + '.received', context)
        if target and args.stage == 'before_delivery':
            pause('before_delivery')
        try:
            result = original_deliver(name, event, key, *a, **kw)
        except Exception as exc:
            if target and args.marker:
                atomic_json(args.marker + '.error', {**context, 'error_type': type(exc).__name__})
            raise
        observation = {**context, 'completed_at': time.time(), 'result': result}
        if args.observations:
            with Path(args.observations).open('a') as out:
                out.write(json.dumps(observation, sort_keys=True) + '\n')
                out.flush()
        if target and args.marker:
            atomic_json(args.marker + '.completed', observation)
        return result
    consume_kafka.deliver = deliver
    call_command('consume_kafka', args.consumer, max_messages=0 if args.event else args.max_messages,
                 idle_timeout=0, verbosity=0)


if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    main()
