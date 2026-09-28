"""Disposable acceptance process wrappers; no production fault switches."""
import argparse
from contextlib import contextmanager
import json
import hashlib
import os
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def atomic_json(path, value):
    target = Path(path)
    temporary = target.with_suffix(target.suffix + '.tmp')
    temporary.write_text(json.dumps(value, sort_keys=True) + '\n')
    temporary.replace(target)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('role', choices=['consumer', 'publisher', 'rebuild', 'business', 'restore_check'])
    p.add_argument('--consumer', choices=['notification', 'analytics'])
    p.add_argument('--stage', default='normal')
    p.add_argument('--event')
    p.add_argument('--marker')
    p.add_argument('--observations')
    p.add_argument('--max-messages', type=int, default=0)
    p.add_argument('--data-file')
    args = p.parse_args()
    import django
    django.setup()
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
