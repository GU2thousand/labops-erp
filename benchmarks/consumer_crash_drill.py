"""SIGKILL the real Kafka consumer at two transaction boundaries, then replay.

Only generated labops_phase8_* databases/topics/groups are created or removed.
Requires local PostgreSQL CREATEDB access and a local Kafka-compatible broker.
No production command hooks are added: the child wraps the real command at the
precise boundary, signals readiness to its parent, and is killed externally.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse, urlunparse
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def write_json(path, value):
    temporary = Path(str(path) + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def worker(args):
    import django
    django.setup()
    from django.core.management import call_command
    from labops import events
    from labops.management.commands import consume_kafka

    original_consumer = consume_kafka.Consumer
    def isolated_consumer(config):
        return original_consumer({**config, 'group.id': args.group,
            'session.timeout.ms': 6000, 'heartbeat.interval.ms': 2000})
    consume_kafka.Consumer = isolated_consumer
    original_deliver = consume_kafka.deliver
    original_constraints = events.connection.check_constraints
    delivery = {}

    def pause_at_boundary():
        write_json(args.marker, {**delivery, 'stage': args.stage, 'pid': os.getpid()})
        while True:
            signal.pause()  # Parent sends real SIGKILL; no Python cleanup runs.

    def constraints(*a, **kw):
        original_constraints(*a, **kw)
        if args.stage == 'before_commit':
            pause_at_boundary()
    events.connection.check_constraints = constraints

    def deliver(name, event, key):
        assert event['event_id'] == args.event, 'Unexpected event in isolated topic'
        delivery.update(event_id=event['event_id'], delivery_key=key)
        write_json(str(args.marker) + '.received', delivery)
        result = original_deliver(name, event, key)
        if args.stage == 'after_commit':
            pause_at_boundary()
        return result
    consume_kafka.deliver = deliver
    call_command('consume_kafka', args.consumer, max_messages=1, idle_timeout=30)
    assert delivery, 'Consumer timed out without receiving the event'
    write_json(args.marker, {**delivery, 'stage': 'completed'})


def run(args):
    import psycopg
    from psycopg import sql
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    parsed = urlparse(args.database_url)
    if parsed.scheme not in {'postgres', 'postgresql'} or parsed.hostname not in {'localhost', '127.0.0.1'}:
        raise ValueError('This drill requires a loopback PostgreSQL URL')
    if any(server.split(':')[0] not in {'localhost', '127.0.0.1'} for server in args.bootstrap_servers.split(',')):
        raise ValueError('This drill requires loopback broker addresses')
    name = 'labops_phase8_' + uuid.uuid4().hex[:12]
    db_url = urlunparse(parsed._replace(path='/' + name))
    admin_url = urlunparse(parsed._replace(path='/postgres'))
    broker = AdminClient({'bootstrap.servers': args.bootstrap_servers})
    topics, groups, children = [], [], []
    admin = psycopg.connect(admin_url, autocommit=True)
    admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    # Prevent inherited legacy POSTGRES_* overrides selecting a different DB.
    for key in list(os.environ):
        if key.startswith('POSTGRES_'):
            os.environ.pop(key)
    os.environ.update(DJANGO_SETTINGS_MODULE='config.settings', LABOPS_DB_MODE='postgres',
        DATABASE_URL=db_url, LABOPS_EVENT_TRANSPORT='local', REDIS_URL='',
        OTEL_EXPORTER_OTLP_ENDPOINT='', KAFKA_BOOTSTRAP_SERVERS=args.bootstrap_servers)
    report = {'test': 'actual_consumer_sigkill', 'database': name,
              'isolation': 'new disposable database and one topic/group per case', 'cases': []}
    try:
        import django
        django.setup()
        from django.conf import settings
        from django.core.management import call_command
        from django.db import connections
        from django.utils import timezone
        from labops.models import User, Task, StockBalance, InventoryProjection, OutboxEvent, ProcessedEvent, Notification, FailedDelivery
        from labops.inventory.services import issue, reconcile
        from labops.events import producer, publish_one
        call_command('migrate', interactive=False, verbosity=0)
        call_command('seed_demo', verbosity=0)
        call_command('rebuild_inventory_projection', verbosity=0)
        settings.EVENT_TRANSPORT = 'kafka'
        user = User.objects.get(email='admin@labops.local')
        task = Task.objects.filter(status='IN_PROGRESS', project__status='ACTIVE').first()
        balance = StockBalance.objects.filter(on_hand_qty__gt=10, batch__expires_on__gte=timezone.localdate()).first()
        sender = producer()

        def effects(consumer, event):
            count = ProcessedEvent.objects.filter(consumer_name=consumer, event_id=event.id).count()
            if consumer == 'analytics':
                value = str(InventoryProjection.objects.get(batch_id=balance.batch_id, warehouse_id=balance.warehouse_id).quantity)
            else:
                value = Notification.objects.filter(event_id=event.id).count()
            return {'processed_records': count, 'effect': value}

        def committed(group, topic):
            c = Consumer({'bootstrap.servers': args.bootstrap_servers, 'group.id': group})
            try:
                return c.committed([TopicPartition(topic, 0)], timeout=10)[0].offset
            finally:
                c.close()

        def start(consumer, stage, group, event, marker, log):
            env = {**os.environ, 'LABOPS_EVENT_TRANSPORT': 'kafka', 'KAFKA_TOPIC': settings.KAFKA_TOPIC}
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--worker',
                '--consumer', consumer, '--stage', stage, '--group', group,
                '--event', str(event.id), '--marker', str(marker)], cwd=ROOT, env=env,
                stdout=log, stderr=log)
            children.append(child)
            return child

        with tempfile.TemporaryDirectory(prefix=name + '_') as temporary:
            for consumer in ['analytics', 'notification']:
                for stage in ['before_commit', 'after_commit']:
                    case = consumer + '_' + stage
                    topic = name + '_' + case
                    group = name + '_' + case
                    topics.append(topic); groups.append(group)
                    broker.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(20)
                    settings.KAFKA_TOPIC = topic
                    movement = issue(user, {'task_id': str(task.id), 'lines': [{'batch_id': str(balance.batch_id),
                        'warehouse_id': str(balance.warehouse_id), 'qty': 1}]}, name + case, 'phase8-sigkill')
                    event = OutboxEvent.objects.get(aggregate_id=movement.id)
                    assert publish_one(sender)
                    event.refresh_from_db(); assert event.status == 'PUBLISHED'
                    before = effects(consumer, event)
                    marker = Path(temporary) / (case + '.json')
                    with (Path(temporary) / (case + '.log')).open('w+') as log:
                        child = start(consumer, stage, group, event, marker, log)
                        deadline = time.monotonic() + 60
                        while not marker.exists():
                            if child.poll() is not None or time.monotonic() > deadline:
                                log.seek(0); raise AssertionError('Consumer did not reach boundary: ' + log.read())
                            time.sleep(.1)
                        paused = json.loads(marker.read_text())
                        offset = int(paused['delivery_key'].rsplit(':', 1)[1])
                        assert committed(group, topic) <= offset, 'Offset acknowledged before kill'
                        visible = effects(consumer, event)
                        if stage == 'before_commit':
                            assert visible == before, 'Uncommitted consumer effect became visible'
                        else:
                            assert visible['processed_records'] == 1
                        os.kill(child.pid, signal.SIGKILL)
                        assert child.wait(timeout=10) == -signal.SIGKILL
                        after_kill = effects(consumer, event)
                        assert after_kill == visible
                        recovered_marker = Path(temporary) / (case + '-recovered.json')
                        recovered = start(consumer, 'recover', group, event, recovered_marker, log)
                        assert recovered.wait(timeout=60) == 0, 'Recovery consumer failed'
                        replay = json.loads(recovered_marker.read_text())
                        assert replay['delivery_key'] == paused['delivery_key'], 'Recovery did not redeliver same broker offset'
                        after = effects(consumer, event)
                        assert after['processed_records'] == 1
                        if consumer == 'analytics':
                            from decimal import Decimal
                            assert Decimal(after['effect']) == Decimal(before['effect']) - 1
                        else:
                            assert after['effect'] == len(event.payload_json['recipients'])
                        assert committed(group, topic) == offset + 1
                        if stage == 'after_commit':
                            assert after == after_kill, 'Replay duplicated an already committed effect'
                        report['cases'].append({'consumer': consumer, 'kill_boundary': stage,
                            'signal': 'SIGKILL', 'exit_code': -9, 'event_id': str(event.id),
                            'broker_offset_replayed': offset, 'offset_committed_after_recovery': offset + 1,
                            'before': before, 'visible_at_kill': visible, 'after_recovery': after,
                            'exactly_one_database_effect': True})
                        # Bring the counterpart up to date before the next ledger mutation.
                        other = 'notification' if consumer == 'analytics' else 'analytics'
                        other_group = group + '_counterpart'; groups.append(other_group)
                        counterpart = start(other, 'recover', other_group, event,
                            Path(temporary) / (case + '-counterpart.json'), log)
                        assert counterpart.wait(timeout=60) == 0
                    assert not reconcile()
                    assert all(InventoryProjection.objects.get(batch_id=b.batch_id, warehouse_id=b.warehouse_id).quantity == b.on_hand_qty for b in StockBalance.objects.all())
                    assert not FailedDelivery.objects.exists()
                    print('Passed ' + case, flush=True)
        report.update(ledger_reconciles=True, projection_matches_all_balances=True, failed_deliveries=0)
        write_json(args.output, report)
        print(json.dumps(report, indent=2))
    finally:
        for child in children:
            if child.poll() is None:
                child.kill(); child.wait(timeout=10)
        # Only resources whose fresh names were generated in this invocation.
        for group, future in broker.delete_consumer_groups(groups).items() if groups else []:
            try: future.result(20)
            except Exception as exc: print('Group cleanup:', group, type(exc).__name__, file=sys.stderr)
        for topic, future in broker.delete_topics(topics).items() if topics else []:
            try: future.result(20)
            except Exception as exc: print('Topic cleanup:', topic, type(exc).__name__, file=sys.stderr)
        if 'connections' in locals(): connections.close_all()
        admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        admin.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database-url', default=os.environ.get('DRILL_DATABASE_URL'))
    parser.add_argument('--bootstrap-servers', default='127.0.0.1:19093')
    parser.add_argument('--output', type=Path, default=ROOT / 'benchmarks/results/consumer-crash.json')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    for option in ['consumer', 'stage', 'group', 'event', 'marker']:
        parser.add_argument('--' + option, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args)
    elif not args.database_url:
        parser.error('Set DRILL_DATABASE_URL to a local PostgreSQL URL with CREATEDB access')
    else:
        run(args)
