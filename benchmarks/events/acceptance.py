"""Real, isolated PostgreSQL / RF3 SASL_SSL acceptance; see README.md."""
import argparse
from collections import Counter
import csv
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import signal
import subprocess
import sys
import time
from urllib.parse import urlparse, urlunparse
import uuid

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + '\n')
    temporary.replace(path)


def canonical_hash(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':'),
                                     default=str).encode()).hexdigest()


def load_environment(path):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:]
        key, separator, raw = line.partition('=')
        if not separator or not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
            raise ValueError('Invalid client.env assignment')
        parsed = shlex.split(raw, comments=True)
        os.environ[key] = parsed[0] if len(parsed) == 1 else raw


def stable_committed_offsets(configs, topic, *, timeout=30, consumer_factory=None,
                             monotonic=time.monotonic, sleep=time.sleep, on_retry=None):
    """Require two complete equal snapshots; refresh only coordinator errors.

    A fresh unassigned client repeats FindCoordinator after startup/election.
    Authentication, authorization, topic errors and malformed responses fail
    immediately rather than becoming a successful empty offset snapshot.
    """
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, OFFSET_INVALID
    factory = consumer_factory or Consumer
    coordinator_codes = {KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_NOT_AVAILABLE,
                         KafkaError.COORDINATOR_LOAD_IN_PROGRESS, KafkaError._WAIT_COORD}
    deadline = monotonic() + timeout
    previous = None
    attempts = 0
    while monotonic() < deadline:
        attempts += 1
        snapshot = {}
        try:
            for name, config in configs.items():
                client = factory(config)
                try:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError('Committed-offset snapshot deadline expired')
                    partitions = client.committed([TopicPartition(topic, n) for n in range(3)],
                                                  timeout=min(10, remaining))
                    errors = [p.error for p in partitions if p.error is not None]
                    # A mixed response must never retry past an ACL failure.
                    terminal = next((error for error in errors
                                     if error.code() not in coordinator_codes), None)
                    if terminal is not None:
                        raise KafkaException(terminal)
                    if errors:
                        raise KafkaException(errors[0])
                    expected = {(topic, n) for n in range(3)}
                    assert len(partitions) == 3 and {(p.topic, p.partition) for p in partitions} == expected, \
                        'Committed-offset response must include exactly the three requested partitions'
                    assert all(p.offset == OFFSET_INVALID or p.offset >= 0 for p in partitions), \
                        'Committed-offset response contains an invalid offset sentinel'
                    snapshot[name] = {str(p.partition): p.offset for p in partitions}
                finally:
                    client.close()
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if not isinstance(error, KafkaError) or error.code() not in coordinator_codes:
                raise
            previous = None
            if on_retry:
                on_retry(error.code(), attempts)
        else:
            if snapshot == previous:
                return snapshot
            previous = snapshot
        remaining = deadline - monotonic()
        if remaining > 0:
            sleep(min(.25, remaining))
    raise TimeoutError('Committed-offset snapshot did not stabilize within its deadline')


class Harness:
    def __init__(self, args):
        self.args = args
        self.evidence = args.evidence_dir
        self.evidence.mkdir(parents=True, exist_ok=True)
        with (self.evidence / '.acceptance-started').open('x') as marker:
            marker.write(args.run_id + '\n')
        for directory in ('logs', 'metrics', 'markers', 'backup'):
            (self.evidence / directory).mkdir(exist_ok=True)
        (self.evidence / 'errors.jsonl').touch(exist_ok=False)
        self.children = []
        self.child_metrics = {}
        self.child_groups = {}
        self.workers = {}
        self.shutdowns = []
        self.supervisor_restarts = []
        self.events = []
        self.cases = []
        self.logs = []
        self.secrets = json.loads((args.generated_dir / 'secrets.json').read_text())
        self.env = {**os.environ, 'DJANGO_SETTINGS_MODULE': 'config.settings',
                    'LABOPS_DB_MODE': 'postgres', 'LABOPS_EVENT_TRANSPORT': 'kafka',
                    'REDIS_URL': '', 'OTEL_EXPORTER_OTLP_ENDPOINT': '',
                    'WORKER_METRICS_ENABLED': '1'}
        for name, password in self.secrets.items():
            self.env[f'KAFKA_{name.upper()}_SASL_USERNAME'] = name
            self.env[f'KAFKA_{name.upper()}_SASL_PASSWORD'] = password
        os.environ.update(self.env)
        import django
        django.setup()
        from django.conf import settings
        from django.core.management import call_command
        from django.db import connection, connections
        from labops import models, events
        from labops.inventory import services
        from labops.kafka_config import common_config, consumer_config, consumer_group
        self.settings = settings
        self.call_command = call_command
        self.connection, self.connections = connection, connections
        self.models, self.api, self.services = models, events, services
        self.consumer_config, self.consumer_group = consumer_config, consumer_group
        self.configs = {name: common_config(name) for name in self.secrets}
        self.configs['ca_path'] = settings.KAFKA_SSL_CA_LOCATION
        self.configs['broker_log_reader'] = lambda _case, start, end: self.compose(
            'logs', '--no-color', '--since', start, '--until', end,
            'redpanda-0', 'redpanda-1', 'redpanda-2')
        self.started_at = time.time()

    def command(self, argv, *, timeout=60, binary=False, input=None):
        result = subprocess.run(argv, cwd=ROOT, env=self.env, input=input,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=timeout, text=not binary)
        if result.returncode:
            # This helper is used only for non-secret command arguments.
            raise RuntimeError(f'{argv[0]} failed ({result.returncode}): '
                               + (result.stderr.decode(errors='replace') if binary else result.stderr)[-1500:])
        return result.stdout

    def compose(self, *arguments, timeout=90, binary=False):
        return self.command(['docker', 'compose', '-p', self.env['LABOPS_VALIDATION_PROJECT'],
                             '-f', 'infra/events/validation/compose.yaml', *arguments],
                            timeout=timeout, binary=binary)

    def wait(self, check, message, timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                result = check()
                if result:
                    return result
            except Exception as exc:
                last = type(exc).__name__
                self.connections.close_all()
            time.sleep(.15)
        raise AssertionError(f'{message}; last_error_type={last}')

    def spawn(self, name, argv, role, *, metrics_port=None, extra_env=None):
        env = {**self.env, 'KAFKA_SASL_USERNAME': role,
               'KAFKA_SASL_PASSWORD': self.secrets[role],
               'WORKER_METRICS_PORT': str(metrics_port or (21000 + len(self.children)))}
        if extra_env:
            env.update(extra_env)
        log = (self.evidence / 'logs' / f'{name}-{len(self.children)}.log').open('x')
        self.logs.append(log)
        process = subprocess.Popen([sys.executable, *argv], cwd=ROOT, env=env,
                                   stdout=log, stderr=log)
        self.children.append(process)
        self.child_metrics[process.pid] = int(env['WORKER_METRICS_PORT'])
        consumer = None
        if '--consumer' in argv:
            consumer = argv[argv.index('--consumer') + 1]
        elif 'consume_kafka' in argv:
            consumer = argv[argv.index('consume_kafka') + 1]
        self.child_groups[process.pid] = (env['KAFKA_GROUP_PREFIX'] + '.' + consumer + '.v1'
                                        if consumer in {'notification', 'analytics'} else None)
        return process

    def stop_consumer_role(self, name):
        for label in list(self.workers):
            if label == name or label.startswith(name + '-'):
                self.stop(label)
        for child in self.children:
            if self.child_groups.get(child.pid) != self.consumer_group(name) or child.poll() is not None:
                continue
            child.send_signal(signal.SIGTERM)
            escalated = False
            began = time.monotonic()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                escalated = True
                child.kill()
                child.wait(timeout=10)
            self.shutdowns.append({'worker': name + '-untracked-instance', 'pid': child.pid,
                'requested_signal': 'SIGTERM', 'forced_SIGKILL': escalated,
                'exit_code': child.returncode, 'elapsed_seconds': time.monotonic() - began})
        assert not any(child.poll() is None and self.child_groups.get(child.pid) == self.consumer_group(name)
                       for child in self.children), 'An old consumer group instance remains alive'

    def group_assignment(self, name, *, client_id=None):
        from confluent_kafka import ConsumerGroupState
        from confluent_kafka.admin import AdminClient
        group = self.consumer_group(name)
        description = AdminClient(self.configs['admin']).describe_consumer_groups([group])[group].result(timeout=10)
        if description.state != ConsumerGroupState.STABLE or len(description.members) != 1:
            return False
        member = description.members[0]
        parts = member.assignment.topic_partitions
        expected = {(self.settings.KAFKA_TOPIC, partition) for partition in range(3)}
        if len(parts) != 3 or {(part.topic, part.partition) for part in parts} != expected:
            return False
        if client_id and member.client_id != client_id:
            return False
        return {'group': group, 'state': str(description.state), 'member_count': 1,
                'client_id': member.client_id, 'assignments': [
                    {'topic': part.topic, 'partition': part.partition}
                    for part in member.assignment.topic_partitions]}

    def supervisor_after_postgres_restart(self, reason):
        before = {name: {'pid': process.pid, 'exit_code_before_restart': process.poll()}
                  for name, process in self.workers.items()}
        self.stop('publisher')
        self.stop_consumer_role('notification')
        self.stop_consumer_role('analytics')
        self.start_publisher()
        self.start_consumer('notification')
        self.start_consumer('analytics')
        assignments = {name: self.wait(lambda name=name: self.group_assignment(name,
            client_id='acceptance-' + str(self.workers[name].pid)),
            'Supervised consumer did not regain all partitions: ' + name, timeout=90)
            for name in ('notification', 'analytics')}
        evidence = {'reason': reason, 'previous_processes': before,
                    'restarted_processes': {name: {'pid': process.pid} for name, process in self.workers.items()},
                    'stable_assignments_after_restart': assignments,
                    'method': 'explicit validation supervisor creates new process/DB session after PostgreSQL restart'}
        self.supervisor_restarts.append(evidence)
        return evidence

    def stop(self, name, *, kill=False):
        process = self.workers.pop(name, None)
        if process and process.poll() is None:
            started = time.monotonic()
            escalated = False
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                escalated = True
                process.kill()
                process.wait(timeout=10)
            self.shutdowns.append({'worker': name, 'pid': process.pid,
                'requested_signal': 'SIGKILL' if kill else 'SIGTERM',
                'forced_SIGKILL': escalated, 'exit_code': process.returncode,
                'elapsed_seconds': time.monotonic() - started})
        self.sync_metrics_targets()

    def sync_metrics_targets(self):
        """Publish only supervised workers, excluding temporary fault children."""
        path = self.args.generated_dir / 'metrics' / 'targets.json'
        if not path.parent.is_dir():
            return
        targets = [{'targets': [f'127.0.0.1:{self.child_metrics[process.pid]}'],
                    'labels': {'worker': name}}
                   for name, process in sorted(self.workers.items()) if process.poll() is None]
        write_json(path, targets)
        path.chmod(0o644)

    def start_consumer(self, name, suffix=''):
        label = name + suffix
        process = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                             '--observations', str(self.evidence / 'logs' / f'{label}-deliveries.jsonl')], name)
        self.workers[label] = process
        self.sync_metrics_targets()
        return process

    def start_publisher(self):
        self.workers['publisher'] = self.spawn('publisher-' + str(len(self.children)),
            ['manage.py', 'publish_events', '--loop', '--limit', '500'], 'publisher', metrics_port=21000)
        self.sync_metrics_targets()

    def setup(self):
        parsed = urlparse(self.env['DATABASE_URL'])
        if parsed.hostname not in {'localhost', '127.0.0.1'} or parsed.path != '/labops_events':
            raise ValueError('Harness requires disposable loopback /labops_events database')
        if self.env.get('LABOPS_VALIDATION_PROJECT') != 'labops_events_' + self.args.run_id:
            raise ValueError('Compose project/run-id mismatch')
        if self.settings.KAFKA_GROUP_PREFIX != 'labops.' + self.args.run_id:
            raise ValueError('Group prefix/run-id mismatch')
        if self.settings.KAFKA_SECURITY_PROTOCOL != 'SASL_SSL':
            raise ValueError('Secure acceptance requires SASL_SSL')
        if not all(server.split(':')[0] in {'localhost', '127.0.0.1'}
                   for server in self.settings.KAFKA_BOOTSTRAP_SERVERS.split(',')):
            raise ValueError('Harness broker faults are loopback-only')
        self.call_command('migrate', interactive=False, verbosity=0)
        if self.models.User.objects.exists():
            raise ValueError('Database is not fresh; create a new disposable Compose project')
        self.settings.EVENT_TRANSPORT = 'local'
        self.call_command('seed_demo', verbosity=0)
        self.call_command('rebuild_inventory_projection', verbosity=0)
        self.settings.EVENT_TRANSPORT = 'kafka'
        self.admin = self.models.User.objects.get(email='admin@labops.local')
        self.reviewer = self.models.User.objects.get(email='reviewer@labops.local')
        self.task = self.models.Task.objects.filter(status='IN_PROGRESS', project__status='ACTIVE').first()
        self.source = self.models.Warehouse.objects.get(code='WH-01')
        self.target = self.models.Warehouse.objects.get(code='WH-03')
        self.batch = self.models.Batch.objects.filter(item__is_active=True).order_by('created_at').first()
        self.prepare_order()
        self.baseline = self.snapshot()
        from confluent_kafka.admin import AdminClient
        admin = AdminClient(self.configs['admin'])
        metadata = admin.list_topics(timeout=20)
        topic = metadata.topics[self.settings.KAFKA_TOPIC]
        if len(topic.partitions) != 3 or any(len(p.replicas) != 3 for p in topic.partitions.values()):
            raise AssertionError('Actual inventory topic must have 3 partitions and RF3')
        write_json(self.evidence / 'harness-manifest.json', {
            'run_id': self.args.run_id, 'commit': self.command(['git', 'rev-parse', 'HEAD']).strip(),
            'acceptance_tier': self.args.tier, 'generation_window_tolerance_fraction': .05,
            'worktree_dirty': bool(self.command(['git', 'status', '--porcelain']).strip()),
            'python': sys.version, 'host': platform.platform(), 'cpu_count': os.cpu_count(),
            'security_protocol': 'SASL_SSL', 'compose_project': self.env['LABOPS_VALIDATION_PROJECT'],
            'retry_policy': {'seconds': self.settings.EVENT_RETRY_SECONDS,
                'jitter_fraction': self.settings.EVENT_RETRY_JITTER,
                'jitter_range': 'base through base*(1+jitter), deterministic per identity/attempt',
                'maximum_scheduled_delay_seconds': max(self.settings.EVENT_RETRY_SECONDS) * (1 + self.settings.EVENT_RETRY_JITTER),
                'scope': 'explicit disposable validation configuration, frozen before worker startup; production default timing unmeasured'},
            'topic': self.settings.KAFKA_TOPIC,
            'replicas': {str(n): p.replicas for n, p in topic.partitions.items()},
            'client_config_hash': canonical_hash({k: v for k, v in self.env.items()
                if k.startswith(('KAFKA_', 'EVENT_')) and 'PASSWORD' not in k}),
            'fault_scope': 'three broker processes on one Docker host; independent AZ not tested',
            'images': json.loads(self.compose('images', '--format', 'json'))})
        write_json(self.evidence / 'offsets-before.json', self.offsets())

    def prepare_order(self):
        from django.utils import timezone
        from labops.purchasing import services
        maximum = self.args.events + self.args.fault_events * 4 + self.args.fault_repetitions * 20 + 1000
        quantity = maximum * 4
        rid = self.args.run_id[:40] + '-setup'
        request = services.write_request(self.admin, {'reason': 'Synthetic isolated event acceptance',
            'lines': [{'item_id': str(self.batch.item_id), 'qty': quantity,
                       'needed_by': str(timezone.localdate() + timedelta(days=1))}]}, rid)
        request = services.request_action(self.admin, request.id, 'submit',
            {'expected_version': request.version}, rid)
        request = services.request_action(self.reviewer, request.id, 'decision',
            {'expected_version': request.version, 'decision': 'APPROVE', 'reason': 'Isolated acceptance'}, rid)
        order = services.write_order(self.admin, {'supplier_id': str(self.models.Supplier.objects.first().id),
            'lines': [{'request_line_id': str(request.lines.first().id), 'qty': quantity, 'unit_price': '1'}]}, rid)
        self.order = services.order_action(self.admin, order.id, 'confirm',
            {'expected_version': order.version}, rid)
        self.order_line = self.order.lines.first()
        self.cycle_issue = None

    def generate(self, count, label, *, rate=None):
        from django.utils import timezone
        from labops.purchasing.services import create_receipt
        rate = rate or self.args.rate
        started = time.monotonic()
        ids = []
        for index in range(count):
            target = started + index / rate
            while time.monotonic() < target:
                time.sleep(min(.05, target - time.monotonic()))
            seq = len(self.events)
            key = f'{self.args.run_id}:{seq}'
            rid = f'acceptance-{seq}'
            before = time.time()
            from django.db import transaction
            with transaction.atomic():
                with self.connection.cursor() as cursor:
                    cursor.execute('SELECT txid_current()::text')
                    inserted_xid = cursor.fetchone()[0]
                position = seq % 4
                if position == 0 or (position == 3 and self.cycle_issue is None):
                    receipt = create_receipt(self.admin, {'order_id': str(self.order.id), 'lines': [{
                        'order_line_id': str(self.order_line.id), 'warehouse_id': str(self.source.id),
                        'qty': '4', 'batch_no': f'ACCEPTANCE-{self.args.run_id[:24]}-{seq}',
                        'supplier_lot': 'SYNTHETIC', 'expires_on': str(timezone.localdate() + timedelta(days=365))}]}, rid)
                    self.batch = receipt.lines.first().batch
                    movement = self.services.post_receipt(self.admin, receipt.id,
                        {'expected_version': receipt.version, 'receipt_id': str(receipt.id)}, key, rid)
                elif position == 1:
                    movement = self.services.issue(self.admin, {'task_id': str(self.task.id), 'lines': [{
                        'batch_id': str(self.batch.id), 'warehouse_id': str(self.source.id), 'qty': '1'}]}, key, rid)
                    self.cycle_issue = movement
                elif position == 2:
                    movement = self.services.transfer(self.admin, {'batch_id': str(self.batch.id),
                        'from_warehouse_id': str(self.source.id), 'to_warehouse_id': str(self.target.id), 'qty': '1'}, key, rid)
                else:
                    movement = self.services.reverse(self.admin, self.cycle_issue.id,
                        {'reason': 'Synthetic acceptance reversal'}, key, rid)
                    self.cycle_issue = None
            transaction_return = time.time()
            with self.connection.cursor() as cursor:
                cursor.execute('SHOW track_commit_timestamp')
                enabled = cursor.fetchone()[0] == 'on'
                if enabled:
                    cursor.execute('SELECT pg_xact_commit_timestamp(%s::xid)', [inserted_xid])
                    committed_at = cursor.fetchone()[0]
                else:
                    committed_at = None
            event = self.models.OutboxEvent.objects.get(aggregate_id=movement.id)
            item = {'event_id': str(event.id), 'movement_id': str(movement.id), 'kind': movement.type,
                    'scenario': label, 'command_started_at': before,
                    'transaction_return_observed_at': transaction_return,
                    'insert_transaction_xid': inserted_xid,
                    'outbox_transaction_commit_at': committed_at.timestamp() if committed_at else None,
                    'outbox_created_at': event.created_at.timestamp(),
                    'payload_bytes': len(json.dumps(self.api.envelope(event)).encode())}
            ids.append(str(event.id))
            self.events.append(item)
            with (self.evidence / 'events.jsonl').open('a') as out:
                out.write(json.dumps(item, sort_keys=True) + '\n')
            for name, process in self.workers.items():
                if process.poll() is not None:
                    raise AssertionError(f'Worker exited during generation: {name} ({process.returncode})')
        elapsed = time.monotonic() - started
        return ids, {'input': count, 'completed_commands': count, 'elapsed_seconds': elapsed,
                     'target_rate': rate, 'actual_command_rate': count / elapsed if elapsed else None,
                     'schedule_lateness_seconds': max(0, elapsed - count / rate)}

    def offsets(self):
        def retry(code, attempt):
            with (self.evidence / 'errors.jsonl').open('a') as out:
                out.write(json.dumps({'kind': 'offset_coordinator_refresh',
                                      'error_code': code, 'attempt': attempt}) + '\n')
        return stable_committed_offsets({name: self.consumer_config(name)
            for name in ('notification', 'analytics')}, self.settings.KAFKA_TOPIC, on_retry=retry)

    def drained(self, ids, timeout=180):
        expected = len(ids)
        self.wait(lambda: all(self.models.ProcessedEvent.objects.filter(
            consumer_name=name, event_id__in=ids).count() == expected
            for name in ('notification', 'analytics')) and
            self.models.OutboxEvent.objects.filter(id__in=ids, status='PUBLISHED').count() == expected,
            f'{expected} inventory effects did not drain', timeout=timeout)

    def snapshot(self, ids=None):
        from django.db.models import Sum
        ledger = sorted((str(row['batch_id']), str(row['warehouse_id']), str(row['quantity']))
            for row in self.models.StockMovementLine.objects.filter(movement__status='POSTED')
            .values('batch_id', 'warehouse_id').annotate(quantity=Sum('delta_qty')))
        balance = sorted((str(b), str(w), str(q)) for b, w, q in self.models.StockBalance.objects
                         .values_list('batch_id', 'warehouse_id', 'on_hand_qty'))
        projection = sorted((str(b), str(w), str(q)) for b, w, q in self.models.InventoryProjection.objects
                            .values_list('batch_id', 'warehouse_id', 'quantity'))
        notifications = self.models.Notification.objects.all()
        processed = self.models.ProcessedEvent.objects.all()
        if ids is not None:
            notifications = notifications.filter(event_id__in=ids)
            processed = processed.filter(event_id__in=ids)
        notification_rows = sorted((str(e), str(u)) for e, u in notifications.values_list('event_id', 'user_id'))
        dedupe_rows = sorted((str(c), str(e)) for c, e in processed.values_list('consumer_name', 'event_id'))
        active_users = set(str(x) for x in self.models.User.objects.filter(is_active=True).values_list('id', flat=True))
        expected_notifications = None
        if ids is not None:
            expected_notifications = sum(len(set(row.payload_json['recipients']) & active_users)
                for row in self.models.OutboxEvent.objects.filter(id__in=ids))
        def quantities(rows):
            return {(b, w): Decimal(q) for b, w, q in rows}
        ledger_qty, balance_qty, projection_qty = map(quantities, (ledger, balance, projection))
        all_keys = set(ledger_qty) | set(balance_qty) | set(projection_qty)
        mismatches = [{'batch_id': b, 'warehouse_id': w,
            'ledger': str(ledger_qty.get((b, w), 0)),
            'balance': str(balance_qty.get((b, w), 0)),
            'projection': str(projection_qty.get((b, w), 0))}
            for b, w in sorted(all_keys) if len({mapping.get((b, w), Decimal(0))
                                                for mapping in (ledger_qty, balance_qty, projection_qty)}) != 1]
        return {'ledger_rows': len(ledger), 'ledger_hash': canonical_hash(ledger),
                'balance_rows': len(balance), 'balance_hash': canonical_hash(balance),
                'projection_rows': len(projection), 'projection_hash': canonical_hash(projection),
                'mismatches': mismatches, 'notification_count': len(notification_rows),
                'notification_hash': canonical_hash(notification_rows),
                'expected_notification_count': expected_notifications,
                'dedupe_count': len(dedupe_rows), 'dedupe_hash': canonical_hash(dedupe_rows),
                'consumer_counts': dict(Counter(c for c, _ in dedupe_rows)),
                'failed_deliveries': dict(Counter(self.models.FailedDelivery.objects.values_list('status', flat=True)))}

    def duplicate_drill(self, ids):
        before = self.snapshot(ids)
        sender = self.api.producer()
        sent = 0
        started = time.monotonic()
        for event in self.models.OutboxEvent.objects.filter(id__in=ids):
            for _ in range(2):
                self.api.send(sender, self.settings.KAFKA_TOPIC,
                              f'{event.aggregate_type}:{event.aggregate_id}', self.api.envelope(event))
                sent += 1
        self.wait_for_log_deliveries(ids, minimum=(sent + len(ids)) * 2, timeout=180)
        after = self.snapshot(ids)
        for key in ('notification_count', 'notification_hash', 'dedupe_count', 'dedupe_hash', 'projection_hash'):
            assert before[key] == after[key], f'Duplicate changed {key}'
        self.cases.append({'name': 'duplicates', 'unique_input': len(ids), 'duplicate_broker_records': sent,
            'extra_database_effects': 0, 'before': before, 'after': after,
            'elapsed_seconds': time.monotonic() - started, 'passed': True})

    def wait_for_log_deliveries(self, ids, minimum, timeout=90):
        wanted = set(ids)
        def count():
            count = 0
            for path in (self.evidence / 'logs').glob('*-deliveries.jsonl'):
                for line in path.read_text().splitlines():
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    if row.get('event_id') in wanted:
                        count += 1
            return count >= minimum
        self.wait(count, 'Expected broker redeliveries were not observed', timeout)

    def marker(self, label):
        return self.evidence / 'markers' / f'{label}.json'

    def wait_marker(self, marker, child, timeout=90):
        def ready():
            if marker.exists():
                return json.loads(marker.read_text())
            if child.poll() is not None:
                raise RuntimeError(f'Fault process exited early ({child.returncode})')
            return False
        return self.wait(ready, 'Fault boundary not reached: ' + marker.name, timeout)

    def consumer_crashes(self):
        for name in ('notification', 'analytics'):
            for stage in ('before_commit', 'after_commit'):
                for repetition in range(self.args.fault_repetitions):
                    self.stop_consumer_role(name)
                    ids, _ = self.generate(1, name + '-' + stage)
                    eid = ids[0]
                    label = f'{name}-{stage}-{repetition}'
                    marker = self.marker(label)
                    child = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                        '--stage', stage, '--event', eid, '--marker', str(marker)], name)
                    paused = self.wait_marker(marker, child)
                    topic, partition, offset = paused['delivery_key'].rsplit(':', 3)[-3:]
                    before_offset = self.offsets()[name][partition]
                    assert before_offset <= int(offset), 'Consumer committed before crash boundary'
                    effect_count = self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).count()
                    assert effect_count == (1 if stage == 'after_commit' else 0)
                    child.kill()
                    assert child.wait(timeout=10) == -signal.SIGKILL
                    recovered = self.marker(label + '-recover')
                    child = self.spawn(label + '-recover', [str(HERE / 'workers.py'), 'consumer',
                        '--consumer', name, '--event', eid, '--marker', str(recovered), '--max-messages', '1'], name)
                    replay = self.wait_marker(Path(str(recovered) + '.completed'), child, timeout=90)
                    assert replay['delivery_key'] == paused['delivery_key'], 'Same broker offset was not replayed'
                    self.wait(lambda: self.offsets()[name][partition] == int(offset) + 1,
                              'Recovery did not commit offset')
                    child.send_signal(signal.SIGTERM)
                    child.wait(timeout=30)
                    assert self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).count() == 1
                    self.start_consumer(name)
                    self.drained(ids)
                    self.cases.append({'name': 'consumer_sigkill', 'consumer': name, 'stage': stage,
                        'repetition': repetition, 'event_id': eid, 'source_replayed': paused['delivery_key'],
                        'offset_before_kill': before_offset, 'offset_after_recovery': int(offset) + 1,
                        'effects_visible_before_kill': effect_count, 'effects_after_recovery': 1, 'passed': True})

    def publisher_crashes(self):
        self.stop('publisher')
        for stage in ('before_send', 'after_ack', 'stale_owner'):
            for repetition in range(self.args.fault_repetitions):
                ids, _ = self.generate(1, 'publisher-' + stage)
                eid = ids[0]
                label = f'publisher-{stage}-{repetition}'
                marker = self.marker(label)
                child = self.spawn(label, [str(HERE / 'workers.py'), 'publisher', '--stage', stage,
                    '--event', eid, '--marker', str(marker)], 'publisher')
                paused = self.wait_marker(marker, child)
                if stage != 'stale_owner':
                    child.kill()
                    assert child.wait(timeout=10) == -signal.SIGKILL
                from django.utils import timezone
                original_claim = self.models.OutboxEvent.objects.get(id=eid)
                fixture_expiry = timezone.now() - timedelta(seconds=1)
                self.models.OutboxEvent.objects.filter(id=eid).update(locked_until=fixture_expiry)
                recovered_marker = self.marker(label + '-recover')
                recovered = self.spawn(label + '-recover', [str(HERE / 'workers.py'), 'publisher',
                    '--event', eid, '--marker', str(recovered_marker)], 'publisher')
                self.wait_marker(Path(str(recovered_marker) + '.completed'), recovered)
                assert recovered.wait(timeout=30) == 0
                row = self.models.OutboxEvent.objects.get(id=eid)
                assert row.status == 'PUBLISHED'
                if stage == 'stale_owner':
                    published_at = row.published_at
                    Path(str(marker) + '.release').touch()
                    self.wait_marker(Path(str(marker) + '.completed'), child)
                    assert child.wait(timeout=30) == 0
                    row.refresh_from_db()
                    assert row.published_at == published_at, 'Expired owner changed publication writeback'
                self.drained(ids)
                self.cases.append({'name': 'publisher_crash', 'stage': stage, 'repetition': repetition,
                    'event_id': eid, 'acknowledged_before_kill': paused.get('acknowledged_records', []),
                    'same_event_id_recovered': True, 'expired_owner_writeback_successes': 0 if stage == 'stale_owner' else None,
                    'lease_expiry_method': 'accelerated disposable fixture timestamp',
                    'original_lease_expiry': original_claim.locked_until,
                    'fixture_lease_expiry': fixture_expiry,
                    'original_lease_token': original_claim.lease_token,
                    'natural_lease_wait_verified': False,
                    'passed': True})
        self.start_publisher()

    def broker_fault(self, services, seconds, label):
        before = self.offsets()
        network_before = self.network_snapshot(label + '-before')
        self.compose('stop', '-t', '0', *services)
        started = time.monotonic()
        try:
            ids, workload = self.generate(self.args.fault_events, label)
            if len(services) >= 2:
                # Force a completed publish budget while quorum is unavailable.
                sender = self.api.producer()
                failed = False
                attempted = {'acknowledgements': [], 'delivery_errors': []}
                original_send = self.api.send

                class ObservingProducer:
                    def __getattr__(self, name):
                        return getattr(sender, name)

                    @property
                    def last_security_error(self):
                        return sender.last_security_error

                    @last_security_error.setter
                    def last_security_error(self, value):
                        sender.last_security_error = value

                    def produce(self, *a, **kw):
                        callback = kw.get('on_delivery')

                        def observed(error, message):
                            if error is None:
                                attempted['acknowledgements'].append({'topic': message.topic(),
                                    'partition': message.partition(), 'offset': message.offset()})
                            else:
                                attempted['delivery_errors'].append(error.name())
                            if callback:
                                callback(error, message)
                        kw['on_delivery'] = observed
                        return sender.produce(*a, **kw)

                def observed_send(client, topic, key, value):
                    attempted['event_id'] = value['event_id']
                    return original_send(client, topic, key, value)
                self.api.send = observed_send
                probe_began = time.monotonic()
                claim_budget = (self.settings.EVENT_LEASE_SECONDS +
                    max(self.settings.EVENT_RETRY_SECONDS[:1] or [0]) +
                    self.settings.KAFKA_PUBLISH_FLUSH_SECONDS + 15)
                no_candidate_polls = 0
                try:
                    while time.monotonic() - probe_began < claim_budget:
                        try:
                            self.api.publish_one(ObservingProducer())
                        except Exception as exc:
                            # A claim/pre-send error cannot establish a broker
                            # denial. Only the captured actual send is eligible.
                            if not attempted.get('event_id'):
                                raise
                            failed = True
                            publish_error = type(exc).__name__
                            break
                        if attempted.get('event_id'):
                            break
                        no_candidate_polls += 1
                        time.sleep(.15)
                finally:
                    self.api.send = original_send
                attempted['claim_probe_seconds'] = time.monotonic() - probe_began
                attempted['no_candidate_polls'] = no_candidate_polls
                write_json(self.evidence / (label + '-publication-probe.json'), attempted)
                assert attempted.get('event_id'), 'No naturally eligible outbox was attempted within the bounded quorum probe'
                assert failed, 'Broker falsely acknowledged publication without quorum'
                assert not attempted['acknowledgements'], 'Broker record acknowledged without quorum'
                assert attempted.get('event_id') in ids, 'Quorum failure attempted a different workload event'
                candidate = self.models.OutboxEvent.objects.get(id=attempted['event_id'])
                candidate.refresh_from_db()
                assert candidate.status != 'PUBLISHED', 'Unacknowledged outbox marked published'
                assert self.models.StockMovement.objects.filter(id=candidate.aggregate_id, status='POSTED').exists()
            else:
                publish_error = None
                attempted = {}
            remaining = seconds - (time.monotonic() - started)
            while remaining > 0:
                time.sleep(min(.25, remaining))
                remaining = seconds - (time.monotonic() - started)
            down_snapshot = self.snapshot(ids)
            down_rows = self.outbox_retry_evidence(ids)
            write_json(self.evidence / (label + '-outbox-during-outage.json'), down_rows)
            downtime = time.monotonic() - started
        finally:
            self.compose('start', *services)
        # The frozen drain window begins when stopped processes return, so
        # network/cluster recovery cannot quietly extend the 900-second SLA.
        recovery = time.monotonic()
        completed_seconds = None
        try:
            network_after = self.network_snapshot(label + '-after')
            from network_identity import compare_broker_networks
            network_proof = compare_broker_networks(network_before, network_after,
                self.env['LABOPS_VALIDATION_IPV4_PREFIX'])
            self.wait_brokers()
            health_recovery_seconds = time.monotonic() - recovery
            self.connections.close_all()
            if 'publisher' not in self.workers:
                self.start_publisher()
            remaining = self.args.drain_timeout - (time.monotonic() - recovery)
            assert remaining > 0, 'Broker health recovery exhausted the frozen drain window'
            self.drained(ids, timeout=remaining)
            from benchmarks.events.health import completed_recovery_seconds
            completed_seconds = completed_recovery_seconds(recovery, self.args.drain_timeout)
        finally:
            recovered_rows = self.outbox_retry_evidence(ids)
            write_json(self.evidence / (label + '-outbox-after-recovery.json'), recovered_rows)
            write_json(self.evidence / (label + '-recovery-window.json'), {
                'budget_seconds': self.args.drain_timeout,
                'observation_elapsed_seconds': time.monotonic() - recovery,
                'window_start': 'broker compose start completed',
                'successful_completion_elapsed_seconds': completed_seconds,
                'successful_completion_within_budget': completed_seconds is not None})
            dead = [row['id'] for row in recovered_rows if row['status'] == 'DEAD']
            if dead:
                with (self.evidence / 'errors.jsonl').open('a') as out:
                    out.write(json.dumps({'kind': 'automatic_broker_recovery_exhausted',
                        'scenario': label, 'dead_event_ids': dead, 'operator_requeued_events': 0}) + '\n')
        self.cases.append({'name': label, 'services': services, 'downtime_seconds': downtime,
            'input': len(ids), 'workload': workload, 'publish_denial_error_type': publish_error,
            'attempted_publish_event_id': attempted.get('event_id'),
            'attempted_publish_ack_count': len(attempted.get('acknowledgements', [])) if len(services) >= 2 else None,
            'attempted_publish_delivery_errors': attempted.get('delivery_errors', []),
            'claim_probe_seconds': attempted.get('claim_probe_seconds'),
            'no_candidate_polls': attempted.get('no_candidate_polls'),
            'database_transactions_committed': len(ids), 'down_snapshot': down_snapshot,
            'recovery_method': 'automatic persisted retry schedules and natural lease expiry',
            'operator_requeued_events': 0, 'publishers_killed_by_broker_drill': 0,
            'stable_broker_network': network_proof,
            'broker_health_recovery_seconds': health_recovery_seconds,
            'recovery_window_seconds': self.args.drain_timeout,
            'recovery_window_start': 'broker compose start completed; includes network/cluster health recovery',
            'recovery_drain_seconds': completed_seconds, 'offsets_before': before,
            'offsets_after': self.offsets(), 'passed': True})

    def network_snapshot(self, stage):
        self.command([sys.executable, 'infra/events/validation/collect.py',
            '--env-file', str(self.args.generated_dir / 'client.env'),
            '--run-id', self.args.run_id, '--evidence-dir', str(self.evidence),
            '--network-snapshot-only', '--network-stage', stage])
        return json.loads((self.evidence / ('network-identity-' + stage + '.json')).read_text())

    def outbox_retry_evidence(self, ids):
        return [{**{key: value for key, value in row.items() if key != 'lease_token'},
                 'id': str(row['id']), 'lease_token_present': row['lease_token'] is not None}
                for row in self.models.OutboxEvent.objects.filter(id__in=ids).order_by('created_at', 'id').values(
                    'id', 'status', 'attempts', 'next_attempt_at', 'locked_until', 'lease_token', 'published_at')]

    def wait_brokers(self):
        from confluent_kafka.admin import AdminClient
        import requests
        from benchmarks.events.health import wait_broker_recovery
        from labops.worker_metrics import operation_deadline, OperationDeadlineExceeded
        # A metadata broker count can advertise three nodes while Raft remains
        # leaderless or a surviving node contacts another node's stale address.
        # Retain every local health view and the actual topic leaders/ISR.
        index = len(list(self.evidence.glob('broker-recovery-*-result.json')))
        prefix = self.evidence / f'broker-recovery-{index:02d}'
        last = {}
        session = requests.Session()
        session.auth = (self.configs['admin']['sasl.username'], self.configs['admin']['sasl.password'])
        session.verify = self.configs['ca_path']
        session.trust_env = False

        def observed(snapshot):
            last.clear()
            last.update(snapshot)
            with prefix.with_name(prefix.name + '-observations.jsonl').open('a') as out:
                out.write(json.dumps(snapshot, sort_keys=True) + '\n')

        def health(node, remaining):
            try:
                with operation_deadline(remaining):
                    response = session.get(f'https://127.0.0.1:{19644 + node * 10000}/v1/cluster/health_overview',
                        timeout=min(4, remaining / 2), allow_redirects=False)
                    if response.status_code != 200:
                        raise RuntimeError('Broker health HTTP status is not 200')
                    return response.json()
            except OperationDeadlineExceeded:
                raise TimeoutError('Broker health request exhausted recovery budget') from None

        def metadata(remaining):
            try:
                with operation_deadline(remaining):
                    value = AdminClient(self.configs['admin']).list_topics(timeout=min(4, remaining))
                    topics = {}
                    for name in (self.settings.KAFKA_TOPIC, self.settings.KAFKA_DLQ_TOPIC):
                        topic = value.topics.get(name)
                        if topic is None:
                            continue
                        topics[name] = {'error': topic.error.name() if topic.error else None,
                            'partitions': [{'partition': number, 'leader': part.leader,
                                'replicas': list(part.replicas), 'isr': list(part.isrs),
                                'error': part.error.name() if part.error else None}
                                for number, part in sorted(topic.partitions.items())]}
                    return {'brokers': sorted(value.brokers), 'topics': topics}
            except OperationDeadlineExceeded:
                raise TimeoutError('Kafka metadata request exhausted recovery budget') from None

        try:
            result = wait_broker_recovery(health, metadata,
                (self.settings.KAFKA_TOPIC, self.settings.KAFKA_DLQ_TOPIC),
                timeout=180, on_observation=observed)
        except Exception as exc:
            write_json(prefix.with_name(prefix.name + '-result.json'),
                {'status': 'FAILED', 'timeout_seconds': 180, 'error_type': type(exc).__name__,
                 'last_observation': last})
            raise
        else:
            write_json(prefix.with_name(prefix.name + '-result.json'),
                {'status': 'READY', 'timeout_seconds': 180, 'last_observation': result})
        finally:
            session.close()

    def analytics_outage(self):
        self.stop('analytics')
        started = time.monotonic()
        ids, workload = self.generate(self.args.fault_events, 'analytics_outage')
        self.wait(lambda: self.models.ProcessedEvent.objects.filter(consumer_name='notification',
            event_id__in=ids).count() == len(ids), 'Independent notification consumer stalled')
        parked = self.models.ProcessedEvent.objects.filter(consumer_name='analytics', event_id__in=ids).count()
        assert parked == 0
        while time.monotonic() - started < self.args.consumer_outage_seconds:
            time.sleep(.2)
        self.start_consumer('analytics')
        recovered = time.monotonic()
        self.drained(ids, timeout=self.args.drain_timeout)
        self.cases.append({'name': 'analytics_outage', 'input': len(ids), 'workload': workload,
            'notification_completed_while_analytics_down': len(ids), 'analytics_effects_while_down': 0,
            'catch_up_seconds': time.monotonic() - recovered, 'passed': True})

    def postgres_failure(self):
        self.stop('publisher')
        self.stop('notification')
        self.stop('analytics')
        before = self.snapshot()
        movement_count = self.models.StockMovement.objects.count()
        marker = self.marker('postgres-business')
        data = self.evidence / 'markers' / 'postgres-business-data.json'
        write_json(data, {'user_id': str(self.admin.id), 'key': self.args.run_id + ':pg-rollback',
            'request_id': 'pg-rollback', 'command': {'task_id': str(self.task.id), 'lines': [{
                'batch_id': str(self.batch.id), 'warehouse_id': str(self.source.id), 'qty': '0.1'}]}})
        child = self.spawn('postgres-business', [str(HERE / 'workers.py'), 'business',
            '--data-file', str(data), '--marker', str(marker)], 'publisher')
        paused = self.wait_marker(marker, child)
        assert self.models.StockMovement.objects.count() == movement_count, 'Uncommitted ledger is visible'
        self.compose('stop', '-t', '0', 'postgres')
        try:
            Path(str(marker) + '.release').touch()
            error = self.wait_marker(Path(str(marker) + '.error'), child, timeout=30)
            child.wait(timeout=20)
        finally:
            self.compose('start', 'postgres')
        self.connections.close_all()
        self.wait(lambda: self.models.User.objects.count() > 0, 'PostgreSQL did not recover')
        assert self.models.StockMovement.objects.count() == movement_count
        assert not self.models.StockMovement.objects.filter(id=paused['movement_id']).exists()
        assert not self.models.OutboxEvent.objects.filter(id=paused['event_id']).exists()
        after = self.snapshot()
        for key in ('ledger_hash', 'balance_hash', 'projection_hash', 'dedupe_hash', 'notification_hash'):
            assert before[key] == after[key], 'Business transaction rollback changed ' + key
        self.cases.append({'name': 'postgres_business_commit', 'attempted_commands': 1,
            'committed_commands': 0, 'partial_ledger_or_outbox': 0, 'error_type': error['error_type'], 'passed': True})
        business_supervision = self.supervisor_after_postgres_restart('business transaction fault')
        self.cases[-1]['supervisor_restart'] = business_supervision
        for name in ('notification', 'analytics'):
            for boundary in ('effect_commit', 'failed_delivery_persist'):
                self.stop('publisher')
                self.stop_consumer_role('notification')
                self.stop_consumer_role('analytics')
                label = 'postgres_' + boundary + '_' + name
                marker = self.marker(label)
                target_file = self.evidence / 'markers' / (label + '.target.json')
                stage = 'postgres_effect_commit' if boundary == 'effect_commit' else 'before_delivery'
                child = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                    '--stage', stage, '--event-file', str(target_file), '--marker', str(marker),
                    '--observations', str(self.evidence / 'logs' / (label + '-deliveries.jsonl'))], name)
                assignment = self.wait(lambda: self.group_assignment(name, client_id='acceptance-' + str(child.pid)),
                    'Fault process did not exclusively own all group partitions', timeout=90)
                if boundary == 'effect_commit':
                    ids, _ = self.generate(1, label)
                    eid = ids[0]
                    write_json(target_file, {'event_id': eid})
                    self.start_publisher()
                else:
                    ids = []
                    eid = str(uuid.uuid4())
                    write_json(target_file, {'event_id': eid})
                    poison = {'event_id': eid, 'schema_version': 999, 'payload': {}}
                    self.api.send(self.api.producer(), self.settings.KAFKA_TOPIC, label, poison)
                paused = self.wait_marker(marker, child)
                _, partition, offset = paused['delivery_key'].rsplit(':', 3)[-3:]
                before_offset = self.offsets()[name][partition]
                published = None
                if ids:
                    self.wait(lambda: self.models.OutboxEvent.objects.filter(id=eid, status='PUBLISHED').exists(),
                              'Target outbox did not reach actual broker acknowledgement', timeout=30)
                    published = self.models.OutboxEvent.objects.get(id=eid)
                self.compose('stop', '-t', '0', 'postgres')
                try:
                    Path(str(marker) + '.release').touch()
                    error = self.wait_marker(Path(str(marker) + '.error'), child, timeout=30)
                    after_offset = self.offsets()[name][partition]
                    assert after_offset == before_offset and after_offset <= int(offset), 'Offset advanced without durable outcome'
                    if child.poll() is None:
                        child.kill()
                    child.wait(timeout=10)
                finally:
                    self.compose('start', 'postgres')
                self.connections.close_all()
                self.wait(lambda: self.models.User.objects.count() > 0, 'PostgreSQL did not recover')
                assert not self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).exists()
                assert not self.models.FailedDelivery.objects.filter(consumer_name=name, delivery_key=paused['delivery_key']).exists()
                supervision = self.supervisor_after_postgres_restart(label)
                if ids:
                    self.drained(ids)
                else:
                    self.wait(lambda: self.models.FailedDelivery.objects.filter(consumer_name=name,
                        delivery_key=paused['delivery_key']).exists(), 'Failure not persisted on redelivery')
                self.cases.append({'name': label, 'source': paused['delivery_key'],
                    'offset_before': before_offset, 'offset_while_database_down': after_offset,
                    'failure_type': error['error_type'], 'partial_effect_after_restart': 0,
                    'durable_outcome_after_recovery': True, 'exclusive_fault_assignment_before_input': assignment,
                    'target_event_id': eid, 'outbox_status_before_database_stop': published.status if published else None,
                    'supervisor_restart': supervision, 'passed': True})

    def poison_drill(self):
        sender = self.api.producer()
        before = self.models.FailedDelivery.objects.count()
        candidates = []
        # Include raw invalid JSON, unknown schema/type, decimal overflow/nonfinite,
        # malformed IDs, missing fields, oversized application payload below broker max.
        template = self.api.envelope(self.models.OutboxEvent.objects.filter(transport='kafka').first())
        for index in range(self.args.poison_events):
            candidate = json.loads(json.dumps(template))
            candidate['event_id'] = str(uuid.uuid4())
            case = index % 12
            if case == 0: value = b'{invalid json'
            elif case == 1:
                candidate['schema_version'] = 999; value = json.dumps(candidate).encode()
            elif case == 2:
                candidate['event_type'] = 'unknown'; value = json.dumps(candidate).encode()
            elif case == 3:
                candidate['payload']['lines'][0]['delta_qty'] = 'NaN'; value = json.dumps(candidate).encode()
            elif case == 4:
                candidate['payload']['lines'][0]['delta_qty'] = '0.0000001'; value = json.dumps(candidate).encode()
            elif case == 5:
                candidate['aggregate_id'] = 'not-uuid'; value = json.dumps(candidate).encode()
            elif case == 6:
                candidate.pop('payload'); value = json.dumps(candidate).encode()
            elif case == 7:
                candidate['occurred_at'] = 'not-time'; value = json.dumps(candidate).encode()
            elif case == 8:
                candidate['payload']['body'] = 'X' * (self.settings.EVENT_MAX_PAYLOAD_BYTES + 100); value = json.dumps(candidate).encode()
            elif case == 9:
                candidate['aggregate_version'] = True; value = json.dumps(candidate).encode()
            elif case == 10:
                value = b'X' * 1000000
            else:
                candidate['payload']['body'] = 'valid JSON containing a NUL: \x00'; value = json.dumps(candidate).encode()
            outcomes = []
            sender.produce(self.settings.KAFKA_TOPIC, key=f'poison:{index}', value=value,
                           on_delivery=lambda error, msg: outcomes.append(error))
            assert sender.flush(15) == 0 and outcomes == [None], 'Poison not acknowledged by broker'
            candidates.append({'index': index, 'case': case, 'bytes': len(value),
                               'raw_sha256': hashlib.sha256(value).hexdigest()})
        self.wait(lambda: self.models.FailedDelivery.objects.count() >= before + len(candidates) * 2,
                  'Poison messages were not isolated for both consumers', timeout=180)
        ids, _ = self.generate(4, 'healthy_after_poison')
        self.drained(ids)
        failures = self.models.FailedDelivery.objects.order_by('-created_at')[:len(candidates) * 2]
        persisted = len(failures)
        self.cases.append({'name': 'poison', 'input': len(candidates), 'consumer_input_denominator': len(candidates) * 2,
            'persisted_failed_deliveries': persisted, 'healthy_after_poison': len(ids), 'variants': candidates,
            'status_counts': dict(Counter(x.status for x in failures)), 'passed': True})
        pending_dlq = self.models.FailedDelivery.objects.filter(status='DEAD', dlq_published_at__isnull=True).count()
        child = self.spawn('dlq-independent', ['manage.py', 'publish_dlq', '--limit', str(pending_dlq + 1)], 'dlq')
        assert child.wait(timeout=max(180, pending_dlq * 2)) == 0, 'Independent DLQ worker failed'
        assert not self.models.FailedDelivery.objects.filter(status='DEAD', dlq_published_at__isnull=True).exists()
        from confluent_kafka import Consumer
        # Inspect the DLQ mirror using the disposable infrastructure administrator;
        # the replay service remains restricted to its declared inventory topic.
        config = {**self.configs['admin'], 'group.id': self.settings.KAFKA_GROUP_PREFIX + '.replay.acceptance.v1',
                  'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
                  'enable.auto.offset.store': False}
        reader = Consumer(config)
        expected = {str(row.id): row for row in self.models.FailedDelivery.objects.filter(status='DEAD')}
        seen = set()
        deadline = time.monotonic() + 180
        try:
            reader.subscribe([self.settings.KAFKA_DLQ_TOPIC])
            while time.monotonic() < deadline and set(expected) != seen:
                message = reader.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise AssertionError('DLQ read failed: ' + message.error().name())
                value = json.loads(message.value())
                delivery = expected[value['delivery_id']]
                assert value['original_hash'] == delivery.original_hash
                assert self.api.canonical_payload_hash(value['event']) == delivery.original_hash
                seen.add(value['delivery_id'])
            assert set(expected) == seen, 'Stable DLQ records missing'
        finally:
            reader.close()
        self.cases.append({'name': 'independent_dlq', 'persisted_dead_denominator': pending_dlq,
                           'acknowledged_dlq_mirrors': pending_dlq,
                           'read_stable_delivery_ids': len(seen), 'payload_hashes_verified': len(seen), 'passed': True})

    def retry_drill(self):
        from django.utils import timezone
        for name in ('notification', 'analytics'):
            self.stop(name)
            ids, _ = self.generate(1, 'transient_retry_' + name)
            eid = ids[0]
            label = 'transient-' + name
            marker = self.marker(label)
            child = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                '--stage', 'transient_failure', '--event', eid, '--marker', str(marker),
                '--max-messages', '1'], name)
            self.wait_marker(Path(str(marker) + '.completed'), child)
            assert child.wait(timeout=30) == 0
            row = self.models.FailedDelivery.objects.get(consumer_name=name, envelope__event_id=eid)
            assert row.status == 'RETRY'
            original_hash, original_envelope = row.original_hash, row.envelope
            assert not self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).exists()
            original_due = row.next_attempt_at
            fixture_due = timezone.now()
            self.models.FailedDelivery.objects.filter(id=row.id).update(next_attempt_at=fixture_due)
            self.start_consumer(name)
            healthy, _ = self.generate(4, 'healthy_while_retry_' + name)
            self.drained(healthy)
            retry = self.spawn('retry-independent-' + name, ['manage.py', 'retry_events', '--limit', '100'], 'replay')
            assert retry.wait(timeout=60) == 0
            row.refresh_from_db()
            assert row.status == 'RESOLVED' and row.original_hash == original_hash and row.envelope == original_envelope
            self.drained(ids)
            audit = self.models.DeliveryAudit.objects.filter(delivery_id=row.id).values_list('action', flat=True)
            assert 'PARK' in audit and 'RETRY' in audit
            self.cases.append({'name': 'durable_retry', 'consumer': name, 'event_id': eid,
                'delivery_id': str(row.id), 'original_hash': original_hash, 'healthy_while_parked': len(healthy),
                'retry_due_method': 'accelerated disposable fixture timestamp',
                'original_next_attempt_at': original_due, 'fixture_next_attempt_at': fixture_due,
                'natural_retry_wait_verified': False,
                'audit_actions': list(audit), 'resolved_same_event_id': True, 'passed': True})

    def rebalance(self):
        from confluent_kafka import ConsumerGroupState
        from confluent_kafka.admin import AdminClient
        admin = AdminClient(self.configs['admin'])

        def membership(expected):
            result = {}
            for name in ('notification', 'analytics'):
                group = self.consumer_group(name)
                description = admin.describe_consumer_groups([group])[group].result(timeout=10)
                assignments = [[{'topic': part.topic, 'partition': part.partition}
                                for part in member.assignment.topic_partitions] for member in description.members]
                coordinates = [(part['topic'], part['partition']) for member in assignments for part in member]
                if (description.state != ConsumerGroupState.STABLE or len(description.members) != expected or
                        len(coordinates) != 3 or set(coordinates) !=
                        {(self.settings.KAFKA_TOPIC, partition) for partition in range(3)}):
                    return False
                result[name] = {'group': group, 'state': str(description.state),
                                'member_count': len(description.members), 'assignments': assignments}
            return result
        for repetition in range(self.args.fault_repetitions):
            before = self.wait(lambda: membership(1), 'Groups did not stabilise at one member', timeout=120)
            extras = []
            for name in ('notification', 'analytics'):
                for n in (2, 3):
                    label = name + f'-scale-{n}-{repetition}'
                    self.start_consumer(name, label[len(name):])
                    extras.append(label)
            scaled = self.wait(lambda: membership(3), 'Groups did not actually assign three members', timeout=120)
            ids, _ = self.generate(8, 'rebalance_1_3_1')
            for n, label in enumerate(extras):
                self.stop(label, kill=bool(n % 2))
            restored = self.wait(lambda: membership(1), 'Groups did not recover one member', timeout=120)
            self.drained(ids)
            self.cases.append({'name': 'rebalance_1_3_1', 'repetition': repetition,
                'input': len(ids), 'membership_before': before, 'membership_scaled': scaled,
                'membership_after': restored, 'shutdowns': self.shutdowns[-4:], 'passed': True})

    def restore(self):
        self.stop('publisher')
        self.stop('notification')
        self.stop('analytics')
        baseline = self.snapshot([item['event_id'] for item in self.events])
        watermark = {'created_events': len(self.events), 'last_event_id': self.events[-1]['event_id'],
                     'snapshot_at': time.time(), 'offsets': self.offsets()}
        name = 'restore_' + re.sub('[^a-z0-9_]', '_', self.args.run_id.lower())[:40]
        dump = self.compose('exec', '-T', 'postgres', 'pg_dump', '-U', 'labops', '-Fc',
                            '-d', 'labops_events', binary=True, timeout=180)
        backup = self.evidence / 'backup' / 'postgres.dump'
        backup.write_bytes(dump)
        backup.chmod(0o600)
        import psycopg
        from psycopg import sql
        parsed = urlparse(self.env['DATABASE_URL'])
        admin_url = urlunparse(parsed._replace(path='/postgres'))
        restored_url = urlunparse(parsed._replace(path='/' + name))
        tables = ('stockmovement', 'stockmovementline', 'stockbalance', 'inventoryprojection',
                  'outboxevent', 'processedevent', 'notification', 'faileddelivery', 'deliveryaudit')

        def fingerprints(url):
            result = {}
            with psycopg.connect(url) as database:
                for table in tables:
                    rows = database.execute(sql.SQL('SELECT row_to_json(r) FROM {} r ORDER BY id').format(
                        sql.Identifier('labops_' + table))).fetchall()
                    result[table] = {'count': len(rows), 'sha256': canonical_hash([row[0] for row in rows])}
            return result
        original_fingerprints = fingerprints(self.env['DATABASE_URL'])
        recovery_started = time.monotonic()
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        # Restore inside the fresh DB; the active business DB is never replaced.
        argv = ['docker', 'compose', '-p', self.env['LABOPS_VALIDATION_PROJECT'], '-f',
            'infra/events/validation/compose.yaml', 'exec', '-T', 'postgres',
            'pg_restore', '-U', 'labops', '-d', name, '--exit-on-error']
        self.command(argv, timeout=180, binary=True, input=dump)
        restored_fingerprints = fingerprints(restored_url)
        assert original_fingerprints == restored_fingerprints, 'Restored business/event/audit table hash mismatch'
        with psycopg.connect(restored_url) as restored:
            with restored.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM labops_outboxevent WHERE transport = %s', ('kafka',))
                restored_count = cursor.fetchone()[0]
                assert restored_count == len(self.events)
                cursor.execute('SELECT COUNT(*) FROM labops_processedevent WHERE consumer_name IN (%s,%s)',
                               ('notification', 'analytics'))
                restored_dedupe = cursor.fetchone()[0]
                cursor.execute('DELETE FROM labops_inventoryprojection')
        process = self.spawn('restore-rebuild', [str(HERE / 'workers.py'), 'rebuild'], 'replay',
                             extra_env={'DATABASE_URL': restored_url, 'WORKER_METRICS_ENABLED': '0'})
        assert process.wait(timeout=120) == 0
        with psycopg.connect(restored_url) as restored:
            rows = restored.execute('''SELECT batch_id,warehouse_id,quantity FROM labops_inventoryprojection
                                      ORDER BY batch_id,warehouse_id''').fetchall()
            # Fixed6 is stored as an integer; normalise to the same precision as ORM values.
            rebuilt = [(str(b), str(w), str(Decimal(q) / Decimal(1000000))) for b, w, q in rows]
            normalized = sorted((b, w, str(Decimal(q))) for b, w, q in rebuilt)
            original = sorted((str(b), str(w), str(Decimal(q))) for b, w, q in
                self.models.InventoryProjection.objects.values_list('batch_id', 'warehouse_id', 'quantity'))
            assert normalized == original, 'Restored projection differs from legal ledger'
        replay_ids = [item['event_id'] for item in self.events[:min(1000, len(self.events))]]
        replay_file = self.evidence / 'markers' / 'restore-replay-ids.json'
        write_json(replay_file, replay_ids)
        replay_marker = self.marker('restore-replay')
        replay = self.spawn('restore-replay', [str(HERE / 'workers.py'), 'restore_check',
            '--data-file', str(replay_file), '--marker', str(replay_marker)], 'replay',
            extra_env={'DATABASE_URL': restored_url, 'WORKER_METRICS_ENABLED': '0'})
        self.wait_marker(replay_marker, replay, timeout=180)
        assert replay.wait(timeout=30) == 0
        replay_evidence = json.loads(replay_marker.read_text())
        self.cases.append({'name': 'postgres_restore', 'watermark': watermark,
            'backup_sha256': hashlib.sha256(dump).hexdigest(), 'backup_bytes': len(dump),
            'restored_database': name, 'restored_inventory_outbox_count': restored_count,
            'restored_dedupe_count': restored_dedupe, 'legal_projection_mismatches': 0,
            'snapshot_RPO_seconds': 0, 'measured_restore_seconds': time.monotonic() - recovery_started,
            'baseline': baseline, 'passed': True,
            'table_fingerprints_before': original_fingerprints,
            'table_fingerprints_after_restore': restored_fingerprints,
            'replay_in_restored_database': replay_evidence,
            'limits': ['Snapshot restore at frozen watermark only; PITR and older-snapshot loss not measured',
                       'Broker retention exhaustion and same-name topic replacement not executed']})
        self.start_publisher()
        self.start_consumer('notification')
        self.start_consumer('analytics')

    def latencies(self):
        ids = [item['event_id'] for item in self.events if item['scenario'] == 'steady']
        recorded = {item['event_id']: item for item in self.events}
        with self.connection.cursor() as cursor:
            cursor.execute('SHOW track_commit_timestamp')
            enabled = cursor.fetchone()[0] == 'on'
            if enabled:
                commits = {eid: recorded[eid]['outbox_transaction_commit_at'] for eid in ids}
                cursor.execute('''SELECT consumer_name,event_id::text,pg_xact_commit_timestamp(xmin)
                                  FROM labops_processedevent WHERE event_id = ANY(%s::uuid[])''', [ids])
                effects = {(name, eid): at.timestamp() if at else None for name, eid, at in cursor.fetchall()}
            else:
                commits, effects = {}, {}
        samples = {name: [] for name in ('notification', 'analytics')}
        missing = Counter()
        with (self.evidence / 'latency.csv').open('w', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(['event_id', 'consumer', 'outbox_transaction_commit', 'effect_transaction_commit',
                             'latency_seconds', 'outcome', 'command_return_observed_at'])
            for eid in ids:
                for name in samples:
                    begin, end = commits.get(eid), effects.get((name, eid))
                    latency = end - begin if begin is not None and end is not None else None
                    if latency is not None:
                        assert latency >= 0, 'Consumer effect precedes durable outbox commit'
                        samples[name].append(latency)
                    else:
                        missing[name] += 1
                    writer.writerow([eid, name, begin, end, latency,
                                     'completed' if latency is not None else 'missing_commit_timestamp',
                                     recorded[eid]['transaction_return_observed_at']])
        result = {'method': 'PostgreSQL insertion transaction xid captured before commit; ProcessedEvent xmin; pg_xact_commit_timestamp for both',
                  'track_commit_timestamp': enabled, 'denominator_per_consumer': len(ids), 'consumers': {}}
        for name, values in samples.items():
            values.sort()
            def percentile(p):
                return values[max(0, math.ceil(len(values) * p) - 1)] if values else None
            result['consumers'][name] = {'successful_samples': len(values), 'missing': missing[name],
                'mean_seconds': sum(values) / len(values) if values else None,
                'p95_seconds': percentile(.95), 'p99_seconds': percentile(.99),
                'passed': len(values) == len(ids) and bool(values) and percentile(.95) <= 5 and percentile(.99) <= 15}
        result['passed'] = enabled and all(row['passed'] for row in result['consumers'].values())
        write_json(self.evidence / 'latency-summary.json', result)
        return result

    def collect_metrics(self):
        import urllib.request
        for index, process in enumerate(self.children):
            if process.poll() is not None:
                continue
            port = self.child_metrics[process.pid]
            request = urllib.request.Request(f'http://127.0.0.1:{port}/metrics')
            token = self.env.get('WORKER_METRICS_TOKEN', self.env.get('METRICS_TOKEN', ''))
            if token:
                request.add_header('Authorization', 'Bearer ' + token)
            try:
                payload = urllib.request.urlopen(request, timeout=3).read()
                (self.evidence / 'metrics' / f'worker-{index}.prom').write_bytes(payload)
            except Exception as exc:
                with (self.evidence / 'errors.jsonl').open('a') as out:
                    out.write(json.dumps({'kind': 'metrics_scrape', 'worker_index': index,
                                          'error_type': type(exc).__name__}) + '\n')

    def metrics_acceptance(self):
        import urllib.request
        import urllib.error
        for name, command, role in [('retry', 'retry_events', 'replay'), ('dlq', 'publish_dlq', 'dlq')]:
            self.workers[name] = self.spawn(name + '-metrics-loop',
                ['manage.py', command, '--loop', '--limit', '100'], role)
        self.sync_metrics_targets()
        token = self.env.get('WORKER_METRICS_TOKEN', self.env.get('METRICS_TOKEN', ''))
        cases = []
        for name in ('publisher', 'notification', 'analytics', 'retry', 'dlq'):
            process = self.workers[name]
            port = self.child_metrics[process.pid]
            endpoint = f'http://127.0.0.1:{port}/metrics'

            def scrape():
                request = urllib.request.Request(endpoint, headers={'Authorization': 'Bearer ' + token})
                payload = urllib.request.urlopen(request, timeout=5).read().decode()
                assert f'worker="{name}"' in payload and 'labops_worker_database_available 1.0' in payload
                assert 'labops_worker_outbox_events' in payload and 'labops_worker_failed_deliveries' in payload
                heartbeat = re.search(r'^labops_worker_heartbeat_timestamp_seconds\{worker="' +
                                      re.escape(name) + r'"\} (\S+)$', payload, re.MULTILINE)
                assert heartbeat, 'Actual worker heartbeat metric missing'
                timestamp = float(heartbeat.group(1))
                assert math.isfinite(timestamp) and -2 <= time.time() - timestamp <= 30, 'Worker heartbeat is stale'
                return payload
            payload = self.wait(scrape, 'Worker authenticated metrics missing: ' + name, timeout=30)
            observed_before = time.time()
            before_timestamp = float(re.search(r'^labops_worker_heartbeat_timestamp_seconds\{worker="' +
                re.escape(name) + r'"\} (\S+)$', payload, re.MULTILINE).group(1))

            def advancing():
                candidate = scrape()
                timestamp = float(re.search(r'^labops_worker_heartbeat_timestamp_seconds\{worker="' +
                    re.escape(name) + r'"\} (\S+)$', candidate, re.MULTILINE).group(1))
                return (candidate, timestamp) if timestamp > before_timestamp else False
            after_payload, after_timestamp = self.wait(advancing, 'Worker heartbeat did not advance: ' + name, timeout=15)
            observed_after = time.time()
            denied = False
            try:
                urllib.request.urlopen(endpoint, timeout=5)
            except urllib.error.HTTPError as exc:
                denied = exc.code == 403
            assert denied, 'Unauthenticated worker metrics exposed'
            (self.evidence / 'metrics' / f'{name}-accepted.prom').write_text(payload)
            (self.evidence / 'metrics' / f'{name}-heartbeat-advanced.prom').write_text(after_payload)
            cases.append({'worker': name, 'pid': process.pid, 'port': port,
                'heartbeat_observed': True, 'heartbeat_before': before_timestamp,
                'heartbeat_after': after_timestamp, 'heartbeat_advanced': after_timestamp > before_timestamp,
                'first_scrape_observed_at': observed_before, 'second_scrape_observed_at': observed_after,
                'elapsed_between_samples_seconds': observed_after - observed_before,
                'database_available': True, 'unauthenticated_denied': True})

        def exporter_scrape():
            payload = urllib.request.urlopen('http://127.0.0.1:19308/metrics', timeout=5).read().decode()
            assert re.search(r'^kafka_brokers(?:\{[^}]*\})? 3(?:\.0)?$', payload, re.MULTILINE)
            replicas = [float(line.rsplit(' ', 1)[1]) for line in payload.splitlines()
                if line.startswith('kafka_topic_partition_replicas{') and
                f'topic="{self.settings.KAFKA_TOPIC}"' in line]
            assert len(replicas) == 3 and all(value == 3 for value in replicas)
            assert 'kafka_consumergroup_lag{' in payload
            return payload
        exporter = self.wait(exporter_scrape, 'Exporter actual broker/replica/group lag metrics missing', timeout=60)
        (self.evidence / 'metrics' / 'exporter-accepted.prom').write_text(exporter)
        self.cases.append({'name': 'metrics', 'workers': cases, 'exporter_broker_count': 3,
            'inventory_partition_replicas': [3, 3, 3], 'consumer_group_lag_observed': True,
            'durable_RETRY_DEAD_counts_separate_from_lag': True, 'passed': True})

    def live_alert_acceptance(self):
        import urllib.request
        from urllib.parse import urlencode
        import requests
        base = 'http://127.0.0.1:19091'
        instance = '127.0.0.1:19644'
        alert_query = ('ALERTS{alertname="RedpandaMetricsUnavailable",'
                       'alertstate="firing",instance="' + instance + '"}')
        observations = []

        def query(expression):
            url = base + '/api/v1/query?' + urlencode({'query': expression})
            response = json.loads(urllib.request.urlopen(url, timeout=5).read())
            assert response['status'] == 'success'
            observations.append({'observed_at': time.time(), 'query': expression, 'response': response})
            return response['data']['result']

        def all_brokers_up():
            rows = query('up{job="redpanda"}')
            return rows if len(rows) == 3 and all(float(row['value'][1]) == 1 for row in rows) else False

        baseline = self.wait(all_brokers_up, 'Prometheus broker baseline is not three successful scrapes', timeout=90)
        assert not query(alert_query), 'Target alert was already firing before injection'
        started = time.time()
        self.compose('stop', '-t', '0', 'redpanda-0')
        try:
            firing = self.wait(lambda: query(alert_query), 'Live broker alert did not fire after actual stop', timeout=150)
            assert all(row['metric'].get('instance') == instance and float(row['value'][1]) == 1 for row in firing)
            while time.time() - started < 150:
                time.sleep(.25)
            stopped_for = time.time() - started
        finally:
            self.compose('start', 'redpanda-0')
            write_json(self.evidence / 'metrics' / 'prometheus-live-alert-observations.json', observations)
        session = requests.Session()
        session.auth = ('admin', self.secrets['admin'])
        session.verify = self.settings.KAFKA_SSL_CA_LOCATION

        def broker_endpoint_ready():
            response = session.get('https://' + instance + '/public_metrics', timeout=5)
            return response.status_code == 200 and bool(response.content)
        self.wait(broker_endpoint_ready, 'Restored broker metrics endpoint did not return authenticated HTTP 200', timeout=120)
        restored_up = self.wait(all_brokers_up, 'Prometheus did not recover three healthy broker scrapes', timeout=90)
        self.wait(lambda: not query(alert_query), 'Live broker alert did not resolve', timeout=90)
        ended = time.time()
        raw_ranges = {}
        for name, expression in [('alert', alert_query), ('up', 'up{job="redpanda"}'),
                                  ('replication', 'redpanda_cluster_health_under_replicated_partitions')]:
            url = base + '/api/v1/query_range?' + urlencode({'query': expression,
                'start': started - 10, 'end': ended, 'step': 5})
            response = json.loads(urllib.request.urlopen(url, timeout=5).read())
            assert response['status'] == 'success'
            write_json(self.evidence / 'metrics' / f'prometheus-live-alert-{name}-range.json', response)
            raw_ranges[name] = response['data']['result']
        assert raw_ranges['alert'], 'Live firing interval missing from raw Prometheus history'
        write_json(self.evidence / 'metrics' / 'prometheus-live-alert-observations.json', observations)
        self.cases.append({'name': 'live_broker_metrics_alert', 'alert': 'RedpandaMetricsUnavailable',
            'instance': instance, 'baseline': baseline, 'actual_stop_seconds': stopped_for,
            'firing': firing, 'restored_endpoint_status': 200, 'restored_up': restored_up,
            'alert_cleared': True, 'history_start': started - 10, 'history_end': ended, 'passed': True})

    def run(self):
        self.setup()
        self.start_publisher()
        self.start_consumer('notification')
        self.start_consumer('analytics')
        from confluent_kafka.admin import AdminClient
        admin = AdminClient(self.configs['admin'])

        def ready():
            for name in ('notification', 'analytics'):
                group = self.consumer_group(name)
                description = admin.describe_consumer_groups([group])[group].result(timeout=10)
                if len(description.members) != 1 or len(description.members[0].assignment.topic_partitions) != 3:
                    return False
            return True
        self.wait(ready, 'Steady workload consumers never received partitions', timeout=120)
        steady_started = time.monotonic()
        ids, workload = self.generate(self.args.events, 'steady')
        while time.monotonic() - steady_started < self.args.duration:
            time.sleep(.25)
        self.drained(ids, timeout=self.args.drain_timeout)
        latency = self.latencies()
        snapshot = self.snapshot(ids)
        assert not snapshot['mismatches'] and snapshot['dedupe_count'] == len(ids) * 2
        assert snapshot['notification_count'] == snapshot['expected_notification_count']
        self.cases.append({'name': 'steady', 'workload': workload, 'reconciliation': snapshot,
                           'latency': latency, 'passed': latency['passed'] and
                           workload['schedule_lateness_seconds'] <= max(1, self.args.events / self.args.rate * .05) and
                           (self.args.tier != 'full' or workload['elapsed_seconds'] <= self.args.duration * 1.05)})
        if not latency['passed']:
            raise AssertionError('Steady workload latency threshold or complete sample coverage failed')
        if not self.cases[-1]['passed']:
            raise AssertionError('Business command generation missed frozen workload rate by more than 5 percent')
        self.duplicate_drill(ids[:min(self.args.duplicate_events, len(ids))])
        self.consumer_crashes()
        self.publisher_crashes()
        self.analytics_outage()
        self.broker_fault(['redpanda-0'], self.args.broker_fault_seconds, 'one_broker_stop')
        self.broker_fault(['redpanda-0', 'redpanda-1'], self.args.outage_seconds, 'quorum_loss')
        self.broker_fault(['redpanda-0', 'redpanda-1', 'redpanda-2'], self.args.outage_seconds, 'cluster_outage')
        self.postgres_failure()
        self.retry_drill()
        self.poison_drill()
        self.rebalance()
        from security import run_security_probes
        security = run_security_probes(self.configs, self.settings.KAFKA_TOPIC,
                                       self.args.run_id, self.evidence)
        self.cases.append({'name': 'security', **security})
        if not security['passed']:
            raise AssertionError('Security negative cases did not all demonstrate a denial')
        self.restore()
        from recovery_matrix import run_recovery_matrix
        recovery_cases = run_recovery_matrix(self)
        self.cases.extend(recovery_cases)
        if len(recovery_cases) != 3 or not all(case.get('passed') for case in recovery_cases):
            raise AssertionError('Required recovery matrix failed; inspect recovery-matrix.json')
        all_ids = [item['event_id'] for item in self.events]
        self.drained(all_ids, timeout=self.args.drain_timeout)
        self.metrics_acceptance()
        self.live_alert_acceptance()
        self.collect_metrics()
        reconciliation = self.snapshot(all_ids)
        assert not reconciliation['mismatches']
        assert reconciliation['dedupe_count'] == len(all_ids) * 2
        assert reconciliation['notification_count'] == reconciliation['expected_notification_count']
        write_json(self.evidence / 'reconciliation.json', reconciliation)
        write_json(self.evidence / 'offsets-after.json', self.offsets())
        write_json(self.evidence / 'workload.json', {'steady': workload, 'unique_inventory_events': len(all_ids),
            'event_type_counts': dict(Counter(item['kind'] for item in self.events)),
            'fault_repetitions': self.args.fault_repetitions, 'cases': self.cases})

    def finish(self, error=None):
        for name in list(self.workers):
            self.stop(name)
        for process in self.children:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        for log in self.logs:
            log.close()
        passing_cases = {case.get('name') for case in self.cases if case.get('passed')}
        limits = ['Mandatory independent-host/AZ fault exercise not executed by this same-host harness',
                  'Synthetic command workload; HTTP command throughput and production capacity unmeasured',
                  'PostgreSQL PITR and quantified older-snapshot business losses remain unmeasured',
                  'Publisher crash and standalone durable-retry drills accelerate disposable lease/due timestamps; natural configured TTL/retry wait is unmeasured in those drills',
                  'Broker recovery measures the recorded CI retry policy; the default 60/300/900/3600-second production schedule is not measured by this run',
                  'No external email/SMS exactly-once claim; only effects in this PostgreSQL database']
        for name, description in [('live_broker_metrics_alert', 'live broker alert firing/recovery'),
                                  ('isolated_retention_exhaustion', 'natural broker retention exhaustion'),
                                  ('same_name_topic_recreation', 'same-name topic replacement'),
                                  ('postgres_restart_retry_dead_dedupe', 'RETRY/DEAD/dedupe persistence across restart')]:
            if name not in passing_cases:
                limits.append('Mandatory ' + description + ' has no passing execution evidence')
        if self.args.tier != 'full':
            limits.append('Smoke tier does not establish the 90,000-event/1,800-second/50-per-second capacity target')
        report = {'passed': error is None and all(case.get('passed') for case in self.cases),
            'run_id': self.args.run_id, 'unique_generated_events': len(self.events),
            'acceptance_tier': self.args.tier, 'production_ready': False,
            'full_workload_requested': self.args.tier == 'full',
            'full_workload_targets_passed': self.args.tier == 'full' and 'steady' in passing_cases,
            'steady_inventory_inputs_committed': sum(item['scenario'] == 'steady' for item in self.events),
            'elapsed_seconds': time.time() - self.started_at, 'cases': self.cases,
            'error_type': type(error).__name__ if error else None,
            'error': str(error) if error else None,
            'shutdowns': self.shutdowns,
            'supervisor_restarts': self.supervisor_restarts,
            'limits': limits}
        write_json(self.evidence / 'report.json', report)
        lines = [f"Run {self.args.run_id}: {'PASS' if report['passed'] else 'FAIL'}", '',
                 f"Unique generated inventory events: {len(self.events)}", '',
                 'Cases:']
        lines += [f"- {case['name']}: {'PASS' if case.get('passed') else 'FAIL'}" for case in self.cases]
        lines += ['', 'Limits:'] + ['- ' + value for value in report['limits']]
        if error:
            lines += ['', f'Failure: {type(error).__name__}: {error}']
            with (self.evidence / 'errors.jsonl').open('a') as out:
                out.write(json.dumps({'kind': 'acceptance_failure', 'error_type': type(error).__name__,
                                      'message': str(error)}, sort_keys=True) + '\n')
        with (self.evidence / 'errors.jsonl').open('a') as out:
            try:
                for row in self.models.FailedDelivery.objects.order_by('created_at').values(
                        'id', 'consumer_name', 'delivery_key', 'status', 'failure_class', 'last_error', 'original_hash'):
                    out.write(json.dumps({'kind': 'durable_failed_delivery', **row}, default=str, sort_keys=True) + '\n')
            except Exception as exc:
                out.write(json.dumps({'kind': 'failure_evidence_query', 'error_type': type(exc).__name__}) + '\n')
        (self.evidence / 'summary.md').write_text('\n'.join(lines) + '\n')
        print(json.dumps({'passed': report['passed'], 'evidence_dir': str(self.evidence),
                          'cases_completed': len(self.cases), 'error_type': report['error_type']}), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--tier', choices=['smoke', 'full'], default='smoke')
    p.add_argument('--events', type=int, default=200)
    p.add_argument('--rate', type=float, default=10)
    p.add_argument('--duration', type=float, default=20)
    p.add_argument('--fault-repetitions', type=int, default=1)
    p.add_argument('--evidence-dir', type=Path, required=True)
    p.add_argument('--generated-dir', type=Path, default=ROOT / 'infra/events/validation/generated')
    p.add_argument('--fault-events', type=int, default=20)
    p.add_argument('--duplicate-events', type=int, default=10000)
    p.add_argument('--poison-events', type=int, default=100)
    p.add_argument('--broker-fault-seconds', '--single-broker-outage-seconds', type=float, default=5)
    p.add_argument('--outage-seconds', '--all-broker-outage-seconds', type=float, default=5)
    p.add_argument('--consumer-outage-seconds', type=float, default=5)
    p.add_argument('--drain-timeout', '--drain-timeout-seconds', type=float, default=180)
    args = p.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', args.run_id):
        p.error('--run-id must contain 1-48 lowercase letters, digits, underscore or hyphen')
    bounds = {'events': (4, 1000000), 'rate': (.01, 1000), 'duration': (0, 86400),
              'fault_repetitions': (1, 100), 'fault_events': (1, 100000),
              'duplicate_events': (1, 100000), 'poison_events': (12, 1000),
              'broker_fault_seconds': (1, 3600), 'outage_seconds': (1, 3600),
              'consumer_outage_seconds': (1, 3600), 'drain_timeout': (1, 7200)}
    for name, (minimum, maximum) in bounds.items():
        value = getattr(args, name)
        if not math.isfinite(value) or not minimum <= value <= maximum:
            p.error(f'--{name.replace("_", "-")} must be finite and between {minimum} and {maximum}')
    if args.events >= 90000 and args.tier != 'full':
        p.error('90,000-event acceptance requires explicit --tier full and the frozen full profile')
    if args.tier == 'full':
        if (args.events, args.rate, args.duration) != (90000, 50, 1800):
            p.error('--tier full requires exactly --events 90000 --rate 50 --duration 1800')
        minimums = {'fault_repetitions': 20, 'fault_events': 30000, 'duplicate_events': 10000,
                    'poison_events': 100, 'broker_fault_seconds': 300, 'outage_seconds': 600,
                    'consumer_outage_seconds': 600, 'drain_timeout': 900}
        for name, minimum in minimums.items():
            if getattr(args, name) < minimum:
                p.error(f'--tier full requires --{name.replace("_", "-")} >= {minimum}')
    args.generated_dir = args.generated_dir.resolve()
    args.evidence_dir = args.evidence_dir.resolve()
    load_environment(args.generated_dir / 'client.env')
    for key in list(os.environ):
        if key.startswith('POSTGRES_') and key != 'POSTGRES_PASSWORD':
            os.environ.pop(key)
    harness = Harness(args)
    error = None
    try:
        harness.run()
    except BaseException as exc:
        error = exc
        raise
    finally:
        harness.finish(error)


if __name__ == '__main__':
    main()
