"""Destructive recovery drills confined to fresh, run-scoped validation topics.

The production inventory topic is never deleted, truncated or reconfigured.
Call run_recovery_matrix(harness) after the frozen-watermark restore drill and
append its returned cases to the harness report. A false case is a required
acceptance failure, including a retention simulation without observed cleanup.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlparse, urlunparse


HERE = Path(__file__).resolve().parent
CASE_NAMES = (
    'postgres_restart_retry_dead_dedupe',
    'isolated_retention_exhaustion',
    'same_name_topic_recreation',
)
BUSINESS_TABLES = (
    'stockmovement', 'stockmovementline', 'stockbalance',
    'inventoryprojection', 'outboxevent', 'processedevent', 'notification',
)
EFFECT_KEYS = (
    'ledger_hash', 'balance_hash', 'projection_hash',
    'notification_count', 'notification_hash', 'dedupe_count', 'dedupe_hash',
)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     default=str).encode()).hexdigest()


def _write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + '\n')
    temporary.replace(path)


def _kafka_diagnostics(exc):
    """Keep native error identities without logging broker messages or secrets."""
    error = exc.args[0] if exc.args else None
    code = error.code() if callable(getattr(error, 'code', None)) else getattr(exc, 'kafka_error_code', None)
    if type(code) is not int:
        return {}
    result = {'kafka_error_code': code}
    name = error.name() if callable(getattr(error, 'name', None)) else None
    if isinstance(name, str) and re.fullmatch(r'[_A-Z][_A-Z0-9]*', name):
        result['kafka_error_name'] = name
    for flag in ('retriable', 'fatal'):
        method = getattr(error, flag, None)
        if callable(method):
            result['kafka_error_' + flag] = bool(method())
    return result


def _scope(run_id, case, generation='1'):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', run_id):
        raise ValueError('Recovery run-id must be a disposable validation run identifier')
    if case not in {'restart', 'retention', 'recreate'}:
        raise ValueError('Unknown recovery topic scope')
    if not re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,15}', generation):
        raise ValueError('Invalid recovery generation')
    prefix = f'labops.{run_id}.recovery.{case}'
    return {'topic': prefix + '.v1', 'group_prefix': prefix + '.' + generation,
            'source_cluster': 'validation.' + run_id,
            'source_generation': 'recovery.' + case + '.' + generation}


def describe_plan(run_id):
    """Read-only CLI inspection; does not import Django or connect to a broker."""
    scopes = [_scope(run_id, case) for case in ('restart', 'retention', 'recreate')]
    assert len({scope['topic'] for scope in scopes}) == 3
    assert all(len(scope['topic']) <= 249 and len(scope['group_prefix']) <= 96
               and len(scope['source_generation']) <= 96 for scope in scopes)
    return {'dry_run': True, 'run_id': run_id, 'cases': list(CASE_NAMES),
            'main_topic': f'labops.{run_id}.inventory.v1', 'scopes': scopes,
            'fault_scope': 'Disposable loopback PostgreSQL and RF3 validation broker processes',
            'retention_policy_wait_seconds': 75,
            'retention_fallback': 'Explicit delete_records simulation; required policy gate remains failed',
            'production_topic_destructive_operations': 0}


class _RecoveryMatrix:
    def __init__(self, harness):
        self.h = harness
        self.cases = []
        self.topics = set()
        self.last_kafka_operation = None
        self.run_id = harness.args.run_id
        self.main_topic = harness.settings.KAFKA_TOPIC
        self._guard()
        from confluent_kafka.admin import AdminClient
        self.admin = AdminClient(harness.configs['admin'])

    def _guard(self):
        h = self.h
        describe_plan(self.run_id)
        if h.env.get('LABOPS_VALIDATION_PROJECT') != 'labops_events_' + self.run_id:
            raise ValueError('Recovery faults require the exact disposable Compose project')
        parsed = urlparse(h.env['DATABASE_URL'])
        if parsed.hostname not in {'localhost', '127.0.0.1'} or parsed.path != '/labops_events':
            raise ValueError('Recovery faults require the disposable loopback business database')
        if h.settings.KAFKA_GROUP_PREFIX != 'labops.' + self.run_id:
            raise ValueError('Recovery faults require the run-specific group prefix')
        if h.settings.KAFKA_SOURCE_CLUSTER_ID != 'validation.' + self.run_id:
            raise ValueError('Recovery faults require the run-specific source cluster identity')
        if h.settings.KAFKA_SECURITY_PROTOCOL != 'SASL_SSL':
            raise ValueError('Recovery faults require SASL_SSL')
        if not all(server.strip().rsplit(':', 1)[0] in {'localhost', '127.0.0.1'}
                   for server in h.settings.KAFKA_BOOTSTRAP_SERVERS.split(',')):
            raise ValueError('Recovery broker operations are loopback-only')

    def _assert_topic(self, topic):
        allowed = {_scope(self.run_id, case)['topic']
                   for case in ('restart', 'retention', 'recreate')}
        if topic not in allowed or topic in {self.main_topic, self.h.settings.KAFKA_DLQ_TOPIC}:
            raise ValueError('Destructive operation refused outside sacrificial recovery topics')

    def _error(self, kind, **fields):
        with (self.h.evidence / 'errors.jsonl').open('a') as output:
            output.write(json.dumps({'kind': kind, **fields}, default=str, sort_keys=True) + '\n')

    def _report(self):
        _write_json(self.h.evidence / 'recovery-matrix.json', {
            'run_id': self.run_id, 'cases': self.cases,
            'required_cases': list(CASE_NAMES),
            'passed': len(self.cases) == len(CASE_NAMES) and all(row['passed'] for row in self.cases),
            'sacrificial_topics': sorted(self.topics),
            'last_kafka_operation': self.last_kafka_operation,
            'production_topic_destructive_operations': 0,
            'limits': ['Same-host restart/recovery only; independent host/AZ faults are separate',
                       'Frozen snapshot with a deliberately future broker record; PITR is separate']})

    def _consumer_config(self, group):
        return {**self.h.configs['admin'], 'group.id': group,
                'enable.auto.commit': False, 'enable.auto.offset.store': False,
                'auto.offset.reset': 'earliest', 'isolation.level': 'read_committed'}

    def _producer(self):
        from confluent_kafka import Producer
        from labops.kafka_config import producer_config
        return Producer(producer_config('admin'))

    def _publish(self, topic, value, *, key='recovery'):
        self._assert_topic(topic)
        sender = self._producer()
        acknowledgements = []
        raw = value if isinstance(value, bytes) else json.dumps(
            value, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()

        def delivered(error, message):
            if error is None:
                acknowledgements.append({'topic': message.topic(), 'partition': message.partition(),
                                         'offset': message.offset()})
            else:
                acknowledgements.append({'error_code': error.code()})
        sender.produce(topic, partition=0, key=key, value=raw, on_delivery=delivered)
        assert sender.flush(self.h.settings.KAFKA_PUBLISH_FLUSH_SECONDS) == 0
        assert len(acknowledgements) == 1 and 'error_code' not in acknowledgements[0], 'Recovery record not acknowledged'
        return {**acknowledgements[0], 'raw_sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}

    def _create_topic(self, topic, *, retention_ms=600000, segment_bytes=None):
        self._assert_topic(topic)
        from confluent_kafka.admin import NewTopic
        self.last_kafka_operation = {'step': 'metadata_before_create', 'topic': topic}
        assert topic not in self.admin.list_topics(timeout=15).topics, 'Sacrificial topic already exists'
        config = {'cleanup.policy': 'delete', 'retention.ms': str(retention_ms),
                  'write.caching': 'false', 'compression.type': 'producer'}
        # Redpanda's allowlist_topic_noop_confs includes min.insync.replicas;
        # do not declare or demand a DescribeConfigs value for an ignored key.
        # Actual RF3 and all three ISR members remain mandatory below.
        if segment_bytes is not None:
            config['segment.bytes'] = str(segment_bytes)
        # v26.2.2 rejects topic-level "disabled" although DescribeConfigs may
        # report it when the cluster has write_caching_default=disabled.
        # https://github.com/redpanda-data/redpanda/blob/v26.2.2/src/v/kafka/server/handlers/topics/validators.h#L380-L406
        self.last_kafka_operation = {'step': 'create_topic', 'topic': topic,
                                     'partitions': 1, 'replication_factor': 3, 'config': dict(config)}
        future = self.admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=3,
                                                     config=config)], request_timeout=20)[topic]
        future.result(timeout=25)
        self.topics.add(topic)

        self._wait_topic(topic)
        return config

    def _wait_topic(self, topic):
        self._assert_topic(topic)
        self.last_kafka_operation = {'step': 'wait_topic_full_isr', 'topic': topic}

        def ready():
            metadata = self.admin.list_topics(timeout=5).topics.get(topic)
            return (metadata is not None and metadata.error is None and len(metadata.partitions) == 1
                    and all(part.leader >= 0 and len(part.replicas) == len(part.isrs) == 3
                            for part in metadata.partitions.values()))
        self.h.wait(ready, 'Sacrificial RF3 topic did not become ready', timeout=90)

    def _alter_topic(self, topic, config):
        self._assert_topic(topic)
        from confluent_kafka.admin import ConfigResource, ResourceType
        from infra.events.admin import topic_value_matches
        # Set the complete known configuration of this freshly created topic;
        # no production or internal topic ever reaches this helper.
        resource = ConfigResource(ResourceType.TOPIC, topic, set_config=config)
        self.last_kafka_operation = {'step': 'alter_topic_config', 'topic': topic, 'config': dict(config)}
        self.admin.alter_configs([resource], request_timeout=20)[resource].result(timeout=25)
        self.last_kafka_operation = {'step': 'verify_topic_config', 'topic': topic, 'config': dict(config)}
        observed = self.admin.describe_configs([ConfigResource(ResourceType.TOPIC, topic)],
                                               request_timeout=20)
        values = next(iter(observed.values())).result(timeout=25)
        for name, expected in config.items():
            assert topic_value_matches(name, expected, values[name].value), 'Sacrificial topic configuration did not apply: ' + name

    def _watermarks(self, topic):
        self._assert_topic(topic)
        from confluent_kafka import Consumer, TopicPartition
        client = Consumer(self._consumer_config(f'labops.{self.run_id}.recovery.inspect'))
        try:
            low, high = client.get_watermark_offsets(TopicPartition(topic, 0), timeout=10, cached=False)
            return {'low': low, 'high': high}
        finally:
            client.close()

    def _read_at(self, topic, offset):
        self._assert_topic(topic)
        from confluent_kafka import Consumer, TopicPartition
        reader = Consumer(self._consumer_config(f'labops.{self.run_id}.recovery.read'))
        try:
            reader.assign([TopicPartition(topic, 0, offset)])
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                message = reader.poll(1)
                if message is None:
                    continue
                assert message.error() is None, 'Sacrificial source read failed'
                assert message.partition() == 0 and message.offset() == offset, 'Source coordinate was not preserved'
                return {'topic': message.topic(), 'partition': 0, 'offset': message.offset(),
                        'raw_sha256': hashlib.sha256(message.value()).hexdigest(),
                        'value': json.loads(message.value())}
            raise AssertionError('Sacrificial source coordinate could not be read')
        finally:
            reader.close()

    def _cursor(self, topic, group, *, offset=None):
        self._assert_topic(topic)
        from confluent_kafka import Consumer, TopicPartition
        reader = Consumer(self._consumer_config(group))
        try:
            part = TopicPartition(topic, 0, offset if offset is not None else -1001)
            if offset is not None:
                committed = reader.commit(offsets=[part], asynchronous=False)
                assert committed and all(row.error is None for row in committed)
            result = reader.committed([TopicPartition(topic, 0)], timeout=15)[0]
            assert result.error is None
            return result.offset
        finally:
            reader.close()

    def _worker_env(self, scope, name, *, database_url=None):
        # Only disposable recovery workers borrow validation-admin privileges.
        # The normal publisher and consumers restart with their own identities.
        env = {'KAFKA_TOPIC': scope['topic'], 'KAFKA_GROUP_PREFIX': scope['group_prefix'],
               'KAFKA_SOURCE_CLUSTER_ID': scope['source_cluster'],
               'KAFKA_SOURCE_STREAM_GENERATION': scope['source_generation'],
               f'KAFKA_{name.upper()}_SASL_USERNAME': 'admin',
               f'KAFKA_{name.upper()}_SASL_PASSWORD': self.h.secrets['admin'],
               f'KAFKA_{name.upper()}_SASL_PASSWORD_FILE': self.h.env.get('KAFKA_ADMIN_SASL_PASSWORD_FILE', ''),
               'WORKER_METRICS_ENABLED': '0'}
        if database_url:
            env['DATABASE_URL'] = database_url
        return env

    def _wait_child(self, child, label, timeout=90):
        try:
            code = child.wait(timeout=timeout)
        except Exception:
            child.kill()
            child.wait(timeout=10)
            raise
        assert code == 0, f'Recovery worker failed: {label} ({code})'

    def _consume(self, scope, name, count, *, database_url=None):
        child = self.h.spawn('recovery-' + scope['source_generation'] + '-' + name,
            ['manage.py', 'consume_kafka', name, '--max-messages', str(count), '--idle-timeout', '30'],
            'admin', extra_env=self._worker_env(scope, name, database_url=database_url))
        self._wait_child(child, name)
        cursor = self._cursor(scope['topic'], scope['group_prefix'] + '.' + name + '.v1')
        return {'exit_code': child.returncode, 'group': scope['group_prefix'] + '.' + name + '.v1',
                'committed_offset': cursor, 'source_generation': scope['source_generation']}

    def _pause_main(self):
        for name in ('publisher', 'notification', 'analytics'):
            self.h.stop(name)

    def _resume_main(self):
        h = self.h
        if 'publisher' not in h.workers:
            h.start_publisher()
        for name in ('notification', 'analytics'):
            if name not in h.workers:
                h.start_consumer(name)

    def _drain_main(self, ids):
        self._resume_main()
        try:
            self.h.drained(ids, timeout=self.h.args.drain_timeout)
            self.h.wait_for_log_deliveries(ids, minimum=len(ids) * 2,
                                          timeout=self.h.args.drain_timeout)
        finally:
            self._pause_main()

    def _failure_rows(self, ids):
        return list(self.h.models.FailedDelivery.objects.filter(id__in=ids).order_by('id').values(
            'id', 'consumer_name', 'source_cluster', 'source_generation', 'delivery_key',
            'envelope', 'original_hash', 'failure_class', 'status', 'attempts', 'last_error',
            'next_attempt_at', 'locked_until', 'lease_token', 'dlq_next_attempt_at',
            'dlq_locked_until', 'dlq_lease_token', 'dlq_attempts', 'dlq_published_at',
            'resolved_at', 'resolution_note'))

    def restart(self):
        h = self.h
        scope = _scope(self.run_id, 'restart')
        self._create_topic(scope['topic'])
        ids, workload = h.generate(1, 'recovery_restart')
        event = h.models.OutboxEvent.objects.get(id=ids[0])
        value = h.api.envelope(event)
        record = self._publish(scope['topic'], value)
        assert record['offset'] == 0
        notification = self._consume(scope, 'notification', 1)
        marker = h.marker('recovery-restart-retry')
        child = h.spawn('recovery-restart-transient', [str(HERE / 'workers.py'), 'consumer',
            '--consumer', 'analytics', '--stage', 'transient_failure', '--event', ids[0],
            '--marker', str(marker), '--max-messages', '1'], 'admin',
            extra_env=self._worker_env(scope, 'analytics'))
        h.wait_marker(Path(str(marker) + '.completed'), child, timeout=90)
        self._wait_child(child, 'transient analytics', timeout=30)
        retry = h.models.FailedDelivery.objects.get(consumer_name='analytics',
            source_cluster=scope['source_cluster'], source_generation=scope['source_generation'],
            envelope__event_id=ids[0])
        assert retry.status == 'RETRY' and retry.original_hash == h.api.canonical_payload_hash(value)
        assert not h.models.ProcessedEvent.objects.filter(consumer_name='analytics', event_id=ids[0]).exists()
        poison = json.loads(json.dumps(value))
        poison['schema_version'] = 999
        poison_record = self._publish(scope['topic'], poison, key='same-original-id-poison')
        assert poison_record['offset'] == 1
        dead_worker = self._consume(scope, 'analytics', 1)
        dead = h.models.FailedDelivery.objects.get(consumer_name='analytics',
            source_cluster=scope['source_cluster'], source_generation=scope['source_generation'],
            delivery_key=f"{scope['source_cluster']}:{scope['source_generation']}:{scope['topic']}:0:1")
        assert dead.status == 'DEAD' and dead.envelope['event_id'] == ids[0]
        rows_before = self._failure_rows([retry.id, dead.id])
        marker_before = list(h.models.ProcessedEvent.objects.filter(event_id=ids[0]).values(
            'id', 'consumer_name', 'event_id', 'payload_hash'))
        state_before = h.snapshot(ids)
        began = time.monotonic()
        h.compose('restart', '-t', '0', 'postgres', 'redpanda-0', 'redpanda-1', 'redpanda-2', timeout=120)
        h.connections.close_all()
        h.wait(lambda: h.models.User.objects.count() > 0, 'PostgreSQL restart did not recover', timeout=90)
        h.wait_brokers()
        self._wait_topic(scope['topic'])
        rows_after = self._failure_rows([retry.id, dead.id])
        assert rows_before == rows_after, 'RETRY/DEAD original state lost on restart'
        assert h.api.envelope(h.models.OutboxEvent.objects.get(id=ids[0])) == value, 'Original outbox changed on restart'
        assert marker_before == list(h.models.ProcessedEvent.objects.filter(event_id=ids[0]).values(
            'id', 'consumer_name', 'event_id', 'payload_hash')), 'Dedupe marker lost on restart'
        state_after = h.snapshot(ids)
        assert all(state_before[key] == state_after[key] for key in EFFECT_KEYS), 'Restart changed database effects'
        for original in (record, poison_record):
            restored = self._read_at(scope['topic'], original['offset'])
            assert restored['raw_sha256'] == original['raw_sha256'], 'Broker restart lost original source bytes'
        from django.utils import timezone
        retry.refresh_from_db()
        fixture_due = timezone.now()
        retry_due_fixture = {'kind': 'accelerated_due_time_fixture',
                             'delivery_id': str(retry.id),
                             'original_next_attempt_at': retry.next_attempt_at,
                             'fixture_next_attempt_at': fixture_due,
                             'natural_backoff_wait_measured': False,
                             'applied_after_restart_preservation_check': True}
        # The recovery proof above compares the persisted natural due time.
        # Only subsequent independent-worker resolution uses this explicitly
        # accelerated fixture; it does not measure natural backoff timing.
        h.models.FailedDelivery.objects.filter(id=retry.id).update(next_attempt_at=fixture_due)
        worker = h.spawn('recovery-restart-retry-worker', ['manage.py', 'retry_events', '--limit', '100'],
                         'replay', extra_env={'WORKER_METRICS_ENABLED': '0'})
        self._wait_child(worker, 'independent retry')
        retry.refresh_from_db()
        dead.refresh_from_db()
        assert retry.status == 'RESOLVED' and retry.envelope == value
        assert retry.original_hash == h.api.canonical_payload_hash(value)
        assert dead.status == 'DEAD' and dead.original_hash == h.api.canonical_payload_hash(poison)
        before_duplicates = h.snapshot(ids)
        assert not before_duplicates['mismatches'] and before_duplicates['dedupe_count'] == 2
        self._drain_main(ids)
        after_duplicates = h.snapshot(ids)
        assert all(before_duplicates[key] == after_duplicates[key] for key in EFFECT_KEYS)
        return {'name': CASE_NAMES[0], 'passed': True, 'input': 1, 'workload': workload,
                'event_id': ids[0], 'topic': scope['topic'], 'source_identity': scope,
                'services_restarted': ['postgres', 'redpanda-0', 'redpanda-1', 'redpanda-2'],
                'restart_and_recovery_seconds': time.monotonic() - began,
                'durable_rows_before': rows_before, 'durable_rows_after_restart': rows_after,
                'durable_rows_hash': _hash(rows_before), 'dedupe_marker_preserved': True,
                'notification_worker': notification, 'dead_worker': dead_worker,
                'retry_worker_exit_code': worker.returncode, 'retry_due_time_fixture': retry_due_fixture,
                'resolved_original_event_id': retry.envelope['event_id'],
                'retry_original_hash': retry.original_hash, 'dead_original_hash': dead.original_hash,
                'main_topic_redeliveries': 2, 'duplicate_database_effects': 0,
                'before_main_redelivery': before_duplicates, 'after_main_redelivery': after_duplicates,
                'limits': ['Natural retry backoff timing is unmeasured; independent retry resolution uses an accelerated due-time fixture']}

    def _purge(self, topic, offset):
        self._assert_topic(topic)
        from confluent_kafka import TopicPartition
        part = TopicPartition(topic, 0, offset)
        result = self.admin.delete_records([part], request_timeout=20,
                                           operation_timeout=15)[part].result(timeout=25)
        assert result.low_watermark >= offset, 'Sacrificial log did not truncate to the requested watermark'
        return {'requested_offset': offset, 'low_watermark': result.low_watermark}

    def _fingerprints(self, url, tables=BUSINESS_TABLES):
        import psycopg
        from psycopg import sql
        result = {}
        with psycopg.connect(url) as database:
            for table in tables:
                rows = database.execute(sql.SQL('SELECT row_to_json(r) FROM {} r ORDER BY id').format(
                    sql.Identifier('labops_' + table))).fetchall()
                result[table] = {'count': len(rows), 'sha256': _hash([row[0] for row in rows])}
        return result

    def _snapshot_database(self):
        h = self.h
        import psycopg
        from psycopg import sql
        name = 'recovery_' + re.sub('[^a-z0-9_]', '_', self.run_id)[:40]
        parsed = urlparse(h.env['DATABASE_URL'])
        admin_url = urlunparse(parsed._replace(path='/postgres'))
        restored_url = urlunparse(parsed._replace(path='/' + name))
        dump = h.compose('exec', '-T', 'postgres', 'pg_dump', '-U', 'labops', '-Fc',
                         '-d', 'labops_events', binary=True, timeout=180)
        path = h.evidence / 'backup' / 'recovery-watermark.dump'
        path.write_bytes(dump)
        path.chmod(0o600)
        before = self._fingerprints(h.env['DATABASE_URL'])
        with psycopg.connect(admin_url, autocommit=True) as database:
            database.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        h.command(['docker', 'compose', '-p', h.env['LABOPS_VALIDATION_PROJECT'],
            '-f', 'infra/events/validation/compose.yaml', 'exec', '-T', 'postgres',
            'pg_restore', '-U', 'labops', '-d', name, '--exit-on-error'],
            timeout=180, binary=True, input=dump)
        restored = self._fingerprints(restored_url)
        assert before == restored, 'Older snapshot does not match its frozen business watermark'
        return restored_url, {'database': name, 'backup_sha256': hashlib.sha256(dump).hexdigest(),
                              'backup_bytes': len(dump), 'watermark_event_count': len(h.events),
                              'business_tables_at_watermark': restored}

    def retention(self):
        h = self.h
        scope = _scope(self.run_id, 'retention')
        config = self._create_topic(scope['topic'], retention_ms=-1, segment_bytes=1048576)
        copied_ids = [item['event_id'] for item in h.events[-min(4, len(h.events)):]]
        records = [self._publish(scope['topic'], h.api.envelope(h.models.OutboxEvent.objects.get(id=eid)))
                   for eid in copied_ids]
        assert records and records[0]['offset'] == 0
        cursor_group = scope['group_prefix'] + '.parked.v1'
        assert self._cursor(scope['topic'], cursor_group, offset=0) == 0
        initial = self._watermarks(scope['topic'])
        # Redpanda clamps segment.ms to a cluster minimum (normally 10 minutes).
        # Roll by size instead, without changing any cluster-wide settings.
        filler = b'R' * (750 * 1024)
        filler_records = [self._publish(scope['topic'], filler, key=f'segment-filler-{number}')
                          for number in range(6)]
        config['retention.ms'] = '1000'
        self._alter_topic(scope['topic'], config)
        samples = []
        started = time.monotonic()
        policy_cleanup = False
        last_original_offset = max(record['offset'] for record in records)
        while time.monotonic() - started < 75:
            observed = self._watermarks(scope['topic'])
            samples.append({'elapsed_seconds': time.monotonic() - started, **observed})
            if observed['low'] > last_original_offset:
                policy_cleanup = True
                break
            time.sleep(1)
        policy_wait_seconds = time.monotonic() - started
        method = 'broker_retention_policy_cleanup' if policy_cleanup else 'retention exhaustion simulation'
        limits = []
        fallback = None
        if not policy_cleanup:
            fallback = self._purge(scope['topic'], self._watermarks(scope['topic'])['high'])
            limits.append('Broker cleanup did not advance the low watermark within 75 seconds; delete_records simulates retention exhaustion')
            self._error('retention_policy_gate_not_observed', topic=scope['topic'], wait_seconds=75,
                        recovery_simulation_used=True)
        exhausted = self._watermarks(scope['topic'])
        assert exhausted['low'] > last_original_offset, 'Original copied records remain readable'
        parked_cursor = self._cursor(scope['topic'], cursor_group)
        assert parked_cursor == 0 and parked_cursor < exhausted['low'], 'Parked group is not beyond available retention'
        # Retention proof is recorded above. Remove only remaining sacrificial
        # filler so the following future-record probe has one precise input.
        filler_cleanup = (self._purge(scope['topic'], exhausted['high'])
                          if exhausted['low'] < exhausted['high'] else None)
        config['retention.ms'] = '-1'
        self._alter_topic(scope['topic'], config)
        baseline = h.snapshot(copied_ids)
        assert not baseline['mismatches']
        legal_before = self._fingerprints(h.env['DATABASE_URL'], ('stockmovement', 'stockmovementline', 'stockbalance'))
        h.models.InventoryProjection.objects.all().delete()
        assert h.models.InventoryProjection.objects.count() == 0
        rebuild = h.spawn('recovery-retention-ledger-rebuild', [str(HERE / 'workers.py'), 'rebuild'],
                          'replay', extra_env={'WORKER_METRICS_ENABLED': '0'})
        self._wait_child(rebuild, 'legal-ledger projection rebuild', timeout=180)
        rebuilt = h.snapshot(copied_ids)
        assert not rebuilt['mismatches']
        assert all(baseline[key] == rebuilt[key] for key in EFFECT_KEYS)
        assert legal_before == self._fingerprints(h.env['DATABASE_URL'], ('stockmovement', 'stockmovementline', 'stockbalance'))
        restored_url, watermark = self._snapshot_database()
        restored_before = self._fingerprints(restored_url)
        future_ids, future_workload = h.generate(1, 'recovery_future_after_snapshot')
        future = h.models.OutboxEvent.objects.get(id=future_ids[0])
        value = h.api.envelope(future)
        future_record = self._publish(scope['topic'], value, key='future-after-snapshot')
        assert self._read_at(scope['topic'], future_record['offset'])['value'] == value
        future_worker = self._consume(scope, 'analytics', 1, database_url=restored_url)
        import psycopg
        with psycopg.connect(restored_url) as database:
            row = database.execute('''SELECT id,status,last_error,source_cluster,source_generation,
                delivery_key,original_hash,envelope FROM labops_faileddelivery
                WHERE source_cluster=%s AND source_generation=%s AND envelope->>'event_id'=%s''',
                (scope['source_cluster'], scope['source_generation'], future_ids[0])).fetchone()
            assert row is not None and row[1:3] == ('DEAD', 'missing_business_event'), 'Future broker event was not quarantined'
            assert row[6] == h.api.canonical_payload_hash(value) and row[7] == value
            assert database.execute('SELECT COUNT(*) FROM labops_outboxevent WHERE id=%s', (future.id,)).fetchone()[0] == 0
            assert database.execute('SELECT COUNT(*) FROM labops_stockmovement WHERE id=%s', (future.aggregate_id,)).fetchone()[0] == 0
            assert database.execute('SELECT COUNT(*) FROM labops_processedevent WHERE event_id=%s', (future.id,)).fetchone()[0] == 0
            quarantine = {'id': str(row[0]), 'status': row[1], 'error_code': row[2],
                          'source_cluster': row[3], 'source_generation': row[4],
                          'delivery_key': row[5], 'original_hash': row[6], 'event_id': future_ids[0]}
        restored_after = self._fingerprints(restored_url)
        assert restored_before == restored_after, 'Future broker delta invented or changed restored business truth'
        self._error('older_snapshot_future_event_quarantine', **quarantine)
        self._drain_main(future_ids)
        assert not h.snapshot(future_ids)['mismatches']
        return {'name': CASE_NAMES[1], 'passed': policy_cleanup, 'topic': scope['topic'],
                'source_identity': scope, 'retention_configuration': {'retention.ms': '1000', 'segment.bytes': '1048576'},
                'policy_cleanup_observed': policy_cleanup, 'exhaustion_method': method,
                'retention_wait_seconds': policy_wait_seconds,
                'copied_original_event_ids': copied_ids, 'copied_original_records': records,
                'segment_filler_records': filler_records, 'initial_watermarks': initial,
                'watermark_samples': samples, 'exhausted_watermarks': exhausted,
                'parked_group': cursor_group, 'parked_committed_offset': parked_cursor,
                'delete_records_simulation': fallback, 'remaining_filler_cleanup': filler_cleanup,
                'legal_ledger_rebuild': {'before': baseline, 'after': rebuilt, 'mismatches': 0,
                                         'business_ledger_fingerprints': legal_before, 'exit_code': rebuild.returncode},
                'older_snapshot': watermark, 'future_event_id': future_ids[0],
                'future_broker_record': future_record, 'future_workload': future_workload,
                'future_worker': future_worker, 'future_quarantine': quarantine,
                'restored_business_tables_before': restored_before,
                'restored_business_tables_after': restored_after,
                'fabricated_future_business_effects': 0, 'limits': limits}

    def recreate(self):
        h = self.h
        first = _scope(self.run_id, 'recreate', '1')
        second = _scope(self.run_id, 'recreate', '2')
        topic = first['topic']
        self._create_topic(topic)
        original = h.models.OutboxEvent.objects.get(id=h.events[-1]['event_id'])
        value = h.api.envelope(original)
        before_effects = h.snapshot([str(original.id)])
        assert before_effects['dedupe_count'] == 2 and not before_effects['mismatches']
        first_poison = json.loads(json.dumps(value))
        first_poison['schema_version'] = 999
        first_record = self._publish(topic, first_poison, key='generation-one')
        assert first_record['offset'] == 0
        first_worker = self._consume(first, 'analytics', 1)
        first_row = h.models.FailedDelivery.objects.get(consumer_name='analytics',
            source_cluster=first['source_cluster'], source_generation=first['source_generation'],
            envelope__event_id=str(original.id))
        preserved = self._failure_rows([first_row.id])
        assert first_row.status == 'DEAD'
        self._assert_topic(topic)
        self.admin.delete_topics([topic], request_timeout=20, operation_timeout=15)[topic].result(timeout=25)
        h.wait(lambda: topic not in self.admin.list_topics(timeout=5).topics,
               'Deleted sacrificial topic remains in metadata', timeout=60)
        self._create_topic(topic)
        assert self._watermarks(topic) == {'low': 0, 'high': 0}, 'Recreated topic did not start with fresh offsets'
        second_poison = json.loads(json.dumps(value))
        second_poison['event_type'] = 'inventory.unknown.posted'
        second_record = self._publish(topic, second_poison, key='generation-two')
        assert second_record['offset'] == 0
        second_worker = self._consume(second, 'analytics', 1)
        second_row = h.models.FailedDelivery.objects.get(consumer_name='analytics',
            source_cluster=second['source_cluster'], source_generation=second['source_generation'],
            envelope__event_id=str(original.id))
        assert second_row.status == 'DEAD' and second_row.id != first_row.id
        assert preserved == self._failure_rows([first_row.id]), 'New source generation overwrote original failure evidence'
        assert first_row.original_hash != second_row.original_hash
        assert first_row.original_hash == h.api.canonical_payload_hash(first_poison)
        assert second_row.original_hash == h.api.canonical_payload_hash(second_poison)
        assert first_row.envelope['event_id'] == second_row.envelope['event_id'] == str(original.id)
        assert first_row.delivery_key.rsplit(':', 3)[-3:] == second_row.delivery_key.rsplit(':', 3)[-3:]
        assert not h.models.DeliveryAudit.objects.filter(delivery_id__in=[first_row.id, second_row.id],
                                                        action='SOURCE_CONFLICT').exists()
        duplicate_records = [self._publish(topic, value, key='original-id-duplicate') for _ in range(2)]
        assert [row['offset'] for row in duplicate_records] == [1, 2]
        # This fresh group starts at the two duplicate records. Its preceding
        # poison coordinate has already been tested by the analytics worker.
        notification_group = second['group_prefix'] + '.notification.v1'
        assert self._cursor(topic, notification_group, offset=1) == 1
        analytics_duplicates = self._consume(second, 'analytics', 2)
        notification_duplicates = self._consume(second, 'notification', 2)
        assert analytics_duplicates['committed_offset'] == notification_duplicates['committed_offset'] == 3
        after_effects = h.snapshot([str(original.id)])
        assert all(before_effects[key] == after_effects[key] for key in EFFECT_KEYS), 'Topic recreation duplicates changed business effects'
        original.refresh_from_db()
        assert h.api.envelope(original) == value and original.payload_hash == h.api.canonical_payload_hash(value)
        return {'name': CASE_NAMES[2], 'passed': True, 'topic': topic, 'original_event_id': str(original.id),
                'source_generations': [first['source_generation'], second['source_generation']],
                'same_broker_coordinates': {'topic': topic, 'partition': 0, 'offset': 0},
                'first_generation_record': first_record, 'second_generation_record': second_record,
                'first_worker': first_worker, 'second_worker': second_worker,
                'distinct_failed_delivery_ids': [str(first_row.id), str(second_row.id)],
                'retained_original_hashes': [first_row.original_hash, second_row.original_hash],
                'failed_rows_after_recreation': self._failure_rows([first_row.id, second_row.id]),
                'source_coordinate_collisions': 0, 'original_failure_modified': False,
                'same_id_duplicate_broker_records': duplicate_records,
                'analytics_duplicate_worker': analytics_duplicates,
                'notification_duplicate_worker': notification_duplicates,
                'notification_duplicate_start_offset': 1,
                'before': before_effects, 'after': after_effects, 'duplicate_database_effects': 0}

    def run(self):
        h = self.h
        h.drained([item['event_id'] for item in h.events], timeout=h.args.drain_timeout)
        self._pause_main()
        try:
            for name, exercise in zip(CASE_NAMES, (self.restart, self.retention, self.recreate)):
                began = time.monotonic()
                print(json.dumps({'recovery_case': name, 'state': 'started'}), flush=True)
                try:
                    case = exercise()
                except Exception as exc:
                    case = {'name': name, 'passed': False, 'error_type': type(exc).__name__,
                            'error_code': self.h.api.safe_error(exc),
                            'last_kafka_operation': self.last_kafka_operation,
                            **_kafka_diagnostics(exc),
                            'elapsed_seconds': time.monotonic() - began}
                    # Assertion text is authored by this validation module and
                    # contains no connection strings, client config or payload.
                    if isinstance(exc, AssertionError):
                        case['failure'] = str(exc)
                    self.cases.append(case)
                    self._error('recovery_matrix_failure', **case)
                    self._report()
                    break
                self.cases.append(case)
                self._report()
                print(json.dumps({'recovery_case': name, 'state': 'passed' if case['passed'] else 'failed'}), flush=True)
        finally:
            # Validation failures retain topics/backup/failed rows for diagnosis.
            # The owning workflow performs exact-project volume cleanup later.
            self._resume_main()
        return self.cases


def run_recovery_matrix(harness):
    """Return actual case evidence; the caller must fail on any false case."""
    return _RecoveryMatrix(harness).run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true', required=True)
    parser.add_argument('--run-id', required=True)
    options = parser.parse_args()
    print(json.dumps(describe_plan(options.run_id), indent=2, sort_keys=True))
