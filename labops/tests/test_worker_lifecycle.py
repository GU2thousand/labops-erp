import base64
import socket
import signal
import time
from contextlib import contextmanager, nullcontext, ExitStack
from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from unittest import skipUnless
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from django.core.management import call_command
from django.core.exceptions import ImproperlyConfigured
from django.db import OperationalError, connection
from django.test import SimpleTestCase, TransactionTestCase, override_settings

from labops.worker_metrics import (start_worker_metrics, stop_worker_metrics, StopController,
                                   operation_deadline, OperationDeadlineExceeded)
from labops.publisher_shards import ShardOwnershipLost


class Message:
    def __init__(self, value=b'{"schema_version":999}', offset=1):
        self.raw = value
        self.number = offset

    def value(self): return self.raw
    def error(self): return None
    def topic(self): return 'inventory'
    def partition(self): return 0
    def offset(self): return self.number


@override_settings(WORKER_METRICS_ENABLED=False, KAFKA_REQUIRE_SECURITY=False,
                   KAFKA_SECURITY_PROTOCOL='PLAINTEXT', KAFKA_SOURCE_CLUSTER_ID='cluster-test',
                   KAFKA_SOURCE_STREAM_GENERATION='3')
class WorkerLifecycleTests(SimpleTestCase):
    def run_consumer(self, client, deliver, **options):
        with patch('labops.management.commands.consume_kafka.Consumer', return_value=client), \
             patch('labops.management.commands.consume_kafka.deliver', deliver), \
             patch('labops.management.commands.consume_kafka.database_processing_budget', return_value=nullcontext()):
            call_command('consume_kafka', 'analytics', max_messages=1, stdout=StringIO(), **options)

    def test_effect_or_durable_parking_precedes_commit(self):
        for outcome in (True, False):
            calls = []
            client = MagicMock()
            client.poll.return_value = Message()
            client.commit.side_effect = lambda **kwargs: calls.append('offset_commit') or []
            deliver = MagicMock(side_effect=lambda *args, **kwargs: calls.append('durable_outcome') or outcome)
            self.run_consumer(client, deliver)
            self.assertEqual(calls, ['durable_outcome', 'offset_commit'])
            self.assertEqual(deliver.call_args.args[2], 'cluster-test:3:inventory:0:1')
            self.assertEqual(deliver.call_args.kwargs, {'source_cluster': 'cluster-test', 'source_generation': '3'})
            client.commit.assert_called_once_with(message=client.poll.return_value, asynchronous=False)

    def test_database_failure_never_commits_and_closes_client(self):
        client = MagicMock()
        client.poll.return_value = Message()
        with self.assertRaises(OperationalError):
            self.run_consumer(client, MagicMock(side_effect=OperationalError('database unavailable')))
        client.commit.assert_not_called()
        client.close.assert_called_once()

    def test_commit_failure_stops_before_next_delivery(self):
        client = MagicMock()
        client.poll.return_value = Message()
        client.commit.side_effect = RuntimeError('commit failed')
        with self.assertRaises(RuntimeError):
            self.run_consumer(client, MagicMock(return_value=True))
        client.poll.assert_called_once()
        client.close.assert_called_once()

    def test_nonfinite_surrogate_and_nested_poison_keep_raw_bytes(self):
        payloads = [b'{"delta":NaN}', b'{"delta":1e400}', b'{"title":"\\ud800"}',
                    b'{"title":"\\u0000"}', b'{"nested":[{"\\u0000":"value"}]}',
                    b'[' * 1500 + b']' * 1500]
        for raw in payloads:
            with self.subTest(raw=raw[:30]):
                client = MagicMock()
                client.poll.return_value = Message(raw)
                client.commit.return_value = []
                deliver = MagicMock(return_value=False)
                self.run_consumer(client, deliver)
                self.assertEqual(deliver.call_args.args[1], {'invalid_payload_base64': base64.b64encode(raw).decode()})
                client.commit.assert_called_once()

    def test_lost_partition_does_not_commit_completed_effect(self):
        client = MagicMock()
        client.poll.return_value = Message()
        callbacks = {}
        client.subscribe.side_effect = lambda topics, **kwargs: callbacks.update(kwargs)
        def deliver(*args, **kwargs):
            callbacks['on_lost'](client, [SimpleNamespace(topic='inventory', partition=0)])
            return True
        with self.assertRaisesMessage(Exception, 'ownership lost'):
            self.run_consumer(client, MagicMock(side_effect=deliver))
        client.commit.assert_not_called()

    def test_database_transaction_commit_precedes_offset(self):
        calls = []
        @contextmanager
        def database_budget():
            yield
            calls.append('database_commit')
        client = MagicMock()
        client.poll.return_value = Message()
        client.commit.side_effect = lambda **kwargs: calls.append('offset_commit') or []
        with patch('labops.management.commands.consume_kafka.Consumer', return_value=client), \
             patch('labops.management.commands.consume_kafka.deliver', return_value=False), \
             patch('labops.management.commands.consume_kafka.database_processing_budget', database_budget):
            call_command('consume_kafka', 'analytics', max_messages=1, stdout=StringIO())
        self.assertEqual(calls, ['database_commit', 'offset_commit'])

    def test_publisher_runs_only_outbox_and_honors_shard(self):
        broker = MagicMock()
        broker.flush.return_value = 0
        shard_owner = MagicMock(spec=['assert_owned'])
        with patch('labops.management.commands.publish_events.producer', return_value=broker), \
             patch('labops.management.commands.publish_events.publish_one', return_value=False) as publish, \
             patch('labops.management.commands.publish_events.publisher_shard_owner', return_value=nullcontext(shard_owner)) as owner, \
             patch('labops.management.commands.publish_events.database_statement_budget', return_value=nullcontext()), \
             patch('labops.events.retry_deliveries') as retry, patch('labops.events.publish_dlq') as dlq:
            call_command('publish_events', limit=5, shard_index=1, shard_count=3)
        publish.assert_called_once_with(broker, shard_index=1, shard_count=3, ownership_check=shard_owner.assert_owned)
        owner.assert_called_once_with(1, 3)
        retry.assert_not_called()
        dlq.assert_not_called()
        broker.flush.assert_called_once_with(12)

    @contextmanager
    def publisher_loop(self):
        module = 'labops.management.commands.publish_events'
        stop = StopController()
        stop.wait = MagicMock(side_effect=lambda seconds: stop.request())
        broker = MagicMock()
        broker.flush.return_value = 0
        owner = MagicMock(spec=['assert_owned'])
        with ExitStack() as patches:
            patches.enter_context(patch(module + '.StopController', return_value=stop))
            patches.enter_context(patch(module + '.producer', return_value=broker))
            ownership = patches.enter_context(patch(module + '.publisher_shard_owner', return_value=nullcontext(owner)))
            patches.enter_context(patch(module + '.database_statement_budget', return_value=nullcontext()))
            publish = patches.enter_context(patch(module + '.publish_one'))
            yield SimpleNamespace(stop=stop, broker=broker, owner=owner,
                                  ownership=ownership, publish=publish)

    def test_publisher_continues_after_full_success_and_waits_when_idle(self):
        with self.publisher_loop() as loop:
            loop.publish.side_effect = [True, True, False]
            call_command('publish_events', loop=True, limit=2, stdout=StringIO())
        self.assertEqual(loop.publish.call_count, 3)
        loop.stop.wait.assert_called_once_with(1)
        loop.ownership.assert_called_once_with(0, 1)
        self.assertEqual(loop.owner.assert_owned.call_count, 3)
        loop.broker.flush.assert_called_once()

    def test_publisher_partial_and_failed_batches_retain_wait(self):
        for outcome in (False, RuntimeError('publication failed')):
            with self.subTest(outcome=type(outcome).__name__), self.publisher_loop() as loop:
                loop.publish.side_effect = [True, outcome]
                if isinstance(outcome, Exception):
                    with self.assertLogs('labops', level='ERROR'):
                        call_command('publish_events', loop=True, limit=3, stdout=StringIO())
                else:
                    call_command('publish_events', loop=True, limit=3, stdout=StringIO())
            self.assertEqual(loop.publish.call_count, 2)
            loop.stop.wait.assert_called_once_with(1)
            loop.broker.flush.assert_called_once()

    def test_publisher_database_context_failure_after_success_does_not_skip_wait(self):
        @contextmanager
        def failed_commit_budget(*args):
            yield
            raise OperationalError('database context failed after publication')
        with self.publisher_loop() as loop, \
             patch('labops.management.commands.publish_events.database_statement_budget', failed_commit_budget), \
             patch('labops.management.commands.publish_events.connections') as database:
            loop.publish.return_value = True
            with self.assertLogs('labops', level='ERROR'):
                call_command('publish_events', loop=True, limit=1, stdout=StringIO())
        loop.publish.assert_called_once()
        loop.stop.wait.assert_called_once_with(1)
        database['default'].close.assert_called_once()

    def test_publisher_owner_loss_after_full_batch_purges_without_wait_or_flush(self):
        with self.publisher_loop() as loop:
            loop.publish.return_value = True
            loop.owner.assert_owned.side_effect = [None, None, ShardOwnershipLost('owner session lost')]
            with self.assertRaises(ShardOwnershipLost):
                call_command('publish_events', loop=True, limit=2, stdout=StringIO())
        self.assertEqual(loop.publish.call_count, 2)
        loop.stop.wait.assert_not_called()
        loop.ownership.assert_called_once_with(0, 1)
        loop.broker.purge.assert_called_once_with(in_queue=True, in_flight=True, blocking=False)
        loop.broker.poll.assert_called_once_with(0)
        loop.broker.flush.assert_not_called()

    def test_publisher_stop_during_last_success_exits_without_new_batch(self):
        with self.publisher_loop() as loop:
            calls = []
            def publish(*args, **kwargs):
                calls.append('acknowledged')
                if len(calls) == 2:
                    loop.stop.request()
                return True
            loop.publish.side_effect = publish
            call_command('publish_events', loop=True, limit=2, stdout=StringIO())
        self.assertEqual(calls, ['acknowledged', 'acknowledged'])
        self.assertEqual(loop.owner.assert_owned.call_count, 2)
        loop.stop.wait.assert_not_called()
        loop.broker.flush.assert_called_once()

    def test_dlq_poison_does_not_prevent_next_row(self):
        broker = MagicMock()
        broker.flush.return_value = 0
        with patch('labops.management.commands.publish_dlq.producer', return_value=broker) as producer, \
             patch('labops.management.commands.publish_dlq.database_statement_budget', return_value=nullcontext()), \
             patch('labops.management.commands.publish_dlq.publish_dlq', side_effect=[0, 1]) as publish:
            call_command('publish_dlq', limit=2)
        producer.assert_called_once_with(role='dlq')
        self.assertEqual(publish.call_count, 2)

    def test_retry_is_independent_of_broker(self):
        with patch('labops.management.commands.retry_events.retry_deliveries', side_effect=[1, 0]) as retry, \
             patch('labops.management.commands.retry_events.database_processing_budget', return_value=nullcontext()), \
             patch('labops.events.producer') as producer:
            call_command('retry_events', limit=2, stdout=StringIO())
        self.assertEqual(retry.call_count, 2)
        producer.assert_not_called()

    def test_database_errors_close_app_connection_and_retry_next_loop(self):
        cases = [('publish_events', 'publish_one', 'database_statement_budget'),
                 ('retry_events', 'retry_deliveries', 'database_processing_budget'),
                 ('publish_dlq', 'publish_dlq', 'database_statement_budget')]
        for command, operation, budget in cases:
            with self.subTest(command=command), ExitStack() as patches:
                module = 'labops.management.commands.' + command
                stop = StopController()
                stop.wait = MagicMock()
                broker = MagicMock()
                broker.flush.return_value = 0
                calls = []
                def process(*args, **kwargs):
                    calls.append('attempt')
                    if len(calls) == 1:
                        raise OperationalError('connection is closed')
                    stop.request()
                    return 1
                patches.enter_context(patch(module + '.StopController', return_value=stop))
                patches.enter_context(patch(module + '.' + budget, return_value=nullcontext()))
                processed = patches.enter_context(patch(module + '.' + operation, side_effect=process))
                database = patches.enter_context(patch(module + '.connections'))
                if command != 'retry_events':
                    patches.enter_context(patch(module + '.producer', return_value=broker))
                if command == 'publish_events':
                    owner = MagicMock(spec=['assert_owned'])
                    ownership = patches.enter_context(patch(module + '.publisher_shard_owner', return_value=nullcontext(owner)))
                call_command(command, loop=True, limit=1, stdout=StringIO())
                self.assertEqual(processed.call_count, 2)
                database['default'].close.assert_called_once()
                if command == 'publish_events':
                    ownership.assert_called_once_with(0, 1)
                    self.assertEqual(owner.assert_owned.call_count, 2)

    def test_shard_session_loss_is_checked_before_stale_application_connection(self):
        owner = MagicMock(spec=['assert_owned'])
        owner.assert_owned.side_effect = ShardOwnershipLost('owner backend lost')
        broker = MagicMock()
        with patch('labops.management.commands.publish_events.producer', return_value=broker), \
             patch('labops.management.commands.publish_events.publisher_shard_owner', return_value=nullcontext(owner)) as ownership, \
             patch('labops.management.commands.publish_events.database_statement_budget') as database_budget, \
             patch('labops.management.commands.publish_events.publish_one') as publish, \
             patch('labops.management.commands.publish_events.connections') as database:
            with self.assertRaises(ShardOwnershipLost):
                call_command('publish_events', loop=True, limit=1)
        ownership.assert_called_once_with(0, 1)
        database_budget.assert_not_called()
        database['default'].close.assert_not_called()
        publish.assert_not_called()
        broker.flush.assert_not_called()

    def test_signal_stops_after_current_durable_delivery(self):
        stop = StopController()
        client = MagicMock()
        client.poll.return_value = Message()
        client.commit.return_value = []
        def deliver(*args, **kwargs):
            stop.request(signal.SIGTERM, None)
            return True
        with patch('labops.management.commands.consume_kafka.StopController', return_value=stop):
            self.run_consumer(client, MagicMock(side_effect=deliver))
        client.poll.assert_called_once()
        client.commit.assert_called_once()
        self.assertTrue(stop.stopped)
        self.assertLessEqual(stop.remaining(), 45)

    def test_signal_during_poll_does_not_start_another_delivery(self):
        stop = StopController()
        client = MagicMock()
        def poll(*args):
            stop.request(signal.SIGTERM, None)
            return Message()
        client.poll.side_effect = poll
        deliver = MagicMock()
        with patch('labops.management.commands.consume_kafka.StopController', return_value=stop):
            self.run_consumer(client, deliver)
        deliver.assert_not_called()
        client.commit.assert_not_called()

    def test_operation_deadline_aborts_broad_business_exception_catch(self):
        caught_business_error = False
        with self.assertRaises(OperationDeadlineExceeded):
            with operation_deadline(.01):
                try:
                    time.sleep(.1)
                except Exception:
                    caught_business_error = True
        self.assertFalse(caught_business_error)

    @override_settings(EVENT_RETRY_STATEMENT_TIMEOUT_MS=0)
    def test_retry_worker_validates_its_timeouts(self):
        with self.assertRaises(ImproperlyConfigured):
            call_command('retry_events', limit=1, stdout=StringIO())

    def test_missing_metrics_secret_fails_closed(self):
        with override_settings(WORKER_METRICS_ENABLED=True, WORKER_METRICS_TOKEN='', WORKER_METRICS_TOKEN_FILE=''):
            with self.assertRaises(ImproperlyConfigured):
                start_worker_metrics('publisher')

    def test_metrics_endpoint_requires_token(self):
        with socket.socket() as socket_probe:
            socket_probe.bind(('127.0.0.1', 0))
            port = socket_probe.getsockname()[1]
        with override_settings(WORKER_METRICS_ENABLED=True, WORKER_METRICS_HOST='127.0.0.1',
                               WORKER_METRICS_PORT=port, WORKER_METRICS_TOKEN='metrics-test-secret'):
            server = start_worker_metrics('publisher')
            try:
                url = f'http://127.0.0.1:{port}/metrics'
                with self.assertRaises(HTTPError) as response:
                    urlopen(url, timeout=5)
                self.assertEqual(response.exception.code, 403)
                with self.assertRaises(HTTPError) as nonascii:
                    urlopen(Request(url, headers={'Authorization': 'Bearer malformed-é'}), timeout=5)
                self.assertEqual(nonascii.exception.code, 403)
                with urlopen(Request(url, headers={'Authorization': 'Bearer metrics-test-secret'}), timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn(b'labops_worker_heartbeat_timestamp_seconds', response.read())
            finally:
                stop_worker_metrics(server)


@skipUnless(connection.vendor == 'postgresql', 'Real PostgreSQL worker connection recovery')
@override_settings(WORKER_METRICS_ENABLED=False, KAFKA_REQUIRE_SECURITY=False,
                   KAFKA_SECURITY_PROTOCOL='PLAINTEXT')
class WorkerPostgreSQLReconnectTests(TransactionTestCase):
    def test_workers_reconnect_after_application_backend_termination(self):
        for command, operation in [('publish_events', 'publish_one'),
                                   ('retry_events', 'retry_deliveries'),
                                   ('publish_dlq', 'publish_dlq')]:
            with self.subTest(command=command), ExitStack() as patches:
                module = 'labops.management.commands.' + command
                stop = StopController()
                stop.wait = MagicMock()
                broker = MagicMock()
                broker.flush.return_value = 0
                attempts = []
                def process(*args, **kwargs):
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT pg_backend_pid()')
                        attempts.append(cursor.fetchone()[0])
                        if len(attempts) == 1:
                            # Kill only this test's application backend. The
                            # dedicated publisher owner must remain alive.
                            cursor.execute('SELECT pg_terminate_backend(pg_backend_pid())')
                        else:
                            cursor.execute('SELECT 1')
                            self.assertEqual(cursor.fetchone()[0], 1)
                    if command == 'publish_events':
                        kwargs['ownership_check']()
                    stop.request()
                    return 1
                patches.enter_context(patch(module + '.StopController', return_value=stop))
                patches.enter_context(patch(module + '.' + operation, side_effect=process))
                if command != 'retry_events':
                    patches.enter_context(patch(module + '.producer', return_value=broker))
                call_command(command, loop=True, limit=1, stdout=StringIO())
                self.assertEqual(len(attempts), 2)
                self.assertNotEqual(attempts[0], attempts[1])
