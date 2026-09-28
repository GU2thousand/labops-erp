import base64
import socket
import signal
import time
from contextlib import contextmanager, nullcontext
from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from django.core.management import call_command
from django.core.exceptions import ImproperlyConfigured
from django.db import OperationalError
from django.test import SimpleTestCase, override_settings

from labops.worker_metrics import (start_worker_metrics, stop_worker_metrics, StopController,
                                   operation_deadline, OperationDeadlineExceeded)


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
        with patch('labops.management.commands.publish_events.producer', return_value=broker), \
             patch('labops.management.commands.publish_events.publish_one', return_value=False) as publish, \
             patch('labops.management.commands.publish_events.database_statement_budget', return_value=nullcontext()), \
             patch('labops.events.retry_deliveries') as retry, patch('labops.events.publish_dlq') as dlq:
            call_command('publish_events', limit=5, shard_index=1, shard_count=3)
        publish.assert_called_once_with(broker, shard_index=1, shard_count=3)
        retry.assert_not_called()
        dlq.assert_not_called()
        broker.flush.assert_called_once_with(12)

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
