import gc
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import weakref

from confluent_kafka import ConsumerGroupState, KafkaError, KafkaException
from django.test import SimpleTestCase
from benchmarks.events.acceptance import Harness


class GroupAssignmentObserverTests(SimpleTestCase):
    def description(self, *, state=ConsumerGroupState.STABLE, client_id='acceptance-123', partitions=(0, 1, 2)):
        return SimpleNamespace(state=state, members=[SimpleNamespace(client_id=client_id,
            assignment=SimpleNamespace(topic_partitions=[SimpleNamespace(topic='run.inventory.v1', partition=n)
                                                       for n in partitions]))])

    def probe(self, description=None, *, error=None, expected='acceptance-123', probe_timeout=10):
        harness = Harness.__new__(Harness)
        harness.configs = {'admin': {'bootstrap.servers': 'unused.test:9092'}}
        harness.consumer_group = lambda name: 'run.' + name + '.v1'
        harness.settings = SimpleNamespace(KAFKA_TOPIC='run.inventory.v1')
        owner = self
        self.native_calls = []
        self.live_during_completion = False

        class Future:
            def __init__(self, client):
                self.client = weakref.ref(client)

            def result(self, timeout):
                gc.collect()
                owner.live_during_completion = self.client() is not None
                owner.assertTrue(owner.live_during_completion,
                                 'Asynchronous admin request lost its native client')
                owner.assertEqual(timeout, probe_timeout)
                if error:
                    raise error
                return description or owner.description()

        class NativeClient:
            def __init__(self, config):
                owner.assertEqual(config, harness.configs['admin'])

            def describe_consumer_groups(self, groups, **kwargs):
                owner.native_calls.append((groups, kwargs))
                return {groups[0]: Future(self)}

        with TemporaryDirectory() as directory:
            harness.evidence = Path(directory)
            try:
                with patch('confluent_kafka.admin.AdminClient', NativeClient):
                    return harness.group_assignment('notification', client_id=expected, timeout=probe_timeout)
            finally:
                records = (harness.evidence / 'group-assignment-observations.jsonl').read_text().splitlines()
                self.observations = [json.loads(value) for value in records]

    def test_native_client_remains_alive_until_asynchronous_completion(self):
        result = self.probe()
        self.assertTrue(self.live_during_completion)
        self.assertEqual(self.native_calls, [(['run.notification.v1'], {'request_timeout': 8})])
        self.assertEqual(result['member_count'], 1)
        self.assertEqual(result['client_id'], 'acceptance-123')
        self.assertEqual(self.observations[0]['member_count'], 1)
        self.assertEqual(len(self.observations[0]['members'][0]['assignments']), 3)

    def test_wrong_client_or_unstable_or_partial_assignment_remains_unready(self):
        for description in (self.description(client_id='acceptance-999'),
                            self.description(state=ConsumerGroupState.PREPARING_REBALANCING),
                            self.description(partitions=(0, 1))):
            with self.subTest(description=description):
                self.assertFalse(self.probe(description))
                self.assertTrue(self.live_during_completion)

    def test_protocol_failure_is_not_suppressed_and_safe_code_is_retained(self):
        failure = KafkaException(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED, 'private-password'))
        with self.assertRaises(KafkaException):
            self.probe(error=failure)
        self.assertEqual(self.observations[0]['kafka_error_code'], KafkaError.GROUP_AUTHORIZATION_FAILED)
        self.assertNotIn('private-password', json.dumps(self.observations))

    def test_python_future_timeout_stays_a_failed_observation(self):
        with self.assertRaises(TimeoutError):
            self.probe(error=TimeoutError('private diagnostic'))
        self.assertEqual(self.observations[0]['error_type'], 'TimeoutError')
        self.assertNotIn('private diagnostic', json.dumps(self.observations))

    def test_last_probe_shares_the_remaining_native_and_future_budget(self):
        self.assertTrue(self.probe(probe_timeout=5))
        self.assertEqual(self.native_calls, [(['run.notification.v1'], {'request_timeout': 4})])
        self.assertEqual(self.observations[0]['future_timeout_seconds'], 5)

    def test_ready_observation_after_assignment_deadline_cannot_pass(self):
        harness = Harness.__new__(Harness)
        clock = [100.0]
        calls = []
        def query(name, **kwargs):
            calls.append((name, kwargs))
            clock[0] += 2.001
            return {'client_id': 'acceptance-123', 'member_count': 1}
        harness.group_assignment = query
        with TemporaryDirectory() as directory:
            harness.evidence = Path(directory)
            with patch('benchmarks.events.acceptance.time.monotonic', lambda: clock[0]), \
                 self.assertRaises(AssertionError):
                harness.wait_group_assignment('notification', client_id='acceptance-123',
                    message='No assignment in window', timeout=2)
            evidence = json.loads((harness.evidence / 'group-assignment-observations.jsonl').read_text())
        self.assertTrue(evidence['late_ready_result'])
        self.assertEqual(evidence['budget_seconds'], 2)
        self.assertEqual(calls[0][1]['timeout'], 2)

    def test_ready_observation_within_assignment_window_returns_exact_result(self):
        harness = Harness.__new__(Harness)
        clock = [100.0]
        result = {'client_id': 'acceptance-123', 'member_count': 1}
        def query(name, **kwargs):
            clock[0] += .5
            return result
        harness.group_assignment = query
        with patch('benchmarks.events.acceptance.time.monotonic', lambda: clock[0]):
            self.assertIs(harness.wait_group_assignment('notification', client_id='acceptance-123',
                message='No assignment in window', timeout=2), result)
