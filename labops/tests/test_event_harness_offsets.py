from collections import deque
from types import SimpleNamespace
from django.test import SimpleTestCase
from confluent_kafka import KafkaError, KafkaException, OFFSET_INVALID
from benchmarks.events.acceptance import stable_committed_offsets


class OffsetSnapshotTests(SimpleTestCase):
    topic = 'isolated.inventory.v1'

    def parts(self, offset=OFFSET_INVALID):
        return [SimpleNamespace(topic=self.topic, partition=n, offset=offset, error=None)
                for n in range(3)]

    def read(self, responses, *, timeout=2):
        queued = deque(responses)
        self.created = []; self.now = 0; self.retries = []
        owner = self
        class Client:
            def __init__(self, config):
                self.config = config; self.closed = False; owner.created.append(self)
            def committed(self, partitions, timeout):
                owner.assertEqual([(p.topic, p.partition) for p in partitions],
                                  [(owner.topic, n) for n in range(3)])
                owner.assertGreater(timeout, 0)
                item = queued.popleft()
                if isinstance(item, Exception): raise item
                return item
            def close(self): self.closed = True
        def pause(seconds): self.now += seconds
        return stable_committed_offsets({'notification': {'group.id': 'notification'},
            'analytics': {'group.id': 'analytics'}}, self.topic, timeout=timeout,
            consumer_factory=Client, monotonic=lambda: self.now, sleep=pause,
            on_retry=lambda code, attempt: self.retries.append((code, attempt)))

    def test_coordinator_election_refreshes_then_requires_two_complete_equal_snapshots(self):
        transient = KafkaException(KafkaError(KafkaError.NOT_COORDINATOR))
        result = self.read([transient] + [self.parts() for _ in range(4)])
        self.assertEqual(result, {name: {str(n): OFFSET_INVALID for n in range(3)}
                                 for name in ('notification', 'analytics')})
        self.assertEqual(self.retries, [(KafkaError.NOT_COORDINATOR, 1)])
        self.assertEqual(len(self.created), 5)
        self.assertTrue(all(client.closed for client in self.created))

    def test_changing_offsets_need_another_matching_snapshot(self):
        result = self.read([self.parts(n) for n in (0, 0, 1, 1, 1, 1)])
        self.assertEqual(result['analytics'], {'0': 1, '1': 1, '2': 1})
        self.assertEqual(len(self.created), 6)

    def test_authorization_and_noncoordinator_errors_fail_without_retry(self):
        for code in (KafkaError.GROUP_AUTHORIZATION_FAILED, KafkaError._AUTHENTICATION,
                     KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._TIMED_OUT):
            with self.subTest(code=code), self.assertRaises(KafkaException):
                self.read([KafkaException(KafkaError(code))])
            self.assertEqual(self.retries, [])
            self.assertEqual(len(self.created), 1)
            self.assertTrue(self.created[0].closed)

    def test_partition_coordinator_error_is_retried_but_partial_response_is_rejected(self):
        values = self.parts(); values[1].error = KafkaError(KafkaError.COORDINATOR_LOAD_IN_PROGRESS)
        self.read([values] + [self.parts(5) for _ in range(4)])
        self.assertEqual(self.retries, [(KafkaError.COORDINATOR_LOAD_IN_PROGRESS, 1)])
        with self.assertRaises(AssertionError): self.read([self.parts()[:2]])
        self.assertTrue(self.created[0].closed)

    def test_persistent_coordinator_error_hits_frozen_deadline(self):
        values = [KafkaException(KafkaError(KafkaError.COORDINATOR_NOT_AVAILABLE)) for _ in range(4)]
        with self.assertRaises(TimeoutError): self.read(values, timeout=1)
        self.assertEqual(self.now, 1)
        self.assertEqual(len(self.created), 4)
        self.assertTrue(all(client.closed for client in self.created))

    def test_mixed_partition_errors_do_not_mask_authorization_failure(self):
        values = self.parts()
        values[0].error = KafkaError(KafkaError.NOT_COORDINATOR)
        values[1].error = KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED)
        with self.assertRaises(KafkaException) as caught: self.read([values])
        self.assertEqual(caught.exception.args[0].code(), KafkaError.GROUP_AUTHORIZATION_FAILED)
        self.assertEqual(self.retries, []); self.assertTrue(self.created[0].closed)

    def test_duplicate_partition_wrong_topic_and_other_negative_offsets_fail(self):
        duplicate = self.parts(); duplicate[2].partition = 1
        wrong_topic = self.parts(); wrong_topic[2].topic = 'another.topic'
        for values in (duplicate, wrong_topic, self.parts(-2)):
            with self.subTest(values=values), self.assertRaises(AssertionError): self.read([values])
