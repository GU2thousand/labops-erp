import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID

from django.test import SimpleTestCase, override_settings

from benchmarks.events.acceptance import Harness
from benchmarks.events.delivery_contract import qualify_delivery_observations
from benchmarks.events.health import completed_recovery_seconds
from labops.events import send


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds

    def completion(self, started, budget):
        # The health helper's default clock is bound at module import. Inject
        # this fake clock explicitly while retaining its real deadline check.
        return completed_recovery_seconds(started, budget, monotonic=self.monotonic)


class HarnessDuplicateOffsetTests(SimpleTestCase):
    ids = tuple(str(UUID(int=value)) for value in (1, 2))
    topic = 'isolated.inventory.v1'
    source = ('isolated-cluster', 'run-20260927')

    def harness(self, directory):
        harness = Harness.__new__(Harness)
        harness.evidence = Path(directory)
        (harness.evidence / 'logs').mkdir()
        harness.settings = SimpleNamespace(KAFKA_TOPIC=self.topic)
        harness.delivery_proofs = []
        harness.cases = []
        harness.connections = SimpleNamespace(close_all=Mock())
        return harness

    def snapshot(self, notification=5, analytics=5):
        return {'notification': {'0': notification, '1': 0, '2': -1001},
                'analytics': {'0': analytics, '1': 0, '2': -1001}}

    def coordinates(self):
        return {self.ids[0]: [{'partition': 0, 'offset': 3}, {'partition': 0, 'offset': 4}]}

    def observations(self, harness):
        path = harness.evidence / 'duplicate-offset-observations.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()]

    def test_late_successful_cursor_response_is_retained_and_cannot_pass(self):
        clock = FakeClock()
        calls = []
        with TemporaryDirectory() as directory:
            harness = self.harness(directory)
            def query(*, timeout):
                calls.append(timeout)
                clock.advance(1.001)
                return self.snapshot()
            harness.offsets = query
            with patch('benchmarks.events.acceptance.time.monotonic', clock.monotonic), \
                 patch('benchmarks.events.acceptance.time.sleep', clock.advance), \
                 patch('benchmarks.events.health.completed_recovery_seconds', clock.completion), \
                 self.assertRaises(TimeoutError):
                harness.wait_for_acknowledged_offsets(self.coordinates(), timeout=1)
            observations = self.observations(harness)
        self.assertEqual(calls, [1])
        self.assertEqual(len(observations), 1)
        self.assertTrue(observations[0]['passed'])
        self.assertEqual(observations[0]['raw_next_offsets'], self.snapshot())
        self.assertGreater(observations[0]['elapsed_seconds'], 1)

    def test_both_group_cursors_must_pass_ack_and_share_one_remaining_budget(self):
        clock = FakeClock()
        calls = []
        snapshots = [self.snapshot(analytics=4), self.snapshot()]
        with TemporaryDirectory() as directory:
            harness = self.harness(directory)
            def query(*, timeout):
                calls.append(timeout)
                clock.advance(.1)
                return snapshots[len(calls) - 1]
            harness.offsets = query
            with patch('benchmarks.events.acceptance.time.monotonic', clock.monotonic), \
                 patch('benchmarks.events.acceptance.time.sleep', clock.advance), \
                 patch('benchmarks.events.health.completed_recovery_seconds', clock.completion):
                proof = harness.wait_for_acknowledged_offsets(self.coordinates(), timeout=1)
            observations = self.observations(harness)
        self.assertTrue(proof['passed'])
        self.assertEqual([row['passed'] for row in observations], [False, True])
        self.assertEqual(observations[0]['missing'][0]['consumer'], 'analytics')
        self.assertEqual(observations[0]['missing'][0]['observed_next_offset'], 4)
        self.assertEqual(proof['required_next_offsets'], {'0': 5})
        self.assertEqual(calls[0], 1)
        self.assertAlmostEqual(calls[1], .75)
        self.assertLess(calls[1], calls[0])

    @override_settings(KAFKA_DLQ_TOPIC='isolated.inventory.dlq.v1',
                       KAFKA_MESSAGE_MAX_BYTES=1_048_576,
                       KAFKA_PRODUCER_QUEUE_WAIT_SECONDS=1,
                       KAFKA_PUBLISH_FLUSH_SECONDS=1)
    def test_duplicate_drill_chains_real_send_callbacks_and_durable_proof_before_snapshot(self):
        clock = FakeClock()
        timeline = []
        event_rows = [SimpleNamespace(id=UUID(event_id), aggregate_type='stock_movement',
                                     aggregate_id=UUID(int=100 + index))
                      for index, event_id in enumerate(self.ids)]
        initial = {self.ids[0]: (0, 40), self.ids[1]: (2, 100)}
        required = {event_id: [{'partition': partition, 'offset': offset + increment}
                              for increment in (1, 2)]
                    for event_id, (partition, offset) in initial.items()}
        owner = self
        with TemporaryDirectory() as directory:
            harness = self.harness(directory)

            def record_delivery(event_id, partition, offset):
                for consumer in ('notification', 'analytics'):
                    row = {'event_id': event_id, 'consumer': consumer,
                           'delivery_key': ':'.join((*self.source, self.topic,
                                                    str(partition), str(offset))),
                           'received_at': 1.0, 'completed_at': 2.0, 'result': True}
                    with (harness.evidence / 'logs' / f'{consumer}-deliveries.jsonl').open('a') as output:
                        output.write(json.dumps(row) + '\n')

            for event_id, (partition, offset) in initial.items():
                record_delivery(event_id, partition, offset)

            class Producer:
                last_security_error = None

                def __init__(self):
                    self.pending = []
                    self.sent = []
                    self.next_offsets = {event_id: offset + 1
                                         for event_id, (_partition, offset) in initial.items()}

                def produce(self, topic, *, key, value, on_delivery):
                    owner.assertEqual(topic, owner.topic)
                    event_id = json.loads(value)['event_id']
                    partition = initial[event_id][0]
                    offset = self.next_offsets[event_id]
                    self.next_offsets[event_id] += 1
                    message = SimpleNamespace(topic=lambda: topic,
                                              partition=lambda: partition, offset=lambda: offset)
                    self.sent.append((event_id, partition, offset))
                    self.pending.append((on_delivery, message, event_id, partition, offset))

                def flush(self, timeout):
                    owner.assertEqual(timeout, 1)
                    clock.advance(.1)
                    while self.pending:
                        callback, message, event_id, partition, offset = self.pending.pop(0)
                        callback(None, message)
                        record_delivery(event_id, partition, offset)
                    return 0

            producer = Producer()
            harness.api = SimpleNamespace(producer=lambda: producer, source_identity=lambda: self.source,
                                          send=send, envelope=lambda event: {'event_id': str(event.id)})
            def filter_events(**kwargs):
                self.assertEqual(kwargs, {'id__in': list(self.ids)})
                return event_rows
            harness.models = SimpleNamespace(OutboxEvent=SimpleNamespace(
                objects=SimpleNamespace(filter=filter_events)))
            baseline = {'notification_count': 2, 'notification_hash': 'notification-hash',
                        'dedupe_count': 4, 'dedupe_hash': 'dedupe-hash', 'projection_hash': 'projection-hash'}
            def business_snapshot(ids):
                self.assertEqual(ids, list(self.ids))
                timeline.append('snapshot-before' if not timeline else 'snapshot-after')
                if timeline[-1] == 'snapshot-after':
                    self.assertIn('durable-cursor-proof', timeline)
                    self.assertLess(timeline.index('durable-cursor-proof'), len(timeline) - 1)
                return dict(baseline)
            harness.snapshot = business_snapshot
            cursor_calls = []
            def offsets(*, timeout):
                cursor_calls.append(timeout)
                clock.advance(.1)
                complete = len(cursor_calls) == 2
                return {consumer: {'0': 43, '1': 0,
                                   '2': 103 if complete or consumer == 'notification' else 102}
                        for consumer in ('notification', 'analytics')}
            harness.offsets = offsets
            real_cursor_wait = harness.wait_for_acknowledged_offsets
            def cursor_wait(coordinates, timeout):
                timeline.append('cursor-wait')
                self.assertEqual(coordinates, required)
                self.assertLess(timeout, 180)
                result = real_cursor_wait(coordinates, timeout)
                timeline.append('durable-cursor-proof')
                return result
            harness.wait_for_acknowledged_offsets = cursor_wait
            def qualify(*args, **kwargs):
                timeline.append('receipt-proof')
                self.assertEqual(kwargs['required_coordinates'], required)
                clock.advance(.5)
                return qualify_delivery_observations(*args, **kwargs)

            with patch('benchmarks.events.acceptance.time.monotonic', clock.monotonic), \
                 patch('benchmarks.events.acceptance.time.sleep', clock.advance), \
                 patch('benchmarks.events.health.completed_recovery_seconds', clock.completion), \
                 patch('benchmarks.events.delivery_contract.qualify_delivery_observations', qualify):
                harness.duplicate_drill(list(self.ids))
            publications = [json.loads(line) for line in (
                harness.evidence / 'duplicate-publications.jsonl').read_text().splitlines()]
            receipt = json.loads((harness.evidence / 'delivery-proof-000.json').read_text())
            cursor_observations = self.observations(harness)
            requests = json.loads((harness.evidence / 'duplicate-requests.json').read_text())

        self.assertEqual(len(producer.sent), 4)
        self.assertEqual([(record['event_id'], record['partition'], record['offset'])
                          for record in publications], producer.sent)
        self.assertTrue(all(record['source_cluster'] == self.source[0]
                            and record['source_generation'] == self.source[1] for record in publications))
        self.assertEqual(requests['requested_new_broker_records'], 4)
        self.assertEqual(receipt['expected_acknowledged_coordinate_observations'], 8)
        self.assertEqual(receipt['observed_acknowledged_coordinate_observations'], 8)
        self.assertEqual([row['passed'] for row in cursor_observations], [False, True])
        self.assertEqual(timeline, ['snapshot-before', 'receipt-proof', 'cursor-wait',
                                    'durable-cursor-proof', 'snapshot-after'])
        self.assertEqual(harness.cases[0]['acknowledged_coordinate_commit_coverage']['required_next_offsets'],
                         {'0': 43, '2': 103})
        self.assertTrue(harness.cases[0]['passed'])
        self.assertEqual(harness.cases[0]['extra_database_effects'], 0)
        harness.connections.close_all.assert_not_called()
