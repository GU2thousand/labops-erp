"""Exercise the real fault methods without Docker, Kafka, or a database."""
from contextlib import contextmanager
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase
from benchmarks.events.acceptance import Harness, write_json
from benchmarks.events.health import completed_recovery_seconds
from benchmarks.events.workload_contract import qualify_fault_workload


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def monotonic(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class CountQuery:
    def __init__(self, count):
        self.get_count = count

    def count(self):
        return self.get_count()


class FaultRecoveryBudgetTests(SimpleTestCase):
    def make_harness(self, evidence, *, startup=0, predicate=0, analytics_running=False):
        """Only external boundaries are fake; fault/drained/wait methods are real."""
        owner = self
        harness = Harness.__new__(Harness)
        harness.evidence = Path(evidence)
        harness.args = SimpleNamespace(tier='smoke', fault_events=1, rate=1,
            consumer_outage_seconds=1, drain_timeout=900)
        harness.env = {'LABOPS_VALIDATION_IPV4_PREFIX': '10.243.77'}
        harness.cases = []
        harness.workers = {'publisher': object()}
        harness.connections = SimpleNamespace(close_all=lambda: None)
        self.clock = FakeClock()
        self.drain_budgets = []
        self.analytics_running = analytics_running
        self.compose_start_completed = None

        class ProcessedManager:
            def filter(self, **kwargs):
                role = kwargs['consumer_name']
                return CountQuery(lambda: 1 if role == 'notification' or owner.analytics_running else 0)

        class OutboxManager:
            def filter(self, **kwargs):
                owner.assertEqual(kwargs['status'], 'PUBLISHED')
                def result():
                    # A successful database response can arrive after the wait
                    # loop's pre-call deadline check; retain its entire cost.
                    owner.clock.advance(predicate)
                    return 1
                return CountQuery(result)

        harness.models = SimpleNamespace(ProcessedEvent=SimpleNamespace(objects=ProcessedManager()),
                                         OutboxEvent=SimpleNamespace(objects=OutboxManager()))

        def stop(role):
            if role == 'analytics':
                self.analytics_running = False

        def start_consumer(role):
            self.assertEqual(role, 'analytics')
            self.clock.advance(startup)
            self.analytics_running = True

        def generate(count, label):
            self.assertEqual(count, 1)
            self.clock.advance(1)
            return ['event-one'], {'input': 1, 'completed_commands': 1, 'elapsed_seconds': 1,
                'target_rate': 1, 'actual_command_rate': 1, 'schedule_lateness_seconds': 0}

        def compose(action, *args):
            if action == 'start':
                self.clock.advance(7)
                self.compose_start_completed = self.clock.monotonic()

        def drained(ids, timeout):
            self.drain_budgets.append(timeout)
            return Harness.drained(harness, ids, timeout=timeout)

        harness.stop = stop
        harness.start_consumer = start_consumer
        harness.stop_consumer_role = stop
        harness.wait_consumer_pool = lambda role, **kwargs: {'consumer': role, 'exact_owned_assignment': True}
        harness.ensure_consumer_pool = harness.wait_consumer_pool
        harness.generate = generate
        harness.compose = compose
        harness.drained = drained
        harness.offsets = lambda: {'notification': {'0': 0}, 'analytics': {'0': 0}}
        harness.snapshot = lambda ids=None: {'ledger_unchanged': True}
        harness.network_snapshot = lambda stage: {'stage': stage}
        harness.wait_brokers = lambda: None
        harness.outbox_retry_evidence = lambda ids: [{'id': 'event-one', 'status': 'PENDING'}]
        return harness

    @contextmanager
    def fake_runtime(self, *, qualification_cost=0, qualification_write_cost=0):
        def qualify(*args, **kwargs):
            self.clock.advance(qualification_cost)
            return qualify_fault_workload(*args, **kwargs)

        def persist(path, value):
            if Path(path).name.endswith('-workload-qualification.json'):
                self.clock.advance(qualification_write_cost)
            return write_json(path, value)

        def complete(started, budget):
            return completed_recovery_seconds(started, budget, monotonic=self.clock.monotonic)

        with patch('benchmarks.events.acceptance.time.monotonic', self.clock.monotonic), \
             patch('benchmarks.events.acceptance.time.sleep', self.clock.advance), \
             patch('benchmarks.events.acceptance.write_json', persist), \
             patch('benchmarks.events.health.completed_recovery_seconds', complete), \
             patch('benchmarks.events.workload_contract.qualify_fault_workload', qualify), \
             patch('network_identity.compare_broker_networks', return_value={'passed': True}):
            yield

    def window(self, directory, name):
        return json.loads((Path(directory) / (name + '-recovery-window.json')).read_text())

    def test_analytics_rejects_successful_database_response_after_900_seconds(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory, startup=10, predicate=890.001)
            with self.fake_runtime(), self.assertRaises(TimeoutError):
                harness.analytics_outage()
            evidence = self.window(directory, 'analytics_outage')
            self.assertEqual(self.drain_budgets, [890])
            self.assertAlmostEqual(evidence['observation_elapsed_seconds'], 900.001)
            self.assertIsNone(evidence['successful_completion_elapsed_seconds'])
            self.assertFalse(evidence['successful_completion_within_budget'])
            self.assertEqual(harness.cases, [])

    def test_analytics_startup_exhaustion_is_retained_and_never_starts_an_extra_drain_window(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory, startup=901)
            with self.fake_runtime(), self.assertRaisesRegex(AssertionError, 'startup exhausted'):
                harness.analytics_outage()
            evidence = self.window(directory, 'analytics_outage')
            self.assertEqual(self.drain_budgets, [])
            self.assertEqual(evidence['budget_seconds'], 900)
            self.assertEqual(evidence['observation_elapsed_seconds'], 901)
            self.assertFalse(evidence['successful_completion_within_budget'])

    def test_analytics_success_reports_startup_and_drain_as_one_measured_window(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory, startup=100, predicate=200)
            with self.fake_runtime():
                harness.analytics_outage()
            evidence = self.window(directory, 'analytics_outage')
            self.assertEqual(self.drain_budgets, [800])
            self.assertEqual(evidence['observation_elapsed_seconds'], 300)
            self.assertEqual(evidence['successful_completion_elapsed_seconds'], 300)
            self.assertTrue(evidence['successful_completion_within_budget'])
            self.assertEqual(harness.cases[0]['catch_up_seconds'], 300)

    def test_broker_clock_includes_qualification_and_artifact_io_after_start_completes(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory, predicate=850.001, analytics_running=True)
            with self.fake_runtime(qualification_cost=20, qualification_write_cost=30), \
                 self.assertRaises(TimeoutError):
                harness.broker_fault(['redpanda-0'], 1, 'one_broker_stop')
            evidence = self.window(directory, 'one_broker_stop')
            self.assertEqual(self.compose_start_completed, 108)
            self.assertEqual(self.drain_budgets, [850])
            self.assertAlmostEqual(evidence['observation_elapsed_seconds'], 900.001)
            self.assertFalse(evidence['successful_completion_within_budget'])
            self.assertEqual(evidence['window_start'], 'broker compose start completed')
            self.assertTrue((Path(directory) / 'one_broker_stop-outbox-after-recovery.json').exists())
            self.assertEqual(harness.cases, [])

    def test_slow_broker_qualification_artifact_can_exhaust_budget_before_database_drain(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory, analytics_running=True)
            with self.fake_runtime(qualification_cost=400, qualification_write_cost=501), \
                 self.assertRaisesRegex(AssertionError, 'exhausted the frozen drain window'):
                harness.broker_fault(['redpanda-0'], 1, 'one_broker_stop')
            evidence = self.window(directory, 'one_broker_stop')
            self.assertEqual(self.drain_budgets, [])
            self.assertEqual(evidence['observation_elapsed_seconds'], 901)
            self.assertIsNone(evidence['successful_completion_elapsed_seconds'])
            self.assertFalse(evidence['successful_completion_within_budget'])
            qualification = json.loads((Path(directory) /
                'one_broker_stop-workload-qualification.json').read_text())
            self.assertTrue(qualification['passed'])

    def test_broker_pool_assignment_exhausts_original_window_before_any_drain(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory, analytics_running=True)
            def slow_pool(role, **kwargs):
                self.clock.advance(901)
                return {'owned_members': True}
            harness.ensure_consumer_pool = slow_pool
            with self.fake_runtime(), self.assertRaisesRegex(AssertionError, 'assignment exhausted'):
                harness.broker_fault(['redpanda-0'], 1, 'one_broker_stop')
            evidence = self.window(directory, 'one_broker_stop')
            self.assertEqual(self.drain_budgets, [])
            self.assertEqual(evidence['observation_elapsed_seconds'], 901)
            self.assertFalse(evidence['successful_completion_within_budget'])

    def test_analytics_recovery_cannot_hide_missing_continuing_notification_member(self):
        with TemporaryDirectory() as directory:
            harness = self.make_harness(directory)
            def missing_pool(role, **kwargs):
                self.assertEqual(role, 'notification')
                raise AssertionError('Selected notification pool is incomplete')
            harness.ensure_consumer_pool = missing_pool
            with self.fake_runtime(), self.assertRaisesRegex(AssertionError, 'pool is incomplete'):
                harness.analytics_outage()
            evidence = self.window(directory, 'analytics_outage')
            self.assertEqual(self.drain_budgets, [])
            self.assertFalse(evidence['successful_completion_within_budget'])
