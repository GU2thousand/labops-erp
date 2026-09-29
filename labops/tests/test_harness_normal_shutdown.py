"""Owned exits qualify independently of optional consumer DB-close receipts."""
import json
from pathlib import Path
import signal
import subprocess
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from benchmarks.events.acceptance import Harness


class Process:
    def __init__(self, pid, *, exit_code=0, already_exited=False, timeout=False):
        self.pid = pid
        self.returncode = exit_code if already_exited else None
        self.exit_code = exit_code
        self.timeout = timeout
        self.signals = []
        self.kills = 0
        self.signal_error = None

    def poll(self):
        return self.returncode

    def send_signal(self, value):
        if self.signal_error is not None:
            raise self.signal_error
        self.signals.append(value)
        if value == signal.SIGKILL:
            self.returncode = -signal.SIGKILL

    def wait(self, timeout=None):
        if self.timeout and self.returncode is None:
            raise subprocess.TimeoutExpired('owned-shutdown-fixture', timeout)
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.returncode

    def kill(self):
        self.kills += 1
        self.returncode = -signal.SIGKILL


class NormalShutdownTests(SimpleTestCase):
    def harness(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        h = object.__new__(Harness)
        h.evidence = Path(temporary.name)
        (h.evidence / 'logs').mkdir()
        (h.evidence / 'errors.jsonl').touch()
        h.workers, h.children, h.logs, h.shutdowns = {}, [], [], []
        h.child_identities, h.child_groups, h.worker_closures = {}, {}, {}
        h.rejected_child_generations, h.cleanup_errors = {}, []
        h.worker_exit_observations = {}
        h.sync_metrics_targets = Mock()
        h.consumer_group = lambda name: 'shutdown.' + name
        return h

    def add(self, h, role='publisher', *, tracked=True, receipt=None, **changes):
        process = Process(100 + len(h.children), **changes)
        h.children.append(process)
        h.child_identities[process.pid] = {'role': role, 'generation': len(h.children) - 1,
            'identity_receipt': receipt, 'application_name': 'shutdown-' + str(process.pid),
            'start_time_ticks': 1000 + process.pid, 'fault_stage': 'normal'}
        if tracked:
            h.workers[role] = process
        return process

    def observations(self, h):
        return [json.loads(line) for line in
            (h.evidence / 'worker-exit-observations.jsonl').read_text().splitlines()]

    def finish_fixture(self, h):
        h.args = SimpleNamespace(run_id='normal-shutdown', tier='smoke')
        h.events = [{'event_id': str(index)} for index in range(4)]
        h.cases = [{'name': 'steady', 'passed': True}]
        h.delivery_proofs, h.supervisor_restarts, h.generation_topologies = [], [], []
        h.started_at = time.time()
        h.generation = Mock()
        h.generation.profile = {}
        h.generation.finalize.return_value = {'batches': [
            {'status': 'succeeded', 'label': 'steady', 'committed': 4}],
            'totals': {'requested': 4, 'committed': 4, 'identified_events': 4},
            'requested_numeric_profile': {'events': 4}}
        h.models = Mock()
        h.models.FailedDelivery.objects.order_by.return_value.values.return_value = []
        h.settle_worker_sessions = Mock(return_value=True)
        final = {'database_observed': True, 'offsets_observed': True, 'errors': [],
            'unpublished_count': 0, 'actual_inventory_outbox_count': 4,
            'consumers': {name: {'incomplete_count': 0} for name in ('notification', 'analytics')},
            'reconciliation': {'mismatches': [], 'dedupe_count': 8,
                'notification_count': 12, 'expected_notification_count': 12}}
        for field in ('processed_hash_conflicts', 'event_log_ids_missing_from_database',
                      'actual_committed_ids_missing_from_event_log',
                      'journal_committed_movements_missing_from_database',
                      'database_committed_movements_missing_from_journal',
                      'journal_identified_events_missing_from_database'):
            final[field] = []
        h.final_inventory_evidence = Mock(return_value=final)

    def test_no_receipt_publisher_retry_and_dlq_nonzero_exits_fail_cleanup(self):
        for role in ('publisher', 'retry', 'dlq'):
            with self.subTest(role=role):
                h = self.harness()
                self.add(h, role, exit_code=1)
                self.assertIsInstance(h.cleanup_workers(), AssertionError)
                self.assertTrue(h.cleanup_errors)
                outcome = self.observations(h)[0]
                self.assertEqual(outcome['exit_code'], 1)
                self.assertTrue(outcome['normal_exit_required'])
                self.assertFalse(outcome['passed'])
                self.assertFalse(h.worker_closures)

    def test_no_receipt_zero_exits_are_recorded_without_inventing_db_close(self):
        h = self.harness()
        for role in ('publisher', 'retry', 'dlq'):
            self.add(h, role)
        log = Mock()
        h.logs.append(log)
        self.assertIsNone(h.cleanup_workers())
        self.assertEqual(len(self.observations(h)), 3)
        self.assertTrue(all(row['passed'] and row['normal_exit_required'] for row in self.observations(h)))
        self.assertFalse(h.cleanup_errors)
        self.assertFalse(h.worker_closures)
        log.close.assert_called_once_with()

    def test_normal_shutdown_timeout_and_forced_sigkill_fail(self):
        h = self.harness()
        process = self.add(h, timeout=True)
        self.assertIsInstance(h.cleanup_workers(), AssertionError)
        self.assertEqual(process.kills, 1)
        self.assertTrue(h.shutdowns[0]['forced_SIGKILL'])
        outcome = self.observations(h)[0]
        self.assertTrue(outcome['forced_SIGKILL'])
        self.assertFalse(outcome['intentional_SIGKILL'])
        self.assertFalse(outcome['passed'])

    def test_forced_sigkill_cannot_pass_even_with_zero_exit_code(self):
        h = self.harness()
        process = self.add(h, already_exited=True)
        with self.assertRaises(AssertionError):
            h.observe_worker_close(process, forced_sigkill=True)
        self.assertFalse(self.observations(h)[0]['passed'])

    def test_already_dead_normal_slot_and_untracked_children_are_checked(self):
        for tracked in (True, False):
            with self.subTest(tracked=tracked):
                h = self.harness()
                process = self.add(h, tracked=tracked, exit_code=1, already_exited=True)
                self.assertIsInstance(h.cleanup_workers(), AssertionError)
                self.assertFalse(process.signals)
                self.assertTrue(h.cleanup_errors)
                self.assertFalse(self.observations(h)[0]['passed'])

    def test_untracked_live_child_forced_reap_is_not_a_normal_close(self):
        h = self.harness()
        process = self.add(h, tracked=False)
        self.assertIsInstance(h.cleanup_workers(), AssertionError)
        self.assertEqual(process.kills, 1)
        self.assertTrue(self.observations(h)[0]['forced_SIGKILL'])

    def test_fault_stage_label_cannot_exempt_a_nonzero_normal_exit(self):
        h = self.harness()
        process = self.add(h, exit_code=1, already_exited=True)
        h.child_identities[process.pid]['fault_stage'] = 'stale_owner'
        self.assertIsInstance(h.cleanup_workers(), AssertionError)
        self.assertFalse(self.observations(h)[0]['expected_fault_exit'])

    def test_explicit_fault_exit_is_retained_for_final_cleanup(self):
        h = self.harness()
        self.add(h, exit_code=1)
        h.stop('publisher', expected_fault_exit=True)
        self.assertIsNone(h.cleanup_workers())
        self.assertEqual(len(self.observations(h)), 1)
        self.assertTrue(self.observations(h)[0]['expected_fault_exit'])
        self.assertTrue(self.observations(h)[0]['passed'])
        self.assertFalse(h.cleanup_errors)

    def test_consumer_replacement_rejects_unexpected_exit_by_default(self):
        h = self.harness()
        h.consumer_topology = 'single'
        self.add(h, 'notification', exit_code=1, already_exited=True)
        h.start_consumer, h.wait_consumer_pool = Mock(), Mock()
        with self.assertRaises(AssertionError):
            h.ensure_consumer_pool('notification')
        h.start_consumer.assert_not_called()
        h.wait_consumer_pool.assert_not_called()
        self.assertFalse(self.observations(h)[0]['expected_fault_exit'])

    def test_known_fault_replacement_requires_explicit_caller_expectation(self):
        h = self.harness()
        h.consumer_topology = 'single'
        self.add(h, 'notification', exit_code=1, already_exited=True)
        h.start_consumer = Mock()
        h.wait_consumer_pool = Mock(return_value={'membership_observed': True})
        self.assertEqual(h.ensure_consumer_pool('notification', expected_fault_exit=True),
                         {'membership_observed': True})
        h.start_consumer.assert_called_once_with('notification')
        self.assertTrue(self.observations(h)[0]['expected_fault_exit'])

    def test_analytics_outage_does_not_exempt_continuing_notification_exit(self):
        h = self.harness()
        h.consumer_topology = 'single'
        self.add(h, 'notification', exit_code=1, already_exited=True)
        h.args = SimpleNamespace(fault_events=1, rate=1, tier='smoke',
                                 consumer_outage_seconds=1, drain_timeout=900)
        h.stop_consumer_role, h.start_consumer_pool = Mock(), Mock()
        h.start_consumer, h.wait_consumer_pool, h.drained = Mock(), Mock(), Mock()
        h.connections = Mock()
        h.models = SimpleNamespace(ProcessedEvent=SimpleNamespace(objects=SimpleNamespace(
            filter=lambda consumer_name, **kwargs: SimpleNamespace(
                count=lambda: 1 if consumer_name == 'notification' else 0))))
        clock = [0.]
        def generate(*args):
            clock[0] += 1
            return ['event-one'], {'input': 1, 'completed_commands': 1, 'elapsed_seconds': 1,
                'target_rate': 1, 'actual_command_rate': 1, 'schedule_lateness_seconds': 0}
        h.generate = generate
        with patch('benchmarks.events.acceptance.time.monotonic', side_effect=lambda: clock[0]), \
             self.assertRaises(AssertionError):
            h.analytics_outage()
        h.start_consumer.assert_not_called()
        h.drained.assert_not_called()
        self.assertFalse(self.observations(h)[0]['expected_fault_exit'])

    def test_intentional_sigkill_requires_the_signal_and_survives_recheck(self):
        h = self.harness()
        process = self.add(h)
        h.stop('publisher', kill=True)
        self.assertEqual(process.returncode, -signal.SIGKILL)
        self.assertIsNone(h.cleanup_workers())
        self.assertTrue(self.observations(h)[0]['intentional_SIGKILL_observed'])
        self.assertFalse(h.cleanup_errors)

    def test_already_exited_target_cannot_be_reported_as_injected_sigkill(self):
        for exit_code in (0, 1, -signal.SIGKILL):
            with self.subTest(exit_code=exit_code):
                h = self.harness()
                process = self.add(h, exit_code=exit_code, already_exited=True)
                with self.assertRaisesMessage(AssertionError, 'SIGKILL target already exited'):
                    h.stop('publisher', kill=True)
                self.assertFalse(process.signals)
                self.assertTrue(h.cleanup_errors)

    def test_sigkill_exception_cannot_exempt_an_unrelated_exit(self):
        h = self.harness()
        process = self.add(h, exit_code=1, already_exited=True)
        with self.assertRaises(AssertionError):
            h.observe_worker_close(process, expected_sigkill=True)
        self.assertFalse(self.observations(h)[0]['intentional_SIGKILL_observed'])

    def test_failed_normal_exit_cannot_be_reclassified_after_observation(self):
        h = self.harness()
        process = self.add(h, exit_code=1, already_exited=True)
        with self.assertRaises(AssertionError):
            h.observe_worker_close(process)
        with self.assertRaises(AssertionError):
            h.observe_worker_close(process, expected_fault_exit=True)
        self.assertEqual(len(self.observations(h)), 1)
        self.assertFalse(self.observations(h)[0]['expected_fault_exit'])

    def test_normal_consumer_still_requires_real_complete_close_receipt(self):
        for stage in ('normal', 'transient_failure', 'before_commit'):
            with self.subTest(stage=stage):
                h = self.harness()
                process = self.add(h, 'notification', receipt='required-but-missing')
                h.child_identities[process.pid]['fault_stage'] = stage
                self.assertIsInstance(h.cleanup_workers(), AssertionError)
                self.assertTrue(self.observations(h)[0]['passed'])
                self.assertFalse(h.worker_closures[process.pid]['owning_close_receipt_complete'])

    def test_group_stop_checks_already_dead_untracked_normal_member(self):
        h = self.harness()
        failed = self.add(h, 'notification-old', tracked=False, exit_code=1, already_exited=True)
        healthy = self.add(h, 'notification-live', tracked=False)
        for process in (failed, healthy):
            h.child_groups[process.pid] = h.consumer_group('notification')
        with self.assertRaises(AssertionError):
            h.stop_consumer_role('notification')
        self.assertEqual(healthy.returncode, 0)
        self.assertTrue(h.cleanup_errors)

    def test_direct_failed_consumer_receipt_remains_failed_on_final_recheck(self):
        h = self.harness()
        process = self.add(h, 'notification', receipt='required-but-missing', already_exited=True)
        with self.assertRaises(AssertionError):
            h.observe_worker_close(process)
        self.assertEqual(h.cleanup_errors[0]['stage'], 'worker_close_qualification')
        self.assertIsInstance(h.cleanup_workers(), AssertionError)
        self.assertFalse(h.worker_closures[process.pid]['owning_close_receipt_complete'])
        self.assertTrue(h.cleanup_errors)

    def test_first_shutdown_error_keeps_later_reaps_and_all_log_closes(self):
        h = self.harness()
        failed = self.add(h)
        original = RuntimeError('signal boundary')
        failed.signal_error = original
        healthy = self.add(h, 'retry')
        first_log, second_log = Mock(), Mock()
        first_log.close.side_effect = OSError('log close boundary')
        h.logs = [first_log, second_log]
        self.assertIs(h.cleanup_workers(), original)
        self.assertEqual(healthy.returncode, 0)
        self.assertEqual(failed.returncode, -signal.SIGKILL)
        first_log.close.assert_called_once_with()
        second_log.close.assert_called_once_with()

    def test_exit_failure_writes_failed_report_and_raises_without_primary_error(self):
        h = self.harness()
        self.add(h, exit_code=1)
        self.finish_fixture(h)
        with self.assertRaises(AssertionError):
            h.finish()
        report = json.loads((h.evidence / 'report.json').read_text())
        self.assertFalse(report['passed'])
        self.assertFalse(report['owned_worker_cleanup_complete'])
        self.assertFalse(report['owned_worker_exit_observations'][0]['passed'])
        self.assertEqual(report['error_type'], 'AssertionError')

    def test_primary_failure_is_retained_when_cleanup_also_fails(self):
        h = self.harness()
        self.add(h, exit_code=1)
        self.finish_fixture(h)
        original = ValueError('original business boundary')
        report = h.finish(original)
        self.assertFalse(report['passed'])
        self.assertFalse(report['owned_worker_cleanup_complete'])
        self.assertEqual(report['error_type'], 'ValueError')
        self.assertEqual(report['error'], str(original))
        self.assertTrue((h.evidence / 'report.json').is_file())
