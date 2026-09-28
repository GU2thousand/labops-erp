"""Harness boundaries retain committed truth and make final failures fail CI."""
from contextlib import nullcontext
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from django.test import SimpleTestCase

from benchmarks.events.acceptance import Harness, main
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile


def args(directory, **changes):
    values = dict(run_id='harness-accounting', tier='smoke', events=4, rate=50,
        duration=0, fault_repetitions=1, fault_events=1, duplicate_events=1,
        poison_events=12, broker_fault_seconds=1, outage_seconds=1,
        consumer_outage_seconds=1, drain_timeout=900,
        evidence_dir=directory, generated_dir=directory)
    values.update(changes)
    return SimpleNamespace(**values)


class HarnessGenerationAccountingTests(SimpleTestCase):
    def harness(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        h = object.__new__(Harness)
        h.args, h.evidence = args(directory), directory
        h.generation = GenerationJournal(directory, h.args.run_id, numeric_profile(h.args))
        self.addCleanup(h.generation.finalize)
        h.events, h.workers, h.children, h.logs = [], {}, [], []
        h.cases, h.delivery_proofs, h.shutdowns, h.supervisor_restarts = [], [], [], []
        h.connections, h.connection, h.models, h.services, h.api = [Mock() for _ in range(5)]
        h.models.FailedDelivery.objects.order_by.return_value.values.return_value = []
        h.started_at = 0
        (directory / 'errors.jsonl').touch()
        h.admin = Mock()
        h.order, h.order_line, h.source = [SimpleNamespace(id=uuid4()) for _ in range(3)]
        h.cycle_issue = None
        return h

    def receipt(self):
        receipt = Mock()
        receipt.id, receipt.version = uuid4(), 1
        receipt.lines.first.return_value.batch = SimpleNamespace(id=uuid4())
        return receipt

    def cursor(self, *, error=None):
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone.return_value = ['123']
        if error:
            cursor.execute.side_effect = error
        return cursor

    def test_real_business_transaction_exception_keeps_original_and_failed_denominator(self):
        h = self.harness()
        original = RuntimeError('business boundary')
        h.connection.cursor.return_value = self.cursor()
        h.services.post_receipt.side_effect = original
        with patch('django.db.transaction.atomic', return_value=nullcontext()), \
             patch('labops.purchasing.services.create_receipt', return_value=self.receipt()):
            with self.assertRaises(RuntimeError) as raised:
                h.generate(4, 'steady')
        self.assertIs(raised.exception, original)
        batch = h.generation.summary()['batches'][0]
        self.assertEqual((batch['requested'], batch['attempted'], batch['committed'],
                          batch['failed_before_commit'], batch['unattempted']), (4, 1, 0, 1, 3))
        self.assertEqual(batch['failure']['stage'], 'business_transaction')
        self.assertEqual(h.events, [])

    def test_real_post_commit_observation_exception_preserves_commit_and_movement_identity(self):
        h = self.harness()
        original = RuntimeError('timestamp boundary')
        movement = SimpleNamespace(id=uuid4(), type='RECEIPT')
        h.services.post_receipt.return_value = movement
        h.connection.cursor.side_effect = [self.cursor(), self.cursor(error=original)]
        with patch('django.db.transaction.atomic', return_value=nullcontext()), \
             patch('labops.purchasing.services.create_receipt', return_value=self.receipt()):
            with self.assertRaises(RuntimeError) as raised:
                h.generate(4, 'steady')
        self.assertIs(raised.exception, original)
        batch = h.generation.summary()['batches'][0]
        self.assertEqual((batch['requested'], batch['attempted'], batch['committed'],
            batch['failed_before_commit'], batch['post_commit_observation_failed'],
            batch['identified_events'], batch['unattempted']), (4, 1, 1, 0, 1, 0, 3))
        self.assertEqual(batch['failure']['stage'], 'commit_timestamp_observation')
        self.assertEqual(h.generation.committed_attempts()[0]['movement_id'], str(movement.id))
        self.assertEqual(h.events, [])

    def test_journal_io_error_does_not_replace_original_business_exception(self):
        h = self.harness()
        original = RuntimeError('original')
        h._generate_commands = Mock(side_effect=original)
        with patch.object(h.generation, 'finish_failure', side_effect=OSError('journal unavailable')):
            with self.assertRaises(RuntimeError) as raised:
                h.generate(4, 'steady')
        self.assertIs(raised.exception, original)
        self.assertEqual(h._generation_accounting_error, 'OSError')

    def complete_state(self, event_id):
        return {'database_observed': True, 'offsets_observed': True, 'errors': [],
            'actual_inventory_outbox_count': 1, 'unpublished_count': 0,
            'consumers': {name: {'incomplete_count': 0} for name in ('notification', 'analytics')},
            'event_log_ids_missing_from_database': [], 'actual_committed_ids_missing_from_event_log': [],
            'journal_committed_movements_missing_from_database': [],
            'database_committed_movements_missing_from_journal': [],
            'journal_identified_events_missing_from_database': [], 'processed_hash_conflicts': [],
            'reconciliation': {'mismatches': [], 'dedupe_count': 2,
                'notification_count': 3, 'expected_notification_count': 3}}

    def identified(self, h):
        movement, event = str(uuid4()), str(uuid4())
        batch = h.generation.begin_batch(1, 50, 'steady')
        attempt = h.generation.attempt(batch)
        h.generation.commit(batch, attempt, movement)
        h.generation.identify_event(batch, attempt, event)
        h.generation.finish_success(batch)
        h.events = [{'event_id': event, 'scenario': 'steady'}]
        return movement, event

    def test_final_reconciliation_is_required_for_pass_and_report_retained_on_failure(self):
        for defect in ('mismatches', 'dedupe_count', 'notification_count', 'processed_hash_conflicts'):
            with self.subTest(defect=defect):
                h = self.harness()
                _, event = self.identified(h)
                state = self.complete_state(event)
                if defect == 'processed_hash_conflicts':
                    state[defect] = [{'event_id': event, 'consumer': 'analytics'}]
                elif defect == 'mismatches':
                    state['reconciliation'][defect] = [{'ledger': '1', 'projection': '2'}]
                else:
                    state['reconciliation'][defect] += 1
                h.final_inventory_evidence = Mock(return_value=state)
                report = h.finish()
                self.assertFalse(report['passed'])
                self.assertFalse(report['final_inventory_complete'])
                self.assertFalse(json.loads((h.evidence / 'report.json').read_text())['passed'])

    def test_top_level_committed_count_comes_from_journal_even_without_event_observation(self):
        h = self.harness()
        batch = h.generation.begin_batch(4, 50, 'steady')
        attempt = h.generation.attempt(batch)
        h.generation.commit(batch, attempt, str(uuid4()))
        h.generation.finish_failure(batch, 'commit_timestamp_observation', 'RuntimeError', attempt)
        h.final_inventory_evidence = Mock(return_value={'database_observed': False,
            'offsets_observed': False, 'errors': [{'stage': 'database_snapshot', 'error_type': 'RuntimeError'}]})
        report = h.finish(RuntimeError('original'))
        self.assertEqual(report['steady_inventory_inputs_committed'], 1)
        self.assertEqual(report['unique_generated_events'], 0)
        self.assertFalse(report['generation_accounting_complete'])
        self.assertNotIn('actual_inventory_outbox_count', report['final_inventory_state'])

    def test_final_db_snapshot_recovers_committed_identity_absent_from_event_log(self):
        h = self.harness()
        movement, event = str(uuid4()), str(uuid4())
        batch = h.generation.begin_batch(4, 50, 'steady')
        attempt = h.generation.attempt(batch)
        h.generation.commit(batch, attempt, movement)
        h.generation.finish_failure(batch, 'commit_timestamp_observation', 'RuntimeError', attempt)
        h.models.OutboxEvent.objects.filter.return_value.order_by.return_value.values.return_value = [
            {'id': event, 'aggregate_id': movement, 'payload_hash': 'abc', 'status': 'PENDING'}]
        h.models.ProcessedEvent.objects.filter.return_value.values.return_value = []
        h.snapshot = Mock(return_value={'mismatches': [], 'dedupe_count': 0})
        h.offsets = Mock(return_value={'notification': {'0': -1001}, 'analytics': {'0': -1001}})
        final = h.final_inventory_evidence()
        self.assertEqual(final['actual_inventory_outbox_count'], 1)
        self.assertEqual(final['actual_committed_ids_missing_from_event_log'], [event])
        self.assertEqual(final['unpublished_count'], 1)
        self.assertEqual(final['consumers']['analytics']['incomplete_event_ids'], [event])
        self.assertEqual(final['database_committed_movements_missing_from_journal'], [])
        self.assertEqual(json.loads((h.evidence / 'final-outbox-state.json').read_text())[0]['id'], event)

    def test_unavailable_final_queries_retain_unknown_not_zero_and_fail(self):
        h = self.harness()
        h.models.OutboxEvent.objects.filter.side_effect = RuntimeError('database unavailable')
        h.offsets = Mock(side_effect=TimeoutError('coordinator unavailable'))
        final = h.final_inventory_evidence()
        self.assertFalse(final['database_observed'])
        self.assertFalse(final['offsets_observed'])
        self.assertNotIn('actual_inventory_outbox_count', final)
        self.assertEqual([item['stage'] for item in final['errors']], ['database_snapshot', 'committed_offsets'])
        self.assertEqual(json.loads((h.evidence / 'final-inventory-state.json').read_text()), final)

    def argv(self, directory, *, full=False, **changes):
        profile = dict(events=90000, rate=50, duration=1800, fault_repetitions=20,
            fault_events=30000, duplicate_events=10000, poison_events=100,
            broker_fault_seconds=300, outage_seconds=600, consumer_outage_seconds=600,
            drain_timeout=900) if full else dict(events=4, rate=50, duration=0,
            fault_repetitions=1, fault_events=1, duplicate_events=1, poison_events=12,
            broker_fault_seconds=1, outage_seconds=1, consumer_outage_seconds=1, drain_timeout=900)
        profile.update(changes)
        return ['acceptance.py', '--run-id', 'harness-cli', '--tier', 'full' if full else 'smoke',
            '--evidence-dir', str(directory), '--generated-dir', str(directory)] + [
            value for key, val in profile.items() for value in ('--' + key.replace('_', '-'), str(val))]

    def test_nonpassing_final_report_fails_cli_even_when_run_returned_successfully(self):
        h = self.harness()
        fake = Mock()
        fake.finish.return_value = {'passed': False}
        # Use a new directory: main freezes its own exclusive profile first.
        directory = h.evidence / 'cli'
        with patch('sys.argv', self.argv(directory)), patch('benchmarks.events.acceptance.load_environment'), \
             patch('benchmarks.events.acceptance.Harness', return_value=fake):
            with self.assertRaisesRegex(AssertionError, 'Final durable accounting'):
                main()
        fake.run.assert_called_once()
        fake.finish.assert_called_once_with(None)
        self.assertTrue((directory / 'requested-profile.json').exists())

    def test_finalization_failure_preserves_original_runtime_exception(self):
        h = self.harness()
        original = RuntimeError('original runtime')
        fake = Mock()
        fake.run.side_effect = original
        fake.finish.side_effect = OSError('evidence unavailable')
        with patch('sys.argv', self.argv(h.evidence / 'cli')), patch('benchmarks.events.acceptance.load_environment'), \
             patch('benchmarks.events.acceptance.Harness', return_value=fake):
            with self.assertRaises(RuntimeError) as raised:
                main()
        self.assertIs(raised.exception, original)

    def test_startup_failure_freezes_numeric_requests_and_preserves_original(self):
        h = self.harness()
        directory = h.evidence / 'startup'
        original = OSError('client environment unavailable')
        with patch('sys.argv', self.argv(directory)), \
             patch('benchmarks.events.acceptance.load_environment', side_effect=original):
            with self.assertRaises(OSError) as raised:
                main()
        self.assertIs(raised.exception, original)
        saved = json.loads((directory / 'startup-failure.json').read_text())
        self.assertFalse(saved['passed'])
        self.assertEqual(saved['requested_numeric_profile']['events'], 4)
        self.assertEqual(saved['generation']['totals']['requested'], 0)
        self.assertFalse(saved['database_state_observed'])

    def test_full_profile_cannot_extend_recovery_sla_or_reduce_duplicate_denominator(self):
        h = self.harness()
        for changes in ({'drain_timeout': 901}, {'drain_timeout': 1800}, {'duplicate_events': 90001}):
            directory = h.evidence / ('invalid-' + next(iter(changes)))
            with self.subTest(changes=changes), patch('sys.argv', self.argv(directory, full=True, **changes)):
                with self.assertRaises(SystemExit) as raised:
                    main()
            self.assertEqual(raised.exception.code, 2)
            self.assertFalse(directory.exists())
