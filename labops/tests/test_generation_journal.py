"""Requested denominators and durable accounting across generation failures."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from django.test import SimpleTestCase

from benchmarks.events.generation_journal import GenerationJournal, numeric_profile


def requested_args(**overrides):
    return SimpleNamespace(events=90000, rate=50.0, duration=1800.0,
        fault_repetitions=20, fault_events=30000, duplicate_events=10000,
        poison_events=100, broker_fault_seconds=300.0, outage_seconds=600.0,
        consumer_outage_seconds=600.0, drain_timeout=900.0,
        tier='full', secret='private-password', **overrides)


class GenerationJournalTests(SimpleTestCase):
    def journal(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        journal = GenerationJournal(Path(directory.name), 'journal-test',
                                    numeric_profile(requested_args()))
        self.addCleanup(journal.finalize)
        return journal

    def identify(self, journal, batch):
        attempt = journal.attempt(batch)
        movement_id, event_id = str(uuid4()), str(uuid4())
        journal.commit(batch, attempt, movement_id=movement_id)
        journal.identify_event(batch, attempt, event_id)
        return attempt, movement_id, event_id

    def test_profile_frozen_before_first_batch_and_only_exact_numeric_requests(self):
        journal = self.journal()
        frozen = json.loads(journal.profile_path.read_text())
        self.assertEqual(frozen['requested_numeric_profile'], numeric_profile(requested_args()))
        self.assertEqual(frozen['requested_numeric_profile']['events'], 90000)
        self.assertEqual(frozen['requested_numeric_profile']['drain_timeout'], 900.0)
        self.assertEqual(len(frozen['requested_numeric_profile']), 11)
        self.assertNotIn('secret', frozen['requested_numeric_profile'])
        self.assertNotIn('private-password', journal.journal_path.read_text())
        self.assertEqual(journal.summary()['totals']['requested'], 0)
        for invalid in (True, float('nan'), float('inf'), '90000'):
            args = requested_args()
            args.events = invalid
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                numeric_profile(args)
        # Query results cannot mutate the denominator retained by the journal.
        report = journal.summary()
        report['requested_numeric_profile']['events'] = 1
        self.assertEqual(journal.summary()['requested_numeric_profile']['events'], 90000)

    def test_failure_before_commit_preserves_requested_and_unattempted_counts(self):
        journal = self.journal()
        batch = journal.begin_batch(5, 50, 'steady')
        self.identify(journal, batch)
        attempted = journal.attempt(batch)
        try:
            raise RuntimeError('private-password business-payload')
        except RuntimeError as exc:
            summary = journal.finish_failure(batch, 'business_transaction',
                                             type(exc).__name__, attempt_id=attempted)
        self.assertEqual({key: summary[key] for key in
            ('requested', 'attempted', 'committed', 'failed_before_commit',
             'post_commit_observation_failed', 'identified_events', 'unattempted')},
            {'requested': 5, 'attempted': 2, 'committed': 1, 'failed_before_commit': 1,
             'post_commit_observation_failed': 0, 'identified_events': 1, 'unattempted': 3})
        self.assertEqual(summary['failure'],
                         {'stage': 'business_transaction', 'error_type': 'RuntimeError'})
        final = journal.finalize()
        self.assertEqual(final['totals']['requested'], 5)
        for path in (journal.journal_path, journal.summary_path):
            raw = path.read_text()
            self.assertNotIn('private-password', raw)
            self.assertNotIn('business-payload', raw)
        saved = json.loads(journal.summary_path.read_text())
        self.assertEqual(saved['totals']['failed_before_commit'], 1)
        self.assertEqual(saved['totals']['unattempted'], 3)

    def test_post_commit_lookup_failure_keeps_committed_command_without_observed_event(self):
        journal = self.journal()
        batch = journal.begin_batch(4, 50, 'broker_fault')
        attempt = journal.attempt(batch)
        movement_id = str(uuid4())
        journal.commit(batch, attempt, movement_id=movement_id)
        summary = journal.finish_failure(batch, 'event_lookup', 'OperationalError',
                                         attempt_id=attempt)
        self.assertEqual(summary['requested'], 4)
        self.assertEqual(summary['attempted'], 1)
        self.assertEqual(summary['committed'], 1)
        self.assertEqual(summary['failed_before_commit'], 0)
        self.assertEqual(summary['post_commit_observation_failed'], 1)
        self.assertEqual(summary['identified_events'], 0)
        self.assertEqual(summary['unattempted'], 3)
        self.assertEqual(journal.committed_attempts()[0]['movement_id'], movement_id)
        self.assertIsNone(journal.committed_attempts()[0]['event_id'])
        raw = [json.loads(line) for line in journal.journal_path.read_text().splitlines()]
        self.assertEqual([line['action'] for line in raw],
            ['profile_frozen', 'batch_started', 'command_attempted', 'command_committed',
             'command_failed', 'batch_failed'])
        self.assertEqual(raw[3]['movement_id'], movement_id)
        self.assertFalse(journal.summary()['journal_database_atomic'])
        self.assertTrue(journal.summary()['database_reconciliation_required'])

    def test_failure_after_event_identification_never_relabels_commit_as_rollback(self):
        journal = self.journal()
        batch = journal.begin_batch(2, 50, 'worker_check')
        attempt, _, event_id = self.identify(journal, batch)
        summary = journal.finish_failure(batch, 'worker_health', 'AssertionError',
                                         attempt_id=attempt)
        self.assertEqual(summary['committed'], 1)
        self.assertEqual(summary['identified_events'], 1)
        self.assertEqual(summary['failed_before_commit'], 0)
        self.assertEqual(summary['post_commit_observation_failed'], 1)
        self.assertEqual(journal.committed_attempts()[0]['event_id'], event_id)

    def test_repeated_scenario_batches_keep_independent_requested_denominators(self):
        journal = self.journal()
        first = journal.begin_batch(2, 50, 'single_broker_outage')
        self.identify(journal, first)
        self.identify(journal, first)
        journal.finish_success(first)
        second = journal.begin_batch(5, 50, 'single_broker_outage')
        attempt = journal.attempt(second)
        journal.finish_failure(second, 'business_transaction', 'OperationalError',
                               attempt_id=attempt)
        self.assertNotEqual(first, second)
        report = journal.finalize()
        self.assertEqual([batch['requested'] for batch in report['batches']], [2, 5])
        self.assertEqual([batch['status'] for batch in report['batches']], ['succeeded', 'failed'])
        self.assertEqual(report['totals']['requested'], 7)
        self.assertEqual(report['totals']['attempted'], 3)
        self.assertEqual(report['totals']['committed'], 2)
        self.assertEqual(report['totals']['unattempted'], 4)
        self.assertEqual(report['requested_numeric_profile']['fault_events'], 30000)
        self.assertEqual(report['requested_numeric_profile']['fault_repetitions'], 20)

    def test_attempt_and_identification_flush_without_per_command_summary_or_extra_fsync(self):
        journal = self.journal()
        batch = journal.begin_batch(1, 50, 'steady')
        prefix = journal.journal_path.read_bytes()
        with patch('benchmarks.events.generation_journal.os.fsync') as fsync:
            attempt = journal.attempt(batch)
            fsync.assert_not_called()
            self.assertTrue(journal.journal_path.read_bytes().startswith(prefix))
            self.assertFalse(journal.summary_path.exists())
            journal.commit(batch, attempt, movement_id=str(uuid4()))
            self.assertEqual(fsync.call_count, 1)
            journal.identify_event(batch, attempt, str(uuid4()))
            self.assertEqual(fsync.call_count, 1)
            self.assertFalse(journal.summary_path.exists())
            journal.finish_success(batch)
            self.assertEqual(fsync.call_count, 3)  # batch journal + summary file
        self.assertTrue(journal.summary_path.exists())
        raw = [json.loads(line) for line in journal.journal_path.read_text().splitlines()]
        self.assertEqual([line['sequence'] for line in raw], list(range(1, len(raw) + 1)))

    def test_incomplete_finalization_retains_pending_boundaries_and_rejects_false_success(self):
        journal = self.journal()
        batch = journal.begin_batch(3, 50, 'steady')
        attempt = journal.attempt(batch)
        with self.assertRaises(ValueError):
            journal.finish_success(batch)
        with self.assertRaises(ValueError):
            journal.finish_failure(batch, 'generation', 'RuntimeError')
        journal.commit(batch, attempt)
        final = journal.finalize()
        self.assertEqual(final['totals']['requested'], 3)
        self.assertEqual(final['totals']['committed'], 1)
        self.assertEqual(final['totals']['pending_observation'], 1)
        self.assertEqual(final['totals']['post_commit_observation_failed'], 0)
        self.assertEqual(final['totals']['unattempted'], 2)
        with self.assertRaises(RuntimeError):
            journal.begin_batch(1, 50, 'late')
        self.assertEqual(len(journal.summary()['batches']), 1)

    def test_invalid_raw_error_text_and_attempt_overrun_cannot_change_counts(self):
        journal = self.journal()
        batch = journal.begin_batch(1, 50, 'steady')
        attempt = journal.attempt(batch)
        with self.assertRaises(ValueError):
            journal.finish_failure(batch, 'business_transaction', 'password=private value',
                                   attempt_id=attempt)
        self.assertEqual(journal.batch_summary(batch)['pending_before_commit'], 1)
        journal.commit(batch, attempt)
        with self.assertRaises(ValueError):
            journal.commit(batch, attempt)
        journal.identify_event(batch, attempt, str(uuid4()))
        with self.assertRaises(ValueError):
            journal.attempt(batch)
        journal.finish_success(batch)
        self.assertEqual(journal.batch_summary(batch)['requested'], 1)
        self.assertNotIn('private value', journal.journal_path.read_text())
