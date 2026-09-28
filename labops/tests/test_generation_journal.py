"""Requested denominators and durable accounting across generation failures."""
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
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
        self.assertEqual(len(frozen['requested_numeric_profile']), 12)
        self.assertIs(frozen['requested_numeric_profile']['runtime_diagnostics_enabled'], False)
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

    def test_diagnostics_opt_in_survives_profile_copy_without_string_coercion(self):
        for enabled in (False, True):
            profile = numeric_profile(requested_args(runtime_diagnostics=enabled))
            self.assertIs(profile['runtime_diagnostics_enabled'], enabled)
            self.assertEqual(numeric_profile(profile), profile)
        for invalid in ('false', 'true', 0, 1, None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                numeric_profile(requested_args(runtime_diagnostics=invalid))

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

    def test_four_concurrent_lanes_preserve_all_counts_ids_and_raw_transition_order(self):
        journal = self.journal()
        commit_barrier = Barrier(4, timeout=10)

        def lane(index):
            batch = journal.begin_batch(100, 12.5, 'steady_lane_' + str(index))
            identities = []
            for command in range(100):
                attempt = journal.attempt(batch)
                movement_id, event_id = str(uuid4()), str(uuid4())
                # All lanes reach a pending transaction before racing to write
                # their commit transition through the shared journal writer.
                commit_barrier.wait()
                journal.commit(batch, attempt, movement_id=movement_id)
                journal.identify_event(batch, attempt, event_id)
                identities.append((batch, attempt, movement_id, event_id))
                if command % 25 == 0:
                    snapshot = journal.summary()
                    snapshot['totals']['committed'] = -1
                    snapshot['requested_numeric_profile']['events'] = -1
                    snapshot['batches'][0]['requested'] = -1
            journal.finish_success(batch)
            return identities

        with ThreadPoolExecutor(max_workers=4) as executor:
            groups = list(executor.map(lane, range(4)))
        identities = [identity for group in groups for identity in group]
        final = journal.finalize()
        self.assertEqual({key: final['totals'][key] for key in
            ('requested', 'attempted', 'committed', 'identified_events',
             'failed_before_commit', 'post_commit_observation_failed', 'unattempted')},
            {'requested': 400, 'attempted': 400, 'committed': 400, 'identified_events': 400,
             'failed_before_commit': 0, 'post_commit_observation_failed': 0, 'unattempted': 0})
        self.assertEqual(len({identity[0] for identity in identities}), 4)
        self.assertEqual(len({identity[2] for identity in identities}), 400)
        self.assertEqual(len({identity[3] for identity in identities}), 400)
        self.assertEqual(final['requested_numeric_profile']['events'], 90000)
        self.assertTrue(all(batch['requested'] == 100 and batch['status'] == 'succeeded'
                            for batch in final['batches']))
        raw = [json.loads(line) for line in journal.journal_path.read_text().splitlines()]
        self.assertEqual(len(raw), 1210)  # frozen profile + 4 batches + 3*400 + 4 finishes + final
        self.assertEqual([item['sequence'] for item in raw], list(range(1, 1211)))
        commands = {}
        for item in raw:
            if 'attempt_id' in item:
                commands.setdefault((item['batch_id'], item['attempt_id']), []).append(item)
        self.assertEqual(len(commands), 400)
        for batch, attempt, movement_id, event_id in identities:
            records = commands[(batch, attempt)]
            self.assertEqual([item['action'] for item in records],
                             ['command_attempted', 'command_committed', 'event_identified'])
            self.assertEqual(records[1]['movement_id'], movement_id)
            self.assertEqual(records[2]['event_id'], event_id)
        committed = journal.committed_attempts()
        self.assertEqual(len(committed), 400)
        committed[0]['event_id'] = 'mutated-copy'
        self.assertNotEqual(journal.committed_attempts()[0]['event_id'], 'mutated-copy')
        self.assertEqual(json.loads(journal.summary_path.read_text()), final)
