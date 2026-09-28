"""The actual concurrent Harness retains global mix and partial commit truth."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from django.test import SimpleTestCase

from benchmarks.events.acceptance import Harness
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile


class HarnessLaneAccountingTests(SimpleTestCase):
    def harness(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        h = object.__new__(Harness)
        h.evidence = Path(directory.name)
        h.args = SimpleNamespace(run_id='lane-accounting', events=32, rate=1000,
            duration=0, fault_repetitions=1, fault_events=1, duplicate_events=1,
            poison_events=12, broker_fault_seconds=1, outage_seconds=1,
            consumer_outage_seconds=1, drain_timeout=900)
        h.generation = GenerationJournal(h.evidence, h.args.run_id, numeric_profile(h.args))
        self.addCleanup(h.generation.finalize)
        h._next_command_index = 0
        h._event_lock = threading.Lock()
        h.generation_topologies, h.events = [], []
        h.business_lanes = [{'batch': None, 'cycle_issue': None} for _ in range(4)]
        h.connections = Mock()
        h.shutdown_threads = []
        h.connections.close_all.side_effect = lambda: h.shutdown_threads.append(threading.get_ident())
        return h

    def command(self, h, *, failure_index=None, original=None):
        attempted = []
        lock = threading.Lock()

        def execute(seq, label, batch, state, data, *, lane, scheduled_at):
            with lock:
                attempted.append((lane, seq))
            attempt = h.generation.attempt(batch)
            state.update(attempt=attempt, stage='business_transaction')
            # Sleeping releases the GIL and forces lane overlap without a DB.
            time.sleep(.002)
            movement, event = str(uuid4()), str(uuid4())
            h.generation.commit(batch, attempt, movement)
            state['stage'] = 'outbox_observation'
            if seq == failure_index:
                raise original
            h.generation.identify_event(batch, attempt, event)
            item = {'event_id': event, 'movement_id': movement, 'global_index': seq,
                'business_lane': lane, 'scenario': label,
                'kind': ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')[seq % 4]}
            data['batch'] = SimpleNamespace(id=uuid4())
            with h._event_lock:
                h.events.append(item)
            state['attempt'] = None
            return item
        h._execute_inventory_command = execute
        return attempted

    def test_actual_concurrent_wrapper_keeps_exact_global_mix_lane_batches_and_cleanup(self):
        h = self.harness()
        attempted = self.command(h)
        ids, workload = h.generate(32, 'steady')
        self.assertEqual(len(ids), 32)
        self.assertEqual(len(set(ids)), 32)
        self.assertEqual([row['global_index'] for row in h.events], list(range(32)))
        self.assertEqual(ids, [row['event_id'] for row in h.events])
        self.assertEqual({kind: sum(row['kind'] == kind for row in h.events)
            for kind in ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')},
            {'RECEIPT': 8, 'ISSUE': 8, 'TRANSFER': 8, 'REVERSAL': 8})
        for lane in range(4):
            self.assertEqual([seq for owner, seq in attempted if owner == lane],
                [seq for seq in range(32) if (seq // 4) % 4 == lane])
        summary = h.generation.summary()
        self.assertEqual([batch['label'] for batch in summary['batches']], ['steady'] * 4)
        self.assertEqual([batch['committed'] for batch in summary['batches']], [8] * 4)
        self.assertEqual(summary['totals']['committed'], 32)
        self.assertEqual(workload['completed_commands'], 32)
        self.assertEqual(workload['actual_command_rate'], 32 / workload['elapsed_seconds'])
        self.assertEqual(len(h.shutdown_threads), 4)
        self.assertNotIn(threading.get_ident(), h.shutdown_threads)
        self.assertEqual(len(set(h.shutdown_threads)), 4)
        topology = json.loads((h.evidence / 'generation-topology-001.json').read_text())
        self.assertTrue(topology['passed'])
        self.assertEqual(sum(batch['requested'] for batch in topology['journal_batches']), 32)

    def test_partial_lane_failure_retains_original_commits_and_unattempted_denominator(self):
        h = self.harness()
        original = RuntimeError('post-commit observation failed')
        attempted = self.command(h, failure_index=4, original=original)
        with self.assertRaises(RuntimeError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        summary = h.generation.summary()
        self.assertEqual(summary['totals']['requested'], 32)
        self.assertEqual(summary['totals']['attempted'], len(attempted))
        self.assertEqual(summary['totals']['committed'], len(attempted))
        self.assertEqual(summary['totals']['post_commit_observation_failed'], 1)
        self.assertEqual(summary['totals']['failed_before_commit'], 0)
        self.assertEqual(summary['totals']['identified_events'], len(attempted) - 1)
        self.assertEqual(summary['totals']['unattempted'], 32 - len(attempted))
        self.assertGreater(summary['totals']['unattempted'], 0)
        self.assertTrue(all(batch['status'] in {'succeeded', 'failed'} for batch in summary['batches']))
        self.assertEqual(len(h.shutdown_threads), 4)
        topology = json.loads((h.evidence / 'generation-topology-001.json').read_text())
        self.assertFalse(topology['passed'])
        self.assertEqual(topology['error_type'], 'RuntimeError')
        self.assertEqual(sum(batch['committed'] for batch in topology['batches']), len(attempted))

    def complete_finish(self, h, error):
        h.args.tier = 'smoke'
        h.children, h.logs, h.shutdowns, h.supervisor_restarts = [], [], [], []
        h.workers, h.cases, h.delivery_proofs = {}, [], []
        h.started_at = 0
        h.models = Mock()
        h.models.FailedDelivery.objects.order_by.return_value.values.return_value = []
        (h.evidence / 'errors.jsonl').touch()
        h.final_inventory_evidence = Mock(return_value={
            'database_observed': True, 'offsets_observed': True, 'errors': [],
            'actual_inventory_outbox_count': 32, 'unpublished_count': 0,
            'consumers': {role: {'incomplete_count': 0} for role in ('notification', 'analytics')},
            'event_log_ids_missing_from_database': [], 'actual_committed_ids_missing_from_event_log': [],
            'journal_committed_movements_missing_from_database': [],
            'database_committed_movements_missing_from_journal': [],
            'journal_identified_events_missing_from_database': [], 'processed_hash_conflicts': [],
            'reconciliation': {'mismatches': [], 'dedupe_count': 64,
                'notification_count': 0, 'expected_notification_count': 0}})
        report = h.finish(error)
        self.assertTrue(report['generation_accounting_complete'])
        self.assertTrue(report['final_reconciliation_complete'])
        self.assertFalse(report['generation_topologies_complete'])
        self.assertFalse(report['final_inventory_complete'])
        self.assertFalse(report['passed'], 'A complete journal/effect count must not hide orchestration failure')
        return report

    def test_observer_failure_after_all_commits_keeps_topology_and_final_report_failed(self):
        h = self.harness()
        self.command(h)
        original = OSError('summary observer failed')
        dumps = json.dumps

        def observe_json(value, *args, **kwargs):
            if isinstance(value, dict) and value.get('kind') == 'summary':
                raise original
            return dumps(value, *args, **kwargs)

        with patch('benchmarks.events.acceptance.json.dumps', side_effect=observe_json):
            with self.assertRaises(OSError) as raised:
                h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        self.assertEqual(h.generation.summary()['totals']['committed'], 32)
        self.assertFalse(json.loads((h.evidence / 'generation-topology-001.json').read_text())['passed'])
        self.complete_finish(h, None)  # Even a caller that caught the error cannot report PASS.

    def test_cleanup_failure_after_all_commits_keeps_exact_counts_and_failed_report(self):
        h = self.harness()
        self.command(h)
        original = OSError('own-thread cleanup failed')
        calls = []

        def cleanup():
            calls.append(threading.get_ident())
            # Ensure all worker commands have completed before cleanup fails.
            if h.generation.summary()['totals']['identified_events'] == 32:
                raise original

        h.connections.close_all.side_effect = cleanup
        with self.assertRaises(OSError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        self.assertEqual(h.generation.summary()['totals']['identified_events'], 32)
        self.assertEqual(len(calls), 4)
        self.assertNotIn(threading.get_ident(), calls)
        self.assertFalse(json.loads((h.evidence / 'generation-topology-001.json').read_text())['passed'])
        self.complete_finish(h, original)
