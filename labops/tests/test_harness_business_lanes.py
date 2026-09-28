"""Real PostgreSQL business coverage for the bounded acceptance generator.

This small integration fixture checks commands and consumer effects. It does
not start Kafka, publish records, or establish the full capacity target.
"""
from collections import Counter
from decimal import Decimal
import io
import json
import math
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
from unittest import skipUnless

from django.conf import settings
from django.core.management import call_command
from django.db import connection, connections
from django.test import TransactionTestCase, override_settings

from benchmarks.events.acceptance import Harness
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
from labops import events, models
from labops.inventory import services
from labops.purchasing.services import received_qty


class ObservedConnections:
    """Observe the real thread-local close without replacing database work."""

    def __init__(self):
        self.closed = []
        self.lock = threading.Lock()

    def close_all(self):
        wrapper = connections['default']
        current = threading.current_thread()
        before = {
            'thread': current,
            'thread_id': current.ident,
            'wrapper_id': id(wrapper),
            'wrapper': wrapper,
            'backend_pid': wrapper.connection.info.backend_pid,
            'in_atomic_block': wrapper.in_atomic_block,
        }
        connections.close_all()
        before['connection_closed'] = wrapper.connection is None
        with self.lock:
            self.closed.append(before)


@skipUnless(connection.vendor == 'postgresql', 'Real PostgreSQL lane transactions required')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class HarnessBusinessLaneTests(TransactionTestCase):
    def make_harness(self, directory):
        harness = object.__new__(Harness)
        harness.args = SimpleNamespace(
            run_id='lanes-pg-integration', events=32, rate=1000.0, duration=0.0,
            fault_repetitions=1, fault_events=4, duplicate_events=1,
            poison_events=1, broker_fault_seconds=1.0, outage_seconds=1.0,
            consumer_outage_seconds=1.0, drain_timeout=900.0,
            evidence_dir=directory, generated_dir=directory, tier='smoke',
        )
        harness.evidence = directory
        harness.generation = GenerationJournal(
            directory, harness.args.run_id, numeric_profile(harness.args))
        self.addCleanup(harness.generation.finalize)
        harness.events = []
        harness.workers = {}
        harness._event_lock = threading.Lock()
        harness._next_command_index = 0
        harness.generation_topologies = []
        harness.settings = settings
        harness.call_command = call_command
        harness.connection = connection  # Resolves the owning lane's real wrapper.
        harness.connections = ObservedConnections()
        harness.models, harness.api, harness.services = models, events, services
        harness.admin = models.User.objects.get(email='admin@labops.local')
        harness.reviewer = models.User.objects.get(email='reviewer@labops.local')
        harness.task = models.Task.objects.filter(
            status='IN_PROGRESS', project__status='ACTIVE').first()
        harness.source = models.Warehouse.objects.get(code='WH-01')
        harness.target = models.Warehouse.objects.get(code='WH-03')
        harness.batch = models.Batch.objects.filter(item__is_active=True).order_by('created_at').first()
        return harness

    def assert_business_cycles(self, harness, movements, cycles=2):
        batches = set()
        for lane, data in enumerate(harness.business_lanes):
            with self.subTest(lane=lane):
                self.assertEqual(data['project'].status, 'ACTIVE')
                self.assertEqual(data['task'].status, 'IN_PROGRESS')
                self.assertEqual(data['task'].project_id, data['project'].id)
                self.assertEqual(data['task'].assignee_id, harness.admin.id)
                self.assertEqual(data['order'].status, 'CONFIRMED')
                self.assertEqual(data['order_line'].order_id, data['order'].id)
                self.assertEqual(received_qty(data['order_line']), Decimal(4 * cycles))
                self.assertIsNone(data['cycle_issue'])
                lane_batches = set()
                for receipt_index in (lane * 4 + cycle * 16 for cycle in range(cycles)):
                    receipt, issue, transfer, reversal = (
                        movements[receipt_index + offset] for offset in range(4))
                    self.assertEqual(receipt.type, 'RECEIPT')
                    self.assertEqual(receipt.receipt.status, 'POSTED')
                    self.assertEqual(receipt.receipt.order_id, data['order'].id)
                    receipt_line = receipt.receipt.lines.get()
                    self.assertEqual(receipt_line.order_line_id, data['order_line'].id)
                    self.assertEqual(receipt_line.qty, Decimal('4'))
                    self.assertEqual(receipt_line.warehouse_id, harness.source.id)
                    posted_receipt_line = receipt.lines.get()
                    self.assertEqual(posted_receipt_line.receipt_line_id, receipt_line.id)
                    self.assertEqual(posted_receipt_line.warehouse_id, harness.source.id)
                    self.assertEqual(posted_receipt_line.delta_qty, Decimal('4'))
                    batch_id = receipt_line.batch_id
                    lane_batches.add(batch_id)
                    for movement in (receipt, issue, transfer, reversal):
                        self.assertEqual(movement.status, 'POSTED')
                        self.assertEqual(set(movement.lines.values_list('batch_id', flat=True)), {batch_id})
                    issue_line = issue.lines.get()
                    self.assertEqual(issue.type, 'ISSUE')
                    self.assertEqual(issue_line.task_id, data['task'].id)
                    self.assertEqual(issue_line.warehouse_id, harness.source.id)
                    self.assertEqual(issue_line.delta_qty, Decimal('-1'))
                    self.assertEqual(transfer.type, 'TRANSFER')
                    self.assertEqual(dict(transfer.lines.values_list('warehouse_id', 'delta_qty')),
                                     {harness.source.id: Decimal('-1'), harness.target.id: Decimal('1')})
                    self.assertEqual(transfer.lines.count(), 2)
                    self.assertEqual(set(transfer.lines.values_list('transfer_pair_no', flat=True)), {1})
                    self.assertEqual(reversal.type, 'REVERSAL')
                    self.assertEqual(reversal.reversal_of_id, issue.id)
                    reversed_line = reversal.lines.get()
                    self.assertEqual(reversed_line.reversal_of_line_id, issue_line.id)
                    self.assertEqual(reversed_line.task_id, data['task'].id)
                    self.assertEqual(reversed_line.warehouse_id, harness.source.id)
                    self.assertEqual(reversed_line.delta_qty, Decimal('1'))
                    self.assertEqual(dict(models.StockBalance.objects.filter(batch_id=batch_id)
                                          .values_list('warehouse_id', 'on_hand_qty')),
                                     {harness.source.id: Decimal('3'), harness.target.id: Decimal('1')})
                self.assertEqual(len(lane_batches), cycles)
                self.assertTrue(batches.isdisjoint(lane_batches))
                batches.update(lane_batches)
        self.assertEqual(len(batches), 4 * cycles)
        return batches

    def test_four_lanes_commit_real_commands_and_preserve_consumer_effects(self):
        temporary = tempfile.TemporaryDirectory(prefix='labops-real-lanes-')
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        # Commit the demo and ledger-derived baseline before enabling Kafka
        # outbox mode; worker connections must be able to see these records.
        with override_settings(EVENT_TRANSPORT='local'):
            output = io.StringIO()
            call_command('seed_demo', stdout=output)
            call_command('rebuild_inventory_projection', stdout=output)
        harness = self.make_harness(directory)
        self.assertEqual(harness.snapshot()['mismatches'], [])
        baseline_movements = models.StockMovement.objects.count()
        self.assertEqual(models.OutboxEvent.objects.filter(transport='kafka').count(), 0)
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            main_pid = cursor.fetchone()[0]
            cursor.execute('SHOW track_commit_timestamp')
            tracks_commits = cursor.fetchone()[0] == 'on'

        with override_settings(EVENT_TRANSPORT='kafka'):
            harness.prepare_order()
            for key in ('project', 'task', 'order', 'order_line'):
                self.assertEqual(len({data[key].id for data in harness.business_lanes}), 4)
            ids, workload = harness.generate(32, 'steady', rate=1000.0)

        self.assertEqual(workload['input'], 32)
        self.assertEqual(workload['completed_commands'], 32)
        self.assertEqual(workload['generator_topology'], 'parallel-lanes-v1')
        self.assertEqual(workload['lane_count'], 4)
        self.assertEqual(workload['queue_capacity_per_lane'], 4)
        self.assertEqual(workload['target_rate'], 1000.0)
        self.assertEqual(len(ids), 32)
        self.assertEqual(len(set(ids)), 32)
        self.assertEqual([item['global_index'] for item in harness.events], list(range(32)))
        self.assertEqual([item['event_id'] for item in harness.events], ids)
        self.assertEqual(Counter(item['kind'] for item in harness.events),
                         {'RECEIPT': 8, 'ISSUE': 8, 'TRANSFER': 8, 'REVERSAL': 8})
        self.assertEqual(Counter(item['business_lane'] for item in harness.events),
                         {0: 8, 1: 8, 2: 8, 3: 8})
        for item in harness.events:
            self.assertEqual(item['business_lane'], (item['global_index'] // 4) % 4)
            self.assertEqual(item['scenario'], 'steady')
            self.assertTrue(str(item['insert_transaction_xid']).isdigit())
            self.assertGreater(item['payload_bytes'], 0)
            committed_at = item['outbox_transaction_commit_at']
            if tracks_commits:
                self.assertIsNotNone(committed_at)
                self.assertTrue(math.isfinite(committed_at))
                self.assertGreaterEqual(committed_at, item['command_started_at'])
            else:
                self.assertIsNone(committed_at)

        outbox = list(models.OutboxEvent.objects.filter(transport='kafka'))
        self.assertEqual(len(outbox), 32)
        self.assertEqual({str(row.id) for row in outbox}, set(ids))
        self.assertTrue(all(row.event_type.startswith('inventory.') for row in outbox))
        self.assertTrue(all(row.status == 'PENDING' and row.payload_hash for row in outbox))
        movement_ids = {item['movement_id'] for item in harness.events}
        self.assertEqual(len(movement_ids), 32)
        self.assertEqual({str(row.aggregate_id) for row in outbox}, movement_ids)
        self.assertEqual(models.StockMovement.objects.count(), baseline_movements + 32)
        movements = {item['global_index']: models.StockMovement.objects.get(pk=item['movement_id'])
                     for item in harness.events}
        self.assertEqual({row.idempotency_key for row in movements.values()},
                         {f'{harness.args.run_id}:{index}' for index in range(32)})
        self.assert_business_cycles(harness, movements)

        summary = harness.generation.summary()
        self.assertEqual(summary['totals'], {
            'requested': 32, 'attempted': 32, 'committed': 32,
            'failed_before_commit': 0, 'post_commit_observation_failed': 0,
            'identified_events': 32, 'unattempted': 0,
            'pending_before_commit': 0, 'pending_observation': 0,
        })
        self.assertEqual(len(summary['batches']), 4)
        self.assertTrue(all(batch['requested'] == batch['committed'] == 8
                            and batch['status'] == 'succeeded' for batch in summary['batches']))
        attempts = harness.generation.committed_attempts()
        self.assertEqual(len(attempts), 32)
        self.assertEqual({item['event_id'] for item in attempts}, set(ids))
        self.assertEqual({item['movement_id'] for item in attempts}, movement_ids)
        journal = [json.loads(line) for line in harness.generation.journal_path.read_text().splitlines()]
        self.assertEqual(Counter(row['action'] for row in journal)['command_committed'], 32)
        self.assertEqual(Counter(row['action'] for row in journal)['event_identified'], 32)
        self.assertEqual(json.loads(harness.generation.summary_path.read_text()), summary)

        observations = [json.loads(line) for line in
                        (directory / 'generation-schedule-001.jsonl').read_text().splitlines()]
        scheduling = next(row for row in observations if row['kind'] == 'summary')
        self.assertTrue(scheduling['passed'])
        self.assertTrue(scheduling['worker_threads_joined'])
        self.assertTrue(scheduling['worker_completion_observed'])
        for field in ('scheduled_indices', 'started_indices', 'completed_indices'):
            self.assertEqual(scheduling[field], list(range(32)))
        self.assertEqual(scheduling['failed_count'], 0)
        self.assertEqual(scheduling['cancelled_count'], 0)
        self.assertEqual(scheduling['unscheduled_count'], 0)
        self.assertEqual(scheduling['lane_shutdowns'],
                         [{'lane': lane, 'passed': True, 'error_type': None} for lane in range(4)])
        closed = harness.connections.closed
        self.assertEqual(len(closed), 4)
        for field in ('thread_id', 'wrapper_id', 'backend_pid'):
            self.assertEqual(len({row[field] for row in closed}), 4)
        self.assertTrue(all(row['connection_closed'] and not row['in_atomic_block']
                            and not row['thread'].is_alive() for row in closed))
        self.assertEqual({row['thread'].name for row in closed},
                         {f'paced-business-lane-{lane}' for lane in range(4)})
        self.assertNotIn(main_pid, {row['backend_pid'] for row in closed})
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            self.assertEqual(cursor.fetchone()[0], main_pid)

        # Persist lane zero's receipt/issue across a serial fixture boundary.
        # The next parallel call starts midway through that existing cycle;
        # its reversal must retain the issue originally committed by serial work.
        with override_settings(EVENT_TRANSPORT='kafka'):
            serial_ids, serial_workload = harness.generate(2, 'fixture_serial', rate=1000.0)
            self.assertEqual(serial_workload['generator_topology'], 'serial_fault_fixture')
            self.assertEqual([item['global_index'] for item in harness.events[-2:]], [32, 33])
            original_issue = harness.business_lanes[0]['cycle_issue']
            original_batch = harness.business_lanes[0]['batch']
            self.assertIsNotNone(original_issue)
            self.assertEqual(str(original_issue.id), harness.events[-1]['movement_id'])
            continued_ids, continued_workload = harness.generate(14, 'steady', rate=1000.0)
        self.assertEqual(continued_workload['generator_topology'], 'parallel-lanes-v1')
        self.assertEqual(continued_workload['completed_commands'], 14)
        ids += serial_ids + continued_ids
        self.assertEqual(len(ids), 48)
        self.assertEqual(len(set(ids)), 48)
        self.assertEqual([item['global_index'] for item in harness.events], list(range(48)))
        self.assertEqual([item['event_id'] for item in harness.events], ids)
        self.assertEqual(Counter(item['kind'] for item in harness.events),
                         {'RECEIPT': 12, 'ISSUE': 12, 'TRANSFER': 12, 'REVERSAL': 12})
        self.assertEqual(Counter(item['business_lane'] for item in harness.events),
                         {0: 12, 1: 12, 2: 12, 3: 12})
        movements = {item['global_index']: models.StockMovement.objects.get(pk=item['movement_id'])
                     for item in harness.events}
        self.assertEqual(movements[35].reversal_of_id, original_issue.id)
        self.assertEqual(movements[34].lines.first().batch_id, original_batch.id)
        self.assertEqual({row.idempotency_key for row in movements.values()},
                         {f'{harness.args.run_id}:{index}' for index in range(48)})
        self.assertEqual(models.StockMovement.objects.count(), baseline_movements + 48)
        outbox = list(models.OutboxEvent.objects.filter(transport='kafka'))
        self.assertEqual(len(outbox), 48)
        self.assertEqual({str(row.id) for row in outbox}, set(ids))
        self.assertEqual({str(row.aggregate_id) for row in outbox},
                         {str(row.id) for row in movements.values()})
        batches = self.assert_business_cycles(harness, movements, cycles=3)
        for item in harness.events[32:]:
            self.assertEqual(item['business_lane'], (item['global_index'] // 4) % 4)
            committed_at = item['outbox_transaction_commit_at']
            if tracks_commits:
                self.assertIsNotNone(committed_at)
                self.assertTrue(math.isfinite(committed_at))
                self.assertGreaterEqual(committed_at, item['command_started_at'])
            else:
                self.assertIsNone(committed_at)

        final_summary = harness.generation.summary()
        for field in ('requested', 'attempted', 'committed', 'identified_events'):
            self.assertEqual(final_summary['totals'][field], 48)
        for field in ('failed_before_commit', 'post_commit_observation_failed',
                      'unattempted', 'pending_before_commit', 'pending_observation'):
            self.assertEqual(final_summary['totals'][field], 0)
        self.assertEqual([batch['requested'] for batch in final_summary['batches']],
                         [8, 8, 8, 8, 2, 2, 4, 4, 4])
        self.assertTrue(all(batch['status'] == 'succeeded' for batch in final_summary['batches']))
        self.assertEqual(json.loads(harness.generation.summary_path.read_text()), final_summary)
        attempts = harness.generation.committed_attempts()
        self.assertEqual(len(attempts), 48)
        self.assertEqual({item['event_id'] for item in attempts}, set(ids))
        self.assertEqual({item['movement_id'] for item in attempts},
                         {str(row.id) for row in movements.values()})
        journal = [json.loads(line) for line in harness.generation.journal_path.read_text().splitlines()]
        self.assertEqual([row['sequence'] for row in journal], list(range(1, len(journal) + 1)))
        committed = {(row['batch_id'], row['attempt_id']): row['movement_id']
                     for row in journal if row['action'] == 'command_committed'}
        identified = {(row['batch_id'], row['attempt_id']): row['event_id']
                      for row in journal if row['action'] == 'event_identified'}
        self.assertEqual(len(committed), 48)
        self.assertEqual(len(identified), 48)
        self.assertEqual(committed.keys(), identified.keys())
        actual_event_movements = {str(row.id): str(row.aggregate_id) for row in outbox}
        for attempt_key, eid in identified.items():
            self.assertEqual(actual_event_movements[eid], committed[attempt_key])
        self.assertEqual(Counter(row['action'] for row in journal)['batch_succeeded'], 9)
        second_observations = [json.loads(line) for line in
                               (directory / 'generation-schedule-002.jsonl').read_text().splitlines()]
        second_summary = next(row for row in second_observations if row['kind'] == 'summary')
        self.assertTrue(second_summary['passed'])
        self.assertTrue(second_summary['worker_threads_joined'])
        self.assertTrue(second_summary['worker_completion_observed'])
        self.assertEqual(second_summary['completed_indices'], list(range(34, 48)))
        self.assertEqual(second_summary['lane_shutdowns'],
                         [{'lane': lane, 'passed': True, 'error_type': None} for lane in range(4)])
        self.assertEqual(len(harness.connections.closed), 8)
        continued_closed = harness.connections.closed[4:]
        for field in ('thread_id', 'wrapper_id', 'backend_pid'):
            self.assertEqual(len({row[field] for row in continued_closed}), 4)
        self.assertTrue(all(row['connection_closed'] and not row['in_atomic_block']
                            and not row['thread'].is_alive() for row in continued_closed))

        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertTrue(events.process_envelope(consumer, events.envelope(
                    models.OutboxEvent.objects.get(pk=eid))))
        self.assertEqual(services.reconcile(), [])
        for batch_id in batches:
            self.assertEqual(dict(models.InventoryProjection.objects.filter(batch_id=batch_id)
                                  .values_list('warehouse_id', 'quantity')),
                             {harness.source.id: Decimal('3'), harness.target.id: Decimal('1')})
        after = harness.snapshot(ids)
        self.assertEqual(after['mismatches'], [])
        self.assertEqual(after['dedupe_count'], 96)
        self.assertEqual(after['consumer_counts'], {'notification': 48, 'analytics': 48})
        self.assertGreater(after['expected_notification_count'], 0)
        self.assertEqual(after['notification_count'], after['expected_notification_count'])
        self.assertEqual(models.FailedDelivery.objects.count(), 0)
        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertFalse(events.process_envelope(consumer, events.envelope(
                    models.OutboxEvent.objects.get(pk=eid))))
        self.assertEqual(harness.snapshot(ids), after)
        self.assertEqual(services.reconcile(), [])
        print(json.dumps({'real_pg_business_lanes': 4, 'commands': 48,
                          'generation_counts': [32, 2, 14],
                          'committed_journal_records': 48, 'kafka_outbox_rows': 48,
                          'tracked_commit_timestamps': 48 if tracks_commits else 0,
                          'closed_thread_connections': 8, 'consumer_markers': 96,
                          'duplicate_effects': 0, 'reconciliation_mismatches': 0}, sort_keys=True))
