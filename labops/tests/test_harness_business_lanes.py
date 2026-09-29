"""Real PostgreSQL business coverage for the bounded acceptance generator.

This small integration fixture checks commands and consumer effects. It does
not start Kafka, publish records, or establish the full capacity target.
"""
from collections import Counter
from decimal import Decimal
import io
import json
import math
import os
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

    def __init__(self, *, record_backend_identity=False):
        self.closed = []
        self.lock = threading.Lock()
        self.record_backend_identity = record_backend_identity

    def close_all(self):
        wrapper = connections['default']
        current = threading.current_thread()
        raw_connection = wrapper.connection
        before = {
            'thread': current,
            'thread_id': current.ident,
            'wrapper_id': id(wrapper),
            'wrapper': wrapper,
            'backend_pid': raw_connection.info.backend_pid if raw_connection else None,
            'in_atomic_block': wrapper.in_atomic_block,
        }
        if self.record_backend_identity:
            before['backend_start'] = None
            if raw_connection is not None:
                with wrapper.cursor() as cursor:
                    cursor.execute('SELECT backend_start FROM pg_stat_activity WHERE pid = %s',
                                   [before['backend_pid']])
                    before['backend_start'] = cursor.fetchone()[0]
        connections.close_all()
        before['connection_closed'] = wrapper.connection is None
        with self.lock:
            self.closed.append(before)


@skipUnless(connection.vendor == 'postgresql', 'Real PostgreSQL lane transactions required')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class HarnessBusinessLaneTests(TransactionTestCase):
    def setUp(self):
        # TransactionTestCase flushes remove migration-created data between
        # classes. Restore the same singleton required by legal opening stock.
        models.RuntimeState.objects.get_or_create(pk=1)

    def writer_evidence_directory(self, name):
        supplied = os.environ.get('LABOPS_WRITER_PROOF_EVIDENCE')
        if supplied:
            directory = Path(supplied) / name
            directory.mkdir(parents=True, exist_ok=False)
            return directory
        temporary = tempfile.TemporaryDirectory(prefix='labops-' + name + '-')
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def make_harness(self, directory, *, writer_topology=None,
                     writer_topology_version=None, event_count=32):
        harness = object.__new__(Harness)
        harness.args = SimpleNamespace(
            run_id='lanes-pg-integration', events=event_count, rate=1000.0, duration=0.0,
            fault_repetitions=1, fault_events=4, duplicate_events=1,
            poison_events=1, broker_fault_seconds=1.0, outage_seconds=1.0,
            consumer_outage_seconds=1.0, drain_timeout=900.0,
            evidence_dir=directory, generated_dir=directory, tier='smoke',
        )
        if writer_topology is not None:
            harness.args.writer_topology = writer_topology
        if writer_topology_version is not None:
            harness.args.writer_topology_version = writer_topology_version
        if writer_topology is not None:
            lane_count = harness.writer_lanes
            profile = {'scope': 'real PostgreSQL thread fixture below the CLI capacity threshold',
                'writer_topology': writer_topology, 'writer_topology_version': writer_topology_version,
                'lane_count': lane_count, 'cycle_length': 4, 'queue_capacity_per_lane': 4,
                'generator_topology': 'parallel-lanes-v1', 'process_generation': False,
                'generation_counts': [lane_count * 8, 2, lane_count * 4 - 2],
                'generation_modes': ['threads', 'serial', 'threads'],
                'capacity_target_proven': False, 'kafka_publication_proven': False}
            with (directory / 'fixture-execution-profile.json').open('x') as output:
                json.dump(profile, output, sort_keys=True)
                output.write('\n')
                output.flush()
                os.fsync(output.fileno())
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
        harness.connections = ObservedConnections(record_backend_identity=writer_topology is not None)
        harness.models, harness.api, harness.services = models, events, services
        harness.admin = models.User.objects.get(email='admin@labops.local')
        harness.reviewer = models.User.objects.get(email='reviewer@labops.local')
        harness.task = models.Task.objects.filter(
            status='IN_PROGRESS', project__status='ACTIVE').first()
        harness.source = models.Warehouse.objects.get(code='WH-01')
        harness.target = models.Warehouse.objects.get(code='WH-03')
        harness.batch = models.Batch.objects.filter(item__is_active=True).order_by('created_at').first()
        return harness

    def assert_business_cycles(self, harness, movements, cycles=2, *, lane_count=4):
        self.assertEqual(len(harness.business_lanes), lane_count)
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
                for receipt_index in (lane * 4 + cycle * lane_count * 4 for cycle in range(cycles)):
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
        self.assertEqual(len(batches), lane_count * cycles)
        return batches

    def assert_closed_thread_sessions(self, observations, main_backend_pid):
        self.assertTrue(all(row['connection_closed'] and not row['in_atomic_block']
                            and not row['thread'].is_alive() for row in observations))
        identities = [(row['backend_pid'], row['backend_start']) for row in observations
                      if row['backend_pid'] is not None]
        self.assertNotIn(main_backend_pid, {pid for pid, _start in identities})
        self.assertTrue(all(start is not None for _pid, start in identities))
        queries = []
        with connection.cursor() as cursor:
            for pid, backend_start in identities:
                sql = 'SELECT count(*) FROM pg_stat_activity WHERE pid = %s AND backend_start = %s'
                cursor.execute(sql, [pid, backend_start])
                actual_count = cursor.fetchone()[0]
                queries.append({'sql': sql, 'parameters': [pid, backend_start.isoformat()],
                                'actual_count': actual_count})
                self.assertEqual(actual_count, 0)
        return queries

    def write_writer_business_proof(self, directory, harness, movements, ids, *, modes,
                                    generation_counts, baseline_movements, session_queries,
                                    process_batches=(), continuity=()):
        # These rows are queried from the real transaction database after both
        # original and duplicate deliveries; they are not synthetic totals.
        with connection.cursor() as cursor:
            sql = 'SELECT pg_backend_pid(), current_database()'
            cursor.execute(sql)
            parent_identity = cursor.fetchone()
        outbox = models.OutboxEvent.objects.filter(pk__in=ids).order_by('id')
        batches = {str(batch_id) for row in movements.values()
                   for batch_id in row.lines.values_list('batch_id', flat=True)}
        rows = {
            'fixture_scope': 'real PostgreSQL commands and effects; no Kafka publication or capacity qualification',
            'writer_topology': harness.args.writer_topology,
            'writer_topology_version': harness.args.writer_topology_version,
            'lane_count': len(harness.business_lanes),
            'actual_command_count': len(harness.events),
            'actual_per_kind': dict(Counter(item['kind'] for item in harness.events)),
            'actual_per_lane': dict(Counter(item['business_lane'] for item in harness.events)),
            'parent_process_pid': os.getpid(),
            'parent_identity_query': {'sql': sql, 'actual_row': list(parent_identity)},
            'modes': list(modes), 'generation_counts': list(generation_counts),
            'baseline_movement_count': baseline_movements,
            'actual_movement_count': models.StockMovement.objects.count(),
            'events': harness.events,
            'generation_topologies': harness.generation_topologies,
            'unfinished_cycle_continuity': list(continuity),
            'business_lanes': [{'lane': lane, **{key + '_id': str(data[key].id)
                                for key in ('project', 'task', 'order', 'order_line')},
                                'cycle_issue_id': str(data['cycle_issue'].id) if data['cycle_issue'] else None,
                                'batch_id': str(data['batch'].id) if data['batch'] else None}
                               for lane, data in enumerate(harness.business_lanes)],
            'projects': list(models.Project.objects.filter(pk__in=[data['project'].id for data in harness.business_lanes])
                             .order_by('id').values('id', 'status')),
            'tasks': list(models.Task.objects.filter(pk__in=[data['task'].id for data in harness.business_lanes])
                          .order_by('id').values('id', 'status', 'project_id', 'assignee_id')),
            'orders': list(models.PurchaseOrder.objects.filter(pk__in=[data['order'].id for data in harness.business_lanes])
                           .order_by('id').values('id', 'status')),
            'order_lines': list(models.OrderLine.objects.filter(pk__in=[data['order_line'].id for data in harness.business_lanes])
                                .order_by('id').values('id', 'order_id', 'qty')),
            'receipts': list(models.Receipt.objects.filter(order_id__in=[data['order'].id for data in harness.business_lanes])
                             .order_by('id').values('id', 'order_id', 'status')),
            'receipt_lines': list(models.ReceiptLine.objects.filter(order_line_id__in=[data['order_line'].id for data in harness.business_lanes])
                                  .order_by('id').values('id', 'receipt_id', 'order_line_id', 'batch_id', 'warehouse_id', 'qty')),
            'movements': list(models.StockMovement.objects.filter(pk__in=[row.id for row in movements.values()])
                              .order_by('id').values('id', 'type', 'status', 'idempotency_key',
                                                     'request_hash', 'receipt_id', 'reversal_of_id')),
            'movement_lines': list(models.StockMovementLine.objects.filter(movement_id__in=[row.id for row in movements.values()])
                                   .order_by('id').values('id', 'movement_id', 'batch_id', 'warehouse_id',
                                                          'task_id', 'delta_qty', 'receipt_line_id',
                                                          'reversal_of_line_id', 'transfer_pair_no')),
            'outbox': list(outbox.values('id', 'event_type', 'aggregate_id', 'status', 'payload_hash')),
            'consumer_markers': list(models.ProcessedEvent.objects.filter(event_id__in=ids)
                                   .order_by('consumer_name', 'event_id')
                                   .values('consumer_name', 'event_id', 'payload_hash')),
            'notifications': list(models.Notification.objects.filter(event_id__in=ids)
                                 .order_by('event_id', 'user_id').values('event_id', 'user_id', 'title')),
            'balances': list(models.StockBalance.objects.filter(batch_id__in=batches)
                             .order_by('batch_id', 'warehouse_id').values('batch_id', 'warehouse_id', 'on_hand_qty')),
            'projections': list(models.InventoryProjection.objects.filter(batch_id__in=batches)
                                .order_by('batch_id', 'warehouse_id').values('batch_id', 'warehouse_id', 'quantity')),
            'actual_snapshot_after_duplicates': harness.snapshot(ids),
            'actual_reconciliation': services.reconcile(),
            'owned_thread_close_observations': [
                {key: value for key, value in row.items() if key not in ('thread', 'wrapper')}
                | {'thread_name': row['thread'].name, 'thread_alive': row['thread'].is_alive(),
                   'wrapper_connection_is_none': row['wrapper'].connection is None}
                for row in getattr(harness.connections, 'closed', ())],
            'owned_session_absence_queries': session_queries,
            'process_batches': list(process_batches),
        }
        with (directory / 'writer-business-proof.json').open('x') as output:
            json.dump(rows, output, indent=2, sort_keys=True, default=str)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())

    def test_four_lanes_commit_real_commands_and_preserve_consumer_effects(self):
        self.exercise_business_lanes()

    def test_six_lanes_commit_real_commands_and_preserve_consumer_effects(self):
        self.exercise_business_lanes(lane_count=6, writer_topology='writers-6',
                                     writer_topology_version='writer-topology-v1')

    def exercise_business_lanes(self, *, lane_count=4, writer_topology=None,
                                writer_topology_version=None):
        first_count = lane_count * 8
        continued_count = lane_count * 4 - 2
        total = lane_count * 12
        if writer_topology is None:
            temporary = tempfile.TemporaryDirectory(prefix='labops-real-lanes-')
            self.addCleanup(temporary.cleanup)
            directory = Path(temporary.name)
        else:
            directory = self.writer_evidence_directory('six-thread')
        # Commit the demo and ledger-derived baseline before enabling Kafka
        # outbox mode; worker connections must be able to see these records.
        with override_settings(EVENT_TRANSPORT='local'):
            output = io.StringIO()
            call_command('seed_demo', stdout=output)
            call_command('rebuild_inventory_projection', stdout=output)
        harness = (self.make_harness(directory) if writer_topology is None else
                   self.make_harness(directory, writer_topology=writer_topology,
                                     writer_topology_version=writer_topology_version,
                                     event_count=first_count))
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
                self.assertEqual(len({data[key].id for data in harness.business_lanes}), lane_count)
            ids, workload = harness.generate(first_count, 'steady', rate=1000.0)

        self.assertEqual(workload['input'], first_count)
        self.assertEqual(workload['completed_commands'], first_count)
        self.assertEqual(workload['generator_topology'], 'parallel-lanes-v1')
        self.assertEqual(workload['lane_count'], lane_count)
        self.assertEqual(workload['queue_capacity_per_lane'], 4)
        self.assertEqual(workload['target_rate'], 1000.0)
        self.assertEqual(len(ids), first_count)
        self.assertEqual(len(set(ids)), first_count)
        self.assertEqual([item['global_index'] for item in harness.events], list(range(first_count)))
        self.assertEqual([item['event_id'] for item in harness.events], ids)
        self.assertEqual(Counter(item['kind'] for item in harness.events),
                         {kind: lane_count * 2 for kind in ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')})
        self.assertEqual(Counter(item['business_lane'] for item in harness.events),
                         {lane: 8 for lane in range(lane_count)})
        for item in harness.events:
            self.assertEqual(item['business_lane'], (item['global_index'] // 4) % lane_count)
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
        self.assertEqual(len(outbox), first_count)
        self.assertEqual({str(row.id) for row in outbox}, set(ids))
        self.assertTrue(all(row.event_type.startswith('inventory.') for row in outbox))
        self.assertTrue(all(row.status == 'PENDING' and row.payload_hash for row in outbox))
        movement_ids = {item['movement_id'] for item in harness.events}
        self.assertEqual(len(movement_ids), first_count)
        self.assertEqual({str(row.aggregate_id) for row in outbox}, movement_ids)
        self.assertEqual(models.StockMovement.objects.count(), baseline_movements + first_count)
        movements = {item['global_index']: models.StockMovement.objects.get(pk=item['movement_id'])
                     for item in harness.events}
        self.assertEqual({row.idempotency_key for row in movements.values()},
                         {f'{harness.args.run_id}:{index}' for index in range(first_count)})
        self.assert_business_cycles(harness, movements, lane_count=lane_count)

        summary = harness.generation.summary()
        self.assertEqual(summary['totals'], {
            'requested': first_count, 'attempted': first_count, 'committed': first_count,
            'failed_before_commit': 0, 'post_commit_observation_failed': 0,
            'identified_events': first_count, 'unattempted': 0,
            'pending_before_commit': 0, 'pending_observation': 0,
        })
        self.assertEqual(len(summary['batches']), lane_count)
        self.assertTrue(all(batch['requested'] == batch['committed'] == 8
                            and batch['status'] == 'succeeded' for batch in summary['batches']))
        attempts = harness.generation.committed_attempts()
        self.assertEqual(len(attempts), first_count)
        self.assertEqual({item['event_id'] for item in attempts}, set(ids))
        self.assertEqual({item['movement_id'] for item in attempts}, movement_ids)
        journal = [json.loads(line) for line in harness.generation.journal_path.read_text().splitlines()]
        self.assertEqual(Counter(row['action'] for row in journal)['command_committed'], first_count)
        self.assertEqual(Counter(row['action'] for row in journal)['event_identified'], first_count)
        self.assertEqual(json.loads(harness.generation.summary_path.read_text()), summary)

        observations = [json.loads(line) for line in
                        (directory / 'generation-schedule-001.jsonl').read_text().splitlines()]
        scheduling = next(row for row in observations if row['kind'] == 'summary')
        self.assertTrue(scheduling['passed'])
        self.assertTrue(scheduling['worker_threads_joined'])
        self.assertTrue(scheduling['worker_completion_observed'])
        for field in ('scheduled_indices', 'started_indices', 'completed_indices'):
            self.assertEqual(scheduling[field], list(range(first_count)))
        self.assertEqual(scheduling['failed_count'], 0)
        self.assertEqual(scheduling['cancelled_count'], 0)
        self.assertEqual(scheduling['unscheduled_count'], 0)
        self.assertEqual(scheduling['lane_shutdowns'],
                         [{'lane': lane, 'passed': True, 'error_type': None} for lane in range(lane_count)])
        closed = harness.connections.closed
        self.assertEqual(len(closed), lane_count)
        for field in ('thread_id', 'wrapper_id', 'backend_pid'):
            self.assertEqual(len({row[field] for row in closed}), lane_count)
        self.assertTrue(all(row['connection_closed'] and not row['in_atomic_block']
                            and not row['thread'].is_alive() for row in closed))
        self.assertEqual({row['thread'].name for row in closed},
                         {f'paced-business-lane-{lane}' for lane in range(lane_count)})
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
            self.assertEqual([item['global_index'] for item in harness.events[-2:]], [first_count, first_count + 1])
            original_issue = harness.business_lanes[0]['cycle_issue']
            original_batch = harness.business_lanes[0]['batch']
            self.assertIsNotNone(original_issue)
            self.assertEqual(str(original_issue.id), harness.events[-1]['movement_id'])
            continued_ids, continued_workload = harness.generate(continued_count, 'steady', rate=1000.0)
        self.assertEqual(continued_workload['generator_topology'], 'parallel-lanes-v1')
        self.assertEqual(continued_workload['completed_commands'], continued_count)
        ids += serial_ids + continued_ids
        self.assertEqual(len(ids), total)
        self.assertEqual(len(set(ids)), total)
        self.assertEqual([item['global_index'] for item in harness.events], list(range(total)))
        self.assertEqual([item['event_id'] for item in harness.events], ids)
        self.assertEqual(Counter(item['kind'] for item in harness.events),
                         {kind: lane_count * 3 for kind in ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')})
        self.assertEqual(Counter(item['business_lane'] for item in harness.events),
                         {lane: 12 for lane in range(lane_count)})
        movements = {item['global_index']: models.StockMovement.objects.get(pk=item['movement_id'])
                     for item in harness.events}
        self.assertEqual(movements[first_count + 3].reversal_of_id, original_issue.id)
        self.assertEqual(movements[first_count + 2].lines.first().batch_id, original_batch.id)
        self.assertEqual({row.idempotency_key for row in movements.values()},
                         {f'{harness.args.run_id}:{index}' for index in range(total)})
        self.assertEqual(models.StockMovement.objects.count(), baseline_movements + total)
        outbox = list(models.OutboxEvent.objects.filter(transport='kafka'))
        self.assertEqual(len(outbox), total)
        self.assertEqual({str(row.id) for row in outbox}, set(ids))
        self.assertEqual({str(row.aggregate_id) for row in outbox},
                         {str(row.id) for row in movements.values()})
        batches = self.assert_business_cycles(harness, movements, cycles=3, lane_count=lane_count)
        for item in harness.events[first_count:]:
            self.assertEqual(item['business_lane'], (item['global_index'] // 4) % lane_count)
            committed_at = item['outbox_transaction_commit_at']
            if tracks_commits:
                self.assertIsNotNone(committed_at)
                self.assertTrue(math.isfinite(committed_at))
                self.assertGreaterEqual(committed_at, item['command_started_at'])
            else:
                self.assertIsNone(committed_at)

        final_summary = harness.generation.summary()
        for field in ('requested', 'attempted', 'committed', 'identified_events'):
            self.assertEqual(final_summary['totals'][field], total)
        for field in ('failed_before_commit', 'post_commit_observation_failed',
                      'unattempted', 'pending_before_commit', 'pending_observation'):
            self.assertEqual(final_summary['totals'][field], 0)
        self.assertEqual([batch['requested'] for batch in final_summary['batches']],
                         [8] * lane_count + [2, 2] + [4] * (lane_count - 1))
        self.assertTrue(all(batch['status'] == 'succeeded' for batch in final_summary['batches']))
        self.assertEqual(json.loads(harness.generation.summary_path.read_text()), final_summary)
        attempts = harness.generation.committed_attempts()
        self.assertEqual(len(attempts), total)
        self.assertEqual({item['event_id'] for item in attempts}, set(ids))
        self.assertEqual({item['movement_id'] for item in attempts},
                         {str(row.id) for row in movements.values()})
        journal = [json.loads(line) for line in harness.generation.journal_path.read_text().splitlines()]
        self.assertEqual([row['sequence'] for row in journal], list(range(1, len(journal) + 1)))
        committed = {(row['batch_id'], row['attempt_id']): row['movement_id']
                     for row in journal if row['action'] == 'command_committed'}
        identified = {(row['batch_id'], row['attempt_id']): row['event_id']
                      for row in journal if row['action'] == 'event_identified'}
        self.assertEqual(len(committed), total)
        self.assertEqual(len(identified), total)
        self.assertEqual(committed.keys(), identified.keys())
        if writer_topology is not None:
            self.assertEqual(set(identified.values()), set(ids))
            self.assertEqual(set(committed.values()), {str(row.id) for row in movements.values()})
            attempted = {(row['batch_id'], row['attempt_id'])
                         for row in journal if row['action'] == 'command_attempted'}
            self.assertEqual(Counter(row['action'] for row in journal)['command_attempted'], total)
            self.assertEqual(attempted, committed.keys())
        actual_event_movements = {str(row.id): str(row.aggregate_id) for row in outbox}
        for attempt_key, eid in identified.items():
            self.assertEqual(actual_event_movements[eid], committed[attempt_key])
        self.assertEqual(Counter(row['action'] for row in journal)['batch_succeeded'], lane_count * 2 + 1)
        second_observations = [json.loads(line) for line in
                               (directory / 'generation-schedule-002.jsonl').read_text().splitlines()]
        second_summary = next(row for row in second_observations if row['kind'] == 'summary')
        self.assertTrue(second_summary['passed'])
        self.assertTrue(second_summary['worker_threads_joined'])
        self.assertTrue(second_summary['worker_completion_observed'])
        self.assertEqual(second_summary['completed_indices'], list(range(first_count + 2, total)))
        self.assertEqual(second_summary['lane_shutdowns'],
                         [{'lane': lane, 'passed': True, 'error_type': None} for lane in range(lane_count)])
        self.assertEqual(len(harness.connections.closed), lane_count * 2)
        continued_closed = harness.connections.closed[lane_count:]
        for field in ('thread_id', 'wrapper_id', 'backend_pid'):
            self.assertEqual(len({row[field] for row in continued_closed}), lane_count)
        self.assertTrue(all(row['connection_closed'] and not row['in_atomic_block']
                            and not row['thread'].is_alive() for row in continued_closed))

        session_queries = []
        if writer_topology is not None:
            session_queries = self.assert_closed_thread_sessions(harness.connections.closed, main_pid)

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
        self.assertEqual(after['dedupe_count'], total * 2)
        self.assertEqual(after['consumer_counts'], {'notification': total, 'analytics': total})
        self.assertGreater(after['expected_notification_count'], 0)
        self.assertEqual(after['notification_count'], after['expected_notification_count'])
        self.assertEqual(models.FailedDelivery.objects.count(), 0)
        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertFalse(events.process_envelope(consumer, events.envelope(
                    models.OutboxEvent.objects.get(pk=eid))))
        self.assertEqual(harness.snapshot(ids), after)
        self.assertEqual(services.reconcile(), [])
        if writer_topology is not None:
            self.write_writer_business_proof(directory, harness, movements, ids,
                modes=['threads', 'serial', 'threads'],
                generation_counts=[first_count, 2, continued_count],
                baseline_movements=baseline_movements, session_queries=session_queries,
                continuity=[{'preceding_issue_index': first_count + 1,
                    'original_issue_id': str(original_issue.id), 'original_batch_id': str(original_batch.id),
                    'continued_transfer_index': first_count + 2,
                    'actual_transfer_batch_ids': list(movements[first_count + 2].lines.values_list('batch_id', flat=True)),
                    'continued_reversal_index': first_count + 3,
                    'actual_reversal_of_id': str(movements[first_count + 3].reversal_of_id)}])
        print(json.dumps({'real_pg_business_lanes': lane_count, 'commands': total,
                          'generation_counts': [first_count, 2, continued_count],
                          'committed_journal_records': total, 'kafka_outbox_rows': total,
                          'tracked_commit_timestamps': total if tracks_commits else 0,
                          'closed_thread_connections': lane_count * 2, 'consumer_markers': total * 2,
                          'duplicate_effects': 0, 'reconciliation_mismatches': 0}, sort_keys=True))
