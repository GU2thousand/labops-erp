"""Real PostgreSQL correctness of the acceptance-only spawned generator.

The small fixtures exercise the normal inventory and consumer service
APIs. It does not publish Kafka records or qualify any capacity target.
"""
from collections import Counter
import io
import json
import math
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
from urllib.parse import unquote, urlparse

from django.conf import settings
from django.core.management import call_command
from django.db import connection, connections
from django.test import override_settings

from benchmarks.events.acceptance import Harness
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
from benchmarks.events.origin_journal import _validate_database
from labops import events, models
from labops.inventory import services
from labops.tests import test_harness_business_lanes as fixture


class HarnessProcessLaneTests(fixture.HarnessBusinessLaneTests):
    def make_harness(self, directory, *, writer_topology=None,
                     writer_topology_version=None, event_count=32,
                     generation_counts=None, generation_modes=None):
        harness = object.__new__(Harness)
        harness.args = SimpleNamespace(
            run_id='spawn-pg-integration', events=event_count, rate=1000.0, duration=0.0,
            fault_repetitions=1, fault_events=4, duplicate_events=1,
            poison_events=1, broker_fault_seconds=1.0, outage_seconds=1.0,
            consumer_outage_seconds=1.0, drain_timeout=900.0,
            evidence_dir=directory, generated_dir=directory, tier='smoke',
            process_generation=True, runtime_diagnostics=False,
        )
        if writer_topology is not None:
            harness.args.writer_topology = writer_topology
        if writer_topology_version is not None:
            harness.args.writer_topology_version = writer_topology_version
        lane_count = harness.writer_lanes if writer_topology is not None else 4
        harness.evidence = directory
        fixture_profile = {'scope': 'test-only forced spawn selection below the CLI capacity threshold',
            'generator_topology': 'spawn-lanes-v1', 'start_method': 'spawn', 'lane_count': lane_count,
            'queue_capacity_per_lane': 4, 'force_process_generation': True,
            'runtime_diagnostics_enabled': False, 'generation_counts': generation_counts or [32, 2, 14],
            'capacity_target_proven': False}
        if writer_topology is not None:
            fixture_profile.update(writer_topology=writer_topology,
                                   writer_topology_version=writer_topology_version,
                                   generation_modes=['spawn', 'serial', 'spawn'],
                                   cycle_length=4, result_capacity_total=16)
        if generation_modes is not None:
            fixture_profile.update(generator_topology='mixed-lanes-fixture-v1',
                                   generation_modes=generation_modes,
                                   forced_spawn_batches=[index + 1 for index, mode in enumerate(generation_modes)
                                                         if mode == 'spawn'])
        with (directory / 'fixture-execution-profile.json').open('x') as out:
            json.dump(fixture_profile, out, sort_keys=True)
            out.write('\n')
            out.flush()
            os.fsync(out.fileno())
        harness.generation = GenerationJournal(directory, harness.args.run_id, numeric_profile(harness.args))
        self.addCleanup(lambda: harness.generation.finalize())
        harness.events, harness.workers = [], {}
        harness._event_lock = threading.Lock()
        harness._next_command_index = 0
        harness.generation_topologies = []
        harness.settings, harness.call_command = settings, call_command
        harness.connection, harness.connections = connection, connections
        harness.models, harness.api, harness.services = models, events, services
        harness.admin = models.User.objects.get(email='admin@labops.local')
        harness.reviewer = models.User.objects.get(email='reviewer@labops.local')
        harness.task = models.Task.objects.filter(status='IN_PROGRESS', project__status='ACTIVE').first()
        harness.source = models.Warehouse.objects.get(code='WH-01')
        harness.target = models.Warehouse.objects.get(code='WH-03')
        harness.batch = models.Batch.objects.filter(item__is_active=True).order_by('created_at').first()
        harness.process_generation_enabled = True
        harness.runtime_diagnostics_enabled = False
        return harness

    def evidence_directory(self):
        supplied = os.environ.get('LABOPS_PROCESS_PROOF_EVIDENCE')
        if supplied:
            directory = Path(supplied)
            directory.mkdir(parents=True, exist_ok=True)
            self.assertFalse((directory / 'generation-journal.jsonl').exists())
            return directory
        temporary = tempfile.TemporaryDirectory(prefix='labops-spawn-pg-')
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def assert_process_schedule(self, directory, number, indices, parent_backend_pid, actual_database,
                                *, lane_count=4):
        rows = [json.loads(line) for line in
                (directory / f'generation-schedule-{number:03d}.jsonl').read_text().splitlines()]
        summaries = [row for row in rows if row['kind'] == 'summary']
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertTrue(summary['lifecycle_complete'])
        self.assertTrue(summary['channels_closed'])
        self.assertEqual(summary['profile']['start_method'], 'spawn')
        self.assertEqual(summary['profile']['lanes'], lane_count)
        self.assertEqual(summary['profile']['queue_capacity'], 4)
        if lane_count == 6:
            self.assertEqual(summary['profile']['result_capacity'], 16)
            self.assertEqual(summary['profile']['writer_topology'], 'writers-6')
            self.assertEqual(summary['profile']['writer_topology_version'], 'writer-topology-v1')
        for field in ('requested_count', 'scheduled_count', 'started_count', 'completed_count'):
            self.assertEqual(summary[field], len(indices))
        for field in ('failed_count', 'cancelled_count', 'unscheduled_count', 'unknown_count'):
            self.assertEqual(summary[field], 0)
        self.assertEqual(summary['unknown_indices'], [])
        self.assertIsNone(summary['first_error'])
        self.assertEqual(summary['cleanup_errors'], [])
        for kind in ('scheduled', 'started', 'completed'):
            self.assertEqual(sorted(row['global_index'] for row in rows if row['kind'] == kind), indices)
        ready = {row['lane']: row for row in rows if row['kind'] == 'ready'}
        self.assertEqual(set(ready), set(range(lane_count)))
        self.assertEqual(len([row for row in rows if row['kind'] == 'ready']), lane_count)
        self.assertEqual(len({row['pid'] for row in ready.values()}), lane_count)
        self.assertNotIn(os.getpid(), {row['pid'] for row in ready.values()})
        identities = {lane: row['metadata']['backend_identity'] for lane, row in ready.items()}
        self.assertEqual(len({row['backend_pid'] for row in identities.values()}), lane_count)
        self.assertNotIn(parent_backend_pid, {row['backend_pid'] for row in identities.values()})
        self.assertEqual(len({row['application_name'] for row in identities.values()}), lane_count)
        self.assertTrue(all(row['database_name'] == actual_database and row['backend_start']
                            for row in identities.values()))
        for row in ready.values():
            namespace = row['metadata']['runtime_namespace']
            self.assertEqual(namespace['EVENT_TRANSPORT'], 'kafka')
            for name in ('KAFKA_TOPIC', 'KAFKA_SOURCE_CLUSTER_ID', 'KAFKA_SOURCE_STREAM_GENERATION',
                         'EVENT_MAX_PAYLOAD_BYTES'):
                self.assertEqual(namespace[name], getattr(settings, name))
        self.assertEqual(len(summary['children']), lane_count)
        process_absence = []
        for child in summary['children']:
            lane = child['lane']
            self.assertEqual(child['pid'], ready[lane]['pid'])
            self.assertTrue(child['ready'])
            self.assertTrue(child['cleanup_complete'])
            self.assertTrue(child['reaped'])
            self.assertEqual(child['exitcode'], 0)
            metadata = child['cleanup_metadata']
            self.assertEqual(metadata['backend_identity'], identities[lane])
            self.assertFalse(metadata['in_atomic_block'])
            self.assertTrue(metadata['connection_closed'])
            with self.assertRaises(ProcessLookupError) as absent:
                os.kill(child['pid'], 0)
            process_absence.append({'pid': child['pid'], 'operation': 'os.kill(pid, 0)',
                                    'actual_exception': type(absent.exception).__name__,
                                    'actual_errno': absent.exception.errno})
        # Backend identity includes start time: a recycled numeric PID cannot
        # silently satisfy this assertion after the child's client has exited.
        absence_queries = []
        with connection.cursor() as cursor:
            for identity in identities.values():
                sql = 'SELECT count(*) FROM pg_stat_activity WHERE pid = %s AND backend_start = %s'
                parameters = [identity['backend_pid'], identity['backend_start']]
                cursor.execute(sql, parameters)
                actual_count = cursor.fetchone()[0]
                absence_queries.append({'sql': sql, 'parameters': parameters, 'actual_count': actual_count})
                self.assertEqual(actual_count, 0)
        return {'requested': len(indices), 'global_indices': indices, 'start_method': 'spawn',
            'children': summary['children'], 'ready_backend_identities': identities,
            'registered_backends_absent_after_reap': True,
            'child_processes_absent_after_reap': True, 'lifecycle_complete': True,
            'owned_session_absence_queries': absence_queries,
            'actual_child_process_absence_observations': process_absence}

    def test_four_lanes_commit_real_commands_and_preserve_consumer_effects(self):
        self.exercise_process_lanes()

    def test_six_lanes_commit_real_commands_and_preserve_consumer_effects(self):
        self.exercise_process_lanes(lane_count=6, writer_topology='writers-6',
                                    writer_topology_version='writer-topology-v1')

    def exercise_process_lanes(self, *, lane_count=4, writer_topology=None,
                               writer_topology_version=None):
        first_count = lane_count * 8
        continued_count = lane_count * 4 - 2
        total = lane_count * 12
        directory = (self.evidence_directory() if writer_topology is None else
                     self.writer_evidence_directory('six-spawn'))
        with override_settings(EVENT_TRANSPORT='local'):
            output = io.StringIO()
            call_command('seed_demo', stdout=output)
            call_command('rebuild_inventory_projection', stdout=output)
        harness = (self.make_harness(directory) if writer_topology is None else
                   self.make_harness(directory, writer_topology=writer_topology,
                                     writer_topology_version=writer_topology_version,
                                     event_count=first_count,
                                     generation_counts=[first_count, 2, continued_count]))
        self.assertEqual(harness.snapshot()['mismatches'], [])
        baseline_movements = models.StockMovement.objects.count()
        self.assertEqual(models.OutboxEvent.objects.filter(transport='kafka').count(), 0)
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid(), current_database()')
            parent_backend_pid, actual_database = cursor.fetchone()
            cursor.execute('SHOW track_commit_timestamp')
            tracks_commits = cursor.fetchone()[0] == 'on'
        self.assertEqual(actual_database, connection.settings_dict['NAME'])
        if os.environ.get('LABOPS_TEST_DB'):
            self.assertEqual(actual_database, os.environ['LABOPS_TEST_DB'])
        if os.environ.get('DATABASE_URL'):
            base_database = unquote(urlparse(os.environ['DATABASE_URL']).path.lstrip('/'))
            self.assertNotEqual(actual_database, base_database)

        with override_settings(EVENT_TRANSPORT='kafka'):
            harness.prepare_order()
            for key in ('project', 'task', 'order', 'order_line'):
                self.assertEqual(len({data[key].id for data in harness.business_lanes}), lane_count)
            ids, first_workload = harness.generate(first_count, 'steady', rate=1000.0)
        self.assertEqual(first_workload['input'], first_count)
        self.assertEqual(first_workload['completed_commands'], first_count)
        self.assertEqual(first_workload['generator_topology'], 'spawn-lanes-v1')
        first_lifecycle = self.assert_process_schedule(directory, 1, list(range(first_count)),
                                                      parent_backend_pid, actual_database, lane_count=lane_count)
        movements = {item['global_index']: models.StockMovement.objects.get(pk=item['movement_id'])
                     for item in harness.events}
        self.assert_business_cycles(harness, movements, cycles=2, lane_count=lane_count)

        with override_settings(EVENT_TRANSPORT='kafka'):
            serial_ids, serial_workload = harness.generate(2, 'fixture_serial', rate=1000.0)
            self.assertEqual(serial_workload['generator_topology'], 'serial_fault_fixture')
            self.assertEqual([item['global_index'] for item in harness.events[-2:]], [first_count, first_count + 1])
            original_issue = harness.business_lanes[0]['cycle_issue']
            original_batch = harness.business_lanes[0]['batch']
            self.assertIsNotNone(original_issue)
            self.assertEqual(str(original_issue.id), harness.events[-1]['movement_id'])
            continued_ids, continued_workload = harness.generate(continued_count, 'steady', rate=1000.0)
        self.assertEqual(continued_workload['input'], continued_count)
        self.assertEqual(continued_workload['completed_commands'], continued_count)
        self.assertEqual(continued_workload['generator_topology'], 'spawn-lanes-v1')
        second_lifecycle = self.assert_process_schedule(directory, 2, list(range(first_count + 2, total)),
                                                       parent_backend_pid, actual_database, lane_count=lane_count)
        ids += serial_ids + continued_ids
        self.assertEqual(len(ids), total)
        self.assertEqual(len(set(ids)), total)
        self.assertEqual([item['global_index'] for item in harness.events], list(range(total)))
        self.assertEqual([item['event_id'] for item in harness.events], ids)
        self.assertEqual(Counter(item['kind'] for item in harness.events),
                         {kind: lane_count * 3 for kind in ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')})
        self.assertEqual(Counter(item['business_lane'] for item in harness.events),
                         {lane: 12 for lane in range(lane_count)})
        for item in harness.events:
            self.assertEqual(item['business_lane'], (item['global_index'] // 4) % lane_count)
            self.assertTrue(str(item['insert_transaction_xid']).isdigit())
            self.assertGreater(item['payload_bytes'], 0)
            committed_at = item['outbox_transaction_commit_at']
            if tracks_commits:
                self.assertIsNotNone(committed_at)
                self.assertTrue(math.isfinite(committed_at))
                self.assertGreaterEqual(committed_at, item['command_started_at'])
            else:
                self.assertIsNone(committed_at)
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
        self.assertTrue(all(row.status == 'PENDING' and row.payload_hash for row in outbox))
        batches = self.assert_business_cycles(harness, movements, cycles=3, lane_count=lane_count)

        # Exercise the exact ORM adapter used after a lost child result, with
        # real original posted rows. This query-only probe does not erase the
        # existing origin evidence or append a synthetic recovery annotation.
        event_by_index = {item['global_index']: item for item in harness.events}
        adapter_rows = []
        with override_settings(EVENT_TRANSPORT='kafka'):
            for plan, _directory in harness.generation._plans.values():
                for command in plan['commands']:
                    facts = harness.process_database_facts(plan, command)
                    validated = _validate_database(plan, command, attempt=None, facts=facts)
                    adapter_rows.append({'origin_id': plan['origin_id'], 'command': command,
                                         'facts': facts, 'validation': validated})
                    (directory / 'database-adapter-proof.json').write_text(
                        json.dumps(adapter_rows, indent=2, sort_keys=True) + '\n')
                    self.assertEqual(validated['state'], 'committed_recovered')
                    item = event_by_index[command['global_index']]
                    self.assertEqual(validated['movement_id'], item['movement_id'])
                    self.assertEqual(validated['event_id'], item['event_id'])
                    self.assertEqual(validated['request_hash'], movements[command['global_index']].request_hash)
                    self.assertEqual(validated['payload_hash'], models.OutboxEvent.objects.get(pk=item['event_id']).payload_hash)
        self.assertEqual(len(adapter_rows), total - 2)
        self.assertEqual(Counter(row['command']['kind'] for row in adapter_rows),
                         {'RECEIPT': lane_count * 3 - 1, 'ISSUE': lane_count * 3 - 1,
                          'TRANSFER': lane_count * 3, 'REVERSAL': lane_count * 3})

        summary = harness.generation.finalize()
        for field in ('requested', 'attempted', 'committed', 'identified_events'):
            self.assertEqual(summary['totals'][field], total)
        for field in ('failed_before_commit', 'post_commit_observation_failed', 'unattempted',
                      'pending_before_commit', 'pending_observation', 'commit_unknown',
                      'attempted_unknown', 'integrity_failed', 'database_recovered_committed',
                      'database_identified_events'):
            self.assertEqual(summary['totals'][field], 0)
        self.assertEqual(len(summary['batches']), lane_count * 2 + 1)
        self.assertEqual(Counter(batch['requested'] for batch in summary['batches']),
                         {8: lane_count, 2: 2, 4: lane_count - 1})
        self.assertTrue(all(batch['status'] == 'succeeded' for batch in summary['batches']))
        self.assertEqual(json.loads((directory / 'generation-summary.json').read_text()), summary)
        parent_rows = [json.loads(line) for line in
                       (directory / 'generation-journal.jsonl').read_text().splitlines()]
        origins = sorted(directory.rglob('origin-journal.jsonl'))
        self.assertEqual(len(origins), lane_count * 2)
        journal = []
        for path in [directory / 'generation-journal.jsonl', *origins]:
            records = [json.loads(line) for line in path.read_text().splitlines()]
            # Only one origin owns each sequence. No artificial global order
            # is introduced between independently durable child streams.
            self.assertEqual([row['sequence'] for row in records], list(range(1, len(records) + 1)))
            journal.extend(records)
        self.assertEqual(Counter(row['action'] for row in parent_rows)['command_committed'], 2)
        self.assertEqual(Counter(row['action'] for row in journal)['command_committed'], total)
        self.assertEqual(Counter(row['action'] for row in journal)['event_identified'], total)
        def attempt_key(row):
            return (row.get('origin_id', 'coordinator_serial'), row['batch_id'],
                    row.get('ordinal', row.get('attempt_id')))
        committed = {attempt_key(row): row['movement_id']
                     for row in journal if row['action'] == 'command_committed'}
        identified = {attempt_key(row): row['event_id']
                      for row in journal if row['action'] == 'event_identified'}
        self.assertEqual(len(committed), total)
        self.assertEqual(len(identified), total)
        self.assertEqual(committed.keys(), identified.keys())
        if writer_topology is not None:
            self.assertEqual(set(identified.values()), set(ids))
            self.assertEqual(set(committed.values()), {str(row.id) for row in movements.values()})
            attempted = {attempt_key(row) for row in journal if row['action'] == 'command_attempted'}
            self.assertEqual(Counter(row['action'] for row in journal)['command_attempted'], total)
            self.assertEqual(attempted, committed.keys())
        actual_event_movements = {str(row.id): str(row.aggregate_id) for row in outbox}
        for attempt_key, eid in identified.items():
            self.assertEqual(actual_event_movements[eid], committed[attempt_key])
        self.assertEqual(Counter(row['action'] for row in journal)['batch_succeeded'], lane_count * 2 + 1)

        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertTrue(events.process_envelope(consumer, events.envelope(
                    models.OutboxEvent.objects.get(pk=eid))))
        self.assertEqual(services.reconcile(), [])
        # The inherited cycle assertion checks receipt/order links, movement
        # types, original reversal relationships and exact stock balances.
        self.assert_business_cycles(harness, movements, cycles=3, lane_count=lane_count)
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
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid(), current_database()')
            self.assertEqual(cursor.fetchone(), (parent_backend_pid, actual_database))
        proof = {'database_vendor': connection.vendor, 'actual_test_database': actual_database,
            'parent_process_pid': os.getpid(), 'parent_backend_pid': parent_backend_pid,
            'commands': total, 'generation_counts': [first_count, 2, continued_count], 'per_kind': dict(Counter(item['kind'] for item in harness.events)),
            'committed_journal_records': total, 'kafka_outbox_rows': total,
            'consumer_markers': total * 2, 'notifications': after['notification_count'],
            'duplicate_effects': 0, 'reconciliation_mismatches': 0, 'unique_cycle_batches': len(batches),
            'tracked_commit_timestamps': total if tracks_commits else 0,
            'actual_database_adapter_commands': len(adapter_rows),
            'actual_database_adapter_kinds': dict(Counter(row['command']['kind'] for row in adapter_rows)),
            'database_adapter_recovered_identity_checks': True,
            'cross_batch_original_issue_preserved': True, 'cross_batch_original_batch_preserved': True,
            'process_batches': [first_lifecycle, second_lifecycle],
            'runtime_diagnostics_requested': False, 'capacity_target_proven': False,
            'kafka_publication_proven': False}
        (directory / 'process-proof-summary.json').write_text(json.dumps(proof, indent=2, sort_keys=True) + '\n')
        if writer_topology is not None:
            self.write_writer_business_proof(directory, harness, movements, ids,
                modes=['spawn', 'serial', 'spawn'],
                generation_counts=[first_count, 2, continued_count],
                baseline_movements=baseline_movements,
                session_queries=[query for batch in (first_lifecycle, second_lifecycle)
                                 for query in batch['owned_session_absence_queries']],
                process_batches=[first_lifecycle, second_lifecycle],
                continuity=[{'preceding_issue_index': first_count + 1,
                    'original_issue_id': str(original_issue.id), 'original_batch_id': str(original_batch.id),
                    'continued_transfer_index': first_count + 2,
                    'actual_transfer_batch_ids': list(movements[first_count + 2].lines.values_list('batch_id', flat=True)),
                    'continued_reversal_index': first_count + 3,
                    'actual_reversal_of_id': str(movements[first_count + 3].reversal_of_id)}])
        print(json.dumps({'real_pg_spawn_process_commands': total, 'generation_counts': [first_count, 2, continued_count],
                          'consumer_markers': total * 2, 'reconciliation_mismatches': 0,
                          'capacity_target_proven': False}, sort_keys=True))

    def test_six_lanes_preserve_unfinished_cycle_across_threads_spawn_threads(self):
        directory = self.writer_evidence_directory('six-mixed')
        with override_settings(EVENT_TRANSPORT='local'):
            output = io.StringIO()
            call_command('seed_demo', stdout=output)
            call_command('rebuild_inventory_projection', stdout=output)
        harness = self.make_harness(directory, writer_topology='writers-6',
            writer_topology_version='writer-topology-v1', event_count=48,
            generation_counts=[26, 2, 20], generation_modes=['threads', 'spawn', 'threads'])
        harness.connections = fixture.ObservedConnections(record_backend_identity=True)
        baseline_movements = models.StockMovement.objects.count()
        self.assertEqual(harness.snapshot()['mismatches'], [])
        self.assertEqual(models.OutboxEvent.objects.filter(transport='kafka').count(), 0)
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid(), current_database()')
            parent_backend_pid, actual_database = cursor.fetchone()

        with override_settings(EVENT_TRANSPORT='kafka'):
            harness.prepare_order()
            for key in ('project', 'task', 'order', 'order_line'):
                self.assertEqual(len({data[key].id for data in harness.business_lanes}), 6)
            # A complete six-lane cycle precedes this unfinished receipt/issue;
            # the new process clients must resume indices 26 and 27, not zero.
            harness.process_generation_enabled = False
            first_ids, first_workload = harness.generate(26, 'steady', rate=1000.0)
            self.assertEqual(first_workload['generator_topology'], 'parallel-lanes-v1')
            self.assertEqual(first_workload['completed_commands'], 26)
            original_issue = harness.business_lanes[0]['cycle_issue']
            original_batch = harness.business_lanes[0]['batch']
            self.assertIsNotNone(original_issue)
            self.assertEqual(str(original_issue.id), harness.events[25]['movement_id'])
            self.assertEqual(harness.events[24]['kind'], 'RECEIPT')
            self.assertEqual(harness.events[25]['kind'], 'ISSUE')
            harness.process_generation_enabled = True
            process_ids, process_workload = harness.generate(2, 'steady', rate=1000.0)
            self.assertEqual(process_workload['generator_topology'], 'spawn-lanes-v1')
            self.assertEqual(process_workload['completed_commands'], 2)
            self.assertIsNone(harness.business_lanes[0]['cycle_issue'])
            self.assertEqual(harness.business_lanes[0]['batch'].id, original_batch.id)
            harness.process_generation_enabled = False
            final_ids, final_workload = harness.generate(20, 'steady', rate=1000.0)
            self.assertEqual(final_workload['generator_topology'], 'parallel-lanes-v1')
            self.assertEqual(final_workload['completed_commands'], 20)
        ids = first_ids + process_ids + final_ids
        self.assertEqual(len(ids), 48)
        self.assertEqual(len(set(ids)), 48)
        self.assertEqual([row['global_index'] for row in harness.events], list(range(48)))
        self.assertEqual([row['event_id'] for row in harness.events], ids)
        self.assertEqual(Counter(row['kind'] for row in harness.events),
                         {'RECEIPT': 12, 'ISSUE': 12, 'TRANSFER': 12, 'REVERSAL': 12})
        self.assertEqual(Counter(row['business_lane'] for row in harness.events),
                         {lane: 8 for lane in range(6)})
        for row in harness.events:
            self.assertEqual(row['business_lane'], (row['global_index'] // 4) % 6)
            self.assertEqual(row['kind'], ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')[row['global_index'] % 4])
            self.assertEqual(row['scenario'], 'steady')
            self.assertTrue(str(row['insert_transaction_xid']).isdigit())
            self.assertGreater(row['payload_bytes'], 0)
        movements = {row['global_index']: models.StockMovement.objects.get(pk=row['movement_id'])
                     for row in harness.events}
        self.assertEqual(movements[27].reversal_of_id, original_issue.id)
        self.assertEqual(set(movements[26].lines.values_list('batch_id', flat=True)), {original_batch.id})
        self.assertEqual({row.idempotency_key for row in movements.values()},
                         {f'{harness.args.run_id}:{index}' for index in range(48)})
        self.assertEqual(models.StockMovement.objects.count(), baseline_movements + 48)
        outbox = list(models.OutboxEvent.objects.filter(transport='kafka'))
        self.assertEqual(len(outbox), 48)
        self.assertEqual({str(row.id) for row in outbox}, set(ids))
        self.assertEqual({str(row.aggregate_id) for row in outbox},
                         {str(row.id) for row in movements.values()})
        self.assertTrue(all(row.status == 'PENDING' and row.payload_hash for row in outbox))
        self.assert_business_cycles(harness, movements, cycles=2, lane_count=6)

        lifecycle = self.assert_process_schedule(directory, 2, [26, 27],
            parent_backend_pid, actual_database, lane_count=6)
        for number, indices in ((1, list(range(26))), (3, list(range(28, 48)))):
            rows = [json.loads(line) for line in
                    (directory / f'generation-schedule-{number:03d}.jsonl').read_text().splitlines()]
            summaries = [row for row in rows if row['kind'] == 'summary']
            self.assertEqual(len(summaries), 1)
            schedule = summaries[0]
            self.assertTrue(schedule['passed'])
            self.assertTrue(schedule['worker_threads_joined'])
            self.assertTrue(schedule['worker_completion_observed'])
            self.assertEqual(schedule['profile']['lanes'], 6)
            self.assertEqual(schedule['profile']['cycle_length'], 4)
            self.assertEqual(schedule['profile']['queue_capacity'], 4)
            for field in ('scheduled_indices', 'started_indices', 'completed_indices'):
                self.assertEqual(schedule[field], indices)
            for field in ('failed_count', 'cancelled_count', 'unscheduled_count'):
                self.assertEqual(schedule[field], 0)
            self.assertEqual(schedule['lane_shutdowns'],
                             [{'lane': lane, 'passed': True, 'error_type': None} for lane in range(6)])
        self.assertEqual(len(harness.connections.closed), 12)
        first_closes, final_closes = harness.connections.closed[:6], harness.connections.closed[6:]
        self.assertEqual(len({row['backend_pid'] for row in first_closes}), 6)
        self.assertTrue(all(row['backend_pid'] is not None for row in first_closes))
        # Lane zero's cycle was completed by the process batch. Its final
        # thread owns no database connection and still must report cleanup.
        idle_closes = [row for row in final_closes if row['backend_pid'] is None]
        self.assertEqual(len(idle_closes), 1)
        self.assertEqual(idle_closes[0]['thread'].name, 'paced-business-lane-0')
        self.assertEqual(len({row['backend_pid'] for row in final_closes if row['backend_pid'] is not None}), 5)
        thread_queries = self.assert_closed_thread_sessions(harness.connections.closed, parent_backend_pid)

        adapter_rows = []
        with override_settings(EVENT_TRANSPORT='kafka'):
            for plan, _origin_directory in harness.generation._plans.values():
                for command in plan['commands']:
                    facts = harness.process_database_facts(plan, command)
                    validated = _validate_database(plan, command, attempt=None, facts=facts)
                    adapter_rows.append({'origin_id': plan['origin_id'], 'command': command,
                                         'facts': facts, 'validation': validated})
                    self.assertEqual(validated['state'], 'committed_recovered')
                    item = harness.events[command['global_index']]
                    self.assertEqual(validated['movement_id'], item['movement_id'])
                    self.assertEqual(validated['event_id'], item['event_id'])
                    self.assertEqual(validated['request_hash'], movements[command['global_index']].request_hash)
                    self.assertEqual(validated['payload_hash'], models.OutboxEvent.objects.get(pk=item['event_id']).payload_hash)
        self.assertEqual([row['command']['global_index'] for row in adapter_rows], [26, 27])
        self.assertEqual([row['command']['kind'] for row in adapter_rows], ['TRANSFER', 'REVERSAL'])
        (directory / 'database-adapter-proof.json').write_text(json.dumps(adapter_rows, indent=2, sort_keys=True) + '\n')
        summary = harness.generation.finalize()
        for field in ('requested', 'attempted', 'committed', 'identified_events'):
            self.assertEqual(summary['totals'][field], 48)
        for field in ('failed_before_commit', 'post_commit_observation_failed', 'unattempted',
                      'pending_before_commit', 'pending_observation', 'commit_unknown',
                      'attempted_unknown', 'integrity_failed', 'database_recovered_committed',
                      'database_identified_events'):
            self.assertEqual(summary['totals'][field], 0)
        self.assertEqual(len(summary['batches']), 18)
        self.assertTrue(all(row['status'] == 'succeeded' for row in summary['batches']))
        plans_by_lane = {plan['lane']: (plan, origin_directory)
                         for plan, origin_directory in harness.generation._plans.values()}
        self.assertEqual(set(plans_by_lane), set(range(6)))
        self.assertEqual(plans_by_lane[0][0]['indices'], [26, 27])
        for lane in range(1, 6):
            plan, origin_directory = plans_by_lane[lane]
            self.assertEqual(plan['indices'], [])
            self.assertEqual(plan['commands'], [])
            actual_plan = json.loads((origin_directory / 'origin-plan.json').read_text())
            self.assertEqual(actual_plan, plan)
            batches_for_origin = [row for row in summary['batches']
                                  if row.get('origin_id') == plan['origin_id']]
            self.assertEqual(len(batches_for_origin), 1)
            self.assertEqual(batches_for_origin[0]['status'], 'succeeded')
            for field in ('requested', 'attempted', 'committed', 'identified_events',
                          'failed_before_commit', 'post_commit_observation_failed', 'unattempted',
                          'pending_before_commit', 'pending_observation', 'commit_unknown'):
                self.assertEqual(batches_for_origin[0][field], 0)
            actual_records = [json.loads(line) for line in
                              (origin_directory / 'origin-journal.jsonl').read_text().splitlines()]
            self.assertEqual([row['action'] for row in actual_records],
                             ['origin_frozen', 'batch_started', 'batch_succeeded', 'origin_finalized'])
        parent_rows = [json.loads(line) for line in
                       (directory / 'generation-journal.jsonl').read_text().splitlines()]
        self.assertEqual(Counter(row['action'] for row in parent_rows)['command_committed'], 46)
        journal = []
        origins = sorted(directory.rglob('origin-journal.jsonl'))
        self.assertEqual(len(origins), 6)
        for path in [directory / 'generation-journal.jsonl', *origins]:
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row['sequence'] for row in records], list(range(1, len(records) + 1)))
            journal.extend(records)
        self.assertEqual(Counter(row['action'] for row in journal)['command_committed'], 48)
        self.assertEqual(Counter(row['action'] for row in journal)['event_identified'], 48)
        def attempt_key(row):
            return (row.get('origin_id', 'coordinator_serial'), row['batch_id'],
                    row.get('ordinal', row.get('attempt_id')))
        committed = {attempt_key(row): row['movement_id']
                     for row in journal if row['action'] == 'command_committed'}
        identified = {attempt_key(row): row['event_id']
                      for row in journal if row['action'] == 'event_identified'}
        self.assertEqual(len(committed), 48)
        self.assertEqual(len(identified), 48)
        self.assertEqual(committed.keys(), identified.keys())
        self.assertEqual(set(identified.values()), set(ids))
        self.assertEqual(set(committed.values()), {str(row.id) for row in movements.values()})
        attempted = {attempt_key(row) for row in journal if row['action'] == 'command_attempted'}
        self.assertEqual(Counter(row['action'] for row in journal)['command_attempted'], 48)
        self.assertEqual(attempted, committed.keys())
        actual_event_movements = {str(row.id): str(row.aggregate_id) for row in outbox}
        for key, event_id in identified.items():
            self.assertEqual(actual_event_movements[event_id], committed[key])

        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertTrue(events.process_envelope(consumer, events.envelope(models.OutboxEvent.objects.get(pk=eid))))
        after = harness.snapshot(ids)
        self.assertEqual(after['mismatches'], [])
        self.assertEqual(after['dedupe_count'], 96)
        self.assertEqual(after['consumer_counts'], {'notification': 48, 'analytics': 48})
        self.assertGreater(after['expected_notification_count'], 0)
        self.assertEqual(after['notification_count'], after['expected_notification_count'])
        self.assertEqual(models.FailedDelivery.objects.count(), 0)
        self.assertEqual(services.reconcile(), [])
        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertFalse(events.process_envelope(consumer, events.envelope(models.OutboxEvent.objects.get(pk=eid))))
        self.assertEqual(harness.snapshot(ids), after)
        self.assertEqual(services.reconcile(), [])
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid(), current_database()')
            self.assertEqual(cursor.fetchone(), (parent_backend_pid, actual_database))
        self.write_writer_business_proof(directory, harness, movements, ids,
            modes=['threads', 'spawn', 'threads'], generation_counts=[26, 2, 20],
            baseline_movements=baseline_movements,
            session_queries=thread_queries + lifecycle['owned_session_absence_queries'],
            process_batches=[lifecycle], continuity=[{'preceding_issue_index': 25,
                'original_issue_id': str(original_issue.id), 'original_batch_id': str(original_batch.id),
                'continued_transfer_index': 26,
                'actual_transfer_batch_ids': list(movements[26].lines.values_list('batch_id', flat=True)),
                'continued_reversal_index': 27, 'actual_reversal_of_id': str(movements[27].reversal_of_id)}])
