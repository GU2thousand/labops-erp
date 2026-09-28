"""Real PostgreSQL correctness of the acceptance-only spawned generator.

The 48-command fixture exercises the normal inventory and consumer service
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
    def make_harness(self, directory):
        harness = object.__new__(Harness)
        harness.args = SimpleNamespace(
            run_id='spawn-pg-integration', events=32, rate=1000.0, duration=0.0,
            fault_repetitions=1, fault_events=4, duplicate_events=1,
            poison_events=1, broker_fault_seconds=1.0, outage_seconds=1.0,
            consumer_outage_seconds=1.0, drain_timeout=900.0,
            evidence_dir=directory, generated_dir=directory, tier='smoke',
            process_generation=True, runtime_diagnostics=False,
        )
        harness.evidence = directory
        fixture_profile = {'scope': 'test-only forced spawn selection below the CLI capacity threshold',
            'generator_topology': 'spawn-lanes-v1', 'start_method': 'spawn', 'lane_count': 4,
            'queue_capacity_per_lane': 4, 'force_process_generation': True,
            'runtime_diagnostics_enabled': False, 'generation_counts': [32, 2, 14],
            'capacity_target_proven': False}
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

    def assert_process_schedule(self, directory, number, indices, parent_backend_pid, actual_database):
        rows = [json.loads(line) for line in
                (directory / f'generation-schedule-{number:03d}.jsonl').read_text().splitlines()]
        summaries = [row for row in rows if row['kind'] == 'summary']
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertTrue(summary['lifecycle_complete'])
        self.assertTrue(summary['channels_closed'])
        self.assertEqual(summary['profile']['start_method'], 'spawn')
        self.assertEqual(summary['profile']['lanes'], 4)
        self.assertEqual(summary['profile']['queue_capacity'], 4)
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
        self.assertEqual(set(ready), {0, 1, 2, 3})
        self.assertEqual(len([row for row in rows if row['kind'] == 'ready']), 4)
        self.assertEqual(len({row['pid'] for row in ready.values()}), 4)
        self.assertNotIn(os.getpid(), {row['pid'] for row in ready.values()})
        identities = {lane: row['metadata']['backend_identity'] for lane, row in ready.items()}
        self.assertEqual(len({row['backend_pid'] for row in identities.values()}), 4)
        self.assertNotIn(parent_backend_pid, {row['backend_pid'] for row in identities.values()})
        self.assertEqual(len({row['application_name'] for row in identities.values()}), 4)
        self.assertTrue(all(row['database_name'] == actual_database and row['backend_start']
                            for row in identities.values()))
        for row in ready.values():
            namespace = row['metadata']['runtime_namespace']
            self.assertEqual(namespace['EVENT_TRANSPORT'], 'kafka')
            for name in ('KAFKA_TOPIC', 'KAFKA_SOURCE_CLUSTER_ID', 'KAFKA_SOURCE_STREAM_GENERATION',
                         'EVENT_MAX_PAYLOAD_BYTES'):
                self.assertEqual(namespace[name], getattr(settings, name))
        self.assertEqual(len(summary['children']), 4)
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
            with self.assertRaises(ProcessLookupError):
                os.kill(child['pid'], 0)
        # Backend identity includes start time: a recycled numeric PID cannot
        # silently satisfy this assertion after the child's client has exited.
        with connection.cursor() as cursor:
            for identity in identities.values():
                cursor.execute('SELECT count(*) FROM pg_stat_activity WHERE pid = %s AND backend_start = %s',
                               [identity['backend_pid'], identity['backend_start']])
                self.assertEqual(cursor.fetchone()[0], 0)
        return {'requested': len(indices), 'global_indices': indices, 'start_method': 'spawn',
            'children': summary['children'], 'ready_backend_identities': identities,
            'registered_backends_absent_after_reap': True,
            'child_processes_absent_after_reap': True, 'lifecycle_complete': True}

    def test_four_lanes_commit_real_commands_and_preserve_consumer_effects(self):
        directory = self.evidence_directory()
        with override_settings(EVENT_TRANSPORT='local'):
            output = io.StringIO()
            call_command('seed_demo', stdout=output)
            call_command('rebuild_inventory_projection', stdout=output)
        harness = self.make_harness(directory)
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
                self.assertEqual(len({data[key].id for data in harness.business_lanes}), 4)
            ids, first_workload = harness.generate(32, 'steady', rate=1000.0)
        self.assertEqual(first_workload['input'], 32)
        self.assertEqual(first_workload['completed_commands'], 32)
        self.assertEqual(first_workload['generator_topology'], 'spawn-lanes-v1')
        first_lifecycle = self.assert_process_schedule(directory, 1, list(range(32)),
                                                      parent_backend_pid, actual_database)
        movements = {item['global_index']: models.StockMovement.objects.get(pk=item['movement_id'])
                     for item in harness.events}
        self.assert_business_cycles(harness, movements, cycles=2)

        with override_settings(EVENT_TRANSPORT='kafka'):
            serial_ids, serial_workload = harness.generate(2, 'fixture_serial', rate=1000.0)
            self.assertEqual(serial_workload['generator_topology'], 'serial_fault_fixture')
            self.assertEqual([item['global_index'] for item in harness.events[-2:]], [32, 33])
            original_issue = harness.business_lanes[0]['cycle_issue']
            original_batch = harness.business_lanes[0]['batch']
            self.assertIsNotNone(original_issue)
            self.assertEqual(str(original_issue.id), harness.events[-1]['movement_id'])
            continued_ids, continued_workload = harness.generate(14, 'steady', rate=1000.0)
        self.assertEqual(continued_workload['input'], 14)
        self.assertEqual(continued_workload['completed_commands'], 14)
        self.assertEqual(continued_workload['generator_topology'], 'spawn-lanes-v1')
        second_lifecycle = self.assert_process_schedule(directory, 2, list(range(34, 48)),
                                                       parent_backend_pid, actual_database)
        ids += serial_ids + continued_ids
        self.assertEqual(len(ids), 48)
        self.assertEqual(len(set(ids)), 48)
        self.assertEqual([item['global_index'] for item in harness.events], list(range(48)))
        self.assertEqual([item['event_id'] for item in harness.events], ids)
        self.assertEqual(Counter(item['kind'] for item in harness.events),
                         {'RECEIPT': 12, 'ISSUE': 12, 'TRANSFER': 12, 'REVERSAL': 12})
        self.assertEqual(Counter(item['business_lane'] for item in harness.events),
                         {0: 12, 1: 12, 2: 12, 3: 12})
        for item in harness.events:
            self.assertEqual(item['business_lane'], (item['global_index'] // 4) % 4)
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
        self.assertTrue(all(row.status == 'PENDING' and row.payload_hash for row in outbox))
        batches = self.assert_business_cycles(harness, movements, cycles=3)

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
        self.assertEqual(len(adapter_rows), 46)
        self.assertEqual(Counter(row['command']['kind'] for row in adapter_rows),
                         {'RECEIPT': 11, 'ISSUE': 11, 'TRANSFER': 12, 'REVERSAL': 12})

        summary = harness.generation.finalize()
        for field in ('requested', 'attempted', 'committed', 'identified_events'):
            self.assertEqual(summary['totals'][field], 48)
        for field in ('failed_before_commit', 'post_commit_observation_failed', 'unattempted',
                      'pending_before_commit', 'pending_observation', 'commit_unknown',
                      'attempted_unknown', 'integrity_failed', 'database_recovered_committed',
                      'database_identified_events'):
            self.assertEqual(summary['totals'][field], 0)
        self.assertEqual(len(summary['batches']), 9)
        self.assertEqual(Counter(batch['requested'] for batch in summary['batches']),
                         {8: 4, 2: 2, 4: 3})
        self.assertTrue(all(batch['status'] == 'succeeded' for batch in summary['batches']))
        self.assertEqual(json.loads((directory / 'generation-summary.json').read_text()), summary)
        parent_rows = [json.loads(line) for line in
                       (directory / 'generation-journal.jsonl').read_text().splitlines()]
        origins = sorted(directory.rglob('origin-journal.jsonl'))
        self.assertEqual(len(origins), 8)
        journal = []
        for path in [directory / 'generation-journal.jsonl', *origins]:
            records = [json.loads(line) for line in path.read_text().splitlines()]
            # Only one origin owns each sequence. No artificial global order
            # is introduced between independently durable child streams.
            self.assertEqual([row['sequence'] for row in records], list(range(1, len(records) + 1)))
            journal.extend(records)
        self.assertEqual(Counter(row['action'] for row in parent_rows)['command_committed'], 2)
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
        actual_event_movements = {str(row.id): str(row.aggregate_id) for row in outbox}
        for attempt_key, eid in identified.items():
            self.assertEqual(actual_event_movements[eid], committed[attempt_key])
        self.assertEqual(Counter(row['action'] for row in journal)['batch_succeeded'], 9)

        for consumer in ('notification', 'analytics'):
            for eid in ids:
                self.assertTrue(events.process_envelope(consumer, events.envelope(
                    models.OutboxEvent.objects.get(pk=eid))))
        self.assertEqual(services.reconcile(), [])
        # The inherited cycle assertion checks receipt/order links, movement
        # types, original reversal relationships and exact stock balances.
        self.assert_business_cycles(harness, movements, cycles=3)
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
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid(), current_database()')
            self.assertEqual(cursor.fetchone(), (parent_backend_pid, actual_database))
        proof = {'database_vendor': connection.vendor, 'actual_test_database': actual_database,
            'parent_process_pid': os.getpid(), 'parent_backend_pid': parent_backend_pid,
            'commands': 48, 'generation_counts': [32, 2, 14], 'per_kind': dict(Counter(item['kind'] for item in harness.events)),
            'committed_journal_records': 48, 'kafka_outbox_rows': 48,
            'consumer_markers': 96, 'notifications': after['notification_count'],
            'duplicate_effects': 0, 'reconciliation_mismatches': 0, 'unique_cycle_batches': len(batches),
            'tracked_commit_timestamps': 48 if tracks_commits else 0,
            'actual_database_adapter_commands': len(adapter_rows),
            'actual_database_adapter_kinds': dict(Counter(row['command']['kind'] for row in adapter_rows)),
            'database_adapter_recovered_identity_checks': True,
            'cross_batch_original_issue_preserved': True, 'cross_batch_original_batch_preserved': True,
            'process_batches': [first_lifecycle, second_lifecycle],
            'runtime_diagnostics_requested': False, 'capacity_target_proven': False,
            'kafka_publication_proven': False}
        (directory / 'process-proof-summary.json').write_text(json.dumps(proof, indent=2, sort_keys=True) + '\n')
        print(json.dumps({'real_pg_spawn_process_commands': 48, 'generation_counts': [32, 2, 14],
                          'consumer_markers': 96, 'reconciliation_mismatches': 0,
                          'capacity_target_proven': False}, sort_keys=True))
