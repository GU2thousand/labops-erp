"""Mock process transport while retaining the real reserved origin accounting."""
from copy import deepcopy
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID

from django.test import SimpleTestCase

from benchmarks.events.acceptance import Harness, write_json as persist_json
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
from benchmarks.events.origin_journal import CompositeGenerationJournal, OriginJournal


class DriverPrimaryFailure(BaseException):
    pass


class SessionSettlementFailure(RuntimeError):
    pass


class OriginReconciliationFailure(RuntimeError):
    pass


class OriginSummaryFailure(RuntimeError):
    pass


class HarnessProcessAccountingTests(SimpleTestCase):
    count = 16

    def identity(self, number):
        return SimpleNamespace(id=UUID(int=number))

    def harness(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        harness = object.__new__(Harness)
        harness.evidence = Path(temporary.name)
        harness.args = SimpleNamespace(run_id='mock-process-accounting', events=self.count,
            rate=1000.0, duration=0, fault_repetitions=1, fault_events=1,
            duplicate_events=1, poison_events=12, broker_fault_seconds=1,
            outage_seconds=1, consumer_outage_seconds=1, drain_timeout=900)
        parent = GenerationJournal(harness.evidence, harness.args.run_id, numeric_profile(harness.args))
        self.addCleanup(parent.finalize)
        harness.generation = CompositeGenerationJournal(parent)
        harness._next_command_index = 0
        harness.generation_topologies, harness.events, harness.process_batches = [], [], []
        harness.process_generation_enabled = True
        harness.runtime_diagnostics_enabled = False
        harness.workers = {}
        harness.admin, harness.source, harness.target = (self.identity(number) for number in (1, 2, 3))
        harness.business_lanes = [
            {key: self.identity(20 + lane * 4 + position)
             for position, key in enumerate(('project', 'task', 'order', 'order_line'))}
            for lane in range(4)]
        for lane in harness.business_lanes:
            lane.update(batch=None, cycle_issue=None)
        harness.settings = SimpleNamespace(EVENT_TRANSPORT='kafka',
            KAFKA_TOPIC='caller.inventory.v1', KAFKA_SOURCE_CLUSTER_ID='caller-cluster',
            KAFKA_SOURCE_STREAM_GENERATION='caller-generation', EVENT_MAX_PAYLOAD_BYTES=1_048_576)
        harness.connection = SimpleNamespace(settings_dict={
            'ENGINE': 'django.db.backends.sqlite3', 'NAME': 'caller-test-database.sqlite3',
            'HOST': '', 'PORT': '', 'USER': '', 'PASSWORD': 'fixture-private-bootstrap',
            'OPTIONS': {'timeout': 7}})
        harness.env = {'DATABASE_URL': 'postgresql://unused.invalid/environment-database',
                       'KAFKA_TOPIC': 'environment.inventory.v1'}
        harness.settle_process_sessions = Mock(return_value=True)
        harness.process_database_facts = Mock()
        return harness

    def topology(self, harness):
        return json.loads((harness.evidence / 'generation-topology-001.json').read_text())

    def successful_driver(self, harness, *, lifecycle_complete=True, captured=None):
        def drive(count, rate, worker_factory, bootstrap, **callbacks):
            self.assertEqual(count, self.count)
            if captured is not None:
                captured.append(deepcopy(bootstrap))
            items = []
            for plan in bootstrap['plans']:
                lane, pid = plan['lane'], 4000 + plan['lane']
                callbacks['on_process_started'](lane, pid)
                callbacks['on_process_ready'](lane, pid, {
                    'backend_identity': {'database_name': bootstrap['database_config']['NAME'],
                                         'application_name': plan['application_name']},
                    'runtime_namespace': bootstrap['runtime_settings']})
                origin = OriginJournal(plan['directory'], run_id=plan['run_id'],
                    origin_id=plan['origin_id'], profile=bootstrap['profile'],
                    label=plan['label'], lane=lane, indices=plan['indices'],
                    rate=plan['rate'], context=plan['context'])
                batch = origin.begin_batch(len(plan['indices']), plan['rate'], plan['label'])
                try:
                    for index in plan['indices']:
                        callbacks['on_observation']({'kind': 'started', 'lane': lane, 'global_index': index})
                        ordinal = origin.attempt(batch, global_index=index)
                        movement_id, event_id = (str(UUID(int=1000 + index * 2 + offset))
                                                 for offset in (0, 1))
                        origin.commit(batch, ordinal, movement_id, request_hash='a' * 64)
                        origin.identify_event(batch, ordinal, event_id, payload_hash='b' * 64)
                        item = {'event_id': event_id, 'movement_id': movement_id,
                                'global_index': index, 'business_lane': lane,
                                'scenario': plan['label']}
                        callbacks['on_observation']({'kind': 'completed', 'lane': lane,
                                                    'global_index': index, 'result': item})
                        items.append(item)
                    origin.finish_success(batch)
                finally:
                    origin.finalize()
                callbacks['on_observation']({'kind': 'cleanup_complete', 'lane': lane,
                    'metadata': {'connection_closed': True,
                                 'lane_state': {'batch_id': None, 'cycle_issue_id': None}}})
                callbacks['on_observation']({'kind': 'process_exit', 'lane': lane, 'exitcode': 0})
            callbacks['on_observation']({'kind': 'summary', 'lifecycle_complete': lifecycle_complete,
                'channels_closed': True, 'worker_processes_joined': True, 'worker_completion_observed': True})
            return items
        return drive

    def assert_reserved_unattempted(self, harness):
        summary = harness.generation.summary()
        self.assertEqual(summary['totals']['requested'], self.count)
        self.assertEqual(summary['totals']['unattempted'], self.count)
        for field in ('attempted', 'committed', 'identified_events', 'commit_unknown', 'attempted_unknown'):
            self.assertEqual(summary['totals'][field], 0)
        self.assertEqual(harness._next_command_index, self.count)
        self.assertFalse(harness.generation_topologies[-1]['passed'])
        self.assertFalse(self.topology(harness)['passed'])

    def test_automatic_selection_is_frozen_at_512_and_test_force_is_explicit(self):
        harness = self.harness()
        harness.process_generation_enabled = None
        harness._generate_processes = Mock(return_value='processes')
        harness._generate_concurrent_threads = Mock(return_value='threads')
        self.assertEqual(harness._generate_concurrent(511, 'steady', rate=1000), 'threads')
        self.assertEqual(harness._generate_concurrent(512, 'steady', rate=1000), 'processes')
        harness.process_generation_enabled = True
        self.assertEqual(harness._generate_concurrent(1, 'steady', rate=1000), 'processes')
        harness.process_generation_enabled = False
        self.assertEqual(harness._generate_concurrent(90000, 'steady', rate=1000), 'threads')
        self.assertEqual(harness._generate_processes.call_count, 2)
        self.assertEqual(harness._generate_concurrent_threads.call_count, 2)

    def test_setup_failure_reserves_whole_request_before_any_driver_can_start(self):
        harness = self.harness()
        original = OSError('Caller database configuration unavailable')
        class UnavailableConnection:
            @property
            def settings_dict(self):
                raise original
        harness.connection = UnavailableConnection()
        with patch('benchmarks.events.process_generation.run_paced_processes') as driver, \
             self.assertRaises(OSError) as raised:
            harness._generate_processes(self.count, 'steady', rate=1000)
        self.assertIs(raised.exception, original)
        driver.assert_not_called()
        harness.settle_process_sessions.assert_not_called()
        self.assert_reserved_unattempted(harness)

    def test_plan_persistence_failure_retains_all_unattempted_origin_reservations(self):
        harness = self.harness()
        original = OSError('Frozen plan could not be persisted')
        def persist(path, value):
            if Path(path).name == 'process-generation-plan-001.json':
                raise original
            return persist_json(path, value)
        with patch('benchmarks.events.acceptance.write_json', persist), \
             patch('benchmarks.events.process_generation.run_paced_processes') as driver, \
             self.assertRaises(OSError) as raised:
            harness._generate_processes(self.count, 'steady', rate=1000)
        self.assertIs(raised.exception, original)
        driver.assert_not_called()
        self.assertEqual(len(harness.generation_topologies[-1]['origin_plans']), 4)
        self.assert_reserved_unattempted(harness)

    def test_primary_base_exception_survives_all_secondary_closeout_failures(self):
        harness = self.harness()
        original = DriverPrimaryFailure('Driver primary failure')
        harness.settle_process_sessions.side_effect = SessionSettlementFailure('Session cleanup failed')
        with patch('benchmarks.events.process_generation.run_paced_processes', side_effect=original), \
             patch.object(harness.generation, 'reconcile_origin',
                          side_effect=OriginReconciliationFailure('Reconciliation failed')), \
             patch.object(harness.generation, 'summary', side_effect=OriginSummaryFailure('Summary failed')), \
             self.assertRaises(DriverPrimaryFailure) as raised:
            harness._generate_processes(self.count, 'steady', rate=1000)
        self.assertIs(raised.exception, original)
        topology = self.topology(harness)
        self.assertFalse(topology['passed'])
        self.assertFalse(topology['owned_sessions_settled'])
        self.assertEqual(topology['requested'], self.count)
        self.assertEqual(topology['secondary_errors'], [
            {'stage': 'session_settlement', 'error_type': 'SessionSettlementFailure'},
            *[{'stage': 'origin_reconciliation', 'error_type': 'OriginReconciliationFailure'} for _ in range(4)],
            {'stage': 'topology_persistence', 'error_type': 'OriginSummaryFailure'}])
        self.assertEqual(harness._next_command_index, self.count)

    def test_complete_original_journals_cannot_hide_failed_driver_lifecycle(self):
        harness = self.harness()
        with patch('benchmarks.events.process_generation.run_paced_processes',
                   self.successful_driver(harness, lifecycle_complete=False)), \
             self.assertRaises(RuntimeError):
            harness._generate_processes(self.count, 'steady', rate=1000)
        summary = harness.generation.summary()
        for field in ('requested', 'attempted', 'committed', 'identified_events'):
            self.assertEqual(summary['totals'][field], self.count)
        self.assertTrue(all(batch['status'] == 'succeeded' for batch in summary['batches']))
        topology = self.topology(harness)
        self.assertFalse(topology['passed'])
        self.assertFalse(topology['driver_summary']['lifecycle_complete'])
        self.assertTrue(topology['owned_sessions_settled'])

    def test_private_bootstrap_uses_actual_caller_database_and_frozen_runtime_namespace(self):
        harness = self.harness()
        captured = []
        expected_config = deepcopy(harness.connection.settings_dict)
        with patch.dict(os.environ, {'DATABASE_URL': harness.env['DATABASE_URL'],
                                     'KAFKA_TOPIC': 'environment.inventory.v1',
                                     'KAFKA_SOURCE_CLUSTER_ID': 'environment-cluster'}), \
             patch('benchmarks.events.process_generation.run_paced_processes',
                   self.successful_driver(harness, captured=captured)):
            ids, workload = harness._generate_processes(self.count, 'steady', rate=1000)
        self.assertEqual(len(ids), self.count)
        self.assertEqual(workload['completed_commands'], self.count)
        self.assertEqual(len(captured), 1)
        bootstrap = captured[0]
        self.assertEqual(bootstrap['database_config']['NAME'], expected_config['NAME'])
        self.assertEqual(bootstrap['runtime_settings'], {
            'EVENT_TRANSPORT': 'kafka', 'KAFKA_TOPIC': 'caller.inventory.v1',
            'KAFKA_SOURCE_CLUSTER_ID': 'caller-cluster',
            'KAFKA_SOURCE_STREAM_GENERATION': 'caller-generation', 'EVENT_MAX_PAYLOAD_BYTES': 1_048_576})
        self.assertTrue(bootstrap['database_config']['PASSWORD'] == expected_config['PASSWORD'],
                        'Private bootstrap did not retain caller authentication')
        self.assertTrue(all(plan['database_name'] == expected_config['NAME'] for plan in bootstrap['plans']))
        artifacts = '\n'.join(path.read_text() for path in harness.evidence.glob('*.json'))
        self.assertFalse(expected_config['PASSWORD'] in artifacts,
                         'Private database authentication was written to an artifact')
        persisted = json.loads((harness.evidence / 'process-generation-plan-001.json').read_text())
        self.assertIs(persisted['private_database_config_included'], False)
        self.assertNotIn('database_config', persisted)
        self.assertTrue(self.topology(harness)['passed'])
