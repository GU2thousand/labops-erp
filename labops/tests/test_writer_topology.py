"""Selected writer admission and parent-owned plans, without application I/O."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID

from django.test import SimpleTestCase

from benchmarks.events.acceptance import Harness, main, freeze_generation_execution_profile
from benchmarks.events.business_commands import InventoryProcessWorker, validate_process_bootstrap
from benchmarks.events.consumer_topology import topology_profile, freeze_topology
from benchmarks.events.concurrent_generation import allocate_lane_indices
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
from benchmarks.events.origin_journal import CompositeGenerationJournal
from benchmarks.events.writer_topology import (
    writer_profile, resolve_profile_writer, generator_roles, freeze_writer_topology,
)
from labops.tests.test_origin_journal import requested_profile


class WriterTopologyTests(SimpleTestCase):
    def profile(self, preset='writers-6'):
        return requested_profile(writer_topology=preset, writer_topology_version='writer-topology-v1',
                                 runtime_diagnostics_enabled=False)

    def bootstrap(self):
        profile = self.profile()
        allocation = allocate_lane_indices(6, lanes=6, start_index=22)
        plans = []
        for lane, indices in enumerate(allocation):
            context = {name: str(UUID(int=number)) for name, number in (
                ('actor_id', 1), ('source_warehouse_id', 2), ('target_warehouse_id', 3),
                ('project_id', 100 + lane), ('task_id', 200 + lane),
                ('order_id', 300 + lane), ('order_line_id', 400 + lane))}
            context.update(batch_id=None, cycle_issue_id=None, source_cluster='validation.fixed',
                source_generation='validation.v1', topic='labops.fixed.inventory.v1',
                database_scope_digest='a' * 64, source_context_digest='b' * 64)
            plans.append({'run_id': 'writer-control', 'origin_id': 'origin_' + str(lane),
                'label': 'steady', 'lane': lane, 'indices': list(indices), 'rate': 50 / 6,
                'context': context, 'application_name': 'owned-' + str(lane),
                'database_name': 'isolated', 'directory': '/unused/origin-' + str(lane),
                'path': '/unused/origin-' + str(lane)})
        return {'profile': profile, 'plans': plans, 'rate': 50., 'writer_topology': 'writers-6',
            'start_index': 22, 'requested_count': 6, 'database_config': {'NAME': 'isolated'},
            'runtime_diagnostics_enabled': False, 'diagnostic_profile_enabled': False}

    def test_closed_presets_preserve_semantic_dimensions_and_refuse_eight(self):
        self.assertEqual(generator_roles(), tuple('generator-' + str(n) for n in range(4)))
        self.assertEqual(generator_roles('writers-6'), tuple('generator-' + str(n) for n in range(6)))
        for preset, lanes in (('writers-4', 4), ('writers-6', 6)):
            value = writer_profile(preset)
            self.assertEqual([value[key] for key in ('lanes', 'cycle_length', 'queue_capacity', 'result_capacity')],
                             [lanes, 4, 4, 16])
            self.assertEqual(value['assignment'], f'(global_index//4)%{lanes}')
        for invalid in ('writers-8', 'foreign', 6, True, None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                writer_profile(invalid)

    def test_legacy_numeric_shape_and_digest_are_preserved_without_inserting_metadata(self):
        legacy = requested_profile()
        original = json.dumps(legacy, sort_keys=True, separators=(',', ':')).encode()
        frozen = numeric_profile(legacy)
        self.assertEqual(len(frozen), 13)
        self.assertEqual(frozen, legacy)
        self.assertEqual(resolve_profile_writer(legacy), 'writers-4')
        self.assertEqual(hashlib.sha256(json.dumps(frozen, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                         hashlib.sha256(original).hexdigest())
        self.assertNotIn('writer_topology', legacy)
        for preset in ('writers-4', 'writers-6'):
            explicit = numeric_profile(self.profile(preset))
            self.assertEqual(explicit['writer_topology'], preset)
            self.assertEqual(explicit['writer_topology_version'], 'writer-topology-v1')

    def test_partial_unknown_or_profiled_writer_metadata_fails_before_path_setup(self):
        for updates in ({'writer_topology': 'writers-6'}, {'writer_topology_version': 'writer-topology-v1'},
                {'writer_topology': 'writers-6', 'writer_topology_version': 'foreign'},
                {'writer_topology': True, 'writer_topology_version': 'writer-topology-v1'},
                {'writer_topology': 'writers-6', 'writer_topology_version': 'writer-topology-v1',
                 'diagnostic_profile': True}):
            with self.subTest(updates=updates), TemporaryDirectory() as directory:
                args = SimpleNamespace(evidence_dir=Path(directory), **updates)
                with self.assertRaises(ValueError):
                    Harness(args)
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_joint_cli_profile_guard_refuses_invalid_requests_before_journal_or_environment(self):
        for args in (['--writer-topology', 'writers-6', '--diagnostic-profile'],
                     ['--writer-topology', 'writers-4', '--consumer-topology', 'notification-dual', '--diagnostic-profile'],
                     ['--writer-topology', 'writers-8']):
            with self.subTest(args=args), TemporaryDirectory() as directory:
                argv = ['acceptance', '--run-id', 'refused', '--evidence-dir', directory, *args]
                with patch('sys.argv', argv), patch('benchmarks.events.acceptance.load_environment') as environment, \
                     patch('benchmarks.events.acceptance.Harness') as harness, self.assertRaises(SystemExit):
                    main()
                environment.assert_not_called()
                harness.assert_not_called()
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_joint_profile_freezes_six_without_changing_consumer_members_or_ack(self):
        with TemporaryDirectory() as directory:
            freeze_generation_execution_profile(directory, 'joint', writer_topology='writers-6',
                                                consumer_topology='notification-dual')
            value = json.loads((Path(directory) / 'generation-execution-profile.json').read_text())
            self.assertEqual(value['writer_profile']['preset'], 'writers-6')
            consumer = value['consumer_topology']
            self.assertEqual([consumer[key] for key in ('writer_lanes', 'publisher_members',
                'notification_members', 'analytics_members', 'inventory_partitions')], [6, 1, 2, 1, 3])
            self.assertEqual(consumer['ack_policy'], topology_profile()['ack_policy'])
            self.assertEqual(value['result_capacity'], 16)
            self.assertEqual(value['selection_policy']['spawn_minimum_batch_count'], 512)
            self.assertEqual(json.loads((Path(directory) / 'writer-topology.json').read_text())['lanes'], 6)

    def test_frozen_writer_and_composed_consumer_cannot_be_relabelled(self):
        with TemporaryDirectory() as directory:
            writer = Path(directory) / 'writer.json'
            consumer = Path(directory) / 'consumer.json'
            freeze_writer_topology(writer, 'frozen', 'writers-6')
            freeze_topology(consumer, 'frozen', 'notification-dual', writer_topology='writers-6')
            original = (writer.read_bytes(), consumer.read_bytes())
            with self.assertRaises(ValueError):
                freeze_writer_topology(writer, 'frozen', 'writers-4')
            with self.assertRaises(ValueError):
                freeze_topology(consumer, 'frozen', 'notification-dual', writer_topology='writers-4')
            self.assertEqual((writer.read_bytes(), consumer.read_bytes()), original)

    def test_selected_harness_selection_is_frozen_against_runtime_argument_adaptation(self):
        harness = object.__new__(Harness)
        harness.args = SimpleNamespace(**self.profile())
        self.assertEqual(harness.writer_lanes, 6)
        harness.args.writer_topology = 'writers-4'
        self.assertEqual(harness.writer_topology, 'writers-6')
        self.assertEqual(harness.writer_lanes, 6)

    def test_partial_nonzero_six_plan_preserves_all_idle_origins_and_exact_input(self):
        bootstrap = self.bootstrap()
        self.assertEqual([plan['indices'] for plan in bootstrap['plans']],
                         [[24, 25, 26, 27], [], [], [], [], [22, 23]])
        self.assertEqual(validate_process_bootstrap(0, bootstrap), ('writers-6', 6))
        self.assertEqual(validate_process_bootstrap(5, bootstrap), ('writers-6', 6))

    def test_corruption_only_in_extra_plan_is_refused_before_django_or_origin_paths(self):
        for corruption in ('lane', 'rate', 'indices', 'context', 'profile', 'missing_reservation'):
            bootstrap = self.bootstrap()
            if corruption == 'lane': bootstrap['plans'][5]['lane'] = 4
            elif corruption == 'rate': bootstrap['plans'][5]['rate'] = 12.5
            elif corruption == 'indices': bootstrap['plans'][5]['indices'] = [0]
            elif corruption == 'context': bootstrap['plans'][5]['context']['task_id'] = bootstrap['plans'][0]['context']['task_id']
            elif corruption == 'profile': bootstrap['plans'][5]['requested_numeric_profile'] = self.profile('writers-4')
            else: del bootstrap['requested_count']
            django = Mock()
            with self.subTest(corruption=corruption), patch.dict('sys.modules', {'django': django}), \
                 patch('benchmarks.events.business_commands.Path.mkdir') as mkdir, self.assertRaises(ValueError):
                InventoryProcessWorker(0, bootstrap)
            django.setup.assert_not_called()
            mkdir.assert_not_called()

    def test_child_and_parent_selector_or_diagnostic_mismatch_is_refused_before_setup(self):
        for key, value in (('writer_topology', 'writers-4'), ('diagnostic_profile_enabled', True),
                           ('runtime_diagnostics_enabled', True)):
            bootstrap = deepcopy(self.bootstrap())
            bootstrap[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_process_bootstrap(0, bootstrap)

    def test_supplied_generation_mismatch_or_invalid_metadata_fails_before_any_setup_io(self):
        journals = [requested_profile(), self.profile('writers-4'),
            requested_profile(writer_topology='writers-6'),
            requested_profile(writer_topology='writers-6', writer_topology_version='foreign'),
            requested_profile(writer_topology=True, writer_topology_version='writer-topology-v1'),
            requested_profile(writer_topology='writers-8', writer_topology_version='writer-topology-v1')]
        for journal in journals:
            with self.subTest(journal=journal), TemporaryDirectory() as directory:
                args = SimpleNamespace(evidence_dir=Path(directory), **self.profile())
                with patch('benchmarks.events.acceptance.Path.mkdir') as mkdir, \
                     patch('benchmarks.events.acceptance.load_environment') as environment, self.assertRaises(ValueError):
                    Harness(args, generation=SimpleNamespace(profile=journal))
                mkdir.assert_not_called()
                environment.assert_not_called()
                self.assertEqual(list(Path(directory).iterdir()), [])
        with TemporaryDirectory() as directory:
            args = SimpleNamespace(evidence_dir=Path(directory), **self.profile('writers-4'))
            with patch('benchmarks.events.acceptance.Path.mkdir') as mkdir, self.assertRaises(ValueError):
                Harness(args, generation=SimpleNamespace(profile=self.profile()))
            mkdir.assert_not_called()
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_extra_lane_journal_planning_failure_retains_all_reservations_without_launching_work(self):
        for observer_failure in (False, True):
            with self.subTest(observer_failure=observer_failure), TemporaryDirectory() as directory:
                harness = object.__new__(Harness)
                harness.args = SimpleNamespace(**self.profile())
                harness.evidence = Path(directory)
                harness._next_command_index = 0
                harness.generation_topologies, harness.runtime_diagnostic_errors = [], []
                harness.generation = Mock()
                original = OSError('extra lane journal startup')
                attempts = []
                def begin(count, rate, label):
                    lane = len(attempts)
                    attempts.append((lane, count, rate, label))
                    if lane == 4:
                        raise original
                    return 'batch-' + str(lane)
                harness.generation.begin_batch.side_effect = begin
                harness.generation.batch_summary.return_value = {'status': 'failed'}
                with patch('benchmarks.events.concurrent_generation.run_paced_lanes') as driver:
                    if observer_failure:
                        observer_error = RuntimeError('observer startup')
                        harness._record_failed_generation_start(24, 'steady', 50., 0, observer_error)
                        self.assertEqual(harness.generation_topologies[0]['error_type'], 'RuntimeError')
                    else:
                        with self.assertRaises(OSError) as raised:
                            harness._generate_concurrent_threads(24, 'steady', rate=50.)
                        self.assertIs(raised.exception, original)
                    driver.assert_not_called()
                self.assertEqual(len(attempts), 6)
                topology = harness.generation_topologies[0]
                self.assertEqual([row['requested'] for row in topology['journal_batches']], [4] * 6)
                self.assertEqual(sorted(index for row in topology['journal_batches'] for index in row['indices']), list(range(24)))
                self.assertIsNone(topology['journal_batches'][4]['batch_id'])
                self.assertEqual(topology['journal_batches'][4]['status'], 'creation_unknown')
                self.assertEqual(topology['journal_batches'][5]['batch_id'], 'batch-5')
                self.assertEqual((topology['requested'], topology['unattempted'], topology['committed']), (24, 24, 0))
                self.assertEqual(harness._next_command_index, 24)
                self.assertFalse(topology['passed'])
                self.assertEqual(harness._generation_accounting_error, 'OSError')

    def test_extra_process_plan_failure_retains_prefix_and_all_lane_reservations_without_spawn(self):
        with TemporaryDirectory() as directory:
            harness = object.__new__(Harness)
            harness.args = SimpleNamespace(run_id='writer-control', **self.profile())
            harness.evidence = Path(directory)
            harness._next_command_index = 0
            harness.generation_topologies = []
            harness.process_generation_enabled = True
            parent = GenerationJournal(harness.evidence, harness.args.run_id, self.profile())
            harness.generation = CompositeGenerationJournal(parent)
            harness.admin, harness.source, harness.target = (
                SimpleNamespace(id=UUID(int=n)) for n in (1, 2, 3))
            harness.settings = SimpleNamespace(EVENT_TRANSPORT='kafka', KAFKA_TOPIC='fixed.inventory.v1',
                KAFKA_SOURCE_CLUSTER_ID='validation.fixed', KAFKA_SOURCE_STREAM_GENERATION='validation.v1',
                EVENT_MAX_PAYLOAD_BYTES=1024)
            harness.connection = SimpleNamespace(settings_dict={'ENGINE': 'django.db.backends.postgresql',
                'HOST': 'unused', 'PORT': '5432', 'NAME': 'isolated', 'USER': 'unused'})
            harness.business_lanes = [{name: SimpleNamespace(id=UUID(int=base + lane))
                for name, base in (('project', 100), ('task', 200), ('order', 300), ('order_line', 400))}
                | {'batch': None, 'cycle_issue': None} for lane in range(6)]
            harness.merge_origin_events = Mock()
            original = OSError('extra lane plan failed')
            add = harness.generation.add_origin_plan
            attempted = []
            def add_plan(plan):
                attempted.append(plan['lane'])
                if plan['lane'] == 4:
                    raise original
                return add(plan)
            try:
                with patch.object(harness.generation, 'add_origin_plan', side_effect=add_plan), \
                     patch('benchmarks.events.process_generation.run_paced_processes') as driver, \
                     self.assertRaises(OSError) as raised:
                    harness._generate_processes(24, 'steady', rate=50.)
                self.assertIs(raised.exception, original)
                driver.assert_not_called()
                self.assertEqual(attempted, list(range(5)))
                topology = harness.generation_topologies[0]
                self.assertEqual([plan['lane'] for plan in topology['origin_plans']], list(range(4)))
                reservations = topology['origin_reservations']
                self.assertEqual([row['requested'] for row in reservations], [4] * 6)
                self.assertEqual(sorted(index for row in reservations for index in row['indices']), list(range(24)))
                self.assertEqual([row['status'] for row in reservations],
                    ['created'] * 4 + ['creation_unknown', 'not_created'])
                self.assertTrue(all(row['fallback_status'] == 'created' for row in reservations[4:]))
                totals = harness.generation.summary()['totals']
                self.assertEqual((totals['requested'], totals['attempted'], totals['committed'],
                    totals['identified_events'], totals['unattempted']), (24, 0, 0, 0, 24))
                self.assertEqual(harness._next_command_index, 24)
                self.assertFalse(topology['passed'])
                self.assertEqual(topology['error_type'], 'OSError')
                self.assertEqual(topology['secondary_errors'], [])
            finally:
                parent.finalize()
