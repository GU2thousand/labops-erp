"""Instrumentation shares Harness clocks and must not hide business failures."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from django.test import SimpleTestCase

from benchmarks.events.acceptance import Harness, main, write_json as persist_fixture_json
from benchmarks.events.container_diagnostics import DEFAULT_SERVICES
from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
from benchmarks.events.origin_journal import CompositeGenerationJournal
from benchmarks.events.process_resources import GENERATOR_ROLES, summarize_process_resources
from labops.tests.test_process_resources import ProcessFixture


class HarnessProcessCleanupCatalogTests(SimpleTestCase):
    """Exercise Harness's real observer with exact-PID, resource-free fixtures."""

    def run_observer(self, script):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        fixture = ProcessFixture(required_workers=())
        h = object.__new__(Harness)
        h.evidence = Path(directory.name)
        h.args = SimpleNamespace(run_id='cleanup-catalog-control')
        h._next_command_index = 0
        h.events, h.workers, h.generation_topologies = [], {}, []
        h.process_generation_enabled = h.runtime_diagnostics_enabled = True
        h._active_process_catalog = fixture.catalog
        h.generation = Mock(spec=CompositeGenerationJournal)
        # Deliberately omit business accounting: resource completeness alone
        # must never turn this lifecycle control into passing acceptance.
        h.generation.summary.return_value = {'batches': []}
        h.settings = SimpleNamespace(EVENT_TRANSPORT='kafka', KAFKA_TOPIC='control.inventory',
            KAFKA_SOURCE_CLUSTER_ID='control.cluster', KAFKA_SOURCE_STREAM_GENERATION=1,
            EVENT_MAX_PAYLOAD_BYTES=1048576)
        h.connection = SimpleNamespace(settings_dict={'ENGINE': 'controlled-no-database',
            'HOST': 'controlled', 'PORT': '0', 'NAME': 'controlled', 'USER': 'controlled'})
        h.admin, h.source, h.target = [SimpleNamespace(id=name) for name in ('admin', 'source', 'target')]
        h.business_lanes = [{**{name: SimpleNamespace(id=f'{name}-{lane}')
            for name in ('task', 'project', 'order', 'order_line')},
            'batch': None, 'cycle_issue': None} for lane in range(4)]
        h.settle_process_sessions = Mock(return_value=True)
        h.merge_origin_events = Mock()
        h.restore_process_lane_state = Mock()
        h.process_database_facts = Mock()

        def driver(count, rate, worker, bootstrap, *, start_index, on_observation,
                   on_process_started, on_process_ready):
            for plan in bootstrap['plans']:
                lane, role = plan['lane'], f'generator-{plan["lane"]}'
                fixture.set(role)
                on_process_started(lane, fixture.pids[role])
                on_process_ready(lane, fixture.pids[role], {
                    'backend_identity': {'database_name': bootstrap['database_config']['NAME'],
                                         'application_name': plan['application_name']},
                    'runtime_namespace': bootstrap['runtime_settings'],
                    'process_snapshot': fixture.child(role)})
            script(fixture, on_observation)
            return []

        with patch('benchmarks.events.process_generation.run_paced_processes', side_effect=driver):
            try:
                h._generate_processes(16, 'steady', rate=50)
            except AssertionError:
                raise
            except Exception as error:
                return h, fixture, error
        self.fail('Control fixture lacks business accounting and must remain failed acceptance')

    def cleanup(self, fixture, observe, lane):
        role = f'generator-{lane}'
        fixture.set(role, user=30, system=15)
        metadata = {'connection_closed': True, 'process_snapshot': fixture.child(role),
                    'lane_state': {'batch_id': None, 'cycle_issue_id': None}}
        observe({'kind': 'cleanup_complete', 'lane': lane, 'pid': fixture.pids[role], 'metadata': metadata})
        return metadata

    def exit(self, fixture, observe, lane, code=0):
        observe({'kind': 'process_exit', 'lane': lane, 'pid': fixture.pids[f'generator-{lane}'],
                 'exitcode': code})

    def finish_others(self, fixture, observe, target=2):
        for lane in range(4):
            if lane != target:
                self.cleanup(fixture, observe, lane)
                del fixture.files[f'/proc/{fixture.pids[f"generator-{lane}"]}/stat']
                self.exit(fixture, observe, lane)

    def test_cleanup_then_sample_skips_finished_child_but_requires_actual_successful_exits(self):
        def script(fixture, observe):
            live = fixture.clean()
            receipt = self.cleanup(fixture, observe, 2)
            path = f'/proc/{fixture.pids["generator-2"]}/stat'
            del fixture.files[path]  # The child may disappear before its exit frame.
            reads = len(fixture.reads)
            between = fixture.clean()
            row = between['processes'][2]
            self.assertEqual((row['stage'], row['cleaned'], row['exitcode']), ('cleaned', True, None))
            self.assertFalse(row['applicable'])
            self.assertIsNone(row['value'])
            self.assertEqual(row['final_snapshot'], receipt['process_snapshot'])
            self.assertEqual(row['errors'], [])
            self.assertNotIn(path, fixture.reads[reads:])
            for lane in (0, 1, 3):
                self.assertIn(f'/proc/{fixture.pids[f"generator-{lane}"]}/stat', fixture.reads[reads:])
                self.cleanup(fixture, observe, lane)
                del fixture.files[f'/proc/{fixture.pids[f"generator-{lane}"]}/stat']
            cleaned = fixture.clean()
            self.assertFalse(summarize_process_resources([live, between], cleaned)['collection_complete'])
            self.assertTrue(all(row['exitcode'] is None for row in cleaned['processes'][:4]))
            for lane in range(4):
                self.exit(fixture, observe, lane)
            final = fixture.clean()
            self.assertTrue(summarize_process_resources([live, between], final)['collection_complete'])
            self.assertTrue(all(row['stage'] == 'exited' and row['exitcode'] == 0
                                for row in final['processes'][:4]))

        h, fixture, error = self.run_observer(script)
        self.assertIsInstance(error, RuntimeError)
        self.assertFalse(h.generation_topologies[-1]['passed'])
        self.assertEqual(fixture.clean()['processes'][2]['errors'], [])

    def test_missing_receipt_or_unclosed_connection_does_not_end_live_sampling(self):
        for boundary in ('missing_metadata', 'missing_snapshot', 'unclosed', 'non_boolean_closed'):
            with self.subTest(boundary=boundary):
                def script(fixture, observe):
                    live = fixture.clean()
                    self.finish_others(fixture, observe)
                    metadata = {'connection_closed': True, 'process_snapshot': fixture.child('generator-2'),
                                'lane_state': {'batch_id': None, 'cycle_issue_id': None}}
                    if boundary == 'missing_metadata':
                        metadata = None
                    elif boundary == 'missing_snapshot':
                        metadata.pop('process_snapshot')
                    else:
                        metadata['connection_closed'] = False if boundary == 'unclosed' else 'true'
                    observe({'kind': 'cleanup_complete', 'lane': 2, 'metadata': metadata})
                    reads = len(fixture.reads)
                    between = fixture.clean()
                    row = between['processes'][2]
                    self.assertEqual((row['stage'], row['cleaned'], row['exitcode']), ('running', False, None))
                    self.assertTrue(row['applicable'])
                    self.assertIn(f'/proc/{fixture.pids["generator-2"]}/stat', fixture.reads[reads:])
                    self.exit(fixture, observe, 2)
                    self.assertFalse(summarize_process_resources([live, between], fixture.clean())['collection_complete'])

                _h, _fixture, error = self.run_observer(script)
                self.assertIsInstance(error, RuntimeError)

    def test_stale_foreign_or_invalid_receipt_retains_error_and_never_marks_cleaned(self):
        changes = ({'pid': 9999}, {'start_time_ticks': 99}, {'source': 'kernel_proc_stat'},
                   {'captured_monotonic': 99}, {'rss_bytes': None}, {'user_cpu_seconds': .01})
        for change in changes:
            with self.subTest(change=change):
                def script(fixture, observe):
                    receipt = fixture.child('generator-2') | change
                    observe({'kind': 'cleanup_complete', 'lane': 2,
                        'metadata': {'connection_closed': True, 'process_snapshot': receipt}})

                _h, fixture, error = self.run_observer(script)
                self.assertIsInstance(error, ValueError)
                row = fixture.clean()['processes'][2]
                self.assertEqual((row['stage'], row['cleaned'], row['exitcode']), ('running', False, None))
                self.assertIsNone(row['final_snapshot'])
                self.assertIn({'stage': 'final_receipt', 'error_type': 'ValueError', 'occurrences': 1}, row['errors'])

    def test_real_live_read_failure_before_cleanup_is_not_erased_by_valid_receipt_and_exit(self):
        def script(fixture, observe):
            live = fixture.clean()
            path = f'/proc/{fixture.pids["generator-2"]}/stat'
            fixture.files[path] = ValueError('Controlled unavailable live process')
            failed = fixture.clean()
            self.assertEqual(failed['processes'][2]['stage'], 'running')
            self.assertEqual(failed['processes'][2]['value']['status'], 'unavailable')
            self.cleanup(fixture, observe, 2)
            del fixture.files[path]
            self.exit(fixture, observe, 2)
            self.finish_others(fixture, observe)
            final = fixture.clean()
            self.assertFalse(summarize_process_resources([live, failed], final)['collection_complete'])
            self.assertIn({'stage': 'live_sample', 'error_type': 'ValueError', 'occurrences': 1},
                          final['processes'][2]['errors'])

        _h, _fixture, error = self.run_observer(script)
        self.assertIsInstance(error, RuntimeError)

    def test_actual_negative_exit_stays_incomplete_with_or_without_cleanup_receipt(self):
        for cleaned in (False, True):
            with self.subTest(cleaned=cleaned):
                def script(fixture, observe):
                    live = fixture.clean()
                    self.finish_others(fixture, observe)
                    if cleaned:
                        self.cleanup(fixture, observe, 2)
                    del fixture.files[f'/proc/{fixture.pids["generator-2"]}/stat']
                    self.exit(fixture, observe, 2, code=-9)
                    final = fixture.clean()
                    row = final['processes'][2]
                    self.assertEqual((row['stage'], row['cleaned'], row['exitcode']), ('exited', cleaned, -9))
                    self.assertEqual(row['final_snapshot'] is not None, cleaned)
                    self.assertFalse(summarize_process_resources([live], final)['collection_complete'])

                _h, _fixture, error = self.run_observer(script)
                self.assertIsInstance(error, RuntimeError)


class Clock:
    def __init__(self):
        self.now = 100.0

    def advance(self, seconds):
        self.now += seconds


class RuntimeProbe:
    """A resource-free observer with visible startup, persistence and cleanup."""
    def __init__(self, clock, path, *, enter_error=None, exit_error=None,
                 summary_error=None, collection_complete=True, lifecycle_complete=True):
        self.clock, self.path = clock, path
        self.enter_error, self.exit_error = enter_error, exit_error
        self.summary_error = summary_error
        self.collection_complete, self.lifecycle_complete = collection_complete, lifecycle_complete
        self.active = self.closed = self.persisted = False
        self.exit_exception = None

    def __enter__(self):
        self.clock.advance(2)
        if self.enter_error:
            raise self.enter_error
        self.active = True
        return self

    def __exit__(self, kind, value, traceback):
        self.exit_exception = value
        self.active, self.closed = False, True
        self.clock.advance(3)  # Stop/join the sampler.
        self.path.write_text(json.dumps({'samples': [{'sequence': 1}, {'sequence': 2}],
                                        'business_error_type': kind.__name__ if kind else None}))
        self.clock.advance(5)  # Persist the diagnostic evidence.
        self.persisted = True
        if self.exit_error:
            raise self.exit_error
        return False

    def summary(self):
        self.clock.advance(2)
        if self.summary_error:
            raise self.summary_error
        return {'collection_complete': self.closed and self.persisted and self.collection_complete,
                'lifecycle_complete': self.closed and self.persisted and self.lifecycle_complete,
                'expected_resource_roles': list(DEFAULT_SERVICES), 'known_stopped_roles': [],
                'observed_cpu_memory_roles': [role for role in DEFAULT_SERVICES
                    if self.collection_complete or role != 'postgres'],
                'sample_count': 2, 'sampler_joined': self.closed}


class CommandProbe:
    def __init__(self, *, summary_error=None, exit_error=None, diagnostic_errors=None,
                 collection_complete=True):
        self.active = self.closed = False
        self.error = None
        self.summary_error = summary_error
        self.exit_error = exit_error
        self.diagnostic_errors = diagnostic_errors or []
        self.collection_complete = collection_complete

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, kind, value, traceback):
        self.active, self.closed, self.error = False, True, value
        if self.exit_error:
            raise self.exit_error
        return False

    def summary(self):
        if self.summary_error:
            raise self.summary_error
        return {'observed': self.closed, 'completed': self.closed,
                'collection_complete': self.closed and self.collection_complete,
                'sql': {'count': 3, 'operation_counts': {'SELECT': 2, 'INSERT': 1}},
                'physical_commit': {'attempts': 1, 'wall_seconds': 0.125},
                'diagnostic_errors': self.diagnostic_errors,
                'error_type': type(self.error).__name__ if self.error else None}


class HarnessRuntimeDiagnosticsTests(SimpleTestCase):
    def harness(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        h = object.__new__(Harness)
        h.evidence = Path(directory.name)
        h.args = SimpleNamespace(run_id='diagnostics-harness', tier='smoke', events=32,
            rate=50, duration=0, fault_repetitions=1, fault_events=1,
            duplicate_events=1, poison_events=12, broker_fault_seconds=1,
            outage_seconds=1, consumer_outage_seconds=1, drain_timeout=900)
        h.runtime_diagnostics_enabled = True
        h.runtime_diagnostics, h.runtime_diagnostic_errors = [], []
        h.generation_topologies, h.events = [], []
        h._next_command_index = 0
        h.generation = GenerationJournal(h.evidence, h.args.run_id, numeric_profile(h.args))
        self.addCleanup(h.generation.finalize)
        h.container_resources = Mock()
        h.container_resources.profile.return_value = {'scope': 'synthetic_unit_test',
            'containers': {role: {'status': 'available'} for role in DEFAULT_SERVICES}}
        h.connection = object()
        h._event_lock = threading.RLock()
        h.business_lanes = [{'batch': None, 'cycle_issue': None} for _ in range(4)]
        return h

    def test_default_configuration_does_not_discover_or_wrap_business_commands(self):
        h = self.harness()
        h.compose, h.command = Mock(), Mock()
        h.env = {'LABOPS_VALIDATION_PROJECT': 'owned-test-project'}
        with patch('benchmarks.events.container_diagnostics.ContainerResources.from_compose') as discover:
            h.configure_runtime_diagnostics()
        discover.assert_not_called()
        self.assertFalse(h.runtime_diagnostics_enabled)
        self.assertIsNone(h.runtime_resource_factory)
        profile = json.loads((h.evidence / 'runtime-resource-profile.json').read_text())
        self.assertEqual(profile['status'], 'NOT_REQUESTED')
        self.assertFalse(profile['applicable'])
        self.assertIsNone(profile['collection_complete'])
        h._execute_inventory_command_body = Mock(return_value={'event_id': 'original'})
        with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics') as observer:
            result = h._execute_inventory_command(0, 'steady', 'batch-1', {}, {},
                                                   lane=0, scheduled_at=123.5)
        observer.assert_not_called()
        self.assertEqual(result, {'event_id': 'original'})
        h._execute_inventory_command_body.assert_called_once()
        h._generate_concurrent = Mock(return_value=(['original'], {'elapsed_seconds': 6}))
        with patch('benchmarks.events.runtime_diagnostics.RuntimeDiagnostics') as sampler:
            self.assertEqual(h.generate(32, 'steady'), (['original'], {'elapsed_seconds': 6}))
        sampler.assert_not_called()

    def test_enabled_configuration_defers_fresh_discovery_until_measured_batch(self):
        h = self.harness()
        h.args.runtime_diagnostics = True
        h.compose, h.command = Mock(), Mock()
        h.env = {'LABOPS_VALIDATION_PROJECT': 'owned-test-project'}
        with patch('benchmarks.events.container_diagnostics.ContainerResources.from_compose') as discover:
            h.configure_runtime_diagnostics()
            discover.assert_not_called()
            resources = h.runtime_resource_factory()
        self.assertIs(resources, discover.return_value)
        discover.assert_called_once_with(h.compose, command_callback=h.command,
            expected_project='owned-test-project')
        profile = json.loads((h.evidence / 'runtime-resource-profile.json').read_text())
        self.assertEqual(profile['status'], 'REQUESTED_NOT_STARTED')
        self.assertTrue(profile['enabled'] and profile['applicable'])
        self.assertIsNone(profile['collection_complete'])

    def test_cli_freezes_opt_in_before_environment_setup_for_both_tiers(self):
        for tier, enabled in (('smoke', False), ('smoke', True), ('full', False), ('full', True)):
            with self.subTest(tier=tier, enabled=enabled):
                h = self.harness()
                directory = h.evidence / 'cli'
                argv = ['acceptance.py', '--run-id', 'diagnostic-policy', '--tier', tier,
                        '--evidence-dir', str(directory)]
                if tier == 'full':
                    argv += ['--events', '90000', '--rate', '50', '--duration', '1800',
                             '--fault-repetitions', '20', '--fault-events', '30000',
                             '--broker-fault-seconds', '300', '--outage-seconds', '600',
                             '--consumer-outage-seconds', '600', '--drain-timeout', '900']
                if enabled:
                    argv.append('--runtime-diagnostics')
                original = OSError('environment setup boundary')
                def unavailable_environment(_path):
                    requested = json.loads((directory / 'requested-profile.json').read_text())
                    execution = json.loads((directory / 'generation-execution-profile.json').read_text())
                    self.assertIs(requested['requested_numeric_profile']['runtime_diagnostics_enabled'], enabled)
                    policy = execution['runtime_diagnostics']
                    self.assertIs(policy['enabled'], enabled)
                    self.assertIs(policy['applicable'], enabled)
                    self.assertEqual(policy['request_status'], 'REQUESTED' if enabled else 'NOT_REQUESTED')
                    raise original
                with patch('sys.argv', argv), patch('benchmarks.events.acceptance.load_environment',
                        side_effect=unavailable_environment), self.assertRaises(OSError) as raised:
                    main()
                self.assertIs(raised.exception, original)
                failure = json.loads((directory / 'startup-failure.json').read_text())
                self.assertIs(failure['requested_numeric_profile']['runtime_diagnostics_enabled'], enabled)

    def generation_body(self, h, clock, probe, *, error=None, scenario='steady'):
        ids = [str(uuid4()) for _ in range(32)]

        def body(count, label, *, rate):
            self.assertTrue(probe.active, 'Sampler scope must surround generation.')
            self.assertEqual((count, label, rate), (32, scenario, 50))
            self.assertEqual(len(h.business_lanes), 4)
            h._next_command_index += count
            clock.advance(7)
            topology = {'scenario': label, 'passed': error is None,
                'generator_topology': 'parallel-lanes-v1', 'requested': count,
                'lane_count': 4, 'queue_capacity_per_lane': 4, 'global_target_rate': rate,
                'assignment': '(global_index//4)%4', 'elapsed_seconds': 7,
                'committed': 6 if error else count, 'unattempted': 26 if error else 0}
            h.generation_topologies.append(topology)
            # Retain this imported alias when injecting a failure in Harness's
            # later rewrite; the fixture's initial evidence must still persist.
            persist_fixture_json(h.evidence / 'generation-topology-001.json', topology)
            if error:
                raise error
            return ids, {'input': count, 'completed_commands': count, 'target_rate': rate,
                         'generator_topology': 'parallel-lanes-v1', 'lane_count': 4}

        h._generate_concurrent = Mock(side_effect=body)
        return ids

    def runtime_patch(self, h, clock, probe):
        return patch('benchmarks.events.runtime_diagnostics.RuntimeDiagnostics', return_value=probe), \
            patch('benchmarks.events.acceptance.time.monotonic', side_effect=lambda: clock.now)

    def fail_topology_rewrite(self, error):
        def write(path, value):
            if Path(path).name.startswith('generation-topology-'):
                raise error
            return persist_fixture_json(path, value)
        return write

    def test_generation_rate_and_topology_include_sampler_start_stop_persistence_and_summary(self):
        h, clock = self.harness(), Clock()
        path = h.evidence / 'runtime-diagnostics-001.json'
        probe = RuntimeProbe(clock, path)
        ids = self.generation_body(h, clock, probe)
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch as factory, clock_patch:
            result, workload = h.generate(32, 'steady')
        factory.assert_called_once_with(path, scenario='steady',
            resource_sampler=h.container_resources.snapshot, known_stopped_roles=(),
            expected_resource_roles=DEFAULT_SERVICES)
        self.assertIs(result, ids)
        self.assertTrue(probe.closed and probe.persisted)
        self.assertEqual(workload['elapsed_seconds'], 19)
        self.assertEqual(workload['actual_command_rate'], 32 / 19)
        self.assertAlmostEqual(workload['schedule_lateness_seconds'], 19 - 32 / 50)
        self.assertEqual((workload['input'], workload['completed_commands'], workload['target_rate']),
                         (32, 32, 50))
        topology = json.loads((h.evidence / 'generation-topology-001.json').read_text())
        self.assertTrue(topology['passed'])
        self.assertEqual(topology['elapsed_seconds'], 19)
        self.assertTrue(topology['elapsed_includes_diagnostics_cleanup'])
        self.assertEqual(topology['runtime_diagnostics_artifact'], path.name)
        self.assertEqual(json.loads(path.read_text())['samples'], [{'sequence': 1}, {'sequence': 2}])
        self.assertEqual(h.runtime_diagnostics[0]['elapsed_seconds'], 19)
        self.assertNotIn('samples', h.runtime_diagnostics[0]['summary'])

    def test_partial_generation_keeps_original_error_diagnostics_and_failed_topology(self):
        h, clock = self.harness(), Clock()
        original = RuntimeError('post-commit observation')
        probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json')
        self.generation_body(h, clock, probe, error=original)
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch, clock_patch, self.assertRaises(RuntimeError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        self.assertIs(probe.exit_exception, original)
        self.assertTrue(probe.closed and probe.persisted)
        topology = json.loads((h.evidence / 'generation-topology-001.json').read_text())
        self.assertFalse(topology['passed'])
        self.assertEqual((topology['committed'], topology['unattempted']), (6, 26))
        self.assertEqual(topology['elapsed_seconds'], 19)
        self.assertEqual(h.runtime_diagnostics[0]['artifact'], probe.path.name)

    def test_summary_io_failure_cannot_replace_original_generation_error(self):
        h, clock = self.harness(), Clock()
        original = RuntimeError('business error')
        probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json',
                             summary_error=OSError('summary unavailable'))
        self.generation_body(h, clock, probe, error=original)
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch, clock_patch, self.assertRaises(RuntimeError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        self.assertIn('OSError', h.runtime_diagnostic_errors)
        self.assertFalse(h.runtime_diagnostics[0]['summary']['observed'])
        self.assertFalse(h.generation_topologies[0]['passed'])

    def test_sampler_exit_error_cannot_mask_partial_generation_business_error(self):
        h, clock = self.harness(), Clock()
        original = RuntimeError('business failure during generation')
        probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json',
                             exit_error=OSError('sampler evidence persistence'))
        self.generation_body(h, clock, probe, error=original)
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch, clock_patch, self.assertRaises(RuntimeError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        self.assertIs(probe.exit_exception, original)
        self.assertTrue(probe.closed and probe.persisted)
        self.assertIn('OSError', h.runtime_diagnostic_errors)
        self.assertFalse(h.generation_topologies[0]['passed'])

    def test_final_clock_failure_retains_original_business_error_and_unknown_timing(self):
        for boundary in ('monotonic', 'process_time'):
            for error_type in (OSError, KeyboardInterrupt):
                with self.subTest(boundary=boundary, secondary=error_type.__name__):
                    h, clock = self.harness(), Clock()
                    original = RuntimeError('business original before final clock sample')
                    secondary = error_type('final diagnostic clock unavailable')
                    probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json')
                    self.generation_body(h, clock, probe, error=original)
                    actual_fake_body = h._generate_concurrent.side_effect
                    failed = {'body': False}

                    def body(*args, **kwargs):
                        try:
                            return actual_fake_body(*args, **kwargs)
                        except BaseException as error:
                            self.assertIs(error, original)
                            failed['body'] = True
                            raise

                    def monotonic():
                        if failed['body'] and boundary == 'monotonic':
                            raise secondary
                        return clock.now

                    def process_time():
                        if failed['body'] and boundary == 'process_time':
                            raise secondary
                        return 10.0

                    h._generate_concurrent.side_effect = body
                    with patch('benchmarks.events.runtime_diagnostics.RuntimeDiagnostics', return_value=probe), \
                         patch('benchmarks.events.acceptance.time.monotonic', side_effect=monotonic), \
                         patch('benchmarks.events.acceptance.time.process_time', side_effect=process_time), \
                         self.assertRaises(RuntimeError) as raised:
                        h.generate(32, 'steady')
                    self.assertIs(raised.exception, original)
                    self.assertTrue(failed['body'])
                    self.assertTrue(probe.closed and probe.persisted)
                    evidence = h.runtime_diagnostics[0]
                    self.assertIsNone(evidence['whole_generation_process_cpu_seconds'])
                    if boundary == 'monotonic':
                        self.assertIsNone(evidence['elapsed_seconds'])
                    else:
                        self.assertEqual(evidence['elapsed_seconds'], 19)
                    self.assertIn(error_type.__name__, h.runtime_diagnostic_errors)
                    topology = json.loads((h.evidence / 'generation-topology-001.json').read_text())
                    self.assertFalse(topology['passed'])
                    self.assertEqual(topology['error_type'], 'RuntimeError')
                    self.assertEqual(topology['elapsed_seconds'], evidence['elapsed_seconds'])

                    # Even a caller that caught the original error cannot turn
                    # unknown clock evidence into a passing final report.
                    h.children, h.logs, h.shutdowns, h.supervisor_restarts = [], [], [], []
                    h.workers, h.cases, h.delivery_proofs = {}, [], []
                    h.started_at, h.models = 0, Mock()
                    h.models.FailedDelivery.objects.order_by.return_value.values.return_value = []
                    (h.evidence / 'errors.jsonl').touch()
                    h.final_inventory_evidence = Mock(return_value={
                        'database_observed': True, 'offsets_observed': True, 'errors': [],
                        'actual_inventory_outbox_count': 0, 'unpublished_count': 0,
                        'consumers': {role: {'incomplete_count': 0} for role in ('notification', 'analytics')},
                        'event_log_ids_missing_from_database': [], 'actual_committed_ids_missing_from_event_log': [],
                        'journal_committed_movements_missing_from_database': [],
                        'database_committed_movements_missing_from_journal': [],
                        'journal_identified_events_missing_from_database': [], 'processed_hash_conflicts': [],
                        'reconciliation': {'mismatches': [], 'dedupe_count': 0,
                            'notification_count': 0, 'expected_notification_count': 0}})
                    with patch('builtins.print'):
                        report = h.finish()
                    self.assertFalse(report['runtime_diagnostics_complete'])
                    self.assertFalse(report['generation_topologies_complete'])
                    self.assertFalse(report['passed'])
                    self.assertFalse(json.loads((h.evidence / 'report.json').read_text())['passed'])

    def test_topology_rewrite_error_cannot_replace_original_generation_error(self):
        h, clock = self.harness(), Clock()
        original = RuntimeError('business error')
        probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json')
        self.generation_body(h, clock, probe, error=original)
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch, clock_patch, \
             patch('benchmarks.events.acceptance.write_json',
                   side_effect=self.fail_topology_rewrite(OSError('metadata write'))), \
             self.assertRaises(RuntimeError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        self.assertIn('OSError', h.runtime_diagnostic_errors)
        self.assertTrue(probe.persisted)
        self.assertFalse(h.generation_topologies[0]['passed'])

    def test_diagnostic_failure_after_successful_generation_marks_topology_failed(self):
        for boundary in ('exit', 'summary', 'topology_write'):
            with self.subTest(boundary=boundary):
                h, clock = self.harness(), Clock()
                original = OSError('diagnostic evidence unavailable')
                probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json',
                    exit_error=original if boundary == 'exit' else None,
                    summary_error=original if boundary == 'summary' else None)
                self.generation_body(h, clock, probe)
                observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
                write_patch = (patch('benchmarks.events.acceptance.write_json',
                                    side_effect=self.fail_topology_rewrite(original))
                    if boundary == 'topology_write' else patch('benchmarks.events.acceptance.write_json', wraps=persist_fixture_json))
                with observer_patch, clock_patch, write_patch, self.assertRaises(OSError) as raised:
                    h.generate(32, 'steady')
                self.assertIs(raised.exception, original)
                self.assertTrue(probe.closed and probe.persisted)
                self.assertFalse(h.generation_topologies[0]['passed'])

    def test_sampler_startup_failure_does_not_modify_a_prior_same_scenario_topology(self):
        h, clock = self.harness(), Clock()
        prior = {'scenario': 'steady', 'passed': True, 'elapsed_seconds': 5}
        h.generation_topologies.append(prior.copy())
        original = OSError('sampler startup')
        probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json', enter_error=original)
        h._generate_concurrent = Mock()
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch, clock_patch, self.assertRaises(OSError) as raised:
            h.generate(32, 'steady')
        self.assertIs(raised.exception, original)
        h._generate_concurrent.assert_not_called()
        self.assertEqual(h.generation_topologies[0], prior)
        self.assertEqual(len(h.generation_topologies), 2)
        failed = h.generation_topologies[1]
        self.assertFalse(failed['passed'])
        self.assertEqual((failed['requested'], failed['attempted'], failed['committed'],
                          failed['unattempted']), (32, 0, 0, 32))
        self.assertEqual(h._next_command_index, 32)
        totals = h.generation.summary()['totals']
        self.assertEqual((totals['requested'], totals['attempted'], totals['committed'],
                          totals['unattempted']), (32, 0, 0, 32))
        self.assertFalse((h.evidence / 'generation-topology-001.json').exists())
        self.assertFalse(json.loads((h.evidence / 'generation-topology-002.json').read_text())['passed'])

    def test_fault_missing_resource_coverage_preserves_core_generation_results(self):
        for scenario in ('analytics_outage', 'one_broker_stop', 'quorum_loss', 'cluster_outage'):
            with self.subTest(scenario=scenario):
                h, clock = self.harness(), Clock()
                probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json',
                                     collection_complete=False, lifecycle_complete=True)
                ids = self.generation_body(h, clock, probe, scenario=scenario)
                observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
                with observer_patch, clock_patch:
                    observed_ids, workload = h.generate(32, scenario)
                self.assertIs(observed_ids, ids)
                self.assertEqual((workload['input'], workload['completed_commands'], workload['target_rate']),
                                 (32, 32, 50))
                self.assertEqual(workload['actual_command_rate'], 32 / 19)
                self.assertTrue(h.generation_topologies[0]['passed'])
                summary = h.runtime_diagnostics[0]['summary']
                self.assertFalse(summary['collection_complete'])
                self.assertTrue(summary['lifecycle_complete'])
                self.assertEqual(set(summary['expected_resource_roles'])
                                 - set(summary['observed_cpu_memory_roles']), {'postgres'})
                self.assertEqual(h.runtime_diagnostic_errors, [])

    def test_steady_missing_resource_coverage_rejects_completed_generation_topology(self):
        h, clock = self.harness(), Clock()
        probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json',
                             collection_complete=False, lifecycle_complete=True)
        self.generation_body(h, clock, probe)
        observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
        with observer_patch, clock_patch:
            _, workload = h.generate(32, 'steady')
        self.assertEqual((workload['input'], workload['completed_commands'], workload['target_rate']), (32, 32, 50))
        self.assertFalse(h.generation_topologies[0]['passed'])
        self.assertEqual(h.generation_topologies[0]['error_type'], 'IncompleteRuntimeDiagnostics')
        self.assertFalse(h.runtime_diagnostics[0]['summary']['collection_complete'])
        self.assertTrue(h.runtime_diagnostics[0]['summary']['lifecycle_complete'])

    def test_incomplete_sampler_lifecycle_rejects_every_fault_generation_topology(self):
        for scenario in ('analytics_outage', 'one_broker_stop', 'quorum_loss', 'cluster_outage'):
            with self.subTest(scenario=scenario):
                h, clock = self.harness(), Clock()
                probe = RuntimeProbe(clock, h.evidence / 'runtime-diagnostics-001.json',
                                     collection_complete=True, lifecycle_complete=False)
                self.generation_body(h, clock, probe, scenario=scenario)
                observer_patch, clock_patch = self.runtime_patch(h, clock, probe)
                with observer_patch, clock_patch:
                    _, workload = h.generate(32, scenario)
                self.assertEqual(workload['completed_commands'], 32)
                self.assertFalse(h.generation_topologies[0]['passed'])
                self.assertEqual(h.generation_topologies[0]['error_type'], 'IncompleteRuntimeDiagnostics')
                self.assertFalse(h.runtime_diagnostics[0]['summary']['lifecycle_complete'])

    def test_command_observer_surrounds_body_on_own_connection_and_records_identity(self):
        h, probe = self.harness(), CommandProbe()
        state, data = {'stage': 'initialization'}, {'batch': object()}
        expected = {'event_id': str(uuid4())}

        def body(*args, **kwargs):
            self.assertTrue(probe.active)
            self.assertEqual(args, (17, 'steady', 'batch-1', state, data))
            self.assertEqual(kwargs, {'lane': 0, 'scheduled_at': 123.5})
            return expected

        h._execute_inventory_command_body = Mock(side_effect=body)
        with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics', return_value=probe) as factory:
            result = h._execute_inventory_command(17, 'steady', 'batch-1', state, data,
                                                  lane=0, scheduled_at=123.5)
        factory.assert_called_once_with(h.connection)
        self.assertIs(result, expected)
        self.assertTrue(probe.closed)
        row = json.loads((h.evidence / 'command-diagnostics.jsonl').read_text())
        self.assertEqual((row['global_index'], row['business_lane'], row['kind'], row['scenario']),
                         (17, 0, 'ISSUE', 'steady'))
        self.assertEqual(row['sql']['count'], 3)
        self.assertEqual(row['physical_commit']['wall_seconds'], .125)
        self.assertIsNone(row['error_type'])

    def test_resource_discovery_and_observer_construction_failures_retain_unattempted_work(self):
        for boundary in ('resource_discovery', 'observer_constructor'):
            with self.subTest(boundary=boundary):
                h = self.harness()
                original = OSError('diagnostic startup')
                prior = {'scenario': 'steady', 'passed': True, 'elapsed_seconds': 5}
                h.generation_topologies.append(prior.copy())
                h._generate_concurrent = Mock()
                if boundary == 'resource_discovery':
                    h.runtime_resource_factory = Mock(side_effect=original)
                with patch('benchmarks.events.runtime_diagnostics.RuntimeDiagnostics',
                           side_effect=original) as constructor, \
                     self.assertRaises(OSError) as raised:
                    h.generate(32, 'steady')
                self.assertIs(raised.exception, original)
                h._generate_concurrent.assert_not_called()
                if boundary == 'resource_discovery':
                    constructor.assert_not_called()
                else:
                    constructor.assert_called_once()
                self.assertEqual(h.generation_topologies[0], prior)
                failed = h.generation_topologies[1]
                self.assertFalse(failed['passed'])
                self.assertEqual((failed['requested'], failed['unattempted'],
                                  failed['global_target_rate'], failed['lane_count']), (32, 32, 50, 4))
                self.assertEqual(h._next_command_index, 32)
                totals = h.generation.summary()['totals']
                self.assertEqual((totals['requested'], totals['attempted'], totals['committed'],
                                  totals['unattempted']), (32, 0, 0, 32))
                self.assertFalse(h.runtime_diagnostics[0]['summary']['collection_complete'])
                self.assertIn('OSError', h.runtime_diagnostic_errors)

    def test_command_pre_and_postcommit_failures_keep_raw_identity_and_summary(self):
        for seq, stage in ((17, 'business_transaction'), (22, 'commit_timestamp_observation')):
            with self.subTest(stage=stage):
                h, probe = self.harness(), CommandProbe()
                original = RuntimeError(stage)
                state = {'stage': stage}

                def body(*args, **kwargs):
                    self.assertTrue(probe.active)
                    raise original

                h._execute_inventory_command_body = Mock(side_effect=body)
                lane = (seq // 4) % 4
                with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics', return_value=probe), \
                     self.assertRaises(RuntimeError) as raised:
                    h._execute_inventory_command(seq, 'steady', 'batch-1', state, {},
                                                 lane=lane, scheduled_at=123.5)
                self.assertIs(raised.exception, original)
                self.assertTrue(probe.closed)
                row = json.loads((h.evidence / 'command-diagnostics.jsonl').read_text())
                self.assertEqual((row['global_index'], row['business_lane'], row['kind']),
                    (seq, lane, ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')[seq % 4]))
                self.assertEqual(row['error_type'], 'RuntimeError')
                self.assertEqual(row['physical_commit']['wall_seconds'], .125)

    def test_command_diagnostic_io_error_keeps_business_exception_but_fails_success(self):
        for business_error in (None, RuntimeError('business original')):
            with self.subTest(business_error=business_error is not None):
                h = self.harness()
                diagnostic_error = OSError('command summary')
                probe = CommandProbe(summary_error=diagnostic_error)
                h._execute_inventory_command_body = Mock(
                    side_effect=business_error, return_value={'event_id': str(uuid4())})
                expected = business_error or diagnostic_error
                with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics', return_value=probe), \
                     self.assertRaises(type(expected)) as raised:
                    h._execute_inventory_command(20, 'steady', 'batch-1', {}, {},
                                                 lane=1, scheduled_at=123.5)
                self.assertIs(raised.exception, expected)
                self.assertTrue(probe.closed)
                self.assertIn('OSError', h.runtime_diagnostic_errors)

    def test_command_file_write_error_preserves_business_error_and_fails_success(self):
        for business_error in (None, RuntimeError('business original')):
            with self.subTest(business_error=business_error is not None):
                h, probe = self.harness(), CommandProbe()
                diagnostic_error = OSError('command log unavailable')
                h._execute_inventory_command_body = Mock(
                    side_effect=business_error, return_value={'event_id': str(uuid4())})
                expected = business_error or diagnostic_error
                with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics', return_value=probe), \
                     patch.object(Path, 'open', side_effect=diagnostic_error), \
                     self.assertRaises(type(expected)) as raised:
                    h._execute_inventory_command(20, 'steady', 'batch-1', {}, {},
                                                 lane=1, scheduled_at=123.5)
                self.assertIs(raised.exception, expected)
                self.assertTrue(probe.closed)
                self.assertIn('OSError', h.runtime_diagnostic_errors)

    def test_command_observer_close_error_cannot_mask_original_body_failure(self):
        h = self.harness()
        original = RuntimeError('body original')
        probe = CommandProbe(exit_error=OSError('SQL wrapper close'))
        h._execute_inventory_command_body = Mock(side_effect=original)
        with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics', return_value=probe), \
             self.assertRaises(RuntimeError) as raised:
            h._execute_inventory_command(20, 'steady', 'batch-1', {}, {},
                                         lane=1, scheduled_at=123.5)
        self.assertIs(raised.exception, original)
        self.assertTrue(probe.closed)
        self.assertIn('OSError', h.runtime_diagnostic_errors)
        row = json.loads((h.evidence / 'command-diagnostics.jsonl').read_text())
        self.assertEqual((row['global_index'], row['business_lane'], row['error_type']),
                         (20, 1, 'RuntimeError'))

    def test_incomplete_command_summary_or_changed_commit_wrapper_rejects_success(self):
        for boundary in ('incomplete_coverage', 'changed_commit_wrapper'):
            for business_error in (None, RuntimeError('body original')):
                with self.subTest(boundary=boundary, business_error=business_error is not None):
                    h = self.harness()
                    errors = ([{'stage': 'commit_restore', 'error_type': 'CommitWrapperChanged'}]
                              if boundary == 'changed_commit_wrapper' else [])
                    probe = CommandProbe(collection_complete=boundary != 'incomplete_coverage',
                                         diagnostic_errors=errors)
                    h._execute_inventory_command_body = Mock(side_effect=business_error,
                        return_value={'event_id': str(uuid4())})
                    with patch('benchmarks.events.runtime_diagnostics.CommandDiagnostics', return_value=probe), \
                         self.assertRaises(RuntimeError) as raised:
                        h._execute_inventory_command(20, 'steady', 'batch-1', {}, {},
                                                     lane=1, scheduled_at=123.5)
                    h._execute_inventory_command_body.assert_called_once()
                    if business_error is not None:
                        self.assertIs(raised.exception, business_error)
                    row = json.loads((h.evidence / 'command-diagnostics.jsonl').read_text())
                    self.assertEqual((row['global_index'], row['business_lane']), (20, 1))
                    if boundary == 'incomplete_coverage':
                        self.assertFalse(row['collection_complete'])
                        self.assertIn('IncompleteCommandDiagnostics', h.runtime_diagnostic_errors)
                    else:
                        self.assertEqual(row['diagnostic_errors'], errors)
                        self.assertIn('CommitWrapperChanged', h.runtime_diagnostic_errors)

    def test_final_report_retains_summary_errors_without_copying_raw_samples_or_passing(self):
        h = self.harness()
        event = str(uuid4())
        batch = h.generation.begin_batch(1, 50, 'steady')
        attempt = h.generation.attempt(batch)
        h.generation.commit(batch, attempt, str(uuid4()))
        h.generation.identify_event(batch, attempt, event)
        h.generation.finish_success(batch)
        h.events = [{'event_id': event, 'scenario': 'steady'}]
        h.runtime_diagnostics = [{'scenario': 'steady', 'artifact': 'runtime-diagnostics-001.json',
            'elapsed_seconds': 19, 'summary': {'collection_complete': True,
                                             'lifecycle_complete': True, 'sample_count': 2}}]
        h.generation_topologies = [{'scenario': 'steady', 'passed': True}]
        h.children, h.logs, h.shutdowns, h.supervisor_restarts = [], [], [], []
        h.workers, h.cases, h.delivery_proofs = {}, [], []
        h.started_at = 0
        h.models = Mock()
        h.models.FailedDelivery.objects.order_by.return_value.values.return_value = []
        (h.evidence / 'errors.jsonl').touch()
        h.final_inventory_evidence = Mock(return_value={
            'database_observed': True, 'offsets_observed': True, 'errors': [],
            'actual_inventory_outbox_count': 1, 'unpublished_count': 0,
            'consumers': {role: {'incomplete_count': 0} for role in ('notification', 'analytics')},
            'event_log_ids_missing_from_database': [], 'actual_committed_ids_missing_from_event_log': [],
            'journal_committed_movements_missing_from_database': [],
            'database_committed_movements_missing_from_journal': [],
            'journal_identified_events_missing_from_database': [], 'processed_hash_conflicts': [],
            'reconciliation': {'mismatches': [], 'dedupe_count': 2,
                'notification_count': 0, 'expected_notification_count': 0}})
        with patch('builtins.print'):
            passing = h.finish()
        self.assertTrue(passing['runtime_diagnostics_complete'])
        self.assertTrue(passing['passed'], 'Establish a passing baseline before injecting the diagnostic error.')
        fault = {'scenario': 'one_broker_stop', 'artifact': 'runtime-diagnostics-002.json',
            'summary': {'collection_complete': False, 'lifecycle_complete': True,
                        'sample_count': 1, 'expected_resource_roles': list(DEFAULT_SERVICES),
                        'observed_cpu_memory_roles': [role for role in DEFAULT_SERVICES if role != 'redpanda-0']}}
        h.runtime_diagnostics.append(fault)
        with patch('builtins.print'):
            incomplete_coverage = h.finish()
        self.assertTrue(incomplete_coverage['runtime_diagnostics_complete'])
        self.assertTrue(incomplete_coverage['passed'])
        self.assertFalse(incomplete_coverage['runtime_diagnostics'][1]['summary']['collection_complete'])
        fault['summary']['lifecycle_complete'] = False
        with patch('builtins.print'):
            incomplete_lifecycle = h.finish()
        self.assertFalse(incomplete_lifecycle['runtime_diagnostics_complete'])
        self.assertFalse(incomplete_lifecycle['passed'])
        fault['summary']['lifecycle_complete'] = True
        h.runtime_diagnostics[0]['summary']['collection_complete'] = False
        with patch('builtins.print'):
            incomplete_steady = h.finish()
        self.assertFalse(incomplete_steady['runtime_diagnostics_complete'])
        self.assertFalse(incomplete_steady['passed'])
        h.runtime_diagnostics[0]['summary']['collection_complete'] = True
        h.runtime_diagnostic_errors = ['OSError']
        with patch('builtins.print'):
            report = h.finish()
        self.assertTrue(report['generation_accounting_complete'])
        self.assertTrue(report['generation_topologies_complete'])
        self.assertEqual(report['runtime_diagnostics'], h.runtime_diagnostics)
        self.assertEqual(report['runtime_diagnostic_error_types'], ['OSError'])
        self.assertFalse(report['runtime_diagnostics_complete'])
        self.assertFalse(report['passed'])
        self.assertNotIn('samples', json.dumps(report['runtime_diagnostics']))
        retained = json.loads((h.evidence / 'report.json').read_text())
        self.assertFalse(retained['passed'])
        self.assertEqual(retained['runtime_diagnostic_error_types'], ['OSError'])

        # Unrequested collection has no successful-completeness claim and does
        # not replace the existing business/journal/reconciliation gates.
        h.runtime_diagnostics_enabled = False
        h.runtime_diagnostics, h.runtime_diagnostic_errors = [], []
        with patch('builtins.print'):
            disabled = h.finish()
        self.assertTrue(disabled['passed'])
        self.assertFalse(disabled['runtime_diagnostics_enabled'])
        self.assertFalse(disabled['runtime_diagnostics_applicable'])
        self.assertEqual(disabled['runtime_diagnostics_status'], 'NOT_REQUESTED')
        self.assertIsNone(disabled['runtime_diagnostics_complete'])
        self.assertEqual(disabled['runtime_diagnostics'], [])
        h.final_inventory_evidence.return_value['reconciliation']['mismatches'] = ['original business mismatch']
        with patch('builtins.print'):
            broken_business = h.finish()
        self.assertFalse(broken_business['passed'])
