"""Resource-free profiler privacy, owner/exception and acceptance controls."""
from contextlib import nullcontext
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import threading
import time
from types import SimpleNamespace
from unittest import skipUnless
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from benchmarks.events.diagnostic_profile import CPUProfile, request_profile, sanitized_stats
from benchmarks.events.acceptance import Harness, freeze_generation_execution_profile, main
from benchmarks.events.generation_journal import numeric_profile
from labops.tests import test_harness_generation_accounting as accounting

args = accounting.args


def classic_engine_available():
    from cProfile import Profile
    if sys.getprofile() is not None:
        return False
    engine = Profile(timer=time.thread_time_ns, timeunit=1e-9)
    try:
        engine.enable()
        return sys.getprofile() is engine
    finally:
        engine.disable()


CLASSIC_ENGINE_AVAILABLE = classic_engine_available()


class OwnedProfilerFixture:
    """Deterministic control fixture with a real owned thread callback binding."""
    def __init__(self, **kwargs):
        self.arguments = kwargs
        self.enabled = self.disabled = 0
        self.stats = {}

    def enable(self):
        self.enabled += 1
        sys.setprofile(self)

    def disable(self):
        self.disabled += 1
        if sys.getprofile() is self:
            sys.setprofile(None)

    def __call__(self, *values):
        pass

    def create_stats(self):
        self.disable()
        self.stats = {('~', 0, 'fixture'): (1, 1, 0, 0, {})}


class CPUProfileControls(SimpleTestCase):
    def temporary(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def test_real_own_thread_cpu_timer_excludes_sleep_with_separate_wall_clock(self):
        profile = CPUProfile('generator', lane=0)
        with profile.call('generator_command', 12):
            time.sleep(.03)
        result = profile.close(self.temporary() / 'profile.json')
        self.assertEqual(result['complete'], CLASSIC_ENGINE_AVAILABLE)
        self.assertEqual(result['profiled_calls'], int(CLASSIC_ENGINE_AVAILABLE))
        row = profile.phases[0]
        self.assertGreater(row['wall_ns'], 25_000_000)
        self.assertLess(row['thread_cpu_ns'], row['wall_ns'] / 2)
        self.assertIsNone(sys.getprofile())

    @skipUnless(CLASSIC_ENGINE_AVAILABLE, 'Runtime cProfile has no owned classic thread callback')
    def test_factory_uses_integer_current_thread_cpu_timer_and_one_profiler(self):
        from cProfile import Profile
        factory = Mock(side_effect=Profile)
        profile = CPUProfile('generator', lane=1, profiler_factory=factory)
        for ordinal in (1, 2):
            with profile.call('generator_command', ordinal):
                sum(range(10))
        self.assertEqual(factory.call_count, 1)
        self.assertIs(factory.call_args.kwargs['timer'], time.thread_time_ns)
        self.assertEqual(factory.call_args.kwargs['timeunit'], 1e-9)
        value = profile.close(self.temporary() / 'profile.json')
        self.assertEqual(value['profiled_calls'], 2)

    def test_existing_thread_hook_is_not_overwritten_and_work_still_executes(self):
        def existing(frame, event, value):
            pass
        previous = sys.getprofile()
        sys.setprofile(existing)
        try:
            factory = Mock()
            profile = CPUProfile('generator', lane=0, profiler_factory=factory)
            factory.assert_not_called()
            with profile.call('generator_command', 0):
                returned = 7
            self.assertEqual(returned, 7)
            self.assertIs(sys.getprofile(), existing)
            path = self.temporary() / 'profile.json'
            result = profile.close(path)
            self.assertFalse(result['complete'])
            self.assertEqual(profile.graph_unavailable, True)
            self.assertEqual(result['function_graph_unavailable'], True)
            self.assertEqual(json.loads(path.read_text())['function_graph_status'], 'UNAVAILABLE')
        finally:
            sys.setprofile(previous)

    def test_normal_result_args_and_first_base_exception_survive_clock_and_export_errors(self):
        target = SimpleNamespace(call=Mock(return_value=17))
        original = target.call
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.hook(target, 'call', 'receipt_create', expected=original)
        with profile.call('generator_command', 4):
            self.assertEqual(target.call('PRIVATE', option='SECRET'), 17)
        original.assert_called_once_with('PRIVATE', option='SECRET')
        body = KeyboardInterrupt('PRIVATE BODY')
        with patch('benchmarks.events.diagnostic_profile.time.perf_counter_ns', side_effect=SystemExit('PRIVATE CLOCK')):
            with self.assertRaises(KeyboardInterrupt) as raised:
                with profile.call('generator_command', 5):
                    raise body
        self.assertIs(raised.exception, body)
        with patch('benchmarks.events.diagnostic_profile.sanitized_stats', side_effect=OSError('PRIVATE IO')):
            value = profile.close(self.temporary() / 'profile.json')
        self.assertFalse(value['complete'])
        self.assertIs(target.call, original)
        self.assertNotIn('PRIVATE', json.dumps(value))
        self.assertNotIn('SECRET', json.dumps(value))

    def test_hooks_restore_exact_instance_descriptor_or_prior_instance_attribute(self):
        class Target:
            def call(self, value):
                return value
        target = Target()
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.hook(target, 'call', 'provenance_journal', expected=target.call)
        self.assertIn('call', target.__dict__)
        with profile.call('generator_command', 4):
            self.assertEqual(target.call(9), 9)
        profile.close(self.temporary() / 'profile.json')
        self.assertNotIn('call', target.__dict__)
        prior = lambda value: value + 1
        target.call = prior
        second = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        second.hook(target, 'call', 'provenance_journal', expected=prior)
        second.close(self.temporary() / 'second.json')
        self.assertIs(target.call, prior)

    def test_cross_thread_wrapper_delegates_original_without_phase_or_cpu_attribution(self):
        calls = []
        target = SimpleNamespace(call=lambda value: calls.append(value))
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.hook(target, 'call', 'receipt_create', expected=target.call)
        with profile.call('generator_command', 0):
            other = threading.Thread(target=target.call, args=(2,))
            other.start(); other.join()
            target.call(1)
        profile.close(self.temporary() / 'profile.json')
        self.assertEqual(calls, [2, 1])
        self.assertEqual(sum(row['phase'] == 'receipt_create' for row in profile.phases), 1)

    def test_full_sanitized_function_and_caller_graph_has_no_unknown_absolute_paths_or_names(self):
        one = ('/private/SECRET_DSN/home/private.py', 17, 'SECRET_TOKEN')
        two = ('/another/SECRET_DSN/private.py', 17, 'SECRET_TOKEN')
        stats = {one: (1, 1, .1, .2, {two: (1, 1, .1, .2)}), two: (1, 1, .1, .1, {})}
        result = sanitized_stats(stats)
        serialized = json.dumps(result)
        self.assertNotIn('SECRET', serialized)
        self.assertNotIn('/private', serialized)
        self.assertEqual(len(result['functions']), 2)
        ids = {row['id'] for row in result['function_identities']}
        self.assertEqual(len(ids), 2)
        self.assertIn(result['functions'][0]['id'], ids)
        self.assertEqual(sum(len(row['callers']) for row in result['functions']), 1)

    @skipUnless(CLASSIC_ENGINE_AVAILABLE, 'Runtime cProfile has no owned classic thread callback')
    def test_export_is_json_only_exclusive_and_unsafe_statistics_fail_incomplete(self):
        directory = self.temporary()
        profile = CPUProfile('generator', lane=3)
        with profile.call('generator_command', 15):
            sum(range(5))
        path = directory / 'diagnostic-profile.json'
        self.assertTrue(profile.close(path)['complete'])
        self.assertEqual({row.suffix for row in directory.iterdir()}, {'.json'})
        value = json.loads(path.read_text())
        self.assertFalse(value['qualification_admissible'])
        self.assertEqual(value['calls'][0]['ordinal'], 15)
        self.assertEqual(value['timer'], 'thread_time_ns')
        self.assertTrue(value['functions'])
        self.assertEqual(value['coverage']['profiled_calls'], 1)
        original_bytes = path.read_bytes()
        second = CPUProfile('generator', lane=3)
        with second.call('generator_command', 19):
            pass
        self.assertFalse(second.close(path)['complete'])
        self.assertEqual(path.read_bytes(), original_bytes)
        with self.assertRaises(ValueError):
            sanitized_stats({('~', 0, 'unsafe'): (1, 1, float('nan'), 0, {})})

    def test_partial_enable_callback_is_removed_before_work_and_original_error_survives(self):
        class PartialProfiler:
            def __init__(self, **options):
                pass
            def __call__(self, *values):
                pass
            def enable(self):
                sys.setprofile(self)
                raise SystemExit('PRIVATE enable')
            def disable(self):
                raise OSError('PRIVATE disable')
        profile = CPUProfile('generator', lane=0, profiler_factory=PartialProfiler)
        original = KeyboardInterrupt('PRIVATE work')
        with self.assertRaises(KeyboardInterrupt) as raised:
            with profile.call('generator_command', 0):
                self.assertIsNone(sys.getprofile())
                raise original
        self.assertIs(raised.exception, original)
        self.assertIsNone(sys.getprofile())
        self.assertFalse(profile.calls[0]['profiled'])
        self.assertNotIn('PRIVATE', json.dumps(profile.summary()))

    def test_restoration_is_idempotent_after_partial_admission_cleanup(self):
        target = SimpleNamespace(call=lambda: 1)
        original = target.call
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.hook(target, 'call', 'receipt_create', expected=original)
        profile.restore()
        profile.close(self.temporary() / 'profile.json')
        self.assertIs(target.call, original)
        self.assertFalse(any(row['stage'] == 'hook_restore' for row in profile.errors))

    def test_failing_call_and_error_record_sinks_cannot_replace_primary_base_exception(self):
        class BrokenList(list):
            def append(self, value):
                raise SystemExit('PRIVATE recording')
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.calls, profile.errors = BrokenList(), BrokenList()
        original = KeyboardInterrupt('PRIVATE body')
        with self.assertRaises(KeyboardInterrupt) as raised:
            with profile.call('generator_command', 0):
                raise original
        self.assertIs(raised.exception, original)
        self.assertIsNone(sys.getprofile())
        self.assertTrue(profile.recording_failed)
        self.assertFalse(profile.close(self.temporary() / 'profile.json')['complete'])

    def test_nonexistent_authored_root_and_private_nonsource_paths_remain_opaque(self):
        from benchmarks.events.diagnostic_profile import ROOT
        keys = [(str(ROOT / 'labops' / 'SECRET_TOKEN_MISSING.py'), 1, 'private_function'),
                (str(ROOT / 'infra/events/validation/generated/secrets.json'), 1, 'PRIVATE_DSN')]
        with patch('pathlib.Path.read_bytes', side_effect=AssertionError('No private file reads')):
            value = sanitized_stats({key: (1, 1, 0, 0, {}) for key in keys})
        self.assertNotIn('SECRET', json.dumps(value))
        self.assertNotIn('PRIVATE', json.dumps(value))
        self.assertEqual(value['repository_source_sha256'], {})
        self.assertTrue(all(row['source'].startswith('unknown-sha256:') for row in value['functions']))

    def test_graph_failures_persist_independent_phases_without_unsafe_graph_data(self):
        private = ('/PRIVATE_DSN/SECRET_TOKEN.py', 1, 'PRIVATE_FUNCTION')
        cases = [
            ('stats', OSError('PRIVATE stats export'), None, 'OSError'),
            ('negative', None, {private: (1, 1, -.1, .2, {})}, 'ValueError'),
            ('nonfinite', None, {private: (1, 1, float('nan'), .2, {})}, 'ValueError'),
            ('caller_schema', None, {private: (1, 1, .1, .2, {private: (1, 1, .1)})}, 'ValueError'),
        ]
        for name, failure, stats, error_type in cases:
            with self.subTest(name=name):
                profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
                with profile.call('generator_command', 7):
                    sum(range(10))
                path = self.temporary() / 'diagnostic-profile.json'
                with patch('pstats.Stats', side_effect=failure,
                           return_value=SimpleNamespace(stats=stats)):
                    coverage = profile.close(path)
                value = json.loads(path.read_text())
                self.assertTrue(coverage['persisted'])
                self.assertFalse(coverage['complete'])
                self.assertTrue(coverage['function_graph_unavailable'])
                self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')
                self.assertEqual(value['coverage'], coverage)
                self.assertEqual(value['functions'], [])
                self.assertEqual(value['function_identities'], [])
                self.assertEqual(value['repository_source_sha256'], {})
                self.assertEqual(value['calls'][0]['ordinal'], 7)
                self.assertEqual(value['phases'][0]['phase'], 'generator_command')
                self.assertGreaterEqual(value['phases'][0]['thread_cpu_ns'], 0)
                self.assertEqual(coverage['errors'], [{'stage': 'export', 'error_type': error_type}])
                self.assertNotIn('PRIVATE', path.read_text())
                self.assertNotIn('SECRET', path.read_text())

    def test_graph_unavailable_flag_precedes_failed_error_recording_and_stays_incomplete(self):
        class BrokenErrors(list):
            def append(self, value):
                raise OSError('PRIVATE error sink')
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.errors = BrokenErrors()
        with profile.call('generator_command', 0):
            sum(range(5))
        path = self.temporary() / 'diagnostic-profile.json'
        with patch('pstats.Stats', side_effect=ValueError('PRIVATE invalid statistic')):
            coverage = profile.close(path)
        value = json.loads(path.read_text())
        self.assertEqual(coverage['errors'], [])
        self.assertTrue(coverage['recording_failed'])
        self.assertTrue(coverage['function_graph_unavailable'])
        self.assertTrue(coverage['persisted'])
        self.assertFalse(coverage['complete'])
        self.assertEqual(value['coverage'], coverage)
        self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')
        self.assertNotIn('PRIVATE', path.read_text())

    def test_graph_fallback_persists_when_error_method_itself_raises(self):
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        with profile.call('generator_command', 0):
            sum(range(5))
        path = self.temporary() / 'diagnostic-profile.json'
        with patch('pstats.Stats', side_effect=ValueError('PRIVATE invalid statistic')), \
             patch.object(profile, 'error', side_effect=SystemExit('PRIVATE error method')):
            coverage = profile.close(path)
        value = json.loads(path.read_text())
        self.assertEqual(coverage['errors'], [])
        self.assertTrue(coverage['recording_failed'])
        self.assertTrue(coverage['function_graph_unavailable'])
        self.assertTrue(coverage['persisted'])
        self.assertFalse(coverage['complete'])
        self.assertEqual(value['coverage'], coverage)
        self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')
        self.assertNotIn('PRIVATE', path.read_text())

    def test_unbound_engine_is_disabled_before_business_and_never_exports_a_claimed_graph(self):
        class UnboundProfiler:
            def __init__(self, **options):
                self.enabled = False
                self.disabled = 0
            def enable(self):
                self.enabled = True
            def disable(self):
                self.enabled = False
                self.disabled += 1
        for failure in (None, KeyboardInterrupt('PRIVATE primary failure')):
            with self.subTest(failure=failure is not None):
                profile = CPUProfile('generator', lane=0, profiler_factory=UnboundProfiler)
                calls = []
                def business():
                    self.assertFalse(profile.profiler.enabled)
                    self.assertIsNone(sys.getprofile())
                    calls.append(1)
                    if failure is not None:
                        raise failure
                    return 17
                if failure is None:
                    with profile.call('generator_command', 0):
                        self.assertEqual(business(), 17)
                else:
                    with patch.object(profile, 'error', side_effect=SystemExit('PRIVATE error sink')):
                        with self.assertRaises(KeyboardInterrupt) as raised:
                            with profile.call('generator_command', 0):
                                business()
                        self.assertIs(raised.exception, failure)
                path = self.temporary() / 'diagnostic-profile.json'
                with patch('pstats.Stats', side_effect=AssertionError('Unsupported graph was read')) as stats:
                    coverage = profile.close(path)
                stats.assert_not_called()
                value = json.loads(path.read_text())
                self.assertEqual(calls, [1])
                self.assertEqual(profile.profiler.disabled, 1)
                self.assertFalse(coverage['complete'])
                self.assertTrue(coverage['persisted'])
                self.assertEqual(coverage['profiled_calls'], 0)
                self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')
                self.assertEqual(value['functions'], [])
                self.assertEqual(len(value['phases']), 1)
                self.assertEqual(value['phases'][0]['phase'], 'generator_command')
                self.assertEqual(value['phases'][0]['ordinal'], 0)
                self.assertEqual(value['phases'][0]['outcome'], 'error' if failure else 'returned')
                self.assertTrue(value['phases'][0]['complete'])
                self.assertGreaterEqual(value['phases'][0]['wall_ns'], 0)
                self.assertGreaterEqual(value['phases'][0]['thread_cpu_ns'], 0)
                self.assertFalse(profile.recording_active)
                if failure is None:
                    self.assertEqual(coverage['errors'], [
                        {'stage': 'enable', 'error_type': 'UnsupportedProfileEngine'}])
                else:
                    self.assertTrue(coverage['recording_failed'])
                self.assertNotIn('PRIVATE', path.read_text())

    def test_changed_hook_and_failed_error_sink_never_claim_restored_or_replace_primary_failure(self):
        target = SimpleNamespace(call=lambda: 1)
        replacement = lambda: 2
        profile = CPUProfile('generator', lane=0, profiler_factory=OwnedProfilerFixture)
        profile.hook(target, 'call', 'receipt_create', expected=target.call)
        original = KeyboardInterrupt('PRIVATE business failure')
        with patch.object(profile, 'error', side_effect=OSError('PRIVATE error sink')):
            with self.assertRaises(KeyboardInterrupt) as raised:
                with profile.call('generator_command', 0):
                    target.call = replacement
                    raise original
            path = self.temporary() / 'diagnostic-profile.json'
            coverage = profile.close(path)
        self.assertIs(raised.exception, original)
        self.assertIs(target.call, replacement)
        self.assertFalse(coverage['hooks_restored'])
        self.assertFalse(coverage['complete'])
        self.assertTrue(coverage['recording_failed'])
        self.assertTrue(coverage['persisted'])
        self.assertFalse(json.loads(path.read_text())['coverage']['hooks_restored'])
        profile.restore()
        self.assertFalse(profile.hooks_restored)

    def test_failed_or_none_factory_keeps_graph_unavailable_with_normal_business_return(self):
        cases = [('failed', Mock(side_effect=OSError('PRIVATE admission failure'))),
                 ('none', Mock(return_value=None))]
        for name, factory in cases:
            with self.subTest(name=name), \
                 patch.object(CPUProfile, 'error', side_effect=SystemExit('PRIVATE recording failure')):
                profile = CPUProfile('generator', lane=0, profiler_factory=factory)
                calls = []
                with profile.call('generator_command', 0):
                    calls.append(17)
                path = self.temporary() / 'diagnostic-profile.json'
                with patch('pstats.Stats', side_effect=AssertionError('Unavailable graph was read')) as stats:
                    coverage = profile.close(path)
                stats.assert_not_called()
                value = json.loads(path.read_text())
                self.assertEqual(calls, [17])
                self.assertTrue(coverage['function_graph_unavailable'])
                self.assertTrue(coverage['recording_failed'])
                self.assertFalse(coverage['complete'])
                self.assertTrue(coverage['persisted'])
                self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')
                self.assertEqual(value['functions'], [])
                self.assertEqual(len(value['phases']), 1)
                self.assertEqual(value['phases'][0]['ordinal'], 0)
                self.assertNotIn('PRIVATE', path.read_text())

    def test_actual_runtime_engine_keeps_business_once_and_reports_its_real_capability(self):
        profile = CPUProfile('generator', lane=0)
        calls = []
        with profile.call('generator_command', 0):
            calls.append(17)
        path = self.temporary() / 'diagnostic-profile.json'
        coverage = profile.close(path)
        value = json.loads(path.read_text())
        self.assertEqual(calls, [17])
        self.assertIsNone(sys.getprofile())
        self.assertTrue(coverage['persisted'])
        self.assertEqual(len(value['phases']), 1)
        self.assertEqual(value['phases'][0]['phase'], 'generator_command')
        self.assertTrue(value['phases'][0]['complete'])
        self.assertGreaterEqual(value['phases'][0]['wall_ns'], 0)
        self.assertGreaterEqual(value['phases'][0]['thread_cpu_ns'], 0)
        if CLASSIC_ENGINE_AVAILABLE:
            self.assertTrue(coverage['complete'])
            self.assertEqual(coverage['profiled_calls'], 1)
            self.assertTrue(value['functions'])
            self.assertEqual(value['function_graph_status'], 'COMPLETE')
        else:
            self.assertFalse(coverage['complete'])
            self.assertEqual(coverage['profiled_calls'], 0)
            self.assertEqual(coverage['errors'], [{'stage': 'enable', 'error_type': 'UnsupportedProfileEngine'}])
            self.assertEqual(value['functions'], [])
            self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')

    def test_unavailable_graph_keeps_nested_owner_phases_without_cross_thread_or_new_admission(self):
        factory = Mock(return_value=None)
        profile = CPUProfile('generator', lane=0, profiler_factory=factory)
        calls = []
        target = SimpleNamespace(call=lambda value: calls.append(value))
        profile.hook(target, 'call', 'receipt_create', expected=target.call, ordinal=True)
        with profile.call('generator_command', 7):
            worker = threading.Thread(target=target.call, args=(2,))
            worker.start()
            worker.join(timeout=2)
            self.assertFalse(worker.is_alive())
            with profile.call('receipt_post', 999):
                target.call(1)
        path = self.temporary() / 'diagnostic-profile.json'
        coverage = profile.close(path)
        value = json.loads(path.read_text())
        self.assertEqual(calls, [2, 1])
        self.assertEqual(factory.call_count, 1)
        self.assertEqual(coverage['requested_calls'], 1)
        self.assertEqual(coverage['profiled_calls'], 0)
        self.assertFalse(coverage['complete'])
        self.assertFalse(value['qualification_admissible'])
        self.assertEqual(Counter(row['phase'] for row in value['phases']), Counter({
            'generator_command': 1, 'receipt_post': 1, 'receipt_create': 1}))
        self.assertEqual(next(row['ordinal'] for row in value['phases'] if row['phase'] == 'receipt_create'), 1)
        self.assertEqual(next(row['ordinal'] for row in value['phases'] if row['phase'] == 'receipt_post'), 7)
        self.assertEqual(value['calls'][0]['ordinal'], 7)
        self.assertTrue(all(row['complete'] and row['wall_ns'] >= 0 and row['thread_cpu_ns'] >= 0
                            for row in value['phases']))


class DiagnosticProfileHarnessControls(SimpleTestCase):
    def harness(self):
        return accounting.HarnessGenerationAccountingTests.harness(self)

    def test_independent_boolean_frozen_before_setup_and_invalid_nonboolean_rejected(self):
        with TemporaryDirectory() as name:
            directory = Path(name)
            frozen_args = args(directory, events=512, diagnostic_profile=True, runtime_diagnostics=False)
            self.assertTrue(numeric_profile(frozen_args)['diagnostic_profile_enabled'])
            freeze_generation_execution_profile(directory, frozen_args.run_id,
                diagnostic_profile=True, runtime_diagnostics=False)
            value = json.loads((directory / 'generation-execution-profile.json').read_text())
            self.assertFalse(value['qualification_admissible'])
            self.assertTrue(value['diagnostic_profile']['enabled'])
            self.assertFalse(value['runtime_diagnostics']['enabled'])
        with self.assertRaises(ValueError):
            request_profile('true')
        with self.assertRaises(ValueError):
            numeric_profile(args(Path('/tmp'), diagnostic_profile=1))

    def test_off_publisher_argv_and_coverage_have_no_profile_factory_or_profile_io(self):
        h = self.harness()
        h.spawn = Mock(return_value=Mock())
        h.sync_metrics_targets = Mock()
        with patch('benchmarks.events.diagnostic_profile.CPUProfile') as factory, \
             patch('pathlib.Path.read_text', side_effect=AssertionError('Profile I/O when OFF')), \
             patch('pathlib.Path.open', side_effect=AssertionError('Profile I/O when OFF')):
            h.start_publisher()
            result = h.diagnostic_profile_evidence()
        factory.assert_not_called()
        self.assertEqual(h.spawn.call_args.args[1], ['manage.py', 'publish_events', '--loop', '--limit', '500'])
        self.assertEqual(result['status'], 'NOT_REQUESTED')
        self.assertIsNone(result['complete'])
        self.assertTrue(result['qualification_admissible'])

    def test_enabled_publisher_bootstrap_and_missing_files_are_incomplete(self):
        h = self.harness()
        h.diagnostic_profile_enabled = True
        h.publisher_profile_paths = []
        h.spawn, h.sync_metrics_targets = Mock(), Mock()
        h.start_publisher()
        self.assertTrue(h.spawn.call_args.args[1][0].endswith('profile_publisher.py'))
        self.assertNotIn('manage.py', h.spawn.call_args.args[1])
        result = h.diagnostic_profile_evidence()
        self.assertFalse(result['qualification_admissible'])
        self.assertFalse(result['complete'])
        self.assertEqual(result['rows'][0]['error_type'], 'FileNotFoundError')

    def test_startup_failure_retains_requested512_and_inadmissibility_before_environment(self):
        with TemporaryDirectory() as name:
            directory = Path(name)
            argv = ['acceptance', '--run-id', 'profile-startup', '--events', '512', '--rate', '50',
                '--duration', '0', '--diagnostic-profile', '--evidence-dir', str(directory)]
            original = OSError('environment unavailable')
            with patch.object(sys, 'argv', argv), \
                 patch('benchmarks.events.acceptance.load_environment', side_effect=original):
                with self.assertRaises(OSError) as raised:
                    main()
            self.assertIs(raised.exception, original)
            value = json.loads((directory / 'startup-failure.json').read_text())
            self.assertFalse(value['qualification_admissible'])
            self.assertEqual(value['requested_numeric_profile']['events'], 512)
            self.assertTrue(value['requested_numeric_profile']['diagnostic_profile_enabled'])
            self.assertEqual(value['steady_command_denominator'], {'requested': 512,
                'attempted': 0, 'committed': 0, 'unattempted': 512,
                'scope': 'harness setup failed before generation'})

    def test_private_spawn_bootstrap_copies_the_independent_request_without_production_changes(self):
        with patch.object(accounting, 'args', side_effect=lambda directory: args(directory, diagnostic_profile=True)):
            h = self.harness()
        h.diagnostic_profile_enabled = True
        h.process_generation_enabled = True
        h._next_command_index = 0
        from uuid import uuid4
        h.admin = SimpleNamespace(id=uuid4())
        h.target = SimpleNamespace(id=uuid4())
        h.settings = SimpleNamespace(EVENT_TRANSPORT='kafka', KAFKA_TOPIC='inventory-test',
            KAFKA_SOURCE_CLUSTER_ID='test', KAFKA_SOURCE_STREAM_GENERATION='v1', EVENT_MAX_PAYLOAD_BYTES=262144)
        h.connection.settings_dict = {'ENGINE': 'django.db.backends.postgresql', 'HOST': 'localhost',
            'PORT': '1', 'NAME': 'test_database', 'USER': 'test', 'PASSWORD': 'PRIVATE_PASSWORD'}
        h.business_lanes = [{name: SimpleNamespace(id=uuid4()) for name in
            ('project', 'task', 'order', 'order_line')} for _ in range(4)]
        for lane in h.business_lanes:
            lane.update(batch=None, cycle_issue=None)
        h.generation_topologies, h.process_batches = [], []
        h.settle_process_sessions = Mock(return_value=True)
        h.merge_origin_events = Mock()
        h.process_database_facts = Mock()
        captured = {}
        original = RuntimeError('controlled driver failure')
        def driver(count, rate, factory, bootstrap, **options):
            captured.update(bootstrap)
            raise original
        with patch('benchmarks.events.process_generation.run_paced_processes', side_effect=driver):
            with self.assertRaises(RuntimeError) as raised:
                h._generate_processes(16, 'steady', rate=h.args.rate)
        self.assertIs(raised.exception, original)
        self.assertTrue(captured['diagnostic_profile_enabled'])
        self.assertFalse(captured['runtime_diagnostics_enabled'])
        self.assertEqual(captured['database_config']['PASSWORD'], 'PRIVATE_PASSWORD')
        self.assertEqual([len(plan['indices']) for plan in captured['plans']], [4, 4, 4, 4])
        self.assertEqual(captured['runtime_settings']['EVENT_TRANSPORT'], 'kafka')
        self.assertTrue(captured['profile']['diagnostic_profile_enabled'])
        for path in h.evidence.rglob('*.json'):
            self.assertNotIn('PRIVATE_PASSWORD', path.read_text())

    def test_final_admissibility_is_false_even_with_all_ordinary_checks_complete(self):
        h = self.harness()
        h.diagnostic_profile_enabled = True
        # Reuse the genuine final-accounting baseline with all ordinary
        # reconciliation predicates satisfied; profile admission is separate.
        h.final_inventory_evidence = Mock(return_value={
            'database_observed': True, 'offsets_observed': True, 'errors': [], 'unpublished_count': 0,
            'actual_inventory_outbox_count': 0, 'processed_hash_conflicts': [], 'consumers': {},
            'event_log_ids_missing_from_database': [], 'actual_committed_ids_missing_from_event_log': [],
            'journal_committed_movements_missing_from_database': [],
            'database_committed_movements_missing_from_journal': [],
            'journal_identified_events_missing_from_database': [],
            'reconciliation': {'mismatches': [], 'dedupe_count': 0,
                'notification_count': 0, 'expected_notification_count': 0}})
        h.diagnostic_profile_evidence = Mock(return_value={'enabled': True, 'complete': True,
            'status': 'COMPLETE', 'qualification_admissible': False})
        result = h.finish()
        self.assertTrue(result['final_inventory_complete'])
        self.assertFalse(result['qualification_admissible'])
        self.assertFalse(result['passed'])
        self.assertFalse(result['full_workload_targets_passed'])

    def test_writer_complete_requires_every_exact_frozen_ordinal_not_just_counts(self):
        h = self.harness()
        h.diagnostic_profile_enabled = True
        h.publisher_profile_paths = [h.evidence / 'publisher-profile-000.json']
        plans = []
        for lane in range(4):
            directory = h.evidence / f'lane-{lane}'
            directory.mkdir()
            indices = [lane * 4, lane * 4 + 1]
            plans.append({'directory': str(directory), 'lane': lane, 'indices': indices})
            value = {'qualification_admissible': False, 'calls': [{'ordinal': i} for i in indices],
                'coverage': {'role': 'generator', 'lane': lane, 'complete': True,
                    'requested_calls': 2, 'profiled_calls': 2}}
            (directory / 'diagnostic-profile.json').write_text(json.dumps(value))
        h.process_batches = [{'origin_plans': plans}]
        h.publisher_profile_paths[0].write_text(json.dumps({'qualification_admissible': False,
            'calls': [{'ordinal': None}], 'coverage': {'role': 'publisher', 'lane': None,
                'complete': True, 'requested_calls': 1, 'profiled_calls': 1}}))
        self.assertTrue(h.diagnostic_profile_evidence()['complete'])
        path = Path(plans[0]['directory']) / 'diagnostic-profile.json'
        for ordinals in ([0, 0], [0, 99], [0]):
            value = json.loads(path.read_text())
            value['calls'] = [{'ordinal': i} for i in ordinals]
            path.write_text(json.dumps(value))
            self.assertFalse(h.diagnostic_profile_evidence()['complete'])


from collections import Counter
from contextlib import ExitStack, contextmanager, redirect_stderr
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from benchmarks.events.diagnostic_profile import CPUProfile
from benchmarks.events.profile_publisher import ROOT, install_publisher_hooks, run_publisher
from labops import events as publisher_events, worker_metrics as publisher_metrics
from labops.management.commands import publish_events as publisher_command
from labops.publisher_shards import PublisherShardOwner


class PublisherPrimaryFailure(BaseException):
    pass


class PublisherProfilerFixture:
    def __init__(self, **kwargs):
        self.arguments = kwargs
        self.enabled = self.disabled = 0

    def enable(self):
        self.enabled += 1
        sys.setprofile(self)

    def disable(self):
        self.disabled += 1
        if sys.getprofile() is self:
            sys.setprofile(None)

    def __call__(self, *values):
        pass


class PublisherProfileControlTests(SimpleTestCase):
    def bindings(self):
        return [(publisher_command, 'publish_one'), (publisher_events, 'claim_event'),
                (publisher_events, 'envelope'), (publisher_events, 'owned_event'),
                (publisher_events, 'send'), (PublisherShardOwner, 'assert_owned'),
                (publisher_metrics.StopController, 'wait'),
                (publisher_command, 'database_statement_budget')]

    def profile(self, role):
        result = CPUProfile(role, profiler_factory=PublisherProfilerFixture)
        self.created_profile = result
        return result

    @contextmanager
    def deterministic_export(self, *, failure=None):
        with ExitStack() as stack:
            stack.enter_context(patch('benchmarks.events.diagnostic_profile.os.getpid', return_value=0))
            stack.enter_context(patch('benchmarks.events.diagnostic_profile.threading.get_native_id', return_value=0))
            stack.enter_context(patch('pstats.Stats', side_effect=failure,
                                      return_value=SimpleNamespace(stats={})))
            yield

    @contextmanager
    def callbacks(self):
        """Only temporary bindings change; no production source is rewritten.

        Mark the controlled originals as admission-compatible repository
        callbacks so tests can exercise real installation without DB work.
        The deterministic exporter records no artificial function graph.
        """
        probe = SimpleNamespace(calls=[], publish_result=object(), lease_result=object(),
                                budget_result=object(), send_error=None, budget_exits=[])
        def record(name, args, kwargs):
            probe.calls.append((name, args, dict(kwargs)))
        def claim(*args, **kwargs):
            record('claim_event', args, kwargs)
            return probe.event
        def envelope(*args, **kwargs):
            record('envelope', args, kwargs)
            return probe.envelope
        def lease(*args, **kwargs):
            record('owned_event', args, kwargs)
            return probe.lease_result
        def transmit(*args, **kwargs):
            record('send', args, kwargs)
            if probe.send_error is not None:
                raise probe.send_error
        def ownership(*args, **kwargs):
            record('assert_owned', args, kwargs)
        def idle(*args, **kwargs):
            record('wait', args, kwargs)
        def publish(*args, **kwargs):
            record('publish_one', args, kwargs)
            event = publisher_events.claim_event(shard_index=kwargs['shard_index'],
                                                 shard_count=kwargs['shard_count'])
            value = publisher_events.envelope(event)
            kwargs['ownership_check']()
            publisher_events.owned_event(event)
            publisher_events.send(args[0], 'fixture.inventory.v1', 'fixture-key', value)
            return probe.publish_result
        class Budget:
            def __enter__(self):
                record('budget_enter', (), {})
                return probe.budget_result
            def __exit__(self, *info):
                record('budget_exit', info, {})
                probe.budget_exits.append(info)
                return False
        def budget(*args, **kwargs):
            record('database_statement_budget', args, kwargs)
            return Budget()
        probe.event, probe.envelope = object(), {'fixture': True}
        originals = [(publisher_events, 'publish_one', publish),
                     (publisher_command, 'publish_one', publish),
                     (publisher_events, 'claim_event', claim), (publisher_events, 'envelope', envelope),
                     (publisher_events, 'owned_event', lease), (publisher_events, 'send', transmit),
                     (PublisherShardOwner, 'assert_owned', ownership),
                     (publisher_metrics.StopController, 'wait', idle),
                     (publisher_metrics, 'database_statement_budget', budget),
                     (publisher_command, 'database_statement_budget', budget)]
        for _target, _name, function in originals:
            function.__code__ = function.__code__.replace(co_filename=str(ROOT / 'labops' / 'events.py'))
        with ExitStack() as stack:
            for target, name, original in originals:
                stack.enter_context(patch.object(target, name, original))
            yield probe, originals

    def assert_restored(self, originals):
        for target, name, original in originals:
            self.assertIs(getattr(target, name), original, f'Publisher binding {name} was not restored exactly')

    def test_real_production_aliases_restore_after_injected_management_callback_return(self):
        originals = [(target, name, getattr(target, name)) for target, name in self.bindings()]
        event_publish = publisher_events.publish_one
        result = object()
        options = {'limit': 17, 'loop': False, 'metrics_port': None}
        calls = []
        def management(*args, **kwargs):
            calls.append((args, kwargs))
            self.assertIsNot(publisher_command.publish_one, originals[0][2])
            self.assertIs(publisher_events.publish_one, event_publish)
            return result
        with TemporaryDirectory() as directory, self.deterministic_export():
            output = Path(directory) / 'publisher-profile-fixture.json'
            observed = run_publisher(output, options, call_command=management, profile_factory=self.profile)
            coverage = json.loads(output.read_text())['coverage']
        self.assertIs(observed, result)
        self.assertEqual(calls, [(('publish_events',), options)])
        self.assert_restored(originals)
        self.assertTrue(coverage['complete'])
        self.assertTrue(self.created_profile.hooks_restored)

    def test_original_once_args_returns_and_all_fixed_publisher_phase_scopes_are_preserved(self):
        with self.callbacks() as (probe, originals), TemporaryDirectory() as directory, self.deterministic_export():
            producer, owner, stop, budget_token = object(), PublisherShardOwner.__new__(PublisherShardOwner), object(), object()
            expected_ownership = None
            def management(name, **options):
                nonlocal expected_ownership
                self.assertEqual(name, 'publish_events')
                expected_ownership = owner.assert_owned
                with publisher_command.database_statement_budget(2.5, fixture=budget_token) as value:
                    self.assertIs(value, probe.budget_result)
                    observed = publisher_command.publish_one(producer, shard_index=2, shard_count=4,
                                                             ownership_check=expected_ownership)
                publisher_metrics.StopController.wait(stop, .125)
                return observed
            output = Path(directory) / 'publisher-profile-fixture.json'
            result = run_publisher(output, {'limit': 1}, call_command=management, profile_factory=self.profile)
            document = json.loads(output.read_text())
            self.assert_restored(originals)
        self.assertIs(result, probe.publish_result)
        self.assertEqual(Counter(name for name, _args, _kwargs in probe.calls), Counter({
            'database_statement_budget': 1, 'budget_enter': 1, 'budget_exit': 1,
            'publish_one': 1, 'claim_event': 1, 'envelope': 1, 'owned_event': 1,
            'assert_owned': 1, 'send': 1, 'wait': 1}))
        calls = {name: (args, kwargs) for name, args, kwargs in probe.calls}
        self.assertEqual(calls['publish_one'], ((producer,), {'shard_index': 2, 'shard_count': 4,
                                                           'ownership_check': expected_ownership}))
        self.assertEqual(calls['database_statement_budget'], ((2.5,), {'fixture': budget_token}))
        self.assertEqual(calls['send'], ((producer, 'fixture.inventory.v1', 'fixture-key', probe.envelope), {}))
        self.assertEqual(calls['assert_owned'], ((owner,), {}))
        self.assertEqual(calls['wait'], ((stop, .125), {}))
        self.assertEqual(probe.budget_exits, [(None, None, None)])
        self.assertEqual(Counter(row['phase'] for row in document['phases']), Counter({
            'publisher_lifecycle': 1, 'publish_one_composite': 1, 'claim_commit_composite': 1,
            'envelope_composite': 1, 'lease_check': 1, 'send_composite': 1, 'shard_ownership': 1,
            'idle_wait': 1, 'budget_setup_composite': 1, 'budget_restore_composite': 1}))
        nested = {'publish_one_composite', 'claim_commit_composite', 'envelope_composite',
                  'lease_check', 'send_composite', 'shard_ownership'}
        self.assertTrue(all(row['ordinal'] == 1 for row in document['phases'] if row['phase'] in nested))
        self.assertTrue(document['coverage']['complete'])

    def test_first_base_exception_survives_export_failure_with_budget_exit_and_alias_restoration(self):
        original = PublisherPrimaryFailure('PRIVATE business exception text')
        with self.callbacks() as (probe, originals), TemporaryDirectory() as directory, \
             self.deterministic_export(failure=OSError('PRIVATE diagnostic export text')):
            probe.send_error = original
            owner = PublisherShardOwner.__new__(PublisherShardOwner)
            def management(*args, **kwargs):
                with publisher_command.database_statement_budget(2.5):
                    return publisher_command.publish_one(object(), shard_index=0, shard_count=1,
                                                         ownership_check=owner.assert_owned)
            with self.assertRaises(PublisherPrimaryFailure) as raised:
                run_publisher(Path(directory) / 'publisher-profile-fixture.json', {},
                              call_command=management, profile_factory=self.profile)
            self.assertIs(raised.exception, original)
            self.assert_restored(originals)
            self.assertIs(probe.budget_exits[0][1], original)
            self.assertIs(probe.budget_exits[0][0], PublisherPrimaryFailure)
            self.assertIsNotNone(probe.budget_exits[0][2])
        self.assertEqual(Counter(name for name, _args, _kwargs in probe.calls)['send'], 1)
        self.assertEqual(self.created_profile.errors, [{'stage': 'export', 'error_type': 'OSError'}])
        self.assertFalse(self.created_profile.summary()['complete'])
        self.assertTrue(self.created_profile.hooks_restored)
        self.assertNotIn('PRIVATE', json.dumps(self.created_profile.summary()))

    def test_cross_thread_hook_calls_original_once_without_phase_attribution(self):
        with self.callbacks() as (probe, originals), TemporaryDirectory() as directory, self.deterministic_export():
            result, failures = [], []
            argument = object()
            def invoke():
                try:
                    result.append(publisher_events.owned_event(argument, fixture=True))
                except BaseException as error:
                    failures.append(error)
            def management(*args, **kwargs):
                worker = threading.Thread(target=invoke)
                worker.start()
                worker.join(timeout=2)
                self.assertFalse(worker.is_alive())
                return 'management-return'
            output = Path(directory) / 'publisher-profile-fixture.json'
            self.assertEqual(run_publisher(output, {}, call_command=management,
                                          profile_factory=self.profile), 'management-return')
            document = json.loads(output.read_text())
            self.assert_restored(originals)
        self.assertEqual(failures, [])
        self.assertEqual(result, [probe.lease_result])
        self.assertEqual(probe.calls, [('owned_event', (argument,), {'fixture': True})])
        self.assertEqual([row['phase'] for row in document['phases']], ['publisher_lifecycle'])
        self.assertTrue(document['coverage']['complete'])

    def test_mismatched_import_alias_is_incomplete_and_never_silently_rebound(self):
        original = publisher_events.publish_one
        def unexpected(*args, **kwargs):
            return 'unexpected'
        calls = []
        with patch.object(publisher_command, 'publish_one', unexpected), \
             TemporaryDirectory() as directory, self.deterministic_export():
            def management(*args, **kwargs):
                calls.append((args, kwargs))
                self.assertIs(publisher_command.publish_one, unexpected)
                self.assertIs(publisher_events.publish_one, original)
                return 'normal-management-return'
            output = Path(directory) / 'publisher-profile-fixture.json'
            self.assertEqual(run_publisher(output, {'limit': 2}, call_command=management,
                                          profile_factory=self.profile), 'normal-management-return')
            self.assertIs(publisher_command.publish_one, unexpected)
            coverage = json.loads(output.read_text())['coverage']
        self.assertEqual(calls, [(('publish_events',), {'limit': 2})])
        self.assertEqual(coverage['errors'], [{'stage': 'publisher_hook_admission', 'error_type': 'RuntimeError'}])
        self.assertFalse(coverage['complete'])
        self.assertEqual(self.created_profile.hooks, [])

    def test_primary_management_base_exception_survives_close_and_error_recording_failures(self):
        original = KeyboardInterrupt('PRIVATE management')
        profile = CPUProfile('publisher', profiler_factory=PublisherProfilerFixture)
        def management(*args, **kwargs):
            raise original
        with patch('benchmarks.events.profile_publisher.install_publisher_hooks'), \
             patch.object(profile, 'close', side_effect=SystemExit('PRIVATE close')), \
             patch.object(profile, 'error', side_effect=OSError('PRIVATE error sink')), \
             patch('builtins.print', side_effect=OSError('PRIVATE stderr sink')):
            with self.assertRaises(KeyboardInterrupt) as raised:
                run_publisher(Path('/not-written/publisher-profile-000.json'), {},
                    call_command=management, profile_factory=lambda role: profile)
        self.assertIs(raised.exception, original)

    def test_failed_graph_export_keeps_return_and_hooks_with_safe_incomplete_stderr(self):
        originals = [(target, name, getattr(target, name)) for target, name in self.bindings()]
        sink = io.StringIO()
        result = object()
        with TemporaryDirectory() as directory, redirect_stderr(sink), \
             self.deterministic_export(failure=OSError('PRIVATE graph export')):
            output = Path(directory) / 'publisher-profile-fixture.json'
            observed = run_publisher(output, {}, call_command=lambda *a, **kw: result,
                                     profile_factory=self.profile)
            document = json.loads(output.read_text())
        self.assertIs(observed, result)
        self.assert_restored(originals)
        metadata = json.loads(sink.getvalue())
        self.assertEqual(metadata['kind'], 'publisher_diagnostic_profile')
        self.assertEqual(metadata['status'], 'INCOMPLETE')
        self.assertEqual(metadata['coverage'], document['coverage'])
        self.assertTrue(metadata['coverage']['persisted'])
        self.assertFalse(metadata['coverage']['complete'])
        self.assertEqual(document['function_graph_status'], 'UNAVAILABLE')
        self.assertEqual(document['functions'], [])
        self.assertEqual([row['phase'] for row in document['phases']], ['publisher_lifecycle'])
        self.assertNotIn('PRIVATE', sink.getvalue())
        self.assertNotIn(str(output), sink.getvalue())

    def test_first_business_failure_survives_graph_and_stderr_failures_with_restored_hooks(self):
        original = PublisherPrimaryFailure('PRIVATE business failure')
        with self.callbacks() as (probe, originals), TemporaryDirectory() as directory, \
             self.deterministic_export(failure=ValueError('PRIVATE graph failure')), \
             patch('builtins.print', side_effect=OSError('PRIVATE stderr failure')):
            probe.send_error = original
            owner = PublisherShardOwner.__new__(PublisherShardOwner)
            def management(*args, **kwargs):
                with publisher_command.database_statement_budget(2.5):
                    return publisher_command.publish_one(object(), shard_index=0, shard_count=1,
                                                         ownership_check=owner.assert_owned)
            output = Path(directory) / 'publisher-profile-fixture.json'
            with self.assertRaises(PublisherPrimaryFailure) as raised:
                run_publisher(output, {}, call_command=management, profile_factory=self.profile)
            coverage = json.loads(output.read_text())['coverage']
            self.assertIs(raised.exception, original)
            self.assert_restored(originals)
            self.assertIs(probe.budget_exits[0][1], original)
            self.assertFalse(coverage['complete'])
            self.assertTrue(coverage['persisted'])
            self.assertTrue(coverage['function_graph_unavailable'])

    def test_alias_admission_and_broken_error_sink_preserve_management_once_and_first_failure(self):
        event_publish = publisher_events.publish_one
        unexpected = lambda *args, **kwargs: 'unexpected'
        for failure in (None, PublisherPrimaryFailure('PRIVATE primary failure')):
            with self.subTest(failure=failure is not None), \
                 patch.object(publisher_command, 'publish_one', unexpected), \
                 TemporaryDirectory() as directory, self.deterministic_export(), redirect_stderr(io.StringIO()):
                originals = [(target, name, getattr(target, name)) for target, name in self.bindings()]
                calls = []
                returned = object()
                def factory(role):
                    profile = self.profile(role)
                    profile.error = Mock(side_effect=SystemExit('PRIVATE recording failure'))
                    return profile
                def management(*args, **kwargs):
                    calls.append((args, kwargs))
                    self.assertIs(publisher_command.publish_one, unexpected)
                    self.assertIs(publisher_events.publish_one, event_publish)
                    if failure is not None:
                        raise failure
                    return returned
                output = Path(directory) / 'publisher-profile-fixture.json'
                if failure is None:
                    self.assertIs(run_publisher(output, {'limit': 2}, call_command=management,
                                                profile_factory=factory), returned)
                else:
                    with self.assertRaises(PublisherPrimaryFailure) as raised:
                        run_publisher(output, {'limit': 2}, call_command=management, profile_factory=factory)
                    self.assertIs(raised.exception, failure)
                self.assertEqual(calls, [(('publish_events',), {'limit': 2})])
                self.assert_restored(originals)
                coverage = json.loads(output.read_text())['coverage']
                self.assertTrue(coverage['recording_failed'])
                self.assertTrue(coverage['hooks_restored'])
                self.assertTrue(coverage['persisted'])
                self.assertFalse(coverage['complete'])
                self.assertNotIn('PRIVATE', output.read_text())

    def test_unavailable_publisher_graph_retains_all_pinned_phases_and_publish_ordinals(self):
        class UnboundProfiler:
            def __init__(self, **options):
                self.enabled = False
            def enable(self):
                self.enabled = True
            def disable(self):
                self.enabled = False
        for factory in (UnboundProfiler, lambda **options: None):
            with self.subTest(constructor_failure=factory is not UnboundProfiler), \
                 self.callbacks() as (probe, originals), TemporaryDirectory() as directory, \
                 redirect_stderr(io.StringIO()):
                profiles = []
                def create_profile(role):
                    profile = CPUProfile(role, profiler_factory=factory)
                    profiles.append(profile)
                    return profile
                owner = PublisherShardOwner.__new__(PublisherShardOwner)
                def management(*args, **kwargs):
                    if profiles[0].profiler is not None:
                        self.assertFalse(profiles[0].profiler.enabled)
                    for _ in range(2):
                        with publisher_command.database_statement_budget(2.5):
                            self.assertIs(publisher_command.publish_one(object(), shard_index=0, shard_count=1,
                                ownership_check=owner.assert_owned), probe.publish_result)
                    publisher_metrics.StopController.wait(object(), .125)
                    return probe.publish_result
                output = Path(directory) / 'publisher-profile-fixture.json'
                with patch('pstats.Stats', side_effect=AssertionError('Unavailable graph was read')) as stats:
                    self.assertIs(run_publisher(output, {}, call_command=management,
                                              profile_factory=create_profile), probe.publish_result)
                stats.assert_not_called()
                self.assert_restored(originals)
                value = json.loads(output.read_text())
                self.assertEqual(value['function_graph_status'], 'UNAVAILABLE')
                self.assertEqual(value['functions'], [])
                self.assertFalse(value['coverage']['complete'])
                self.assertTrue(value['coverage']['persisted'])
                self.assertTrue(value['coverage']['hooks_restored'])
                self.assertEqual(value['coverage']['profiled_calls'], 0)
                self.assertEqual(value['coverage']['requested_calls'], 1)
                self.assertFalse(value['qualification_admissible'])
                self.assertEqual(Counter(row['phase'] for row in value['phases']), Counter({
                    'publisher_lifecycle': 1, 'publish_one_composite': 2, 'claim_commit_composite': 2,
                    'envelope_composite': 2, 'lease_check': 2, 'send_composite': 2,
                    'shard_ownership': 2, 'idle_wait': 1, 'budget_setup_composite': 2,
                    'budget_restore_composite': 2}))
                self.assertEqual([row['ordinal'] for row in value['phases']
                                  if row['phase'] == 'publish_one_composite'], [1, 2])
                self.assertEqual(Counter(name for name, _args, _kwargs in probe.calls)['send'], 2)
                self.assertTrue(all(row['complete'] and row['wall_ns'] >= 0 and row['thread_cpu_ns'] >= 0
                                    for row in value['phases']))
