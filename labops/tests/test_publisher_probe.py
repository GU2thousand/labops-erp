"""Finite publisher observations keep failed gates and never qualify capacity."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from benchmarks.events import publisher_probe as probe
from benchmarks.events.writer_topology import WRITER_TOPOLOGY_VERSION
from labops.tests import test_harness_generation_accounting as accounting


class PublisherProbeTests(SimpleTestCase):
    def temporary(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name)

    def argv(self, directory, *extra):
        return ['publisher_probe', '--run-id', 'finite-probe',
            '--evidence-dir', str(directory), *extra]

    def report(self, *, gate=False):
        return {'final_inventory_complete': True, 'owned_worker_cleanup_complete': True,
            'steady_inventory_inputs_committed': 4, 'cases': [{'name': 'steady', 'passed': gate}]}

    def fake_main_harness(self, directory, *, error=None, observation='complete'):
        h = SimpleNamespace(run=Mock(side_effect=error), finish=Mock(return_value=self.report()),
            metrics_samples=SimpleNamespace(errors=[], publisher_ack_count=4),
            probe_output=directory / 'publisher.json')
        if observation == 'complete':
            h.probe_output.write_text(json.dumps({'function_graph_status': 'UNAVAILABLE',
                'publisher_observation': {'complete': True, 'records': [{'private_internal': 'omitted'}]}}))
        elif observation == 'invalid':
            h.probe_output.write_text('{invalid-json')
        elif observation == 'nonobject':
            h.probe_output.write_text('[]')
        return h

    def call_main(self, directory, harness, *extra):
        with patch.object(sys, 'argv', self.argv(directory, *extra)), \
             patch.object(probe, 'load_environment') as loader, \
             patch.object(probe.subprocess, 'check_output', return_value='source-head\n'), \
             patch.object(probe, 'PublisherProbe', return_value=harness) as factory, \
             patch.dict(os.environ, {'POSTGRES_HOST': 'private-host'}, clear=False), \
             redirect_stdout(io.StringIO()):
            probe.main()
        return loader, factory

    def test_rejects_nonfinite_and_out_of_bounds_inputs_before_environment_or_harness(self):
        requests = [('--events', '3'), ('--events', '3001'), ('--rate', 'nan'),
            ('--rate', 'inf'), ('--rate', '0'), ('--rate', '50.001'),
            ('--duration', 'nan'), ('--duration', 'inf'), ('--duration', '-1'),
            ('--duration', '300.001'), ('--run-id', '../foreign'),
            ('--events', '3000', '--rate', '.01', '--duration', '0')]
        for request in requests:
            with self.subTest(request=request):
                directory = self.temporary() / 'evidence'
                with patch.object(sys, 'argv', self.argv(directory, *request)), \
                     patch.object(probe, 'load_environment') as loader, \
                     patch.object(probe, 'PublisherProbe') as factory, \
                     redirect_stdout(io.StringIO()), patch('sys.stderr', new=io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        probe.main()
                self.assertEqual(raised.exception.code, 2)
                loader.assert_not_called()
                factory.assert_not_called()
                self.assertFalse(directory.exists())

    def test_default_six_writers_dual_consumers_are_versioned_and_function_profiling_stays_off(self):
        directory = self.temporary()
        h = self.fake_main_harness(directory)
        loader, factory = self.call_main(directory, h, '--events', '4', '--duration', '0')
        args = factory.call_args.args[0]
        self.assertEqual((args.writer_topology, args.writer_topology_version),
                         ('writers-6', WRITER_TOPOLOGY_VERSION))
        self.assertEqual(args.consumer_topology, 'notification-dual')
        self.assertFalse(args.diagnostic_profile)
        self.assertEqual(args.diagnostic_profile_engine, 'cprofile')
        self.assertEqual(args.tier, 'smoke')
        self.assertEqual(args.events, 4)
        loader.assert_called_once()
        h.run.assert_called_once_with()
        h.finish.assert_called_once_with(None)
        manifest = json.loads((directory / 'publisher-probe-manifest.json').read_text())
        self.assertFalse(manifest['qualification_admissible'])
        self.assertFalse(manifest['function_profiling_enabled'])
        self.assertTrue(manifest['diagnostic_only'])

    def test_admit_only_accepts_exact_six_writer_profile_without_environment_or_evidence_io(self):
        directory = self.temporary() / 'no-files'
        with patch.object(sys, 'argv', self.argv(directory, '--admit-only', '--events', '3000',
                    '--rate', '50', '--duration', '60')), \
             patch.object(probe, 'load_environment') as loader, \
             patch.object(probe, 'PublisherProbe') as factory, \
             patch.object(probe.subprocess, 'check_output') as git:
            probe.main()
        loader.assert_not_called()
        factory.assert_not_called()
        git.assert_not_called()
        self.assertFalse(directory.exists())

    def test_missed_steady_gates_are_retained_as_complete_diagnostic_and_capacity_false(self):
        directory = self.temporary()
        h = self.fake_main_harness(directory)
        self.call_main(directory, h, '--events', '4', '--duration', '0')
        result = json.loads((directory / 'publisher-probe-result.json').read_text())
        self.assertEqual(result['status'], 'COMPLETE')
        self.assertFalse(result['steady_gate_passed'])
        self.assertFalse(result['qualification_admissible'])
        self.assertFalse(result['capacity_accepted'])
        self.assertEqual(result['events_requested'], 4)
        self.assertEqual(result['events_committed'], 4)
        self.assertEqual(result['fault_cases_executed'], 0)
        self.assertNotIn('records', result['observation'])

    def test_original_body_failure_is_preserved_after_owned_finish_and_incomplete_result(self):
        for observation in ('complete', 'missing', 'invalid', 'nonobject'):
            with self.subTest(observation=observation):
                directory = self.temporary()
                original = RuntimeError('business body failed')
                h = self.fake_main_harness(directory, error=original, observation=observation)
                with self.assertRaises(RuntimeError) as raised:
                    self.call_main(directory, h, '--events', '4', '--duration', '0')
                self.assertIs(raised.exception, original)
                h.finish.assert_called_once_with(original)
                result = json.loads((directory / 'publisher-probe-result.json').read_text())
                self.assertEqual(result['status'], 'INCOMPLETE')
                self.assertFalse(result['capacity_accepted'])
                self.assertEqual(result['events_requested'], 4)

    def test_missing_observation_fails_successful_body_after_retaining_incomplete_result(self):
        directory = self.temporary()
        h = self.fake_main_harness(directory, observation='missing')
        with self.assertRaises(AssertionError):
            self.call_main(directory, h, '--events', '4', '--duration', '0')
        h.finish.assert_called_once_with(None)
        result = json.loads((directory / 'publisher-probe-result.json').read_text())
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertFalse(result['qualification_admissible'])

    def test_metrics_incomplete_does_not_turn_into_complete_observation(self):
        directory = self.temporary()
        h = self.fake_main_harness(directory)
        h.metrics_samples.errors = [{'error_type': 'TimeoutError', 'stage': 'sample'}]
        with self.assertRaises(AssertionError):
            self.call_main(directory, h, '--events', '4', '--duration', '0')
        result = json.loads((directory / 'publisher-probe-result.json').read_text())
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertEqual(result['metrics_errors'], h.metrics_samples.errors)

    def test_final_publisher_ack_count_must_cover_all_requested_events(self):
        directory = self.temporary()
        h = self.fake_main_harness(directory)
        h.metrics_samples.publisher_ack_count = 3
        with self.assertRaises(AssertionError):
            self.call_main(directory, h, '--events', '4', '--duration', '0')
        result = json.loads((directory / 'publisher-probe-result.json').read_text())
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertEqual(result['last_publisher_ack_count'], 3)

    def test_finish_failure_is_propagated_after_incomplete_result_is_saved(self):
        directory = self.temporary()
        h = self.fake_main_harness(directory)
        original = OSError('worker cleanup failed')
        h.finish.side_effect = original
        with self.assertRaises(OSError) as raised:
            self.call_main(directory, h, '--events', '4', '--duration', '0')
        self.assertIs(raised.exception, original)
        result = json.loads((directory / 'publisher-probe-result.json').read_text())
        self.assertEqual(result['status'], 'INCOMPLETE')
        self.assertIn({'stage': 'harness_finish', 'error_type': 'OSError'}, result['observation_errors'])

    def test_environment_failure_preserves_requested_denominator_before_generation(self):
        directory = self.temporary()
        original = OSError('environment unavailable')
        with patch.object(sys, 'argv', self.argv(directory, '--events', '12', '--duration', '0')), \
             patch.object(probe, 'load_environment', side_effect=original), \
             patch.object(probe.subprocess, 'check_output', return_value='source-head\n'), \
             patch.object(probe, 'PublisherProbe') as factory:
            with self.assertRaises(OSError) as raised:
                probe.main()
        self.assertIs(raised.exception, original)
        factory.assert_not_called()
        result = json.loads((directory / 'startup-failure.json').read_text())
        self.assertFalse(result['qualification_admissible'])
        self.assertEqual((result['events_requested'], result['attempted'],
                          result['committed'], result['unattempted']), (12, 0, 0, 12))
        self.assertFalse(result['business_execution_started'])

    def test_startup_evidence_save_failure_cannot_replace_original_environment_error(self):
        directory = self.temporary()
        original, secondary = OSError('environment unavailable'), ValueError('startup evidence failure')
        with patch.object(sys, 'argv', self.argv(directory, '--events', '4', '--duration', '0')), \
             patch.object(probe, 'load_environment', side_effect=original), \
             patch.object(probe.subprocess, 'check_output', return_value='source-head\n'), \
             patch.object(probe, 'write_json', side_effect=secondary):
            with self.assertRaises(OSError) as raised:
                probe.main()
        self.assertIs(raised.exception, original)

    def run_harness(self, *, latency=False, lateness=2, generate_error=None):
        directory = self.temporary()
        (directory / 'metrics').mkdir()
        h = object.__new__(probe.PublisherProbe)
        h.args = SimpleNamespace(events=4, rate=50, duration=0, drain_timeout=900)
        h.evidence = directory
        h.children = [SimpleNamespace(pid=481, poll=lambda: None)]
        h.workers = {'publisher': h.children[0]}
        h.child_metrics = {481: 21000}
        h.env = {'WORKER_METRICS_TOKEN': 'PRIVATE_METRICS_TOKEN'}
        h.cases = []
        h.setup, h.start_publisher, h.restore_consumer_pools = Mock(), Mock(), Mock()
        h.generate = Mock(return_value=(['a', 'b', 'c', 'd'],
            {'schedule_lateness_seconds': lateness}), side_effect=generate_error)
        h.latencies = Mock(return_value={'passed': latency})
        h.snapshot = Mock(return_value={'mismatches': [], 'dedupe_count': 8,
            'notification_count': 4, 'expected_notification_count': 4})
        h.metrics_samples = probe.WorkerMetricsSamples(h, interval=60)
        h.drained = Mock()
        return h

    def metric_payload(self, count=None):
        heartbeat = ('# TYPE labops_worker_heartbeat_timestamp_seconds gauge\n'
            'labops_worker_heartbeat_timestamp_seconds{worker="publisher"} 1\n')
        if count is None:
            return heartbeat.encode()
        return (heartbeat + '# TYPE labops_worker_publish_ack_seconds histogram\n'
            f'labops_worker_publish_ack_seconds_bucket{{worker="publisher",le="0.1"}} {count}\n'
            f'labops_worker_publish_ack_seconds_bucket{{worker="publisher",le="+Inf"}} {count}\n'
            f'labops_worker_publish_ack_seconds_count{{worker="publisher"}} {count}\n'
            f'labops_worker_publish_ack_seconds_sum{{worker="publisher"}} {count * .01}\n').encode()

    def test_metrics_cover_generation_through_final_drain_before_worker_stop_and_never_save_token(self):
        h = self.run_harness()
        state = {'drained': False}
        requests = []

        def drain(ids, **kwargs):
            self.assertTrue(h.metrics_samples.thread.is_alive())
            self.assertFalse(h.metrics_samples.stop.is_set())
            self.assertEqual(ids, ['a', 'b', 'c', 'd'])
            state['drained'] = True

        def response(request, **kwargs):
            requests.append(request)
            count = 4 if state['drained'] else 0
            return io.BytesIO(self.metric_payload(count))

        h.drained.side_effect = drain
        with patch.object(probe.urllib.request, 'urlopen', side_effect=response):
            h.run()
        self.assertFalse(h.cases[0]['passed'])
        self.assertFalse(h.metrics_samples.thread.is_alive())
        sampling = json.loads((h.evidence / 'metrics' / 'sampling.json').read_text())
        self.assertTrue(sampling['collection_complete'])
        saved = [row for row in sampling['samples'] if row['status'] == 'SAVED']
        self.assertGreaterEqual(len(saved), 2)
        self.assertEqual(saved[0]['publisher_ack_count'], 0)
        self.assertEqual(saved[-1]['publisher_ack_count'], 4)
        self.assertEqual(sampling['last_publisher_ack_count'], 4)
        self.assertTrue(all(request.get_header('Authorization') == 'Bearer PRIVATE_METRICS_TOKEN'
                            for request in requests))
        for path in h.evidence.rglob('*'):
            if path.is_file():
                self.assertNotIn(b'PRIVATE_METRICS_TOKEN', path.read_bytes())

    def test_generate_failure_still_joins_metrics_sampler_and_saves_final_metrics(self):
        original = RuntimeError('committed observation failed')
        h = self.run_harness(generate_error=original)
        with patch.object(probe.urllib.request, 'urlopen',
                          side_effect=lambda *a, **kw: io.BytesIO(self.metric_payload(0))):
            with self.assertRaises(RuntimeError) as raised:
                h.run()
        self.assertIs(raised.exception, original)
        self.assertFalse(h.metrics_samples.thread.is_alive())
        self.assertTrue((h.evidence / 'metrics' / 'sampling.json').exists())
        self.assertGreaterEqual(len(h.metrics_samples.samples), 2)
        h.drained.assert_not_called()

    def test_metrics_failure_records_bounded_type_and_cannot_leak_exception_text_or_token(self):
        h = self.run_harness()
        original = OSError('failure containing PRIVATE_METRICS_TOKEN')
        with patch.object(probe.urllib.request, 'urlopen', side_effect=original):
            with h.metrics_samples.collecting():
                pass
        sampling = json.loads((h.evidence / 'metrics' / 'sampling.json').read_text())
        self.assertFalse(sampling['collection_complete'])
        self.assertEqual({row['error_type'] for row in sampling['errors']}, {'OSError'})
        self.assertNotIn('PRIVATE_METRICS_TOKEN', json.dumps(sampling))

    def test_metrics_file_token_has_worker_precedence_and_is_never_saved(self):
        h = self.run_harness()
        token_file = self.temporary() / 'private-token'
        token_file.write_text('FILE_METRICS_TOKEN\n')
        h.env['WORKER_METRICS_TOKEN_FILE'] = str(token_file)
        requests = []

        def response(request, **kwargs):
            requests.append(request)
            return io.BytesIO(self.metric_payload(4))

        with patch.object(probe.urllib.request, 'urlopen', side_effect=response):
            with h.metrics_samples.collecting():
                pass
        self.assertTrue(requests)
        self.assertTrue(all(request.get_header('Authorization') == 'Bearer FILE_METRICS_TOKEN'
                            for request in requests))
        for path in h.evidence.rglob('*'):
            if path.is_file():
                self.assertNotIn(b'FILE_METRICS_TOKEN', path.read_bytes())
                self.assertNotIn(b'PRIVATE_METRICS_TOKEN', path.read_bytes())

    def test_http_success_without_valid_worker_metrics_is_incomplete(self):
        for payload in (b'', b'<html>not metrics</html>', b'unrelated_metric 1\n'):
            with self.subTest(payload=payload):
                h = self.run_harness()
                with patch.object(probe.urllib.request, 'urlopen',
                                  side_effect=lambda *a, **kw: io.BytesIO(payload)):
                    with h.metrics_samples.collecting():
                        pass
                sampling = json.loads((h.evidence / 'metrics' / 'sampling.json').read_text())
                self.assertFalse(sampling['collection_complete'])
                self.assertTrue(sampling['errors'])
                self.assertIsNone(sampling['last_publisher_ack_count'])

    def test_initial_absent_ack_is_unobserved_then_final_histogram_is_retained(self):
        h = self.run_harness()
        payloads = [self.metric_payload(), self.metric_payload(4)]
        with patch.object(probe.urllib.request, 'urlopen',
                          side_effect=lambda *a, **kw: io.BytesIO(payloads.pop(0))):
            with h.metrics_samples.collecting():
                pass
        self.assertEqual(h.metrics_samples.samples[0]['publisher_ack_histogram_status'], 'NOT_YET_OBSERVED')
        self.assertEqual(h.metrics_samples.samples[-1]['publisher_ack_count'], 4)
        self.assertFalse(h.metrics_samples.errors)

    def test_oversized_metric_response_is_bounded_failure_and_never_saved_as_raw_metric(self):
        h = self.run_harness()
        oversized = b'x' * 1_048_577
        with patch.object(probe.urllib.request, 'urlopen',
                          side_effect=lambda *a, **kw: io.BytesIO(oversized)):
            with h.metrics_samples.collecting():
                pass
        sampling = json.loads((h.evidence / 'metrics' / 'sampling.json').read_text())
        self.assertFalse(sampling['collection_complete'])
        self.assertEqual({row['error_type'] for row in sampling['errors']}, {'ValueError'})
        self.assertFalse(list((h.evidence / 'metrics').glob('*.prom')))

    def test_secondary_sampling_save_error_does_not_replace_original_body_exception(self):
        h = self.run_harness()
        original, secondary = RuntimeError('original body'), OSError('evidence disk failure')
        with patch.object(probe.urllib.request, 'urlopen',
                          side_effect=lambda *a, **kw: io.BytesIO(self.metric_payload(0))), \
             patch.object(probe, 'write_json', side_effect=secondary):
            with self.assertRaises(RuntimeError) as raised:
                with h.metrics_samples.collecting():
                    raise original
        self.assertIs(raised.exception, original)
        self.assertFalse(h.metrics_samples.thread.is_alive())

    def test_thread_start_and_secondary_join_failure_preserve_original_start_error(self):
        h = self.run_harness()
        original = ValueError('thread startup control')
        thread = Mock()
        thread.ident = None
        thread.start.side_effect = original
        thread.join.side_effect = RuntimeError('cannot join unstarted thread')
        thread.is_alive.return_value = False
        with patch.object(probe.urllib.request, 'urlopen',
                          side_effect=lambda *a, **kw: io.BytesIO(self.metric_payload(0))), \
             patch.object(probe.threading, 'Thread', return_value=thread):
            with self.assertRaises(ValueError) as raised:
                with h.metrics_samples.collecting():
                    self.fail('Body entered after failed sampler start')
        self.assertIs(raised.exception, original)
        self.assertTrue((h.evidence / 'metrics' / 'sampling.json').exists())
        sampling = json.loads((h.evidence / 'metrics' / 'sampling.json').read_text())
        self.assertFalse(sampling['collection_complete'])

    def test_secondary_join_failure_cannot_replace_original_business_error(self):
        h = self.run_harness()
        original = ValueError('business control')
        thread = Mock()
        thread.ident = 481
        thread.join.side_effect = OSError('sampler join control')
        thread.is_alive.return_value = False
        with patch.object(probe.urllib.request, 'urlopen',
                          side_effect=lambda *a, **kw: io.BytesIO(self.metric_payload(0))), \
             patch.object(probe.threading, 'Thread', return_value=thread):
            with self.assertRaises(ValueError) as raised:
                with h.metrics_samples.collecting():
                    raise original
        self.assertIs(raised.exception, original)
        sampling = json.loads((h.evidence / 'metrics' / 'sampling.json').read_text())
        self.assertFalse(sampling['collection_complete'])
        self.assertIn({'stage': 'metrics_sampler_join', 'error_type': 'OSError'}, sampling['errors'])

    def test_observation_admissibility_is_independent_of_function_profile_and_ordinary_final_checks(self):
        for observation, function_profile, expected in ((False, False, True),
                (True, False, False), (False, True, False), (True, True, False)):
            with self.subTest(observation=observation, function_profile=function_profile):
                h = accounting.HarnessGenerationAccountingTests.harness(self)
                h.publisher_observation_enabled = observation
                h.cases = [{'name': 'steady', 'passed': True}]
                h.final_inventory_evidence = Mock(return_value={
                    'database_observed': True, 'offsets_observed': True, 'errors': [], 'unpublished_count': 0,
                    'actual_inventory_outbox_count': 0, 'processed_hash_conflicts': [], 'consumers': {},
                    'event_log_ids_missing_from_database': [], 'actual_committed_ids_missing_from_event_log': [],
                    'journal_committed_movements_missing_from_database': [],
                    'database_committed_movements_missing_from_journal': [],
                    'journal_identified_events_missing_from_database': [],
                    'reconciliation': {'mismatches': [], 'dedupe_count': 0,
                        'notification_count': 0, 'expected_notification_count': 0}})
                h.diagnostic_profile_evidence = Mock(return_value={'enabled': function_profile,
                    'complete': True, 'status': 'COMPLETE'})
                with redirect_stdout(io.StringIO()):
                    report = h.finish()
                self.assertTrue(report['final_inventory_complete'])
                self.assertEqual(report['qualification_admissible'], expected)
                self.assertEqual(report['passed'], expected)
                self.assertEqual(report['publisher_observation_enabled'], observation)

    def test_actual_workflow_admission_preserves_observation_and_legacy_profile_guards(self):
        # Execute the authored early shell admission with real pure-topology
        # scripts; all generated frozen files stay in a disposable directory.
        workflow = (probe.ROOT / '.github/workflows/events-validation.yml').read_text()
        beginning = workflow.index('      - name: Admit joint writer and consumer request before Python setup\n')
        end = workflow.index('      - uses: actions/setup-python', beginning)
        block = workflow[beginning:end].split('        run: |\n', 1)[1]
        script = '\n'.join(line[10:] for line in block.splitlines())
        script = script.replace('python3 ', shlex.quote(sys.executable) + ' ')
        for name in ('consumer_topology', 'writer_topology', 'publisher_probe'):
            relative = f'benchmarks/events/{name}.py'
            script = script.replace(relative, shlex.quote(str(probe.ROOT / relative)))
        cases = [
            ('true', 'false', 'smoke', 'cprofile', 'writers-6', 'notification-dual', 'legacy', 'false', True),
            ('false', 'true', 'smoke', 'cprofile', 'writers-4', 'single', 'legacy', 'false', True),
            ('true', 'true', 'smoke', 'cprofile', 'writers-4', 'single', 'legacy', 'false', False),
            ('true', 'false', 'full', 'cprofile', 'writers-6', 'notification-dual', 'legacy', 'false', False),
            ('false', 'true', 'smoke', 'cprofile', 'writers-6', 'single', 'legacy', 'false', False),
            ('false', 'true', 'smoke', 'cprofile', 'writers-4', 'notification-dual', 'legacy', 'false', False),
            ('false', 'false', 'smoke', 'python-profile-owned', 'writers-4', 'single', 'legacy', 'false', False),
            ('true', 'false', 'smoke', 'cprofile', 'writers-6', 'notification-dual', 'native-scoped', 'false', True),
            ('true', 'false', 'smoke', 'cprofile', 'writers-6', 'notification-dual', 'unknown', 'false', False),
            ('false', 'false', 'smoke', 'cprofile', 'writers-6', 'notification-dual', 'native-scoped', 'false', False),
            ('true', 'true', 'smoke', 'cprofile', 'writers-6', 'notification-dual', 'native-scoped', 'false', False),
            ('true', 'false', 'smoke', 'cprofile', 'writers-6', 'notification-dual', 'native-scoped', 'true', False),
            ('true', 'false', 'smoke', 'cprofile', 'writers-4', 'notification-dual', 'native-scoped', 'false', False),
            ('true', 'false', 'smoke', 'cprofile', 'writers-6', 'single', 'native-scoped', 'false', False),
            ('true', 'false', 'full', 'cprofile', 'writers-6', 'notification-dual', 'native-scoped', 'false', False)]
        for observation, profiling, tier, engine, writer, consumer, mode, runtime, allowed in cases:
            with self.subTest(observation=observation, profiling=profiling, writer=writer,
                    consumer=consumer, mode=mode, runtime_diagnostics=runtime):
                env = {**os.environ, 'PUBLISHER_OBSERVATION': observation, 'DIAGNOSTIC_PROFILE': profiling,
                    'PUBLISHER_OBSERVATION_MODE': mode, 'RUNTIME_DIAGNOSTICS': runtime,
                    'EVENTS_TIER': tier, 'DIAGNOSTIC_PROFILE_ENGINE': engine, 'WRITER_TOPOLOGY': writer,
                    'CONSUMER_TOPOLOGY': consumer, 'GITHUB_RUN_ID': '11', 'GITHUB_RUN_ATTEMPT': '1',
                    'EVENTS_COUNT': '3000', 'EVENTS_RATE': '50', 'EVENTS_DURATION': '60'}
                result = subprocess.run(['bash', '-eu', '-c', script], env=env,
                    cwd=self.temporary(), capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode == 0, allowed, result.stderr + result.stdout)
