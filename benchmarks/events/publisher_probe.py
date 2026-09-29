"""Finite same-topology publisher measurement, never capacity acceptance.

This uses the real harness's fixtures, legal commands, ownership, native clients,
drain and cleanup. Its sole steady case is a diagnostic input, and does not run
the fault matrix. Function profiling remains OFF with its original guards intact.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from benchmarks.events.acceptance import Harness, HERE, load_environment, write_json
from benchmarks.events.consumer_topology import PRESETS
from benchmarks.events.writer_topology import WRITER_PRESETS, WRITER_TOPOLOGY_VERSION


class WorkerMetricsSamples:
    """Observe each live worker before generation through the final drain."""
    def __init__(self, harness, interval=2):
        self.harness = harness
        self.interval = interval
        self.stop = threading.Event()
        self.sequence = 0
        self.errors = []
        self.samples = []
        self.thread = None
        self.publisher_ack_count = None

    def sample(self):
        harness = self.harness
        self.sequence += 1
        for index, process in enumerate(list(harness.children)):
            if process.poll() is not None:
                continue
            began = time.time_ns()
            item = {'sample': self.sequence, 'worker_index': index, 'pid': process.pid,
                    'started_epoch_ns': began}
            path = harness.evidence / 'metrics' / f'sample-{self.sequence:04d}-worker-{index:03d}.prom'
            request = urllib.request.Request(
                f'http://127.0.0.1:{harness.child_metrics[process.pid]}/metrics')
            try:
                token_file = harness.env.get('WORKER_METRICS_TOKEN_FILE')
                token = (Path(token_file).read_text().strip() if token_file else
                         harness.env.get('WORKER_METRICS_TOKEN', harness.env.get('METRICS_TOKEN', '')))
                if token:
                    request.add_header('Authorization', 'Bearer ' + token)
                with urllib.request.urlopen(request, timeout=3) as response:
                    payload = response.read(1_048_577)
                if len(payload) > 1_048_576:
                    raise ValueError('Worker metric response exceeded the finite byte bound')
                from prometheus_client.parser import text_string_to_metric_families
                values = [sample for family in text_string_to_metric_families(payload.decode())
                          for sample in family.samples]
                heartbeat = [sample for sample in values
                    if sample.name == 'labops_worker_heartbeat_timestamp_seconds'
                    and sample.labels.get('worker') in {'publisher', 'notification', 'analytics'}
                    and math.isfinite(sample.value) and sample.value > 0]
                if not heartbeat:
                    raise ValueError('Worker heartbeat metric unavailable')
                if process is harness.workers.get('publisher'):
                    counts = [sample.value for sample in values
                        if sample.name == 'labops_worker_publish_ack_seconds_count'
                        and sample.labels == {'worker': 'publisher'}]
                    if counts:
                        count = counts[0]
                        totals = [sample.value for sample in values
                            if sample.name == 'labops_worker_publish_ack_seconds_sum'
                            and sample.labels == {'worker': 'publisher'}]
                        buckets = [sample for sample in values
                            if sample.name == 'labops_worker_publish_ack_seconds_bucket'
                            and sample.labels.get('worker') == 'publisher']
                        if (len(counts) != 1 or len(totals) != 1 or not buckets
                                or not math.isfinite(count) or count < 0
                                or not math.isfinite(totals[0]) or totals[0] < 0):
                            raise ValueError('Publisher ACK histogram unavailable')
                        self.publisher_ack_count = count
                        item['publisher_ack_count'] = count
                    elif self.publisher_ack_count is not None:
                        raise ValueError('Previously observed publisher ACK histogram disappeared')
                    else:
                        item['publisher_ack_histogram_status'] = 'NOT_YET_OBSERVED'
                with path.open('xb') as stream:
                    stream.write(payload)
                item.update(status='SAVED', path=str(path.relative_to(harness.evidence)),
                            bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
            except Exception as error:
                item.update(status='UNAVAILABLE', error_type=type(error).__name__)
                self.errors.append(dict(item))
            item['ended_epoch_ns'] = time.time_ns()
            self.samples.append(item)

    def loop(self):
        try:
            while not self.stop.wait(self.interval):
                self.sample()
        except BaseException as error:
            self.errors.append({'stage': 'metrics_sampler', 'error_type': type(error).__name__})

    @contextmanager
    def collecting(self):
        began = time.time_ns()
        original, entered = None, False
        try:
            self.sample()
            self.thread = threading.Thread(target=self.loop, name='publisher-probe-metrics', daemon=True)
            self.thread.start()
            entered = True
            yield
        except BaseException as error:
            original = error
            if not entered:
                self.errors.append({'stage': 'metrics_sampler_start', 'error_type': type(error).__name__})
            raise
        finally:
            self.stop.set()
            joined, join_error = True, None
            try:
                if self.thread is not None and self.thread.ident is not None:
                    self.thread.join(timeout=15)
                    joined = not self.thread.is_alive()
                    if not joined:
                        self.errors.append({'stage': 'metrics_sampler_join', 'error_type': 'TimeoutError'})
            except BaseException as error:
                joined, join_error = False, error
                self.errors.append({'stage': 'metrics_sampler_join', 'error_type': type(error).__name__})
            try:
                if joined:
                    self.sample()
                write_json(self.harness.evidence / 'metrics' / 'sampling.json', {
                    'started_epoch_ns': began, 'ended_epoch_ns': time.time_ns(),
                    'interval_seconds': self.interval, 'samples': self.samples, 'errors': self.errors,
                    'last_publisher_ack_count': self.publisher_ack_count,
                    'scope': 'before generation through successful drain or original failure, before worker stop',
                    'collection_complete': not self.errors})
            except BaseException:
                if original is None:
                    raise
            if original is None and join_error is not None:
                raise join_error


class PublisherProbe(Harness):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.publisher_observation_enabled = True
        self.probe_output = None
        self.metrics_samples = WorkerMetricsSamples(self)

    def start_publisher(self):
        self.probe_output = self.evidence / 'logs' / f'publisher-profile-{len(self.children):03d}.json'
        argv = [str(HERE / 'profile_publisher.py'), '--output', str(self.probe_output), '--observation-only']
        self.workers['publisher'] = self.spawn('publisher-' + str(len(self.children)), argv, 'publisher')
        self.sync_metrics_targets()

    def run(self):
        self.setup()
        self.start_publisher()
        self.restore_consumer_pools(timeout=120)
        with self.metrics_samples.collecting():
            started = time.monotonic()
            ids, workload = self.generate(self.args.events, 'steady')
            while time.monotonic() - started < self.args.duration:
                time.sleep(.25)
            self.drained(ids, timeout=self.args.drain_timeout)
            latency = self.latencies()
            snapshot = self.snapshot(ids)
            assert not snapshot['mismatches'] and snapshot['dedupe_count'] == len(ids) * 2
            assert snapshot['notification_count'] == snapshot['expected_notification_count']
            self.cases.append({'name': 'steady', 'workload': workload, 'reconciliation': snapshot,
                'latency': latency, 'passed': latency['passed'] and
                workload['schedule_lateness_seconds'] <= max(1, self.args.events / self.args.rate * .05)})
            # A missed rate/latency gate is recorded, never retried or converted
            # to acceptance. It does not discard otherwise usable observation.


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--evidence-dir', type=Path, required=True)
    p.add_argument('--generated-dir', type=Path, default=ROOT / 'infra/events/validation/generated')
    p.add_argument('--writer-topology', choices=WRITER_PRESETS, default='writers-6')
    p.add_argument('--consumer-topology', choices=PRESETS, default='notification-dual')
    p.add_argument('--events', type=int, default=3000)
    p.add_argument('--rate', type=float, default=50)
    p.add_argument('--duration', type=float, default=60)
    p.add_argument('--runtime-diagnostics', action='store_true')
    p.add_argument('--admit-only', action='store_true', help='Validate finite inputs without opening services or files')
    return p


def main():
    p = parser()
    args = p.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', args.run_id):
        p.error('Invalid disposable run ID')
    if not 4 <= args.events <= 3000:
        p.error('Finite observation requires 4 through 3000 events')
    if not math.isfinite(args.rate) or not .01 <= args.rate <= 50:
        p.error('Finite observation rate must be positive and at most 50')
    if not math.isfinite(args.duration) or not 0 <= args.duration <= 300:
        p.error('Finite observation duration must be between zero and 300 seconds')
    if args.events / args.rate > 300:
        p.error('Finite observation requires a scheduled generation window at most 300 seconds')
    if args.admit_only:
        return
    args.writer_topology_version = WRITER_TOPOLOGY_VERSION
    args.tier = 'smoke'
    args.fault_repetitions = 1
    args.fault_events = 20
    args.duplicate_events = 10000
    args.poison_events = 100
    args.broker_fault_seconds = args.outage_seconds = args.consumer_outage_seconds = 5
    args.drain_timeout = 900
    args.diagnostic_profile = False
    args.diagnostic_profile_engine = 'cprofile'
    args.generated_dir = args.generated_dir.resolve()
    args.evidence_dir = args.evidence_dir.resolve()
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    manifest = {'diagnostic_only': True, 'qualification_admissible': False,
        'source_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'started_epoch_ns': time.time_ns(), 'writer_topology': args.writer_topology,
        'consumer_topology': args.consumer_topology, 'events': args.events, 'rate': args.rate,
        'duration': args.duration, 'function_profiling_enabled': False,
        'runtime_diagnostics_enabled': args.runtime_diagnostics,
        'scope': 'one finite steady diagnostic; all matrix fault cases intentionally unexecuted',
        'source_sha256': {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (Path(__file__), HERE / 'acceptance.py', HERE / 'profile_publisher.py',
                         HERE / 'diagnostic_profile.py', ROOT / 'labops/events.py')}}
    with (args.evidence_dir / 'publisher-probe-manifest.json').open('x') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    harness, original = None, None
    try:
        load_environment(args.generated_dir / 'client.env')
        for key in list(os.environ):
            if key.startswith('POSTGRES_') and key != 'POSTGRES_PASSWORD':
                os.environ.pop(key)
        harness = PublisherProbe(args)
        harness.run()
    except BaseException as error:
        original = error
        raise
    finally:
        if harness is not None:
            report, observations, finish_error = {}, {}, None
            observation_errors = []
            try:
                report = harness.finish(original)
            except BaseException as error:
                finish_error = error
                observation_errors.append({'stage': 'harness_finish', 'error_type': type(error).__name__})
            try:
                observations = json.loads(harness.probe_output.read_text())
                if not isinstance(observations, dict):
                    raise ValueError('Publisher observation must be an object')
            except BaseException as error:
                observations = {}
                observation_errors.append({'stage': 'publisher_observation_read', 'error_type': type(error).__name__})
            try:
                observation = observations.get('publisher_observation', {})
                if not isinstance(observation, dict):
                    observation = {}
                    observation_errors.append({'stage': 'publisher_observation_shape', 'error_type': 'ValueError'})
                complete = (original is None and not observation_errors and report.get('final_inventory_complete')
                    and report.get('owned_worker_cleanup_complete') and not harness.metrics_samples.errors
                    and harness.metrics_samples.publisher_ack_count is not None
                    and harness.metrics_samples.publisher_ack_count >= args.events
                    and observation.get('complete') is True)
                result = {'status': 'COMPLETE' if complete else 'INCOMPLETE', 'diagnostic_only': True,
                    'qualification_admissible': False, 'capacity_accepted': False,
                    'function_graph_status': observations.get('function_graph_status', 'UNAVAILABLE'),
                    'source_revision': manifest['source_revision'], 'events_requested': args.events,
                    'events_committed': report.get('steady_inventory_inputs_committed'),
                    'steady_gate_passed': bool(report.get('cases') and report['cases'][0]['passed']),
                    'fault_cases_executed': 0, 'observation': {
                        key: value for key, value in observation.items() if key not in {'records', 'attempts'}},
                    'metrics_errors': harness.metrics_samples.errors,
                    'last_publisher_ack_count': harness.metrics_samples.publisher_ack_count,
                    'observation_errors': observation_errors,
                    'finished_epoch_ns': time.time_ns()}
                write_json(args.evidence_dir / 'publisher-probe-result.json', result)
                print(json.dumps({key: result[key] for key in (
                    'status', 'diagnostic_only', 'capacity_accepted', 'events_requested',
                    'events_committed', 'steady_gate_passed', 'function_graph_status')}, sort_keys=True), flush=True)
            except BaseException:
                if original is None:
                    raise
            if original is None:
                if finish_error is not None:
                    raise finish_error
                if not complete:
                    raise AssertionError('Incomplete publisher observation; retain all original evidence')
        else:
            try:
                write_json(args.evidence_dir / 'startup-failure.json', {
                    'diagnostic_only': True, 'qualification_admissible': False, 'capacity_accepted': False,
                    'stage': 'environment_or_harness_setup',
                    'error_type': type(original).__name__ if original else None,
                    'events_requested': args.events, 'attempted': 0, 'committed': 0, 'unattempted': args.events,
                    'business_execution_started': False, 'database_state_observed': False})
            except BaseException:
                if original is None:
                    raise


if __name__ == '__main__':
    main()
