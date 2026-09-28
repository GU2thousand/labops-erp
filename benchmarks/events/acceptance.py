"""Real, isolated PostgreSQL / RF3 SASL_SSL acceptance; see README.md."""
import argparse
import copy
from collections import Counter
import csv
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
from urllib.parse import urlparse, urlunparse
import uuid

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from benchmarks.events.consumer_topology import (
    PRESETS, DEFAULT_PRESET, topology_profile, freeze_topology, consumer_roles, worker_roles,
    assignment_result,
)


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + '\n')
    temporary.replace(path)


def canonical_hash(rows):
    return hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':'),
                                     default=str).encode()).hexdigest()


def load_environment(path):
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:]
        key, separator, raw = line.partition('=')
        if not separator or not re.fullmatch(r'[A-Z][A-Z0-9_]*', key):
            raise ValueError('Invalid client.env assignment')
        parsed = shlex.split(raw, comments=True)
        os.environ[key] = parsed[0] if len(parsed) == 1 else raw


def freeze_generation_execution_profile(evidence, run_id, *, runtime_diagnostics=False,
                                        diagnostic_profile=False, diagnostic_profile_engine='cprofile',
                                        consumer_topology=DEFAULT_PRESET):
    from benchmarks.events.process_generation import frozen_process_profile
    from benchmarks.events.runtime_diagnostics import diagnostics_profile
    from benchmarks.events.diagnostic_profile import request_profile
    request = request_profile(diagnostic_profile, diagnostic_profile_engine)
    topology = freeze_topology(Path(evidence) / 'consumer-topology.json', run_id,
                              consumer_topology, diagnostic_profile=diagnostic_profile)
    path = Path(evidence) / 'generation-execution-profile.json'
    with path.open('x') as out:
        json.dump({'run_id': run_id, **frozen_process_profile(),
            'consumer_topology': topology,
            'diagnostic_profile': request,
            'qualification_admissible': not diagnostic_profile,
            'selection_policy': {'mode': 'automatic', 'spawn_minimum_batch_count': 512,
                'smaller_capacity_batches': 'four FIFO thread lanes',
                'capacity_batches_at_or_above_threshold': 'four fresh spawn clients',
                'test_only_force_override': False},
            'runtime_diagnostics': {**diagnostics_profile(),
                'enabled': runtime_diagnostics, 'applicable': runtime_diagnostics,
                'request_status': 'REQUESTED' if runtime_diagnostics else 'NOT_REQUESTED',
                'lifecycle_required_scenarios': 'every concurrent capacity batch',
                'complete_collection_required_scenarios': ['steady'],
                'fault_resource_scope': 'explicit optional missing resource coverage; existing business/count/rate/recovery gates unchanged',
                'elapsed_scope': 'discovery/startup, business commands, joined lane/sampler cleanup, required raw persistence and summary'},
            'capacity_scenarios': ['steady', 'analytics_outage', 'one_broker_stop', 'quorum_loss', 'cluster_outage'],
            'small_fault_drills': 'serial execution using the same global cycle/lane mapping; topology reported per batch'}, out, sort_keys=True)
        out.write('\n')
        out.flush()
        os.fsync(out.fileno())


def stable_committed_offsets(configs, topic, *, timeout=30, consumer_factory=None,
                             monotonic=time.monotonic, sleep=time.sleep, on_retry=None):
    """Require two complete equal snapshots; refresh only coordinator errors.

    A fresh unassigned client repeats FindCoordinator after startup/election.
    Authentication, authorization, topic errors and malformed responses fail
    immediately rather than becoming a successful empty offset snapshot.
    """
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition, OFFSET_INVALID
    factory = consumer_factory or Consumer
    coordinator_codes = {KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_NOT_AVAILABLE,
                         KafkaError.COORDINATOR_LOAD_IN_PROGRESS, KafkaError._WAIT_COORD}
    deadline = monotonic() + timeout
    previous = None
    attempts = 0
    while monotonic() < deadline:
        attempts += 1
        snapshot = {}
        try:
            for name, config in configs.items():
                client = factory(config)
                try:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise TimeoutError('Committed-offset snapshot deadline expired')
                    partitions = client.committed([TopicPartition(topic, n) for n in range(3)],
                                                  timeout=min(10, remaining))
                    errors = [p.error for p in partitions if p.error is not None]
                    # A mixed response must never retry past an ACL failure.
                    terminal = next((error for error in errors
                                     if error.code() not in coordinator_codes), None)
                    if terminal is not None:
                        raise KafkaException(terminal)
                    if errors:
                        raise KafkaException(errors[0])
                    expected = {(topic, n) for n in range(3)}
                    assert len(partitions) == 3 and {(p.topic, p.partition) for p in partitions} == expected, \
                        'Committed-offset response must include exactly the three requested partitions'
                    assert all(p.offset == OFFSET_INVALID or p.offset >= 0 for p in partitions), \
                        'Committed-offset response contains an invalid offset sentinel'
                    snapshot[name] = {str(p.partition): p.offset for p in partitions}
                finally:
                    client.close()
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if not isinstance(error, KafkaError) or error.code() not in coordinator_codes:
                raise
            previous = None
            if on_retry:
                on_retry(error.code(), attempts)
        else:
            if snapshot == previous:
                return snapshot
            previous = snapshot
        remaining = deadline - monotonic()
        if remaining > 0:
            sleep(min(.25, remaining))
    raise TimeoutError('Committed-offset snapshot did not stabilize within its deadline')


class Harness:
    def __init__(self, args, generation=None):
        self.consumer_topology = getattr(args, 'consumer_topology', DEFAULT_PRESET)
        self.topology = topology_profile(self.consumer_topology,
            diagnostic_profile=getattr(args, 'diagnostic_profile', False))
        self.args = args
        self.evidence = args.evidence_dir
        self.evidence.mkdir(parents=True, exist_ok=True)
        with (self.evidence / '.acceptance-started').open('x') as marker:
            marker.write(args.run_id + '\n')
        for directory in ('logs', 'metrics', 'markers', 'backup'):
            (self.evidence / directory).mkdir(exist_ok=True)
        (self.evidence / 'errors.jsonl').touch(exist_ok=False)
        from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
        self.generation = generation or GenerationJournal(self.evidence, args.run_id, numeric_profile(args))
        if generation is None:
            freeze_generation_execution_profile(self.evidence, args.run_id,
                runtime_diagnostics=getattr(args, 'runtime_diagnostics', False),
                diagnostic_profile=getattr(args, 'diagnostic_profile', False),
                diagnostic_profile_engine=getattr(args, 'diagnostic_profile_engine', 'cprofile'),
                consumer_topology=self.consumer_topology)
        self.diagnostic_profile_enabled = getattr(args, 'diagnostic_profile', False)
        self.diagnostic_profile_engine = getattr(args, 'diagnostic_profile_engine', 'cprofile')
        self.publisher_profile_paths = []
        self.children = []
        self.child_metrics = {}
        self.child_groups = {}
        self.child_identities = {}
        self.rejected_child_generations = {}
        self.cleanup_errors = []
        self.worker_closures = {}
        self.workers = {}
        self.shutdowns = []
        self.supervisor_restarts = []
        self.events = []
        self._event_lock = threading.Lock()
        self._next_command_index = 0
        self.generation_topologies = []
        self.process_generation_enabled = None  # automatic, frozen count threshold
        self.process_batches = []
        self.cases = []
        self.delivery_proofs = []
        self.logs = []
        self.secrets = json.loads((args.generated_dir / 'secrets.json').read_text())
        self.env = {**os.environ, 'DJANGO_SETTINGS_MODULE': 'config.settings',
                    'LABOPS_DB_MODE': 'postgres', 'LABOPS_EVENT_TRANSPORT': 'kafka',
                    'REDIS_URL': '', 'OTEL_EXPORTER_OTLP_ENDPOINT': '',
                    'WORKER_METRICS_ENABLED': '1'}
        for name, password in self.secrets.items():
            self.env[f'KAFKA_{name.upper()}_SASL_USERNAME'] = name
            self.env[f'KAFKA_{name.upper()}_SASL_PASSWORD'] = password
        os.environ.update(self.env)
        import django
        django.setup()
        from django.conf import settings
        from django.core.management import call_command
        from django.db import connection, connections
        from labops import models, events
        from labops.inventory import services
        from labops.kafka_config import common_config, consumer_config, consumer_group
        self.settings = settings
        self.call_command = call_command
        self.connection, self.connections = connection, connections
        self.models, self.api, self.services = models, events, services
        self.consumer_config, self.consumer_group = consumer_config, consumer_group
        self.configs = {name: common_config(name) for name in self.secrets}
        self.configs['ca_path'] = settings.KAFKA_SSL_CA_LOCATION
        self.configs['broker_log_reader'] = lambda _case, start, end: self.compose(
            'logs', '--no-color', '--since', start, '--until', end,
            'redpanda-0', 'redpanda-1', 'redpanda-2')
        self.started_at = time.time()
        self.configure_runtime_diagnostics()

    def configure_runtime_diagnostics(self):
        """Freeze opt-in applicability without discovering unrequested resources."""
        self.runtime_diagnostics_enabled = getattr(self.args, 'runtime_diagnostics', False)
        self.runtime_diagnostics = []
        self.runtime_diagnostic_errors = []
        self.runtime_resource_factory = None
        self.container_resources = None
        if self.runtime_diagnostics_enabled:
            from benchmarks.events.container_diagnostics import ContainerResources
            # Fresh discovery occurs inside each measured batch's clock.
            self.runtime_resource_factory = lambda: ContainerResources.from_compose(self.compose,
                command_callback=self.command, expected_project=self.env['LABOPS_VALIDATION_PROJECT'])
        write_json(self.evidence / 'runtime-resource-profile.json', {
            'enabled': self.runtime_diagnostics_enabled,
            'applicable': self.runtime_diagnostics_enabled,
            'status': 'REQUESTED_NOT_STARTED' if self.runtime_diagnostics_enabled else 'NOT_REQUESTED',
            'collection_complete': None,
            'discovery_scope': 'inside each enabled capacity batch elapsed clock'})

    def command(self, argv, *, timeout=60, binary=False, input=None):
        result = subprocess.run(argv, cwd=ROOT, env=self.env, input=input,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=timeout, text=not binary)
        if result.returncode:
            # This helper is used only for non-secret command arguments.
            raise RuntimeError(f'{argv[0]} failed ({result.returncode}): '
                               + (result.stderr.decode(errors='replace') if binary else result.stderr)[-1500:])
        return result.stdout

    def compose(self, *arguments, timeout=90, binary=False):
        return self.command(['docker', 'compose', '-p', self.env['LABOPS_VALIDATION_PROJECT'],
                             '-f', 'infra/events/validation/compose.yaml', *arguments],
                            timeout=timeout, binary=binary)

    def wait(self, check, message, timeout=90):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                result = check()
                if result:
                    return result
            except Exception as exc:
                last = type(exc).__name__
                self.connections.close_all()
            time.sleep(.15)
        raise AssertionError(f'{message}; last_error_type={last}')

    def spawn(self, name, argv, role, *, metrics_port=None, extra_env=None):
        index = len(self.children)
        env = {**self.env, 'KAFKA_SASL_USERNAME': role,
               'KAFKA_SASL_PASSWORD': self.secrets[role],
               'WORKER_METRICS_PORT': str(metrics_port or (21000 + index))}
        if extra_env:
            env.update(extra_env)
        consumer = argv[argv.index('--consumer') + 1] if '--consumer' in argv else None
        receipt = None
        if consumer in {'notification', 'analytics'} and str(HERE / 'workers.py') in argv:
            receipt = self.evidence / 'logs' / f'{name}-{index}-identity'
            application_name = f'lv-{self.args.run_id}-{index}'
            env.update(LABOPS_VALIDATION_WORKER_IDENTITY=str(receipt),
                LABOPS_VALIDATION_WORKER_ROLE=name,
                LABOPS_VALIDATION_WORKER_APPLICATION_NAME=application_name)
        log_path = self.evidence / 'logs' / f'{name}-{index}.log'
        log = log_path.open('x')
        self.logs.append(log)
        process = subprocess.Popen([sys.executable, *argv], cwd=ROOT, env=env,
                                   stdout=log, stderr=log)
        self.children.append(process)
        if process.pid in self.child_identities:
            # A PID-keyed old receipt must never identify a newly started child.
            if not hasattr(self, 'rejected_child_generations'):
                self.rejected_child_generations = {}
            rejected = {'role': name, 'pid': process.pid, 'generation': index,
                'identity_receipt': receipt.name if receipt else None,
                'application_name': env.get('LABOPS_VALIDATION_WORKER_APPLICATION_NAME') if receipt else None,
                'status': 'historical_pid_reuse_rejected', 'log_file': log_path.name}
            self.rejected_child_generations[id(process)] = rejected
            original = AssertionError('A historical owned child PID was reused')
            try:
                process.kill()
                process.wait(timeout=10)
                assert process.poll() is not None, 'Rejected owned child was not reaped'
            except BaseException as error:
                self.record_cleanup_error('rejected_spawn_reap', error, name, process.pid)
            try:
                with (self.evidence / 'worker-processes.jsonl').open('a') as output:
                    output.write(json.dumps(rejected, sort_keys=True) + '\n')
            except BaseException as error:
                self.record_cleanup_error('rejected_spawn_evidence', error, name, process.pid)
            raise original
        self.child_metrics[process.pid] = int(env['WORKER_METRICS_PORT'])
        if consumer is None and 'consume_kafka' in argv:
            consumer = argv[argv.index('consume_kafka') + 1]
        self.child_groups[process.pid] = (env['KAFKA_GROUP_PREFIX'] + '.' + consumer + '.v1'
                                        if consumer in {'notification', 'analytics'} else None)
        from benchmarks.events.process_resources import registered_process_snapshot
        identity = registered_process_snapshot(process.pid)
        self.child_identities[process.pid] = {'role': name, 'logical_consumer': consumer,
            'generation': index, 'pid': process.pid, 'start_time_ticks': identity['start_time_ticks'],
            'process_identity_status': identity['status'], 'client_id': 'acceptance-' + str(process.pid),
            'group': self.child_groups[process.pid], 'metrics_port': self.child_metrics[process.pid],
            'log_file': log_path.name, 'identity_receipt': receipt.name if receipt else None,
            'application_name': env.get('LABOPS_VALIDATION_WORKER_APPLICATION_NAME') if receipt else None,
            'fault_stage': argv[argv.index('--stage') + 1] if '--stage' in argv else 'normal',
            'delivery_file': Path(argv[argv.index('--observations') + 1]).name if '--observations' in argv else None}
        with (self.evidence / 'worker-processes.jsonl').open('a') as output:
            output.write(json.dumps(self.child_identities[process.pid], sort_keys=True) + '\n')
        return process

    def stop_consumer_role(self, name, *, expected_fault_exit=False):
        first = None
        for label in list(self.workers):
            if label == name or label.startswith(name + '-'):
                try:
                    self.stop(label, expected_fault_exit=expected_fault_exit)
                except BaseException as error:
                    if first is None:
                        first = error
                    self.record_cleanup_error('consumer_stop', error, label)
        for child in self.children:
            try:
                if self.child_groups.get(child.pid) != self.consumer_group(name) or child.poll() is not None:
                    continue
                child.send_signal(signal.SIGTERM)
                escalated = False
                began = time.monotonic()
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    escalated = True
                    child.kill()
                    child.wait(timeout=10)
                self.shutdowns.append({'worker': name + '-untracked-instance', 'pid': child.pid,
                    'requested_signal': 'SIGTERM', 'forced_SIGKILL': escalated,
                    'exit_code': child.returncode, 'elapsed_seconds': time.monotonic() - began})
                self.observe_worker_close(child, expected_fault_exit=expected_fault_exit)
            except BaseException as error:
                if first is None:
                    first = error
                self.record_cleanup_error('consumer_untracked_stop', error, name, child.pid)
        for child in self.children:
            try:
                if self.child_groups.get(child.pid) == self.consumer_group(name) and child.poll() is None:
                    raise AssertionError('An old consumer group instance remains alive')
            except BaseException as error:
                if first is None:
                    first = error
                self.record_cleanup_error('consumer_reap_incomplete', error, name, child.pid)
        if first is not None:
            raise first

    def record_cleanup_error(self, stage, error, role=None, pid=None):
        if not hasattr(self, 'cleanup_errors'):
            self.cleanup_errors = []
        self.cleanup_errors.append({'stage': stage, 'error_type': type(error).__name__,
                                    'role': role, 'pid': pid})

    def group_assignment(self, name, *, client_id=None, client_ids=None, timeout=10):
        from confluent_kafka import ConsumerGroupState
        from confluent_kafka.admin import AdminClient
        group = self.consumer_group(name)
        began = time.monotonic()
        request_timeout = min(8, timeout * .8)
        observation = {'observed_at': time.time(), 'group': group,
                       'expected_client_id': client_id, 'request_timeout_seconds': request_timeout,
                       'expected_client_ids': list(client_ids) if client_ids is not None else [client_id],
                       'future_timeout_seconds': timeout}
        try:
            # The returned future does not retain the native AdminClient. A
            # temporary client is destroyed before its asynchronous request
            # completes, leaving an uncompleted Python future. Keep this local
            # reference alive through result() and bound the native request too.
            admin = AdminClient(self.configs['admin'])
            pending = admin.describe_consumer_groups([group], request_timeout=request_timeout)[group]
            description = pending.result(timeout=timeout)
            observation['state'] = str(description.state)
            observation['member_count'] = len(description.members)
            observation['members'] = [
                {'client_id': member.client_id, 'assignments': [
                    {'topic': part.topic, 'partition': part.partition}
                    for part in (getattr(member.assignment, 'topic_partitions', None) or [])]}
                for member in description.members]
        except Exception as exc:
            observation['error_type'] = type(exc).__name__
            error = exc.args[0] if exc.args else None
            numeric = error.code() if hasattr(error, 'code') and callable(error.code) else None
            if type(numeric) is int:
                observation['kafka_error_code'] = numeric
            raise
        finally:
            observation['elapsed_seconds'] = time.monotonic() - began
            with (self.evidence / 'group-assignment-observations.jsonl').open('a') as out:
                out.write(json.dumps(observation, sort_keys=True) + '\n')
        expected_ids = tuple(client_ids) if client_ids is not None else (client_id,)
        result = assignment_result(description, topic=self.settings.KAFKA_TOPIC,
            client_ids=expected_ids, stable_state=ConsumerGroupState.STABLE)
        return {'group': group, **result} if result else False

    def wait_group_assignment(self, name, *, client_id=None, client_ids=None, message, timeout=90):
        """Share the original assignment deadline with every native request."""
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            try:
                options = {'client_ids': client_ids} if client_ids is not None else {'client_id': client_id}
                result = self.group_assignment(name, **options, timeout=min(10, remaining))
            except Exception as exc:
                last = type(exc).__name__
                result = False
            if time.monotonic() >= deadline:
                with (self.evidence / 'group-assignment-observations.jsonl').open('a') as out:
                    out.write(json.dumps({'kind': 'assignment_deadline_expired',
                        'expected_client_id': client_id, 'consumer': name,
                        'budget_seconds': timeout, 'late_ready_result': bool(result),
                        'last_error_type': last}) + '\n')
                break
            if result:
                return result
            time.sleep(min(.15, max(0, deadline - time.monotonic())))
        raise AssertionError(f'{message}; last_error_type={last}')

    def pool_roles(self, name):
        return consumer_roles(name, getattr(self, 'consumer_topology', DEFAULT_PRESET))

    def start_consumer_pool(self, name):
        """Leave every started child owned even if a later slot fails to start."""
        for label in self.pool_roles(name):
            if label in self.workers:
                raise AssertionError('Consumer pool role already started: ' + label)
            suffix = label[len(name):]
            if suffix:
                self.start_consumer(name, suffix)
            else:
                self.start_consumer(name)

    def wait_consumer_pool(self, name, *, timeout=90, labels=None):
        deadline = time.monotonic() + timeout
        labels = tuple(labels if labels is not None else self.pool_roles(name))
        processes = [self.workers[label] for label in labels]
        assert all(process.poll() is None for process in processes), 'Owned consumer exited before readiness'
        assert len({process.pid for process in processes}) == len(processes), 'Owned consumer PID reused across roles'
        identities = [copy.deepcopy(self.child_identities[process.pid]) for process in processes]
        for label, process, identity in zip(labels, processes, identities):
            def receipt_ready():
                assert process.poll() is None, 'Owned consumer exited during readiness'
                path = self.evidence / 'logs' / (identity['identity_receipt'] + '.started')
                if not path.exists():
                    return False
                value = json.loads(path.read_text())
                snapshot, backend = value.get('process_snapshot', {}), value.get('backend_identity', {})
                assert (value.get('status') == 'ready' and value.get('pid') == process.pid
                    and value.get('role') == label and snapshot.get('status') == 'available'
                    and identity['process_identity_status'] == 'available'
                    and snapshot.get('pid') == process.pid and snapshot.get('start_time_ticks') == identity['start_time_ticks']
                    and backend.get('application_name') == identity['application_name']
                    and backend.get('database_name') == 'labops_events'
                    and type(backend.get('backend_pid')) is int
                    and value.get('autocommit') is True and value.get('in_atomic_block') is False), 'Worker identity receipt mismatch'
                return value
            remaining = deadline - time.monotonic()
            assert remaining > 0, 'Consumer readiness exhausted its original budget'
            identity['startup_receipt'] = self.wait(receipt_ready, 'Worker startup identity missing: ' + label, remaining)
        remaining = deadline - time.monotonic()
        assert remaining > 0, 'Consumer startup exhausted assignment budget'
        result = self.wait_group_assignment(name, client_ids=[identity['client_id'] for identity in identities],
            message='Owned consumer pool did not obtain exact partitions: ' + name, timeout=remaining)
        from benchmarks.events.process_resources import registered_process_snapshot
        for label, process, identity in zip(labels, processes, identities):
            assert self.workers.get(label) is process and process.pid == identity['pid'], 'Owned consumer generation changed during readiness'
            assert all(self.child_identities[process.pid].get(key) == identity.get(key)
                for key in ('role', 'pid', 'generation', 'start_time_ticks', 'client_id')), 'Owned consumer identity changed during readiness'
            assert process.poll() is None, 'Owned consumer exited during group observation'
            current = registered_process_snapshot(process.pid)
            assert (current['status'] == 'available' and current['pid'] == process.pid
                    and current['start_time_ticks'] == identity['start_time_ticks']), 'Owned consumer start identity changed'
        assert time.monotonic() < deadline, 'Late consumer readiness cannot pass'
        result['owned_processes'] = copy.deepcopy(identities)
        with (self.evidence / 'consumer-pool-readiness.jsonl').open('a') as output:
            output.write(json.dumps(result, sort_keys=True) + '\n')
        return result

    def restore_consumer_pool(self, name, *, timeout=90):
        deadline = time.monotonic() + timeout
        self.start_consumer_pool(name)
        remaining = deadline - time.monotonic()
        assert remaining > 0, 'Consumer pool startup exhausted readiness budget'
        result = self.wait_consumer_pool(name, timeout=remaining)
        assert time.monotonic() < deadline, 'Late restored pool cannot pass original readiness deadline'
        return result

    def wait_consumer_pools(self, *, timeout=120, labels_by_name=None):
        """Both logical groups share the original stage readiness deadline."""
        deadline = time.monotonic() + timeout
        results = {}
        for name in ('notification', 'analytics'):
            remaining = deadline - time.monotonic()
            assert remaining > 0, 'Consumer groups exhausted shared readiness budget'
            labels = labels_by_name[name] if labels_by_name is not None else None
            results[name] = self.wait_consumer_pool(name, timeout=remaining, labels=labels)
            assert time.monotonic() < deadline, 'Late group cannot pass shared readiness deadline'
        return results

    def restore_consumer_pools(self, *, timeout=120):
        deadline = time.monotonic() + timeout
        for name in ('notification', 'analytics'):
            self.start_consumer_pool(name)
        remaining = deadline - time.monotonic()
        assert remaining > 0, 'Consumer groups startup exhausted shared readiness budget'
        result = self.wait_consumer_pools(timeout=remaining)
        assert time.monotonic() < deadline, 'Late restored groups cannot pass original readiness deadline'
        return result

    def ensure_consumer_pool(self, name, *, timeout=90):
        """Replace only missing/exited declared slots, keeping live owners unchanged."""
        deadline = time.monotonic() + timeout
        for label in self.pool_roles(name):
            process = self.workers.get(label)
            if process is None or process.poll() is not None:
                if process is not None:
                    self.stop(label, expected_fault_exit=True)
                suffix = label[len(name):]
                if suffix:
                    self.start_consumer(name, suffix)
                else:
                    self.start_consumer(name)
        remaining = deadline - time.monotonic()
        assert remaining > 0, 'Consumer replacement exhausted recovery budget'
        result = self.wait_consumer_pool(name, timeout=remaining)
        assert time.monotonic() < deadline, 'Late replacement pool cannot pass original recovery deadline'
        return result

    def supervisor_after_postgres_restart(self, reason):
        before = {name: {'pid': process.pid, 'exit_code_before_restart': process.poll()}
                  for name, process in self.workers.items()}
        self.stop('publisher')
        self.stop_consumer_role('notification', expected_fault_exit=True)
        self.stop_consumer_role('analytics', expected_fault_exit=True)
        self.settle_worker_sessions()
        self.start_publisher()
        assignments = {name: self.restore_consumer_pool(name, timeout=90)
            for name in ('notification', 'analytics')}
        evidence = {'reason': reason, 'previous_processes': before,
                    'restarted_processes': {name: {'pid': process.pid} for name, process in self.workers.items()},
                    'stable_assignments_after_restart': assignments,
                    'method': 'explicit validation supervisor creates new process/DB session after PostgreSQL restart'}
        self.supervisor_restarts.append(evidence)
        return evidence

    def stop(self, name, *, kill=False, expected_fault_exit=False):
        process = self.workers.pop(name, None)
        if process and process.poll() is None:
            started = time.monotonic()
            escalated = False
            process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                escalated = True
                process.kill()
                process.wait(timeout=10)
            self.shutdowns.append({'worker': name, 'pid': process.pid,
                'requested_signal': 'SIGKILL' if kill else 'SIGTERM',
                'forced_SIGKILL': escalated, 'exit_code': process.returncode,
                'elapsed_seconds': time.monotonic() - started})
        if process:
            self.observe_worker_close(process, expected_fault_exit=expected_fault_exit,
                                      expected_sigkill=kill)
        self.sync_metrics_targets()

    def observe_worker_close(self, process, *, expected_fault_exit=False, expected_sigkill=False):
        rejected = getattr(self, 'rejected_child_generations', {}).get(id(process))
        if rejected is not None:
            if 'close_outcome' not in rejected:
                outcome = {'pid': process.pid, 'role': rejected['role'], 'generation': rejected['generation'],
                    'exit_code': process.poll(), 'closed_receipt_present': False,
                    'owning_close_receipt_complete': False, 'intentional_SIGKILL': True,
                    'fault_context': False, 'historical_pid_reuse_rejected': True,
                    'all_process_sessions_closed': None}
                rejected['close_outcome'] = outcome
                with (self.evidence / 'worker-close-observations.jsonl').open('a') as output:
                    output.write(json.dumps(outcome, sort_keys=True) + '\n')
            return
        identity = getattr(self, 'child_identities', {}).get(process.pid)
        if not identity or not identity.get('identity_receipt'):
            return
        if not hasattr(self, 'worker_closures'):
            self.worker_closures = {}
        if process.pid in self.worker_closures:
            return
        path = self.evidence / 'logs' / (identity['identity_receipt'] + '.closed')
        value = json.loads(path.read_text()) if path.exists() else None
        snapshot = (value.get('process_snapshot') or {}) if value else {}
        complete = bool(value and value.get('status') == 'closed' and value.get('cleanup_complete') is True
            and value.get('connection_closed') is True and not value.get('errors')
            and value.get('role') == identity['role'] and value.get('pid') == process.pid
            and value.get('expected_application_name') == identity['application_name']
            and snapshot.get('status') == 'available' and snapshot.get('pid') == process.pid
            and snapshot.get('start_time_ticks') == identity['start_time_ticks'])
        fault_context = expected_fault_exit or identity.get('fault_stage', 'normal') != 'normal'
        outcome = {'pid': process.pid, 'role': identity['role'], 'generation': identity['generation'],
            'exit_code': process.poll(), 'closed_receipt_present': value is not None,
            'owning_close_receipt_complete': complete, 'intentional_SIGKILL': expected_sigkill,
            'fault_context': fault_context, 'all_process_sessions_closed': None}
        self.worker_closures[process.pid] = outcome
        with (self.evidence / 'worker-close-observations.jsonl').open('a') as output:
            output.write(json.dumps(outcome, sort_keys=True) + '\n')
        if not expected_sigkill and not fault_context:
            assert process.poll() == 0 and complete, 'Graceful owned worker exit or close receipt failed'

    def cleanup_workers(self):
        """Attempt every exact owned child; a first failure cannot hide slot1."""
        first = None
        for name in list(self.workers):
            try:
                self.stop(name)
            except BaseException as error:
                if first is None:
                    first = error
                self.record_cleanup_error('final_worker_stop', error, name)
        for process in self.children:
            try:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                assert process.poll() is not None, 'Owned child was not reaped'
                self.observe_worker_close(process)
            except BaseException as error:
                if first is None:
                    first = error
                self.record_cleanup_error('final_child_reap', error, pid=process.pid)
        for log in self.logs:
            try:
                log.close()
            except BaseException as error:
                if first is None:
                    first = error
                self.record_cleanup_error('final_log_close', error)
        return first

    def settle_worker_sessions(self, *, timeout=30):
        """Observe absence of only exact authored worker namespaces; never kill sessions."""
        identities = [*getattr(self, 'child_identities', {}).values(),
                      *getattr(self, 'rejected_child_generations', {}).values()]
        names = sorted({row['application_name'] for row in identities
                        if row.get('application_name')})
        if not names:
            return True
        deadline = time.monotonic() + timeout
        observed = None
        try:
            while time.monotonic() < deadline:
                with self.connection.cursor() as cursor:
                    cursor.execute('SELECT pid,application_name FROM pg_stat_activity WHERE application_name = ANY(%s)', [names])
                    observed = [{'backend_pid': pid, 'application_name': name} for pid, name in cursor.fetchall()]
                if not observed and time.monotonic() < deadline:
                    return True
                time.sleep(min(.1, max(0, deadline - time.monotonic())))
            raise AssertionError('Owned validation worker database sessions remain or arrived late')
        finally:
            with (self.evidence / 'worker-session-settlement.jsonl').open('a') as output:
                output.write(json.dumps({'application_names': names, 'remaining_owned_sessions': observed,
                    'complete': observed == [] and time.monotonic() < deadline,
                    'scope': 'exact authored application identities only; no backend termination'}) + '\n')

    def sync_metrics_targets(self):
        """Publish only supervised workers, excluding temporary fault children."""
        path = self.args.generated_dir / 'metrics' / 'targets.json'
        if not path.parent.is_dir():
            return
        targets = [{'targets': [f'127.0.0.1:{self.child_metrics[process.pid]}'],
                    'labels': {'worker': 'notification' if name in self.pool_roles('notification') else name,
                               'owned_role': name, 'owned_pid': str(process.pid)}}
                   for name, process in sorted(self.workers.items()) if process.poll() is None]
        write_json(path, targets)
        path.chmod(0o644)

    def start_consumer(self, name, suffix=''):
        label = name + suffix
        assert label not in self.workers, 'Consumer role already supervised'
        generation = len(self.children)
        process = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                             '--observations', str(self.evidence / 'logs' / f'{label}-{generation}-deliveries.jsonl')], name)
        self.workers[label] = process
        self.sync_metrics_targets()
        return process

    def start_publisher(self):
        argv = ['manage.py', 'publish_events', '--loop', '--limit', '500']
        if getattr(self, 'diagnostic_profile_enabled', False):
            path = self.evidence / 'logs' / f'publisher-profile-{len(self.children):03d}.json'
            self.publisher_profile_paths.append(path)
            argv = [str(HERE / 'profile_publisher.py'), '--output', str(path),
                    '--profile-engine', getattr(self, 'diagnostic_profile_engine', 'cprofile')]
        self.workers['publisher'] = self.spawn('publisher-' + str(len(self.children)),
            argv, 'publisher')
        self.sync_metrics_targets()

    def setup(self):
        parsed = urlparse(self.env['DATABASE_URL'])
        if parsed.hostname not in {'localhost', '127.0.0.1'} or parsed.path != '/labops_events':
            raise ValueError('Harness requires disposable loopback /labops_events database')
        if self.env.get('LABOPS_VALIDATION_PROJECT') != 'labops_events_' + self.args.run_id:
            raise ValueError('Compose project/run-id mismatch')
        if self.settings.KAFKA_GROUP_PREFIX != 'labops.' + self.args.run_id:
            raise ValueError('Group prefix/run-id mismatch')
        if self.settings.KAFKA_SECURITY_PROTOCOL != 'SASL_SSL':
            raise ValueError('Secure acceptance requires SASL_SSL')
        if not all(server.split(':')[0] in {'localhost', '127.0.0.1'}
                   for server in self.settings.KAFKA_BOOTSTRAP_SERVERS.split(',')):
            raise ValueError('Harness broker faults are loopback-only')
        self.call_command('migrate', interactive=False, verbosity=0)
        if self.models.User.objects.exists():
            raise ValueError('Database is not fresh; create a new disposable Compose project')
        self.settings.EVENT_TRANSPORT = 'local'
        self.call_command('seed_demo', verbosity=0)
        self.call_command('rebuild_inventory_projection', verbosity=0)
        self.settings.EVENT_TRANSPORT = 'kafka'
        self.admin = self.models.User.objects.get(email='admin@labops.local')
        self.reviewer = self.models.User.objects.get(email='reviewer@labops.local')
        self.task = self.models.Task.objects.filter(status='IN_PROGRESS', project__status='ACTIVE').first()
        self.source = self.models.Warehouse.objects.get(code='WH-01')
        self.target = self.models.Warehouse.objects.get(code='WH-03')
        self.batch = self.models.Batch.objects.filter(item__is_active=True).order_by('created_at').first()
        self.prepare_order()
        self.baseline = self.snapshot()
        from confluent_kafka.admin import AdminClient
        admin = AdminClient(self.configs['admin'])
        metadata = admin.list_topics(timeout=20)
        topic = metadata.topics[self.settings.KAFKA_TOPIC]
        if len(topic.partitions) != 3 or any(len(p.replicas) != 3 for p in topic.partitions.values()):
            raise AssertionError('Actual inventory topic must have 3 partitions and RF3')
        write_json(self.evidence / 'harness-manifest.json', {
            'run_id': self.args.run_id, 'commit': self.command(['git', 'rev-parse', 'HEAD']).strip(),
            'acceptance_tier': self.args.tier, 'generation_window_tolerance_fraction': .05,
            'generation_execution_profile': json.loads((self.evidence / 'generation-execution-profile.json').read_text()),
            'worktree_dirty': bool(self.command(['git', 'status', '--porcelain']).strip()),
            'python': sys.version, 'host': platform.platform(), 'cpu_count': os.cpu_count(),
            'security_protocol': 'SASL_SSL', 'compose_project': self.env['LABOPS_VALIDATION_PROJECT'],
            'retry_policy': {'seconds': self.settings.EVENT_RETRY_SECONDS,
                'jitter_fraction': self.settings.EVENT_RETRY_JITTER,
                'jitter_range': 'base through base*(1+jitter), deterministic per identity/attempt',
                'maximum_scheduled_delay_seconds': max(self.settings.EVENT_RETRY_SECONDS) * (1 + self.settings.EVENT_RETRY_JITTER),
                'scope': 'explicit disposable validation configuration, frozen before worker startup; production default timing unmeasured'},
            'topic': self.settings.KAFKA_TOPIC,
            'replicas': {str(n): p.replicas for n, p in topic.partitions.items()},
            'client_config_hash': canonical_hash({k: v for k, v in self.env.items()
                if k.startswith(('KAFKA_', 'EVENT_')) and 'PASSWORD' not in k}),
            'fault_scope': 'three broker processes on one Docker host; independent AZ not tested',
            'images': json.loads(self.compose('images', '--format', 'json'))})
        write_json(self.evidence / 'offsets-before.json', self.offsets())

    def prepare_order(self):
        from django.utils import timezone
        from labops.purchasing import services
        from labops.projects import services as projects
        maximum = self.args.events + self.args.fault_events * 4 + self.args.fault_repetitions * 20 + 1000
        quantity = maximum * 4
        self.business_lanes = []
        for lane in range(4):
            rid = self.args.run_id[:36] + '-setup-' + str(lane)
            project = projects.write_project(self.admin, {'code': 'AC-' + self.args.run_id + '-' + str(lane),
                'name': 'Isolated acceptance lane ' + str(lane)}, rid)
            project = projects.project_action(self.admin, project.id, 'transition',
                {'expected_version': project.version, 'target_status': 'ACTIVE'}, rid)
            project = projects.project_action(self.admin, project.id, 'members',
                {'expected_version': project.version, 'user_id': str(self.reviewer.id)}, rid)
            task = projects.write_task(self.admin, {'project_id': str(project.id),
                'title': 'Isolated inventory acceptance', 'assignee_id': str(self.admin.id)}, rid)
            task = projects.task_action(self.admin, task.id, 'transition',
                {'expected_version': task.version, 'target_status': 'IN_PROGRESS'}, rid)
            request = services.write_request(self.admin, {'reason': 'Synthetic isolated event acceptance',
                'project_id': str(project.id),
                'lines': [{'item_id': str(self.batch.item_id), 'qty': quantity,
                           'needed_by': str(timezone.localdate() + timedelta(days=1))}]}, rid)
            request = services.request_action(self.admin, request.id, 'submit',
                {'expected_version': request.version}, rid)
            request = services.request_action(self.reviewer, request.id, 'decision',
                {'expected_version': request.version, 'decision': 'APPROVE', 'reason': 'Isolated acceptance'}, rid)
            order = services.write_order(self.admin, {'supplier_id': str(self.models.Supplier.objects.first().id),
                'lines': [{'request_line_id': str(request.lines.first().id), 'qty': quantity, 'unit_price': '1'}]}, rid)
            order = services.order_action(self.admin, order.id, 'confirm',
                {'expected_version': order.version}, rid)
            self.business_lanes.append({'project': project, 'task': task,
                                        'order': order, 'order_line': order.lines.first(),
                                        'batch': None, 'cycle_issue': None})
        self.order = self.business_lanes[0]['order']
        self.order_line = self.business_lanes[0]['order_line']
        self.cycle_issue = None
        write_json(self.evidence / 'business-lane-topology.json', {
            'lane_count': 4, 'assignment': '(global_index//4)%4', 'position': 'global_index%4',
            'cycle': ['RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL'],
            'shared_durable_journal': True, 'per_lane_journal_batches': True,
            'shared_read_only_context': {'actor_id': str(self.admin.id),
                'source_warehouse_id': str(self.source.id), 'target_warehouse_id': str(self.target.id),
                'item_id': str(self.batch.item_id)},
            'lock_scope': 'normal application locks retained; independent project/task/order/orderline and per-cycle batch/balance',
            'lanes': [{'lane': n, **{key + '_id': str(data[key].id)
                for key in ('project', 'task', 'order', 'order_line')}}
                for n, data in enumerate(self.business_lanes)]})

    def generate(self, count, label, *, rate=None):
        rate = rate or self.args.rate
        if hasattr(self, 'business_lanes') and label in {
                'steady', 'analytics_outage', 'one_broker_stop', 'quorum_loss', 'cluster_outage'}:
            if getattr(self, 'runtime_diagnostics_enabled', False):
                return self._generate_measured(count, label, rate=rate)
            return self._generate_concurrent(count, label, rate=rate)
        batch = self.generation.begin_batch(count, rate, label)
        self._generation_attempt = None
        self._generation_stage = 'initialization'
        try:
            ids, workload = self._generate_commands(count, label, rate=rate, batch=batch)
        except BaseException as exc:
            try:
                self.generation.finish_failure(batch, self._generation_stage, type(exc).__name__,
                    attempt_id=self._generation_attempt)
            except Exception as accounting_error:
                # A filesystem/journal error must not replace the business or
                # observation exception. Final accounting remains incomplete.
                self._generation_accounting_error = type(accounting_error).__name__
            raise
        self.generation.finish_success(batch)
        return ids, workload

    def _generate_measured(self, count, label, *, rate):
        from benchmarks.events.runtime_diagnostics import RuntimeDiagnostics
        from benchmarks.events.container_diagnostics import DEFAULT_SERVICES
        began = time.monotonic()
        planned_start_index = self._next_command_index
        number = len(self.runtime_diagnostics) + 1
        path = self.evidence / f'runtime-diagnostics-{number:03d}.json'
        process_cpu_began = time.process_time()
        # A prior fault may have restarted the same isolated container. Freeze
        # its current identity before this batch; never attribute a reused PID
        # or old process's counters to a replacement instance.
        try:
            resources = (self.runtime_resource_factory() if hasattr(self, 'runtime_resource_factory')
                         else self.container_resources)
            resource_profile = resources.profile()
            write_json(self.evidence / f'runtime-resource-profile-{number:03d}.json', resource_profile)
            frozen_resources = resource_profile['containers']
            stopped_roles = tuple(role for role, row in frozen_resources.items()
                                  if role in DEFAULT_SERVICES and row.get('status') == 'known_stopped')
            process_options = {}
            if self.use_process_generation(count):
                self._active_process_catalog = self.create_process_catalog(label)
                process_options['managed_process_sampler'] = self._active_process_catalog.sample
                process_options['expected_process_profile'] = self._active_process_catalog.profile()
                write_json(self.evidence / f'process-resource-profile-{number:03d}.json',
                           self._active_process_catalog.profile())
            observer = RuntimeDiagnostics(path, scenario=label,
                consumer_topology=getattr(self, 'consumer_topology', DEFAULT_PRESET),
                resource_sampler=resources.snapshot,
                known_stopped_roles=stopped_roles,
                expected_resource_roles=tuple(role for role in DEFAULT_SERVICES if role not in stopped_roles),
                **process_options)
        except BaseException as startup_error:
            self.runtime_diagnostic_errors.append(type(startup_error).__name__)
            self.runtime_diagnostics.append({'scenario': label, 'artifact': path.name,
                'summary': {'collection_complete': False, 'observed': False,
                            'error_type': type(startup_error).__name__}})
            self._record_failed_generation_start(count, label, rate,
                planned_start_index, startup_error)
            raise
        discovery_elapsed = time.monotonic() - began
        topology_count = len(self.generation_topologies)
        original = None
        try:
            with observer:
                try:
                    result = self._generate_concurrent(count, label, rate=rate)
                except BaseException as exc:
                    original = exc
                    raise
        except BaseException as exc:
            if original is None:
                original = exc
            elif exc is not original:
                self.runtime_diagnostic_errors.append(type(exc).__name__)
            if self._next_command_index == planned_start_index:
                self._record_failed_generation_start(count, label, rate,
                    planned_start_index, original)
            raise original
        finally:
            # Stop/join, snapshot persistence and all instrumentation overhead
            # consume the same generation clock, including on partial failure.
            diagnostic_failure = None
            try:
                summary = observer.summary()
            except BaseException as diagnostic_error:
                diagnostic_failure = diagnostic_error
                self.runtime_diagnostic_errors.append(type(diagnostic_error).__name__)
                summary = {'observed': False, 'error_type': type(diagnostic_error).__name__}
            elapsed, process_cpu_elapsed = None, None
            try:
                elapsed = time.monotonic() - began
                process_cpu_elapsed = time.process_time() - process_cpu_began
            except BaseException as diagnostic_error:
                diagnostic_failure = diagnostic_failure or diagnostic_error
                self.runtime_diagnostic_errors.append(type(diagnostic_error).__name__)
            self.runtime_diagnostics.append({'scenario': label,
                'artifact': path.name, 'elapsed_seconds': elapsed,
                'resource_discovery_seconds': discovery_elapsed,
                'whole_generation_process_cpu_seconds': process_cpu_elapsed,
                'summary': summary})
            if len(self.generation_topologies) > topology_count:
                topology = self.generation_topologies[-1]
                if topology['scenario'] == label:
                    topology.update(elapsed_seconds=elapsed,
                        runtime_diagnostics_artifact=path.name,
                        elapsed_includes_diagnostics_cleanup=True,
                        elapsed_includes_diagnostics_artifact_persistence=True,
                        final_topology_metadata_rewrite='after the measured diagnostic completion boundary')
                    measurement_failed = (summary.get('lifecycle_complete') is not True
                        or label == 'steady' and summary.get('collection_complete') is not True)
                    if original is not None or diagnostic_failure is not None or measurement_failed:
                        topology.update(passed=False,
                            error_type=type(original or diagnostic_failure).__name__
                                if original is not None or diagnostic_failure is not None else 'IncompleteRuntimeDiagnostics')
                    try:
                        write_json(self.evidence / f'generation-topology-{len(self.generation_topologies):03d}.json', topology)
                    except BaseException as diagnostic_error:
                        self.runtime_diagnostic_errors.append(type(diagnostic_error).__name__)
                        diagnostic_failure = diagnostic_failure or diagnostic_error
                        topology.update(passed=False, error_type=type(original or diagnostic_failure).__name__)
            if original is None and diagnostic_failure is not None:
                raise diagnostic_failure
        ids, workload = result
        workload.update(elapsed_seconds=elapsed,
            actual_command_rate=count / elapsed if elapsed else None,
            schedule_lateness_seconds=max(0, elapsed - count / rate),
            runtime_diagnostics_artifact=path.name,
            elapsed_boundary='before diagnostic startup/lane batch setup through all worker commits/observations, joined connection/sampler cleanup and diagnostic persistence')
        return ids, workload

    def _record_failed_generation_start(self, count, label, rate, start_index, error):
        """Retain an observer-start failure's entire reserved input denominator."""
        from benchmarks.events.concurrent_generation import allocate_lane_indices
        allocation = allocate_lane_indices(count, start_index=start_index)
        self._next_command_index = start_index + count
        topology = {'scenario': label, 'generator_topology': 'parallel-lanes-v1',
            'requested': count, 'attempted': 0, 'committed': 0, 'identified_events': 0,
            'unattempted': count, 'start_global_index': start_index,
            'global_target_rate': rate, 'nominal_per_lane_average_rate': rate / 4,
            'lane_count': 4, 'cycle_length': 4, 'queue_capacity_per_lane': 4,
            'assignment': '(global_index//4)%4', 'position': 'global_index%4',
            'passed': False, 'stage': 'runtime_diagnostics_startup',
            'error_type': type(error).__name__, 'journal_batches': [], 'batches': []}
        self.generation_topologies.append(topology)
        try:
            for lane, indices in enumerate(allocation):
                batch = self.generation.begin_batch(len(indices), rate / 4, label)
                topology['journal_batches'].append({'lane': lane, 'batch_id': batch,
                    'requested': len(indices)})
                self.generation.finish_failure(batch, 'runtime_diagnostics_startup', type(error).__name__)
                topology['batches'].append(self.generation.batch_summary(batch))
            write_json(self.evidence / f'generation-topology-{len(self.generation_topologies):03d}.json', topology)
        except BaseException as accounting_error:
            self._generation_accounting_error = type(accounting_error).__name__
            self.runtime_diagnostic_errors.append(type(accounting_error).__name__)

    def _generate_commands(self, count, label, *, rate, batch):
        rate = rate or self.args.rate
        started = time.monotonic()
        ids = []
        start_index = getattr(self, '_next_command_index', len(self.events))
        self._next_command_index = start_index + count
        for index in range(count):
            self._generation_attempt = None
            self._generation_stage = 'pacing'
            target = started + index / rate
            while time.monotonic() < target:
                time.sleep(min(.05, target - time.monotonic()))
            seq = start_index + index
            lane = (seq // 4) % 4
            data = (self.business_lanes[lane] if hasattr(self, 'business_lanes') else {
                'order': self.order, 'order_line': self.order_line,
                'batch': getattr(self, 'batch', None), 'cycle_issue': self.cycle_issue})
            state = {'attempt': None, 'stage': 'initialization'}
            try:
                item = self._execute_inventory_command(seq, label, batch, state, data,
                                                       lane=lane, scheduled_at=target)
            finally:
                self._generation_attempt = state['attempt']
                self._generation_stage = state['stage']
                self.batch, self.cycle_issue = data['batch'], data['cycle_issue']
            ids.append(item['event_id'])
        elapsed = time.monotonic() - started
        return ids, {'input': count, 'completed_commands': count, 'elapsed_seconds': elapsed,
                     'target_rate': rate, 'actual_command_rate': count / elapsed if elapsed else None,
                     'schedule_lateness_seconds': max(0, elapsed - count / rate),
                     'generator_topology': 'serial_fault_fixture', 'lane_assignment': '(global_index//4)%4'}

    def _generate_concurrent(self, count, label, *, rate):
        if self.use_process_generation(count):
            return self._generate_processes(count, label, rate=rate)
        return self._generate_concurrent_threads(count, label, rate=rate)

    def use_process_generation(self, count):
        selection = getattr(self, 'process_generation_enabled', False)
        return count >= 512 if selection is None else selection is True

    def _generate_concurrent_threads(self, count, label, *, rate):
        from benchmarks.events.concurrent_generation import allocate_lane_indices, run_paced_lanes
        began = time.monotonic()
        start_index = self._next_command_index
        self._next_command_index += count
        allocation = allocate_lane_indices(count, start_index=start_index)
        batches = [self.generation.begin_batch(len(indices), rate / 4, label) for indices in allocation]
        states = [{'attempt': None, 'stage': 'initialization', 'error_type': None} for _ in range(4)]
        number = len(self.generation_topologies) + 1
        topology = {'scenario': label, 'generator_topology': 'parallel-lanes-v1',
            'requested': count, 'global_target_rate': rate, 'start_global_index': start_index,
            'nominal_per_lane_average_rate': rate / 4,
            'journal_batch_target_rate_scope': 'nominal per-lane average; global pacing is applied once by scheduler',
            'lane_count': 4, 'cycle_length': 4, 'queue_capacity_per_lane': 4,
            'assignment': '(global_index//4)%4', 'position': 'global_index%4',
            'journal_batches': [{'lane': n, 'batch_id': batch, 'requested': len(allocation[n])}
                                for n, batch in enumerate(batches)]}
        self.generation_topologies.append(topology)
        write_json(self.evidence / f'generation-topology-{number:03d}.json', topology)
        observations = self.evidence / f'generation-schedule-{number:03d}.jsonl'

        def observe(value):
            with self._event_lock, observations.open('a') as out:
                out.write(json.dumps(value, sort_keys=True) + '\n')

        def execute(lane, seq, target):
            state = states[lane]
            try:
                return self._execute_inventory_command(seq, label, batches[lane], state,
                    self.business_lanes[lane], lane=lane, scheduled_at=target)
            except BaseException as exc:
                state['error_type'] = type(exc).__name__
                raise

        try:
            # Django connections are thread-local. The shutdown callback runs
            # in its owning lane, after every in-flight transaction has returned.
            items = run_paced_lanes(count, rate, execute, start_index=start_index,
                on_observation=observe, on_lane_shutdown=lambda _lane: self.connections.close_all())
        except BaseException as exc:
            for lane, batch in enumerate(batches):
                try:
                    summary = self.generation.batch_summary(batch)
                    if summary['identified_events'] == summary['requested'] and not states[lane]['error_type']:
                        self.generation.finish_success(batch)
                    else:
                        self.generation.finish_failure(batch,
                            states[lane]['stage'] if states[lane]['error_type'] else 'peer_lane_failure',
                            states[lane]['error_type'] or type(exc).__name__, attempt_id=states[lane]['attempt'])
                except Exception as accounting_error:
                    self._generation_accounting_error = type(accounting_error).__name__
            topology.update({'passed': False, 'error_type': type(exc).__name__,
                             'elapsed_seconds': time.monotonic() - began,
                             'batches': [self.generation.batch_summary(batch) for batch in batches]})
            try:
                write_json(self.evidence / f'generation-topology-{number:03d}.json', topology)
            except Exception as accounting_error:
                self._generation_accounting_error = type(accounting_error).__name__
            raise
        for batch in batches:
            self.generation.finish_success(batch)
        elapsed = time.monotonic() - began
        # Raw event lines retain completion order; the in-memory catalogue keeps
        # globally assigned order for deterministic recovery fixtures.
        self.events.sort(key=lambda item: item['global_index'])
        last = self.business_lanes[((start_index + count - 1) // 4) % 4]
        self.batch, self.cycle_issue = last['batch'], last['cycle_issue']
        topology.update({'passed': True, 'elapsed_seconds': elapsed,
                         'batches': [self.generation.batch_summary(batch) for batch in batches]})
        write_json(self.evidence / f'generation-topology-{number:03d}.json', topology)
        return [item['event_id'] for item in items], {
            'input': count, 'completed_commands': len(items), 'elapsed_seconds': elapsed,
            'target_rate': rate, 'actual_command_rate': count / elapsed if elapsed else None,
            'schedule_lateness_seconds': max(0, elapsed - count / rate),
            'generator_topology': 'parallel-lanes-v1', 'lane_count': 4,
            'queue_capacity_per_lane': 4, 'topology_artifact': f'generation-topology-{number:03d}.json',
            'elapsed_boundary': 'before lane batch setup through all worker commits/observations and joined connection cleanup'}

    def create_process_catalog(self, label):
        from benchmarks.events.process_resources import ProcessResources, GENERATOR_ROLES
        preset = getattr(self, 'consumer_topology', DEFAULT_PRESET)
        stopped = ('analytics',) if label == 'analytics_outage' else ()
        expected = ('publisher', *self.pool_roles('notification'), *self.pool_roles('analytics'))
        required = tuple(name for name in expected if name not in stopped)
        required += tuple(name for name in ('retry', 'dlq') if name in self.workers)
        assert all(name in self.workers for name in required), 'Frozen worker pool member is missing'
        catalog = ProcessResources(required_roles=GENERATOR_ROLES + required,
                                   known_stopped_roles=stopped, consumer_topology=preset)
        for name in required:
            process = self.workers[name]
            if process.poll() is not None:
                raise AssertionError('Managed Kafka worker exited before process workload')
            catalog.register(name, process.pid)
            catalog.ready(name)
        return catalog

    def _generate_processes(self, count, label, *, rate):
        from benchmarks.events.concurrent_generation import allocate_lane_indices
        from benchmarks.events.process_generation import run_paced_processes
        from benchmarks.events.business_commands import inventory_process_worker
        from benchmarks.events.origin_journal import CompositeGenerationJournal
        began = time.monotonic()
        start_index = self._next_command_index
        self._next_command_index += count
        allocation = allocate_lane_indices(count, start_index=start_index)
        number = len(self.generation_topologies) + 1
        path = self.evidence / f'generation-topology-{number:03d}.json'
        schedule_path = self.evidence / f'generation-schedule-{number:03d}.jsonl'
        topology = {'scenario': label, 'generator_topology': 'spawn-lanes-v1',
            'requested': count, 'global_target_rate': rate, 'start_global_index': start_index,
            'lane_count': 4, 'cycle_length': 4, 'queue_capacity_per_lane': 4,
            'result_capacity_total': 16, 'assignment': '(global_index//4)%4',
            'position': 'global_index%4', 'nominal_per_lane_average_rate': rate / 4,
            'origin_plans': [], 'passed': False, 'secondary_errors': [],
            'selection': 'test-only forced' if self.process_generation_enabled is True else 'automatic count>=512'}
        self.generation_topologies.append(topology)
        if not hasattr(self, 'process_batches'):
            self.process_batches = []
        self.process_batches.append(topology)
        plans, items = [], []
        records = {lane: {'lane': lane, 'pid': None, 'ready_metadata': None,
                         'cleanup_metadata': None, 'exitcode': None} for lane in range(4)}
        started_indices = {lane: set() for lane in range(4)}
        original = None
        driver_entered = False

        def secondary(stage, error):
            nonlocal original
            topology['secondary_errors'].append({'stage': stage, 'error_type': type(error).__name__})
            original = original or error
            self._generation_accounting_error = type(error).__name__

        try:
            if not isinstance(self.generation, CompositeGenerationJournal):
                self.generation = CompositeGenerationJournal(self.generation)
            namespace_names = ('EVENT_TRANSPORT', 'KAFKA_TOPIC', 'KAFKA_SOURCE_CLUSTER_ID',
                               'KAFKA_SOURCE_STREAM_GENERATION', 'EVENT_MAX_PAYLOAD_BYTES')
            runtime_settings = {name: getattr(self.settings, name) for name in namespace_names}
            if runtime_settings['EVENT_TRANSPORT'] != 'kafka':
                raise ValueError('Spawn workload requires the caller frozen Kafka transport')
            database_config = copy.deepcopy(self.connection.settings_dict)
            safe_database = {name: str(database_config.get(name, ''))
                             for name in ('ENGINE', 'HOST', 'PORT', 'NAME', 'USER')}
            database_scope_digest = canonical_hash(safe_database)
            for lane, indices in enumerate(allocation):
                data = self.business_lanes[lane]
                context = {'actor_id': str(self.admin.id), 'source_warehouse_id': str(self.source.id),
                    'target_warehouse_id': str(self.target.id), 'task_id': str(data['task'].id),
                    'project_id': str(data['project'].id), 'order_id': str(data['order'].id),
                    'order_line_id': str(data['order_line'].id),
                    'batch_id': str(data['batch'].id) if data['batch'] else None,
                    'cycle_issue_id': str(data['cycle_issue'].id) if data['cycle_issue'] else None,
                    'source_cluster': runtime_settings['KAFKA_SOURCE_CLUSTER_ID'],
                    'source_generation': runtime_settings['KAFKA_SOURCE_STREAM_GENERATION'],
                    'topic': runtime_settings['KAFKA_TOPIC'], 'database_scope_digest': database_scope_digest,
                    'source_context_digest': canonical_hash(runtime_settings)}
                origin_id = f'process_batch_{number:03d}_lane_{lane}'
                application = 'labops.generator.' + hashlib.sha256(
                    (self.args.run_id + ':' + origin_id).encode()).hexdigest()[:32]
                directory = self.evidence / 'origins' / origin_id
                plan = {'origin_id': origin_id, 'run_id': self.args.run_id, 'lane': lane,
                    'label': label, 'indices': list(indices), 'rate': rate / 4,
                    'context': context, 'database_name': database_config['NAME'],
                    'application_name': application, 'directory': str(directory), 'path': str(directory)}
                self.generation.add_origin_plan(plan)
                plans.append(plan)
            topology.update(origin_plans=plans, runtime_namespace=runtime_settings)
            write_json(path, topology)
            write_json(self.evidence / f'process-generation-plan-{number:03d}.json', {
                'scenario': label, 'plans': plans, 'runtime_namespace': runtime_settings,
                'database_scope_digest': database_scope_digest, 'private_database_config_included': False})
            bootstrap = {'plans': plans, 'profile': self.generation.profile, 'rate': rate,
                'database_config': database_config, 'runtime_settings': runtime_settings,
                'runtime_diagnostics_enabled': getattr(self, 'runtime_diagnostics_enabled', False),
                'diagnostic_profile_enabled': getattr(self, 'diagnostic_profile_enabled', False),
                'diagnostic_profile_engine': getattr(self, 'diagnostic_profile_engine', 'cprofile')}
            catalog = getattr(self, '_active_process_catalog', None) if bootstrap['runtime_diagnostics_enabled'] else None

            def started(lane, pid):
                records[lane]['pid'] = pid
                if catalog is not None:
                    catalog.register(f'generator-{lane}', pid)

            def ready(lane, pid, metadata):
                identity = metadata['backend_identity']
                if (identity['database_name'] != database_config['NAME']
                        or identity['application_name'] != plans[lane]['application_name']
                        or metadata['runtime_namespace'] != runtime_settings):
                    raise ValueError('Child runtime handshake differs from frozen caller namespace')
                records[lane]['ready_metadata'] = metadata
                if catalog is not None:
                    catalog.ready(f'generator-{lane}', child_snapshot=metadata['process_snapshot'])

            def observe(value):
                with schedule_path.open('a') as output:
                    output.write(json.dumps(value, sort_keys=True) + '\n')
                lane = value.get('lane')
                if lane in records:
                    if value['kind'] == 'started':
                        started_indices[lane].add(value['global_index'])
                    elif value['kind'] == 'cleanup_complete':
                        records[lane]['cleanup_metadata'] = value.get('metadata')
                        metadata = records[lane]['cleanup_metadata']
                        if (catalog is not None and isinstance(metadata, dict)
                                and metadata.get('connection_closed') is True
                                and metadata.get('process_snapshot') is not None):
                            # A validated child receipt ends its live sampling
                            # span; only the later reaped exit qualifies success.
                            catalog.finalize(f'generator-{lane}',
                                final_snapshot=metadata['process_snapshot'], cleaned=True)
                    elif value['kind'] == 'process_exit':
                        records[lane]['exitcode'] = value.get('exitcode')
                        metadata = records[lane]['cleanup_metadata']
                        if catalog is not None:
                            catalog.finalize(f'generator-{lane}',
                                final_snapshot=metadata.get('process_snapshot') if metadata else None,
                                exitcode=value['exitcode'], cleaned=metadata is not None
                                    and metadata.get('connection_closed') is True)
                    elif value['kind'] == 'completed' and value.get('result') is not None:
                        self.record_generated_event(value['result'])
                if value['kind'] == 'summary':
                    topology['driver_summary'] = value
                for name, process in self.workers.items():
                    if process.poll() is not None:
                        raise AssertionError(f'Managed Kafka worker exited during process generation: {name}')

            driver_entered = True
            items = run_paced_processes(count, rate, inventory_process_worker, bootstrap,
                start_index=start_index, on_observation=observe,
                on_process_started=started, on_process_ready=ready)
        except BaseException as exc:
            original = exc
        finally:
            topology['children'] = list(records.values())
            planned_lanes = {plan['lane'] for plan in plans}
            # A failure while preparing a prefix of plans retains every other
            # requested input as unattempted, rather than losing denominators.
            for lane in set(range(4)) - planned_lanes:
                try:
                    batch = self.generation.begin_batch(len(allocation[lane]), rate / 4, label)
                    self.generation.finish_failure(batch, 'process_plan_startup',
                        type(original).__name__ if original else 'IncompleteProcessPlan')
                except BaseException as error:
                    secondary('startup_denominator', error)
            settled = not driver_entered
            if driver_entered:
                try:
                    settled = self.settle_process_sessions(plans, records)
                    if not settled:
                        raise RuntimeError('Owned child PostgreSQL sessions did not settle')
                except BaseException as error:
                    secondary('session_settlement', error)
            topology['owned_sessions_settled'] = settled
            if original is not None:
                for plan in plans:
                    try:
                        self.generation.note_transport(plan['origin_id'],
                            started_indices=started_indices[plan['lane']], stage='process_transport',
                            error_type=type(original).__name__, execution_permitted=driver_entered)
                        if driver_entered:
                            self.generation.reconcile_origin(plan['origin_id'],
                                settled=settled, query_callback=self.process_database_facts)
                    except BaseException as error:
                        secondary('origin_reconciliation', error)
            try:
                self.merge_origin_events(plans)
                for lane, record in records.items():
                    metadata = record['cleanup_metadata']
                    if metadata and record['exitcode'] == 0:
                        self.restore_process_lane_state(lane, metadata['lane_state'])
            except BaseException as error:
                secondary('origin_closeout', error)
            try:
                topology['batches'] = [batch for batch in self.generation.summary()['batches']
                    if batch.get('origin_id') in {plan['origin_id'] for plan in plans}]
                driver = topology.get('driver_summary', {})
                complete = (driver.get('lifecycle_complete') is True and driver.get('channels_closed') is True
                    and driver.get('worker_processes_joined') is True
                    and driver.get('worker_completion_observed') is True
                    and all(record['exitcode'] == 0 and record['cleanup_metadata']
                        and record['cleanup_metadata'].get('connection_closed') is True
                        for record in records.values())
                    and len(topology['batches']) == 4
                    and all(batch['status'] == 'succeeded'
                        and batch['requested'] == batch['committed'] == batch['identified_events']
                        and not any(batch.get(key, 0) for key in ('commit_unknown', 'attempted_unknown', 'integrity_failed'))
                        for batch in topology['batches']))
                topology.update(passed=original is None and settled and complete,
                    error_type=type(original).__name__ if original else None)
                if not topology['passed'] and original is None:
                    raise RuntimeError('Process lifecycle or original accounting did not qualify')
                write_json(path, topology)
            except BaseException as error:
                secondary('topology_persistence', error)
                topology['passed'] = False
            elapsed = None
            try:
                # Include required final topology persistence in measured time.
                elapsed = time.monotonic() - began
                topology['elapsed_seconds'] = elapsed
                topology['final_elapsed_metadata_rewrite'] = 'after required measured evidence persistence'
                write_json(path, topology)
            except BaseException as error:
                secondary('elapsed_metadata', error)
                topology['passed'] = False
        if original is not None:
            raise original
        self.events.sort(key=lambda item: item['global_index'])
        last = self.business_lanes[((start_index + count - 1) // 4) % 4]
        self.batch, self.cycle_issue = last['batch'], last['cycle_issue']
        return [item['event_id'] for item in items], {'input': count, 'completed_commands': len(items),
            'elapsed_seconds': elapsed, 'target_rate': rate,
            'actual_command_rate': len(items) / elapsed if elapsed else None,
            'schedule_lateness_seconds': max(0, elapsed - count / rate),
            'generator_topology': 'spawn-lanes-v1', 'lane_count': 4, 'queue_capacity_per_lane': 4,
            'topology_artifact': path.name,
            'elapsed_boundary': 'before plan/spawn/init through all commits, origin observations, owning cleanup/reap, session settlement and required evidence'}

    def restore_process_lane_state(self, lane, state):
        data = self.business_lanes[lane]
        batch = self.models.Batch.objects.get(pk=state['batch_id']) if state['batch_id'] else None
        issue = self.models.StockMovement.objects.get(pk=state['cycle_issue_id']) if state['cycle_issue_id'] else None
        if batch is not None and not self.models.ReceiptLine.objects.filter(
                batch=batch, order_line=data['order_line']).exists():
            raise ValueError('Child returned a batch outside its frozen business lane')
        if issue is not None and (issue.type != 'ISSUE' or issue.status != 'POSTED'
                or not issue.lines.filter(batch=batch, task=data['task']).exists()
                or not issue.idempotency_key.startswith(self.args.run_id + ':')):
            raise ValueError('Child returned an issue outside its frozen business lane')
        data['batch'], data['cycle_issue'] = batch, issue

    def merge_origin_events(self, plans):
        known = {item['event_id']: item for item in self.events}
        for plan in plans:
            indices = frozenset(plan['indices'])
            path = Path(plan['directory']) / 'events.jsonl'
            if not path.exists():
                continue
            for line in path.read_text().splitlines():
                item = json.loads(line)
                if item['global_index'] not in indices or item['business_lane'] != plan['lane']:
                    raise ValueError('Origin event is outside its frozen lane reservation')
                if item['event_id'] in known:
                    if known[item['event_id']] != item:
                        raise ValueError('Origin and IPC event observations conflict')
                else:
                    self.record_generated_event(item)
                    known[item['event_id']] = item

    def settle_process_sessions(self, plans, records, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute('SELECT pid,backend_start FROM pg_stat_activity '
                        'WHERE datname = %s AND application_name = ANY(%s)',
                        [self.connection.settings_dict['NAME'], [plan['application_name'] for plan in plans]])
                    remaining = cursor.fetchall()
                if not remaining and time.monotonic() <= deadline:
                    return True
            except Exception:
                return False
            time.sleep(min(.05, max(0, deadline - time.monotonic())))
        return False

    def process_database_facts(self, plan, command):
        """Read exact original ledger/outbox identities after owned sessions settle.

        No broker record, child acknowledgement or reconstructed timestamp can
        manufacture a command. Query failures remain unknown in the composite.
        """
        from labops.event_schema import canonical_payload_hash, validate_inventory_envelope
        context = plan['context']
        namespace = {name: getattr(self.settings, name) for name in (
            'EVENT_TRANSPORT', 'KAFKA_TOPIC', 'KAFKA_SOURCE_CLUSTER_ID',
            'KAFKA_SOURCE_STREAM_GENERATION', 'EVENT_MAX_PAYLOAD_BYTES')}
        safe_database = {name: str(self.connection.settings_dict.get(name, ''))
                         for name in ('ENGINE', 'HOST', 'PORT', 'NAME', 'USER')}
        facts = {'database_observed': True, 'command_key': command['command_key'],
            'session_settled': True,
            'database_scope_matches': canonical_hash(safe_database) == context['database_scope_digest'],
            'source_context_matches': canonical_hash(namespace) == context['source_context_digest'],
            'movement_rows': [], 'outbox_rows': [], 'marker_hashes': []}
        movements = list(self.models.StockMovement.objects.filter(
            idempotency_key=command['command_key']).select_related('receipt', 'reversal_of'))
        for movement in movements:
            lines = list(movement.lines.select_related('batch', 'receipt_line', 'task').order_by('line_no'))
            batch_ids = {line.batch_id for line in lines}
            batch_context = bool(lines) and len(batch_ids) == 1 and self.models.ReceiptLine.objects.filter(
                batch_id=next(iter(batch_ids)), order_line_id=context['order_line_id'],
                receipt__order_id=context['order_id']).exists()
            actor_matches = str(movement.posted_by_id) == context['actor_id']
            line_shape = False
            input_data = None
            if command['kind'] == 'RECEIPT':
                line_shape = (len(lines) == 1 and lines[0].delta_qty == Decimal('4')
                    and str(lines[0].warehouse_id) == context['source_warehouse_id']
                    and movement.receipt_id is not None
                    and str(movement.receipt.order_id) == context['order_id']
                    and lines[0].receipt_line_id is not None
                    and str(lines[0].receipt_line.order_line_id) == context['order_line_id'])
                if movement.receipt_id:
                    input_data = {'expected_version': movement.receipt.version - 1,
                                  'receipt_id': str(movement.receipt_id)}
            elif command['kind'] == 'ISSUE':
                line_shape = (len(lines) == 1 and lines[0].delta_qty == Decimal('-1')
                    and str(lines[0].warehouse_id) == context['source_warehouse_id']
                    and str(lines[0].task_id) == context['task_id'])
                if lines:
                    input_data = {'task_id': context['task_id'], 'lines': [{'batch_id': str(lines[0].batch_id),
                        'warehouse_id': context['source_warehouse_id'], 'qty': '1'}]}
            elif command['kind'] == 'TRANSFER':
                line_shape = (len(lines) == 2 and [(str(line.warehouse_id), line.delta_qty) for line in lines]
                    == [(context['source_warehouse_id'], Decimal('-1')),
                        (context['target_warehouse_id'], Decimal('1'))]
                    and all(line.transfer_pair_no == 1 for line in lines))
                if lines:
                    input_data = {'batch_id': str(lines[0].batch_id),
                        'from_warehouse_id': context['source_warehouse_id'],
                        'to_warehouse_id': context['target_warehouse_id'], 'qty': '1'}
            elif command['kind'] == 'REVERSAL':
                line_shape = (len(lines) == 1 and lines[0].delta_qty == Decimal('1')
                    and str(lines[0].warehouse_id) == context['source_warehouse_id']
                    and str(lines[0].task_id) == context['task_id']
                    and movement.reversal_of_id is not None
                    and movement.reversal_of.type == 'ISSUE'
                    and movement.reversal_of.idempotency_key == f"{plan['run_id']}:{command['global_index'] - 2}"
                    and lines[0].reversal_of_line_id is not None)
                input_data = {'reason': 'Synthetic acceptance reversal'}
            row = {'id': str(movement.id), 'idempotency_key': movement.idempotency_key,
                'status': movement.status, 'type': movement.type, 'actor_id': str(movement.posted_by_id),
                'version': movement.version, 'request_hash': movement.request_hash,
                'context_matches': batch_context and actor_matches and line_shape}
            if input_data is not None:
                row['expected_request_hash'] = self.services.movement_hash(self.admin, command['kind'], input_data)
            facts['movement_rows'].append(row)
            for event in self.models.OutboxEvent.objects.filter(aggregate_id=movement.id):
                value = self.api.raw_envelope(event)
                valid = True
                try:
                    validate_inventory_envelope(value, max_bytes=self.settings.EVENT_MAX_PAYLOAD_BYTES)
                    digest = canonical_payload_hash(value)
                except Exception:
                    valid, digest = False, None
                expected_lines = [{'batch_id': str(line.batch_id), 'warehouse_id': str(line.warehouse_id),
                    'delta_qty': str(line.delta_qty), 'unit_cost': str(line.unit_cost)} for line in lines]
                payload = event.payload_json if isinstance(event.payload_json, dict) else {}
                facts['outbox_rows'].append({'id': str(event.id), 'aggregate_type': event.aggregate_type,
                    'aggregate_id': str(event.aggregate_id), 'aggregate_version': event.aggregate_version,
                    'event_type': event.event_type, 'transport': event.transport,
                    'schema_version': event.schema_version, 'dedupe_key': event.dedupe_key,
                    'payload_hash': event.payload_hash, 'computed_payload_hash': digest, 'schema_valid': valid,
                    'ledger_links_valid': payload.get('movement_id') == str(movement.id)
                        and payload.get('movement_type') == movement.type and payload.get('lines') == expected_lines})
                facts['marker_hashes'].extend(self.models.ProcessedEvent.objects.filter(event_id=event.id)
                    .values_list('payload_hash', flat=True))
        return facts

    def _execute_inventory_command(self, seq, label, batch, state, data, *, lane, scheduled_at):
        if not getattr(self, 'runtime_diagnostics_enabled', False):
            return self._execute_inventory_command_body(seq, label, batch, state, data,
                lane=lane, scheduled_at=scheduled_at)
        from benchmarks.events.runtime_diagnostics import CommandDiagnostics
        observer = CommandDiagnostics(self.connection)
        original = None
        try:
            with observer:
                try:
                    return self._execute_inventory_command_body(seq, label, batch, state, data,
                        lane=lane, scheduled_at=scheduled_at)
                except BaseException as exc:
                    original = exc
                    raise
        except BaseException as exc:
            if original is None:
                original = exc
            elif exc is not original:
                self.runtime_diagnostic_errors.append(type(exc).__name__)
            raise original
        finally:
            try:
                summary = observer.summary()
                row = {'global_index': seq, 'business_lane': lane, 'scenario': label,
                    'kind': ('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL')[seq % 4],
                    **summary}
                with self._event_lock, (self.evidence / 'command-diagnostics.jsonl').open('a') as out:
                    out.write(json.dumps(row, sort_keys=True) + '\n')
                if summary.get('diagnostic_errors') or summary.get('collection_complete') is not True:
                    self.runtime_diagnostic_errors.extend(error.get('error_type', 'CommandDiagnosticError')
                        for error in summary.get('diagnostic_errors', []))
                    if summary.get('collection_complete') is not True:
                        self.runtime_diagnostic_errors.append('IncompleteCommandDiagnostics')
                    if original is None:
                        raise RuntimeError('Command diagnostic restoration or observation failed; inspect command-diagnostics.jsonl')
            except BaseException as diagnostic_error:
                self.runtime_diagnostic_errors.append(type(diagnostic_error).__name__)
                if original is None:
                    raise

    def commit_generated_movement(self, batch, attempt, movement):
        self.generation.commit(batch, attempt, movement_id=str(movement.id))

    def identify_generated_event(self, batch, attempt, event):
        self.generation.identify_event(batch, attempt, str(event.id))

    def record_generated_event(self, item):
        with getattr(self, '_event_lock', threading.Lock()):
            self.events.append(item)
            with (self.evidence / 'events.jsonl').open('a') as out:
                out.write(json.dumps(item, sort_keys=True) + '\n')
        for name, process in self.workers.items():
            if process.poll() is not None:
                raise AssertionError(f'Worker exited during generation: {name} ({process.returncode})')

    def _execute_inventory_command_body(self, seq, label, batch, state, data, *, lane, scheduled_at):
        from benchmarks.events.business_commands import execute_inventory_command
        return execute_inventory_command(self, seq, label, batch, state, data,
                                         lane=lane, scheduled_at=scheduled_at)

    def offsets(self, timeout=30):
        def retry(code, attempt):
            with (self.evidence / 'errors.jsonl').open('a') as out:
                out.write(json.dumps({'kind': 'offset_coordinator_refresh',
                                      'error_code': code, 'attempt': attempt}) + '\n')
        return stable_committed_offsets({name: self.consumer_config(name)
            for name in ('notification', 'analytics')}, self.settings.KAFKA_TOPIC,
            timeout=timeout, on_retry=retry)

    def drained(self, ids, timeout=180):
        expected = len(ids)
        self.wait(lambda: all(self.models.ProcessedEvent.objects.filter(
            consumer_name=name, event_id__in=ids).count() == expected
            for name in ('notification', 'analytics')) and
            self.models.OutboxEvent.objects.filter(id__in=ids, status='PUBLISHED').count() == expected,
            f'{expected} inventory effects did not drain', timeout=timeout)

    def snapshot(self, ids=None):
        from django.db.models import Sum
        ledger = sorted((str(row['batch_id']), str(row['warehouse_id']), str(row['quantity']))
            for row in self.models.StockMovementLine.objects.filter(movement__status='POSTED')
            .values('batch_id', 'warehouse_id').annotate(quantity=Sum('delta_qty')))
        balance = sorted((str(b), str(w), str(q)) for b, w, q in self.models.StockBalance.objects
                         .values_list('batch_id', 'warehouse_id', 'on_hand_qty'))
        projection = sorted((str(b), str(w), str(q)) for b, w, q in self.models.InventoryProjection.objects
                            .values_list('batch_id', 'warehouse_id', 'quantity'))
        notifications = self.models.Notification.objects.all()
        processed = self.models.ProcessedEvent.objects.all()
        if ids is not None:
            notifications = notifications.filter(event_id__in=ids)
            processed = processed.filter(event_id__in=ids)
        notification_rows = sorted((str(e), str(u)) for e, u in notifications.values_list('event_id', 'user_id'))
        dedupe_rows = sorted((str(c), str(e)) for c, e in processed.values_list('consumer_name', 'event_id'))
        active_users = set(str(x) for x in self.models.User.objects.filter(is_active=True).values_list('id', flat=True))
        expected_notifications = None
        if ids is not None:
            expected_notifications = sum(len(set(row.payload_json['recipients']) & active_users)
                for row in self.models.OutboxEvent.objects.filter(id__in=ids))
        def quantities(rows):
            return {(b, w): Decimal(q) for b, w, q in rows}
        ledger_qty, balance_qty, projection_qty = map(quantities, (ledger, balance, projection))
        all_keys = set(ledger_qty) | set(balance_qty) | set(projection_qty)
        mismatches = [{'batch_id': b, 'warehouse_id': w,
            'ledger': str(ledger_qty.get((b, w), 0)),
            'balance': str(balance_qty.get((b, w), 0)),
            'projection': str(projection_qty.get((b, w), 0))}
            for b, w in sorted(all_keys) if len({mapping.get((b, w), Decimal(0))
                                                for mapping in (ledger_qty, balance_qty, projection_qty)}) != 1]
        return {'ledger_rows': len(ledger), 'ledger_hash': canonical_hash(ledger),
                'balance_rows': len(balance), 'balance_hash': canonical_hash(balance),
                'projection_rows': len(projection), 'projection_hash': canonical_hash(projection),
                'mismatches': mismatches, 'notification_count': len(notification_rows),
                'notification_hash': canonical_hash(notification_rows),
                'expected_notification_count': expected_notifications,
                'dedupe_count': len(dedupe_rows), 'dedupe_hash': canonical_hash(dedupe_rows),
                'consumer_counts': dict(Counter(c for c, _ in dedupe_rows)),
                'failed_deliveries': dict(Counter(self.models.FailedDelivery.objects.values_list('status', flat=True)))}

    def duplicate_drill(self, ids):
        before = self.snapshot(ids)
        sender = self.api.producer()
        acknowledgements = []
        cluster, generation = self.api.source_identity()
        write_json(self.evidence / 'duplicate-requests.json', {'event_ids': ids,
            'requested_unique_inventory_ids': len(ids), 'requested_new_broker_records': len(ids) * 2,
            'new_coordinates_required_per_event_per_consumer': 2})
        owner = self

        class ObservingProducer:
            event_id = None

            def __getattr__(self, name):
                return getattr(sender, name)

            @property
            def last_security_error(self):
                return sender.last_security_error

            @last_security_error.setter
            def last_security_error(self, value):
                sender.last_security_error = value

            def produce(self, *args, **kwargs):
                callback = kwargs.get('on_delivery')
                event_id = self.event_id
                def delivered(error, message):
                    if error is None:
                        item = {'event_id': event_id, 'topic': message.topic(),
                            'partition': message.partition(), 'offset': message.offset(),
                            'source_cluster': cluster, 'source_generation': generation}
                        acknowledgements.append(item)
                        with (owner.evidence / 'duplicate-publications.jsonl').open('a') as output:
                            output.write(json.dumps(item, sort_keys=True) + '\n')
                    if callback:
                        callback(error, message)
                kwargs['on_delivery'] = delivered
                return sender.produce(*args, **kwargs)
        observed_sender = ObservingProducer()
        sent = 0
        started = time.monotonic()
        for event in self.models.OutboxEvent.objects.filter(id__in=ids):
            for _ in range(2):
                observed_sender.event_id = str(event.id)
                self.api.send(observed_sender, self.settings.KAFKA_TOPIC,
                              f'{event.aggregate_type}:{event.aggregate_id}', self.api.envelope(event))
                sent += 1
        required = {eid: [] for eid in ids}
        for record in acknowledgements:
            assert record['topic'] == self.settings.KAFKA_TOPIC
            required[record['event_id']].append({'partition': record['partition'], 'offset': record['offset']})
        assert sent == len(ids) * 2 and all(len(parts) == 2 for parts in required.values()), 'Duplicate ACK denominator is incomplete'
        observation_started = time.monotonic()
        coverage = self.wait_for_log_deliveries(ids, minimum=(sent + len(ids)) * 2, timeout=180,
            required_coordinates=required)
        remaining = 180 - (time.monotonic() - observation_started)
        assert remaining > 0, 'Duplicate receipt proof exhausted the frozen observation window'
        cursor_proof = self.wait_for_acknowledged_offsets(required, timeout=remaining)
        from benchmarks.events.health import completed_recovery_seconds
        completed_recovery_seconds(observation_started, 180)
        after = self.snapshot(ids)
        for key in ('notification_count', 'notification_hash', 'dedupe_count', 'dedupe_hash', 'projection_hash'):
            assert before[key] == after[key], f'Duplicate changed {key}'
        self.cases.append({'name': 'duplicates', 'unique_input': len(ids), 'duplicate_broker_records': sent,
            'extra_database_effects': 0, 'before': before, 'after': after,
            'delivery_coverage': coverage,
            'acknowledged_coordinate_commit_coverage': cursor_proof,
            'elapsed_seconds': time.monotonic() - started, 'passed': True})

    def wait_for_log_deliveries(self, ids, minimum, timeout=90, required_coordinates=None):
        from benchmarks.events.delivery_contract import qualify_delivery_observations
        wanted = sorted(set(ids))
        pairs = len(wanted) * 2
        assert pairs and minimum % pairs == 0, 'Delivery denominator is not uniform for all event/consumer pairs'
        required_per_pair = minimum // pairs
        proof = None
        artifact = self.evidence / f'delivery-proof-{len(self.delivery_proofs):03d}.json'

        def observations():
            for path in (self.evidence / 'logs').glob('*-deliveries.jsonl'):
                with path.open() as source:
                    for number, line in enumerate(source, 1):
                        try:
                            row = json.loads(line)
                        except ValueError:
                            continue
                        yield {**row, 'log_file': path.name, 'line_number': number}

        def qualified():
            nonlocal proof
            proof = qualify_delivery_observations(observations(), wanted,
                self.settings.KAFKA_TOPIC, required_per_pair,
                expected_source_identity=self.api.source_identity(),
                required_coordinates=required_coordinates)
            return proof if proof['passed'] else False
        try:
            self.wait(qualified, 'Every inventory event/consumer pair requires distinct broker coordinates', timeout)
        finally:
            write_json(artifact, proof or {'passed': False, 'requested_event_ids': wanted,
                'required_per_pair': required_per_pair, 'expected_pairs': pairs,
                'expected_total_distinct_coordinates': minimum,
                'qualification_error': 'No complete parseable observation proof was obtained'})
            summary = {'artifact': artifact.name, 'passed': bool(proof and proof['passed']),
                'requested_event_count': len(wanted), 'required_per_pair': required_per_pair,
                'expected_pairs': pairs, 'expected_total_distinct_coordinates': minimum,
                'covered_pair_count': proof['covered_pair_count'] if proof else None,
                'qualified_total_distinct_coordinates': proof['qualified_total_distinct_coordinates'] if proof else None}
            self.delivery_proofs.append(summary)
        return summary

    def wait_for_acknowledged_offsets(self, required_coordinates, timeout):
        from benchmarks.events.delivery_contract import acknowledged_offsets_coverage
        from benchmarks.events.health import completed_recovery_seconds
        started = time.monotonic()
        deadline = started + timeout
        while time.monotonic() < deadline:
            snapshot = self.offsets(timeout=min(30, deadline - time.monotonic()))
            proof = acknowledged_offsets_coverage(snapshot, required_coordinates)
            with (self.evidence / 'duplicate-offset-observations.jsonl').open('a') as output:
                output.write(json.dumps({'elapsed_seconds': time.monotonic() - started, **proof}) + '\n')
            if proof['passed']:
                completed_recovery_seconds(started, timeout)
                return proof
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(.15, remaining))
        raise AssertionError('Both consumer groups did not commit beyond every new duplicate ACK coordinate')

    def marker(self, label):
        return self.evidence / 'markers' / f'{label}.json'

    def wait_marker(self, marker, child, timeout=90):
        def ready():
            if marker.exists():
                return json.loads(marker.read_text())
            if child.poll() is not None:
                raise RuntimeError(f'Fault process exited early ({child.returncode})')
            return False
        return self.wait(ready, 'Fault boundary not reached: ' + marker.name, timeout)

    def consumer_crashes(self):
        for name in ('notification', 'analytics'):
            for stage in ('before_commit', 'after_commit'):
                for repetition in range(self.args.fault_repetitions):
                    self.stop_consumer_role(name)
                    ids, _ = self.generate(1, name + '-' + stage)
                    eid = ids[0]
                    label = f'{name}-{stage}-{repetition}'
                    marker = self.marker(label)
                    child = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                        '--stage', stage, '--event', eid, '--marker', str(marker)], name)
                    paused = self.wait_marker(marker, child)
                    topic, partition, offset = paused['delivery_key'].rsplit(':', 3)[-3:]
                    before_offset = self.offsets()[name][partition]
                    assert before_offset <= int(offset), 'Consumer committed before crash boundary'
                    effect_count = self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).count()
                    assert effect_count == (1 if stage == 'after_commit' else 0)
                    child.kill()
                    assert child.wait(timeout=10) == -signal.SIGKILL
                    recovered = self.marker(label + '-recover')
                    child = self.spawn(label + '-recover', [str(HERE / 'workers.py'), 'consumer',
                        '--consumer', name, '--event', eid, '--marker', str(recovered), '--max-messages', '1'], name)
                    replay = self.wait_marker(Path(str(recovered) + '.completed'), child, timeout=90)
                    assert replay['delivery_key'] == paused['delivery_key'], 'Same broker offset was not replayed'
                    self.wait(lambda: self.offsets()[name][partition] == int(offset) + 1,
                              'Recovery did not commit offset')
                    # The target-aware child requests its own graceful stop
                    # immediately after the synchronous target offset ACK.
                    assert child.wait(timeout=30) == 0, 'Target recovery consumer did not close normally'
                    self.observe_worker_close(child)
                    assert self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).count() == 1
                    restoration = self.restore_consumer_pool(name)
                    self.drained(ids)
                    self.cases.append({'name': 'consumer_sigkill', 'consumer': name, 'stage': stage,
                        'repetition': repetition, 'event_id': eid, 'source_replayed': paused['delivery_key'],
                        'offset_before_kill': before_offset, 'offset_after_recovery': int(offset) + 1,
                        'effects_visible_before_kill': effect_count, 'effects_after_recovery': 1, 'passed': True})
                    self.cases[-1]['preset_restoration'] = restoration

    def publisher_crashes(self):
        self.stop('publisher')
        for stage in ('before_send', 'after_ack', 'stale_owner'):
            for repetition in range(self.args.fault_repetitions):
                ids, _ = self.generate(1, 'publisher-' + stage)
                eid = ids[0]
                label = f'publisher-{stage}-{repetition}'
                marker = self.marker(label)
                child = self.spawn(label, [str(HERE / 'workers.py'), 'publisher', '--stage', stage,
                    '--event', eid, '--marker', str(marker)], 'publisher')
                paused = self.wait_marker(marker, child)
                if stage != 'stale_owner':
                    child.kill()
                    assert child.wait(timeout=10) == -signal.SIGKILL
                from django.utils import timezone
                original_claim = self.models.OutboxEvent.objects.get(id=eid)
                fixture_expiry = timezone.now() - timedelta(seconds=1)
                self.models.OutboxEvent.objects.filter(id=eid).update(locked_until=fixture_expiry)
                recovered_marker = self.marker(label + '-recover')
                recovered = self.spawn(label + '-recover', [str(HERE / 'workers.py'), 'publisher',
                    '--event', eid, '--marker', str(recovered_marker)], 'publisher')
                self.wait_marker(Path(str(recovered_marker) + '.completed'), recovered)
                assert recovered.wait(timeout=30) == 0
                row = self.models.OutboxEvent.objects.get(id=eid)
                assert row.status == 'PUBLISHED'
                if stage == 'stale_owner':
                    published_at = row.published_at
                    Path(str(marker) + '.release').touch()
                    self.wait_marker(Path(str(marker) + '.completed'), child)
                    assert child.wait(timeout=30) == 0
                    row.refresh_from_db()
                    assert row.published_at == published_at, 'Expired owner changed publication writeback'
                self.drained(ids)
                self.cases.append({'name': 'publisher_crash', 'stage': stage, 'repetition': repetition,
                    'event_id': eid, 'acknowledged_before_kill': paused.get('acknowledged_records', []),
                    'same_event_id_recovered': True, 'expired_owner_writeback_successes': 0 if stage == 'stale_owner' else None,
                    'lease_expiry_method': 'accelerated disposable fixture timestamp',
                    'original_lease_expiry': original_claim.locked_until,
                    'fixture_lease_expiry': fixture_expiry,
                    'original_lease_token': original_claim.lease_token,
                    'natural_lease_wait_verified': False,
                    'passed': True})
        self.start_publisher()

    def broker_fault(self, services, seconds, label):
        before = self.offsets()
        network_before = self.network_snapshot(label + '-before')
        self.compose('stop', '-t', '0', *services)
        started = time.monotonic()
        try:
            ids, workload = self.generate(self.args.fault_events, label)
            if len(services) >= 2:
                # Force a completed publish budget while quorum is unavailable.
                sender = self.api.producer()
                failed = False
                attempted = {'acknowledgements': [], 'delivery_errors': []}
                original_send = self.api.send

                class ObservingProducer:
                    def __getattr__(self, name):
                        return getattr(sender, name)

                    @property
                    def last_security_error(self):
                        return sender.last_security_error

                    @last_security_error.setter
                    def last_security_error(self, value):
                        sender.last_security_error = value

                    def produce(self, *a, **kw):
                        callback = kw.get('on_delivery')

                        def observed(error, message):
                            if error is None:
                                attempted['acknowledgements'].append({'topic': message.topic(),
                                    'partition': message.partition(), 'offset': message.offset()})
                            else:
                                attempted['delivery_errors'].append(error.name())
                            if callback:
                                callback(error, message)
                        kw['on_delivery'] = observed
                        return sender.produce(*a, **kw)

                def observed_send(client, topic, key, value):
                    attempted['event_id'] = value['event_id']
                    return original_send(client, topic, key, value)
                self.api.send = observed_send
                probe_began = time.monotonic()
                claim_budget = (self.settings.EVENT_LEASE_SECONDS +
                    max(self.settings.EVENT_RETRY_SECONDS[:1] or [0]) +
                    self.settings.KAFKA_PUBLISH_FLUSH_SECONDS + 15)
                no_candidate_polls = 0
                try:
                    while time.monotonic() - probe_began < claim_budget:
                        try:
                            self.api.publish_one(ObservingProducer())
                        except Exception as exc:
                            # A claim/pre-send error cannot establish a broker
                            # denial. Only the captured actual send is eligible.
                            if not attempted.get('event_id'):
                                raise
                            failed = True
                            publish_error = type(exc).__name__
                            break
                        if attempted.get('event_id'):
                            break
                        no_candidate_polls += 1
                        time.sleep(.15)
                finally:
                    self.api.send = original_send
                attempted['claim_probe_seconds'] = time.monotonic() - probe_began
                attempted['no_candidate_polls'] = no_candidate_polls
                write_json(self.evidence / (label + '-publication-probe.json'), attempted)
                assert attempted.get('event_id'), 'No naturally eligible outbox was attempted within the bounded quorum probe'
                assert failed, 'Broker falsely acknowledged publication without quorum'
                assert not attempted['acknowledgements'], 'Broker record acknowledged without quorum'
                assert attempted.get('event_id') in ids, 'Quorum failure attempted a different workload event'
                candidate = self.models.OutboxEvent.objects.get(id=attempted['event_id'])
                candidate.refresh_from_db()
                assert candidate.status != 'PUBLISHED', 'Unacknowledged outbox marked published'
                assert self.models.StockMovement.objects.filter(id=candidate.aggregate_id, status='POSTED').exists()
            else:
                publish_error = None
                attempted = {}
            remaining = seconds - (time.monotonic() - started)
            while remaining > 0:
                time.sleep(min(.25, remaining))
                remaining = seconds - (time.monotonic() - started)
            down_snapshot = self.snapshot(ids)
            down_rows = self.outbox_retry_evidence(ids)
            write_json(self.evidence / (label + '-outbox-during-outage.json'), down_rows)
            downtime = time.monotonic() - started
        finally:
            self.compose('start', *services)
        recovery = time.monotonic()
        from benchmarks.events.workload_contract import qualify_fault_workload
        qualification = qualify_fault_workload(workload, tier=self.args.tier,
            requested_events=self.args.fault_events, configured_rate=self.args.rate,
            actual_input_count=len(ids), requested_min_outage_seconds=seconds,
            measured_outage_seconds=downtime)
        write_json(self.evidence / (label + '-workload-qualification.json'), qualification)
        # The frozen drain window begins when stopped processes return, so
        # network/cluster recovery cannot quietly extend the 900-second SLA.
        completed_seconds = None
        try:
            network_after = self.network_snapshot(label + '-after')
            from network_identity import compare_broker_networks
            network_proof = compare_broker_networks(network_before, network_after,
                self.env['LABOPS_VALIDATION_IPV4_PREFIX'])
            self.wait_brokers()
            health_recovery_seconds = time.monotonic() - recovery
            self.connections.close_all()
            if 'publisher' not in self.workers:
                self.start_publisher()
            remaining = self.args.drain_timeout - (time.monotonic() - recovery)
            assert remaining > 0, 'Broker health recovery exhausted the frozen drain window'
            for name in ('notification', 'analytics'):
                self.ensure_consumer_pool(name, timeout=remaining)
                remaining = self.args.drain_timeout - (time.monotonic() - recovery)
                assert remaining > 0, 'Broker consumer assignment exhausted the frozen drain window'
            self.drained(ids, timeout=remaining)
            from benchmarks.events.health import completed_recovery_seconds
            completed_seconds = completed_recovery_seconds(recovery, self.args.drain_timeout)
        finally:
            recovered_rows = self.outbox_retry_evidence(ids)
            write_json(self.evidence / (label + '-outbox-after-recovery.json'), recovered_rows)
            write_json(self.evidence / (label + '-recovery-window.json'), {
                'budget_seconds': self.args.drain_timeout,
                'observation_elapsed_seconds': time.monotonic() - recovery,
                'window_start': 'broker compose start completed',
                'successful_completion_elapsed_seconds': completed_seconds,
                'successful_completion_within_budget': completed_seconds is not None})
            dead = [row['id'] for row in recovered_rows if row['status'] == 'DEAD']
            if dead:
                with (self.evidence / 'errors.jsonl').open('a') as out:
                    out.write(json.dumps({'kind': 'automatic_broker_recovery_exhausted',
                        'scenario': label, 'dead_event_ids': dead, 'operator_requeued_events': 0}) + '\n')
        self.cases.append({'name': label, 'services': services, 'downtime_seconds': downtime,
            'requested_minimum_outage_seconds': seconds,
            'input': len(ids), 'workload': workload, 'workload_qualification': qualification,
            'publish_denial_error_type': publish_error,
            'attempted_publish_event_id': attempted.get('event_id'),
            'attempted_publish_ack_count': len(attempted.get('acknowledgements', [])) if len(services) >= 2 else None,
            'attempted_publish_delivery_errors': attempted.get('delivery_errors', []),
            'claim_probe_seconds': attempted.get('claim_probe_seconds'),
            'no_candidate_polls': attempted.get('no_candidate_polls'),
            'database_transactions_committed': len(ids), 'down_snapshot': down_snapshot,
            'recovery_method': 'automatic persisted retry schedules and natural lease expiry',
            'operator_requeued_events': 0, 'publishers_killed_by_broker_drill': 0,
            'stable_broker_network': network_proof,
            'broker_health_recovery_seconds': health_recovery_seconds,
            'recovery_window_seconds': self.args.drain_timeout,
            'recovery_window_start': 'broker compose start completed; includes network/cluster health recovery',
            'recovery_drain_seconds': completed_seconds, 'offsets_before': before,
            'offsets_after': self.offsets(), 'passed': qualification['passed']})
        if not qualification['passed']:
            raise AssertionError('Broker fault workload missed the frozen generation/count/outage contract')

    def network_snapshot(self, stage):
        self.command([sys.executable, 'infra/events/validation/collect.py',
            '--env-file', str(self.args.generated_dir / 'client.env'),
            '--run-id', self.args.run_id, '--evidence-dir', str(self.evidence),
            '--network-snapshot-only', '--network-stage', stage])
        return json.loads((self.evidence / ('network-identity-' + stage + '.json')).read_text())

    def outbox_retry_evidence(self, ids):
        return [{**{key: value for key, value in row.items() if key != 'lease_token'},
                 'id': str(row['id']), 'lease_token_present': row['lease_token'] is not None}
                for row in self.models.OutboxEvent.objects.filter(id__in=ids).order_by('created_at', 'id').values(
                    'id', 'status', 'attempts', 'next_attempt_at', 'locked_until', 'lease_token', 'published_at')]

    def wait_brokers(self):
        from confluent_kafka.admin import AdminClient
        import requests
        from benchmarks.events.health import wait_broker_recovery
        from labops.worker_metrics import operation_deadline, OperationDeadlineExceeded
        # A metadata broker count can advertise three nodes while Raft remains
        # leaderless or a surviving node contacts another node's stale address.
        # Retain every local health view and the actual topic leaders/ISR.
        index = len(list(self.evidence.glob('broker-recovery-*-result.json')))
        prefix = self.evidence / f'broker-recovery-{index:02d}'
        last = {}
        session = requests.Session()
        session.auth = (self.configs['admin']['sasl.username'], self.configs['admin']['sasl.password'])
        session.verify = self.configs['ca_path']
        session.trust_env = False

        def observed(snapshot):
            last.clear()
            last.update(snapshot)
            with prefix.with_name(prefix.name + '-observations.jsonl').open('a') as out:
                out.write(json.dumps(snapshot, sort_keys=True) + '\n')

        def health(node, remaining):
            try:
                with operation_deadline(remaining):
                    response = session.get(f'https://127.0.0.1:{19644 + node * 10000}/v1/cluster/health_overview',
                        timeout=min(4, remaining / 2), allow_redirects=False)
                    if response.status_code != 200:
                        raise RuntimeError('Broker health HTTP status is not 200')
                    return response.json()
            except OperationDeadlineExceeded:
                raise TimeoutError('Broker health request exhausted recovery budget') from None

        def metadata(remaining):
            try:
                with operation_deadline(remaining):
                    value = AdminClient(self.configs['admin']).list_topics(timeout=min(4, remaining))
                    topics = {}
                    for name in (self.settings.KAFKA_TOPIC, self.settings.KAFKA_DLQ_TOPIC):
                        topic = value.topics.get(name)
                        if topic is None:
                            continue
                        topics[name] = {'error': topic.error.name() if topic.error else None,
                            'partitions': [{'partition': number, 'leader': part.leader,
                                'replicas': list(part.replicas), 'isr': list(part.isrs),
                                'error': part.error.name() if part.error else None}
                                for number, part in sorted(topic.partitions.items())]}
                    return {'brokers': sorted(value.brokers), 'topics': topics}
            except OperationDeadlineExceeded:
                raise TimeoutError('Kafka metadata request exhausted recovery budget') from None

        try:
            result = wait_broker_recovery(health, metadata,
                (self.settings.KAFKA_TOPIC, self.settings.KAFKA_DLQ_TOPIC),
                timeout=180, on_observation=observed)
        except Exception as exc:
            write_json(prefix.with_name(prefix.name + '-result.json'),
                {'status': 'FAILED', 'timeout_seconds': 180, 'error_type': type(exc).__name__,
                 'last_observation': last})
            raise
        else:
            write_json(prefix.with_name(prefix.name + '-result.json'),
                {'status': 'READY', 'timeout_seconds': 180, 'last_observation': result})
        finally:
            session.close()

    def analytics_outage(self):
        self.stop_consumer_role('analytics')
        started = time.monotonic()
        ids, workload = self.generate(self.args.fault_events, 'analytics_outage')
        self.wait(lambda: self.models.ProcessedEvent.objects.filter(consumer_name='notification',
            event_id__in=ids).count() == len(ids), 'Independent notification consumer stalled')
        parked = self.models.ProcessedEvent.objects.filter(consumer_name='analytics', event_id__in=ids).count()
        assert parked == 0
        while time.monotonic() - started < self.args.consumer_outage_seconds:
            time.sleep(.2)
        downtime = time.monotonic() - started
        from benchmarks.events.workload_contract import qualify_fault_workload
        qualification = qualify_fault_workload(workload, tier=self.args.tier,
            requested_events=self.args.fault_events, configured_rate=self.args.rate,
            actual_input_count=len(ids), requested_min_outage_seconds=self.args.consumer_outage_seconds,
            measured_outage_seconds=downtime)
        write_json(self.evidence / 'analytics_outage-workload-qualification.json', qualification)
        # Process startup belongs to the same frozen recovery deadline.
        recovered = time.monotonic()
        completed_seconds = None
        try:
            self.start_consumer_pool('analytics')
            remaining = self.args.drain_timeout - (time.monotonic() - recovered)
            assert remaining > 0, 'Analytics process startup exhausted the frozen recovery window'
            self.wait_consumer_pool('analytics', timeout=remaining)
            remaining = self.args.drain_timeout - (time.monotonic() - recovered)
            assert remaining > 0, 'Analytics assignment exhausted the frozen recovery window'
            self.ensure_consumer_pool('notification', timeout=remaining)
            remaining = self.args.drain_timeout - (time.monotonic() - recovered)
            assert remaining > 0, 'Notification assignment exhausted the frozen recovery window'
            self.drained(ids, timeout=remaining)
            from benchmarks.events.health import completed_recovery_seconds
            completed_seconds = completed_recovery_seconds(recovered, self.args.drain_timeout)
        finally:
            write_json(self.evidence / 'analytics_outage-recovery-window.json', {
                'budget_seconds': self.args.drain_timeout,
                'observation_elapsed_seconds': time.monotonic() - recovered,
                'window_start': 'analytics replacement start requested; includes startup and effect drain',
                'successful_completion_elapsed_seconds': completed_seconds,
                'successful_completion_within_budget': completed_seconds is not None})
        self.cases.append({'name': 'analytics_outage', 'input': len(ids), 'workload': workload,
            'workload_qualification': qualification, 'downtime_seconds': downtime,
            'requested_minimum_outage_seconds': self.args.consumer_outage_seconds,
            'notification_completed_while_analytics_down': len(ids), 'analytics_effects_while_down': 0,
            'catch_up_seconds': completed_seconds, 'recovery_window_seconds': self.args.drain_timeout,
            'recovery_window_start': 'analytics replacement start requested; includes startup and effect drain',
            'passed': qualification['passed']})
        if not qualification['passed']:
            raise AssertionError('Analytics outage workload missed the frozen generation/count/outage contract')

    def postgres_failure(self):
        self.stop('publisher')
        self.stop_consumer_role('notification')
        self.stop_consumer_role('analytics')
        before = self.snapshot()
        movement_count = self.models.StockMovement.objects.count()
        marker = self.marker('postgres-business')
        data = self.evidence / 'markers' / 'postgres-business-data.json'
        write_json(data, {'user_id': str(self.admin.id), 'key': self.args.run_id + ':pg-rollback',
            'request_id': 'pg-rollback', 'command': {'task_id': str(self.task.id), 'lines': [{
                'batch_id': str(self.batch.id), 'warehouse_id': str(self.source.id), 'qty': '0.1'}]}})
        child = self.spawn('postgres-business', [str(HERE / 'workers.py'), 'business',
            '--data-file', str(data), '--marker', str(marker)], 'publisher')
        paused = self.wait_marker(marker, child)
        assert self.models.StockMovement.objects.count() == movement_count, 'Uncommitted ledger is visible'
        self.compose('stop', '-t', '0', 'postgres')
        try:
            Path(str(marker) + '.release').touch()
            error = self.wait_marker(Path(str(marker) + '.error'), child, timeout=30)
            child.wait(timeout=20)
        finally:
            self.compose('start', 'postgres')
        self.connections.close_all()
        self.wait(lambda: self.models.User.objects.count() > 0, 'PostgreSQL did not recover')
        assert self.models.StockMovement.objects.count() == movement_count
        assert not self.models.StockMovement.objects.filter(id=paused['movement_id']).exists()
        assert not self.models.OutboxEvent.objects.filter(id=paused['event_id']).exists()
        after = self.snapshot()
        for key in ('ledger_hash', 'balance_hash', 'projection_hash', 'dedupe_hash', 'notification_hash'):
            assert before[key] == after[key], 'Business transaction rollback changed ' + key
        self.cases.append({'name': 'postgres_business_commit', 'attempted_commands': 1,
            'committed_commands': 0, 'partial_ledger_or_outbox': 0, 'error_type': error['error_type'], 'passed': True})
        business_supervision = self.supervisor_after_postgres_restart('business transaction fault')
        self.cases[-1]['supervisor_restart'] = business_supervision
        for name in ('notification', 'analytics'):
            for boundary in ('effect_commit', 'failed_delivery_persist'):
                self.stop('publisher')
                self.stop_consumer_role('notification')
                self.stop_consumer_role('analytics')
                label = 'postgres_' + boundary + '_' + name
                marker = self.marker(label)
                target_file = self.evidence / 'markers' / (label + '.target.json')
                stage = 'postgres_effect_commit' if boundary == 'effect_commit' else 'before_delivery'
                child = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                    '--stage', stage, '--event-file', str(target_file), '--marker', str(marker),
                    '--observations', str(self.evidence / 'logs' / (label + '-deliveries.jsonl'))], name)
                assignment = self.wait_group_assignment(name, client_id='acceptance-' + str(child.pid),
                    message='Fault process did not exclusively own all group partitions', timeout=90)
                if boundary == 'effect_commit':
                    ids, _ = self.generate(1, label)
                    eid = ids[0]
                    write_json(target_file, {'event_id': eid})
                    self.start_publisher()
                else:
                    ids = []
                    eid = str(uuid.uuid4())
                    write_json(target_file, {'event_id': eid})
                    poison = {'event_id': eid, 'schema_version': 999, 'payload': {}}
                    self.api.send(self.api.producer(), self.settings.KAFKA_TOPIC, label, poison)
                paused = self.wait_marker(marker, child)
                _, partition, offset = paused['delivery_key'].rsplit(':', 3)[-3:]
                before_offset = self.offsets()[name][partition]
                published = None
                if ids:
                    self.wait(lambda: self.models.OutboxEvent.objects.filter(id=eid, status='PUBLISHED').exists(),
                              'Target outbox did not reach actual broker acknowledgement', timeout=30)
                    published = self.models.OutboxEvent.objects.get(id=eid)
                self.compose('stop', '-t', '0', 'postgres')
                try:
                    Path(str(marker) + '.release').touch()
                    error = self.wait_marker(Path(str(marker) + '.error'), child, timeout=30)
                    after_offset = self.offsets()[name][partition]
                    assert after_offset == before_offset and after_offset <= int(offset), 'Offset advanced without durable outcome'
                    if child.poll() is None:
                        child.kill()
                    child.wait(timeout=10)
                finally:
                    self.compose('start', 'postgres')
                self.connections.close_all()
                self.wait(lambda: self.models.User.objects.count() > 0, 'PostgreSQL did not recover')
                assert not self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).exists()
                assert not self.models.FailedDelivery.objects.filter(consumer_name=name, delivery_key=paused['delivery_key']).exists()
                supervision = self.supervisor_after_postgres_restart(label)
                if ids:
                    self.drained(ids)
                else:
                    self.wait(lambda: self.models.FailedDelivery.objects.filter(consumer_name=name,
                        delivery_key=paused['delivery_key']).exists(), 'Failure not persisted on redelivery')
                self.cases.append({'name': label, 'source': paused['delivery_key'],
                    'offset_before': before_offset, 'offset_while_database_down': after_offset,
                    'failure_type': error['error_type'], 'partial_effect_after_restart': 0,
                    'durable_outcome_after_recovery': True, 'exclusive_fault_assignment_before_input': assignment,
                    'target_event_id': eid, 'outbox_status_before_database_stop': published.status if published else None,
                    'supervisor_restart': supervision, 'passed': True})

    def poison_drill(self):
        sender = self.api.producer()
        before = self.models.FailedDelivery.objects.count()
        candidates = []
        # Include raw invalid JSON, unknown schema/type, decimal overflow/nonfinite,
        # malformed IDs, missing fields, oversized application payload below broker max.
        template = self.api.envelope(self.models.OutboxEvent.objects.filter(transport='kafka').first())
        for index in range(self.args.poison_events):
            candidate = json.loads(json.dumps(template))
            candidate['event_id'] = str(uuid.uuid4())
            case = index % 12
            if case == 0: value = b'{invalid json'
            elif case == 1:
                candidate['schema_version'] = 999; value = json.dumps(candidate).encode()
            elif case == 2:
                candidate['event_type'] = 'unknown'; value = json.dumps(candidate).encode()
            elif case == 3:
                candidate['payload']['lines'][0]['delta_qty'] = 'NaN'; value = json.dumps(candidate).encode()
            elif case == 4:
                candidate['payload']['lines'][0]['delta_qty'] = '0.0000001'; value = json.dumps(candidate).encode()
            elif case == 5:
                candidate['aggregate_id'] = 'not-uuid'; value = json.dumps(candidate).encode()
            elif case == 6:
                candidate.pop('payload'); value = json.dumps(candidate).encode()
            elif case == 7:
                candidate['occurred_at'] = 'not-time'; value = json.dumps(candidate).encode()
            elif case == 8:
                candidate['payload']['body'] = 'X' * (self.settings.EVENT_MAX_PAYLOAD_BYTES + 100); value = json.dumps(candidate).encode()
            elif case == 9:
                candidate['aggregate_version'] = True; value = json.dumps(candidate).encode()
            elif case == 10:
                value = b'X' * 1000000
            else:
                candidate['payload']['body'] = 'valid JSON containing a NUL: \x00'; value = json.dumps(candidate).encode()
            outcomes = []
            sender.produce(self.settings.KAFKA_TOPIC, key=f'poison:{index}', value=value,
                           on_delivery=lambda error, msg: outcomes.append(error))
            assert sender.flush(15) == 0 and outcomes == [None], 'Poison not acknowledged by broker'
            candidates.append({'index': index, 'case': case, 'bytes': len(value),
                               'raw_sha256': hashlib.sha256(value).hexdigest()})
        self.wait(lambda: self.models.FailedDelivery.objects.count() >= before + len(candidates) * 2,
                  'Poison messages were not isolated for both consumers', timeout=180)
        ids, _ = self.generate(4, 'healthy_after_poison')
        self.drained(ids)
        failures = self.models.FailedDelivery.objects.order_by('-created_at')[:len(candidates) * 2]
        persisted = len(failures)
        self.cases.append({'name': 'poison', 'input': len(candidates), 'consumer_input_denominator': len(candidates) * 2,
            'persisted_failed_deliveries': persisted, 'healthy_after_poison': len(ids), 'variants': candidates,
            'status_counts': dict(Counter(x.status for x in failures)), 'passed': True})
        pending_dlq = self.models.FailedDelivery.objects.filter(status='DEAD', dlq_published_at__isnull=True).count()
        child = self.spawn('dlq-independent', ['manage.py', 'publish_dlq', '--limit', str(pending_dlq + 1)], 'dlq')
        assert child.wait(timeout=max(180, pending_dlq * 2)) == 0, 'Independent DLQ worker failed'
        assert not self.models.FailedDelivery.objects.filter(status='DEAD', dlq_published_at__isnull=True).exists()
        from confluent_kafka import Consumer
        # Inspect the DLQ mirror using the disposable infrastructure administrator;
        # the replay service remains restricted to its declared inventory topic.
        config = {**self.configs['admin'], 'group.id': self.settings.KAFKA_GROUP_PREFIX + '.replay.acceptance.v1',
                  'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
                  'enable.auto.offset.store': False}
        reader = Consumer(config)
        expected = {str(row.id): row for row in self.models.FailedDelivery.objects.filter(status='DEAD')}
        seen = set()
        deadline = time.monotonic() + 180
        try:
            reader.subscribe([self.settings.KAFKA_DLQ_TOPIC])
            while time.monotonic() < deadline and set(expected) != seen:
                message = reader.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise AssertionError('DLQ read failed: ' + message.error().name())
                value = json.loads(message.value())
                delivery = expected[value['delivery_id']]
                assert value['original_hash'] == delivery.original_hash
                assert self.api.canonical_payload_hash(value['event']) == delivery.original_hash
                seen.add(value['delivery_id'])
            assert set(expected) == seen, 'Stable DLQ records missing'
        finally:
            reader.close()
        self.cases.append({'name': 'independent_dlq', 'persisted_dead_denominator': pending_dlq,
                           'acknowledged_dlq_mirrors': pending_dlq,
                           'read_stable_delivery_ids': len(seen), 'payload_hashes_verified': len(seen), 'passed': True})

    def retry_drill(self):
        from django.utils import timezone
        for name in ('notification', 'analytics'):
            self.stop_consumer_role(name)
            ids, _ = self.generate(1, 'transient_retry_' + name)
            eid = ids[0]
            label = 'transient-' + name
            marker = self.marker(label)
            child = self.spawn(label, [str(HERE / 'workers.py'), 'consumer', '--consumer', name,
                '--stage', 'transient_failure', '--event', eid, '--marker', str(marker),
                '--max-messages', '1'], name)
            self.wait_marker(Path(str(marker) + '.completed'), child)
            assert child.wait(timeout=30) == 0
            row = self.models.FailedDelivery.objects.get(consumer_name=name, envelope__event_id=eid)
            assert row.status == 'RETRY'
            original_hash, original_envelope = row.original_hash, row.envelope
            assert not self.models.ProcessedEvent.objects.filter(consumer_name=name, event_id=eid).exists()
            original_due = row.next_attempt_at
            fixture_due = timezone.now()
            self.models.FailedDelivery.objects.filter(id=row.id).update(next_attempt_at=fixture_due)
            restoration = self.restore_consumer_pool(name)
            healthy, _ = self.generate(4, 'healthy_while_retry_' + name)
            self.drained(healthy)
            retry = self.spawn('retry-independent-' + name, ['manage.py', 'retry_events', '--limit', '100'], 'replay')
            assert retry.wait(timeout=60) == 0
            row.refresh_from_db()
            assert row.status == 'RESOLVED' and row.original_hash == original_hash and row.envelope == original_envelope
            self.drained(ids)
            audit = self.models.DeliveryAudit.objects.filter(delivery_id=row.id).values_list('action', flat=True)
            assert 'PARK' in audit and 'RETRY' in audit
            self.cases.append({'name': 'durable_retry', 'consumer': name, 'event_id': eid,
                'delivery_id': str(row.id), 'original_hash': original_hash, 'healthy_while_parked': len(healthy),
                'retry_due_method': 'accelerated disposable fixture timestamp',
                'original_next_attempt_at': original_due, 'fixture_next_attempt_at': fixture_due,
                'natural_retry_wait_verified': False,
                'audit_actions': list(audit), 'resolved_same_event_id': True, 'passed': True})
            self.cases[-1]['preset_restoration'] = restoration

    def rebalance(self):
        for repetition in range(self.args.fault_repetitions):
            for name in ('notification', 'analytics'):
                self.stop_consumer_role(name)
                self.start_consumer(name)
            before = self.wait_consumer_pools(timeout=120,
                labels_by_name={name: [name] for name in ('notification', 'analytics')})
            extras = []
            for name in ('notification', 'analytics'):
                for n in (2, 3):
                    label = name + f'-scale-{n}-{repetition}'
                    self.start_consumer(name, label[len(name):])
                    extras.append(label)
            scaled = self.wait_consumer_pools(timeout=120, labels_by_name={name:
                [name] + [label for label in extras if label.startswith(name + '-')]
                for name in ('notification', 'analytics')})
            ids, _ = self.generate(8, 'rebalance_1_3_1')
            for n, label in enumerate(extras):
                self.stop(label, kill=bool(n % 2))
            restored = self.wait_consumer_pools(timeout=120,
                labels_by_name={name: [name] for name in ('notification', 'analytics')})
            self.drained(ids)
            shutdowns = self.shutdowns[-4:]
            for name in ('notification', 'analytics'):
                self.stop_consumer_role(name)
            selected = self.restore_consumer_pools(timeout=120)
            self.cases.append({'name': 'rebalance_1_3_1', 'repetition': repetition,
                'input': len(ids), 'membership_before': before, 'membership_scaled': scaled,
                'membership_after': restored, 'shutdowns': shutdowns,
                'selected_preset_restoration': selected, 'passed': True})

    def restore(self):
        self.stop('publisher')
        self.stop_consumer_role('notification')
        self.stop_consumer_role('analytics')
        baseline = self.snapshot([item['event_id'] for item in self.events])
        watermark = {'created_events': len(self.events), 'last_event_id': self.events[-1]['event_id'],
                     'snapshot_at': time.time(), 'offsets': self.offsets()}
        name = 'restore_' + re.sub('[^a-z0-9_]', '_', self.args.run_id.lower())[:40]
        dump = self.compose('exec', '-T', 'postgres', 'pg_dump', '-U', 'labops', '-Fc',
                            '-d', 'labops_events', binary=True, timeout=180)
        backup = self.evidence / 'backup' / 'postgres.dump'
        backup.write_bytes(dump)
        backup.chmod(0o600)
        import psycopg
        from psycopg import sql
        parsed = urlparse(self.env['DATABASE_URL'])
        admin_url = urlunparse(parsed._replace(path='/postgres'))
        restored_url = urlunparse(parsed._replace(path='/' + name))
        tables = ('stockmovement', 'stockmovementline', 'stockbalance', 'inventoryprojection',
                  'outboxevent', 'processedevent', 'notification', 'faileddelivery', 'deliveryaudit')

        def fingerprints(url):
            result = {}
            with psycopg.connect(url) as database:
                for table in tables:
                    rows = database.execute(sql.SQL('SELECT row_to_json(r) FROM {} r ORDER BY id').format(
                        sql.Identifier('labops_' + table))).fetchall()
                    result[table] = {'count': len(rows), 'sha256': canonical_hash([row[0] for row in rows])}
            return result
        original_fingerprints = fingerprints(self.env['DATABASE_URL'])
        recovery_started = time.monotonic()
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        # Restore inside the fresh DB; the active business DB is never replaced.
        argv = ['docker', 'compose', '-p', self.env['LABOPS_VALIDATION_PROJECT'], '-f',
            'infra/events/validation/compose.yaml', 'exec', '-T', 'postgres',
            'pg_restore', '-U', 'labops', '-d', name, '--exit-on-error']
        self.command(argv, timeout=180, binary=True, input=dump)
        restored_fingerprints = fingerprints(restored_url)
        assert original_fingerprints == restored_fingerprints, 'Restored business/event/audit table hash mismatch'
        with psycopg.connect(restored_url) as restored:
            with restored.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) FROM labops_outboxevent WHERE transport = %s', ('kafka',))
                restored_count = cursor.fetchone()[0]
                assert restored_count == len(self.events)
                cursor.execute('SELECT COUNT(*) FROM labops_processedevent WHERE consumer_name IN (%s,%s)',
                               ('notification', 'analytics'))
                restored_dedupe = cursor.fetchone()[0]
                cursor.execute('DELETE FROM labops_inventoryprojection')
        process = self.spawn('restore-rebuild', [str(HERE / 'workers.py'), 'rebuild'], 'replay',
                             extra_env={'DATABASE_URL': restored_url, 'WORKER_METRICS_ENABLED': '0'})
        assert process.wait(timeout=120) == 0
        with psycopg.connect(restored_url) as restored:
            rows = restored.execute('''SELECT batch_id,warehouse_id,quantity FROM labops_inventoryprojection
                                      ORDER BY batch_id,warehouse_id''').fetchall()
            # Fixed6 is stored as an integer; normalise to the same precision as ORM values.
            rebuilt = [(str(b), str(w), str(Decimal(q) / Decimal(1000000))) for b, w, q in rows]
            normalized = sorted((b, w, str(Decimal(q))) for b, w, q in rebuilt)
            original = sorted((str(b), str(w), str(Decimal(q))) for b, w, q in
                self.models.InventoryProjection.objects.values_list('batch_id', 'warehouse_id', 'quantity'))
            assert normalized == original, 'Restored projection differs from legal ledger'
        replay_ids = [item['event_id'] for item in self.events[:min(1000, len(self.events))]]
        replay_file = self.evidence / 'markers' / 'restore-replay-ids.json'
        write_json(replay_file, replay_ids)
        replay_marker = self.marker('restore-replay')
        replay = self.spawn('restore-replay', [str(HERE / 'workers.py'), 'restore_check',
            '--data-file', str(replay_file), '--marker', str(replay_marker)], 'replay',
            extra_env={'DATABASE_URL': restored_url, 'WORKER_METRICS_ENABLED': '0'})
        self.wait_marker(replay_marker, replay, timeout=180)
        assert replay.wait(timeout=30) == 0
        replay_evidence = json.loads(replay_marker.read_text())
        self.cases.append({'name': 'postgres_restore', 'watermark': watermark,
            'backup_sha256': hashlib.sha256(dump).hexdigest(), 'backup_bytes': len(dump),
            'restored_database': name, 'restored_inventory_outbox_count': restored_count,
            'restored_dedupe_count': restored_dedupe, 'legal_projection_mismatches': 0,
            'snapshot_RPO_seconds': 0, 'measured_restore_seconds': time.monotonic() - recovery_started,
            'baseline': baseline, 'passed': True,
            'table_fingerprints_before': original_fingerprints,
            'table_fingerprints_after_restore': restored_fingerprints,
            'replay_in_restored_database': replay_evidence,
            'limits': ['Snapshot restore at frozen watermark only; PITR and older-snapshot loss not measured',
                       'Broker retention exhaustion and same-name topic replacement not executed']})
        self.start_publisher()
        self.cases[-1]['preset_restoration'] = {name: self.restore_consumer_pool(name)
            for name in ('notification', 'analytics')}

    def latencies(self):
        ids = [item['event_id'] for item in self.events if item['scenario'] == 'steady']
        recorded = {item['event_id']: item for item in self.events}
        with self.connection.cursor() as cursor:
            cursor.execute('SHOW track_commit_timestamp')
            enabled = cursor.fetchone()[0] == 'on'
            if enabled:
                commits = {eid: recorded[eid]['outbox_transaction_commit_at'] for eid in ids}
                cursor.execute('''SELECT consumer_name,event_id::text,pg_xact_commit_timestamp(xmin)
                                  FROM labops_processedevent WHERE event_id = ANY(%s::uuid[])''', [ids])
                effects = {(name, eid): at.timestamp() if at else None for name, eid, at in cursor.fetchall()}
            else:
                commits, effects = {}, {}
        samples = {name: [] for name in ('notification', 'analytics')}
        missing = Counter()
        with (self.evidence / 'latency.csv').open('w', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(['event_id', 'consumer', 'outbox_transaction_commit', 'effect_transaction_commit',
                             'latency_seconds', 'outcome', 'command_return_observed_at'])
            for eid in ids:
                for name in samples:
                    begin, end = commits.get(eid), effects.get((name, eid))
                    latency = end - begin if begin is not None and end is not None else None
                    if latency is not None:
                        assert latency >= 0, 'Consumer effect precedes durable outbox commit'
                        samples[name].append(latency)
                    else:
                        missing[name] += 1
                    writer.writerow([eid, name, begin, end, latency,
                                     'completed' if latency is not None else 'missing_commit_timestamp',
                                     recorded[eid]['transaction_return_observed_at']])
        result = {'method': 'PostgreSQL insertion transaction xid captured before commit; ProcessedEvent xmin; pg_xact_commit_timestamp for both',
                  'track_commit_timestamp': enabled, 'denominator_per_consumer': len(ids), 'consumers': {}}
        for name, values in samples.items():
            values.sort()
            def percentile(p):
                return values[max(0, math.ceil(len(values) * p) - 1)] if values else None
            result['consumers'][name] = {'successful_samples': len(values), 'missing': missing[name],
                'mean_seconds': sum(values) / len(values) if values else None,
                'p95_seconds': percentile(.95), 'p99_seconds': percentile(.99),
                'passed': len(values) == len(ids) and bool(values) and percentile(.95) <= 5 and percentile(.99) <= 15}
        result['passed'] = enabled and all(row['passed'] for row in result['consumers'].values())
        write_json(self.evidence / 'latency-summary.json', result)
        return result

    def collect_metrics(self):
        import urllib.request
        for index, process in enumerate(self.children):
            if process.poll() is not None:
                continue
            port = self.child_metrics[process.pid]
            request = urllib.request.Request(f'http://127.0.0.1:{port}/metrics')
            token = self.env.get('WORKER_METRICS_TOKEN', self.env.get('METRICS_TOKEN', ''))
            if token:
                request.add_header('Authorization', 'Bearer ' + token)
            try:
                payload = urllib.request.urlopen(request, timeout=3).read()
                (self.evidence / 'metrics' / f'worker-{index}.prom').write_bytes(payload)
            except Exception as exc:
                with (self.evidence / 'errors.jsonl').open('a') as out:
                    out.write(json.dumps({'kind': 'metrics_scrape', 'worker_index': index,
                                          'error_type': type(exc).__name__}) + '\n')

    def metrics_acceptance(self):
        import urllib.request
        import urllib.error
        for name, command, role in [('retry', 'retry_events', 'replay'), ('dlq', 'publish_dlq', 'dlq')]:
            self.workers[name] = self.spawn(name + '-metrics-loop',
                ['manage.py', command, '--loop', '--limit', '100'], role)
        self.sync_metrics_targets()
        token = self.env.get('WORKER_METRICS_TOKEN', self.env.get('METRICS_TOKEN', ''))
        cases = []
        for name in worker_roles(getattr(self, 'consumer_topology', DEFAULT_PRESET)):
            process = self.workers[name]
            logical_name = 'notification' if name in self.pool_roles('notification') else name
            port = self.child_metrics[process.pid]
            endpoint = f'http://127.0.0.1:{port}/metrics'

            def scrape():
                request = urllib.request.Request(endpoint, headers={'Authorization': 'Bearer ' + token})
                payload = urllib.request.urlopen(request, timeout=5).read().decode()
                assert f'worker="{logical_name}"' in payload and 'labops_worker_database_available 1.0' in payload
                assert 'labops_worker_outbox_events' in payload and 'labops_worker_failed_deliveries' in payload
                heartbeat = re.search(r'^labops_worker_heartbeat_timestamp_seconds\{worker="' +
                                      re.escape(logical_name) + r'"\} (\S+)$', payload, re.MULTILINE)
                assert heartbeat, 'Actual worker heartbeat metric missing'
                timestamp = float(heartbeat.group(1))
                assert math.isfinite(timestamp) and -2 <= time.time() - timestamp <= 30, 'Worker heartbeat is stale'
                return payload
            payload = self.wait(scrape, 'Worker authenticated metrics missing: ' + name, timeout=30)
            observed_before = time.time()
            before_timestamp = float(re.search(r'^labops_worker_heartbeat_timestamp_seconds\{worker="' +
                re.escape(logical_name) + r'"\} (\S+)$', payload, re.MULTILINE).group(1))

            def advancing():
                candidate = scrape()
                timestamp = float(re.search(r'^labops_worker_heartbeat_timestamp_seconds\{worker="' +
                    re.escape(logical_name) + r'"\} (\S+)$', candidate, re.MULTILINE).group(1))
                return (candidate, timestamp) if timestamp > before_timestamp else False
            after_payload, after_timestamp = self.wait(advancing, 'Worker heartbeat did not advance: ' + name, timeout=15)
            observed_after = time.time()
            denied = False
            try:
                urllib.request.urlopen(endpoint, timeout=5)
            except urllib.error.HTTPError as exc:
                denied = exc.code == 403
            assert denied, 'Unauthenticated worker metrics exposed'
            (self.evidence / 'metrics' / f'{name}-accepted.prom').write_text(payload)
            (self.evidence / 'metrics' / f'{name}-heartbeat-advanced.prom').write_text(after_payload)
            cases.append({'worker': name, 'logical_worker': logical_name, 'pid': process.pid, 'port': port,
                'process_identity': copy.deepcopy(self.child_identities[process.pid]),
                'heartbeat_observed': True, 'heartbeat_before': before_timestamp,
                'heartbeat_after': after_timestamp, 'heartbeat_advanced': after_timestamp > before_timestamp,
                'first_scrape_observed_at': observed_before, 'second_scrape_observed_at': observed_after,
                'elapsed_between_samples_seconds': observed_after - observed_before,
                'database_available': True, 'unauthenticated_denied': True})

        def exporter_scrape():
            payload = urllib.request.urlopen('http://127.0.0.1:19308/metrics', timeout=5).read().decode()
            with (self.evidence / 'metrics' / 'exporter-observations.jsonl').open('a') as output:
                output.write(json.dumps({'observed_at': time.time(), 'raw_exposition': payload}) + '\n')
            assert re.search(r'^kafka_brokers(?:\{[^}]*\})? 3(?:\.0)?$', payload, re.MULTILINE)
            replicas = [float(line.rsplit(' ', 1)[1]) for line in payload.splitlines()
                if line.startswith('kafka_topic_partition_replicas{') and
                f'topic="{self.settings.KAFKA_TOPIC}"' in line]
            assert len(replicas) == 3 and all(value == 3 for value in replicas)
            from benchmarks.events.metrics_contract import require_consumer_group_lag
            lag = require_consumer_group_lag(payload,
                [self.consumer_group(name) for name in ('notification', 'analytics')],
                self.settings.KAFKA_TOPIC)
            return payload, lag
        exporter, lag = self.wait(exporter_scrape, 'Exporter exact broker/replica/two-group partition lag metrics missing', timeout=60)
        (self.evidence / 'metrics' / 'exporter-accepted.prom').write_text(exporter)
        write_json(self.evidence / 'metrics' / 'exporter-lag-coverage.json', lag)
        self.cases.append({'name': 'metrics', 'workers': cases, 'exporter_broker_count': 3,
            'inventory_partition_replicas': [3, 3, 3], 'consumer_group_lag_observed': True,
            'consumer_group_lag_coverage': lag,
            'durable_RETRY_DEAD_counts_separate_from_lag': True, 'passed': True})

    def live_alert_acceptance(self):
        import urllib.request
        from urllib.parse import urlencode
        import requests
        base = 'http://127.0.0.1:19091'
        instance = '127.0.0.1:19644'
        alert_query = ('ALERTS{alertname="RedpandaMetricsUnavailable",'
                       'alertstate="firing",instance="' + instance + '"}')
        observations = []

        def query(expression):
            url = base + '/api/v1/query?' + urlencode({'query': expression})
            response = json.loads(urllib.request.urlopen(url, timeout=5).read())
            assert response['status'] == 'success'
            observations.append({'observed_at': time.time(), 'query': expression, 'response': response})
            return response['data']['result']

        def all_brokers_up():
            rows = query('up{job="redpanda"}')
            return rows if len(rows) == 3 and all(float(row['value'][1]) == 1 for row in rows) else False

        baseline = self.wait(all_brokers_up, 'Prometheus broker baseline is not three successful scrapes', timeout=90)
        assert not query(alert_query), 'Target alert was already firing before injection'
        started = time.time()
        self.compose('stop', '-t', '0', 'redpanda-0')
        try:
            firing = self.wait(lambda: query(alert_query), 'Live broker alert did not fire after actual stop', timeout=150)
            assert all(row['metric'].get('instance') == instance and float(row['value'][1]) == 1 for row in firing)
            while time.time() - started < 150:
                time.sleep(.25)
            stopped_for = time.time() - started
        finally:
            self.compose('start', 'redpanda-0')
            write_json(self.evidence / 'metrics' / 'prometheus-live-alert-observations.json', observations)
        session = requests.Session()
        session.auth = ('admin', self.secrets['admin'])
        session.verify = self.settings.KAFKA_SSL_CA_LOCATION

        def broker_endpoint_ready():
            response = session.get('https://' + instance + '/public_metrics', timeout=5)
            return response.status_code == 200 and bool(response.content)
        self.wait(broker_endpoint_ready, 'Restored broker metrics endpoint did not return authenticated HTTP 200', timeout=120)
        restored_up = self.wait(all_brokers_up, 'Prometheus did not recover three healthy broker scrapes', timeout=90)
        self.wait(lambda: not query(alert_query), 'Live broker alert did not resolve', timeout=90)
        ended = time.time()
        raw_ranges = {}
        for name, expression in [('alert', alert_query), ('up', 'up{job="redpanda"}'),
                                  ('replication', 'redpanda_cluster_health_under_replicated_partitions')]:
            url = base + '/api/v1/query_range?' + urlencode({'query': expression,
                'start': started - 10, 'end': ended, 'step': 5})
            response = json.loads(urllib.request.urlopen(url, timeout=5).read())
            assert response['status'] == 'success'
            write_json(self.evidence / 'metrics' / f'prometheus-live-alert-{name}-range.json', response)
            raw_ranges[name] = response['data']['result']
        assert raw_ranges['alert'], 'Live firing interval missing from raw Prometheus history'
        write_json(self.evidence / 'metrics' / 'prometheus-live-alert-observations.json', observations)
        self.cases.append({'name': 'live_broker_metrics_alert', 'alert': 'RedpandaMetricsUnavailable',
            'instance': instance, 'baseline': baseline, 'actual_stop_seconds': stopped_for,
            'firing': firing, 'restored_endpoint_status': 200, 'restored_up': restored_up,
            'alert_cleared': True, 'history_start': started - 10, 'history_end': ended, 'passed': True})

    def run(self):
        self.setup()
        self.start_publisher()
        self.restore_consumer_pools(timeout=120)
        steady_started = time.monotonic()
        ids, workload = self.generate(self.args.events, 'steady')
        while time.monotonic() - steady_started < self.args.duration:
            time.sleep(.25)
        self.drained(ids, timeout=self.args.drain_timeout)
        latency = self.latencies()
        snapshot = self.snapshot(ids)
        assert not snapshot['mismatches'] and snapshot['dedupe_count'] == len(ids) * 2
        assert snapshot['notification_count'] == snapshot['expected_notification_count']
        self.cases.append({'name': 'steady', 'workload': workload, 'reconciliation': snapshot,
                           'latency': latency, 'passed': latency['passed'] and
                           workload['schedule_lateness_seconds'] <= max(1, self.args.events / self.args.rate * .05) and
                           (self.args.tier != 'full' or workload['elapsed_seconds'] <= self.args.duration * 1.05)})
        if not latency['passed']:
            raise AssertionError('Steady workload latency threshold or complete sample coverage failed')
        if not self.cases[-1]['passed']:
            raise AssertionError('Business command generation missed frozen workload rate by more than 5 percent')
        self.duplicate_drill(ids[:min(self.args.duplicate_events, len(ids))])
        self.consumer_crashes()
        self.publisher_crashes()
        self.analytics_outage()
        self.broker_fault(['redpanda-0'], self.args.broker_fault_seconds, 'one_broker_stop')
        self.broker_fault(['redpanda-0', 'redpanda-1'], self.args.outage_seconds, 'quorum_loss')
        self.broker_fault(['redpanda-0', 'redpanda-1', 'redpanda-2'], self.args.outage_seconds, 'cluster_outage')
        self.postgres_failure()
        self.retry_drill()
        self.poison_drill()
        self.rebalance()
        from security import run_security_probes
        security = run_security_probes(self.configs, self.settings.KAFKA_TOPIC,
                                       self.args.run_id, self.evidence)
        self.cases.append({'name': 'security', **security})
        if not security['passed']:
            raise AssertionError('Security negative cases did not all demonstrate a denial')
        self.restore()
        from recovery_matrix import run_recovery_matrix
        recovery_cases = run_recovery_matrix(self)
        self.cases.extend(recovery_cases)
        if len(recovery_cases) != 3 or not all(case.get('passed') for case in recovery_cases):
            raise AssertionError('Required recovery matrix failed; inspect recovery-matrix.json')
        all_ids = [item['event_id'] for item in self.events]
        self.drained(all_ids, timeout=self.args.drain_timeout)
        self.metrics_acceptance()
        self.live_alert_acceptance()
        self.collect_metrics()
        reconciliation = self.snapshot(all_ids)
        assert not reconciliation['mismatches']
        assert reconciliation['dedupe_count'] == len(all_ids) * 2
        assert reconciliation['notification_count'] == reconciliation['expected_notification_count']
        write_json(self.evidence / 'reconciliation.json', reconciliation)
        write_json(self.evidence / 'offsets-after.json', self.offsets())
        write_json(self.evidence / 'workload.json', {'steady': workload, 'unique_inventory_events': len(all_ids),
            'event_type_counts': dict(Counter(item['kind'] for item in self.events)),
            'fault_repetitions': self.args.fault_repetitions, 'cases': self.cases})

    def final_inventory_evidence(self):
        """Retain actual isolated DB truth even when generation/reporting failed."""
        result = {'database_observed': False, 'offsets_observed': False,
                  'event_log_identified_count': len(self.events), 'errors': []}
        observed = {item['event_id'] for item in self.events}
        try:
            self.connections.close_all()
            rows = list(self.models.OutboxEvent.objects.filter(transport='kafka').order_by('created_at', 'id').values(
                'id', 'aggregate_id', 'aggregate_type', 'aggregate_version', 'payload_hash',
                'status', 'attempts', 'next_attempt_at', 'locked_until', 'lease_token',
                'published_at', 'created_at'))
            write_json(self.evidence / 'final-outbox-state.json', rows)
            actual = {str(row['id']) for row in rows}
            movements = {str(row['aggregate_id']) for row in rows}
            markers = list(self.models.ProcessedEvent.objects.filter(
                consumer_name__in=('notification', 'analytics')).values('consumer_name', 'event_id', 'payload_hash'))
            markers = [row for row in markers if str(row['event_id']) in actual]
            write_json(self.evidence / 'final-processed-state.json', markers)
            hashes = {str(row['id']): row['payload_hash'] for row in rows}
            marker_conflicts = [{'consumer': row['consumer_name'], 'event_id': str(row['event_id'])}
                for row in markers if row['payload_hash'] != hashes[str(row['event_id'])]]
            consumers = {}
            for name in ('notification', 'analytics'):
                completed = {str(row['event_id']) for row in markers if row['consumer_name'] == name}
                consumers[name] = {'expected_inventory_ids': len(actual), 'completed_unique_ids': len(completed),
                    'incomplete_count': len(actual - completed), 'incomplete_event_ids': sorted(actual - completed)}
            recorded = self.generation.committed_attempts()
            recorded_movements = {item['movement_id'] for item in recorded if item['movement_id']}
            recorded_events = {item['event_id'] for item in recorded if item['event_id']}
            result.update({'database_observed': True, 'actual_inventory_outbox_count': len(actual),
                'outbox_status_counts': dict(Counter(row['status'] for row in rows)),
                'unpublished_count': sum(row['status'] != 'PUBLISHED' for row in rows),
                'consumers': consumers, 'actual_committed_ids_missing_from_event_log': sorted(actual - observed),
                'event_log_ids_missing_from_database': sorted(observed - actual),
                'journal_committed_attempts': len(recorded),
                'journal_committed_movements_missing_from_database': sorted(recorded_movements - movements),
                'database_committed_movements_missing_from_journal': sorted(movements - recorded_movements),
                'journal_identified_events_missing_from_database': sorted(recorded_events - actual),
                'processed_hash_conflicts': marker_conflicts,
                'journal_database_atomic': False})
            try:
                reconciliation = self.snapshot(sorted(actual))
                write_json(self.evidence / 'final-reconciliation.json', reconciliation)
                result['reconciliation'] = reconciliation
            except Exception as exc:
                result['errors'].append({'stage': 'reconciliation', 'error_type': type(exc).__name__})
        except Exception as exc:
            result['errors'].append({'stage': 'database_snapshot', 'error_type': type(exc).__name__})
        try:
            offsets = self.offsets()
            write_json(self.evidence / 'final-offsets.json', offsets)
            result['offsets_observed'] = True
        except Exception as exc:
            from benchmarks.events.recovery_matrix import _kafka_diagnostics
            result['errors'].append({'stage': 'committed_offsets', 'error_type': type(exc).__name__,
                                    **_kafka_diagnostics(exc)})
        write_json(self.evidence / 'final-inventory-state.json', result)
        return result

    def finish(self, error=None):
        cleanup_error = self.cleanup_workers()
        try:
            self.settle_worker_sessions()
        except BaseException as secondary:
            if cleanup_error is None:
                cleanup_error = secondary
            self.record_cleanup_error('final_worker_sessions', secondary)
        original_error = error
        if error is None:
            error = cleanup_error
        generation = self.generation.finalize()
        final_state = self.final_inventory_evidence()
        passing_cases = {case.get('name') for case in self.cases if case.get('passed')}
        limits = ['Mandatory independent-host/AZ fault exercise not executed by this same-host harness',
                  'Synthetic command workload; HTTP command throughput and production capacity unmeasured',
                  'PostgreSQL PITR and quantified older-snapshot business losses remain unmeasured',
                  'Publisher crash and standalone durable-retry drills accelerate disposable lease/due timestamps; natural configured TTL/retry wait is unmeasured in those drills',
                  'Broker recovery measures the recorded CI retry policy; the default 60/300/900/3600-second production schedule is not measured by this run',
                  'No external email/SMS exactly-once claim; only effects in this PostgreSQL database']
        for name, description in [('live_broker_metrics_alert', 'live broker alert firing/recovery'),
                                  ('isolated_retention_exhaustion', 'natural broker retention exhaustion'),
                                  ('same_name_topic_recreation', 'same-name topic replacement'),
                                  ('postgres_restart_retry_dead_dedupe', 'RETRY/DEAD/dedupe persistence across restart')]:
            if name not in passing_cases:
                limits.append('Mandatory ' + description + ' has no passing execution evidence')
        if self.args.tier != 'full':
            limits.append('Smoke tier does not establish the 90,000-event/1,800-second/50-per-second capacity target')
        reconciliation = final_state.get('reconciliation', {})
        reconciliation_complete = (bool(reconciliation)
            and not reconciliation['mismatches']
            and reconciliation['dedupe_count'] == 2 * final_state['actual_inventory_outbox_count']
            and reconciliation['notification_count'] == reconciliation['expected_notification_count'])
        generation_complete = (all(batch['status'] == 'succeeded' for batch in generation['batches'])
            and generation['totals']['requested'] == generation['totals']['committed']
            == generation['totals']['identified_events']
            and not any(generation['totals'].get(key, 0) for key in ('commit_unknown', 'attempted_unknown', 'integrity_failed'))
            and not getattr(self, '_generation_accounting_error', None))
        topologies_complete = all(topology.get('passed') is True
                                  for topology in getattr(self, 'generation_topologies', []))
        diagnostics_enabled = getattr(self, 'runtime_diagnostics_enabled', False)
        diagnostics_complete = (bool(getattr(self, 'runtime_diagnostics', []))
            and not getattr(self, 'runtime_diagnostic_errors', [])
            and all(row.get('summary', {}).get('lifecycle_complete') is True
                    and (row.get('scenario') != 'steady'
                         or row.get('summary', {}).get('collection_complete') is True)
                    for row in getattr(self, 'runtime_diagnostics', [])))
        diagnostics_qualified = not diagnostics_enabled or diagnostics_complete
        profile_evidence = self.diagnostic_profile_evidence()
        qualification_admissible = not profile_evidence['enabled']
        final_complete = (final_state['database_observed'] and final_state['offsets_observed']
            and not final_state['errors'] and not final_state['unpublished_count']
            and reconciliation_complete and generation_complete and topologies_complete and diagnostics_qualified
            and not final_state['processed_hash_conflicts']
            and all(not consumer['incomplete_count'] for consumer in final_state['consumers'].values())
            and not final_state['event_log_ids_missing_from_database']
            and not final_state['actual_committed_ids_missing_from_event_log']
            and not final_state['journal_committed_movements_missing_from_database']
            and not final_state['database_committed_movements_missing_from_journal']
            and not final_state['journal_identified_events_missing_from_database'])
        owned_cleanup_complete = not getattr(self, 'cleanup_errors', [])
        final_complete = final_complete and owned_cleanup_complete
        fault_targets_passed = all(any(case.get('name') == name and case.get('passed') and
            case.get('workload_qualification', {}).get('capacity_qualified') for case in self.cases)
            for name in ('analytics_outage', 'one_broker_stop', 'quorum_loss', 'cluster_outage'))
        report = {'passed': qualification_admissible and error is None and all(case.get('passed') for case in self.cases) and final_complete,
            'qualification_admissible': qualification_admissible,
            'diagnostic_profile': profile_evidence,
            'consumer_topology': getattr(self, 'topology', topology_profile()),
            'owned_worker_cleanup_complete': owned_cleanup_complete,
            'owned_worker_cleanup_errors': getattr(self, 'cleanup_errors', []),
            'owned_worker_close_observations': list(getattr(self, 'worker_closures', {}).values()),
            'run_id': self.args.run_id, 'unique_generated_events': len(self.events),
            'acceptance_tier': self.args.tier, 'production_ready': False,
            'full_workload_requested': self.args.tier == 'full',
            'full_workload_targets_passed': qualification_admissible and self.args.tier == 'full' and 'steady' in passing_cases and fault_targets_passed,
            'requested_numeric_profile': generation['requested_numeric_profile'],
            'generation_accounting': generation, 'final_inventory_state': final_state,
            'generation_topologies': getattr(self, 'generation_topologies', []),
            'generation_topologies_complete': topologies_complete,
            'runtime_diagnostics': getattr(self, 'runtime_diagnostics', []),
            'runtime_diagnostic_error_types': getattr(self, 'runtime_diagnostic_errors', []),
            'runtime_diagnostics_enabled': diagnostics_enabled,
            'runtime_diagnostics_applicable': diagnostics_enabled,
            'runtime_diagnostics_status': ('COMPLETE' if diagnostics_complete else 'INCOMPLETE')
                if diagnostics_enabled else 'NOT_REQUESTED',
            'runtime_diagnostics_complete': diagnostics_complete if diagnostics_enabled else None,
            'generation_accounting_complete': generation_complete,
            'generation_accounting_error_type': getattr(self, '_generation_accounting_error', None),
            'final_reconciliation_complete': reconciliation_complete,
            'final_inventory_complete': final_complete, 'delivery_proofs': self.delivery_proofs,
            'steady_inventory_inputs_committed': sum(batch['committed'] for batch in generation['batches']
                                                      if batch['label'] == 'steady'),
            'elapsed_seconds': time.time() - self.started_at, 'cases': self.cases,
            'error_type': type(error).__name__ if error else None,
            'error': str(error) if error else None,
            'shutdowns': self.shutdowns,
            'supervisor_restarts': self.supervisor_restarts,
            'limits': limits}
        write_json(self.evidence / 'report.json', report)
        lines = [f"Run {self.args.run_id}: {'PASS' if report['passed'] else 'FAIL'}", '',
                 f"Unique generated inventory events: {len(self.events)}", '',
                 'Cases:']
        lines += [f"- {case['name']}: {'PASS' if case.get('passed') else 'FAIL'}" for case in self.cases]
        lines += ['', 'Limits:'] + ['- ' + value for value in report['limits']]
        if error:
            lines += ['', f'Failure: {type(error).__name__}: {error}']
            with (self.evidence / 'errors.jsonl').open('a') as out:
                out.write(json.dumps({'kind': 'acceptance_failure', 'error_type': type(error).__name__,
                                      'message': str(error)}, sort_keys=True) + '\n')
        with (self.evidence / 'errors.jsonl').open('a') as out:
            try:
                for row in self.models.FailedDelivery.objects.order_by('created_at').values(
                        'id', 'consumer_name', 'delivery_key', 'status', 'failure_class', 'last_error', 'original_hash'):
                    out.write(json.dumps({'kind': 'durable_failed_delivery', **row}, default=str, sort_keys=True) + '\n')
            except Exception as exc:
                out.write(json.dumps({'kind': 'failure_evidence_query', 'error_type': type(exc).__name__}) + '\n')
        (self.evidence / 'summary.md').write_text('\n'.join(lines) + '\n')
        print(json.dumps({'passed': report['passed'], 'evidence_dir': str(self.evidence),
                          'cases_completed': len(self.cases), 'error_type': report['error_type']}), flush=True)
        if original_error is None and cleanup_error is not None:
            raise cleanup_error
        return report

    def diagnostic_profile_evidence(self):
        """Missing or interrupted outputs remain unknown; OFF performs no I/O."""
        enabled = getattr(self, 'diagnostic_profile_enabled', False)
        engine = self.generation.profile.get('diagnostic_profile_engine', 'cprofile')
        from benchmarks.events.diagnostic_profile import CALLER_SCHEMAS
        result = {'enabled': enabled, 'applicable': enabled, 'qualification_admissible': not enabled,
            'status': 'NOT_REQUESTED', 'complete': None, 'rows': []}
        if not enabled:
            return result
        paths = [(path, 'publisher', None, [None]) for path in getattr(self, 'publisher_profile_paths', [])]
        for topology in getattr(self, 'process_batches', []):
            paths += [(Path(plan['directory']) / 'diagnostic-profile.json', 'generator', plan['lane'],
                       plan['indices']) for plan in topology.get('origin_plans', [])]
        for path, role, lane, expected_indices in paths:
            expected = len(expected_indices)
            row = {'artifact': str(path.relative_to(self.evidence)), 'role': role,
                   'lane': lane, 'expected_calls': expected, 'complete': False}
            try:
                value = json.loads(path.read_text())
                coverage = value['coverage']
                row.update(coverage=coverage, sha256=hashlib.sha256(path.read_bytes()).hexdigest())
                row['complete'] = (coverage['complete'] is True and coverage['role'] == role
                    and coverage['lane'] == lane and coverage['requested_calls'] == expected
                    and coverage['profiled_calls'] == expected
                    and len(value['calls']) == expected
                    and len({call['ordinal'] for call in value['calls']}) == expected
                    and {call['ordinal'] for call in value['calls']} == set(expected_indices)
                    and value['qualification_admissible'] is False
                    and value['engine'] == engine and value['caller_schema'] == CALLER_SCHEMAS[engine]
                    and value['function_graph_status'] == 'COMPLETE'
                    and isinstance(value['functions'], list) and bool(value['functions']))
                if engine == 'python-profile-owned':
                    row['complete'] = (row['complete']
                        and value['callback_admission'] == 'sys.getprofile() is owned adapter'
                        and value['bias_seconds'] == 0
                        and value['repository_source_sha256'].get('repository/benchmarks/events/diagnostic_profile.py')
                            == hashlib.sha256((HERE / 'diagnostic_profile.py').read_bytes()).hexdigest()
                        and all(type(edge['total_calls']) is int and edge['total_calls'] >= 0
                            and edge['primitive_calls'] is None and edge['self_cpu_seconds'] is None
                            and edge['cumulative_cpu_seconds'] is None and edge['timing_status'] == 'UNMEASURED'
                            for function in value['functions'] for edge in function['callers']))
            except BaseException as exc:
                row['error_type'] = type(exc).__name__
            result['rows'].append(row)
        writers = {row['lane'] for row in result['rows'] if row['role'] == 'generator'}
        result['complete'] = (writers == set(range(4)) and any(row['role'] == 'publisher'
            for row in result['rows']) and all(row['complete'] for row in result['rows']))
        result['status'] = 'COMPLETE' if result['complete'] else 'INCOMPLETE'
        return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True)
    p.add_argument('--tier', choices=['smoke', 'full'], default='smoke')
    p.add_argument('--consumer-topology', choices=PRESETS, default=DEFAULT_PRESET,
                   help='Explicit frozen consumer preset; nondefault requires function profiling OFF')
    p.add_argument('--runtime-diagnostics', action='store_true',
                   help='Opt in to timed CPU/SQL/PostgreSQL/container diagnostics; disabled by default')
    p.add_argument('--diagnostic-profile', action='store_true',
                   help='Opt in to own-thread CPU profiles; enabled runs are never acceptance-admissible')
    from benchmarks.events.diagnostic_profile import PROFILE_ENGINES, request_profile
    p.add_argument('--diagnostic-profile-engine', choices=PROFILE_ENGINES, default='cprofile',
                   help='Explicit diagnostic engine; a nondefault engine requires --diagnostic-profile')
    p.add_argument('--events', type=int, default=200)
    p.add_argument('--rate', type=float, default=10)
    p.add_argument('--duration', type=float, default=20)
    p.add_argument('--fault-repetitions', type=int, default=1)
    p.add_argument('--evidence-dir', type=Path, required=True)
    p.add_argument('--generated-dir', type=Path, default=ROOT / 'infra/events/validation/generated')
    p.add_argument('--fault-events', type=int, default=20)
    p.add_argument('--duplicate-events', type=int, default=10000)
    p.add_argument('--poison-events', type=int, default=100)
    p.add_argument('--broker-fault-seconds', '--single-broker-outage-seconds', type=float, default=5)
    p.add_argument('--outage-seconds', '--all-broker-outage-seconds', type=float, default=5)
    p.add_argument('--consumer-outage-seconds', type=float, default=5)
    p.add_argument('--drain-timeout', '--drain-timeout-seconds', type=float, default=180)
    args = p.parse_args()
    try:
        request_profile(args.diagnostic_profile, args.diagnostic_profile_engine)
        topology_profile(args.consumer_topology, diagnostic_profile=args.diagnostic_profile)
    except ValueError as error:
        p.error(str(error))
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', args.run_id):
        p.error('--run-id must contain 1-48 lowercase letters, digits, underscore or hyphen')
    bounds = {'events': (4, 1000000), 'rate': (.01, 1000), 'duration': (0, 86400),
              'fault_repetitions': (1, 100), 'fault_events': (1, 100000),
              'duplicate_events': (1, 100000), 'poison_events': (12, 1000),
              'broker_fault_seconds': (1, 3600), 'outage_seconds': (1, 3600),
              'consumer_outage_seconds': (1, 3600), 'drain_timeout': (1, 7200)}
    for name, (minimum, maximum) in bounds.items():
        value = getattr(args, name)
        if not math.isfinite(value) or not minimum <= value <= maximum:
            p.error(f'--{name.replace("_", "-")} must be finite and between {minimum} and {maximum}')
    if args.events >= 90000 and args.tier != 'full':
        p.error('90,000-event acceptance requires explicit --tier full and the frozen full profile')
    if args.tier == 'full':
        if (args.events, args.rate, args.duration) != (90000, 50, 1800):
            p.error('--tier full requires exactly --events 90000 --rate 50 --duration 1800')
        minimums = {'fault_repetitions': 20, 'fault_events': 30000, 'duplicate_events': 10000,
                    'poison_events': 100, 'broker_fault_seconds': 300, 'outage_seconds': 600,
                    'consumer_outage_seconds': 600, 'drain_timeout': 900}
        for name, minimum in minimums.items():
            if getattr(args, name) < minimum:
                p.error(f'--tier full requires --{name.replace("_", "-")} >= {minimum}')
        if args.drain_timeout != 900:
            p.error('--tier full requires exactly --drain-timeout 900')
        if args.duplicate_events > args.events:
            p.error('--tier full requires --duplicate-events <= --events; the requested denominator cannot be reduced')
    args.generated_dir = args.generated_dir.resolve()
    args.evidence_dir = args.evidence_dir.resolve()
    from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
    generation = GenerationJournal(args.evidence_dir, args.run_id, numeric_profile(args))
    harness = None
    error = None
    try:
        freeze_generation_execution_profile(args.evidence_dir, args.run_id,
            runtime_diagnostics=args.runtime_diagnostics, diagnostic_profile=args.diagnostic_profile,
            diagnostic_profile_engine=args.diagnostic_profile_engine,
            consumer_topology=args.consumer_topology)
        load_environment(args.generated_dir / 'client.env')
        for key in list(os.environ):
            if key.startswith('POSTGRES_') and key != 'POSTGRES_PASSWORD':
                os.environ.pop(key)
        harness = Harness(args, generation=generation)
        harness.run()
    except BaseException as exc:
        error = exc
        raise
    finally:
        try:
            if harness is not None:
                report = harness.finish(error)
                if error is None and not report['passed']:
                    if not report.get('qualification_admissible', True):
                        raise AssertionError('Profiled run is diagnostic only; acceptance qualification is inadmissible')
                    raise AssertionError('Final durable accounting or reconciliation did not qualify; inspect report.json')
            else:
                generation.finalize()
                write_json(args.evidence_dir / 'startup-failure.json', {
                    'passed': False, 'error_type': type(error).__name__ if error else None,
                    'stage': 'environment_or_harness_setup', 'requested_numeric_profile': generation.profile,
                    'qualification_admissible': not args.diagnostic_profile,
                    'steady_command_denominator': {'requested': args.events,
                        'attempted': 0, 'committed': 0, 'unattempted': args.events,
                        'scope': 'harness setup failed before generation'},
                    'generation': generation.summary(), 'database_state_observed': False})
        except BaseException as finalization_error:
            if error is None:
                raise
            # Preserve the original failure if evidence collection itself fails.
            print(json.dumps({'passed': False, 'evidence_finalization_error_type':
                type(finalization_error).__name__}), flush=True)


if __name__ == '__main__':
    main()
