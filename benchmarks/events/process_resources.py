"""Measurements of only the exact PIDs supplied by the owning coordinator.

Only /proc/<registered PID>/stat is read. No process discovery, environment,
command line, SQL or exception message crosses this boundary. CPU is cumulative
utime/stime divided by SC_CLK_TCK; RSS is pages multiplied by SC_PAGE_SIZE and
is an approximate kernel observation. PID and start ticks identify one process.
Semantics: https://docs.kernel.org/filesystems/proc.html#process-specific-subdirectories
"""
from __future__ import annotations

from copy import deepcopy
import math
import os
from pathlib import Path
import re
import threading
import time

from benchmarks.events.container_diagnostics import parse_proc_stat
from benchmarks.events.consumer_topology import DEFAULT_PRESET, worker_roles, topology_profile


GENERATOR_ROLES = tuple(f'generator-{lane}' for lane in range(4))
WORKER_ROLES = ('publisher', 'notification', 'analytics', 'retry', 'dlq')
PROCESS_ROLES = GENERATOR_ROLES + WORKER_ROLES
COUNTERS = ('user_cpu_seconds', 'system_cpu_seconds', 'rss_bytes')
UINT64_MAX = (1 << 64) - 1


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _uint(value, *, positive=False):
    if isinstance(value, str):
        _check(re.fullmatch(r'0|[1-9][0-9]*', value) is not None, 'Invalid unsigned identity')
        value = int(value)
    _check(type(value) is int and (1 if positive else 0) <= value <= UINT64_MAX,
           'Invalid unsigned identity')
    return value


def _number(value):
    try:
        return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None
    except (OverflowError, ValueError):
        return None


def _error(error):
    name = type(error).__name__
    return name if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', name) else 'Exception'


def process_resources_profile(required_roles=GENERATOR_ROLES, known_stopped_roles=(), *, consumer_topology=DEFAULT_PRESET):
    workers = worker_roles(consumer_topology)
    roles = GENERATOR_ROLES + workers
    required, stopped = tuple(required_roles), tuple(known_stopped_roles)
    _check(len(required) == len(set(required)) and set(required).issubset(roles)
           and set(GENERATOR_ROLES).issubset(required), 'Invalid required process roles')
    _check(len(stopped) == len(set(stopped)) and set(stopped).issubset(workers),
           'Invalid stopped process roles')
    return {'version': 'scoped-process-resources-v1', 'planned_roles': list(roles),
        'consumer_topology': topology_profile(consumer_topology),
        'required_roles': list(required), 'known_stopped_roles': list(stopped),
        'stages': ['planned', 'startup', 'running', 'cleaned', 'exited'],
        'identity_scope': 'coordinator supplied exact PID plus process start ticks; no discovery',
        'read_scope': 'registered PID stat only; double read validates identity and monotonic CPU',
        'cpu_scope': 'one exact process, including its threads; excludes descendant processes',
        'cpu_unit': 'seconds; process utime/stime jiffies divided by SC_CLK_TCK',
        'rss_scope': 'approximate current resident pages multiplied by SC_PAGE_SIZE, bytes',
        'sampling_span': 'each applicable live process from registration until explicit cleanup/exit',
        'generator_delta_span': 'child ready snapshot through child final snapshot before exit; excludes startup before ready and reap after final',
        'worker_delta_span': 'latest worker generation ready observation through final live kernel observation; stopped/unobserved spans remain unknown',
        'exit_scope': 'final child-origin counters are receipt evidence, never a fresh post-exit kernel observation',
        'coverage_scope': 'every applicable live sample requires numeric CPU/RSS and matching identity; no union forgiveness',
        'cpu_deltas_overlap_wall': True, 'sum_cpu_deltas_is_wall_time': False}


def _stat_reader(pid, *, proc_root, read_text, clock_ticks, page_size, start_time_ticks=None):
    pid = _uint(pid, positive=True)
    root = Path(proc_root).resolve()
    candidate = root / str(pid) / 'stat'
    # Resolve both before and after reads. A supplied fake/proc mount cannot
    # redirect a registered PID to a different tree through a symlink.
    _check(candidate.resolve() == candidate and candidate.is_relative_to(root),
           'Process statistics escape their exact PID path')
    values = []
    for _ in range(2):
        _check(candidate.resolve() == candidate and candidate.is_relative_to(root),
               'Process statistics escape their exact PID path')
        text = read_text(candidate)
        _check(isinstance(text, str) and len(text) <= 16384, 'Invalid process statistics')
        row = parse_proc_stat(text, clock_ticks=clock_ticks, page_size=page_size)
        state = text[text.rfind(')') + 1:].split()[0]
        _check(state in {'R', 'S', 'D', 'T', 't', 'I', 'W', 'P', 'K'},
               'Process is exited or has unknown state')
        _check(candidate.resolve() == candidate, 'Process statistics path changed during read')
        _check(row['pid'] == pid, 'Process PID changed')
        _check(start_time_ticks is None or row['start_time_ticks'] == start_time_ticks,
               'Process start identity changed')
        _uint(row['rss_bytes'])
        _check(all(_number(row[name]) is not None for name in COUNTERS), 'Process counters unavailable')
        values.append(row)
    _check(values[0]['start_time_ticks'] == values[1]['start_time_ticks'], 'Process identity changed during read')
    _check(all(values[1][name] >= values[0][name] for name in COUNTERS[:2]), 'Process CPU counter reset')
    return values[-1]


def registered_process_snapshot(pid, *, proc_root='/proc', read_text=None, clock_ticks=None,
                                page_size=None, monotonic=None):
    """Read only one explicitly owned PID, never discover processes."""
    result = {'source': 'kernel_proc_stat', 'pid': pid, 'status': 'unavailable',
        'start_time_ticks': None, **{name: None for name in COUNTERS}, 'captured_monotonic': None}
    try:
        ticks = _uint(os.sysconf('SC_CLK_TCK') if clock_ticks is None else clock_ticks, positive=True)
        pages = _uint(os.sysconf('SC_PAGE_SIZE') if page_size is None else page_size, positive=True)
        row = _stat_reader(pid, proc_root=proc_root,
            read_text=read_text or (lambda path: Path(path).read_text(encoding='utf-8')),
            clock_ticks=ticks, page_size=pages)
        captured = _number((monotonic or time.monotonic)())
        _check(captured is not None, 'Invalid process observation clock')
        result.update(row, status='available', captured_monotonic=captured)
    except Exception as error:
        result['error_type'] = _error(error)
    return result


def own_process_snapshot(**options):
    """Child-origin numeric receipt; reads only the calling process's own PID."""
    return {**registered_process_snapshot(os.getpid(), **options), 'source': 'child_origin'}


class ProcessResources:
    """Thread-safe owned-process catalog; registration never scans /proc.

    A role may explicitly restart after an exited generation; old generations
    and read failures remain evidence. Planned roles are not live observations.
    The caller must supply final receipts from its child message channel and
    actual reap results, not cached samples or invented exit states.
    """
    def __init__(self, *, required_roles=GENERATOR_ROLES, known_stopped_roles=(),
                 consumer_topology=DEFAULT_PRESET,
                 proc_root='/proc', read_text=None, clock_ticks=None, page_size=None,
                 monotonic=None):
        self._profile = process_resources_profile(required_roles, known_stopped_roles,
            consumer_topology=consumer_topology)
        self.worker_roles = worker_roles(consumer_topology)
        self.process_roles = GENERATOR_ROLES + self.worker_roles
        self.proc_root = Path(proc_root).resolve()
        self.read_text = read_text or (lambda path: Path(path).read_text(encoding='utf-8'))
        self.clock_ticks = _uint(os.sysconf('SC_CLK_TCK') if clock_ticks is None else clock_ticks, positive=True)
        self.page_size = _uint(os.sysconf('SC_PAGE_SIZE') if page_size is None else page_size, positive=True)
        self.monotonic = monotonic or time.monotonic
        self._lock = threading.RLock()
        self._records = {role: [] for role in self.process_roles}
        self._declared_stopped = set(known_stopped_roles)

    def profile(self):
        return {**deepcopy(self._profile), 'clock_ticks_per_second': self.clock_ticks,
            'page_size_bytes': self.page_size}

    def _role(self, role):
        _check(isinstance(role, str) and role in self.process_roles, 'Unowned process role')
        return role

    def _active(self, role):
        rows = self._records[self._role(role)]
        _check(bool(rows), 'Process role has not started')
        return rows[-1]

    def _record_error(self, row, stage, error):
        # Repeated unavailable reads remain counted without growing the
        # catalog quadratically inside the bounded raw sample artifact.
        name = _error(error)
        for item in row['errors']:
            if item['stage'] == stage and item['error_type'] == name:
                item['occurrences'] += 1
                return item
        if len(row['errors']) >= 64:
            name = 'Exception'
            for item in row['errors']:
                if item['stage'] == 'process_observation' and item['error_type'] == name:
                    item['occurrences'] += 1
                    return item
        item = {'stage': stage if len(row['errors']) < 64 else 'process_observation',
            'error_type': name, 'occurrences': 1}
        row['errors'].append(item)
        return item

    def _read(self, row):
        try:
            value = _stat_reader(row['pid'], proc_root=self.proc_root, read_text=self.read_text,
                clock_ticks=self.clock_ticks, page_size=self.page_size,
                start_time_ticks=row['start_time_ticks'])
            if row['start_time_ticks'] is None:
                row['start_time_ticks'] = value['start_time_ticks']
            previous = row.get('_last_value')
            _check(previous is None or all(value[name] >= previous[name] for name in COUNTERS[:2]),
                   'Process CPU counter reset')
            captured = _number(self.monotonic())
            _check(captured is not None, 'Invalid process observation clock')
            result = {'status': 'available', 'source': 'kernel_proc_stat', **value,
                'captured_monotonic': captured}
            row['_last_value'] = deepcopy(result)
            return result
        except Exception as error:
            item = self._record_error(row, 'live_sample', error)
            return {'status': 'unavailable', 'source': 'kernel_proc_stat',
                'pid': row['pid'], 'start_time_ticks': row['start_time_ticks'],
                **{name: None for name in COUNTERS}, 'captured_monotonic': None,
                'error_type': item['error_type']}

    def register(self, role, pid, start_time_ticks=None):
        with self._lock:
            role, pid = self._role(role), _uint(pid, positive=True)
            if start_time_ticks is not None:
                start_time_ticks = _uint(start_time_ticks)
            rows = self._records[role]
            _check(not rows or rows[-1]['stage'] == 'exited', 'Process role already active')
            _check(len(rows) < 128, 'Process generation bound exceeded')
            _check(not any(records and records[-1]['stage'] in ('startup', 'running')
                       and records[-1]['pid'] == pid for records in self._records.values()),
                   'Process PID already owned by another role')
            row = {'role': role, 'generation': len(rows), 'stage': 'startup', 'pid': pid,
                'start_time_ticks': start_time_ticks, 'ready_snapshot': None,
                'final_snapshot': None, 'cleaned': False, 'exitcode': None,
                'known_stopped': False, 'errors': []}
            rows.append(row)
            self._declared_stopped.discard(role)
            baseline = self._read(row)
            row['registration_snapshot'] = baseline
            return deepcopy(baseline)

    def _receipt(self, row, snapshot):
        _check(isinstance(snapshot, dict) and snapshot.get('source') == 'child_origin'
               and snapshot.get('status') == 'available', 'Missing child-origin observation')
        pid, start = _uint(snapshot.get('pid'), positive=True), _uint(snapshot.get('start_time_ticks'))
        _check(pid == row['pid'] and start == row['start_time_ticks'], 'Child process identity mismatch')
        captured = _number(snapshot.get('captured_monotonic'))
        _check(captured is not None, 'Invalid child observation clock')
        clean = {'source': 'child_origin', 'status': 'available', 'pid': pid,
            'start_time_ticks': start, 'captured_monotonic': captured}
        for name in COUNTERS:
            value = _uint(snapshot.get(name)) if name == 'rss_bytes' else _number(snapshot.get(name))
            _check(value is not None, 'Invalid child process counter')
            clean[name] = value
        return clean

    def ready(self, role, child_snapshot=None):
        with self._lock:
            row = self._active(role)
            _check(row['stage'] == 'startup', 'Process role is not starting')
            live = self._read(row)
            try:
                _check(live['status'] == 'available', 'Ready process counters unavailable')
                _check(role not in GENERATOR_ROLES or child_snapshot is not None,
                       'Generator ready receipt must originate in the child')
                ready = self._receipt(row, child_snapshot) if child_snapshot is not None else live
                _check(all(live[name] >= ready[name] for name in COUNTERS[:2]), 'Ready child CPU exceeds fresh process counters')
            except Exception as error:
                self._record_error(row, 'ready_receipt', error)
                raise
            row['ready_snapshot'], row['stage'] = deepcopy(ready), 'running'
            return deepcopy(ready)

    def finalize(self, role, final_snapshot=None, *, exitcode=None, cleaned=False, known_stopped=False):
        with self._lock:
            row = self._active(role)
            _check(row['stage'] in ('startup', 'running', 'cleaned'), 'Process role has already exited')
            _check(type(cleaned) is bool and type(known_stopped) is bool,
                   'Invalid cleanup declaration')
            _check(exitcode is None or (type(exitcode) is int and -(1 << 31) <= exitcode < (1 << 31)),
                   'Invalid process exit code')
            _check(not known_stopped or role in self.worker_roles, 'Generator cannot be excluded as stopped')
            if final_snapshot is not None:
                try:
                    final = self._receipt(row, final_snapshot)
                    ready = row['ready_snapshot']
                    _check(ready is not None and final['captured_monotonic'] >= ready['captured_monotonic']
                           and all(final[name] >= ready[name] for name in COUNTERS[:2]),
                           'Final child counters precede ready observation')
                    previous = row.get('_last_value')
                    # A sampler can observe after the child's final receipt.
                    # Compare counters only when its observation precedes final.
                    _check(previous is None or previous['captured_monotonic'] > final['captured_monotonic']
                           or all(final[name] >= previous[name] for name in COUNTERS[:2]),
                           'Final child CPU counter reset')
                    row['final_snapshot'] = deepcopy(final)
                except Exception as error:
                    self._record_error(row, 'final_receipt', error)
                    raise
            row['cleaned'] = row['cleaned'] or cleaned
            row['known_stopped'] = known_stopped
            row['exitcode'] = exitcode
            row['stage'] = 'exited' if exitcode is not None else ('cleaned' if row['cleaned'] else row['stage'])
            return self._public(row, None)

    def _public(self, row, value):
        return {key: deepcopy(row[key]) for key in ('role', 'generation', 'stage', 'pid',
            'start_time_ticks', 'registration_snapshot', 'ready_snapshot', 'final_snapshot',
            'cleaned', 'exitcode', 'known_stopped', 'errors')} | {
                'applicable': row['stage'] in ('startup', 'running'), 'value': deepcopy(value)}

    def sample(self):
        with self._lock:
            rows = []
            for role in self.process_roles:
                records = self._records[role]
                if not records:
                    rows.append({'role': role, 'generation': None, 'stage': 'exited' if role in self._declared_stopped else 'planned',
                        'applicable': False, 'pid': None, 'start_time_ticks': None,
                        'known_stopped': role in self._declared_stopped, 'value': None,
                        'ready_snapshot': None, 'final_snapshot': None, 'cleaned': False,
                        'exitcode': None, 'errors': []})
                else:
                    for row in records:
                        value = self._read(row) if row['stage'] in ('startup', 'running') else None
                        rows.append(self._public(row, value))
            return {'observed': True, 'profile': self.profile(), 'processes': rows}


def sanitize_process_resources(value, *, consumer_topology=DEFAULT_PRESET, expected_profile=None):
    """Whitelist the callback boundary and validate its role contract afresh."""
    invalid = {'observed': False, 'profile': None, 'processes': None,
        'errors': [{'stage': 'process_catalog', 'error_type': 'InvalidProcessCatalog'}]}
    if not isinstance(value, dict) or value.get('observed') is not True:
        return invalid
    workers = worker_roles(consumer_topology)
    roles = GENERATOR_ROLES + workers
    profile, rows = value.get('profile'), value.get('processes')
    if not isinstance(profile, dict) or not isinstance(rows, list) or len(rows) > len(roles)*128:
        return invalid
    try:
        clean_profile = process_resources_profile(profile.get('required_roles'), profile.get('known_stopped_roles'),
            consumer_topology=consumer_topology)
        _check(profile.get('consumer_topology') == topology_profile(consumer_topology)
               and profile.get('planned_roles') == list(roles), 'Process topology profile changed')
        clean_profile['clock_ticks_per_second'] = _uint(profile.get('clock_ticks_per_second'), positive=True)
        clean_profile['page_size_bytes'] = _uint(profile.get('page_size_bytes'), positive=True)
        _check(expected_profile is None or clean_profile == expected_profile,
               'Owned process role contract changed')
    except (TypeError, ValueError):
        return invalid
    clean, identities, process_identities, active_pids = [], set(), set(), set()
    for row in rows:
        try:
            _check(isinstance(row, dict) and row.get('role') in roles, 'Invalid process role')
            role, stage = row['role'], row.get('stage')
            _check(stage in ('planned', 'startup', 'running', 'cleaned', 'exited'), 'Invalid process stage')
            generation = None if row.get('generation') is None else _uint(row['generation'])
            _check(generation is None or generation < 128, 'Invalid process generation')
            key = (role, generation)
            _check(key not in identities, 'Duplicate process identity')
            identities.add(key)
            pid = None if row.get('pid') is None else _uint(row['pid'], positive=True)
            start = None if row.get('start_time_ticks') is None else _uint(row['start_time_ticks'])
            _check(stage not in ('startup', 'running', 'cleaned') or (pid is not None and generation is not None),
                   'Live process identity missing')
            _check((generation is None) == (pid is None), 'Process generation identity missing')
            _check(stage != 'planned' or generation is None, 'Started process falsely declared planned')
            applicable = stage in ('startup', 'running')
            _check(row.get('applicable') is applicable, 'Process applicability contradicts lifecycle')
            if pid is not None and start is not None:
                _check((pid, start) not in process_identities, 'One process has multiple role identities')
                process_identities.add((pid, start))
            if applicable:
                _check(pid not in active_pids, 'One live PID has multiple roles')
                active_pids.add(pid)
            known_stopped = row.get('known_stopped') is True
            _check(not known_stopped or (role in workers and stage == 'exited'), 'Invalid stopped process declaration')
            errors = []
            _check(isinstance(row.get('errors'), list) and len(row['errors']) <= 65,
                   'Invalid process error records')
            for error in row['errors']:
                if isinstance(error, dict):
                    name = error.get('error_type')
                    if isinstance(name, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', name):
                        errors.append({'stage': error.get('stage') if error.get('stage') in
                            {'live_sample', 'ready_receipt', 'final_receipt'} else 'process_observation',
                            'error_type': name, 'occurrences': _uint(error.get('occurrences'), positive=True)})
            item = {'role': role, 'generation': generation, 'stage': stage, 'pid': pid,
                'start_time_ticks': start, 'applicable': applicable, 'known_stopped': known_stopped,
                'cleaned': row.get('cleaned') is True, 'exitcode': row.get('exitcode') if
                    type(row.get('exitcode')) is int and -(1 << 31) <= row['exitcode'] < (1 << 31) else None,
                'errors': errors}
            for field in ('value', 'registration_snapshot', 'ready_snapshot', 'final_snapshot'):
                observation = row.get(field)
                item[field] = None
                if observation is None:
                    continue
                _check(isinstance(observation, dict), 'Invalid process observation')
                source = observation.get('source')
                _check(source in ('kernel_proc_stat', 'child_origin'), 'Invalid process observation source')
                _check(field not in ('value', 'registration_snapshot') or source == 'kernel_proc_stat',
                       'A child receipt cannot replace a fresh kernel observation')
                _check(role not in GENERATOR_ROLES or field not in ('ready_snapshot', 'final_snapshot')
                       or source == 'child_origin', 'Generator receipt must originate in child')
                observed_pid = _uint(observation.get('pid'), positive=True)
                observed_start = None if observation.get('start_time_ticks') is None else _uint(observation['start_time_ticks'])
                status = observation.get('status')
                _check(status in ('available', 'unavailable'), 'Invalid process observation status')
                _check(observed_pid == pid and (observed_start == start or
                    (status == 'unavailable' and observed_start is None)), 'Process observation identity mismatch')
                captured = _number(observation.get('captured_monotonic'))
                counters = {name: _number(observation.get(name)) for name in COUNTERS}
                if counters['rss_bytes'] is not None:
                    counters['rss_bytes'] = _uint(observation['rss_bytes'])
                if status == 'available':
                    _check(start is not None and captured is not None and all(v is not None for v in counters.values()),
                           'Numeric process observation incomplete')
                else:
                    # Never promote retained old scalar values after a failed read.
                    captured, counters = None, {name: None for name in COUNTERS}
                observed = {'source': source, 'status': status, 'pid': observed_pid,
                    'start_time_ticks': observed_start, 'captured_monotonic': captured, **counters}
                error_type = observation.get('error_type')
                if isinstance(error_type, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', error_type):
                    observed['error_type'] = error_type
                item[field] = observed
            # A post-exit snapshot is never a fresh kernel observation.
            _check(applicable or item['value'] is None, 'Exited process has a fresh observation')
            clean.append(item)
        except (TypeError, ValueError):
            return invalid
    if not set(roles).issubset({row['role'] for row in clean}):
        return invalid
    return {'observed': True, 'profile': clean_profile, 'processes': clean, 'errors': []}


def summarize_process_resources(samples, final, *, consumer_topology=DEFAULT_PRESET, expected_profile=None):
    """Per-sample live coverage plus final child ready/cleanup/reap proof."""
    result = {'supplied': True, 'collection_complete': False, 'profile': None,
        'live_sample_count': len(samples), 'live_sample_coverage_complete': bool(samples),
        'observed_live_roles': [], 'generator_complete_roles': [], 'worker_complete_roles': [],
        'required_roles': [], 'known_stopped_roles': [], 'generator_cpu_deltas': {},
        'generator_cpu_delta_spans_seconds': {}, 'worker_cpu_deltas': {},
        'worker_cpu_delta_spans_seconds': {},
        'error_type_counts': {}, 'catalog_error_occurrence_counts': {},
        'error_count_scope': 'qualification error mentions; catalog occurrence counts preserve actual recorded read/receipt failures'}
    errors, observed = {}, set()
    def fail(name):
        errors[name] = errors.get(name, 0) + 1
    def callback_errors(value):
        if isinstance(value, dict):
            for error in value.get('errors', []):
                if isinstance(error, dict):
                    name = error.get('error_type')
                    if isinstance(name, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', name):
                        fail(name)
    contract, identities, latest_live, invalid_identities = None, {}, {}, set()
    roles = GENERATOR_ROLES + worker_roles(consumer_topology)
    def continuity(row):
        key = (row['role'], row['generation'])
        if row['generation'] is None:
            return
        identity = (row['pid'], row['start_time_ticks'])
        previous_identity = identities.get(key)
        if (previous_identity is not None and (identity[0] != previous_identity[0]
                or (previous_identity[1] is not None and identity[1] != previous_identity[1]))):
            result['live_sample_coverage_complete'] = False
            fail('ProcessIdentityContinuityChanged')
            invalid_identities.add(key)
        else:
            identities[key] = identity
        value = row['value']
        if row['applicable'] and value is not None and value['status'] == 'available':
            previous = latest_live.get(key)
            if previous is not None and (value['captured_monotonic'] <= previous['captured_monotonic']
                    or any(value[name] < previous[name] for name in COUNTERS[:2])):
                result['live_sample_coverage_complete'] = False
                fail('ProcessObservationClockOrCounterReset')
                invalid_identities.add(key)
            latest_live[key] = value
    for sample in samples:
        if not isinstance(sample, dict) or sample.get('observed') is not True:
            result['live_sample_coverage_complete'] = False
            fail('ProcessCatalogUnavailable')
            callback_errors(sample)
            continue
        profile = sample['profile']
        if expected_profile is not None and profile != expected_profile:
            result['live_sample_coverage_complete'] = False
            fail('OwnedProcessContractChanged')
        if profile.get('consumer_topology') != topology_profile(consumer_topology):
            result['live_sample_coverage_complete'] = False
            fail('ProcessCatalogTopologyChanged')
        if contract is None:
            contract = profile
        elif profile != contract:
            result['live_sample_coverage_complete'] = False
            fail('ProcessCatalogContractChanged')
        for row in sample['processes']:
            continuity(row)
            if row['errors']:
                result['live_sample_coverage_complete'] = False
                for error in row['errors']:
                    fail(error['error_type'])
            if row['applicable']:
                value = row['value']
                if value is None or value['status'] != 'available':
                    result['live_sample_coverage_complete'] = False
                    fail('RequiredLiveProcessCounterUnavailable')
                else:
                    observed.add(row['role'])
    if not isinstance(final, dict) or final.get('observed') is not True:
        fail('FinalProcessCatalogUnavailable')
        callback_errors(final)
        result['error_type_counts'], result['observed_live_roles'] = errors, sorted(observed)
        return result
    if contract is not None and final['profile'] != contract:
        result['live_sample_coverage_complete'] = False
        fail('ProcessCatalogContractChanged')
    if final['profile'].get('consumer_topology') != topology_profile(consumer_topology):
        fail('ProcessCatalogTopologyChanged')
    if expected_profile is not None and final['profile'] != expected_profile:
        fail('OwnedProcessContractChanged')
    result['profile'] = deepcopy(final['profile'])
    result['required_roles'] = final['profile']['required_roles']
    result['known_stopped_roles'] = final['profile']['known_stopped_roles']
    final_rows = {role: [row for row in final['processes'] if row['role'] == role] for role in roles}
    final_live_complete = True
    for role, rows in final_rows.items():
        if not rows:
            final_live_complete = False
            fail('RequiredProcessRoleMissing')
            continue
        for row in rows:
            continuity(row)
            if row['errors']:
                final_live_complete = False
                for error in row['errors']:
                    fail(error['error_type'])
                    name = error['error_type']
                    result['catalog_error_occurrence_counts'][name] = (
                        result['catalog_error_occurrence_counts'].get(name, 0) + error['occurrences'])
            if row['applicable'] and (row['value'] is None or row['value']['status'] != 'available'):
                final_live_complete = False
                fail('RequiredFinalLiveProcessCounterUnavailable')
        if role in GENERATOR_ROLES:
            row = rows[-1]
            ready, end = row['ready_snapshot'], row['final_snapshot']
            if end is not None and end['status'] == 'available':
                for previous in (row['registration_snapshot'], latest_live.get((role, row['generation']))):
                    if (previous is not None and previous['status'] == 'available'
                            and previous['captured_monotonic'] <= end['captured_monotonic']
                            and any(end[name] < previous[name] for name in COUNTERS[:2])):
                        invalid_identities.add((role, row['generation']))
                        final_live_complete = False
                        fail('ChildFinalProcessCounterReset')
            good = (len(rows) == 1 and row['stage'] == 'exited' and row['cleaned'] and row['exitcode'] == 0
                and ready is not None and ready['status'] == 'available' and ready['source'] == 'child_origin' and end is not None
                and end['status'] == 'available' and end['source'] == 'child_origin'
                and end['captured_monotonic'] >= ready['captured_monotonic']
                and all(end[name] >= ready[name] for name in COUNTERS[:2]) and not row['errors']
                and (role, row['generation']) not in invalid_identities)
            result['generator_cpu_deltas'][role] = {name: end[name]-ready[name] if good else None for name in COUNTERS[:2]}
            result['generator_cpu_delta_spans_seconds'][role] = (
                end['captured_monotonic']-ready['captured_monotonic'] if good else None)
            if good:
                result['generator_complete_roles'].append(role)
            else:
                fail('GeneratorReadyFinalReapProofIncomplete')
        else:
            row = rows[-1]
            known_stopped = role in result['known_stopped_roles'] and row['known_stopped']
            ready, end = row['ready_snapshot'], row['value']
            live = (row['stage'] == 'running' and row['ready_snapshot'] is not None
                and row['ready_snapshot']['status'] == 'available' and row['value'] is not None
                and row['value']['status'] == 'available' and not row['errors']
                and (role, row['generation']) not in invalid_identities
                and end['captured_monotonic'] >= ready['captured_monotonic']
                and all(end[name] >= ready[name] for name in COUNTERS[:2]))
            result['worker_cpu_deltas'][role] = {name: end[name]-ready[name] if live else None for name in COUNTERS[:2]}
            result['worker_cpu_delta_spans_seconds'][role] = (
                end['captured_monotonic']-ready['captured_monotonic'] if live else None)
            stopped = known_stopped and (row['generation'] is None or (row['cleaned'] and row['exitcode'] is not None))
            if role in result['required_roles']:
                if live or stopped:
                    result['worker_complete_roles'].append(role)
                else:
                    fail('RequiredWorkerProcessProofIncomplete')
    result['observed_live_roles'], result['error_type_counts'] = sorted(observed), errors
    result['collection_complete'] = bool(result['live_sample_coverage_complete'] and final_live_complete
        and not errors and set(GENERATOR_ROLES).issubset(result['generator_complete_roles'])
        and (set(result['required_roles'])-set(GENERATOR_ROLES)).issubset(result['worker_complete_roles']))
    return result
