"""Read-only, whitelist-only resources for this disposable validation project.

Linux cgroup v2 counters describe the container subtree; process fallback only
describes its inspected main PID. Unavailable counters remain unavailable rather
than being reported as zero. No environment, commands, credentials or queries
are inspected. Semantics: https://docs.kernel.org/admin-guide/cgroup-v2.html and
https://docs.docker.com/engine/containers/resource_constraints/.
"""
from copy import deepcopy
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import time


DEFAULT_SERVICES = ('redpanda-0', 'redpanda-1', 'redpanda-2', 'postgres',
                    'kafka-exporter', 'prometheus')
SAFE_INSPECT_FORMAT = (
    '{"container_id":{{json .Id}},"name":{{json .Name}},'
    '"status":{{json .State.Status}},"pid":{{json .State.Pid}},'
    '"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
    '"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
    '"nano_cpus":{{json .HostConfig.NanoCpus}},'
    '"cpu_quota":{{json .HostConfig.CpuQuota}},'
    '"cpu_period":{{json .HostConfig.CpuPeriod}},'
    '"memory_bytes":{{json .HostConfig.Memory}},'
    '"cpuset_cpus":{{json .HostConfig.CpusetCpus}}}')
IDENTITY_FIELDS = ('container_id', 'name', 'status', 'pid', 'project', 'service',
                   'nano_cpus', 'cpu_quota', 'cpu_period', 'memory_bytes', 'cpuset_cpus')
CGROUP_COUNTER_FILES = ('cpu_stat', 'io_stat', 'memory_current', 'memory_events')


def _unavailable_container(error_type, *, identity=None, status='unavailable'):
    missing = {'status': 'unavailable', 'error_type': error_type}
    result = {'status': status, 'error_type': error_type,
              'process': {**missing, 'stat': dict(missing), 'io': dict(missing)},
              'cgroup_v2': {**missing, 'path': None,
                            'files': {name: dict(missing) for name in CGROUP_COUNTER_FILES}}}
    if identity is not None:
        result['identity'] = deepcopy(identity)
        result['role'] = identity['service']
    if status == 'known_stopped':
        result['known_stopped'] = True
    return result


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _unsigned(value):
    _check(isinstance(value, str) and re.fullmatch(r'[0-9]+', value) is not None,
           'Expected an unsigned integer counter')
    number = int(value)
    _check(number <= 18446744073709551615, 'Kernel counter exceeds unsigned 64-bit range')
    return number


def parse_flat_counters(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        # Linux v6.17 also emits core_sched.force_idle_usec. Preserve future
        # numeric fields without treating dotted namespaces as malformed data.
        _check(len(fields) == 2 and re.fullmatch(r'[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*', fields[0]) is not None,
               'Malformed kernel counter')
        _check(fields[0] not in result, 'Duplicate kernel counter')
        result[fields[0]] = _unsigned(fields[1])
    return result


def parse_io_stat(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        _check(bool(fields) and re.fullmatch(r'[0-9]+:[0-9]+', fields[0]) is not None,
               'Malformed block-device identity')
        device = fields[0]
        _check(device not in result, 'Duplicate block-device identity')
        counters = {}
        for field in fields[1:]:
            key, separator, value = field.partition('=')
            _check(separator and re.fullmatch(r'[a-z][a-z0-9_]*', key) is not None
                   and key not in counters, 'Malformed block-device counter')
            counters[key] = _unsigned(value)
        result[device] = counters
    return result


def parse_cgroup_path(text, cgroup_root='/sys/fs/cgroup'):
    candidates = [line[3:] for line in text.splitlines() if line.startswith('0::')]
    _check(len(candidates) == 1, 'Missing or ambiguous unified cgroup membership')
    value = candidates[0]
    _check(value.startswith('/') and value != '/' and not value.endswith(' (deleted)')
           and '\x00' not in value and '\\' not in value and '//' not in value,
           'Unsafe or root cgroup path')
    _check(all(part not in ('', '.', '..') for part in value[1:].split('/')),
           'Cgroup traversal is forbidden')
    root = Path(cgroup_root).resolve()
    path = (root / str(PurePosixPath(value)).lstrip('/')).resolve()
    _check(path != root and path.is_relative_to(root), 'Cgroup path escapes its root')
    return path


def parse_proc_stat(text, *, clock_ticks=100, page_size=4096):
    # comm may itself contain spaces or parentheses; fields after its final ')'
    # start at field 3 (state). Never return the process name or command text.
    left, right = text.find('('), text.rfind(')')
    _check(left > 0 and right > left, 'Malformed process statistics')
    pid = _unsigned(text[:left].strip())
    fields = text[right + 1:].split()
    _check(len(fields) >= 22 and clock_ticks > 0 and page_size > 0,
           'Incomplete process statistics')
    return {'pid': pid, 'user_cpu_seconds': _unsigned(fields[11]) / clock_ticks,
            'system_cpu_seconds': _unsigned(fields[12]) / clock_ticks,
            'start_time_ticks': _unsigned(fields[19]),
            'rss_bytes': _unsigned(fields[21]) * page_size}


def _cpu_max(text):
    fields = text.split()
    _check(len(fields) == 2, 'Malformed CPU quota')
    period = _unsigned(fields[1])
    _check(period > 0, 'CPU period must be positive')
    quota = None if fields[0] == 'max' else _unsigned(fields[0])
    _check(quota is None or quota > 0, 'CPU quota must be positive')
    return {'quota_usec': quota, 'period_usec': period,
            'quota_cpus': quota / period if quota is not None else None,
            'unlimited': quota is None}


def _memory_max(text):
    value = text.strip()
    return {'bytes': None if value == 'max' else _unsigned(value), 'unlimited': value == 'max'}


def _affinity(text):
    value = text.strip()
    cpus = set()
    _check(bool(value), 'CPU affinity is unavailable')
    for field in value.split(','):
        bounds = field.split('-')
        _check(len(bounds) in (1, 2), 'Malformed CPU affinity')
        first, last = _unsigned(bounds[0]), _unsigned(bounds[-1])
        _check(first <= last <= 1048576, 'Malformed CPU affinity range')
        cpus.update(range(first, last + 1))
    return {'cpu_ids': sorted(cpus), 'cpu_count': len(cpus)}


def _process_affinity(text):
    rows = [line.split(':', 1)[1] for line in text.splitlines()
            if line.startswith('Cpus_allowed_list:')]
    _check(len(rows) == 1, 'Missing process CPU affinity')
    return _affinity(rows[0])


def _command(argv):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=15)
    if result.returncode:
        # Do not copy stderr, command output or exception messages into evidence.
        raise RuntimeError('Container resource discovery command failed')
    return result.stdout


class ContainerResources:
    @classmethod
    def from_compose(cls, compose_callback, services=DEFAULT_SERVICES, *,
                     command_callback=None, expected_project=None, read_text=None,
                     proc_root='/proc', cgroup_root='/sys/fs/cgroup',
                     clock_ticks=None, page_size=None):
        _check(expected_project is None or (isinstance(expected_project, str) and
               re.fullmatch(r'labops_events_[a-z0-9][a-z0-9_-]{0,47}', expected_project) is not None),
               'Expected project must be an isolated validation project')
        sampler = cls()
        sampler.read_text = read_text or (lambda path: Path(path).read_text())
        sampler.proc_root = Path(proc_root).resolve()
        sampler.cgroup_root = Path(cgroup_root).resolve()
        sampler.clock_ticks = clock_ticks or os.sysconf('SC_CLK_TCK')
        sampler.page_size = page_size or os.sysconf('SC_PAGE_SIZE')
        sampler.records = {}
        sampler.discovery = {'version': 'container-resources-v1', 'captured_at': time.time(),
                             'scope': 'own isolated containers; process fallback is main PID only',
                             'semantics': {'cpu_stat_time_unit': 'microseconds',
                                           'cpu_throttling_scope': 'own cgroup quota; excludes ancestor throttling',
                                           'memory_current_scope': 'cgroup and descendants, bytes',
                                           'io_stat_scope': 'per block-device cumulative bytes and operation counts',
                                           'process_fallback_scope': 'inspected main PID only; excludes child processes'},
                             'containers': {}}
        command_callback = command_callback or _command
        project = expected_project
        seen_ids = set()
        for service in services:
            try:
                _check(service in DEFAULT_SERVICES, 'Unexpected validation service')
                ids = compose_callback('ps', '--all', '-q', service).split()
                _check(len(ids) == 1 and re.fullmatch(r'[a-f0-9]{64}', ids[0]) is not None,
                       'Expected exactly one full container ID')
                row = json.loads(command_callback(['docker', 'inspect', '--format', SAFE_INSPECT_FORMAT, ids[0]]))
                _check(isinstance(row, dict), 'Malformed container inspection')
                identity = {key: row.get(key) for key in IDENTITY_FIELDS}
                actual_project = identity['project']
                _check(isinstance(actual_project, str) and
                       re.fullmatch(r'labops_events_[a-z0-9][a-z0-9_-]{0,47}', actual_project) is not None,
                       'Container is not an isolated validation project')
                if project is None:
                    project = actual_project
                _check(actual_project == project and identity['service'] == service
                       and identity['container_id'] == ids[0] and ids[0] not in seen_ids,
                       'Container project, service or ID mismatch')
                _check(identity['name'] in ('/' + project + '-' + service + '-1',
                                             '/' + project + '_' + service + '_1'),
                       'Container name does not match validation service')
                _check(type(identity['pid']) is int and identity['pid'] >= 0,
                       'Container PID is malformed')
                for key in ('nano_cpus', 'cpu_period', 'memory_bytes'):
                    _check(type(identity[key]) is int and identity[key] >= 0, 'Malformed container limit')
                _check(type(identity['cpu_quota']) is int and identity['cpu_quota'] >= -1
                       and isinstance(identity['cpuset_cpus'], str), 'Malformed container CPU configuration')
                if identity['cpuset_cpus']:
                    _affinity(identity['cpuset_cpus'])
                seen_ids.add(ids[0])
                pid = identity['pid']
                if identity['status'] in ('exited', 'created') and pid == 0:
                    stopped = _unavailable_container('ContainerStopped', identity=identity, status='known_stopped')
                    stopped['limits'] = {name: {'status': 'unavailable', 'error_type': 'ContainerStopped'}
                                         for name in ('cpu_max', 'memory_max', 'cpuset_cpus_effective', 'process_affinity')}
                    sampler.discovery['containers'][service] = stopped
                    continue
                _check(identity['status'] == 'running' and pid > 0,
                       'Container is neither running nor explicitly stopped')
                baseline = sampler._read(sampler.proc_root / str(pid) / 'stat', sampler._proc_stat)
                if baseline['status'] == 'available':
                    _check(baseline['value']['pid'] == pid, 'Inspected process PID mismatch')
                cgroup = sampler._read(sampler.proc_root / str(pid) / 'cgroup',
                                      lambda text: str(parse_cgroup_path(text, sampler.cgroup_root)))
                if cgroup['status'] == 'available':
                    path = Path(cgroup['value'])
                    if not any(part in (ids[0], 'docker-' + ids[0] + '.scope') for part in path.parts):
                        cgroup = {'status': 'unavailable', 'error_type': 'ContainerCgroupIdentityMismatch'}
                path = Path(cgroup['value']) if cgroup['status'] == 'available' else None
                limits = {name: sampler._read(path / filename, parser) if path is not None else
                          {'status': 'unavailable', 'error_type': cgroup['error_type']}
                          for name, filename, parser in (
                              ('cpu_max', 'cpu.max', _cpu_max),
                              ('memory_max', 'memory.max', _memory_max),
                              ('cpuset_cpus_effective', 'cpuset.cpus.effective', _affinity))}
                limits['process_affinity'] = sampler._read(sampler.proc_root / str(pid) / 'status', _process_affinity)
                safe = {'status': 'available', 'identity': identity, 'limits': limits,
                        'cgroup_membership': cgroup, 'process_baseline': baseline}
                sampler.discovery['containers'][service] = safe
                sampler.records[service] = {'identity': identity, 'baseline': baseline, 'cgroup': cgroup, 'path': path}
            except Exception as error:
                sampler.discovery['containers'][service] = _unavailable_container(type(error).__name__)
                sampler.discovery['containers'][service]['role'] = service
        sampler.discovery['project'] = project
        return sampler

    def _proc_stat(self, text):
        return parse_proc_stat(text, clock_ticks=self.clock_ticks, page_size=self.page_size)

    def _read(self, path, parser):
        try:
            resolved = Path(path).resolve()
            _check(resolved.is_relative_to(self.proc_root) or resolved.is_relative_to(self.cgroup_root),
                   'Diagnostic file escapes approved kernel roots')
            return {'status': 'available', 'value': parser(self.read_text(path))}
        except Exception as error:
            return {'status': 'unavailable', 'error_type': type(error).__name__}

    def profile(self):
        return deepcopy(self.discovery)

    def snapshot(self):
        output = {'observed_at': time.time(), 'containers': {}}
        for service, frozen in self.discovery['containers'].items():
            if frozen['status'] != 'available':
                output['containers'][service] = deepcopy(frozen)
                continue
            record = self.records[service]
            identity, baseline = record['identity'], record['baseline']
            pid = identity['pid']
            process_stat = self._read(self.proc_root / str(pid) / 'stat', self._proc_stat)
            same_process = (baseline['status'] == process_stat['status'] == 'available' and
                            process_stat['value']['pid'] == pid and
                            baseline['value']['start_time_ticks'] == process_stat['value']['start_time_ticks'])
            if not same_process:
                error = process_stat.get('error_type', 'ProcessIdentityUnavailableOrChanged')
                output['containers'][service] = _unavailable_container(error, identity=identity)
                continue
            membership = self._read(self.proc_root / str(pid) / 'cgroup',
                                    lambda text: str(parse_cgroup_path(text, self.cgroup_root)))
            valid_cgroup = record['path'] is not None and membership == record['cgroup']
            files = {}
            for name, filename, parser in (('cpu_stat', 'cpu.stat', parse_flat_counters),
                                          ('io_stat', 'io.stat', parse_io_stat),
                                          ('memory_current', 'memory.current', lambda text: _unsigned(text.strip())),
                                          ('memory_events', 'memory.events', parse_flat_counters)):
                files[name] = self._read(record['path'] / filename, parser) if valid_cgroup else {
                    'status': 'unavailable', 'error_type': membership.get('error_type', 'CgroupIdentityUnavailableOrChanged')}
            process_io = self._read(self.proc_root / str(pid) / 'io',
                                    lambda text: parse_flat_counters(text.replace(':', '')))
            row = {'status': 'available', 'role': service, 'identity': deepcopy(identity),
                'cgroup_v2': {'status': 'available' if valid_cgroup else 'unavailable',
                              'path': str(record['path']) if valid_cgroup else None, 'files': files},
                'process': {'status': 'available', 'scope': 'inspected main PID only; excludes child processes',
                            'stat': process_stat, 'io': process_io}}
            cpu = files['cpu_stat']
            if cpu['status'] == 'available' and 'usage_usec' in cpu['value']:
                row['cpu_total_seconds'] = cpu['value']['usage_usec'] / 1000000
                for source, target, divisor in (('throttled_usec', 'cpu_throttled_seconds', 1000000),
                                                ('nr_throttled', 'cpu_throttled_periods', 1),
                                                ('nr_periods', 'cpu_periods', 1)):
                    if source in cpu['value']:
                        row[target] = cpu['value'][source] / divisor
            if files['memory_current']['status'] == 'available':
                row['memory_usage_bytes'] = files['memory_current']['value']
            row['process_cpu_total_seconds'] = (process_stat['value']['user_cpu_seconds'] +
                                                process_stat['value']['system_cpu_seconds'])
            row['process_rss_bytes'] = process_stat['value']['rss_bytes']
            if process_io['status'] == 'available':
                for source, target in (('read_bytes', 'process_read_bytes'), ('write_bytes', 'process_write_bytes')):
                    if source in process_io['value']:
                        row[target] = process_io['value'][source]
            if files['io_stat']['status'] == 'available':
                devices = files['io_stat']['value']
                for source, target in (('rbytes', 'io_read_bytes'), ('wbytes', 'io_write_bytes'),
                                       ('rios', 'io_read_operations'), ('wios', 'io_write_operations')):
                    if all(source in device for device in devices.values()):
                        row[target] = sum(device[source] for device in devices.values())
            if files['memory_events']['status'] == 'available':
                for source, target in (('oom', 'memory_oom_events'), ('oom_kill', 'memory_oom_kill_events'),
                                       ('high', 'memory_high_events'), ('max', 'memory_max_events')):
                    if source in files['memory_events']['value']:
                        row[target] = files['memory_events']['value'][source]
            output['containers'][service] = row
        return output


SafeContainerResources = ContainerResources
