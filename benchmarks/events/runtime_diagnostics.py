"""Bounded measurement for acceptance; never a capacity or causality verdict.

Only authored categories, numeric counters and authorized operational backend
IDs cross the evidence boundary. SQL text, parameters, exception messages,
environment values, connection strings and arbitrary application names do not.
"""
from __future__ import annotations

from collections import Counter
import copy
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import platform
import re
import resource
import threading
import time


INTERVAL_SECONDS = 1.0
MAX_SAMPLES = 7200
CONNECT_TIMEOUT_SECONDS = 1
STATEMENT_TIMEOUT_MS = 250
MAX_BACKENDS = 256
SQL_OPERATIONS = frozenset({'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'SET', 'SHOW',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE'})
CONTAINER_ROLES = frozenset({'postgres', 'redpanda-0', 'redpanda-1', 'redpanda-2',
    'kafka-exporter', 'prometheus'})
RESOURCE_FIELDS = frozenset({'cpu_total_seconds', 'cpu_user_seconds', 'cpu_system_seconds',
    'cpu_percent', 'cpu_quota_cores', 'cpu_throttled_seconds', 'cpu_throttled_periods',
    'cpu_periods', 'memory_usage_bytes', 'memory_limit_bytes', 'memory_working_set_bytes',
    'io_read_bytes', 'io_write_bytes', 'pids_current'})
DATABASE_COUNTERS = ('xact_commit', 'xact_rollback', 'blks_read', 'blks_hit', 'tup_returned',
    'tup_fetched', 'tup_inserted', 'tup_updated', 'tup_deleted', 'conflicts', 'temp_files',
    'temp_bytes', 'deadlocks', 'blk_read_time', 'blk_write_time', 'session_time',
    'active_time', 'idle_in_transaction_time', 'sessions', 'sessions_abandoned',
    'sessions_fatal', 'sessions_killed')
WAL_COUNTERS = ('wal_records', 'wal_fpi', 'wal_bytes', 'wal_buffers_full',
    'wal_write', 'wal_sync', 'wal_write_time', 'wal_sync_time')


def diagnostics_profile():
    return {'version': 'runtime-diagnostics-v1', 'interval_seconds': INTERVAL_SECONDS,
        'max_samples': MAX_SAMPLES, 'connect_timeout_seconds': CONNECT_TIMEOUT_SECONDS,
        'statement_timeout_ms': STATEMENT_TIMEOUT_MS, 'max_database_backends': MAX_BACKENDS,
        'sql_scope': 'client execute wall/count by operation; no statement text or parameters',
        'commit_scope': 'owning Django wrapper commit calls; separate from execute_wrapper',
        'process_cpu_scope': 'harness process including its lane and diagnostic threads; excludes worker subprocesses',
        'thread_cpu_scope': 'own command thread time; Linux sampled own-process thread counters may omit terminated intervals',
        'database_scope': 'current database activity/locks/counters; WAL counters are cluster-wide',
        'resource_scope': 'supplied container counters; running roles require cgroup CPU/memory, observed stopped roles excluded',
        'required_counters': ['process_user_cpu_seconds', 'process_system_cpu_seconds',
            'wall_seconds', 'database_activity_waits', 'database_locks', 'database_counters',
            'running_container_cpu_total_seconds', 'running_container_memory_usage_bytes'],
        'optional_counters': ['host_io', 'container_io', 'thread_snapshot_cpu', 'wal_io_timing'],
        'shutdown_scope': 'owner cleanup is awaited; PG startup/server statement budgets are not a hard client I/O deadline; supplied callbacks must return',
        'interpretation': 'diagnostic observations only; CPU/GIL, lock and I/O causality requires evidence'}


def _error_type(error):
    name = type(error).__name__
    return name if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', name) else 'Exception'


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        if not math.isfinite(value) or value < 0:
            return None
    except (ValueError, OverflowError):
        return None
    if isinstance(value, Decimal):
        return int(value) if value == int(value) else float(value)
    return value


def _operation(sql):
    found = re.match(r'\s*([A-Za-z]+)', sql) if isinstance(sql, str) else None
    value = found.group(1).upper() if found else 'OTHER'
    return value if value in SQL_OPERATIONS else 'OTHER'


def _actual_connection(connection):
    # Resolve the Django proxy in this owning thread; never share its backend.
    from django.utils.connection import ConnectionProxy
    if isinstance(connection, ConnectionProxy):
        return connection._connections[connection._alias]
    return connection


class CommandDiagnostics:
    """Observe one command and restore the exact owning connection instance.

    Process CPU deltas overlap across concurrent commands and must not be added
    together. Thread CPU excludes waiting and does not identify GIL contention.
    A physical commit failure remains the original exception object.
    """
    def __init__(self, connection, *, monotonic=None, thread_clock=None, process_clock=None):
        self.connection = connection
        self.monotonic = monotonic or time.monotonic
        self.thread_clock = thread_clock or getattr(time, 'thread_time', None)
        self.process_clock = process_clock or time.process_time
        self._entered = False
        self._done = False
        self._sql = Counter()
        self._sql_wall = Counter()
        self._sql_errors = Counter()
        self._sql_unknown = set()
        self._commits = 0
        self._commit_wall = 0.0
        self._commit_errors = Counter()
        self._commit_unknown = False
        self._error = None
        self._diagnostic_errors = []

    def __enter__(self):
        self._db = _actual_connection(self.connection)
        self._started = self.monotonic()
        self._process_started = self.process_clock()
        self._thread_started = self.thread_clock() if self.thread_clock else None
        self._prior_commit_present = 'commit' in self._db.__dict__
        self._prior_commit = self._db.__dict__.get('commit')
        original = self._db.commit

        def measured_commit(*args, **kwargs):
            began = self.monotonic()
            self._commits += 1
            try:
                return original(*args, **kwargs)
            except BaseException as error:
                self._commit_errors[_error_type(error)] += 1
                raise
            finally:
                elapsed = self._read_elapsed(began, 'commit_clock')
                if elapsed is not None:
                    self._commit_wall += elapsed
                else:
                    self._commit_unknown = True

        self._measured_commit = measured_commit
        try:
            self._db.commit = measured_commit
            self._wrapper = self._db.execute_wrapper(self._execute)
            self._wrapper.__enter__()
        except BaseException:
            try:
                self._restore_commit()
            except BaseException as error:
                self._diagnostic_errors.append({'stage': 'commit_restore', 'error_type': _error_type(error)})
            raise
        self._entered = True
        return self

    def _execute(self, execute, sql, params, many, context):
        operation = _operation(sql)
        began = self.monotonic()
        self._sql[operation] += 1
        try:
            return execute(sql, params, many, context)
        except BaseException as error:
            self._sql_errors[_error_type(error)] += 1
            raise
        finally:
            elapsed = self._read_elapsed(began, 'sql_clock')
            if elapsed is not None:
                self._sql_wall[operation] += elapsed
            else:
                self._sql_unknown.add(operation)

    def _read_elapsed(self, began, stage):
        try:
            elapsed = _number(self.monotonic()-began)
            if elapsed is None:
                self._diagnostic_errors.append({'stage': stage, 'error_type': 'InvalidClockValue'})
            return elapsed
        except BaseException as error:
            self._diagnostic_errors.append({'stage': stage, 'error_type': _error_type(error)})
            return None

    def _restore_commit(self):
        if self._db.__dict__.get('commit') is not self._measured_commit:
            self._diagnostic_errors.append({'stage': 'commit_restore', 'error_type': 'CommitWrapperChanged'})
            return
        if self._prior_commit_present:
            self._db.commit = self._prior_commit
        else:
            del self._db.commit

    def __exit__(self, exc_type, exc_value, traceback):
        self._error = _error_type(exc_value) if exc_value is not None else None
        interruption = None
        try:
            self._wrapper.__exit__(exc_type, exc_value, traceback)
        except BaseException as error:
            self._diagnostic_errors.append({'stage': 'execute_wrapper_close', 'error_type': _error_type(error)})
            interruption = error
        finally:
            try:
                self._restore_commit()
            except BaseException as error:
                self._diagnostic_errors.append({'stage': 'commit_restore', 'error_type': _error_type(error)})
                if not isinstance(error, Exception) and interruption is None:
                    interruption = error
            for name, clock in (('_ended', self.monotonic), ('_process_ended', self.process_clock),
                                ('_thread_ended', self.thread_clock)):
                try:
                    setattr(self, name, clock() if clock else None)
                except BaseException as error:
                    setattr(self, name, None)
                    self._diagnostic_errors.append({'stage': 'command_clock', 'error_type': _error_type(error)})
                    if not isinstance(error, Exception) and interruption is None:
                        interruption = error
            self._done = True
        if exc_value is None and interruption is not None:
            raise interruption
        return False

    def summary(self):
        def delta(end, start):
            return _number(end-start) if end is not None and start is not None else None
        wall = delta(getattr(self, '_ended', None), getattr(self, '_started', None))
        thread = delta(getattr(self, '_thread_ended', None), getattr(self, '_thread_started', None))
        process = delta(getattr(self, '_process_ended', None), getattr(self, '_process_started', None))
        return {'observed': self._entered, 'completed': self._done,
            'collection_complete': bool(self._entered and self._done and wall is not None and thread is not None
                and process is not None and not self._diagnostic_errors),
            'wall_seconds': wall, 'thread_cpu_seconds': thread, 'process_cpu_seconds': process,
            'process_cpu_deltas_overlap': True, 'error_type': self._error,
            'sql': {'count': sum(self._sql.values()), 'wall_seconds': None if self._sql_unknown else sum(self._sql_wall.values()),
                'operation_counts': dict(self._sql), 'operation_wall_seconds': {
                    name: None if name in self._sql_unknown else self._sql_wall[name] for name in self._sql},
                'error_type_counts': dict(self._sql_errors)},
            'physical_commit': {'attempts': self._commits, 'wall_seconds': None if self._commit_unknown else self._commit_wall,
                'error_type_counts': dict(self._commit_errors)},
            'diagnostic_errors': copy.deepcopy(self._diagnostic_errors)}


def counter_delta(before, after, names):
    """Unknown/missing/reset counters remain unknown, never zero observations."""
    values, reset = {}, []
    for name in names:
        start, end = _number((before or {}).get(name)), _number((after or {}).get(name))
        if start is None or end is None:
            values[name] = None
        elif end < start:
            values[name] = None
            reset.append(name)
        else:
            values[name] = end - start
    return {'values': values, 'reset_counters': reset}


def sanitize_resources(value):
    """Copy only a fixed numeric schema from the optional supplied sampler."""
    rows = value.get('containers') if isinstance(value, dict) else None
    if isinstance(rows, dict):
        rows = [{**row, 'role': role} for role, row in rows.items() if isinstance(row, dict) and role in CONTAINER_ROLES]
    if not isinstance(rows, list):
        return {'observed': False, 'containers': None, 'error_type': 'InvalidResourceSnapshot'}
    result = []
    for row in rows[:16]:
        if not isinstance(row, dict) or row.get('role') not in CONTAINER_ROLES:
            continue
        clean = {'role': row['role'], **{name: _number(row[name]) for name in RESOURCE_FIELDS if name in row}}
        clean['status'] = row.get('status') if row.get('status') in {'available', 'unavailable', 'known_stopped'} else 'unknown'
        clean['known_stopped'] = row.get('known_stopped') is True and clean['status'] == 'known_stopped'
        if clean['status'] == 'unavailable':
            clean['error_type'] = 'ResourceObservationUnavailable'
        # Native ContainerResources reports cgroup files separately from the
        # inspected main-PID fallback. Only cgroup counters qualify coverage.
        group = row.get('cgroup_v2')
        files = group.get('files') if isinstance(group, dict) else None
        if isinstance(files, dict):
            cpu = files.get('cpu_stat', {})
            memory = files.get('memory_current', {})
            # A later native read failure must invalidate this sample even
            # if an upstream row accidentally retains an earlier scalar.
            if clean['known_stopped']:
                for name in RESOURCE_FIELDS:
                    clean.pop(name, None)
            else:
                clean['cpu_total_seconds'] = None
                clean['memory_usage_bytes'] = None
            if cpu.get('status') == 'available' and isinstance(cpu.get('value'), dict):
                for source, target, divisor in (('usage_usec', 'cpu_total_seconds', 1e6),
                        ('user_usec', 'cpu_user_seconds', 1e6), ('system_usec', 'cpu_system_seconds', 1e6),
                        ('throttled_usec', 'cpu_throttled_seconds', 1e6),
                        ('nr_throttled', 'cpu_throttled_periods', 1), ('nr_periods', 'cpu_periods', 1)):
                    number = _number(cpu['value'].get(source))
                    clean[target] = number/divisor if number is not None else None
            if memory.get('status') == 'available':
                clean['memory_usage_bytes'] = _number(memory.get('value'))
            clean['required_counter_status'] = {
                'cpu_stat': cpu.get('status') if cpu.get('status') in {'available', 'unavailable'} else 'unknown',
                'memory_current': memory.get('status') if memory.get('status') in {'available', 'unavailable'} else 'unknown'}
            for name, field in (('cpu_stat', cpu), ('memory_current', memory)):
                if field.get('status') == 'unavailable':
                    error = field.get('error_type')
                    if isinstance(error, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', error):
                        clean['required_counter_status'][name+'_error_type'] = error
            io = files.get('io_stat', {})
            if io.get('status') == 'available' and isinstance(io.get('value'), dict):
                for source, target in (('rbytes', 'io_read_bytes'), ('wbytes', 'io_write_bytes')):
                    values = [_number(record.get(source)) for record in io['value'].values() if isinstance(record, dict)]
                    clean[target] = sum(values) if values and all(number is not None for number in values) else None
        clean['cpu_scope'] = 'container_cgroup_subtree' if clean.get('cpu_total_seconds') is not None else 'unavailable'
        clean['memory_scope'] = 'container_cgroup_subtree' if clean.get('memory_usage_bytes') is not None else 'unavailable'
        result.append(clean)
    return {'observed': True, 'containers': result, 'truncated': len(rows) > 16,
        'supplied_container_count': len(rows), 'retained_container_count': len(result)}


def _read(path):
    return Path(path).read_text(encoding='utf-8')


def _key_numbers(text, suffix=''):
    values = {}
    for line in text.splitlines():
        parts = line.replace(':', ' ').split()
        if len(parts) >= 2 and parts[1].isdigit():
            values[parts[0]+suffix] = int(parts[1])
    return values


def _cgroup_directory():
    try:
        for line in _read('/proc/self/cgroup').splitlines():
            hierarchy, controllers, path = line.split(':', 2)
            if hierarchy == '0' and not controllers:
                root = Path('/sys/fs/cgroup')
                candidate = root / path.lstrip('/')
                # No discovered path crosses into evidence.
                if candidate.is_dir() and candidate.is_relative_to(root):
                    return candidate
    except (OSError, ValueError):
        pass
    return Path('/sys/fs/cgroup')


def system_facts():
    facts = {'platform': platform.system(), 'python_version': platform.python_version(),
        'python_implementation': platform.python_implementation(), 'logical_cpu_count': os.cpu_count(),
        'physical_core_count': None, 'affinity_cpus': None, 'cgroup_cpu_quota_cores': None,
        'cgroup_cpu_period_microseconds': None, 'cgroup_memory_limit_bytes': None, 'errors': []}
    if hasattr(os, 'sched_getaffinity'):
        try:
            facts['affinity_cpus'] = sorted(os.sched_getaffinity(0))
        except OSError as error:
            facts['errors'].append({'stage': 'cpu_affinity', 'error_type': _error_type(error)})
    if platform.system() == 'Linux':
        try:
            cores = set()
            for section in _read('/proc/cpuinfo').strip().split('\n\n'):
                data = {key.strip(): value.strip() for key, value in
                        (line.split(':', 1) for line in section.splitlines() if ':' in line)}
                if 'physical id' in data and 'core id' in data:
                    cores.add((data['physical id'], data['core id']))
            facts['physical_core_count'] = len(cores) if cores else None
        except OSError as error:
            facts['errors'].append({'stage': 'physical_cores', 'error_type': _error_type(error)})
        root = _cgroup_directory()
        try:
            quota, period = _read(root/'cpu.max').strip().split()
            facts['cgroup_cpu_period_microseconds'] = int(period)
            facts['cgroup_cpu_quota_cores'] = None if quota == 'max' else int(quota)/int(period)
            facts['cgroup_cpu_quota_unlimited'] = quota == 'max'
        except (OSError, ValueError, ZeroDivisionError) as error:
            facts['errors'].append({'stage': 'cgroup_cpu_quota', 'error_type': _error_type(error)})
        try:
            limit = _read(root/'memory.max').strip()
            facts['cgroup_memory_limit_bytes'] = None if limit == 'max' else int(limit)
            facts['cgroup_memory_limit_unlimited'] = limit == 'max'
        except (OSError, ValueError) as error:
            facts['errors'].append({'stage': 'cgroup_memory_limit', 'error_type': _error_type(error)})
    return facts


def process_snapshot():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    result = {'user_cpu_seconds': usage.ru_utime, 'system_cpu_seconds': usage.ru_stime,
        'max_rss_bytes': usage.ru_maxrss*(1 if platform.system() == 'Darwin' else 1024),
        'rss_bytes': None, 'io': None, 'threads': None, 'errors': []}
    if platform.system() != 'Linux':
        return result
    try:
        values = _key_numbers(_read('/proc/self/status'))
        result['rss_bytes'] = values.get('VmRSS', 0)*1024 if 'VmRSS' in values else None
    except OSError as error:
        result['errors'].append({'stage': 'process_memory', 'error_type': _error_type(error)})
    try:
        data = _key_numbers(_read('/proc/self/io'))
        result['io'] = {key: data.get(key) for key in ('rchar', 'wchar', 'syscr', 'syscw', 'read_bytes', 'write_bytes', 'cancelled_write_bytes')}
    except OSError as error:
        result['errors'].append({'stage': 'process_io', 'error_type': _error_type(error)})
    threads = []
    ticks = os.sysconf('SC_CLK_TCK')
    for thread in threading.enumerate()[:64]:
        ident = thread.native_id
        if ident is None:
            continue
        name = thread.name if thread.name in {'MainThread', 'runtime-diagnostics'} or re.fullmatch(r'paced-business-lane-[0-3]', thread.name) else 'other'
        try:
            data = _read(f'/proc/self/task/{ident}/stat')
            fields = data[data.rfind(')')+2:].split()
            threads.append({'role': name, 'user_cpu_seconds': int(fields[11])/ticks,
                'system_cpu_seconds': int(fields[12])/ticks})
        except (OSError, ValueError, IndexError):
            # A terminating lane can disappear between enumeration and read.
            result['errors'].append({'stage': 'thread_cpu', 'error_type': 'ThreadObservationUnavailable'})
    result['threads'] = threads
    return result


def host_snapshot():
    result = {'load_average': None, 'memory_bytes': None, 'cpu_jiffies': None,
        'cgroup_cpu': None, 'cgroup_memory_bytes': None, 'errors': []}
    if hasattr(os, 'getloadavg'):
        try:
            result['load_average'] = list(os.getloadavg())
        except OSError as error:
            result['errors'].append({'stage': 'host_load', 'error_type': _error_type(error)})
    if platform.system() != 'Linux':
        return result
    readers = (
        ('host_memory', '/proc/meminfo', lambda text: {key: value*1024 for key, value in _key_numbers(text).items()
            if key in {'MemTotal', 'MemAvailable', 'MemFree', 'SwapTotal', 'SwapFree'}}, 'memory_bytes'),
        ('host_cpu', '/proc/stat', lambda text: dict(zip(('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal'),
            [int(value) for value in text.splitlines()[0].split()[1:9]])), 'cpu_jiffies'),
        ('cgroup_cpu', _cgroup_directory()/'cpu.stat', _key_numbers, 'cgroup_cpu'),
        ('cgroup_memory', _cgroup_directory()/'memory.current', lambda text: int(text.strip()), 'cgroup_memory_bytes'))
    for stage, path, convert, field in readers:
        try:
            result[field] = convert(_read(path))
        except (OSError, ValueError, IndexError) as error:
            result['errors'].append({'stage': stage, 'error_type': _error_type(error)})
    return result


def _category(value):
    if value is None:
        return 'none'
    return value if isinstance(value, str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_ ]{0,79}', value) else 'unknown'


class PostgreSQLSampler:
    """Copied, autocommit wrapper created, used and closed by one sampler thread."""
    def __init__(self, alias):
        self.alias = alias
        self.connection = None

    def open(self):
        from django.db import connections
        base = connections[self.alias]
        if base.vendor != 'postgresql':
            raise ValueError('Diagnostics require PostgreSQL')
        self.connection = base.copy(alias='runtime_diagnostics')
        self.connection.settings_dict['AUTOCOMMIT'] = True
        self.connection.settings_dict['CONN_MAX_AGE'] = None
        self.connection.settings_dict['OPTIONS'] = dict(self.connection.settings_dict.get('OPTIONS', {}))
        self.connection.settings_dict['OPTIONS'].pop('pool', None)
        self.connection.settings_dict['OPTIONS']['connect_timeout'] = CONNECT_TIMEOUT_SECONDS
        previous_options = self.connection.settings_dict['OPTIONS'].get('options', '')
        self.connection.settings_dict['OPTIONS']['options'] = (previous_options +
            ' -c statement_timeout=250ms -c lock_timeout=100ms -c application_name=labops.runtime_diagnostics').strip()
        self.connection.ensure_connection()
        with self.connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = '250ms'")
            cursor.execute("SET lock_timeout = '100ms'")
            cursor.execute("SET application_name = 'labops.runtime_diagnostics'")

    def _query(self, sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]

    def snapshot(self):
        result = {'observed': False, 'settings': None, 'activities': None, 'activity_counts': None,
            'locks': None, 'database': None, 'wal': None, 'errors': []}
        if self.connection is None:
            try:
                self.open()
            except Exception as error:
                result['errors'].append({'stage': 'database_connect', 'error_type': _error_type(error)})
                self.close()
                return result
        queries = (
            ('settings', "SELECT name,setting,unit FROM pg_settings WHERE name IN ('max_connections','shared_buffers','track_io_timing','track_wal_io_timing','synchronous_commit','fsync','track_commit_timestamp')"),
            ('activities', """SELECT pid,backend_type,state,wait_event_type,wait_event,
                CASE WHEN usename=current_user THEN 'same_database_user' ELSE 'other_database_user' END AS user_role,
                CASE WHEN application_name='' THEN 'unset'
                     WHEN application_name='labops.runtime_diagnostics' THEN 'diagnostics'
                     ELSE 'other' END AS application_role, pg_blocking_pids(pid) AS blocking_pids
                FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                ORDER BY pid LIMIT 257"""),
            ('locks', """SELECT locktype,mode,granted,count(*) AS count FROM pg_locks
                WHERE pid<>pg_backend_pid() AND (database=(SELECT oid FROM pg_database WHERE datname=current_database())
                    OR pid IN (SELECT pid FROM pg_stat_activity WHERE datname=current_database()))
                GROUP BY locktype,mode,granted ORDER BY locktype,mode,granted"""),
            ('database', 'SELECT numbackends,'+','.join(DATABASE_COUNTERS)+' FROM pg_stat_database WHERE datname=current_database()'),
            ('wal', 'SELECT '+','.join(WAL_COUNTERS)+' FROM pg_stat_wal'))
        for field, sql in queries:
            try:
                rows = self._query(sql)
                if field == 'activities':
                    result['activities_truncated'] = len(rows) > MAX_BACKENDS
                    rows = rows[:MAX_BACKENDS]
                    clean = []
                    counts = Counter()
                    for row in rows:
                        state = row['state'] if row['state'] in {'active', 'idle', 'idle in transaction', 'idle in transaction (aborted)', 'fastpath function call', 'disabled'} else 'unknown'
                        wait_type, wait = _category(row['wait_event_type']), _category(row['wait_event'])
                        row_clean = {'backend_pid': int(row['pid']), 'backend_type': _category(row['backend_type']),
                            'state': state, 'wait_event_type': wait_type, 'wait_event': wait,
                            'user_role': row['user_role'] if row['user_role'] in {'same_database_user', 'other_database_user'} else 'unknown',
                            'application_role': row['application_role'] if row['application_role'] in {'unset', 'diagnostics', 'other'} else 'unknown',
                            'blocking_pids': [int(value) for value in row['blocking_pids'][:64]]}
                        clean.append(row_clean)
                        counts[(state, wait_type, row_clean['user_role'], row_clean['application_role'])] += 1
                    result[field] = clean
                    result['activity_counts'] = [{'state': key[0], 'wait_event_type': key[1],
                        'user_role': key[2], 'application_role': key[3], 'count': value} for key, value in sorted(counts.items())]
                elif field in {'database', 'wal'}:
                    result[field] = {key: _number(value) for key, value in rows[0].items()} if rows else None
                elif field == 'locks':
                    result[field] = [{'locktype': _category(row['locktype']), 'mode': _category(row['mode']),
                        'granted': bool(row['granted']), 'count': _number(row['count'])} for row in rows[:128]]
                else:
                    clean = []
                    for row in rows:
                        name, setting = row['name'], row['setting']
                        if name in {'max_connections', 'shared_buffers'}:
                            setting = int(setting) if isinstance(setting, str) and setting.isdigit() else None
                        elif name in {'track_io_timing', 'track_wal_io_timing', 'fsync', 'track_commit_timestamp'}:
                            setting = setting if setting in {'on', 'off'} else None
                        elif name == 'synchronous_commit':
                            setting = setting if setting in {'on', 'off', 'local', 'remote_write', 'remote_apply'} else None
                        else:
                            continue
                        clean.append({'name': name, 'setting': setting,
                            'unit': row['unit'] if row['unit'] in {None, '8kB', 'kB', 'B', 'MB', 'ms', 's'} else None})
                    result[field] = clean
            except Exception as error:
                result['errors'].append({'stage': 'database_'+field, 'error_type': _error_type(error)})
        result['observed'] = result['activities'] is not None
        return result

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            finally:
                self.connection = None


class RuntimeDiagnostics:
    def __init__(self, path, *, scenario, database_alias='default', resource_sampler=None,
                 interval_seconds=INTERVAL_SECONDS, max_samples=MAX_SAMPLES,
                 expected_resource_roles=None, known_stopped_roles=(),
                 database_sampler=None, process_reader=None, host_reader=None, facts_reader=None):
        if not isinstance(scenario, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,95}', scenario):
            raise ValueError('Expected an authored diagnostic scenario')
        if isinstance(interval_seconds, bool) or not isinstance(interval_seconds, (int, float)) or not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError('Expected positive finite diagnostic interval')
        if type(max_samples) is not int or not 1 <= max_samples <= MAX_SAMPLES:
            raise ValueError('Invalid diagnostic sample bound')
        self.path, self.scenario = Path(path), scenario
        self.interval, self.max_samples = interval_seconds, max_samples
        self.resource_sampler = resource_sampler
        self.expected_resource_roles = tuple(sorted(CONTAINER_ROLES if expected_resource_roles is None else expected_resource_roles))
        self.known_stopped_roles = tuple(sorted(known_stopped_roles))
        if (not set(self.expected_resource_roles).issubset(CONTAINER_ROLES)
                or not set(self.known_stopped_roles).issubset(CONTAINER_ROLES)
                or set(self.expected_resource_roles) & set(self.known_stopped_roles)):
            raise ValueError('Invalid running/stopped container contract')
        self.database_sampler = database_sampler or PostgreSQLSampler(database_alias)
        self.process_reader = process_reader or process_snapshot
        self.host_reader = host_reader or host_snapshot
        self.facts_reader = facts_reader or system_facts
        self._stop = threading.Event()
        self._sampler_done = threading.Event()
        self._sampler_started = threading.Event()
        self._lock = threading.Lock()
        self._samples = []
        self._errors = []
        self._truncated = False
        self._joined = False
        self._overhead_wall = 0.0
        self._overhead_cpu = 0.0
        self._cleanup_complete = False
        self._persisted = False
        self._start_complete = False

    def __enter__(self):
        self._started = time.monotonic()
        self._started_wall = time.time()
        self._initial_process = self.process_reader()
        self._facts = self.facts_reader()
        self._thread = threading.Thread(target=self._run, name='runtime-diagnostics', daemon=True)
        try:
            self._thread.start()
        except BaseException as error:
            self._errors.append({'stage': 'sampler_start', 'error_type': _error_type(error)})
            self._stop.set()
            # _run acknowledges before it can open a database session. If the
            # native worker has not entered yet, the stop gate prevents any
            # late database work; an already entered worker must finish close.
            if self._sampler_started.is_set() or self._thread.ident is not None:
                self._finish_thread()
            raise
        self._start_complete = True
        return self

    def _safe_read(self, reader, stage, interruptions=None):
        try:
            return reader()
        except BaseException as error:
            if interruptions is not None and not isinstance(error, Exception):
                interruptions.append(error)
            return {'observed': False, 'value': None, 'errors': [{'stage': stage, 'error_type': _error_type(error)}]}

    def _run(self):
        self._sampler_started.set()
        try:
            while not self._stop.is_set():
                if len(self._samples) >= self.max_samples:
                    self._truncated = True
                    break
                began, cpu = time.monotonic(), time.thread_time()
                sample = {'recorded_at': time.time(), 'elapsed_seconds': began-self._started,
                    'process': self._safe_read(self.process_reader, 'process_sample'),
                    'host': self._safe_read(self.host_reader, 'host_sample'),
                    'database': self._safe_read(self.database_sampler.snapshot, 'database_sample'),
                    'resources': None}
                if self.resource_sampler is not None:
                    sample['resources'] = self._safe_read(lambda: sanitize_resources(self.resource_sampler()), 'container_sample')
                duration, cpu_duration = time.monotonic()-began, time.thread_time()-cpu
                sample['sampler_wall_seconds'] = duration
                sample['sampler_thread_cpu_seconds'] = cpu_duration
                with self._lock:
                    self._samples.append(sample)
                    self._overhead_wall += duration
                    self._overhead_cpu += cpu_duration
                self._stop.wait(max(0.0, self.interval-duration))
        except BaseException as error:
            self._errors.append({'stage': 'sampler_loop', 'error_type': _error_type(error)})
        finally:
            try:
                self.database_sampler.close()
            except BaseException as error:
                self._errors.append({'stage': 'database_sampler_close', 'error_type': _error_type(error)})
            else:
                self._cleanup_complete = True
            finally:
                self._sampler_done.set()

    def _finish_thread(self):
        self._stop.set()
        # Startup and server-side statements have configured budgets. They
        # cannot attest a hard client I/O deadline; callbacks must return so
        # this thread can finish cleanup in its owning thread.
        interruption = None
        while not self._sampler_done.is_set():
            try:
                self._sampler_done.wait(timeout=.1)
            except BaseException as error:
                if interruption is None:
                    interruption = error
        while True:
            try:
                self._thread.join()
                break
            except BaseException as error:
                if interruption is None:
                    interruption = error
        self._joined = True
        return interruption

    def __exit__(self, exc_type, exc_value, traceback):
        interruption = self._finish_thread()
        final_interruptions = []
        try:
            self._ended = time.monotonic()
        except BaseException as error:
            self._ended = None
            self._errors.append({'stage': 'final_wall_clock', 'error_type': _error_type(error)})
            if not isinstance(error, Exception):
                final_interruptions.append(error)
        self._final_process = self._safe_read(self.process_reader, 'final_process', final_interruptions)
        if interruption is None and final_interruptions:
            interruption = final_interruptions[0]
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.exists():
                raise FileExistsError('Diagnostic artifact already exists')
            pending = self.path.with_name(self.path.name+'.pending')
            with pending.open('x', encoding='utf-8') as stream:
                json.dump(self._artifact(), stream, sort_keys=True, allow_nan=False)
                stream.write('\n')
            pending.replace(self.path)
            self._persisted = True
        except BaseException as error:
            self._errors.append({'stage': 'diagnostics_persist', 'error_type': _error_type(error)})
            if exc_value is None and interruption is None:
                raise
        if exc_value is None and interruption is not None:
            raise interruption
        return False

    def _artifact(self):
        # This prospective state becomes visible at the final path only after
        # the complete JSON has been closed and atomically published.
        return {**self._summary(persisted=True), 'samples': copy.deepcopy(self._samples)}

    def summary(self):
        return self._summary(persisted=self._persisted)

    def _summary(self, *, persisted):
        first = getattr(self, '_initial_process', {})
        last = getattr(self, '_final_process', {})
        database_samples = [(sample['elapsed_seconds'], sample['database'].get('database'))
            for sample in self._samples if isinstance(sample.get('database'), dict) and sample['database'].get('database') is not None]
        wal_samples = [(sample['elapsed_seconds'], sample['database'].get('wal'))
            for sample in self._samples if isinstance(sample.get('database'), dict) and sample['database'].get('wal') is not None]
        errors = copy.deepcopy(self._errors)
        sample_errors = Counter()
        resources = [sample['resources'] for sample in self._samples if isinstance(sample.get('resources'), dict)]
        covered = set()
        stopped_observed = set()
        resource_errors = False
        database_complete = bool(self._samples)
        for sample in self._samples:
            database = sample.get('database')
            if (not isinstance(database, dict) or not database.get('observed') or database.get('errors')
                    or database.get('activities') is None or database.get('locks') is None or database.get('database') is None):
                database_complete = False
            for item in (database or {}).get('errors', []) if isinstance(database, dict) else []:
                if isinstance(item, dict) and isinstance(item.get('error_type'), str):
                    sample_errors[item['error_type']] += 1
        for resource in resources:
            if not resource.get('observed'):
                resource_errors = True
                sample_errors['ResourceObservationUnavailable'] += 1
            sample_covered, sample_stopped = set(), set()
            for row in resource.get('containers') or []:
                if row.get('known_stopped'):
                    stopped_observed.add(row['role'])
                    sample_stopped.add(row['role'])
                elif row.get('status') == 'unavailable':
                    resource_errors = True
                    sample_errors['ResourceObservationUnavailable'] += 1
                elif row.get('cpu_total_seconds') is not None and row.get('memory_usage_bytes') is not None:
                    covered.add(row['role'])
                    sample_covered.add(row['role'])
            if (not set(self.expected_resource_roles).issubset(sample_covered)
                    or not set(self.known_stopped_roles).issubset(sample_stopped)):
                resource_errors = True
                sample_errors['RequiredResourceCounterUnavailable'] += 1
        required = set(self.expected_resource_roles)
        expected_stopped = set(self.known_stopped_roles)
        cpu_delta = counter_delta(first, last, ('user_cpu_seconds', 'system_cpu_seconds'))
        wall = _number(self._ended-self._started) if getattr(self, '_ended', None) is not None else None
        lifecycle_complete = bool(self._start_complete and self._joined and self._cleanup_complete and persisted and not errors)
        collection_complete = bool(lifecycle_complete and not self._truncated
            and wall is not None and all(value is not None for value in cpu_delta['values'].values())
            and not cpu_delta['reset_counters'] and database_complete and not resource_errors
            and required.issubset(covered) and expected_stopped.issubset(stopped_observed))
        return {'scenario': self.scenario, 'profile': diagnostics_profile(),
            'artifact_name': self.path.name, 'facts': copy.deepcopy(getattr(self, '_facts', None)),
            'started_at': getattr(self, '_started_wall', None),
            'wall_seconds': wall, 'collection_complete': collection_complete,
            'lifecycle_complete': lifecycle_complete, 'persisted': persisted,
            'process_cpu_delta': cpu_delta,
            'initial_process': copy.deepcopy(first), 'final_process': copy.deepcopy(last),
            'sample_count': len(self._samples), 'max_samples': self.max_samples,
            'interval_seconds': self.interval, 'samples_truncated': self._truncated,
            'sampler_joined': self._joined, 'sampler_wall_seconds': self._overhead_wall,
            'sampler_thread_cpu_seconds': self._overhead_cpu,
            'resource_sampler_supplied': self.resource_sampler is not None,
            'expected_resource_roles': list(self.expected_resource_roles),
            'known_stopped_roles': list(self.known_stopped_roles),
            'observed_cpu_memory_roles': sorted(covered), 'observed_known_stopped_roles': sorted(stopped_observed),
            'sample_error_type_counts': dict(sample_errors),
            'database_delta': counter_delta(database_samples[0][1], database_samples[-1][1], DATABASE_COUNTERS) if len(database_samples)>1 else None,
            'database_delta_coverage_seconds': database_samples[-1][0]-database_samples[0][0] if len(database_samples)>1 else None,
            'wal_delta': counter_delta(wal_samples[0][1], wal_samples[-1][1], WAL_COUNTERS) if len(wal_samples)>1 else None,
            'wal_delta_coverage_seconds': wal_samples[-1][0]-wal_samples[0][0] if len(wal_samples)>1 else None,
            'errors': errors}
