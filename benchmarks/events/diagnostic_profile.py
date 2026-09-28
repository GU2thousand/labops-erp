"""Opt-in, own-thread CPU profiling for disposable validation processes only.

No raw stats, arguments, locals, SQL, exception messages or absolute filenames
are persisted. Profiling is overhead-bearing diagnosis, never acceptance.
"""
from contextlib import contextmanager
from functools import wraps
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import sysconfig
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
PHASES = frozenset({'generator_command', 'receipt_create', 'receipt_post',
    'provenance_journal', 'publisher_lifecycle', 'publish_one_composite',
    'budget_setup_composite', 'budget_restore_composite', 'claim_commit_composite',
    'envelope_composite', 'shard_ownership', 'lease_check', 'send_composite', 'idle_wait'})


def request_profile(enabled):
    if type(enabled) is not bool:
        raise ValueError('Diagnostic profile request must be a boolean')
    return {'enabled': enabled, 'applicable': enabled,
        'request_status': 'REQUESTED' if enabled else 'NOT_REQUESTED',
        'qualification_admissible': not enabled,
        'timer': 'thread_time_ns', 'timeunit_seconds': 1e-9,
        'wall_clock': 'perf_counter_ns', 'scope': 'own main thread only',
        'writer_roles': 4, 'publisher_roles': 1,
        'background_threads_included': False, 'native_producer_proxy': False,
        'raw_stats_written': False, 'nested_times_additive': False}


def _safe_name(value):
    if isinstance(value, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*|<(?:module|lambda|listcomp|dictcomp|setcomp|genexpr)>', value):
        return value
    return 'name-sha256:' + hashlib.sha256(str(value).encode()).hexdigest()


def sanitized_stats(stats):
    """Keep the entire function/caller graph with collision-safe opaque IDs."""
    roots = [('repository', ROOT), ('site-packages', Path(sysconfig.get_path('purelib'))),
             ('stdlib', Path(sysconfig.get_path('stdlib')))]
    keys = set(stats)
    for values in stats.values():
        keys.update(values[4])
    identities, used = {}, {}
    sources = {}
    for key in sorted(keys, key=lambda value: repr(value)):
        filename, line, function = key
        source = 'unknown-sha256:' + hashlib.sha256(str(filename).encode()).hexdigest()
        if filename == '~':
            source = 'builtin'
        elif Path(filename).is_absolute():
            path = Path(filename).resolve()
            for label, root in roots:
                try:
                    relative = path.relative_to(root.resolve())
                except ValueError:
                    continue
                repository_source = (label != 'repository' or (relative.parts[0] in {'labops', 'benchmarks', 'config'}
                    or relative.as_posix() == 'manage.py'))
                if (not repository_source or path.suffix != '.py' or not path.is_file()
                        or set(relative.parts) & {'generated', 'private', 'secrets', 'outputs'}):
                    break
                source = label + '/' + relative.as_posix()
                if label == 'repository' and path.is_file():
                    sources[source] = hashlib.sha256(path.read_bytes()).hexdigest()
                break
        identity = hashlib.sha256(json.dumps([source, line, function], separators=(',', ':')).encode()).hexdigest()
        if identity in used and used[identity] != key:
            raise ValueError('Profile function identity collision')
        used[identity] = key
        name = (_safe_name(function) if not source.startswith('unknown-') else
                'name-sha256:' + hashlib.sha256(str(function).encode()).hexdigest())
        identities[key] = {'id': identity, 'source': source, 'line': int(line), 'function': name}
    functions = []
    for key, (primitive, total, own, cumulative, callers) in stats.items():
        if any(not math.isfinite(number) or number < 0 for number in (primitive, total, own, cumulative)):
            raise ValueError('Invalid profile statistic')
        graph = []
        for caller, values in callers.items():
            if not isinstance(values, tuple) or len(values) != 4:
                raise ValueError('Unexpected CPU profile caller schema')
            if any(not math.isfinite(number) or number < 0 for number in values):
                raise ValueError('Invalid profile caller statistic')
            graph.append({'caller_id': identities[caller]['id'], 'primitive_calls': values[0],
                'total_calls': values[1], 'self_cpu_seconds': values[2],
                'cumulative_cpu_seconds': values[3]})
        functions.append({**identities[key], 'primitive_calls': primitive, 'total_calls': total,
            'self_cpu_seconds': own, 'cumulative_cpu_seconds': cumulative,
            'callers': sorted(graph, key=lambda row: row['caller_id'])})
    return {'functions': sorted(functions, key=lambda row: row['id']),
        'function_identities': sorted(identities.values(), key=lambda row: row['id']),
        'repository_source_sha256': sources}


class UnsupportedProfileEngine(RuntimeError):
    """The engine did not bind its callback to the owning thread."""


class CPUProfile:
    """One profiler on one owning thread; diagnostic failure never changes work."""
    def __init__(self, role, *, lane=None, profiler_factory=None):
        if role not in {'generator', 'publisher'} or (role == 'generator' and lane not in range(4)):
            raise ValueError('Invalid fixed profile role')
        self.role, self.lane = role, lane
        self.owner = threading.get_ident()
        self.profiler = None
        self.active = False
        self.errors, self.phases, self.calls, self.hooks = [], [], [], []
        self.ordinal = None
        self.closed = False
        self.persisted = False
        self.hooks_restored = False
        self.hook_restoration_failed = False
        self.hook_sources = {}
        self.recording_failed = False
        self.graph_unavailable = False
        try:
            if sys.getprofile() is not None:
                raise RuntimeError('ExistingProfileHook')
            if profiler_factory is None:
                from cProfile import Profile
                profiler_factory = Profile
            self.profiler = profiler_factory(timer=time.thread_time_ns, timeunit=1e-9)
            if self.profiler is None:
                raise UnsupportedProfileEngine()
        except BaseException as error:
            self.graph_unavailable = True
            self.record_error('admission', error)

    def error(self, stage, error):
        try:
            self.errors.append({'stage': stage, 'error_type': _safe_name(type(error).__name__)})
        except BaseException:
            self.recording_failed = True

    def record_error(self, stage, error):
        try:
            self.error(stage, error)
        except BaseException:
            self.recording_failed = True

    def owning_thread(self):
        return threading.get_ident() == self.owner

    @contextmanager
    def phase(self, name):
        if name not in PHASES:
            raise ValueError('Unknown fixed profile phase')
        if not self.owning_thread() or not self.active:
            yield
            return
        start = None
        original = None
        try:
            start = (time.perf_counter_ns(), time.thread_time_ns())
        except BaseException as error:
            self.record_error('phase_clock_start', error)
        try:
            yield
        except BaseException as error:
            original = error
            raise
        finally:
            try:
                end = (time.perf_counter_ns(), time.thread_time_ns())
                self.phases.append({'phase': name, 'ordinal': self.ordinal,
                    'wall_ns': end[0] - start[0] if start else None,
                    'thread_cpu_ns': end[1] - start[1] if start else None,
                    'outcome': 'error' if original else 'returned',
                    'exception_type': _safe_name(type(original).__name__) if original else None,
                    'complete': start is not None})
            except BaseException as error:
                self.record_error('phase_clock_end', error)

    @contextmanager
    def call(self, phase, ordinal=None):
        if not self.owning_thread():
            yield
            return
        if self.active:
            with self.phase(phase):
                yield
            return
        self.ordinal = ordinal
        original = None
        enabled = False
        try:
            if self.profiler is not None:
                if sys.getprofile() is not None:
                    raise RuntimeError('ExistingProfileHook')
                self.profiler.enable()
                if sys.getprofile() is not self.profiler:
                    # A global monitoring engine can collect other threads,
                    # whose thread CPU clocks cannot form this own-thread graph.
                    self.graph_unavailable = True
                    raise UnsupportedProfileEngine()
                self.active = enabled = True
        except BaseException as error:
            self.record_error('enable', error)
            # enable can install its callback before raising. Remove an owned
            # partial hook before business execution, without touching foreign
            # callbacks or replacing the business exception that follows.
            self.disable_owned()
        try:
            with self.phase(phase):
                yield
        except BaseException as error:
            original = error
            raise
        finally:
            if enabled:
                self.disable_owned()
            self.active = False
            try:
                self.calls.append({'ordinal': ordinal, 'profiled': enabled,
                    'outcome': 'error' if original else 'returned',
                    'exception_type': _safe_name(type(original).__name__) if original else None})
            except BaseException as error:
                self.record_error('call_record', error)

    def disable_owned(self):
        try:
            current = sys.getprofile()
            if current is not None and current is not self.profiler:
                self.record_error('disable', RuntimeError('ForeignProfileHook'))
                return
            if self.profiler is not None:
                self.profiler.disable()
        except BaseException as error:
            self.record_error('disable', error)
        finally:
            try:
                if self.profiler is not None and sys.getprofile() is self.profiler:
                    sys.setprofile(None)
            except BaseException as error:
                self.record_error('disable_cleanup', error)

    def hook(self, target, name, phase, *, expected=None, ordinal=False):
        if self.profiler is None:
            return
        try:
            original = getattr(target, name)
            same = (original is expected or (getattr(original, '__func__', None) is not None
                and original.__func__ is getattr(expected, '__func__', None)
                and original.__self__ is getattr(expected, '__self__', None)))
            if expected is not None and not same:
                raise RuntimeError('UnexpectedCallableBinding')
            function = getattr(original, '__func__', original)
            code = getattr(function, '__code__', None)
            if code is not None:
                source = Path(code.co_filename).resolve()
                if (source.is_relative_to(ROOT) and source.suffix == '.py' and source.is_file()
                        and source.relative_to(ROOT).parts[0] in {'labops', 'benchmarks', 'config'}):
                    self.hook_sources[source.relative_to(ROOT).as_posix()] = hashlib.sha256(source.read_bytes()).hexdigest()
            previous = vars(target).get(name)
            locally_defined = name in vars(target)
            count = 0
            @wraps(original)
            def wrapper(*args, **kwargs):
                nonlocal count
                prior = self.ordinal
                if ordinal and self.owning_thread() and self.active:
                    count += 1
                    self.ordinal = count
                try:
                    with self.phase(phase):
                        return original(*args, **kwargs)
                finally:
                    if ordinal and self.owning_thread():
                        self.ordinal = prior
            setattr(target, name, wrapper)
            self.hooks.append((target, name, previous, wrapper, locally_defined))
        except BaseException as error:
            self.record_error('hook_install', error)

    def restore(self):
        for target, name, previous, wrapper, locally_defined in reversed(self.hooks):
            try:
                if getattr(target, name) is not wrapper:
                    raise RuntimeError('ProfileHookChanged')
                if locally_defined:
                    setattr(target, name, previous)
                else:
                    delattr(target, name)
            except BaseException as error:
                self.hook_restoration_failed = True
                self.record_error('hook_restore', error)
        self.hooks.clear()
        self.hooks_restored = (not self.hook_restoration_failed
            and not any(row['stage'] == 'hook_restore' for row in self.errors))

    def summary(self):
        return {'role': self.role, 'lane': self.lane, 'requested_calls': len(self.calls),
            'profiled_calls': sum(row['profiled'] for row in self.calls),
            'returned_calls': sum(row['outcome'] == 'returned' for row in self.calls),
            'error_calls': sum(row['outcome'] == 'error' for row in self.calls),
            'closed': self.closed, 'persisted': self.persisted,
            'hooks_restored': self.hooks_restored, 'errors': list(self.errors),
            'recording_failed': self.recording_failed,
            'function_graph_unavailable': self.graph_unavailable,
            'complete': self.closed and self.persisted and self.hooks_restored and bool(self.calls)
                and all(row['profiled'] for row in self.calls) and not self.errors
                and not self.recording_failed and not self.graph_unavailable}

    def close(self, path):
        """Export once on owner after all work; preserve every primary failure."""
        if self.closed:
            return self.summary()
        if not self.owning_thread():
            self.record_error('close', RuntimeError('WrongProfileOwner'))
            return self.summary()
        self.restore()
        self.closed = True
        try:
            graph = {'functions': [], 'function_identities': [], 'repository_source_sha256': {}}
            try:
                if self.profiler is not None and not self.graph_unavailable:
                    from pstats import Stats
                    graph = sanitized_stats(Stats(self.profiler).stats)
            except BaseException as error:
                # Preserve independently recorded owner-thread phases even if
                # cProfile statistics cannot form a safe graph. Set the flag
                # first: a broken error sink must never promote this evidence.
                self.graph_unavailable = True
                self.record_error('export', error)
            value = {'schema_version': 1, **request_profile(True),
                'python_version': sys.version.split()[0], 'pid': os.getpid(),
                'thread_native_id': threading.get_native_id(),
                'timer_resolution_seconds': time.get_clock_info('thread_time').resolution,
                'scope': 'own main thread; cProfile enabled only during recorded calls',
                'coverage': self.summary(), 'calls': self.calls, 'phases': self.phases, **graph}
            value['function_graph_status'] = 'UNAVAILABLE' if self.graph_unavailable else 'COMPLETE'
            value['hook_source_sha256'] = self.hook_sources
            # Payload says persisted only if exclusive creation, flush/fsync
            # and atomic replacement succeed; completion is checked by readers.
            value['coverage'].update(persisted=True, complete=bool(self.calls)
                and all(row['profiled'] for row in self.calls) and self.hooks_restored
                and not self.errors and not self.recording_failed and not self.graph_unavailable)
            path = Path(path)
            temporary = path.with_suffix('.tmp')
            with temporary.open('x') as stream:
                json.dump(value, stream, sort_keys=True, allow_nan=False)
                stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
            if path.exists():
                raise FileExistsError('Profile evidence already exists')
            temporary.replace(path)
            self.persisted = True
        except BaseException as error:
            self.record_error('export', error)
        return self.summary()
