"""Closed native-scoped clocks; never an admission decision or capacity gate."""
import os
import threading
import time
from types import FunctionType, MethodType, ModuleType
from uuid import UUID
from django.db import connections
from django.db.models import Model
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.postgresql.base import DatabaseWrapper
from .models import OutboxEvent

ACTIVE = None
MAX_ATTEMPTS = 4096
MAX_RECORDS = 131072
STAGES = frozenset(('admission_call', 'claim_native_admission', 'claim_composite',
    'atomic_claim_execute', 'claim_object_construct', 'claim_physical_commit',
    'envelope', 'lease_check', 'lease_check_execute', 'send', 'delivery_ack',
    'publication_writeback', 'publication_writeback_execute', 'application_other_execute'))


class _MeasureScope:
    def __init__(self, observer, stage):
        self.observer, self.stage, self.row = observer, stage, None
    def __enter__(self):
        self.row = self.observer.start(self.stage)
        return self.row
    def __exit__(self, kind, error, traceback):
        self.observer.finish(self.row, error)
        return False


class _ExecuteScope:
    def __init__(self, observer, stage):
        self.observer, self.stage, self.token = observer, stage, None
    def __enter__(self):
        return self
    def install(self):
        self.observer.enter_execute(self.stage, self)
    def __exit__(self, kind, error, traceback):
        self.observer.leave_execute(self.token, error)
        return False


class _ClaimScope:
    def __init__(self, observer):
        self.observer, self.token = observer, None
    def __enter__(self):
        return self
    def install(self):
        self.observer.enter_claim(self)
    def __exit__(self, kind, error, traceback):
        self.observer.leave_claim(self.token, error)
        return False


class NativePublisherObservation:
    """One publisher owner and bounded native lists; no user observer dispatch."""
    def __init__(self):
        self.pid, self.owner = os.getpid(), threading.get_ident()
        self.policy = self.policy_function = self.policy_code = self.policy_defaults = None
        self.callback_code = self.callback_globals = None
        self.attempt = None
        self.attempts, self.records, self.errors, self.parents = [], [], [], []
        self.seen = self.overflow_attempts = self.overflow_records = 0
        self.clock_calls = self.clock_cpu_ns = self.clock_failures = 0
        self.hook_installs = self.hook_restores = self.hook_failures = 0
        self.ack_late = self.ack_nonowner = self.ack_unavailable = 0
        self.temporary_depth = 0
        self.closed = self.recording_failed = self.policy_bound = False

    def bind(self, policy, send):
        # Bootstrap supplies the existing command global, never a new policy.
        if self.policy_bound or type(send) is not FunctionType or type(policy) is not _Policy:
            raise RuntimeError('NativeObservationPolicyBindingRefused')
        if (any(type(name) is not str for name in _Policy.__dict__)
                or _Policy.__dict__.get('plain') is not _Plain
                or _Plain.__code__ is not _PlainCode or _Plain.__defaults__ is not _PlainDefaults):
            raise RuntimeError('NativeObservationPolicyFunctionRefused')
        self.policy, self.policy_bound = policy, True
        self.policy_function = _Plain
        self.policy_code, self.policy_defaults = self.policy_function.__code__, self.policy_function.__defaults__
        codes = [value for value in send.__code__.co_consts
            if type(value) is type(send.__code__) and value.co_name == '<lambda>']
        if len(codes) != 1:
            raise RuntimeError('NativeObservationCallbackSourceRefused')
        self.callback_code, self.callback_globals = codes[0], send.__globals__

    def error(self, stage, error, primary=None):
        mro = _TypeMROGet(type(error), type(type(error)))
        if (any(kind is _Deadline for kind in mro)
                or not any(kind is Exception for kind in mro)) and primary is None:
            raise error
        self.recording_failed = True
        try:
            name = _TypeNameGet(type(error), type(type(error)))
            self.errors.append({'stage': stage, 'error_type': name if
                type(name) is str and name.isidentifier() else 'UnknownError'})
        except BaseException as secondary:
            secondary_mro = _TypeMROGet(type(secondary), type(type(secondary)))
            if primary is None and (any(kind is _Deadline for kind in secondary_mro)
                    or not any(kind is Exception for kind in secondary_mro)):
                raise

    def clock(self, primary=None):
        self.clock_calls += 1
        try:
            began = time.thread_time_ns()
            value = (time.time_ns(), time.perf_counter_ns(), time.thread_time_ns())
            self.clock_cpu_ns += value[2] - began
            return value
        except BaseException as error:
            self.clock_failures += 1
            self.error('clock', error, primary)
            return None

    def begin_attempt(self):
        self.seen += 1
        if self.attempt is not None:
            self.error('nested_attempt', RuntimeError('NativeObservationNestedAttempt'))
            return None
        if len(self.attempts) >= MAX_ATTEMPTS:
            self.overflow_attempts += 1
            return None
        token = {'attempt_id': self.seen, 'event_id': None, 'row_start': len(self.records),
            'guard_results': [], 'native_claim_results': [], 'claim_path': 'unobserved',
            'publish_result': None, 'publish_result_observed': False, 'empty_claim': None,
            'outcome': 'incomplete', 'exception_type': None, 'temporary_hooks_restored': False,
            'hook_installs_before': self.hook_installs, 'hook_restores_before': self.hook_restores,
            'hook_failures_before': self.hook_failures}
        self.attempts.append(token)
        self.attempt = token
        return token

    def end_attempt(self, token, primary=None):
        if token is None:
            return
        if self.attempt is not token or self.temporary_depth or self.parents:
            self.error('attempt_restore', RuntimeError('NativeObservationScopeChanged'), primary)
        token['outcome'] = 'error' if primary is not None else 'returned'
        token['exception_type'] = _TypeNameGet(type(primary), type(type(primary))) if primary is not None else None
        token['temporary_hooks_restored'] = (self.temporary_depth == 0
            and self.hook_failures == token['hook_failures_before']
            and self.hook_installs - token['hook_installs_before']
                == self.hook_restores - token['hook_restores_before'])
        self.attempt = None

    def guard_result(self, policy, result):
        if self.attempt is not None:
            owned = (policy is self.policy and type(policy) is _Policy
                and all(type(name) is str for name in _Policy.__dict__)
                and _Policy.__dict__.get('plain') is self.policy_function
                and self.policy_function.__code__ is self.policy_code
                and self.policy_function.__defaults__ is self.policy_defaults)
            self.attempt['guard_results'].append(result if owned and type(result) is bool else None)

    def native_result(self, result):
        if self.attempt is not None:
            self.attempt['native_claim_results'].append(result if type(result) is bool else None)
            self.attempt['claim_path'] = 'native_postgresql' if result is True else 'orm_or_unknown'

    def bind_event(self, event):
        if self.attempt is None:
            return
        self.attempt['empty_claim'] = event is None
        if type(event) is OutboxEvent:
            value = object.__getattribute__(event, '__dict__').get('id')
            if type(value) is UUID:
                integer = _UUIDInt.__get__(value, UUID)
                if type(integer) is not int:
                    return
                hex_value = f'{integer:032x}'
                identifier = f'{hex_value[:8]}-{hex_value[8:12]}-{hex_value[12:16]}-{hex_value[16:20]}-{hex_value[20:]}'
                self.attempt['event_id'] = identifier
                for row in self.records[self.attempt['row_start']:]:
                    row['event_id'] = identifier

    def publish_result(self, result):
        if self.attempt is not None:
            self.attempt['publish_result_observed'] = type(result) is bool
            self.attempt['publish_result'] = result if type(result) is bool else None

    def eligible(self):
        return (not self.closed and self.attempt is not None
            and self.owner == threading.get_ident() and self.pid == os.getpid()
            and len(self.attempt['guard_results']) == 1 and self.attempt['guard_results'][0] is True
            and len(self.attempt['native_claim_results']) == 1 and self.attempt['native_claim_results'][0] is True)

    def measure(self, stage):
        return _MeasureScope(self, stage)
    def execute_scope(self, stage):
        return _ExecuteScope(self, stage)
    def claim_scope(self):
        return _ClaimScope(self)

    def start(self, stage):
        if type(stage) is not str or stage not in STAGES:
            raise RuntimeError('UnknownNativeObservationStage')
        try:
            if self.attempt is None or self.owner != threading.get_ident():
                return None
            if len(self.records) >= MAX_RECORDS:
                self.overflow_records += 1
                return None
            stamp = self.clock()
            row = {'id': len(self.records) + 1, 'parent_id': self.parents[-1] if self.parents else None,
                'attempt_id': self.attempt['attempt_id'], 'event_id': self.attempt['event_id'], 'stage': stage,
                'start_epoch_ns': stamp[0] if stamp else None, 'start_perf_ns': stamp[1] if stamp else None,
                'start_thread_cpu_ns': stamp[2] if stamp else None,
                'end_epoch_ns': None, 'wall_ns': None, 'thread_cpu_ns': None,
                'outcome': 'incomplete', 'exception_type': None, 'complete': False}
            self.records.append(row)
            self.parents.append(row['id'])
            return row
        except BaseException as error:
            self.error('measure_start', error)
            return None

    def finish(self, row, primary=None):
        if row is None:
            return
        try:
            stamp = self.clock(primary)
            row['outcome'] = 'error' if primary is not None else 'returned'
            row['exception_type'] = _TypeNameGet(type(primary), type(type(primary))) if primary is not None else None
            if stamp is not None and row['start_perf_ns'] is not None:
                row.update(end_epoch_ns=stamp[0], wall_ns=stamp[1] - row['start_perf_ns'],
                    thread_cpu_ns=stamp[2] - row['start_thread_cpu_ns'], complete=True)
        except BaseException as error:
            self.error('measure_end', error, primary)
        finally:
            if not self.parents or self.parents[-1] != row['id']:
                self.error('parent_restore', RuntimeError('NativeObservationParentChanged'), primary)
            else:
                self.parents.pop()

    def enter_execute(self, stage, target=None):
        if type(stage) is not str or stage not in STAGES:
            self.error('execute_stage', RuntimeError('UnknownNativeObservationStage'))
            return None
        if not self.eligible():
            return None
        database = connections['default']
        if (type(database) is not DatabaseWrapper or type(database.execute_wrappers) is not list
                or database.execute_wrappers):
            self.error('execute_admission', RuntimeError('NativeObservationDatabaseUnsupported'))
            return None
        def execute(original, sql, params, many, context):
            if self.owner != threading.get_ident():
                return original(sql, params, many, context)
            category = stage
            if stage == 'atomic_claim_execute' and not (type(sql) is str
                    and sql.lstrip().startswith('WITH claimed AS (')):
                category = 'application_other_execute'
            with self.measure(category):
                return original(sql, params, many, context)
        token = (database.execute_wrappers, execute)
        if type(target) is _ExecuteScope:
            target.token = token
        elif type(target) is list:
            target[3] = token
        elif target is not None:
            self.error('execute_target', RuntimeError('NativeObservationScopeRefused'))
            return None
        # Native list operations reproduce Django execute_wrapper's lifecycle.
        list.append(token[0], execute)
        self.temporary_depth += 1
        self.hook_installs += 1
        return token

    def leave_execute(self, token, primary=None):
        if token is None:
            return
        try:
            wrappers, execute = token
            if len(wrappers) == 1 and wrappers[0] is execute:
                list.pop(wrappers)
                self.hook_restores += 1
            else:
                self.hook_failures += 1
                self.error('execute_restore', RuntimeError('NativeObservationExecuteChanged'), primary)
        except BaseException as error:
            self.error('execute_restore', error, primary)
        finally:
            self.temporary_depth -= 1

    def enter_claim(self, target=None):
        if not self.eligible():
            return None
        database = connections['default']
        descriptor = Model.__dict__['from_db']
        original_commit = database.commit if type(database) is DatabaseWrapper else None
        data = object.__getattribute__(database, '__dict__') if type(database) is DatabaseWrapper else None
        if (type(descriptor) is not classmethod or descriptor.__func__ is not _FromDB
                or _FromDB.__code__ is not _FromDBCode or _FromDB.__defaults__ is not _FromDBDefaults
                or 'from_db' in OutboxEvent.__dict__ or type(original_commit) is not MethodType
                or original_commit.__func__ is not _Commit
                or _Commit.__code__ is not _CommitCode or _Commit.__defaults__ is not _CommitDefaults
                or type(data) is not dict or 'commit' in data or database.execute_wrappers):
            self.error('claim_hook_admission', RuntimeError('NativeObservationClaimUnsupported'))
            return None
        def from_db(*args, **kwargs):
            if self.owner != threading.get_ident():
                return _FromDB(*args, **kwargs)
            with self.measure('claim_object_construct'):
                return _FromDB(*args, **kwargs)
        replacement = classmethod(from_db)
        def commit(*args, **kwargs):
            if self.owner != threading.get_ident():
                return original_commit(*args, **kwargs)
            with self.measure('claim_physical_commit'):
                return original_commit(*args, **kwargs)
        token = [data, commit, replacement, None, False, False, self.temporary_depth]
        if type(target) is _ClaimScope:
            target.token = token
        elif target is not None:
            self.error('claim_target', RuntimeError('NativeObservationScopeRefused'))
            return None
        self.temporary_depth += 1
        try:
            token[4] = True
            type.__setattr__(OutboxEvent, 'from_db', replacement)
            self.hook_installs += 1
            token[5] = True
            dict.__setitem__(data, 'commit', commit)
            self.hook_installs += 1
            self.enter_execute('atomic_claim_execute', token)
            return token
        except BaseException as error:
            self.leave_claim(token, error)
            self.error('claim_install', error)
            return None

    def leave_claim(self, token, primary=None):
        if token is None:
            return
        data, commit, replacement, execute, installed_model, installed_commit, depth = token
        errors = []
        # Each authored removal is attempted independently. A first control
        # cannot strand the other installed capabilities or replace a primary.
        try:
            self.leave_execute(execute, primary)
        except BaseException as error:
            errors.append(error)
        if installed_commit:
            try:
                if dict.get(data, 'commit') is commit:
                    dict.__delitem__(data, 'commit')
                    self.hook_restores += 1
                elif 'commit' in data:
                    self.hook_failures += 1
                    errors.append(RuntimeError('NativeObservationCommitChanged'))
            except BaseException as error:
                errors.append(error)
        if installed_model:
            try:
                if OutboxEvent.__dict__.get('from_db') is replacement:
                    type.__delattr__(OutboxEvent, 'from_db')
                    self.hook_restores += 1
                elif 'from_db' in OutboxEvent.__dict__:
                    self.hook_failures += 1
                    errors.append(RuntimeError('NativeObservationModelChanged'))
            except BaseException as error:
                errors.append(error)
        # A control may have landed before a native deletion above. A single
        # bounded final removal of still-owned bindings restores original
        # absence; a foreign replacement is never deleted or called.
        if execute is not None:
            try:
                wrappers, wrapper = execute
                if type(wrappers) is list and len(wrappers) == 1 and wrappers[0] is wrapper:
                    list.pop(wrappers)
                    self.hook_restores += 1
                if wrappers:
                    self.hook_failures += 1
            except BaseException as error:
                self.hook_failures += 1
                errors.append(error)
        if installed_commit:
            try:
                if dict.get(data, 'commit') is commit:
                    dict.__delitem__(data, 'commit')
                    self.hook_restores += 1
                if 'commit' in data:
                    self.hook_failures += 1
            except BaseException as error:
                self.hook_failures += 1
                errors.append(error)
        if installed_model:
            try:
                if OutboxEvent.__dict__.get('from_db') is replacement:
                    type.__delattr__(OutboxEvent, 'from_db')
                    self.hook_restores += 1
                if 'from_db' in OutboxEvent.__dict__:
                    self.hook_failures += 1
            except BaseException as error:
                self.hook_failures += 1
                errors.append(error)
        self.temporary_depth = depth
        for error in errors:
            self.error('claim_restore', error, primary if primary is not None else error)
        if primary is None:
            for error in errors:
                mro = _TypeMROGet(type(error), type(type(error)))
                if any(kind is _Deadline for kind in mro) or not any(kind is Exception for kind in mro):
                    raise error

    def callback(self, original):
        if (not self.eligible() or type(original) is not FunctionType
                or original.__code__ is not self.callback_code or original.__globals__ is not self.callback_globals
                or original.__defaults__ is not None or original.__kwdefaults__ is not None
                or original.__dict__ or original.__code__.co_freevars != ('results',)
                or type(original.__closure__) is not tuple or len(original.__closure__) != 1
                or type(original.__closure__[0].cell_contents) is not list):
            self.ack_unavailable += 1
            return original
        attempt = self.attempt
        def delivered(*args, **kwargs):
            primary = None
            try:
                return original(*args, **kwargs)
            except BaseException as error:
                primary = error
                raise
            finally:
                try:
                    if self.owner != threading.get_ident():
                        self.ack_nonowner += 1
                    elif self.attempt is not attempt or attempt is None:
                        self.ack_late += 1
                    elif len(args) != 2 or kwargs:
                        self.ack_unavailable += 1
                    else:
                        with self.measure('delivery_ack') as row:
                            if row is not None:
                                row['delivery_success'] = args[0] is None
                                row['boundary'] = 'native_callback_after_original_callback'
                except BaseException as error:
                    self.error('ack', error, primary)
        return delivered

    def document(self):
        attempts, boundaries, grouped = [], {}, {}
        for row in self.records:
            boundaries[row['stage']] = boundaries.get(row['stage'], 0) + 1
            grouped.setdefault(row['attempt_id'], []).append(row)
        for token in self.attempts:
            rows = grouped.get(token['attempt_id'], [])
            required = {'admission_call', 'claim_native_admission', 'claim_composite',
                'atomic_claim_execute', 'claim_physical_commit'}
            if token['empty_claim'] is not True:
                required |= {'claim_object_construct', 'send', 'delivery_ack', 'publication_writeback_execute'}
            observed = {row['stage'] for row in rows if row['complete'] and row['outcome'] == 'returned'}
            missing = sorted(required - observed)
            acks = [row for row in rows if row['stage'] == 'delivery_ack']
            ack_valid = (not acks if token['empty_claim'] is True else
                len(acks) == 1 and acks[0].get('delivery_success') is True and acks[0]['complete'])
            complete = (token['outcome'] == 'returned' and len(token['guard_results']) == 1
                and token['guard_results'][0] is True and len(token['native_claim_results']) == 1
                and token['native_claim_results'][0] is True and not missing and ack_valid
                and token['temporary_hooks_restored'] and token['publish_result_observed']
                and (token['empty_claim'] is True and token['publish_result'] is False
                    or token['empty_claim'] is False and token['publish_result'] is True
                    and token['event_id'] is not None))
            attempts.append({**token, 'missing_boundaries': missing, 'complete': complete})
        complete = (self.closed and bool(attempts) and all(row['complete'] for row in attempts)
            and not self.errors and not self.recording_failed and not self.overflow_attempts
            and not self.overflow_records and not self.clock_failures and not self.hook_failures
            and self.hook_installs == self.hook_restores and not self.temporary_depth
            and not self.ack_late and not self.ack_nonowner and not self.ack_unavailable)
        return {'schema_version': 1, 'mode': 'native-scoped', 'complete': complete,
            'status': 'COMPLETE_DIAGNOSTIC' if complete else 'OBSERVED_PARTIAL',
            'qualification_admissible': False, 'function_profile_requested': False,
            'function_graph_status': 'NOT_REQUESTED', 'nested_times_additive': False,
            'attempts_seen': self.seen, 'sampled_attempts': len(self.attempts),
            'max_sampled_attempts': MAX_ATTEMPTS, 'max_records': MAX_RECORDS,
            'overflow_attempts': self.overflow_attempts, 'overflow_records': self.overflow_records,
            'clock_read_calls': self.clock_calls, 'clock_read_thread_cpu_ns': self.clock_cpu_ns,
            'clock_failures': self.clock_failures, 'recording_failed': self.recording_failed,
            'hook_installs': self.hook_installs, 'hook_restores': self.hook_restores,
            'hook_failures': self.hook_failures, 'ack_late': self.ack_late,
            'ack_nonowner': self.ack_nonowner, 'ack_unavailable': self.ack_unavailable,
            'errors': self.errors, 'boundaries': boundaries, 'attempts': attempts, 'records': self.records,
            'limitations': ['Native selection and lease update share one atomic execute boundary.',
                'Guard call thread CPU includes its callees; exclusive function-own CPU unavailable.',
                'Callback is sampled after the original callback; pre-callback arrival unavailable.',
                'Claim execute excludes fetching; from_db excludes external converters.',
                'Writeback driver execute includes autocommit; separate final commit unavailable.',
                'Clock-read CPU is only a lower bound on observation overhead.',
                'Unknown, fallback, missing, late and overflow observations are incomplete.']}

    def close(self):
        # Final serialization never dispatches a foreign nested row/container.
        for rows in (self.records, self.errors, self.attempts):
            if type(rows) is not list:
                return {'mode': 'native-scoped', 'complete': False, 'status': 'OBSERVED_PARTIAL',
                    'qualification_admissible': False, 'reason': 'malformed_observation_rows'}
            for row in rows:
                if type(row) is not dict or any(type(name) is not str for name in row):
                    return {'mode': 'native-scoped', 'complete': False, 'status': 'OBSERVED_PARTIAL',
                        'qualification_admissible': False, 'reason': 'malformed_observation_rows'}
                for name, value in row.items():
                    if name in ('guard_results', 'native_claim_results'):
                        valid = type(value) is list and all(item is True or item is False or item is None for item in value)
                    else:
                        valid = value is None or type(value) is int or type(value) is str or type(value) is bool
                    if not valid:
                        return {'mode': 'native-scoped', 'complete': False, 'status': 'OBSERVED_PARTIAL',
                            'qualification_admissible': False, 'reason': 'malformed_observation_rows'}
        self.closed = True
        try:
            return self.document()
        except _Deadline:
            raise
        except Exception as error:
            self.error('close', error, error)
            return {'mode': 'native-scoped', 'complete': False, 'status': 'OBSERVED_PARTIAL',
                'qualification_admissible': False, 'reason': 'incomplete_observation_row_shape'}


# Trusted definition-time strong references to fixed observation dependencies.
from .worker_metrics import PublisherBudgetAdmission as _Policy, OperationDeadlineExceeded as _Deadline
_Plain = _Policy.__dict__['plain']
_PlainCode, _PlainDefaults = _Plain.__code__, _Plain.__defaults__
_FromDB = Model.__dict__['from_db'].__func__
_FromDBCode, _FromDBDefaults = _FromDB.__code__, _FromDB.__defaults__
_Commit = BaseDatabaseWrapper.__dict__['commit']
_CommitCode, _CommitDefaults = _Commit.__code__, _Commit.__defaults__
_UUIDInt = UUID.__dict__['int']
_TypeNameGet = type.__dict__['__name__'].__get__
_TypeMROGet = type.__dict__['__mro__'].__get__
_CLASSES = (NativePublisherObservation, _MeasureScope, _ExecuteScope, _ClaimScope)
_CLASS_SNAPSHOTS = tuple((kind, tuple(kind.__dict__.items())) for kind in _CLASSES)
_METHODS = tuple((name, function, function.__code__, function.__defaults__, function.__closure__,
    tuple(cell.cell_contents for cell in function.__closure__ or ()))
    for kind in _CLASSES for name, function in kind.__dict__.items() if type(function) is FunctionType)
_BUILTINS = tuple((name, name in globals(), globals().get(name),
    __builtins__[name] if type(__builtins__) is dict else vars(__builtins__)[name])
    for name in ('type', 'len', 'vars', 'isinstance', 'str', 'dict', 'list', 'tuple', 'zip', 'any',
        'all', 'sorted', 'object', 'int', 'bool', 'Exception', 'BaseException', 'RuntimeError', 'classmethod'))
_MODULE_REFS = tuple((name, globals()[name]) for name in ('MAX_ATTEMPTS', 'MAX_RECORDS', 'STAGES',
    'NativePublisherObservation', '_MeasureScope', '_ExecuteScope', '_ClaimScope', 'os', 'threading', 'time',
    'FunctionType', 'MethodType', 'UUID', '_UUIDInt', '_TypeNameGet', '_TypeMROGet', 'connections', 'Model', 'BaseDatabaseWrapper', 'DatabaseWrapper',
    'OutboxEvent', '_Policy', '_Deadline', '_Plain', '_PlainCode', '_PlainDefaults', '_FromDB', '_FromDBCode', '_FromDBDefaults', '_Commit', '_CommitCode', '_CommitDefaults'))


def current(caller=None, bootstrap=False, _state=globals(), _type=type, _dict=dict, _str=str, _list=list,
        _tuple=tuple, _len=len, _get=dict.get, _function=FunctionType, _object=object,
        _kind=NativePublisherObservation, _classes=_CLASS_SNAPSHOTS, _methods=_METHODS,
        _builtins=_BUILTINS, _refs=_MODULE_REFS, _int=int, _bool=bool, _module=ModuleType,
        _thread_module=threading, _os_module=os, _thread=threading.get_ident, _pid=os.getpid,
        _time=time, _clocks=(time.time_ns, time.perf_counter_ns, time.thread_time_ns)):
    """Fixed native bootstrap refuses shadows before any observation dispatch."""
    if _type(_state) is not _dict or _get is not _dict.get:
        return None
    for name in _state:
        if _type(name) is not _str:
            return None
    if bootstrap is not True and _get(_state, 'ACTIVE') is None:
        return None
    if caller is None or _get(_state, 'current') is not caller:
        return None
    if _type(caller) is not _function or _type(caller.__builtins__) is not _dict:
        return None
    for name in caller.__builtins__:
        if _type(name) is not _str:
            return None
    for name, present, value, native in _builtins:
        if ((name in _state) != present or _get(_state, name) is not value
                or _get(caller.__builtins__, name) is not native):
            return None
    for name, value in _refs:
        if _get(_state, name) is not value:
            return None
    for module in (_time, _thread_module, _os_module):
        if _type(module) is not _module:
            return None
        for name in module.__dict__:
            if _type(name) is not _str:
                return None
    if (_get(_thread_module.__dict__, 'get_ident') is not _thread
            or _get(_os_module.__dict__, 'getpid') is not _pid
            or _get(_time.__dict__, 'time_ns') is not _clocks[0]
            or _get(_time.__dict__, 'perf_counter_ns') is not _clocks[1]
            or _get(_time.__dict__, 'thread_time_ns') is not _clocks[2]):
        return None
    for kind, expected in _classes:
        if _type(kind) is not _type:
            return None
        mro = kind.__mro__
        if _len(mro) != 2 or mro[0] is not kind or mro[1] is not _object:
            return None
        actual = _tuple(kind.__dict__.items())
        if _len(actual) != _len(expected):
            return None
        for pair, old in zip(actual, expected):
            if _type(pair[0]) is not _str or pair[0] != old[0] or pair[1] is not old[1]:
                return None
    for name, function, code, defaults, closure, values in _methods:
        if (function.__code__ is not code or function.__defaults__ is not defaults
                or function.__kwdefaults__ is not None or function.__dict__
                or function.__closure__ is not closure):
            return None
        if closure is not None:
            for cell, value in zip(closure, values):
                if cell.cell_contents is not value:
                    return None
    observer = _get(_state, 'ACTIVE')
    if bootstrap is True and observer is None:
        return _kind
    if _type(observer) is not _kind:
        return None
    data = _object.__getattribute__(observer, '__dict__')
    if _type(data) is not _dict:
        return None
    for name in data:
        if _type(name) is not _str:
            return None
    for name in ('pid', 'owner', 'seen', 'overflow_attempts', 'overflow_records', 'clock_calls',
            'clock_cpu_ns', 'clock_failures', 'hook_installs', 'hook_restores', 'hook_failures',
            'ack_late', 'ack_nonowner', 'ack_unavailable', 'temporary_depth'):
        if _type(_get(data, name)) is not _int:
            return None
    for name in ('closed', 'recording_failed', 'policy_bound'):
        if _type(_get(data, name)) is not _bool:
            return None
    for name in ('attempts', 'records', 'errors', 'parents'):
        if _type(_get(data, name)) is not _list:
            return None
    if any(_type(value) is not _int for value in data['parents']):
        return None
    # No unknown instance callback can shadow an admitted fixed method.
    for name, _, _, _, _, _ in _methods:
        if name in data:
            return None
    attempt = _get(data, 'attempt')
    if attempt is not None:
        if _type(attempt) is not _dict:
            return None
        for name in attempt:
            if _type(name) is not _str:
                return None
        for name in ('attempt_id', 'row_start', 'hook_installs_before', 'hook_restores_before', 'hook_failures_before'):
            if _type(_get(attempt, name)) is not _int:
                return None
        for name in ('event_id', 'claim_path', 'outcome', 'exception_type'):
            value = _get(attempt, name)
            if value is not None and _type(value) is not _str:
                return None
        for name in ('publish_result', 'empty_claim'):
            value = _get(attempt, name)
            if value is not None and _type(value) is not _bool:
                return None
        for name in ('publish_result_observed', 'temporary_hooks_restored'):
            if _type(_get(attempt, name)) is not _bool:
                return None
        for name in ('guard_results', 'native_claim_results'):
            values = _get(attempt, name)
            if _type(values) is not _list:
                return None
            for value in values:
                if value is not True and value is not False and value is not None:
                    return None
    if (observer.owner != _thread() or observer.pid != _pid()
            or not observer.policy_bound or observer.closed):
        return None
    return observer
