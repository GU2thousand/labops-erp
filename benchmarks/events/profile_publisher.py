"""Owned validation bootstrap; the real production management command is intact."""
from contextlib import ExitStack, contextmanager
from functools import wraps
import ast
import inspect
import json
import os
from pathlib import Path
import re
import sys
import time
import textwrap
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


class PublisherObservation:
    """Bounded clocks around existing operations; never execute extra work.

    SQL execute excludes fetch/materialization. The final autocommit statement
    includes its driver's commit; it is not a separately measured commit.
    """
    MAX_ATTEMPTS = 4096
    MAX_RECORDS = 65536

    def __init__(self, profile):
        self.profile = profile
        self.records, self.parents, self.scopes = [], [], []
        self.attempt = None
        self.samples = []
        self.attempts_seen = self.sampled_attempts = 0
        self.overflow_attempts = self.overflow_records = self.missing_event_ids = 0
        self.clock_read_calls = self.clock_cpu_ns = self.clock_failures = 0
        self.ack_unavailable = self.ack_nonowner = self.ack_late = 0
        self.commit_unavailable = 0
        self.database = None
        from labops import events
        self.publication_code = events.publish_one.__code__
        self.publication_lines = set()
        if self.publication_code.co_name == 'publish_one':
            source, first_line = inspect.getsourcelines(events.publish_one)
            tree = ast.parse(textwrap.dedent(''.join(source)))
            for node in ast.walk(tree):
                if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                        and any(keyword.arg == 'status' and isinstance(keyword.value, ast.Constant)
                            and keyword.value.value == 'PUBLISHED' for keyword in node.value.keywords)):
                    self.publication_lines.update(range(first_line + node.lineno - 1, first_line + node.end_lineno))

    def _error(self, stage, error):
        # Diagnostic errors must not alter the original call's result/error.
        self.profile.recording_failed = True
        try:
            self.profile.record_error(stage, error)
        except BaseException:
            self.profile.recording_failed = True

    def _clock(self):
        self.clock_read_calls += 1
        try:
            before = time.thread_time_ns()
            clocks = (time.time_ns(), time.perf_counter_ns(), time.thread_time_ns())
            self.clock_cpu_ns += clocks[2] - before
            return clocks
        except BaseException as error:
            self.clock_failures += 1
            self._error('publisher_observation_clock', error)
            return None

    def _new_record(self, stage, *, outcome=None):
        if len(self.records) >= self.MAX_RECORDS:
            self.overflow_records += 1
            return None
        clock = self._clock()
        row = {'id': len(self.records) + 1, 'parent_id': self.parents[-1] if self.parents else None,
            'stage': stage, 'ordinal': self.attempt['ordinal'], 'event_id': self.attempt['event_id'],
            'start_epoch_ns': clock[0] if clock else None,
            'start_perf_ns': clock[1] if clock else None,
            'start_thread_cpu_ns': clock[2] if clock else None,
            'end_epoch_ns': None, 'end_perf_ns': None, 'end_thread_cpu_ns': None,
            'wall_ns': None, 'thread_cpu_ns': None, 'outcome': outcome, 'exception_type': None,
            'complete': False}
        self.records.append(row)
        self.attempt['rows'].append(row)
        return row

    @contextmanager
    def _measure(self, stage):
        row, original, parent_installed = None, None, False
        try:
            row = self._new_record(stage)
            if row is not None:
                self.parents.append(row['id'])
                parent_installed = True
        except BaseException as error:
            self._error('publisher_observation_start', error)
        try:
            yield row
        except BaseException as error:
            original = error
            raise
        finally:
            try:
                if row is not None:
                    clock = self._clock()
                    row.update(end_epoch_ns=clock[0] if clock else None,
                        end_perf_ns=clock[1] if clock else None,
                        end_thread_cpu_ns=clock[2] if clock else None,
                        outcome='error' if original is not None else 'returned',
                        exception_type=self._exception_type(original))
                    if clock and row['start_perf_ns'] is not None:
                        row.update(wall_ns=clock[1] - row['start_perf_ns'],
                            thread_cpu_ns=clock[2] - row['start_thread_cpu_ns'], complete=True)
            except BaseException as error:
                self._error('publisher_observation_end', error)
            finally:
                if parent_installed:
                    try:
                        if not self.parents or self.parents[-1] != row['id']:
                            raise RuntimeError('PublisherObservationParentChanged')
                        self.parents.pop()
                    except BaseException as error:
                        self._error('publisher_observation_parent_restore', error)

    @staticmethod
    def _exception_type(error):
        from benchmarks.events.diagnostic_profile import _safe_name
        return _safe_name(type(error).__name__) if error is not None else None

    def _publication_callsite(self):
        frame = sys._getframe(1)
        try:
            while frame is not None:
                if frame.f_code is self.publication_code:
                    return frame.f_lineno in self.publication_lines
                frame = frame.f_back
            return False
        finally:
            del frame

    @contextmanager
    def _scope(self, phase):
        installed = False
        try:
            self.scopes.append(phase)
            installed = True
        except BaseException as error:
            self._error('publisher_observation_scope_start', error)
        try:
            yield
        finally:
            if installed:
                try:
                    self.scopes.pop()
                except BaseException as error:
                    self._error('publisher_observation_scope_restore', error)

    @contextmanager
    def _instance_hook(self, target, name, wrapper):
        previous, installed, local = None, False, False
        try:
            local = name in vars(target)
            previous = vars(target).get(name)
            setattr(target, name, wrapper)
            installed = True
        except BaseException as error:
            self._error('publisher_observation_hook', error)
        try:
            yield installed
        finally:
            if installed:
                try:
                    if vars(target).get(name) is not wrapper:
                        raise RuntimeError('PublisherObservationHookChanged')
                    if local:
                        setattr(target, name, previous)
                    else:
                        delattr(target, name)
                except BaseException as error:
                    self._error('publisher_observation_restore', error)

    @contextmanager
    def _database_scope(self):
        stack, previous = ExitStack(), self.database
        try:
            from django.db import connections, DEFAULT_DB_ALIAS
            self.database = connections[DEFAULT_DB_ALIAS]
            stack.enter_context(self.database.execute_wrapper(self.execute))
        except BaseException as error:
            self._error('publisher_observation_database', error)
        try:
            yield
        finally:
            try:
                stack.close()
            except BaseException as error:
                self._error('publisher_observation_database_restore', error)
            self.database = previous

    @contextmanager
    def _commit_scope(self):
        from django.db.backends.base.base import BaseDatabaseWrapper
        database = self.database
        try:
            original = getattr(database, 'commit', None)
            admitted = (database is not None
                and getattr(original, '__func__', None) is BaseDatabaseWrapper.commit
                and type(database).__module__ in {'django.db.backends.postgresql.base', 'django.db.backends.sqlite3.base'})
        except BaseException as error:
            self.commit_unavailable += 1
            self._error('publisher_observation_commit_admission', error)
            yield
            return
        if not admitted:
            self.commit_unavailable += 1
            yield
            return
        @wraps(original)
        def commit(*args, **kwargs):
            if not self.profile.owning_thread() or self.attempt is not attempt:
                return original(*args, **kwargs)
            with self._measure('claim_physical_commit') as row:
                if row is not None:
                    try:
                        row['database_vendor'] = database.vendor
                        row['boundary'] = 'django_connection_commit_including_driver'
                    except BaseException as error:
                        self._error('publisher_observation_commit_state', error)
                return original(*args, **kwargs)
        attempt = self.attempt
        with self._instance_hook(database, 'commit', commit):
            yield

    def execute(self, execute, sql, params, many, context):
        """Django's wrapper delegates exactly the original statement once."""
        stage = None
        try:
            phase = self.scopes[-1] if self.scopes else None
            if isinstance(sql, str) and '"labops_outboxevent"' in sql:
                status = None
                if sql.lstrip().startswith('UPDATE') and isinstance(params, (tuple, list)):
                    # Model.save orders fields by declaration; lease_token
                    # precedes status. Find the status placeholder in SET only.
                    assignment = sql.partition(' SET ')[2].partition(' WHERE ')[0]
                    match = re.search(r'"status"\s*=\s*%s', assignment)
                    position = assignment[:match.start()].count('%s') if match else None
                    if position is not None and position < len(params):
                        status = params[position]
                if phase == 'claim_commit_composite' and sql.lstrip().startswith('SELECT'):
                    stage = 'claim_query_execute'
                elif (phase == 'claim_commit_composite' and sql.lstrip().startswith('UPDATE')
                        and status == 'PROCESSING'):
                    stage = 'claim_lease_write_execute'
                elif (phase == 'publish_one_composite' and sql.lstrip().startswith('UPDATE')
                        and status == 'PUBLISHED'
                        and self._publication_callsite()):
                    stage = 'publication_mark_execute'
        except BaseException as error:
            self._error('publisher_observation_sql_classification', error)
        if stage is None or not self.profile.owning_thread() or self.attempt is None:
            return execute(sql, params, many, context)
        with self._measure(stage) as row:
            if row is not None:
                try:
                    autocommit, in_atomic = self.database.autocommit, self.database.in_atomic_block
                    row.update(autocommit=autocommit,
                        in_atomic_block=in_atomic,
                        database_vendor=self.database.vendor,
                        boundary=('driver_execute_including_autocommit' if stage == 'publication_mark_execute'
                            and autocommit and not in_atomic else 'driver_execute_excluding_fetch'))
                except BaseException as error:
                    self._error('publisher_observation_database_state', error)
            return execute(sql, params, many, context)

    def _bind_event(self, event):
        try:
            self.attempt['empty_claim'] = event is None
            # Never access a descriptor/deferred field: that could issue SQL.
            from django.db.models import Model
            if (event is not None and inspect.getattr_static(type(event), '__dict__', None)
                    is not inspect.getattr_static(Model, '__dict__')):
                return
            state = object.__getattribute__(event, '__dict__') if event is not None else {}
            value = state.get('id') if type(state) is dict else None
            if type(value) is uuid.UUID:
                self.attempt['event_id'] = str(value)
                for row in self.attempt['rows']:
                    row['event_id'] = str(value)
        except (AttributeError, TypeError):
            # Unsupported return values are missing identity, not business work.
            pass
        except BaseException as error:
            self._error('publisher_observation_event_identity', error)

    def _ack(self, attempt, error):
        if not self.profile.owning_thread():
            self.ack_nonowner += 1
            return
        if self.attempt is not attempt:
            self.ack_late += 1
            return
        try:
            row = self._new_record('delivery_ack', outcome='success' if error is None else 'error')
            if row is not None:
                row.update(end_epoch_ns=row['start_epoch_ns'], end_perf_ns=row['start_perf_ns'],
                    end_thread_cpu_ns=row['start_thread_cpu_ns'], wall_ns=0, thread_cpu_ns=0,
                    complete=row['start_epoch_ns'] is not None,
                    boundary='native_delivery_callback_before_original_callback')
        except BaseException as failure:
            self._error('publisher_observation_ack', failure)

    @contextmanager
    def _delivery_scope(self, producer):
        try:
            kind = type(producer)
            admitted = (kind.__module__ == 'labops.events' and kind.__qualname__ == 'producer.<locals>.Client'
                and 'client' in vars(producer))
        except BaseException as error:
            self.ack_unavailable += 1
            self._error('publisher_observation_produce_admission', error)
            yield
            return
        if not admitted:
            self.ack_unavailable += 1
            yield
            return
        try:
            original, attempt = producer.produce, self.attempt
        except BaseException as error:
            self.ack_unavailable += 1
            self._error('publisher_observation_produce_admission', error)
            yield
            return
        @wraps(original)
        def produce(*args, **kwargs):
            if not self.profile.owning_thread() or self.attempt is not attempt:
                return original(*args, **kwargs)
            callback = kwargs.get('on_delivery')
            if not callable(callback):
                self.ack_unavailable += 1
                return original(*args, **kwargs)
            def delivery(*callback_args, **callback_kwargs):
                if len(callback_args) == 2 and not callback_kwargs:
                    self._ack(attempt, callback_args[0])
                else:
                    self.ack_unavailable += 1
                return callback(*callback_args, **callback_kwargs)
            return original(*args, **dict(kwargs, on_delivery=delivery))
        with self._instance_hook(producer, 'produce', produce) as installed:
            if not installed:
                self.ack_unavailable += 1
            yield

    def invoke(self, phase, original, args, kwargs):
        if not self.profile.owning_thread() or not self.profile.recording_active:
            return original(*args, **kwargs)
        if phase == 'publish_one_composite':
            self.attempts_seen += 1
            if self.attempts_seen > self.MAX_ATTEMPTS:
                self.overflow_attempts += 1
                return original(*args, **kwargs)
            self.sampled_attempts += 1
            prior = self.attempt
            self.attempt = {'ordinal': self.profile.ordinal, 'event_id': None, 'empty_claim': False, 'rows': []}
            outcome = 'error'
            try:
                with self._scope(phase), self._measure(phase), self._database_scope():
                    result = original(*args, **kwargs)
                    outcome = 'returned'
                    return result
            finally:
                try:
                    if self.attempt['event_id'] is None and not self.attempt['empty_claim']:
                        self.missing_event_ids += 1
                    self.attempt['outcome'] = outcome
                    self.samples.append(self.attempt)
                except BaseException as error:
                    self._error('publisher_observation_sample_finalize', error)
                finally:
                    self.attempt = prior
        if self.attempt is None:
            return original(*args, **kwargs)
        with self._scope(phase), self._measure(phase):
            if phase == 'claim_commit_composite':
                with self._commit_scope():
                    result = original(*args, **kwargs)
                self._bind_event(result)
                return result
            if phase == 'send_composite':
                with self._delivery_scope(args[0] if args else kwargs.get('producer')):
                    return original(*args, **kwargs)
            return original(*args, **kwargs)

    def install_model_hooks(self):
        from django.db.models import Model
        from labops.models import OutboxEvent
        if (inspect.getattr_static(OutboxEvent, 'from_db') is not inspect.getattr_static(Model, 'from_db')
                or inspect.getattr_static(OutboxEvent, 'save') is not inspect.getattr_static(Model, 'save')):
            raise RuntimeError('UnexpectedPublisherModelLifecycle')
        for name, stage in [('from_db', 'claim_object_construct'), ('save', 'claim_lease_write_composite')]:
            descriptor = inspect.getattr_static(OutboxEvent, name)
            original = descriptor.__func__ if isinstance(descriptor, classmethod) else descriptor
            def make_wrapper(original, stage):
                @wraps(original)
                def wrapper(*args, **kwargs):
                    if (self.attempt is not None and self.profile.owning_thread()
                            and self.scopes and self.scopes[-1] == 'claim_commit_composite'):
                        with self._measure(stage):
                            return original(*args, **kwargs)
                    return original(*args, **kwargs)
                return wrapper
            wrapper = make_wrapper(original, stage)
            if isinstance(descriptor, classmethod):
                wrapper = classmethod(wrapper)
            previous, local = vars(OutboxEvent).get(name), name in vars(OutboxEvent)
            # Register restoration before mutation: a failed diagnostic sink
            # must never leave an unregistered model wrapper behind.
            self.profile.hooks.append((OutboxEvent, name, previous, wrapper, local))
            setattr(OutboxEvent, name, wrapper)

    def document(self):
        from collections import Counter
        required = {'claim_query_execute', 'claim_object_construct', 'claim_lease_write_execute',
            'claim_physical_commit', 'send_composite', 'delivery_ack', 'publication_mark_execute'}
        attempts = []
        for sample in self.samples:
            rows = sample['rows']
            observed = {row['stage'] for row in rows if row['complete'] and row['outcome'] in {'returned', 'success'}}
            missing = sorted(required - observed) if not sample['empty_claim'] else []
            publication = [row for row in rows if row['stage'] == 'publication_mark_execute']
            autocommit = bool(publication) and all(row.get('autocommit') is True
                and row.get('in_atomic_block') is False for row in publication)
            attempts.append({'ordinal': sample['ordinal'], 'event_id': sample['event_id'],
                'empty_claim': sample['empty_claim'], 'outcome': sample['outcome'],
                'missing_boundaries': missing,
                'publication_autocommit_observed': autocommit,
                'complete': sample['outcome'] == 'returned' and (sample['empty_claim'] or
                    (sample['event_id'] is not None and not missing and autocommit))})
        complete = (bool(attempts) and any(row['event_id'] is not None for row in attempts)
            and all(row['complete'] for row in attempts) and not self.overflow_attempts
            and not self.overflow_records and not self.missing_event_ids and not self.clock_failures
            and not self.ack_unavailable and not self.ack_nonowner and not self.ack_late
            and not self.commit_unavailable and not self.profile.recording_failed
            and self.profile.hooks_restored
            and not any(row['stage'].startswith('publisher_observation') for row in self.profile.errors))
        return {'schema_version': 1, 'max_sampled_attempts': self.MAX_ATTEMPTS,
            'max_records': self.MAX_RECORDS, 'attempts_seen': self.attempts_seen,
            'sampled_attempts': self.sampled_attempts, 'overflow_attempts': self.overflow_attempts,
            'overflow_records': self.overflow_records, 'missing_event_ids': self.missing_event_ids,
            'clock_read_calls': self.clock_read_calls, 'clock_read_thread_cpu_ns': self.clock_cpu_ns,
            'clock_failures': self.clock_failures, 'ack_unavailable': self.ack_unavailable,
            'ack_nonowner': self.ack_nonowner, 'ack_late': self.ack_late,
            'commit_unavailable': self.commit_unavailable,
            'complete': complete, 'status': 'COMPLETE' if complete else 'INCOMPLETE',
            'attempts': attempts,
            'boundaries': dict(Counter(row['stage'] for row in self.records)),
            'event_ids_observed': len({row['event_id'] for row in self.records if row['event_id'] is not None}),
            'epoch_clock': 'time_ns; wall-clock adjustments possible',
            'duration_clock': 'perf_counter_ns', 'cpu_clock': 'thread_time_ns',
            'nested_times_additive': False, 'qualification_admissible': False,
            'clock_overhead_scope': 'clock reads only; total instrumentation overhead unmeasured',
            'limitations': ['claim query execute excludes fetching and ORM conversion outside from_db',
                'claim physical commit includes Django bookkeeping and driver commit',
                'publication mark includes driver autocommit; separate final physical commit unavailable',
                'delivery callback arrival precedes original callback; send return follows ACK validation',
                'first bounded attempts only; overflow and unavailable boundaries are not inferred'],
            'records': self.records}


def install_publisher_hooks(profile):
    from labops import events, worker_metrics
    from labops.management.commands import publish_events as command
    from labops.publisher_shards import PublisherShardOwner
    try:
        if (command.publish_one is not events.publish_one
                or command.database_statement_budget is not worker_metrics.database_statement_budget):
            raise RuntimeError('UnexpectedPublisherAlias')
        expected = [(command, 'publish_one', 'publish_one_composite', events.publish_one),
            (events, 'claim_event', 'claim_commit_composite', events.claim_event),
            (events, 'envelope', 'envelope_composite', events.envelope),
            (events, 'owned_event', 'lease_check', events.owned_event),
            (events, 'send', 'send_composite', events.send),
            (PublisherShardOwner, 'assert_owned', 'shard_ownership', PublisherShardOwner.assert_owned),
            (worker_metrics.StopController, 'wait', 'idle_wait', worker_metrics.StopController.wait)]
        for target, name, phase, original in expected:
            # Only these pinned repository functions may be intercepted. The
            # source hashes are exported by the sanitized function graph.
            filename = Path(original.__code__.co_filename).resolve()
            if not filename.is_relative_to(ROOT / 'labops'):
                raise RuntimeError('UnexpectedPublisherSource')
        observer = PublisherObservation(profile)
        profile.publisher_observation = observer
        observer.install_model_hooks()
        for target, name, phase, original in expected:
            profile.hook(target, name, phase, expected=original, ordinal=name == 'publish_one', observer=observer)
        original = command.database_statement_budget

        @wraps(original)
        @contextmanager
        def budget(*args, **kwargs):
            manager = original(*args, **kwargs)
            with profile.phase('budget_setup_composite'):
                value = manager.__enter__()
            try:
                yield value
            except BaseException:
                info = sys.exc_info()
                with profile.phase('budget_restore_composite'):
                    suppressed = manager.__exit__(*info)
                if not suppressed:
                    raise
            else:
                with profile.phase('budget_restore_composite'):
                    manager.__exit__(None, None, None)

        profile.hooks.append((command, 'database_statement_budget', original, budget, True))
        command.database_statement_budget = budget
    except BaseException as error:
        profile.record_error('publisher_hook_admission', error)
        profile.restore()


def run_publisher(output, options, *, call_command=None, profile_factory=None, engine='cprofile', observation_only=False):
    from benchmarks.events.diagnostic_profile import CPUProfile, _safe_name
    profile = (profile_factory('publisher') if profile_factory else CPUProfile('publisher', engine=engine,
        observation_only=observation_only))
    original = None
    try:
        install_publisher_hooks(profile)
        if call_command is None:
            from django.core.management import call_command
        if profile.engine == 'python-profile-owned':
            return profile.run('publisher_lifecycle', None, call_command, 'publish_events', **options)
        with profile.call('publisher_lifecycle'):
            return call_command('publish_events', **options)
    except BaseException as error:
        original = error
        raise
    finally:
        # close contains diagnostic failures; it cannot replace the real
        # management command's first error or stop its own normal shutdown.
        coverage, close_error = None, None
        previous_primary = profile.primary_error
        if profile.engine == 'python-profile-owned':
            profile.primary_error = original
        try:
            coverage = profile.close(output)
        except BaseException as error:
            close_error = error
            try:
                profile.record_error('publisher_close', error)
            except BaseException as recording_error:
                if (profile.engine == 'python-profile-owned' and original is None
                        and not isinstance(recording_error, Exception)):
                    raise
            if (profile.engine == 'python-profile-owned' and original is None
                    and not isinstance(error, Exception)):
                raise
        finally:
            profile.primary_error = previous_primary
        try:
            if close_error is not None or not isinstance(coverage, dict) or coverage.get('complete') is not True:
                metadata = {'kind': 'publisher_diagnostic_profile', 'status': 'INCOMPLETE'}
                if isinstance(coverage, dict):
                    metadata['coverage'] = coverage
                if close_error is not None:
                    metadata['error_type'] = _safe_name(type(close_error).__name__)
                # This bootstrap is used only for an enabled diagnostic run.
                # Emit no exception text, paths, business data or raw stats.
                print(json.dumps(metadata, sort_keys=True, allow_nan=False), file=sys.stderr, flush=True)
        except BaseException as error:
            if (profile.engine == 'python-profile-owned' and original is None
                    and not isinstance(error, Exception)):
                raise
            # Ordinary log errors and secondary controls keep the known body error.


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--metrics-port', type=int, default=None)
    from benchmarks.events.diagnostic_profile import PROFILE_ENGINES
    parser.add_argument('--profile-engine', choices=PROFILE_ENGINES, default='cprofile')
    parser.add_argument('--observation-only', action='store_true')
    args = parser.parse_args()
    if not args.output.name.startswith('publisher-profile-') or args.output.suffix != '.json':
        parser.error('Output must be an authored publisher-profile JSON filename')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    import django
    django.setup()
    run_publisher(args.output, {'loop': True, 'limit': 500, 'metrics_port': args.metrics_port},
                  engine=args.profile_engine, observation_only=args.observation_only)


if __name__ == '__main__':
    main()
