"""Deadline-context observation delegates original publisher work once."""
from collections import Counter
from contextlib import contextmanager
import json
from pathlib import Path
import signal
from tempfile import TemporaryDirectory
import threading
import time
from unittest import skipUnless
from unittest.mock import Mock, patch
from uuid import uuid4

from django.db import connection, OperationalError
from django.db.backends.postgresql.base import DatabaseWrapper
from django.test import SimpleTestCase, TransactionTestCase

from benchmarks.events.diagnostic_profile import CPUProfile
from benchmarks.events.profile_publisher import PublisherObservation, install_publisher_hooks, run_publisher
from labops.management.commands import publish_events as command
from labops.models import OutboxEvent
from labops.publisher_shards import PublisherShardOwner, ShardOwnershipLost, publisher_shard_owner
from labops.tests.test_diagnostic_profile import DeliveryClientFixture
from labops.tests.test_publisher_claim_reads import ClaimFixture
from labops.worker_metrics import OperationDeadlineExceeded, operation_deadline


class PrimaryFailure(BaseException):
    pass


@contextmanager
def quiet_context(*args, **kwargs):
    yield kwargs.get('token')


class PublisherDeadlineObservationControls(SimpleTestCase):
    def recorder(self):
        profile = CPUProfile('publisher', observation_only=True)
        observer = PublisherObservation(profile)
        profile.publisher_observation = observer
        self.addCleanup(profile.restore)
        return profile, observer

    def test_context_factory_entry_exit_args_and_body_exception_suppression_are_preserved(self):
        for suppress in (False, True):
            profile, observer = self.recorder()
            first, token, calls = PrimaryFailure('PRIVATE body'), object(), []
            @contextmanager
            def manager(*args, **kwargs):
                calls.append(('enter', args, kwargs))
                try:
                    yield token
                except PrimaryFailure as error:
                    calls.append(('error', error))
                    if not suppress:
                        raise
                finally:
                    calls.append(('exit',))
            def factory(*args, **kwargs):
                calls.append(('factory', args, kwargs))
                return manager(*args, **kwargs)
            with profile.call('publisher_lifecycle'):
                if suppress:
                    with observer.deadline_context(factory, (token,), {'fixture': token}) as value:
                        self.assertIs(value, token)
                        raise first
                else:
                    with self.assertRaises(PrimaryFailure) as raised:
                        with observer.deadline_context(factory, (token,), {'fixture': token}) as value:
                            self.assertIs(value, token)
                            raise first
                    self.assertIs(raised.exception, first)
            self.assertEqual([row[0] for row in calls], ['factory', 'enter', 'error', 'exit'])
            self.assertIs(calls[0][1][0], token)
            self.assertIs(calls[0][2]['fixture'], token)
            self.assertIs(calls[2][1], first)
            self.assertEqual(observer.attempts_seen, 1)
            self.assertEqual(observer.samples[0]['outcome'], 'returned' if suppress else 'error')
            self.assertFalse(observer.document()['complete'])
            self.assertNotIn('PRIVATE', json.dumps(observer.document()))
            self.assertIsNone(observer.attempt)
            self.assertEqual(observer.deadline_scope_depth, 0)

    def test_entry_failure_has_one_attempt_no_exit_or_publish_and_keeps_exception_identity(self):
        profile, observer = self.recorder()
        first, calls = PrimaryFailure('PRIVATE entry'), []
        @contextmanager
        def manager():
            calls.append('entry')
            raise first
            yield
        with profile.call('publisher_lifecycle'), self.assertRaises(PrimaryFailure) as raised:
            with observer.deadline_context(manager, (), {}):
                self.fail('Failed context entry must never reach the body')
        self.assertIs(raised.exception, first)
        self.assertEqual(calls, ['entry'])
        sample = observer.document()['attempts'][0]
        self.assertEqual(sample['classification'], 'deadline_setup_failed')
        self.assertEqual(sample['scope'], 'operation_deadline_context')
        self.assertNotIn('deadline_restore_composite', observer.document()['boundaries'])

    def test_unknown_context_uses_original_special_methods_and_suppression(self):
        profile, observer = self.recorder()
        calls, first = [], PrimaryFailure('PRIVATE context')
        class UnknownManager:
            def __enter__(self):
                calls.append('enter')
                return self
            def __exit__(self, *info):
                calls.append(info)
                return True
        manager = UnknownManager()
        manager.__enter__ = Mock(side_effect=AssertionError('Ignore instance enter override'))
        manager.__exit__ = Mock(side_effect=AssertionError('Ignore instance exit override'))
        with profile.call('publisher_lifecycle'):
            with observer.deadline_context(lambda: manager, (), {}) as value:
                self.assertIs(value, manager)
                raise first
        manager.__enter__.assert_not_called()
        manager.__exit__.assert_not_called()
        self.assertEqual(calls[0], 'enter')
        self.assertIs(calls[1][1], first)
        self.assertEqual(observer.context_unavailable, 1)
        self.assertEqual(observer.document()['attempts'][0]['classification'], 'unsupported_context')
        self.assertFalse(observer.document()['complete'])

    def test_bounded_deadlines_do_not_resample_publish_after_overflow_and_foreign_calls_delegate(self):
        profile, observer = self.recorder()
        observer.MAX_ATTEMPTS, observer.MAX_RECORDS = 2, 1
        original = Mock(return_value=17)
        with profile.call('publisher_lifecycle'):
            for _ in range(5):
                with observer.deadline_context(quiet_context, (), {}):
                    self.assertEqual(observer.invoke('publish_one_composite', original, (), {}), 17)
            failures = []
            def foreign():
                try:
                    with observer.deadline_context(quiet_context, (), {}):
                        observer.invoke('publish_one_composite', original, (), {})
                except BaseException as error:
                    failures.append(error)
            thread = threading.Thread(target=foreign)
            thread.start(); thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(original.call_count, 6)
        self.assertEqual(observer.attempts_seen, 5)
        self.assertEqual(observer.sampled_attempts, 2)
        self.assertEqual(observer.overflow_attempts, 3)
        self.assertEqual(len(observer.records), 1)
        self.assertGreater(observer.overflow_records, 0)
        self.assertFalse(observer.document()['complete'])

    def test_recording_failures_do_not_replace_first_body_error_or_skip_context_exit(self):
        profile, observer = self.recorder()
        first, exited = PrimaryFailure('PRIVATE first'), []
        @contextmanager
        def manager():
            try:
                yield
            finally:
                exited.append(True)
        with patch('benchmarks.events.profile_publisher.time.time_ns', side_effect=SystemExit('PRIVATE clock')), \
                patch.object(observer, '_record_error', side_effect=SystemExit('PRIVATE sink')):
            with profile.call('publisher_lifecycle'), self.assertRaises(PrimaryFailure) as raised:
                with observer.deadline_context(manager, (), {}):
                    raise first
        self.assertIs(raised.exception, first)
        self.assertEqual(exited, [True])
        self.assertTrue(profile.recording_failed)
        self.assertIsNone(observer.attempt)
        self.assertEqual(observer.deadline_scope_depth, 0)

    def test_deadline_control_after_entry_and_before_exit_reaches_original_exit_once(self):
        for failure_at in ('setup_end', 'restore_start'):
            profile, observer = self.recorder()
            control, calls = OperationDeadlineExceeded('PRIVATE deadline'), []
            original_clock = time.time_ns
            fired = []
            previous_handler = signal.getsignal(signal.SIGALRM)
            previous_timer = signal.getitimer(signal.ITIMER_REAL)
            @contextmanager
            def manager():
                with operation_deadline(30):
                    calls.append('enter')
                    try:
                        yield
                    except BaseException as error:
                        calls.append(error)
                        raise
                    finally:
                        calls.append('exit')
            def clock():
                phase = observer.scopes[-1] if observer.scopes else None
                target = ('deadline_setup_composite' if failure_at == 'setup_end'
                    else 'deadline_restore_composite')
                if phase == target and calls and not fired:
                    fired.append(True)
                    raise control
                return original_clock()
            with patch('benchmarks.events.profile_publisher.time.time_ns', side_effect=clock):
                with profile.call('publisher_lifecycle'), self.assertRaises(OperationDeadlineExceeded) as raised:
                    with observer.deadline_context(manager, (), {}):
                        calls.append('body')
                    calls.append('continued')
            self.assertIs(raised.exception, control)
            self.assertEqual(calls, ['enter', control, 'exit'] if failure_at == 'setup_end'
                else ['enter', 'body', control, 'exit'])
            self.assertIs(signal.getsignal(signal.SIGALRM), previous_handler)
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[1], previous_timer[1])
            if previous_timer[0] == 0:
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)
            self.assertEqual(observer.document()['attempts'][0]['classification'], 'deadline_exceeded')
            self.assertIsNone(observer.attempt)
            self.assertEqual(observer.parents, [])
            self.assertEqual(observer.scopes, [])

    def test_secondary_deadline_control_and_restore_eligibility_fault_keep_primary_body_error(self):
        for failure_at in ('clock', 'eligibility'):
            profile, observer = self.recorder()
            first, calls, fired = PrimaryFailure('PRIVATE first'), [], []
            control = OperationDeadlineExceeded('PRIVATE secondary')
            original_clock, original_owner = time.time_ns, profile.owning_thread
            @contextmanager
            def manager():
                try:
                    yield
                except BaseException as error:
                    calls.append(error)
                    raise
                finally:
                    calls.append('exit')
            def clock():
                if observer.scopes and observer.scopes[-1] == 'deadline_restore_composite' and not fired:
                    fired.append(True)
                    raise control
                return original_clock()
            def owner():
                if observer.cleanup_primary is first and not fired:
                    fired.append(True)
                    raise OSError('PRIVATE eligibility')
                return original_owner()
            change = (patch('benchmarks.events.profile_publisher.time.time_ns', side_effect=clock)
                if failure_at == 'clock' else patch.object(profile, 'owning_thread', side_effect=owner))
            with change, profile.call('publisher_lifecycle'), self.assertRaises(PrimaryFailure) as raised:
                with observer.deadline_context(manager, (), {}):
                    raise first
            self.assertIs(raised.exception, first)
            self.assertEqual(calls, [first, 'exit'])
            self.assertTrue(fired)
            self.assertFalse(observer.document()['complete'])

    def test_deadline_control_from_error_sink_is_not_contained_before_business_body(self):
        profile, observer = self.recorder()
        control = OperationDeadlineExceeded('PRIVATE sink deadline')
        with patch('benchmarks.events.profile_publisher.time.time_ns', side_effect=OSError('PRIVATE clock')), \
                patch.object(observer, '_record_error', side_effect=control):
            with profile.call('publisher_lifecycle'), self.assertRaises(OperationDeadlineExceeded) as raised:
                with observer.deadline_context(quiet_context, (), {}):
                    self.fail('Production deadline from sink must stop the body')
        self.assertIs(raised.exception, control)
        self.assertIsNone(observer.attempt)

    @skipUnless(hasattr(signal, 'setitimer'), 'POSIX deadline signal')
    def test_real_one_shot_alarm_in_observer_clock_stops_body_and_restores_timer_handler(self):
        profile, observer = self.recorder()
        calls, slept = [], []
        original_clock = time.time_ns
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_timer = signal.getitimer(signal.ITIMER_REAL)
        @contextmanager
        def manager():
            with operation_deadline(.02):
                calls.append('enter')
                try:
                    yield
                except BaseException as error:
                    calls.append(type(error))
                    raise
                finally:
                    calls.append('exit')
        def clock():
            if (observer.scopes and observer.scopes[-1] == 'deadline_setup_composite'
                    and calls and not slept):
                slept.append(True)
                time.sleep(.05)
            return original_clock()
        with patch('benchmarks.events.profile_publisher.time.time_ns', side_effect=clock):
            with profile.call('publisher_lifecycle'), self.assertRaises(OperationDeadlineExceeded):
                with observer.deadline_context(manager, (), {}):
                    calls.append('body')
        self.assertEqual(calls, ['enter', OperationDeadlineExceeded, 'exit'])
        self.assertIs(signal.getsignal(signal.SIGALRM), previous_handler)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[1], previous_timer[1])
        if previous_timer[0] == 0:
            self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)
        self.assertEqual(observer.document()['attempts'][0]['classification'], 'deadline_exceeded')

    def test_nonboolean_publish_result_is_preserved_without_truthiness(self):
        profile, observer = self.recorder()
        class UnknownResult:
            def __bool__(self):
                raise AssertionError('Do not infer a business result')
        result = UnknownResult()
        with profile.call('publisher_lifecycle'), observer.deadline_context(quiet_context, (), {}):
            self.assertIs(observer.invoke('publish_one_composite', lambda: result, (), {}), result)
        sample = observer.document()['attempts'][0]
        self.assertIsNone(sample['publish_result'])
        self.assertEqual(sample['publish_result_status'], 'unsupported_nonbool')
        self.assertEqual(sample['classification'], 'publish_result_unavailable')
        self.assertFalse(sample['complete'])

    def test_deadline_eligibility_error_delegates_original_context_once(self):
        profile, observer = self.recorder()
        calls, first = [], PrimaryFailure('PRIVATE body')
        @contextmanager
        def manager(*args, **kwargs):
            calls.append(('enter', args, kwargs))
            try:
                yield 17
            except PrimaryFailure as error:
                calls.append(error)
                raise
            finally:
                calls.append('exit')
        with profile.call('publisher_lifecycle'):
            with patch.object(profile, 'owning_thread', side_effect=OSError('PRIVATE eligibility')):
                with self.assertRaises(PrimaryFailure) as raised:
                    with observer.deadline_context(manager, (5,), {'fixture': 6}) as value:
                        self.assertEqual(value, 17)
                        raise first
        self.assertIs(raised.exception, first)
        self.assertEqual(calls, [('enter', (5,), {'fixture': 6}), first, 'exit'])
        self.assertEqual(observer.attempts_seen, 0)
        self.assertTrue(profile.recording_failed)

    def test_budget_eligibility_error_delegates_original_context_once(self):
        from labops import worker_metrics
        profile, _ = self.recorder()
        calls, first = [], PrimaryFailure('PRIVATE body')
        @contextmanager
        def manager(*args, **kwargs):
            calls.append(('enter', args, kwargs))
            try:
                yield 17
            except PrimaryFailure as error:
                calls.append(error)
                raise
            finally:
                calls.append('exit')
        with patch.object(command, 'database_statement_budget', manager), \
                patch.object(worker_metrics, 'database_statement_budget', manager):
            install_publisher_hooks(profile)
            with profile.call('publisher_lifecycle'):
                with patch.object(profile, 'owning_thread', side_effect=OSError('PRIVATE eligibility')):
                    with self.assertRaises(PrimaryFailure) as raised:
                        with command.database_statement_budget(5, fixture=6) as value:
                            self.assertEqual(value, 17)
                            raise first
            profile.restore()
        self.assertIs(raised.exception, first)
        self.assertEqual(calls, [('enter', (5,), {'fixture': 6}), first, 'exit'])
        self.assertTrue(profile.recording_failed)

    def test_nested_publish_cannot_rebind_first_event_or_claim_complete_coverage(self):
        profile, observer = self.recorder()
        first = OutboxEvent(id=uuid4())
        second = OutboxEvent(id=uuid4())
        nested = Mock(side_effect=lambda: observer.invoke('claim_commit_composite', lambda: second, (), {}))
        def original():
            observer.invoke('claim_commit_composite', lambda: first, (), {})
            return observer.invoke('publish_one_composite', nested, (), {})
        with profile.call('publisher_lifecycle'), observer.deadline_context(quiet_context, (), {}):
            self.assertIs(observer.invoke('publish_one_composite', original, (), {}), second)
        nested.assert_called_once_with()
        self.assertEqual(observer.sampled_attempts, 1)
        self.assertEqual(observer.samples[0]['event_id'], str(first.pk))
        self.assertTrue(all(row['event_id'] == str(first.pk) for row in observer.records))
        self.assertTrue(profile.recording_failed)
        self.assertFalse(observer.document()['complete'])

    def test_unknown_owner_dict_and_execute_wrapper_getters_are_not_read(self):
        for unknown in ('owner_dict', 'execute_wrapper'):
            profile, observer = self.recorder()
            owner = PublisherShardOwner(0, 1)
            owner._sqlite_owned = True
            getter = Mock(side_effect=AssertionError('Unknown getter must not be read'))
            if unknown == 'owner_dict':
                # type.__dict__ is not assignable. An extension class can own
                # a poisoned instance __dict__ property; substitute that exact
                # module class to exercise the static descriptor refusal.
                unknown_owner = type('PublisherShardOwner', (PublisherShardOwner,),
                    {'__dict__': property(lambda self: getter())})
                owner = unknown_owner(0, 1)
                owner._sqlite_owned = True
                change = patch('labops.publisher_shards.PublisherShardOwner', unknown_owner)
            else:
                # Poison only the dedicated owner's extension class. The
                # already existing application wrapper keeps its normal hook,
                # so deadline setup cannot touch this canary first.
                unknown_database = type('DatabaseWrapper', (DatabaseWrapper,),
                    {'execute_wrapper': property(lambda self: getter())})
                database = unknown_database({**connection.settings_dict, 'OPTIONS': {}}, alias='publisher_shard_owner')
                owner._sqlite_owned = False
                owner._connection = database
                change = patch('django.db.backends.postgresql.base.DatabaseWrapper', unknown_database)
            original = Mock(return_value=17)
            with change, profile.call('publisher_lifecycle'), observer.deadline_context(quiet_context, (), {}):
                self.assertEqual(observer.invoke('shard_ownership', original, (owner,), {}), 17)
            getter.assert_not_called()
            original.assert_called_once_with(owner)
            self.assertEqual(observer.owner_execute_unavailable, 1)
            self.assertFalse(observer.document()['complete'])


@skipUnless(connection.vendor == 'postgresql', 'Real PostgreSQL deadline, budget and dedicated ownership')
class PublisherDeadlinePostgreSQLTests(ClaimFixture, TransactionTestCase):
    def observe(self, management=None):
        with TemporaryDirectory() as name:
            output = Path(name) / 'publisher-loop-observation.json'
            result = run_publisher(output, {'limit': 1, 'loop': False, 'metrics_port': None,
                'shard_index': 0, 'shard_count': 1}, observation_only=True, call_command=management)
            document = json.loads(output.read_text())
        return result, document

    def test_real_command_adds_outer_owner_budget_and_deadline_without_extra_work_or_native_fallback(self):
        self.row()
        baseline_sql, observed_sql, owners, clients = [], [], [], []
        original_owner = PublisherShardOwner.assert_owned
        def owner_spy(owner):
            owners.append(owner)
            return original_owner(owner)
        def client(*args, **kwargs):
            result = DeliveryClientFixture(*args, **kwargs)
            clients.append(result)
            return result
        def collect(target):
            def execute(delegate, sql, params, many, context):
                target.append(sql)
                return delegate(sql, params, many, context)
            return execute
        from django.core.management import call_command
        with patch.object(PublisherShardOwner, 'assert_owned', owner_spy), patch('confluent_kafka.Producer', side_effect=client):
            with connection.execute_wrapper(collect(baseline_sql)):
                call_command('publish_events', limit=1)
            first_owners = len(owners)
            record = self.row()
            with connection.execute_wrapper(collect(observed_sql)):
                _, document = self.observe()
        observation = document['publisher_observation']
        self.assertEqual(Counter(baseline_sql), Counter(observed_sql))
        self.assertEqual(first_owners, len(owners) - first_owners)
        self.assertEqual(Counter(row[0] for row in clients[0].calls), Counter(row[0] for row in clients[1].calls))
        self.assertEqual(observation['attempts_seen'], 1)
        self.assertEqual(observation['sampled_attempts'], 1)
        self.assertTrue(observation['deadline_hook_installed'])
        sample = observation['attempts'][0]
        self.assertEqual(sample['scope'], 'operation_deadline_context')
        self.assertEqual(sample['claim_path'], 'native_postgresql')
        self.assertEqual(sample['event_id'], str(record.pk))
        self.assertTrue(sample['complete'], sample)
        self.assertIs(sample['publish_result'], True)
        self.assertEqual(sample['publish_result_status'], 'observed_bool')
        self.assertTrue(observation['complete'], observation)
        counts = sample['statement_execute_counts']
        self.assertGreater(counts['budget_setup_execute'], 0)
        self.assertEqual(counts['budget_restore_execute'], 1)
        self.assertEqual(counts['outer_shard_ownership_execute'], 1)
        self.assertEqual(counts['inner_shard_ownership_execute'], 2)
        self.assertEqual(observation['boundaries']['publish_one_composite'], 1)
        self.assertEqual(observation['boundaries']['deadline_scope_composite'], 1)
        self.assertTrue(all(row['event_id'] == str(record.pk) for row in observation['records']))
        self.assertFalse(document['qualification_admissible'])
        self.assertFalse(document['coverage']['complete'])
        self.assertEqual(observation['topology_provenance']['actual_topology'], 'NOT_OBSERVED_IN_PROFILE')
        self.assertEqual(observation['topology_provenance']['function_guard_template']['writer_roles'], 4)

    def test_empty_scope_has_one_attempt_outer_owner_and_real_budget_counts_without_ack(self):
        with patch('confluent_kafka.Producer', DeliveryClientFixture):
            _, document = self.observe()
        observation = document['publisher_observation']
        sample = observation['attempts'][0]
        self.assertTrue(sample['empty_claim'])
        self.assertIs(sample['publish_result'], False)
        self.assertTrue(sample['complete'], sample)
        self.assertEqual(sample['classification'], 'empty_claim')
        self.assertEqual(observation['attempts_seen'], 1)
        self.assertNotIn('delivery_ack', observation['boundaries'])
        self.assertEqual(sample['statement_execute_counts']['outer_shard_ownership_execute'], 1)
        self.assertNotIn('inner_shard_ownership_execute', sample['statement_execute_counts'])

    def test_original_budget_swallowed_restore_error_still_returns_publish_result_and_records_failure(self):
        record = self.row()
        profile = CPUProfile('publisher', observation_only=True)
        error = OperationalError('PRIVATE restore failure')
        failed = []
        def fail_restore(execute, sql, params, many, context):
            if profile.publisher_observation.scopes[-1] == 'budget_restore_composite':
                failed.append(True)
                raise error
            return execute(sql, params, many, context)
        def management(*args, **kwargs):
            from labops.events import producer
            broker = producer()
            with publisher_shard_owner(0, 1) as owner:
                with command.operation_deadline(30):
                    owner.assert_owned()
                    with connection.execute_wrapper(fail_restore), command.database_statement_budget(5):
                        return command.publish_one(broker, ownership_check=owner.assert_owned)
        with TemporaryDirectory() as name, patch('confluent_kafka.Producer', DeliveryClientFixture):
            path = Path(name) / 'failed-restore.json'
            result = run_publisher(path, {}, observation_only=True, call_command=management,
                                   profile_factory=lambda role: profile)
            document = json.loads(path.read_text())
        self.assertIs(result, True)
        self.assertEqual(failed, [True])
        sample = document['publisher_observation']['attempts'][0]
        self.assertEqual(sample['outcome'], 'returned')
        self.assertEqual(sample['classification'], 'budget_restore_failed')
        self.assertFalse(sample['complete'])
        self.assertEqual(sample['statement_execute_counts']['budget_restore_execute'], 1)
        self.assertFalse(document['publisher_observation']['complete'])
        self.assertNotIn('PRIVATE', json.dumps(document))
        record.refresh_from_db()
        self.assertEqual(record.status, 'PUBLISHED')

    def test_callable_false_after_real_ack_and_mark_is_preserved_and_never_classified_published(self):
        record = self.row()
        from labops import events
        profile = CPUProfile('publisher', observation_only=True)
        observer = PublisherObservation(profile)
        profile.publisher_observation = observer
        self.addCleanup(profile.restore)
        observer.install_claim_hooks()
        for target, name, phase in [(events, 'claim_event', 'claim_commit_composite'),
                (events, 'send', 'send_composite'), (events, 'owned_event', 'lease_check'),
                (PublisherShardOwner, 'assert_owned', 'shard_ownership')]:
            profile.hook(target, name, phase, expected=getattr(target, name), observer=observer)
        with patch('confluent_kafka.Producer', DeliveryClientFixture), profile.call('publisher_lifecycle'):
            broker = events.producer()
            with publisher_shard_owner(0, 1) as owner:
                with observer.deadline_context(operation_deadline, (30,), {}):
                    owner.assert_owned()
                    with observer.context_delegate('budget', command.database_statement_budget, (5,), {}):
                        def original():
                            self.assertIs(events.publish_one(broker, ownership_check=owner.assert_owned), True)
                            # An extension's actual return is independent of
                            # the original publication's successful stages.
                            return False
                        result = observer.invoke('publish_one_composite', original, (), {})
        profile.restore()
        self.assertIs(result, False)
        observation = observer.document()
        sample = observation['attempts'][0]
        self.assertIs(sample['publish_result'], False)
        self.assertEqual(sample['outcome'], 'returned')
        self.assertEqual(sample['classification'], 'publish_returned_false')
        self.assertFalse(sample['empty_claim'])
        self.assertFalse(sample['complete'])
        self.assertEqual(observation['boundaries']['delivery_ack'], 1)
        self.assertEqual(sample['statement_execute_counts']['publication_mark_execute'], 1)
        self.assertFalse(observation['complete'])
        record.refresh_from_db()
        self.assertEqual(record.status, 'PUBLISHED')

    def test_original_budget_entry_error_propagates_before_publish_and_owner_loss_has_no_budget_work(self):
        for failure_at in ('budget', 'owner'):
            record = self.row()
            profile = CPUProfile('publisher', observation_only=True)
            original = OperationalError('PRIVATE budget failure')
            def fail_setup(execute, sql, params, many, context):
                if profile.publisher_observation.scopes[-1] == 'budget_setup_composite':
                    raise original
                return execute(sql, params, many, context)
            def management(*args, **kwargs):
                with publisher_shard_owner(0, 1) as owner:
                    with command.operation_deadline(30):
                        if failure_at == 'owner':
                            owner._lost = True
                        owner.assert_owned()
                        with connection.execute_wrapper(fail_setup), command.database_statement_budget(5):
                            self.fail('Failed budget entry must never reach publication')
            with TemporaryDirectory() as name:
                path = Path(name) / 'entry-error.json'
                with self.assertRaises(OperationalError if failure_at == 'budget' else ShardOwnershipLost) as raised:
                    run_publisher(path, {}, observation_only=True, call_command=management,
                                  profile_factory=lambda role: profile)
                document = json.loads(path.read_text())
            if failure_at == 'budget':
                self.assertIs(raised.exception, original)
            sample = document['publisher_observation']['attempts'][0]
            self.assertEqual(sample['classification'], 'budget_setup_failed' if failure_at == 'budget' else 'owner_lost')
            self.assertEqual(sample['scope'], 'operation_deadline_context')
            self.assertEqual(document['publisher_observation']['attempts_seen'], 1)
            self.assertNotIn('publish_one_composite', document['publisher_observation']['boundaries'])
            self.assertNotIn('delivery_ack', document['publisher_observation']['boundaries'])
            self.assertFalse(sample['complete'])
            record.refresh_from_db()
            self.assertEqual(record.status, 'PENDING')
