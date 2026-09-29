"""Prepared native-scoped diagnostic controls; PG execution NOTEXECUTED.

The PostgreSQL cases use the existing global policy, real confluent Producer,
real dedicated shard owner, and original Django model/commit functions. They
never send broker records. Nonempty writeback is an explicitly isolated real
SQL/fence proof, not an end-to-end publication or real broker ACK proof.
Synthetic send/callback cases are separately named and cannot qualify a native
publisher attempt. No test replaces protected function code/defaults, rebuilds
the admission policy, or swaps a real Producer's client after admission.
"""
from contextlib import contextmanager
from datetime import timedelta
import json
import sys
from types import CodeType, FunctionType
from unittest import skipUnless
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from confluent_kafka.cimpl import Producer
from django.conf import settings
from django.db import connection, connections
from django.db.backends.postgresql.base import Cursor, ServerBindingCursor
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from labops import events, publisher_observation as observation, worker_metrics
from labops.management.commands import publish_events as command
from labops.models import OutboxEvent
from labops.publisher_shards import ShardOwnershipLost, publisher_shard_owner
from labops.tests.test_diagnostic_profile import DeliveryClientFixture
from labops.tests.test_publisher_budget_reuse_admission import (
    ordinary_connection_capability, plain_tracing,
)
from labops.tests.test_publisher_claim_reads import ClaimFixture
from labops.worker_metrics import OperationDeadlineExceeded, StopController, operation_deadline


class NativeObservationControl(BaseException):
    pass


@contextmanager
def trace_originals(targets, rows, inject=None):
    """Observe original frames, without callable substitution or re-admission.

    targets map labels to (held original function, frame predicate). Some
    Django async-safe decorators share a code object; their original wrapped
    body is traced instead and the held public binding is checked separately.
    Injection is test-only and runs inside a selected original frame.
    """
    held = [(function, function.__code__, function.__defaults__, function.__kwdefaults__)
        for function, _predicate in targets.values()]
    previous = sys.gettrace()
    def tracer(frame, kind, value):
        for label, (function, predicate) in targets.items():
            if frame.f_code is function.__code__ and predicate(frame):
                if kind in ('call', 'return', 'exception'):
                    rows.append((label, kind, dict(frame.f_locals), value))
                if inject is not None:
                    inject(label, frame, kind, value)
        return tracer
    try:
        sys.settrace(tracer)
        yield
    finally:
        sys.settrace(previous)
        for function, code, defaults, kwdefaults in held:
            if (function.__code__ is not code or function.__defaults__ is not defaults
                    or function.__kwdefaults__ is not kwdefaults):
                raise AssertionError('Test must not replace original code/defaults')


def calls(rows, label, kind='call'):
    return [row for row in rows if row[0] == label and row[1] == kind]


def callback_from_original_source(results):
    codes = [value for value in events.send.__code__.co_consts
        if type(value) is CodeType and value.co_name == '<lambda>']
    if len(codes) != 1 or codes[0].co_freevars != ('results',):
        raise AssertionError('Expected exactly the original send callback')
    cell = (lambda: results).__closure__[0]
    return FunctionType(codes[0], events.send.__globals__, closure=(cell,))


class NativeObservationCapabilityTests(SimpleTestCase):
    def owned(self):
        owned = observation.NativePublisherObservation()
        owned.bind(command._BUDGET_ADMISSION, events.send)
        changed = patch.object(observation, 'ACTIVE', owned)
        changed.start()
        self.addCleanup(changed.stop)
        self.assertIs(observation.current(observation.current), owned)
        return owned

    def synthetic_attempt(self):
        # Explicit clock/callback controls only: these literals are NOT an
        # actual plain() or native claim observation and cannot complete.
        owned = self.owned()
        token = owned.begin_attempt()
        owned.guard_result(command._BUDGET_ADMISSION, True)
        owned.native_result(True)
        return owned, token

    def test_bootstrap_and_active_identity_use_the_fixed_constructor(self):
        with patch.object(observation, 'ACTIVE', None):
            self.assertIs(observation.current(observation.current, bootstrap=True),
                observation.NativePublisherObservation)
            self.assertIsNone(observation.current(observation.current))
        owned = self.owned()
        self.assertIs(observation.current(observation.current), owned)
        self.assertIs(observation.current(observation.current, bootstrap=True), owned)
        self.assertIsNone(observation.current(lambda: owned))

    def test_mutable_instance_method_shadow_is_refused_without_call(self):
        owned = self.owned()
        unknown = Mock(side_effect=AssertionError('Unknown observer must never run'))
        for name in ('measure', 'start', 'clock', 'claim_scope', 'execute_scope', 'callback'):
            with self.subTest(method=name):
                with patch.object(owned, name, unknown):
                    self.assertIsNone(observation.current(observation.current))
                    unknown.assert_not_called()
                self.assertIs(observation.current(observation.current), owned)
        self.assertEqual(owned.clock_calls, 0)
        self.assertEqual(owned.hook_installs, 0)

    def test_mutable_class_function_and_native_clock_shadows_are_refused_without_call(self):
        owned = self.owned()
        unknown = Mock(side_effect=AssertionError('Unknown capability must never run'))
        with patch.object(observation.NativePublisherObservation, 'start', unknown):
            self.assertIsNone(observation.current(observation.current))
        unknown.assert_not_called()
        original = observation.NativePublisherObservation.start
        try:
            original.untrusted = unknown
            self.assertIsNone(observation.current(observation.current))
            unknown.assert_not_called()
        finally:
            del original.untrusted
        with patch.object(observation.time, 'time_ns', unknown):
            self.assertIsNone(observation.current(observation.current))
            unknown.assert_not_called()
        self.assertIs(observation.current(observation.current), owned)
        self.assertEqual(owned.clock_calls, 0)

    def test_unknown_active_object_and_non_native_state_never_read_getters(self):
        owned = self.owned()
        unknown = Mock(side_effect=AssertionError('Unknown getter must never run'))
        class Unknown:
            def __getattribute__(self, name):
                return unknown(name)
        with patch.object(observation, 'ACTIVE', Unknown()):
            self.assertIsNone(observation.current(observation.current))
        with patch.object(owned, 'records', Unknown()):
            self.assertIsNone(observation.current(observation.current))
        unknown.assert_not_called()
        self.assertIs(observation.current(observation.current), owned)

    def test_synthetic_original_callback_args_return_identity_and_single_call(self):
        owned, token = self.synthetic_attempt()
        results, message, rows = [], object(), []
        original = callback_from_original_source(results)
        tapped = owned.callback(original)
        self.assertIsNot(tapped, original)
        with trace_originals({'callback': (original, lambda frame: True)}, rows):
            returned = tapped(None, message)
        self.assertIs(returned, None)
        self.assertEqual(results, [None])
        self.assertEqual(len(calls(rows, 'callback')), 1)
        self.assertIs(calls(rows, 'callback')[0][2]['err'], None)
        self.assertIs(calls(rows, 'callback')[0][2]['msg'], message)
        self.assertIs(calls(rows, 'callback', 'return')[0][3], returned)
        owned.end_attempt(token)
        document = owned.close()
        self.assertEqual(document['boundaries'].get('delivery_ack'), 1)
        self.assertFalse(document['complete'], 'Synthetic callback is not real native publication')
        self.assertFalse(document['qualification_admissible'])

    def test_synthetic_original_callback_control_retains_the_same_baseexception(self):
        owned, token = self.synthetic_attempt()
        results, message, rows = [], object(), []
        original = callback_from_original_source(results)
        first, injected = NativeObservationControl('PRIVATE callback control'), []
        def inject(label, frame, kind, value):
            if kind == 'call' and not injected:
                injected.append(first)
                raise first
        primary = None
        try:
            with self.assertRaises(NativeObservationControl) as raised:
                with trace_originals({'callback': (original, lambda frame: True)}, rows, inject):
                    owned.callback(original)(None, message)
            primary = raised.exception
            self.assertIs(primary, first)
            self.assertEqual(len(calls(rows, 'callback')), 1)
            self.assertEqual(results, [])
        finally:
            owned.end_attempt(token, primary)
        self.assertEqual(owned.temporary_depth, 0)
        document = owned.close()
        self.assertFalse(document['complete'])
        self.assertNotIn('PRIVATE', json.dumps(document))

    def test_synthetic_direct_send_preserves_original_args_encoding_flush_and_return(self):
        # Transport fixture, NOT the native Producer: no native admission claim.
        owned, token = self.synthetic_attempt()
        broker, value, rows = DeliveryClientFixture({}), {'trace_context': {}, 'fixture': 'safe'}, []
        original = events.send
        with plain_tracing(), trace_originals({'send': (original, lambda frame: True)}, rows):
            returned = original(broker, settings.KAFKA_TOPIC, 'fixture-key', value)
        self.assertIs(returned, None)
        self.assertEqual(len(calls(rows, 'send')), 1)
        arguments = calls(rows, 'send')[0][2]
        self.assertIs(arguments['producer'], broker)
        self.assertIs(arguments['value'], value)
        self.assertEqual((arguments['topic'], arguments['key']), (settings.KAFKA_TOPIC, 'fixture-key'))
        self.assertIs(calls(rows, 'send', 'return')[0][3], returned)
        self.assertEqual([row[0] for row in broker.calls], ['produce', 'flush'])
        produced, flushed = broker.calls
        self.assertEqual(produced[1], (settings.KAFKA_TOPIC,))
        self.assertEqual(produced[2]['key'], 'fixture-key')
        self.assertEqual(json.loads(produced[2]['value']), value)
        self.assertEqual(flushed[1], (settings.KAFKA_PUBLISH_FLUSH_SECONDS,))
        owned.end_attempt(token)
        document = owned.close()
        self.assertEqual(document['boundaries'].get('delivery_ack'), 1)
        self.assertFalse(document['complete'])
        self.assertFalse(document['qualification_admissible'])


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL native diagnostic controls')
@override_settings(EVENT_LEASE_SECONDS=60)
class NativeObservationPostgreSQLTests(ClaimFixture, TransactionTestCase):
    @contextmanager
    def database(self, server_binding):
        database = connections['default']
        options = database.settings_dict['OPTIONS']
        database.close()
        database.settings_dict['OPTIONS'] = {**options, 'server_side_binding': server_binding,
            'prepare_threshold': None}
        try:
            database.ensure_connection()
            with database.cursor() as cursor:
                self.assertIs(type(cursor.cursor), ServerBindingCursor if server_binding else Cursor)
                cursor.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)",
                    ['37000', '91'])
            yield database
        finally:
            database.close()
            database.settings_dict['OPTIONS'] = options

    @contextmanager
    def native_owner(self, server_binding=False):
        with ordinary_connection_capability(), self.database(server_binding) as database, plain_tracing():
            broker, policy = events.producer(), command._BUDGET_ADMISSION
            self.assertIs(type(broker.client), Producer)
            self.assertIsNotNone(policy)
            self.assertTrue(policy.valid)
            self.assertNotIn('commit', vars(database))
            self.assertNotIn('from_db', vars(OutboxEvent))
            with StopController() as stop, publisher_shard_owner(0, 1) as owner:
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                owned = observation.NativePublisherObservation()
                owned.bind(policy, events.send)
                with patch.object(observation, 'ACTIVE', owned):
                    self.assertIs(observation.current(observation.current), owned)
                    yield database, broker, policy, stop, owner, owned

    def targets(self, database, policy, owner):
        commit_body = getattr(observation._Commit, '__wrapped__', observation._Commit)
        return {
            'plain': (observation._Plain, lambda frame: frame.f_locals.get('self') is policy),
            'native_guard': (events._plain_outbox_claim, lambda frame: True),
            'native_claim': (events._claim_event_postgresql, lambda frame: True),
            'from_db': (observation._FromDB, lambda frame: frame.f_locals.get('cls') is OutboxEvent),
            'commit': (commit_body, lambda frame: frame.f_locals.get('self') is database),
            'owner': (type(owner).assert_owned, lambda frame: frame.f_locals.get('self') is owner),
        }

    def assert_restored(self, database, owned):
        self.assertEqual(database.execute_wrappers, [])
        self.assertNotIn('from_db', vars(OutboxEvent))
        self.assertNotIn('commit', vars(database))
        self.assertIs(database.commit.__func__, observation._Commit)
        self.assertEqual(owned.temporary_depth, 0)
        self.assertEqual(owned.hook_installs, owned.hook_restores)
        self.assertEqual(owned.hook_failures, 0)
        self.assertEqual(owned.parents, [])

    def admission(self, owned, policy, broker, owner, stop):
        self.assertIs(command._BUDGET_ADMISSION, policy, 'Never rebuild a policy to hide drift')
        with owned.measure('admission_call'):
            result = policy.plain(broker, owner, command._budget_aliases(), stop)
            owned.guard_result(policy, result)
        self.assertIs(result, True)
        return result

    def claim(self, owned):
        with owned.measure('claim_composite'):
            event = events.claim_event(shard_index=0, shard_count=1)
        owned.bind_event(event)
        return event

    def test_real_empty_native_publish_restores_before_record_complete_session_and_next_plain(self):
        self.assertFalse(OutboxEvent.objects.filter(transport='kafka').exists())
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.native_owner(enabled) as state:
                database, broker, policy, stop, owner, owned = state
                raw, owner_raw, owner_pid, rows = database.connection, owner._raw_connection, owner.backend_pid, []
                batch = worker_metrics.PublisherBatchBudget(1.25, 3,
                    admission=lambda: self.admission(owned, policy, broker, owner, stop))
                batch.before_deadline()
                token, primary = owned.begin_attempt(), None
                try:
                    with operation_deadline(30), trace_originals(self.targets(database, policy, owner), rows), CaptureQueriesContext(database) as queries:
                        owner.assert_owned()
                        with batch.record(command.database_statement_budget) as record:
                            result = command.publish_one(broker, ownership_check=owner.assert_owned)
                            self.assertIs(result, False)
                            # All temporary measurement hooks are absent BEFORE
                            # both the actual session gate and record.complete.
                            self.assert_restored(database, owned)
                            session = worker_metrics.PublisherBatchBudget._session(database)
                            self.assertIsNotNone(session)
                            self.assertIs(session[0], raw)
                            self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                            owned.publish_result(result)
                            record.complete(result)
                            record.retain(False)
                except BaseException as error:
                    primary = error
                    raise
                finally:
                    owned.end_attempt(token, primary)
                self.assertEqual(token['guard_results'], [True])
                self.assertEqual(token['native_claim_results'], [True])
                self.assertEqual(token['claim_path'], 'native_postgresql')
                self.assertEqual(len(calls(rows, 'plain')), 2)
                self.assertEqual(len(calls(rows, 'native_guard')), 1)
                self.assertEqual(len(calls(rows, 'native_claim')), 1)
                self.assertEqual(len(calls(rows, 'from_db')), 0)
                self.assertEqual(len(calls(rows, 'commit')), 1)
                self.assertIs(calls(rows, 'native_claim', 'return')[0][3], None)
                self.assertIs(calls(rows, 'commit', 'return')[0][3], None)
                self.assertEqual(sum(row['sql'].lstrip().startswith('WITH claimed AS (') for row in queries), 1)
                self.assertEqual(batch.counts['admitted_records'], 1)
                self.assertEqual(batch.counts['fallback_records'], 0)
                self.assertEqual(batch.counts['setup_completed'], 1)
                self.assertEqual(batch.counts['restore_completed'], 1)
                self.assertEqual(batch.returned_false, 1)
                self.assertIs(database.connection, raw)
                self.assertIs(owner._raw_connection, owner_raw)
                self.assertEqual(owner.backend_pid, owner_pid)
                self.assertIsNot(owner_raw, raw)
                owner.assert_owned()
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                document = owned.close()
                self.assertTrue(document['complete'], document)
                self.assertEqual(document['boundaries'].get('atomic_claim_execute'), 1)
                self.assertEqual(document['boundaries'].get('claim_physical_commit'), 1)
                self.assertNotIn('delivery_ack', document['boundaries'])
                self.assertFalse(document['qualification_admissible'])

    def test_real_nonempty_claim_original_args_return_identity_commit_and_autocommit_writeback(self):
        source = self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            raw, owner_raw, rows = database.connection, owner._raw_connection, []
            token = owned.begin_attempt()
            with trace_originals(self.targets(database, policy, owner), rows), CaptureQueriesContext(database) as queries:
                self.admission(owned, policy, broker, owner, stop)
                claimed = self.claim(owned)
                self.assertIs(type(claimed), OutboxEvent)
                self.assertEqual(claimed.pk, source.pk)
                self.assertIs(calls(rows, 'from_db', 'return')[0][3], claimed)
                self.assertIs(calls(rows, 'native_claim', 'return')[0][3], claimed)
                self.assertEqual(len(calls(rows, 'from_db')), 1)
                self.assertEqual(len(calls(rows, 'commit')), 1)
                construct = calls(rows, 'from_db')[0][2]
                self.assertIs(construct['cls'], OutboxEvent)
                self.assertEqual(construct['db'], 'default')
                self.assertIn('lease_token', construct['field_names'])
                guard = calls(rows, 'plain')[0][2]
                self.assertIs(guard['broker'], broker)
                self.assertIs(guard['owner'], owner)
                self.assertIs(guard['stop'], stop)
                self.assertTrue(all(actual is expected for actual, expected in zip(guard['aliases'], policy.aliases)))
                self.assertIs(calls(rows, 'plain', 'return')[0][3], True)
                helper = calls(rows, 'native_claim')[0][2]
                self.assertIs(helper['manager'], OutboxEvent.objects)
                self.assertEqual((helper['shard_index'], helper['shard_count']), (0, 1))
                self.assert_restored(database, owned)
                self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                owner.assert_owned()
                self.assertTrue(events.owned_event(claimed))
                self.assertTrue(database.get_autocommit())
                self.assertFalse(database.in_atomic_block)
                # Isolated actual SQL boundary, no broker ACK/publication claim.
                with owned.measure('publication_writeback'), owned.execute_scope('publication_writeback_execute') as hooks:
                    hooks.install()
                    changed = OutboxEvent.objects.filter(pk=claimed.pk, status='PROCESSING',
                        lease_token=claimed.lease_token, locked_until__gt=timezone.now()).update(
                        status='PUBLISHED', published_at=timezone.now(), locked_until=None,
                        lease_token=None, last_error='')
                self.assertEqual(changed, 1)
                self.assert_restored(database, owned)
                self.assertEqual(len(calls(rows, 'commit')), 1, 'Autocommit writeback is not another Django commit call')
            owned.end_attempt(token)
            persisted = OutboxEvent.objects.get(pk=source.pk)
            self.assertEqual(persisted.status, 'PUBLISHED')
            self.assertIsNone(persisted.lease_token)
            self.assertIsNone(persisted.locked_until)
            self.assertIs(database.connection, raw)
            self.assertIs(owner._raw_connection, owner_raw)
            owner.assert_owned()
            self.assertEqual(sum(row['sql'].lstrip().startswith('WITH claimed AS (') for row in queries), 1)
            document = owned.close()
            self.assertEqual(document['boundaries'].get('claim_object_construct'), 1)
            self.assertEqual(document['boundaries'].get('claim_physical_commit'), 1)
            self.assertEqual(document['boundaries'].get('publication_writeback_execute'), 1)
            self.assertNotIn('delivery_ack', document['boundaries'])
            self.assertFalse(document['complete'], 'Isolated SQL cannot establish an ACKed publication')

    def test_original_from_db_control_rolls_back_and_restores_before_recheck(self):
        self.original_control('from_db', 'call', NativeObservationControl('PRIVATE construction'), 'PENDING')

    def test_original_commit_return_control_preserves_committed_lease_and_same_deadline(self):
        # Inject only after the original physical commit returned. This avoids
        # fabricating a commit failure and makes the committed lease explicit.
        self.original_control('commit', 'return', OperationDeadlineExceeded('PRIVATE commit-return'), 'PROCESSING')

    def original_control(self, role, edge, first, expected_status):
        source = self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            raw, owner_raw, rows, injected = database.connection, owner._raw_connection, [], []
            token, primary = owned.begin_attempt(), None
            def inject(label, frame, kind, value):
                if label == role and kind == edge and not injected:
                    self.assertEqual(token['guard_results'], [True])
                    self.assertEqual(token['native_claim_results'], [True])
                    injected.append(first)
                    raise first
            try:
                with self.assertRaises(type(first)) as raised:
                    with trace_originals(self.targets(database, policy, owner), rows, inject):
                        self.admission(owned, policy, broker, owner, stop)
                        self.claim(owned)
                primary = raised.exception
                self.assertIs(primary, first)
                self.assertEqual(injected, [first])
                self.assertEqual(len(calls(rows, role)), 1)
                self.assertEqual(len(calls(rows, 'native_claim')), 1)
                self.assert_restored(database, owned)
                self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                self.assertIs(database.connection, raw)
                self.assertIs(owner._raw_connection, owner_raw)
                owner.assert_owned()
                persisted = OutboxEvent.objects.get(pk=source.pk)
                self.assertEqual(persisted.status, expected_status)
                if expected_status == 'PROCESSING':
                    self.assertIs(type(persisted.lease_token), UUID)
                    self.assertIsNotNone(persisted.locked_until)
                    self.assertEqual(len(calls(rows, 'commit')), 1)
                else:
                    self.assertIsNone(persisted.lease_token)
                    self.assertEqual(len(calls(rows, 'commit')), 0)
            finally:
                owned.end_attempt(token, primary)
            document = owned.close()
            self.assertFalse(document['complete'])
            self.assertTrue(document['attempts'][0]['temporary_hooks_restored'])
            self.assertEqual(document['attempts'][0]['outcome'], 'error')
            self.assertEqual(document['attempts'][0]['exception_type'], type(first).__name__)
            self.assertNotIn('PRIVATE', json.dumps(document))

    def test_real_changed_lease_fence_rejects_writeback_without_extra_physical_commit(self):
        source = self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            token, rows = owned.begin_attempt(), []
            with trace_originals(self.targets(database, policy, owner), rows):
                self.admission(owned, policy, broker, owner, stop)
                claimed = self.claim(owned)
                self.assert_restored(database, owned)
                replacement = uuid4()
                OutboxEvent.objects.filter(pk=source.pk).update(lease_token=replacement)
                self.assertFalse(events.owned_event(claimed))
                with owned.measure('publication_writeback'), owned.execute_scope('publication_writeback_execute') as hooks:
                    hooks.install()
                    changed = OutboxEvent.objects.filter(pk=claimed.pk, status='PROCESSING',
                        lease_token=claimed.lease_token, locked_until__gt=timezone.now()).update(
                        status='PUBLISHED', published_at=timezone.now(), locked_until=None,
                        lease_token=None, last_error='')
                self.assertEqual(changed, 0)
                self.assertEqual(len(calls(rows, 'commit')), 1)
                self.assert_restored(database, owned)
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                owner.assert_owned()
            owned.end_attempt(token)
            persisted = OutboxEvent.objects.get(pk=source.pk)
            self.assertEqual((persisted.status, persisted.lease_token), ('PROCESSING', replacement))
            self.assertIsNone(persisted.published_at)
            self.assertFalse(owned.close()['complete'])

    def test_real_expired_lease_rejects_writeback_and_preserves_original_token(self):
        source = self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            token = owned.begin_attempt()
            self.admission(owned, policy, broker, owner, stop)
            claimed = self.claim(owned)
            self.assert_restored(database, owned)
            OutboxEvent.objects.filter(pk=source.pk).update(locked_until=timezone.now() - timedelta(seconds=1))
            self.assertFalse(events.owned_event(claimed))
            with owned.execute_scope('publication_writeback_execute') as hooks:
                hooks.install()
                changed = OutboxEvent.objects.filter(pk=claimed.pk, status='PROCESSING',
                    lease_token=claimed.lease_token, locked_until__gt=timezone.now()).update(status='PUBLISHED')
            self.assertEqual(changed, 0)
            self.assert_restored(database, owned)
            self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
            owner.assert_owned()
            owned.end_attempt(token)
            persisted = OutboxEvent.objects.get(pk=source.pk)
            self.assertEqual((persisted.status, persisted.lease_token), ('PROCESSING', claimed.lease_token))
            self.assertIsNone(persisted.published_at)
            self.assertFalse(owned.close()['complete'])

    def test_real_dedicated_owner_loss_keeps_application_session_and_no_reacquisition(self):
        source = self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            raw, owner_raw, owner_pid = database.connection, owner._raw_connection, owner.backend_pid
            token, primary = owned.begin_attempt(), None
            try:
                self.admission(owned, policy, broker, owner, stop)
                claimed = self.claim(owned)
                self.assert_restored(database, owned)
                owner_raw.close()
                with self.assertRaises(ShardOwnershipLost) as raised:
                    owner.assert_owned()
                primary = raised.exception
                self.assertIs(owner._raw_connection, owner_raw)
                self.assertEqual(owner.backend_pid, owner_pid)
                self.assertTrue(owner_raw.closed)
                self.assertTrue(owner._lost)
                with self.assertRaises(ShardOwnershipLost):
                    owner.assert_owned()
                self.assertIs(database.connection, raw)
                self.assertFalse(raw.closed)
                self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))
                persisted = OutboxEvent.objects.get(pk=source.pk)
                self.assertEqual((persisted.status, persisted.lease_token), ('PROCESSING', claimed.lease_token))
                self.assertIsNone(persisted.published_at)
            finally:
                owned.end_attempt(token, primary)
            document = owned.close()
            self.assertFalse(document['complete'])
            self.assertNotIn('send', document['boundaries'])
            self.assertNotIn('publication_writeback_execute', document['boundaries'])

    def test_owned_cleanup_control_before_pop_and_after_return_restore_all_hooks_before_rethrow(self):
        for edge in ('call', 'return'):
            with self.subTest(cleanup_edge=edge):
                self.cleanup_control(edge)

    def test_owned_cleanup_secondary_control_preserves_explicit_scope_fixture_primary_after_real_cte(self):
        # The original from_db Control test separately proves an actual
        # business-function exception. Raising that first error FROM a trace
        # callback disables CPython tracing, so it cannot also inject cleanup.
        # Here a successful real native CTE precedes an explicitly synthetic
        # scope-body primary; only the original cleanup frame is trace-raised.
        self.cleanup_control('call', NativeObservationControl('PRIVATE scope-fixture primary'))

    def test_original_scope_install_return_control_restores_hooks_before_next_native_admission(self):
        for scope_name in ('claim', 'execute'):
            with self.subTest(scope=scope_name):
                self.row()
                with self.native_owner() as state:
                    database, broker, policy, stop, owner, owned = state
                    token, primary, rows, injected = owned.begin_attempt(), None, [], []
                    first = NativeObservationControl('PRIVATE installed-scope control')
                    targets = self.targets(database, policy, owner)
                    scope = observation._ClaimScope if scope_name == 'claim' else observation._ExecuteScope
                    targets['install'] = (scope.install,
                        lambda frame: frame.f_locals.get('self').observer is owned)
                    def inject(label, frame, kind, value):
                        if label == 'install' and kind == 'return' and not injected:
                            self.assertEqual(token['guard_results'], [True])
                            self.assertEqual(token['native_claim_results'], [True])
                            self.assertGreater(owned.temporary_depth, 0)
                            self.assertEqual(len(database.execute_wrappers), 1)
                            injected.append(first)
                            raise first
                    try:
                        try:
                            with trace_originals(targets, rows, inject):
                                self.admission(owned, policy, broker, owner, stop)
                                claimed = self.claim(owned)
                                if scope_name == 'execute':
                                    with owned.execute_scope('publication_writeback_execute') as hooks:
                                        hooks.install()
                                        self.fail('Install-return control must prevent the writeback body')
                        except BaseException as caught:
                            primary = caught
                        self.assertIs(primary, first)
                        self.assertEqual(injected, [first])
                        self.assertEqual(len(calls(rows, 'install')), 1)
                        self.assert_restored(database, owned)
                        self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))
                        self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                        owner.assert_owned()
                    finally:
                        owned.end_attempt(token, primary)
                    next_token = owned.begin_attempt()
                    self.admission(owned, policy, broker, owner, stop)
                    self.claim(owned)
                    self.assertEqual(next_token['native_claim_results'], [True])
                    self.assert_restored(database, owned)
                    owned.end_attempt(next_token)
                    self.assertFalse(owned.close()['complete'])

    def cleanup_control(self, edge, business=None):
        source = self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            rows, injected = [], []
            first_cleanup = NativeObservationControl('PRIVATE cleanup control')
            token, primary = owned.begin_attempt(), None
            targets = self.targets(database, policy, owner)
            targets['leave_execute'] = (observation.NativePublisherObservation.leave_execute,
                lambda frame: frame.f_locals.get('self') is owned and frame.f_locals.get('token') is not None)
            def inject(label, frame, kind, value):
                if label == 'leave_execute' and kind == edge and first_cleanup not in injected:
                    self.assertEqual(token['guard_results'], [True])
                    self.assertEqual(token['native_claim_results'], [True])
                    # Commit and model bindings still belong to this installed
                    # scope when the first execute cleanup is interrupted.
                    self.assertIn('commit', vars(database))
                    self.assertIn('from_db', vars(OutboxEvent))
                    injected.append(first_cleanup)
                    raise first_cleanup
            try:
                try:
                    if business is None:
                        with trace_originals(targets, rows, inject):
                            self.admission(owned, policy, broker, owner, stop)
                            self.claim(owned)
                    else:
                        # Phase 1: actual global plain True, actual native
                        # selection/CTE/from_db/physical commit, all successful.
                        native_rows = []
                        with trace_originals(self.targets(database, policy, owner), native_rows), CaptureQueriesContext(database) as queries:
                            self.admission(owned, policy, broker, owner, stop)
                            claimed = self.claim(owned)
                        self.assertEqual(claimed.pk, source.pk)
                        self.assertEqual(len(calls(native_rows, 'plain')), 1)
                        self.assertIs(calls(native_rows, 'plain', 'return')[0][3], True)
                        self.assertEqual(len(calls(native_rows, 'native_guard')), 1)
                        self.assertIs(calls(native_rows, 'native_guard', 'return')[0][3], True)
                        self.assertEqual(len(calls(native_rows, 'native_claim')), 1)
                        self.assertIs(calls(native_rows, 'native_claim', 'return')[0][3], claimed)
                        self.assertEqual(len(calls(native_rows, 'from_db')), 1)
                        self.assertEqual(len(calls(native_rows, 'commit')), 1)
                        self.assertIs(calls(native_rows, 'commit', 'return')[0][3], None)
                        self.assertEqual(sum(row['sql'].lstrip().startswith('WITH claimed AS (') for row in queries), 1)
                        self.assert_restored(database, owned)
                        # Phase 2 is a labelled scope fixture, not another CTE,
                        # broker ACK, or original business-function exception.
                        # A normal Python raise leaves tracing active until the
                        # single secondary injected in original leave_execute.
                        with trace_originals(targets, rows, inject):
                            with owned.claim_scope() as hooks:
                                hooks.install()
                                injected.append(business)
                                raise business
                except BaseException as caught:
                    primary = caught
                self.assertIs(primary, business if business is not None else first_cleanup)
                self.assertEqual(injected, [first_cleanup] if business is None else [business, first_cleanup])
                self.assertEqual(len(calls(rows, 'leave_execute')), 1)
                if business is not None:
                    self.assertEqual(len(calls(rows, 'native_claim')), 0)
                    self.assertEqual(len(calls(rows, 'from_db')), 0)
                    self.assertEqual(len(calls(rows, 'commit')), 0)
                self.assert_restored(database, owned)
                self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                owner.assert_owned()
            finally:
                owned.end_attempt(token, primary)
            self.assertEqual(token['outcome'], 'error')
            # Reuse the same global policy and real Producer after cleanup.
            # A second real CTE is required: a clean flag alone is insufficient.
            next_token = owned.begin_attempt()
            self.admission(owned, policy, broker, owner, stop)
            next_event = self.claim(owned)
            self.assertEqual(next_token['native_claim_results'], [True])
            self.assertEqual(next_token['claim_path'], 'native_postgresql')
            self.assert_restored(database, owned)
            if next_event is None:
                owned.publish_result(False)
            owned.end_attempt(next_token)
            document = owned.close()
            self.assertFalse(document['complete'])
            self.assertFalse(document['attempts'][0]['complete'])
            self.assertNotIn('PRIVATE', json.dumps(document))

    def test_foreign_execute_binding_is_not_overwritten_and_depth_zero_is_not_restored_proof(self):
        self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            token, rows, replaced = owned.begin_attempt(), [], []
            foreign = Mock(side_effect=AssertionError('Foreign execute binding must never be called'))
            def inject(label, frame, kind, value):
                if label == 'commit' and kind == 'return' and not replaced:
                    self.assertEqual(token['guard_results'], [True])
                    self.assertEqual(token['native_claim_results'], [True])
                    self.assertEqual(len(database.execute_wrappers), 1)
                    replaced.append(database.execute_wrappers[0])
                    database.execute_wrappers[0] = foreign
            try:
                with trace_originals(self.targets(database, policy, owner), rows, inject):
                    self.admission(owned, policy, broker, owner, stop)
                    self.claim(owned)
                self.assertEqual(len(database.execute_wrappers), 1)
                self.assertIs(database.execute_wrappers[0], foreign)
                foreign.assert_not_called()
                self.assertNotIn('commit', vars(database))
                self.assertNotIn('from_db', vars(OutboxEvent))
                self.assertEqual(owned.temporary_depth, 0)
                self.assertGreater(owned.hook_failures, 0)
                owned.end_attempt(token)
                document = owned.close()
                self.assertFalse(token['temporary_hooks_restored'])
                self.assertFalse(document['attempts'][0]['complete'])
                self.assertFalse(document['complete'])
            finally:
                # Test owns this deliberately foreign object. Production must
                # leave it untouched; only the fixture removes its own object.
                if len(database.execute_wrappers) == 1 and database.execute_wrappers[0] is foreign:
                    database.execute_wrappers.pop()
                foreign.assert_not_called()
            self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
            self.assertIsNone(events.claim_event(), 'Restored application still runs a real empty native CTE')
            self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))

    def test_original_business_control_with_unknown_metaclass_never_calls_name_or_mro_getter(self):
        traps = []
        class ExceptionMeta(type):
            def __getattribute__(kind, name):
                if name in ('__name__', '__mro__', '__dict__'):
                    traps.append(name)
                    raise AssertionError('Unknown exception metadata getter must never run')
                return type.__getattribute__(kind, name)
        class MetaclassControl(BaseException, metaclass=ExceptionMeta):
            pass
        first = MetaclassControl('PRIVATE primary control')
        self.row()
        with self.native_owner() as state:
            database, broker, policy, stop, owner, owned = state
            token, primary, rows, injected = owned.begin_attempt(), None, [], []
            def inject(label, frame, kind, value):
                if label == 'from_db' and kind == 'call' and not injected:
                    self.assertEqual(token['guard_results'], [True])
                    self.assertEqual(token['native_claim_results'], [True])
                    injected.append(first)
                    raise first
            try:
                try:
                    with trace_originals(self.targets(database, policy, owner), rows, inject):
                        self.admission(owned, policy, broker, owner, stop)
                        self.claim(owned)
                except BaseException as caught:
                    primary = caught
                self.assertIs(primary, first)
                self.assertEqual(traps, [])
                self.assert_restored(database, owned)
                self.assertIs(policy.plain(broker, owner, command._budget_aliases(), stop), True)
                owner.assert_owned()
            finally:
                owned.end_attempt(token, primary)
            document = owned.close()
            self.assertEqual(document['attempts'][0]['exception_type'], 'MetaclassControl')
            self.assertFalse(document['complete'])
            self.assertEqual(traps, [])
            self.assertNotIn('PRIVATE', json.dumps(document))
