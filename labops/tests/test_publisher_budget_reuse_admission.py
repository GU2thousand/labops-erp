"""Real native identities and static refusal before timeout-reuse SQL.

Unlike controller mechanics fixtures, the positive cases use the command's
actual admission policy, the real confluent producer, and the default tracing
implementations. No broker records are sent by these tests.
"""
from contextlib import ExitStack, contextmanager
import json
from types import MethodType
from unittest import skipUnless
from unittest.mock import Mock, patch

from confluent_kafka.cimpl import Producer
from django.db import connection, connections
from django.db.backends.postgresql.base import Cursor, DatabaseWrapper, ServerBindingCursor
from django.db.models import signals as model_signals
from django.test import SimpleTestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from opentelemetry import context, propagate, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context.contextvars_context import ContextVarsRuntimeContext
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from labops import events, worker_metrics
from labops.management.commands import publish_events as command
from labops.models import OutboxEvent
from labops.publisher_shards import PublisherShardOwner, publisher_shard_owner
from labops.worker_metrics import StopController, operation_deadline


READ = "SELECT current_setting('statement_timeout'), current_setting('lock_timeout'), pg_backend_pid()"
SET = "SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)"


@contextmanager
def ordinary_connection_capability(database=None):
    """Undo only identified Django test artifacts for this proof.

    SimpleTestCase wraps the inherited capability even when the default
    database is allowed. The production policy must continue to refuse that
    wrapper, so the fixture exposes the policy's captured ordinary descriptor
    on the PostgreSQL class. Its teardown can also leave the original bound
    cursor on the persistent default instance; remove only that exact method
    for the fixture and restore the same object afterwards. Unknown instance
    callbacks remain in place, with no production admission relaxation.
    """
    policy = command._BUDGET_ADMISSION
    ensure = next(descriptor for kind, name, descriptor in policy.descriptors
        if kind is DatabaseWrapper and name == 'ensure_connection')
    cursor = next(descriptor for kind, name, descriptor in policy.descriptors
        if kind is DatabaseWrapper and name == 'cursor')
    database = connections['default'] if database is None else database
    original = vars(database).get('cursor')
    removed = (type(database) is DatabaseWrapper and database.alias == 'default'
        and type(original) is MethodType and original.__self__ is database
        and original.__func__ is cursor)
    if removed:
        del vars(database)['cursor']
    try:
        with patch.object(DatabaseWrapper, 'ensure_connection', ensure):
            yield
    finally:
        if removed:
            vars(database)['cursor'] = original


@contextmanager
def plain_tracing():
    """Only module state changes; captured tracing classes remain untouched."""
    with ExitStack() as stack:
        stack.enter_context(patch.object(trace, '_TRACER_PROVIDER', None))
        tracer = trace.get_tracer('labops.budget.admission.tests')
        runtime = ContextVarsRuntimeContext()
        propagator = CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
        stack.enter_context(patch.object(events, 'tracer', tracer))
        stack.enter_context(patch.object(context, '_RUNTIME_CONTEXT', runtime))
        stack.enter_context(patch.object(propagate, '_HTTP_TEXT_FORMAT', propagator))
        yield tracer, runtime


class PublisherBudgetStaticAdmissionTests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(ordinary_connection_capability())
        self.stop = self.stack.enter_context(StopController())
        self.tracer, self.runtime = self.stack.enter_context(plain_tracing())
        configuration = dict(connection.settings_dict)
        configuration.update(ENGINE='django.db.backends.postgresql', OPTIONS={}, AUTOCOMMIT=True)
        self.database = DatabaseWrapper(configuration, alias='default')
        self.stack.enter_context(patch('django.db.connections', {'default': self.database}))
        # This is the ordinary factory and the actual C producer constructor.
        # It may connect in the background, but these cases queue no records.
        self.broker = events.producer()
        self.owner = PublisherShardOwner(0, 1)
        self.policy = command._BUDGET_ADMISSION
        self.assertIsNotNone(self.policy)
        self.assertTrue(self.policy.valid)
        self.assertIs(type(self.broker.client), Producer)
        self.assertTrue(self.plain(), 'Every mutation needs a positive native baseline')
        self.assertIsNone(self.database.connection)

    def plain(self):
        return self.policy.plain(self.broker, self.owner, command._budget_aliases(), self.stop)

    def refused(self):
        self.assertFalse(self.plain())
        self.assertIsNone(self.database.connection, 'Static refusal must not open a database session')

    def test_default_native_policy_is_valid_and_admits_without_opening_database(self):
        self.assertTrue(self.policy.valid)
        self.assertTrue(self.plain())
        self.assertIsNone(self.database.connection)
        extension = Mock(side_effect=AssertionError('Unknown connection capability must not run'))
        with patch.object(DatabaseWrapper, 'ensure_connection', extension):
            self.refused()
        extension.assert_not_called()
        self.assertTrue(self.plain())

    def test_fixture_removes_only_captured_bound_cursor_and_restores_exact_state(self):
        descriptor = next(descriptor for kind, name, descriptor in self.policy.descriptors
            if kind is DatabaseWrapper and name == 'cursor')
        original = descriptor.__get__(self.database, DatabaseWrapper)
        self.assertNotIn('cursor', vars(self.database))
        with ordinary_connection_capability(self.database):
            self.assertNotIn('cursor', vars(self.database))
        self.assertNotIn('cursor', vars(self.database))
        vars(self.database)['cursor'] = original
        marker = RuntimeError('Fixture body must preserve exact restored method')
        try:
            with self.assertRaises(RuntimeError) as raised:
                with ordinary_connection_capability(self.database):
                    self.assertNotIn('cursor', vars(self.database))
                    raise marker
            self.assertIs(raised.exception, marker)
            self.assertIs(vars(self.database)['cursor'], original)
            extension = Mock(side_effect=AssertionError('Unknown cursor must not run'))
            vars(self.database)['cursor'] = extension
            with ordinary_connection_capability(self.database):
                self.assertIs(vars(self.database)['cursor'], extension)
            self.assertIs(vars(self.database)['cursor'], extension)
            extension.assert_not_called()
        finally:
            vars(self.database).pop('cursor', None)

    def test_changed_send_publish_and_command_alias_are_rejected_without_calling_extensions(self):
        for target, name in ((events, 'send'), (events, 'publish_one'), (command, 'publish_one')):
            with self.subTest(binding=name, module=target.__name__):
                extension = Mock(side_effect=AssertionError('Admission must not invoke business extensions'))
                with patch.object(target, name, extension):
                    self.refused()
                extension.assert_not_called()
                self.assertTrue(self.plain())
        getter = Mock(side_effect=AssertionError('Unknown callable metadata must not be read'))
        class UnknownAlias:
            __code__ = property(lambda self: getter('code'))
            __wrapped__ = property(lambda self: getter('wrapped'))
            def __call__(self, *args, **kwargs):
                raise AssertionError('Unknown command alias must not be called')
        with patch.object(command, 'producer', UnknownAlias()):
            self.refused()
        getter.assert_not_called()

    def test_send_support_module_bindings_are_rejected_without_unknown_getters(self):
        for name, attribute in (('json', 'dumps'), ('trace', 'SpanKind')):
            with self.subTest(module_binding=name):
                getter = Mock(side_effect=AssertionError('Unknown support module getter must not run'))
                unknown = type('UnknownSupport', (), {attribute: property(lambda self: getter())})()
                with patch.object(events, name, unknown):
                    self.refused()
                getter.assert_not_called()
                self.assertTrue(self.plain())

    def test_in_place_json_keyword_default_mutation_is_detected(self):
        defaults = json.dumps.__kwdefaults__
        original = defaults['allow_nan']
        try:
            defaults['allow_nan'] = not original
            self.assertIs(json.dumps.__kwdefaults__, defaults)
            self.refused()
        finally:
            defaults['allow_nan'] = original
        self.assertTrue(self.plain())

    def test_json_encoder_decoder_extensions_and_properties_are_rejected_without_execution(self):
        field = OutboxEvent._meta.get_field('payload_json')
        for name in ('encoder', 'decoder'):
            with self.subTest(option=name, extension='callable'):
                extension = Mock(side_effect=AssertionError('JSON extension must not run during admission'))
                with patch.object(field, name, extension):
                    self.refused()
                extension.assert_not_called()
                self.assertTrue(self.plain())
            with self.subTest(option=name, extension='property'):
                getter = Mock(side_effect=AssertionError('JSON metadata property must not be evaluated'))
                with patch.object(type(field), name, property(lambda self: getter()), create=True):
                    self.refused()
                getter.assert_not_called()
                self.assertTrue(self.plain())

    def test_native_field_converter_and_ops_instance_converter_are_rejected_without_side_effects(self):
        field = OutboxEvent._meta.get_field('payload_json')
        for name in ('from_db_value', 'get_db_converters'):
            with self.subTest(field_converter=name):
                getter = Mock(side_effect=AssertionError('Field converter getter must not run'))
                with patch.object(type(field), name, property(lambda self: getter())):
                    self.refused()
                getter.assert_not_called()
                self.assertTrue(self.plain())
        converter = Mock(side_effect=AssertionError('Instance backend converter must not run'))
        with patch.object(self.database.ops, 'get_db_converters', converter):
            self.refused()
        converter.assert_not_called()

    def test_native_client_class_flush_property_is_rejected_without_getter(self):
        getter = Mock(side_effect=AssertionError('Native Client extension must not be read'))
        with patch.object(type(self.broker), 'flush', property(lambda self: getter()), create=True):
            self.refused()
        getter.assert_not_called()

    def test_noop_tracer_and_runtime_instance_overrides_are_rejected_without_invocation(self):
        noop = vars(self.tracer)['_noop_tracer']
        for target, name in ((noop, 'start_span'), (self.runtime, 'get_current')):
            with self.subTest(instance=type(target).__name__, method=name):
                extension = Mock(side_effect=AssertionError('Tracing override must not be called'))
                with patch.object(target, name, extension):
                    self.refused()
                extension.assert_not_called()
                self.assertTrue(self.plain())
        getter = Mock(side_effect=AssertionError('Unknown provider getter must not be read'))
        provider = type('UnknownProvider', (), {'get_tracer': property(lambda self: getter())})()
        with patch.object(trace, '_TRACER_PROVIDER', provider):
            self.refused()
        getter.assert_not_called()

    def test_unknown_runtime_current_context_getter_is_rejected_before_get(self):
        getter = Mock(side_effect=AssertionError('Unknown runtime storage must not be read'))
        storage = type('UnknownStorage', (), {'get': property(lambda self: getter())})()
        with patch.object(self.runtime, '_current_context', storage):
            self.refused()
        getter.assert_not_called()

    def test_replaced_signal_alias_is_rejected_without_receiver_getter(self):
        getter = Mock(side_effect=AssertionError('Unknown signal receiver metadata must not be read'))
        signal = type('UnknownSignal', (), {'receivers': property(lambda self: getter())})()
        with patch.object(model_signals, 'pre_init', signal):
            self.refused()
        getter.assert_not_called()


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL native admission and session gate')
class PublisherBudgetNativeAdmissionPostgreSQLTests(TransactionTestCase):
    @contextmanager
    def database(self, server_binding):
        # Use the real default handler so ORM/native claim, public helper,
        # controller and the dedicated owner share their ordinary routing.
        database = connections['default']
        options = database.settings_dict['OPTIONS']
        database.close()
        database.settings_dict['OPTIONS'] = {**options, 'server_side_binding': server_binding, 'prepare_threshold': None}
        try:
            database.ensure_connection()
            with database.cursor() as cursor:
                self.assertIs(type(cursor.cursor), ServerBindingCursor if server_binding else Cursor)
                cursor.execute(SET, ['37000', '91'])
            yield database
        finally:
            database.close()
            database.settings_dict['OPTIONS'] = options

    def values(self, database):
        with database.connection.cursor() as cursor:
            cursor.execute(READ)
            return cursor.fetchone()

    def test_real_policy_admits_both_binding_modes_and_empty_native_publish_restores_once(self):
        self.assertFalse(OutboxEvent.objects.filter(transport='kafka').exists())
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), ordinary_connection_capability(), self.database(enabled) as database, plain_tracing():
                broker = events.producer()
                self.assertIs(type(broker.client), Producer)
                self.assertIsNotNone(command._BUDGET_ADMISSION)
                self.assertTrue(command._BUDGET_ADMISSION.valid)
                before, raw = self.values(database), database.connection
                with StopController() as stop, publisher_shard_owner(0, 1) as owner:
                    self.assertTrue(command._BUDGET_ADMISSION.plain(broker, owner, command._budget_aliases(), stop))
                    controller = worker_metrics.PublisherBatchBudget(1.25, 3,
                        admission=lambda: command._BUDGET_ADMISSION.plain(broker, owner, command._budget_aliases(), stop))
                    controller.before_deadline()
                    with operation_deadline(30):
                        owner.assert_owned()
                        with CaptureQueriesContext(database) as queries:
                            with controller.record(command.database_statement_budget) as token:
                                result = command.publish_one(broker, ownership_check=owner.assert_owned)
                                self.assertIs(result, False)
                                token.complete(result)
                                token.retain(result)
                    self.assertEqual(sum(row['sql'].lstrip().startswith('WITH previous AS MATERIALIZED') for row in queries), 1)
                    self.assertEqual(sum(row['sql'].lstrip().startswith('WITH claimed AS (') for row in queries), 1)
                    self.assertEqual(sum(row['sql'].lstrip().startswith("SELECT set_config('statement_timeout'") for row in queries), 1)
                    self.assertEqual(controller.counts['admitted_records'], 1)
                    self.assertEqual(controller.counts['fallback_records'], 0)
                    self.assertEqual(controller.counts['setup_completed'], 1)
                    self.assertEqual(controller.counts['restore_completed'], 1)
                    self.assertEqual(controller.counts['reused_records'], 0)
                    self.assertEqual(controller.returned_true, 0)
                    self.assertEqual(controller.returned_false, 1)
                    self.assertEqual(controller.counts['discard_attempts'], 0)
                    self.assertIs(database.connection, raw)
                    self.assertEqual(self.values(database), before)
                    owner.assert_owned()

    def test_unknown_native_row_factory_metadata_is_not_read_by_session_gate(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), ordinary_connection_capability(), self.database(enabled) as database, plain_tracing():
                raw, getter, called = database.connection, Mock(), []
                self.assertIsNotNone(worker_metrics.PublisherBatchBudget._session(database))
                original = raw.row_factory
                class UnknownRowFactory:
                    __module__ = property(lambda self: getter())
                    def __call__(self, cursor):
                        called.append(cursor)
                        raise AssertionError('Session admission must not invoke a row factory')
                try:
                    raw.row_factory = UnknownRowFactory()
                    with CaptureQueriesContext(database) as queries:
                        self.assertIsNone(worker_metrics.PublisherBatchBudget._session(database))
                    self.assertEqual(len(queries), 0)
                    getter.assert_not_called()
                    self.assertEqual(called, [])
                finally:
                    raw.row_factory = original
                extension = Mock(side_effect=AssertionError('Unknown cursor must not run'))
                with patch.object(database, 'cursor', extension), ordinary_connection_capability(database):
                    self.assertIs(vars(database)['cursor'], extension)
                    self.assertIsNone(worker_metrics.PublisherBatchBudget._session(database))
                extension.assert_not_called()
