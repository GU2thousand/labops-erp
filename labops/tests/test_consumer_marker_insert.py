"""Marker SQL admission and actual PostgreSQL equivalence, without changing effects."""
import copy
import hashlib
import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pathlib import Path
from unittest import skipUnless
from unittest.mock import Mock, patch

from django.db import connection, connections, close_old_connections, IntegrityError, transaction, models
from django.db.models.base import ModelBase
from django.db.models.manager import Manager
from django.db.models.query import QuerySet
from django.db.models.signals import pre_init, post_init, pre_save, post_save
from django.db.backends.postgresql.base import DatabaseWrapper, Cursor, ServerBindingCursor
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops import events
from labops.models import Base, ProcessedEvent, Notification, InventoryProjection, FailedDelivery, OutboxEvent, StockMovement, DeliveryAudit
from labops.event_schema import canonical_payload_hash
from labops.tests.test_event_query_reductions import EventQueryFixture


def original_marker(consumer, event_id, digest):
    return ProcessedEvent.objects.get_or_create(consumer_name=consumer, event_id=event_id,
                                               defaults={'payload_hash': digest})


def proof(name, value):
    directory = os.environ.get('LABOPS_MARKER_INSERT_PROOF_EVIDENCE')
    if directory:
        root = Path(__file__).resolve().parents[2]
        value = {**value, 'source_sha256': {
            path: hashlib.sha256((root / path).read_bytes()).hexdigest()
            for path in ('labops/events.py', 'labops/tests/test_consumer_marker_insert.py',
                         'docs/events/acceptance.md')}}
        (Path(directory) / (name + '.json')).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


class MarkerAdmissionTests(SimpleTestCase):
    def setUp(self):
        options = dict(connection.settings_dict)
        options['ENGINE'] = 'django.db.backends.postgresql'
        database = DatabaseWrapper(options, alias='default')
        database.cursor = Mock(side_effect=AssertionError('Admission must not query'))
        database.ensure_connection = Mock(side_effect=AssertionError('Admission must not connect'))
        for change in (patch.object(events, 'connection', database),
                       patch.object(events, 'connections', {'default': database})):
            change.start()
            self.addCleanup(change.stop)
        self.original_descriptors = {
            (target, name): events.inspect.getattr_static(target, name)
            for target in (ProcessedEvent, Base, models.Model)
            for name in events._MARKER_MODEL_METHODS}
        self.original_callable_states = dict(events._MARKER_CALLABLE_STATES)
        self.original_code_hashes = {
            key: hashlib.sha256(state[1].co_code).hexdigest()
            for key, state in self.original_callable_states.items()}
        self.original_model_methods = dict(events._MARKER_MODEL_METHODS)
        self.original_defaults = events._MARKER_DEFAULTS
        self.original_metaclass_call = events.inspect.getattr_static(ModelBase, '__call__')

    def admitted(self):
        return events._plain_processed_marker(ProcessedEvent.objects)

    def assert_model_restored(self):
        """Check shared interpreter state and actual ORM construction after a control."""
        self.assertEqual(events._MARKER_MODEL_METHODS, self.original_model_methods)
        self.assertEqual(events._MARKER_CALLABLE_STATES, self.original_callable_states)
        self.assertIs(events._MARKER_DEFAULTS, self.original_defaults)
        self.assertIs(events.inspect.getattr_static(ModelBase, '__call__'), self.original_metaclass_call)
        for (target, name), descriptor in self.original_descriptors.items():
            self.assertIs(events.inspect.getattr_static(target, name), descriptor)
        for key, (descriptor, code, module, qualname) in self.original_callable_states.items():
            function = events._marker_callable(descriptor)
            self.assertIs(function.__code__, code)
            self.assertEqual(hashlib.sha256(function.__code__.co_code).hexdigest(), self.original_code_hashes[key])
            self.assertEqual((function.__module__, function.__qualname__), (module, qualname))
        self.assertTrue(self.admitted())
        event_id = uuid.uuid4()
        marker = ProcessedEvent(consumer_name='notification', event_id=event_id, payload_hash='a' * 64)
        self.assertIs(type(marker), ProcessedEvent)
        self.assertIs(type(marker.id), uuid.UUID)
        self.assertEqual((marker.consumer_name, marker.event_id, marker.payload_hash),
                         ('notification', event_id, 'a' * 64))
        self.assertTrue(marker._state.adding)
        names = [field.attname for field in ProcessedEvent._meta.concrete_fields]
        values = [getattr(marker, name) for name in names]
        loaded = ProcessedEvent.from_db('default', names, values)
        self.assertIs(type(loaded), ProcessedEvent)
        self.assertEqual([getattr(loaded, name) for name in names], values)
        self.assertFalse(loaded._state.adding)
        self.assertEqual(loaded._state.db, 'default')
        self.assertTrue(self.admitted())

    def test_plain_current_schema_is_admitted_without_database_io(self):
        self.assertTrue(self.admitted())

    def test_warm_ordinary_field_default_caches_remain_admitted(self):
        ProcessedEvent(consumer_name='notification', event_id=uuid.uuid4(), payload_hash='a' * 64)
        for field in ProcessedEvent._meta.local_concrete_fields:
            field.get_default()
        self.assertTrue(self.admitted())

    def test_backend_alias_and_router_extensions_reject_before_router_callback(self):
        for attribute, value in (('vendor', 'sqlite'), ('alias', 'other')):
            with self.subTest(attribute=attribute), patch.object(events.connection, attribute, value):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
        router = Mock()
        router.db_for_write.side_effect = AssertionError('Router must not be probed')
        with patch.object(events.router, 'routers', [router]):
            self.assertFalse(self.admitted())
            router.db_for_write.assert_not_called()
        self.assert_model_restored()
        with override_settings(DATABASE_ROUTERS=[router]):
            self.assertFalse(self.admitted())
            router.db_for_write.assert_not_called()
        self.assert_model_restored()
        with patch.dict(events.connection.settings_dict, {'ENGINE': 'custom.postgresql'}):
            self.assertFalse(self.admitted())
        self.assert_model_restored()
        for value in ({'isolation_level': 1}, {'options': '-c default_transaction_isolation=serializable'}):
            with patch.dict(events.connection.settings_dict['OPTIONS'], value):
                self.assertFalse(self.admitted())
            self.assert_model_restored()

    def test_model_base_constructor_and_save_extensions_are_rejected(self):
        builtin_slots = {'__new__', '__getattribute__', '__setattr__'}
        # Simulate noncanonical captured capabilities without mutating CPython's
        # process-global builtin slots; descriptor restoration can leave a slot.
        for name in builtin_slots:
            with self.subTest(builtin_capability=name), patch.dict(events._MARKER_MODEL_METHODS, {name: Mock()}):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
        for target in (ProcessedEvent, Base, models.Model):
            for name in events._MARKER_MODEL_METHODS:
                if name in builtin_slots:
                    continue
                with self.subTest(target=target.__name__, method=name), patch.object(target, name, Mock()):
                    self.assertFalse(self.admitted())
                self.assert_model_restored()
        actual_static = events.inspect.getattr_static
        custom_call = Mock()
        def effective_descriptor(target, name, *args):
            if target is ModelBase and name == '__call__':
                return custom_call
            return actual_static(target, name, *args)
        # Exercise the effective-descriptor check without changing the global
        # metaclass invocation slot in this shared interpreter.
        with patch.object(events.inspect, 'getattr_static', side_effect=effective_descriptor):
            self.assertFalse(self.admitted())
        self.assert_model_restored()

    def test_managers_querysets_hints_and_insert_extensions_are_rejected(self):
        for manager in (ProcessedEvent.objects, ProcessedEvent._base_manager):
            for name in events._MARKER_MANAGER_METHODS:
                with self.subTest(manager=manager.name, method=name), patch.object(manager, name, Mock()):
                    self.assertFalse(self.admitted())
                self.assert_model_restored()
            for name, value in (('_db', 'other'), ('_hints', {'instance': object()}), ('_queryset_class', type('CustomQuerySet', (QuerySet,), {}))):
                with self.subTest(manager=manager.name, state=name), patch.object(manager, name, value):
                    self.assertFalse(self.admitted())
                self.assert_model_restored()
        for name in events._MARKER_QUERY_METHODS:
            with self.subTest(query_method=name), patch.object(QuerySet, name, Mock()):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
        for kind, name, _, _ in events._MARKER_INSERT_METHODS:
            with self.subTest(insert_method=name), patch.object(kind, name, Mock()):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
        custom = type('CustomManager', (Manager,), {})()
        custom.model = ProcessedEvent
        self.assertFalse(events._plain_processed_marker(custom))
        self.assert_model_restored()

    def test_fields_descriptors_defaults_and_constraint_extensions_are_rejected(self):
        for field in ProcessedEvent._meta.local_concrete_fields:
            for name in events._MARKER_FIELD_METHODS[type(field)]:
                with self.subTest(field=field.name, method=name), patch.object(field, name, Mock()):
                    self.assertFalse(self.admitted())
                self.assert_model_restored()
            with self.subTest(field=field.name, descriptor=True), patch.object(ProcessedEvent, field.name, property(lambda self: None)):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
            with self.subTest(field=field.name, generated=True), patch.object(field, 'generated', True):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
            with self.subTest(field=field.name, placeholder=True), patch.object(field, 'get_placeholder', Mock(), create=True):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
        for name in ('id', 'created_at'):
            field = ProcessedEvent._meta.get_field(name)
            with patch.object(field, 'default', Mock()):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
            with patch.dict(field.__dict__, {'_get_default': Mock()}):
                self.assertFalse(self.admitted())
            self.assert_model_restored()
        field = ProcessedEvent._meta.get_field('payload_hash')
        with patch.object(field, 'max_length', 63):
            self.assertFalse(self.admitted())
        self.assert_model_restored()
        with patch.object(ProcessedEvent._meta, 'constraints', []):
            self.assertFalse(self.admitted())
        self.assert_model_restored()

    def test_all_global_and_sender_init_and_save_listeners_reject(self):
        for signal in (pre_init, post_init, pre_save, post_save):
            for sender in (None, ProcessedEvent):
                with self.subTest(signal=id(signal), global_sender=sender is None):
                    callback = Mock()
                    signal.connect(callback, sender=sender, weak=False)
                    try:
                        self.assertFalse(self.admitted())
                        callback.assert_not_called()
                    finally:
                        signal.disconnect(callback, sender=sender)
                    self.assert_model_restored()

    def test_preimport_style_wrapped_references_and_defaults_are_not_plain(self):
        mappings = (events._MARKER_MODEL_METHODS, events._MARKER_MANAGER_METHODS,
                    events._MARKER_QUERY_METHODS, *events._MARKER_FIELD_METHODS.values())
        for mapping in mappings:
            for name, method in mapping.items():
                # The imported reference itself is replaced, reproducing the
                # pre-import gap rather than relying on a later identity change.
                def wrapper(*args, **kwargs):
                    return None
                wrapper = wraps(events._marker_callable(method))(wrapper)
                state = (wrapper, wrapper.__code__, wrapper.__module__, wrapper.__qualname__)
                with (self.subTest(reference=name), patch.dict(mapping, {name: wrapper}),
                      patch.dict(events._MARKER_CALLABLE_STATES, {id(wrapper): state})):
                    self.assertFalse(events._marker_framework_original())
                self.assert_model_restored()
        for index, default in enumerate(events._MARKER_DEFAULTS):
            @wraps(default)
            def wrapper():
                return None
            changed = list(events._MARKER_DEFAULTS)
            changed[index] = wrapper
            state = (wrapper, wrapper.__code__, wrapper.__module__, wrapper.__qualname__)
            with (patch.object(events, '_MARKER_DEFAULTS', tuple(changed)),
                  patch.dict(events._MARKER_CALLABLE_STATES, {id(wrapper): state})):
                self.assertFalse(events._marker_framework_original())
            self.assert_model_restored()
        actual = models.Model.save
        original_code = actual.__code__
        def changed(*args, **kwargs):
            return None
        try:
            actual.__code__ = changed.__code__
            self.assertFalse(self.admitted())
        finally:
            actual.__code__ = original_code
        self.assertIs(actual.__code__, original_code)
        self.assert_model_restored()

    def test_failed_admission_and_unknown_value_types_use_original_once(self):
        expected = (object(), False)
        with (patch.object(events.inspect, 'getattr_static', side_effect=RuntimeError('inspection')),
                patch.object(ProcessedEvent.objects, 'get_or_create', return_value=expected) as original,
                patch.object(ProcessedEvent, '__init__', side_effect=AssertionError('No candidate construction'))):
            self.assertIs(events._processed_marker('notification', uuid.uuid4(), 'a' * 64), expected)
            original.assert_called_once()
        self.assert_model_restored()
        class Text(str):
            pass
        for values in ((Text('notification'), uuid.uuid4(), 'a' * 64),
                       ('notification', str(uuid.uuid4()), 'a' * 64),
                       ('notification', uuid.uuid4(), lambda: 'a' * 64),
                       ('notification', uuid.uuid4(), 'a' * 65),
                       ('notification', uuid.uuid4(), '\x00' * 64),
                       ('other', uuid.uuid4(), 'a' * 64)):
            with (self.subTest(value_types=[type(value).__name__ for value in values]),
                    patch.object(events, '_plain_processed_marker', return_value=True) as admission,
                    patch.object(ProcessedEvent.objects, 'get_or_create', return_value=expected) as original,
                    patch.object(ProcessedEvent, '__init__', side_effect=AssertionError('No candidate construction'))):
                self.assertIs(events._processed_marker(*values), expected)
                original.assert_called_once()
                admission.assert_not_called()
            self.assert_model_restored()

    def test_admission_does_not_swallow_baseexception(self):
        class Deadline(BaseException):
            pass
        failure = Deadline()
        with patch.object(events.inspect, 'getattr_static', side_effect=failure):
            with self.assertRaises(Deadline) as raised:
                self.admitted()
        self.assertIs(raised.exception, failure)
        self.assert_model_restored()


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL marker equivalence')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'], EVENT_TRANSPORT='kafka')
class PostgreSQLMarkerInsertTests(EventQueryFixture, TransactionTestCase):
    @contextmanager
    def mode(self, enabled):
        options = connection.settings_dict['OPTIONS']
        original = options.get('server_side_binding')
        connection.close()
        options['server_side_binding'] = enabled
        try:
            connection.ensure_connection()
            self.assertIsNone(connection.connection.prepare_threshold)
            with connection.cursor() as cursor:
                expected = ServerBindingCursor if enabled else Cursor
                self.assertIsInstance(cursor.cursor, expected)
                self.assertEqual(isinstance(cursor.cursor, Cursor), not enabled)
                cursor.execute('SELECT pg_backend_pid()')
                pid = cursor.fetchone()[0]
                kind = type(cursor.cursor).__qualname__
            self.assertTrue(events._plain_processed_marker(ProcessedEvent.objects))
            proof(f'binding-{self._testMethodName}-{int(enabled)}', {'server_side_binding': enabled,
                'actual_cursor_class': kind, 'client_cursor': not enabled,
                'prepare_threshold': None, 'backend_pid': pid})
            yield
        finally:
            connection.close()
            options['server_side_binding'] = original

    def helpers(self):
        return (('baseline', original_marker), ('candidate', events._processed_marker))

    def pid(self):
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            return cursor.fetchone()[0]

    def worker(self, function):
        close_old_connections()
        try:
            return function()
        finally:
            connections.close_all()

    def test_new_duplicate_metadata_types_and_normal_statement_counts_match(self):
        for enabled in (False, True):
            with self.mode(enabled):
                observations = {}
                for name, helper in self.helpers():
                    eid = uuid.uuid4()
                    with transaction.atomic(), CaptureQueriesContext(connection) as queries:
                        marker, created = helper('notification', eid, 'a' * 64)
                    self.assertTrue(created)
                    self.assertIs(type(marker.id), uuid.UUID)
                    self.assertIs(type(marker.event_id), uuid.UUID)
                    self.assertFalse(marker._state.adding)
                    self.assertEqual(marker._state.db, 'default')
                    prior = (marker.id, marker.created_at, marker.payload_hash)
                    duplicate, created = helper('notification', eid, 'a' * 64)
                    self.assertFalse(created)
                    self.assertEqual((duplicate.id, duplicate.created_at, duplicate.payload_hash), prior)
                    operations = [query['sql'].split()[0] for query in queries
                                  if 'labops_processedevent' in query['sql'] or query['sql'].startswith(('SAVEPOINT', 'RELEASE'))]
                    expected = ['SELECT', 'SAVEPOINT', 'INSERT', 'RELEASE'] if name == 'baseline' else ['SAVEPOINT', 'INSERT', 'RELEASE']
                    self.assertEqual(operations, expected)
                    observations[name] = {'operations': operations, 'created': True, 'duplicate_created': False,
                        'metadata_preserved': True, 'uuid_types': True, 'saved_state': True}
                proof(f'normal-binding-{int(enabled)}', {'server_side_binding': enabled, 'observations': observations})

    def test_nonstandard_native_isolation_uses_original_orm(self):
        for enabled in (False, True):
            with self.mode(enabled):
                native = connection.connection
                original = native.isolation_level
                try:
                    native.isolation_level = events.postgres_base.IsolationLevel.REPEATABLE_READ
                    self.assertFalse(events._plain_processed_marker(ProcessedEvent.objects))
                    with transaction.atomic(), CaptureQueriesContext(connection) as queries:
                        marker, created = events._processed_marker('notification', uuid.uuid4(), 'a' * 64)
                    self.assertTrue(created)
                    operations = [query['sql'].split()[0] for query in queries
                        if 'labops_processedevent' in query['sql'] or query['sql'].startswith(('SAVEPOINT', 'RELEASE'))]
                    self.assertEqual(operations, ['SELECT', 'SAVEPOINT', 'INSERT', 'RELEASE'])
                finally:
                    native.isolation_level = original
                self.assertTrue(events._plain_processed_marker(ProcessedEvent.objects))
                proof(f'isolation-fallback-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'native_repeatable_read_rejected': True, 'ordinary_orm_operations': operations,
                    'restored_managed_isolation': True})

    def test_legacy_null_and_empty_hashes_backfill_once_without_identity_change(self):
        for enabled in (False, True):
            with self.mode(enabled):
                for consumer in ('notification', 'analytics'):
                    for value in (None, ''):
                        _, event = self.event()
                        retained = ProcessedEvent.objects.create(consumer_name=consumer, event_id=event.pk, payload_hash=value)
                        metadata = (retained.id, retained.created_at)
                        self.assertFalse(events.process_envelope(consumer, events.envelope(event)))
                        retained.refresh_from_db()
                        self.assertEqual((retained.id, retained.created_at), metadata)
                        self.assertEqual(retained.payload_hash, event.payload_hash)
                        self.assertFalse(events.process_envelope(consumer, events.envelope(event)))
                        self.assertFalse(Notification.objects.filter(event=event).exists())
                proof(f'legacy-binding-{int(enabled)}', {'server_side_binding': enabled, 'null_empty': True,
                    'both_consumers': True, 'metadata_preserved': True, 'one_time_orm_backfill': True})

    def test_changed_content_and_missing_business_fail_before_marker(self):
        for enabled in (False, True):
            with self.mode(enabled):
                _, event = self.event()
                changed = copy.deepcopy(events.envelope(event))
                changed['payload']['body'] += ' changed'
                for consumer in ('notification', 'analytics'):
                    with patch.object(events, '_processed_marker', wraps=events._processed_marker) as marker:
                        with self.assertRaises(events.PayloadConflict):
                            events.process_envelope(consumer, changed)
                        marker.assert_not_called()
                # A schema-valid retained envelope whose original is absent.
                missing = copy.deepcopy(events.envelope(event))
                missing['event_id'] = str(uuid.uuid4())
                with patch.object(events, '_processed_marker', wraps=events._processed_marker) as marker:
                    self.assertFalse(events.deliver('notification', missing, f'missing:{enabled}'))
                    marker.assert_not_called()
                self.assertFalse(ProcessedEvent.objects.filter(event_id=event.pk).exists())
                proof(f'prerequisites-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'changed_content_before_marker': True, 'missing_original_before_marker': True})

    def test_wrong_stored_marker_and_original_checksums_are_quarantined(self):
        for enabled in (False, True):
            with self.mode(enabled):
                for consumer in ('notification', 'analytics'):
                    _, event = self.event()
                    marker = ProcessedEvent.objects.create(consumer_name=consumer, event_id=event.pk, payload_hash='0' * 64)
                    retained = (marker.id, marker.created_at, marker.payload_hash)
                    self.assertFalse(events.deliver(consumer, events.envelope(event), f'marker:{consumer}:{enabled}'))
                    marker.refresh_from_db()
                    self.assertEqual((marker.id, marker.created_at, marker.payload_hash), retained)
                    self.assertFalse(Notification.objects.filter(event=event).exists())
                    _, original = self.event(legacy=True)
                    OutboxEvent.objects.filter(pk=original.pk).update(payload_hash='0' * 64)
                    with patch.object(events, '_processed_marker', wraps=events._processed_marker) as attempted:
                        self.assertFalse(events.deliver(consumer, events.raw_envelope(original), f'original:{consumer}:{enabled}'))
                        attempted.assert_not_called()
                self.assertEqual(set(FailedDelivery.objects.values_list('last_error', flat=True)), {'payload_conflict'})
                proof(f'corrupt-content-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'wrong_marker_retained': True, 'wrong_original_before_marker': True, 'production_triggers_enabled': True})

    def test_late_global_and_sender_listeners_use_real_orm_lifecycle(self):
        for enabled in (False, True):
            with self.mode(enabled):
                events._processed_marker('notification', uuid.uuid4(), 'a' * 64)
                for sender in (None, ProcessedEvent):
                    observations = {}
                    for name, helper in self.helpers():
                        calls = []
                        callbacks = []
                        for label, signal in (('pre_init', pre_init), ('post_init', post_init), ('pre_save', pre_save), ('post_save', post_save)):
                            def callback(sender, label=label, **kwargs):
                                if sender is ProcessedEvent:
                                    calls.append(label)
                            callbacks.append((signal, callback))
                            signal.connect(callback, sender=sender, weak=False)
                        try:
                            self.assertFalse(events._plain_processed_marker(ProcessedEvent.objects))
                            eid = uuid.uuid4()
                            helper('notification', eid, 'a' * 64)
                            helper('notification', eid, 'a' * 64)
                            observations[name] = calls
                        finally:
                            for signal, callback in callbacks:
                                signal.disconnect(callback, sender=sender)
                    self.assertEqual(observations['baseline'], observations['candidate'])
                    self.assertEqual(observations['candidate'], ['pre_init', 'post_init', 'pre_save', 'post_save', 'pre_init', 'post_init'])
                proof(f'late-listeners-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'global_and_sender': True, 'real_callbacks_equal': True, 'late_registration_rejected': True})

    def test_custom_save_and_default_fallback_preserve_callback_counts(self):
        for enabled in (False, True):
            with self.mode(enabled):
                for name, helper in self.helpers():
                    calls = []
                    actual = ProcessedEvent.save
                    def save(instance, *args, **kwargs):
                        calls.append('save')
                        return actual(instance, *args, **kwargs)
                    with patch.object(ProcessedEvent, 'save', save):
                        eid = uuid.uuid4()
                        helper('notification', eid, 'a' * 64)
                        helper('notification', eid, 'a' * 64)
                    self.assertEqual(calls, ['save'])
                    field = ProcessedEvent._meta.get_field('id')
                    prior = dict(field.__dict__)
                    generated = []
                    def default():
                        generated.append(True)
                        return uuid.uuid4()
                    try:
                        field.default = default
                        field.__dict__.pop('_get_default', None)
                        eid = uuid.uuid4()
                        helper('notification', eid, 'a' * 64)
                        helper('notification', eid, 'a' * 64)
                        self.assertEqual(generated, [True])
                    finally:
                        field.__dict__.clear()
                        field.__dict__.update(prior)
                proof(f'extensions-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'custom_save_once': True, 'custom_default_once': True, 'duplicate_does_not_invoke_default': True})

    def collide(self, helper, *, appear=None):
        prior = ProcessedEvent.objects.create(consumer_name='other', event_id=uuid.uuid4(), payload_hash='a' * 64)
        eid = uuid.uuid4()
        errors = []
        error_state = []
        observed = []
        def external():
            return ProcessedEvent.objects.create(consumer_name='notification', event_id=eid, payload_hash='b' * 64)
        def execute(call, sql, params, many, context):
            if sql.startswith('INSERT INTO "labops_processedevent"'):
                changed = list(params)
                changed[0] = prior.pk  # Disposable fixture injects a real PK conflict at the driver boundary.
                if appear == 'before':
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        observed.append(pool.submit(self.worker, external).result(timeout=10))
                try:
                    return call(sql, changed, many, context)
                except IntegrityError as error:
                    errors.append(error)
                    error_state.append((error.__cause__, error.__context__))
                    if appear == 'after':
                        with ThreadPoolExecutor(max_workers=1) as pool:
                            observed.append(pool.submit(self.worker, external).result(timeout=10))
                    raise
            return call(sql, params, many, context)
        with transaction.atomic(), connection.execute_wrapper(execute):
            raised_error = None
            try:
                result = helper('notification', eid, 'b' * 64)
            except IntegrityError as error:
                raised_error = error
                self.assertIs(error, errors[0])
                self.assertEqual((error.__cause__, error.__context__), error_state[0])
                result = None
        prior.refresh_from_db()
        self.assertEqual((prior.consumer_name, prior.payload_hash), ('other', 'a' * 64))
        if appear:
            marker, created = result
            self.assertFalse(created)
            self.assertEqual((marker.pk, marker.created_at, marker.payload_hash),
                             (observed[0].pk, observed[0].created_at, observed[0].payload_hash))
        else:
            self.assertIsNotNone(raised_error)
            self.assertIs(raised_error, errors[0])
            self.assertIsNone(result)
            self.assertFalse(ProcessedEvent.objects.filter(consumer_name='notification', event_id=eid).exists())
        return {'created': False if result else None, 'target_appeared': bool(appear),
                'original_exception_preserved': not appear, 'retained_metadata': True,
                'observed_constraints': [error.__cause__.diag.constraint_name for error in errors]}

    def test_sole_primary_key_collision_preserves_original_integrityerror(self):
        for enabled in (False, True):
            with self.mode(enabled):
                observations = {name: self.collide(helper) for name, helper in self.helpers()}
                for value in observations.values():
                    self.assertEqual(value['observed_constraints'], ['labops_processedevent_pkey'])
                proof(f'pk-only-binding-{int(enabled)}', {'server_side_binding': enabled, 'observations': observations})

    def test_simultaneous_pair_and_primary_key_conflicts_match_baseline(self):
        for enabled in (False, True):
            with self.mode(enabled):
                observations = {name: self.collide(helper, appear='before') for name, helper in self.helpers()}
                self.assertFalse(observations['baseline']['created'])
                self.assertFalse(observations['candidate']['created'])
                proof(f'dual-conflict-binding-{int(enabled)}', {'server_side_binding': enabled, 'observations': observations})

    def test_target_appearing_after_unrelated_integrityerror_uses_fresh_get(self):
        for enabled in (False, True):
            with self.mode(enabled):
                observations = {name: self.collide(helper, appear='after') for name, helper in self.helpers()}
                for value in observations.values():
                    self.assertEqual(value['observed_constraints'], ['labops_processedevent_pkey'])
                    self.assertFalse(value['created'])
                proof(f'pk-recovery-binding-{int(enabled)}', {'server_side_binding': enabled, 'observations': observations})

    def test_target_deleted_after_conflict_is_permanent_without_effect(self):
        for enabled in (False, True):
            with self.mode(enabled):
                observations = {}
                for name, helper in self.helpers():
                    eid = uuid.uuid4()
                    inserted = False
                    deleted = False
                    original_errors = []
                    def external_create():
                        return ProcessedEvent.objects.create(consumer_name='notification', event_id=eid, payload_hash='a' * 64)
                    def external_delete():
                        return ProcessedEvent.objects.filter(consumer_name='notification', event_id=eid).delete()[0]
                    def execute(call, sql, params, many, context):
                        nonlocal inserted, deleted
                        if sql.startswith('INSERT INTO "labops_processedevent"'):
                            with ThreadPoolExecutor(max_workers=1) as pool:
                                pool.submit(self.worker, external_create).result(timeout=10)
                            inserted = True
                            try:
                                return call(sql, params, many, context)
                            except IntegrityError as error:
                                original_errors.append(error)
                                raise
                        if inserted and not deleted and sql.startswith('SELECT') and '"labops_processedevent"' in sql:
                            with ThreadPoolExecutor(max_workers=1) as pool:
                                self.assertEqual(pool.submit(self.worker, external_delete).result(timeout=10), 1)
                            deleted = True
                        return call(sql, params, many, context)
                    with transaction.atomic(), connection.execute_wrapper(execute):
                        with self.assertRaises(IntegrityError) as raised:
                            helper('notification', eid, 'a' * 64)
                    self.assertTrue(inserted and deleted)
                    self.assertEqual(events.classify_failure(raised.exception), 'permanent')
                    self.assertFalse(ProcessedEvent.objects.filter(consumer_name='notification', event_id=eid).exists())
                    if name == 'baseline':
                        self.assertIs(raised.exception, original_errors[0])
                    else:
                        self.assertEqual(original_errors, [])
                        self.assertIsInstance(raised.exception.__cause__, ProcessedEvent.DoesNotExist)
                    observations[name] = {'actual_insert_delete': True, 'permanent_failure': True,
                        'marker_absent': True, 'original_sql_error_exists': bool(original_errors)}
                proof(f'deleted-target-binding-{int(enabled)}', {'server_side_binding': enabled, 'observations': observations})

    def wait_for_lock(self, observer, consumer_pid, holder_pid):
        until = time.monotonic() + 5
        while time.monotonic() < until:
            with observer.cursor() as cursor:
                cursor.execute('SELECT wait_event_type, wait_event, pg_blocking_pids(pid) FROM pg_stat_activity WHERE pid = %s', [consumer_pid])
                row = cursor.fetchone()
            if row and row[0] == 'Lock' and holder_pid in row[2]:
                return {'wait_event_type': row[0], 'wait_event': row[1], 'blocking_pids': row[2]}
            time.sleep(.01)
        self.fail('Consumer did not exhibit a real arbiter lock wait')

    def contention(self, helper, commit):
        entered, release, ready = threading.Event(), threading.Event(), threading.Event()
        eid = uuid.uuid4()
        state = {}
        observer = connection.copy(alias='marker_observer')
        observer.ensure_connection()
        try:
            with observer.cursor() as cursor:
                cursor.execute('SELECT pg_backend_pid()')
                observer_pid = cursor.fetchone()[0]
            def holder():
                state['holder_pid'] = self.pid()
                with transaction.atomic():
                    marker = ProcessedEvent.objects.create(consumer_name='notification', event_id=eid, payload_hash='a' * 64)
                    state['holder_metadata'] = (marker.pk, marker.created_at)
                    entered.set()
                    if not release.wait(10):
                        raise RuntimeError('Holder release timed out')
                    if not commit:
                        transaction.set_rollback(True)
            # An observer belongs to the main thread; its actual visibility
            # check is performed there after the consumer signals its boundary.
            effect_ready, finish = threading.Event(), threading.Event()
            def bounded_consumer():
                state['consumer_pid'] = self.pid()
                ready.set()
                with transaction.atomic():
                    marker, created = helper('notification', eid, 'a' * 64)
                    state['result'] = (marker.pk, marker.created_at, created)
                    effect_ready.set()
                    if not finish.wait(10):
                        raise RuntimeError('Consumer commit release timed out')
                return created
            with ThreadPoolExecutor(max_workers=2) as pool:
                holding = pool.submit(self.worker, holder)
                consuming = None
                try:
                    self.assertTrue(entered.wait(10))
                    consuming = pool.submit(self.worker, bounded_consumer)
                    self.assertTrue(ready.wait(10))
                    self.assertEqual(len({state['holder_pid'], state['consumer_pid'], observer_pid}), 3)
                    wait = self.wait_for_lock(observer, state['consumer_pid'], state['holder_pid'])
                    self.assertFalse(consuming.done())
                    release.set()
                    holding.result(timeout=10)
                    self.assertTrue(effect_ready.wait(10))
                    with observer.cursor() as cursor:
                        cursor.execute('SELECT count(*) FROM labops_processedevent WHERE consumer_name=%s AND event_id=%s', ['notification', eid])
                        visible = cursor.fetchone()[0]
                    self.assertEqual(visible, 1 if commit else 0)
                    self.assertFalse(consuming.done())
                finally:
                    release.set()
                    finish.set()
                self.assertEqual(consuming.result(timeout=10), not commit)
            marker = ProcessedEvent.objects.get(consumer_name='notification', event_id=eid)
            self.assertEqual((marker.pk, marker.created_at), state['result'][:2])
            if commit:
                self.assertEqual((marker.pk, marker.created_at), state['holder_metadata'])
            return {'holder_pid': state['holder_pid'], 'consumer_pid': state['consumer_pid'],
                    'observer_pid': observer_pid, 'distinct_backends': True, 'arbiter_lock_observed': True,
                    **wait,
                    'holder_committed': commit, 'consumer_created': not commit,
                    'visible_before_outer_commit': visible, 'target_count_after_commit': 1}
        finally:
            release.set()
            observer.close()

    def test_concurrent_arbiter_commit_and_rollback_preserve_visibility(self):
        for enabled in (False, True):
            with self.mode(enabled):
                for name, helper in self.helpers():
                    for commit in (False, True):
                        value = self.contention(helper, commit)
                        proof(f'arbiter-{name}-binding-{int(enabled)}-commit-{int(commit)}',
                              {'server_side_binding': enabled, 'algorithm': name, **value})

    def test_genuine_deferred_notification_fk_failure_rolls_back_effect_and_parks(self):
        for enabled in (False, True):
            with self.mode(enabled):
                _, event = self.event()
                value = events.envelope(event)
                original_bulk = Notification.objects.bulk_create
                def missing_user(rows, *args, **kwargs):
                    # Retain every valid active recipient so completed==active
                    # passes; the extra row reaches the actual deferred FK hook.
                    extra = Notification(event_id=event.pk, user_id=uuid.uuid4(), title='Fixture', body='Deferred FK')
                    return original_bulk([*rows, extra], *args, **kwargs)
                errors = []
                real_check = connection.check_constraints
                def check(*args, **kwargs):
                    try:
                        return real_check(*args, **kwargs)
                    except IntegrityError as error:
                        errors.append(error)
                        raise
                with (patch.object(Notification.objects, 'bulk_create', side_effect=missing_user),
                        patch.object(connection, 'check_constraints', side_effect=check)):
                    with transaction.atomic():
                        self.assertFalse(events.deliver('notification', value, f'deferred-fk:{enabled}'))
                self.assertEqual(len(errors), 1)
                self.assertEqual(errors[0].__cause__.sqlstate, '23503')
                self.assertTrue(StockMovement.objects.filter(pk=event.aggregate_id, status='POSTED').exists())
                self.assertFalse(ProcessedEvent.objects.filter(event_id=event.pk).exists())
                self.assertFalse(Notification.objects.filter(event=event).exists())
                failed = FailedDelivery.objects.get(delivery_key=f'deferred-fk:{enabled}')
                self.assertEqual((failed.status, failed.last_error, failed.failure_class, failed.attempts),
                                 ('DEAD', 'IntegrityError', 'permanent', 1))
                self.assertEqual(failed.envelope, value)
                self.assertEqual(failed.original_hash, canonical_payload_hash(value))
                self.assertEqual(failed.consumer_name, 'notification')
                self.assertTrue(failed.source_cluster and failed.source_generation)
                self.assertTrue(DeliveryAudit.objects.filter(delivery=failed, action='PARK', outcome='parked',
                                                            original_hash=failed.original_hash).exists())
                self.assertTrue(events.process_envelope('notification', value))
                failed.refresh_from_db()
                self.assertEqual(failed.status, 'DEAD')
                proof(f'deferred-fk-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'sqlstate': '23503', 'reached_constraint_check': True, 'posted_movement_present': True,
                    'marker_effect_rolled_back': True, 'parking_committed': True, 'park_audit': True,
                    'retained_content_hash_coordinates': True, 'direct_reprocessing_succeeded': True,
                    'parked_dead_row_retained': True})

    def test_effect_failure_rolls_back_new_marker_before_successful_retry(self):
        for enabled in (False, True):
            with self.mode(enabled):
                for name, helper in self.helpers():
                    _, event = self.event()
                    value = events.envelope(event)
                    with patch.object(events, '_processed_marker', helper), patch.object(InventoryProjection, 'save', side_effect=RuntimeError('effect')):
                        with self.assertRaises(RuntimeError):
                            events.process_envelope('analytics', value)
                    self.assertFalse(ProcessedEvent.objects.filter(consumer_name='analytics', event_id=event.pk).exists())
                    with patch.object(events, '_processed_marker', helper):
                        self.assertTrue(events.process_envelope('analytics', value))
                        self.assertFalse(events.process_envelope('analytics', value))
                    self.assertEqual(ProcessedEvent.objects.filter(consumer_name='analytics', event_id=event.pk).count(), 1)
                proof(f'effect-rollback-binding-{int(enabled)}', {'server_side_binding': enabled,
                    'both_algorithms': True, 'marker_rollback': True, 'one_successful_retry': True})
