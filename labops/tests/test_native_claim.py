"""One PostgreSQL claim statement, typed reads, and ordinary ORM extensions."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import wraps
import os
from pathlib import Path
import subprocess
import sys
from unittest import skipUnless
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

from django.db import close_old_connections, connection, connections, models, OperationalError, transaction
from django.db.backends.postgresql.base import DatabaseWrapper
from django.db.models.manager import Manager
from django.db.models.query import QuerySet
from django.db.models.signals import pre_init, post_init, pre_save, post_save
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext, isolate_apps
from django.utils import timezone

from labops import events
from labops.models import Base, OutboxEvent
from labops.tests.test_publisher_claim_reads import ClaimFixture


@override_settings(EVENT_LEASE_SECONDS=60)
class NativeClaimAdmissionTests(SimpleTestCase):
    def setUp(self):
        options = {**connection.settings_dict, 'ENGINE': 'django.db.backends.postgresql',
                   'OPTIONS': {'server_side_binding': True, 'prepare_threshold': None}}
        database = DatabaseWrapper(options, alias='default')
        database.cursor = Mock(side_effect=AssertionError('Admission must not query'))
        database.ensure_connection = Mock(side_effect=AssertionError('Admission must not connect'))
        for change in (patch.object(events, 'connection', database),
                       patch.object(events, 'connections', {'default': database})):
            change.start()
            self.addCleanup(change.stop)

    def admitted(self):
        return events._plain_outbox_claim(OutboxEvent.objects)

    def test_plain_schema_admits_without_database_io(self):
        self.assertTrue(self.admitted())
        events.connection.cursor.assert_not_called()
        events.connection.ensure_connection.assert_not_called()

    def test_unknown_backend_model_router_and_random_source_keep_orm(self):
        for attribute, value in (('vendor', 'sqlite'), ('alias', 'other')):
            with self.subTest(attribute=attribute), patch.object(events.connection, attribute, value):
                self.assertFalse(self.admitted())
        with patch.object(events, 'OutboxEvent', Mock()):
            self.assertFalse(self.admitted())
        custom = Mock()
        custom.db_for_write.side_effect = AssertionError('Do not probe a router')
        with patch.object(events.router, 'routers', [custom]):
            self.assertFalse(self.admitted())
            custom.db_for_write.assert_not_called()
        with patch.object(events.uuid, 'uuid4', Mock()) as random:
            self.assertFalse(self.admitted())
            random.assert_not_called()
        with override_settings(EVENT_LEASE_SECONDS=float('nan')):
            self.assertFalse(self.admitted())
        self.assertTrue(self.admitted())

    def test_custom_managers_querysets_and_hints_keep_orm(self):
        class CustomManager(Manager):
            pass
        custom = CustomManager()
        custom.model = OutboxEvent
        self.assertFalse(events._plain_outbox_claim(custom))
        for manager in (OutboxEvent.objects, OutboxEvent._base_manager):
            for name, value in (('_db', 'other'), ('_hints', {'custom': True}),
                    ('_queryset_class', type('CustomQuerySet', (QuerySet,), {})),
                    ('get_queryset', Mock()), ('raw', Mock()), ('filter', Mock())):
                with self.subTest(manager=manager.name, name=name), patch.object(manager, name, value):
                    self.assertFalse(self.admitted())
        with patch.object(QuerySet, '_update', Mock()):
            self.assertFalse(self.admitted())
        self.assertTrue(self.admitted())

    def test_model_lifecycle_extensions_keep_orm(self):
        for target in (OutboxEvent, Base, models.Model):
            for name in events._OUTBOX_CLAIM_MODEL_METHODS:
                with self.subTest(target=target.__name__, name=name), patch.object(target, name, Mock()):
                    self.assertFalse(self.admitted())
        self.assertTrue(self.admitted())

    def test_unknown_fields_and_preparation_extensions_keep_orm(self):
        for name, method in (('status', 'pre_save'), ('lease_token', 'get_db_prep_save'),
                             ('transport', 'get_prep_value'), ('next_attempt_at', 'get_db_prep_value')):
            with self.subTest(name=name, method=method), patch.object(OutboxEvent._meta.get_field(name), method, Mock()):
                self.assertFalse(self.admitted())
        with patch.object(models.CharField, 'get_prep_value', Mock()):
            self.assertFalse(self.admitted())
        field = OutboxEvent._meta.get_field('payload_json')
        original_type = type(field)
        field.__class__ = type('CustomJSONField', (models.JSONField,), {})
        try:
            self.assertFalse(self.admitted())
        finally:
            field.__class__ = original_type
        with patch.object(OutboxEvent, 'lease_token', property(lambda self: None)):
            self.assertFalse(self.admitted())
        with patch.object(OutboxEvent._meta.get_field('locked_until'), 'auto_now', True):
            self.assertFalse(self.admitted())
        with patch.object(OutboxEvent._meta, 'pk', OutboxEvent._meta.get_field('aggregate_id')):
            self.assertFalse(self.admitted())
        with patch.object(OutboxEvent._meta.get_field('id'), 'primary_key', False):
            self.assertFalse(self.admitted())
        with patch.object(OutboxEvent, '_claim_previous_status', property(lambda self: 'PRIVATE'), create=True):
            self.assertFalse(self.admitted())
        self.assertTrue(self.admitted())

    def test_global_and_model_signal_listeners_keep_orm(self):
        for signal in (pre_init, post_init, pre_save, post_save):
            for sender in (None, OutboxEvent):
                callback = Mock()
                signal.connect(callback, sender=sender, weak=False)
                try:
                    self.assertFalse(self.admitted())
                    callback.assert_not_called()
                finally:
                    signal.disconnect(callback, sender=sender)
        self.assertTrue(self.admitted())

    def test_preimport_style_wrapper_cannot_become_plain_capability(self):
        original = models.Model.save
        @wraps(original)
        def wrapped(*args, **kwargs):
            return original(*args, **kwargs)
        capabilities = tuple((kind, name, wrapped if kind is models.Model and name == 'save' else descriptor)
                             for kind, name, descriptor in events._OUTBOX_CLAIM_CAPABILITIES)
        source = events._OUTBOX_CLAIM_CALLABLES[id(original)][1]
        with patch.object(models.Model, 'save', wrapped), \
                patch.object(events, '_OUTBOX_CLAIM_CAPABILITIES', capabilities), \
                patch.dict(events._OUTBOX_CLAIM_CALLABLES, {id(wrapped): (wrapped.__code__, source)}):
            self.assertFalse(self.admitted())
        self.assertTrue(self.admitted())

    def test_preimport_callable_object_imports_safely_and_keeps_orm_without_database_io(self):
        source = '''
import sys
import django
django.setup()
from django.db import connection, models
assert 'labops.events' not in sys.modules
class CallableSave:
    calls = 0
    def __call__(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError('Admission must not invoke custom save')
extension = CallableSave()
models.Model.save = extension
from labops import events
from django.db.backends.postgresql.base import DatabaseWrapper
from unittest.mock import Mock
options = {**connection.settings_dict, 'ENGINE': 'django.db.backends.postgresql',
           'OPTIONS': {'server_side_binding': True, 'prepare_threshold': None}}
database = DatabaseWrapper(options, alias='default')
database.cursor = Mock(side_effect=AssertionError('Do not query'))
database.ensure_connection = Mock(side_effect=AssertionError('Do not connect'))
events.connection = database
events.connections = {'default': database}
assert events._OUTBOX_CLAIM_CALLABLES[id(extension)] is None
assert events._plain_outbox_claim(events.OutboxEvent.objects) is False
assert extension.calls == 0
database.cursor.assert_not_called()
database.ensure_connection.assert_not_called()
print('Unsupported save callable safely retained ORM fallback')
'''
        result = subprocess.run([sys.executable, '-c', source],
            cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=15,
            env={**os.environ, 'DJANGO_SETTINGS_MODULE': 'config.settings', 'LABOPS_DB_MODE': 'sqlite-demo',
                 'LABOPS_DB': ':memory:', 'LABOPS_SECRET_KEY': 'native-claim-import-only', 'LABOPS_DEBUG': '1'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Unsupported save callable safely retained ORM fallback', result.stdout)

    def test_unknown_callable_snapshot_never_reads_descriptor_getters(self):
        class UnknownCallable:
            @property
            def __code__(self):
                raise AssertionError('Do not read unknown code getter')
            @property
            def __module__(self):
                raise AssertionError('Do not read unknown module getter')
        self.assertIsNone(events._outbox_claim_callable_state(models.Model, UnknownCallable()))


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL native claims')
@override_settings(EVENT_LEASE_SECONDS=60)
class NativeClaimPostgreSQLTests(ClaimFixture, TransactionTestCase):
    def setUp(self):
        self.assertTrue(events._plain_outbox_claim(OutboxEvent.objects))

    def native_claim(self, **kwargs):
        with CaptureQueriesContext(connection) as queries:
            claimed = events.claim_event(**kwargs)
        statements = [row['sql'] for row in queries if 'labops_outboxevent' in row['sql']]
        self.assertEqual(len(statements), 1, statements)
        self.assertTrue(statements[0].lstrip().startswith('WITH claimed AS'), statements)
        return claimed, statements[0]

    def test_one_statement_returns_every_typed_persisted_field_and_no_private_annotation(self):
        record = self.row(attempts=2, last_error='Prior failure')
        before = self.fields(record)
        now = timezone.now()
        with patch.object(events.timezone, 'now', return_value=now):
            claimed, sql = self.native_claim()
        expected = {**before, 'status': 'PROCESSING', 'lease_token': claimed.lease_token,
                    'locked_until': now + timedelta(seconds=60)}
        self.assertEqual(self.fields(claimed), expected)
        self.assertEqual(self.fields(OutboxEvent.objects.get(pk=record.pk)), expected)
        self.assertIs(type(claimed.payload_json), dict)
        self.assertIs(type(claimed.id), UUID)
        self.assertIs(type(claimed.aggregate_id), UUID)
        self.assertIs(type(claimed.lease_token), UUID)
        self.assertTrue(timezone.is_aware(claimed.created_at))
        self.assertTrue(timezone.is_aware(claimed.locked_until))
        self.assertIsNone(claimed.published_at)
        self.assertIsNone(claimed.processed_at)
        self.assertFalse(claimed._state.adding)
        self.assertEqual(claimed._state.db, 'default')
        self.assertNotIn('_claim_previous_status', claimed.__dict__)
        self.assertFalse(hasattr(claimed, 'blocked'))
        self.assertEqual(events.raw_envelope(claimed), events.raw_envelope(record))
        self.assertEqual(sql.count('FOR UPDATE SKIP LOCKED'), 1)

    def test_empty_and_only_due_kafka_candidates_preserve_created_at_id_order(self):
        self.assertIsNone(self.native_claim()[0])
        now = timezone.now()
        excluded = [self.row(transport='local'), self.row(status='DEAD'), self.row(status='PUBLISHED'),
            self.row(next_attempt_at=now + timedelta(seconds=1)),
            self.row(status='PROCESSING', locked_until=now + timedelta(seconds=1)),
            self.row(status='PROCESSING', locked_until=None)]
        before = {row.pk: self.fields(row) for row in excluded}
        second = self.row(id=UUID(int=2), created_at=now, next_attempt_at=now)
        first = self.row(id=UUID(int=1), created_at=now, next_attempt_at=now)
        oldest = self.row(created_at=now - timedelta(seconds=1), next_attempt_at=now)
        with patch.object(events.timezone, 'now', return_value=now):
            self.assertEqual([self.native_claim()[0].pk for _ in range(3)], [oldest.pk, first.pk, second.pk])
            self.assertIsNone(self.native_claim()[0])
        for record in excluded:
            self.assertEqual(self.fields(OutboxEvent.objects.get(pk=record.pk)), before[record.pk])

    def test_strict_expiry_and_only_successfully_returned_old_processing_counts_expired(self):
        now = timezone.now()
        previous = uuid4()
        expired = self.row(status='PROCESSING', lease_token=previous, locked_until=now,
                           attempts=3, last_error='Prior failure')
        pending = self.row(next_attempt_at=now)
        with patch.object(events, 'EVENTS') as metric:
            with patch.object(events.timezone, 'now', return_value=now):
                self.assertEqual(self.native_claim()[0].pk, pending.pk)
                self.assertIsNone(self.native_claim()[0])
            metric.labels.assert_not_called()
            with patch.object(events.timezone, 'now', return_value=now + timedelta(microseconds=1)):
                claimed, _ = self.native_claim()
            metric.labels.assert_called_once_with('publisher', 'lease_expired')
            metric.labels.return_value.inc.assert_called_once_with()
        self.assertEqual(claimed.pk, expired.pk)
        self.assertNotEqual(claimed.lease_token, previous)
        self.assertEqual((claimed.attempts, claimed.last_error), (3, 'Prior failure'))

    def test_all_unpublished_predecessors_block_with_transport_and_aggregate_scope(self):
        for status in ('PENDING', 'PROCESSING', 'DEAD'):
            with self.subTest(status=status):
                earlier = self.row(status=status, next_attempt_at=timezone.now() + timedelta(days=1),
                                   locked_until=timezone.now() + timedelta(days=1))
                later = self.row(aggregate_id=earlier.aggregate_id, aggregate_version=2)
                self.assertIsNone(self.native_claim()[0])
                OutboxEvent.objects.filter(pk=earlier.pk).update(status='PUBLISHED')
                self.assertEqual(self.native_claim()[0].pk, later.pk)
        for scope in ({'transport': 'local'}, {'aggregate_type': 'other-type'}):
            earlier = self.row(status='DEAD', **scope)
            later = self.row(aggregate_id=earlier.aggregate_id, aggregate_version=2)
            self.assertEqual(self.native_claim()[0].pk, later.pk)

    def test_skip_locked_contender_claims_next_row_and_shard_filter_precedes_lock(self):
        zero = self.row(aggregate_id=UUID('00000000-0000-0000-0000-000000000001'))
        one = self.row(aggregate_id=UUID('00000001-0000-0000-0000-000000000001'))
        next_one = self.row(aggregate_id=UUID('00000001-0000-0000-0000-000000000002'))
        def contend():
            close_old_connections()
            try:
                return self.native_claim(shard_index=1, shard_count=2)[0]
            finally:
                connections.close_all()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                locked, _ = self.native_claim(shard_index=1, shard_count=2)
                self.assertEqual(locked.pk, one.pk)
                self.assertEqual(locked.publisher_shard, 1)
                competing = pool.submit(contend).result(timeout=3)
                self.assertEqual(competing.pk, next_one.pk)
                self.assertEqual(competing.publisher_shard, 1)
                # The other shard's row was never locked by either claim.
                def lock_other_shard():
                    close_old_connections()
                    try:
                        with transaction.atomic():
                            return OutboxEvent.objects.select_for_update(nowait=True).get(pk=zero.pk).status
                    finally:
                        connections.close_all()
                self.assertEqual(pool.submit(lock_other_shard).result(timeout=3), 'PENDING')
        self.assertEqual(self.native_claim(shard_index=0, shard_count=2)[0].pk, zero.pk)

    def test_committed_lease_is_visible_on_another_connection_before_exact_envelope_send(self):
        record = self.row(legacy=True)
        expected = events.raw_envelope(record)
        database = connection.copy(alias='native_visibility')
        self.addCleanup(database.close)
        def check_send(producer, topic, key, value):
            with database.cursor() as cursor:
                cursor.execute('SELECT status, lease_token, locked_until FROM labops_outboxevent WHERE id = %s', [record.pk])
                status, token, expires = cursor.fetchone()
            self.assertEqual(status, 'PROCESSING')
            self.assertIs(type(token), UUID)
            self.assertTrue(timezone.is_aware(expires))
            self.assertEqual(value, expected)
            self.assertEqual(key, f'{record.aggregate_type}:{record.aggregate_id}')
        with patch.object(events, 'send', side_effect=check_send) as send:
            self.assertTrue(events.publish_one(None))
        send.assert_called_once()
        record.refresh_from_db()
        self.assertEqual(record.status, 'PUBLISHED')
        self.assertTrue(record.payload_hash)

    def test_claim_commit_failure_never_returns_or_sends_and_expired_metric_is_not_rolled_back(self):
        record = self.row(status='PROCESSING', lease_token=uuid4(),
                          locked_until=timezone.now() - timedelta(seconds=1))
        previous = self.fields(record)
        database = connections['default']
        with patch.object(database, 'commit', side_effect=OperationalError('Commit failed')), \
                patch.object(events, 'send') as send, patch.object(events, 'EVENTS') as metric:
            with self.assertRaises(OperationalError):
                events.publish_one(None)
            send.assert_not_called()
            metric.labels.assert_called_once_with('publisher', 'lease_expired')
            metric.labels.return_value.inc.assert_called_once_with()
        self.assertEqual(self.fields(OutboxEvent.objects.get(pk=record.pk)), previous)

    def test_native_execution_failure_propagates_without_orm_retry_or_expired_count(self):
        record = self.row(status='PROCESSING', lease_token=uuid4(),
                          locked_until=timezone.now() - timedelta(seconds=1))
        statements = []
        def fail(execute, sql, params, many, context):
            if 'labops_outboxevent' in sql:
                statements.append(sql)
                raise OperationalError('Execution failed')
            return execute(sql, params, many, context)
        with connection.execute_wrapper(fail), patch.object(events, 'send') as send, \
                patch.object(events, 'EVENTS') as metric:
            with self.assertRaises(OperationalError):
                events.publish_one(None)
            send.assert_not_called()
            metric.labels.assert_not_called()
        self.assertEqual(len(statements), 1)
        self.assertTrue(statements[0].startswith('WITH claimed AS'))
        self.assertEqual(OutboxEvent.objects.get(pk=record.pk).lease_token, record.lease_token)

    def test_returned_row_conversion_failure_rolls_back_claim_without_retry_or_send(self):
        record = self.row(status='PROCESSING', lease_token=uuid4(),
                          locked_until=timezone.now() - timedelta(seconds=1))
        previous = self.fields(record)
        failure = RuntimeError('Returned JSON conversion failed')
        executed = []
        def observe(execute, sql, params, many, context):
            result = execute(sql, params, many, context)
            if sql.lstrip().startswith('WITH claimed AS'):
                executed.append((sql, context['cursor'].rowcount))
            return result
        # Read converters belong to Django for both claim paths. Fail one only
        # after a real UPDATE RETURNING has executed, without disabling native
        # admission or replacing the helper with a test-only implementation.
        with patch.object(models.JSONField, 'from_db_value', side_effect=failure) as convert, \
                connection.execute_wrapper(observe), CaptureQueriesContext(connection) as queries, \
                patch.object(events, 'send') as send:
            self.assertTrue(events._plain_outbox_claim(OutboxEvent.objects))
            with self.assertRaises(RuntimeError) as raised:
                events.publish_one(None)
            self.assertIs(raised.exception, failure)
            convert.assert_called_once()
            send.assert_not_called()
        statements = [row['sql'] for row in queries if 'labops_outboxevent' in row['sql']]
        self.assertEqual(len(statements), 1, statements)
        self.assertTrue(statements[0].lstrip().startswith('WITH claimed AS'))
        self.assertEqual(len(executed), 1)
        self.assertEqual(executed[0][1], 1, 'The one-row lease UPDATE executed before conversion failed')
        self.assertTrue(any(row['sql'] == 'ROLLBACK' for row in queries))
        self.assertEqual(self.fields(OutboxEvent.objects.get(pk=record.pk)), previous)

    def test_signal_fallback_preserves_read_before_save_lifecycle(self):
        record = self.row()
        observations, receivers = [], []
        for name, signal in (('pre_init', pre_init), ('post_init', post_init),
                             ('pre_save', pre_save), ('post_save', post_save)):
            def receiver(sender, signal_name=name, **kwargs):
                instance = kwargs.get('instance')
                observations.append((signal_name, instance.status if instance is not None else None))
            signal.connect(receiver, sender=OutboxEvent, weak=False)
            receivers.append((signal, receiver))
        try:
            with patch.object(events, '_claim_event_postgresql') as native, CaptureQueriesContext(connection) as queries:
                claimed = events.claim_event()
            native.assert_not_called()
            self.assertEqual(claimed.pk, record.pk)
        finally:
            for signal, receiver in receivers:
                signal.disconnect(receiver, sender=OutboxEvent)
        self.assertEqual(observations, [('pre_init', None), ('post_init', 'PENDING'),
                                        ('pre_save', 'PROCESSING'), ('post_save', 'PROCESSING')])
        self.assertEqual(sum(row['sql'].startswith('SELECT') for row in queries), 1)
        self.assertEqual(sum(row['sql'].startswith('UPDATE') for row in queries), 1)

    def test_custom_save_and_field_hook_fallbacks_execute_original_extensions(self):
        record = self.row()
        original = OutboxEvent.save
        called = []
        def save(instance, *args, **kwargs):
            called.append((instance.status, kwargs['update_fields']))
            return original(instance, *args, **kwargs)
        with patch.object(OutboxEvent, 'save', save), patch.object(events, '_claim_event_postgresql') as native:
            self.assertEqual(events.claim_event().pk, record.pk)
        native.assert_not_called()
        self.assertEqual(called, [('PROCESSING', ['status', 'lease_token', 'locked_until'])])
        other = self.row()
        field = OutboxEvent._meta.get_field('lease_token')
        standard_pre_save = field.pre_save
        with patch.object(field, 'pre_save', wraps=standard_pre_save) as hook, \
                patch.object(events, '_claim_event_postgresql') as native:
            self.assertEqual(events.claim_event().pk, other.pk)
        native.assert_not_called()
        hook.assert_called_once()

    def test_queryset_constructor_filter_extension_keeps_row_ineligible_through_orm(self):
        record = self.row()
        previous = self.fields(record)
        original = QuerySet.__init__
        missing = UUID(int=0)
        def restricted_init(queryset, *args, **kwargs):
            original(queryset, *args, **kwargs)
            if queryset.model is OutboxEvent:
                queryset.query.add_q(models.Q(pk=missing))
        # Manager.raw() constructs a QuerySet but does not incorporate its
        # query filters into supplied SQL. A constructor restriction must keep
        # the ordinary filtered candidate query instead of claiming this row.
        with patch.object(QuerySet, '__init__', restricted_init), \
                patch.object(events, '_claim_event_postgresql') as native, \
                CaptureQueriesContext(connection) as queries:
            self.assertFalse(events._plain_outbox_claim(OutboxEvent.objects))
            self.assertIsNone(events.claim_event())
        native.assert_not_called()
        statements = [row['sql'] for row in queries if 'labops_outboxevent' in row['sql']]
        self.assertEqual(len(statements), 1, statements)
        self.assertTrue(statements[0].startswith('SELECT'))
        self.assertEqual(self.fields(OutboxEvent.objects.get(pk=record.pk)), previous)

    @isolate_apps('labops')
    def test_proxy_model_custom_manager_and_from_db_fallbacks_keep_extension_semantics(self):
        class ProxyOutbox(OutboxEvent):
            class Meta:
                proxy = True
                app_label = 'labops'
        record = self.row()
        with patch.object(events, 'OutboxEvent', ProxyOutbox), \
                patch.object(events, '_claim_event_postgresql') as native:
            self.assertIs(type(events.claim_event()), ProxyOutbox)
        native.assert_not_called()
        other = self.row()
        class RestrictedManager(Manager):
            def get_queryset(self):
                return super().get_queryset().filter(status='DEAD')
        manager = RestrictedManager()
        manager.model = OutboxEvent
        with patch.object(OutboxEvent, 'objects', manager), \
                patch.object(events, '_claim_event_postgresql') as native:
            self.assertIsNone(events.claim_event())
        native.assert_not_called()
        seen = []
        original = OutboxEvent.from_db.__func__
        def from_db(cls, database, field_names, values):
            seen.append(values[field_names.index('status')])
            return original(cls, database, field_names, values)
        with patch.object(OutboxEvent, 'from_db', classmethod(from_db)), \
                patch.object(events, '_claim_event_postgresql') as native:
            self.assertEqual(events.claim_event().pk, other.pk)
        native.assert_not_called()
        self.assertEqual(seen, ['PENDING'])
        self.assertEqual(OutboxEvent.objects.get(pk=record.pk).status, 'PROCESSING')

    def test_custom_json_field_converter_falls_back_and_does_not_rewrite_payload(self):
        record = self.row()
        field = OutboxEvent._meta.get_field('payload_json')
        standard_type = type(field)
        class CustomJSONField(models.JSONField):
            def from_db_value(self, value, expression, database):
                result = super().from_db_value(value, expression, database)
                return {**result, '_converted': True}
        field.__class__ = CustomJSONField
        try:
            with patch.object(events, '_claim_event_postgresql') as native:
                claimed = events.claim_event()
            native.assert_not_called()
            self.assertEqual(claimed.pk, record.pk)
            self.assertTrue(claimed.payload_json['_converted'])
        finally:
            field.__class__ = standard_type
        self.assertEqual(OutboxEvent.objects.get(pk=record.pk).payload_json, record.payload_json)

    def test_patched_random_source_keeps_empty_orm_call_count_and_fresh_token(self):
        with patch.object(events.uuid, 'uuid4', return_value=UUID(int=99)) as random, \
                patch.object(events, '_claim_event_postgresql') as native:
            self.assertIsNone(events.claim_event())
            random.assert_not_called()
            native.assert_not_called()
        record = self.row()
        with patch.object(events.uuid, 'uuid4', return_value=UUID(int=99)) as random:
            self.assertEqual(events.claim_event().lease_token, UUID(int=99))
            random.assert_called_once_with()
        self.assertEqual(OutboxEvent.objects.get(pk=record.pk).lease_token, UUID(int=99))
