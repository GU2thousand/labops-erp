"""Configuration checks and actual PostgreSQL parameter-binding boundaries."""
import hashlib
import json
import os
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import subprocess
import sys
from contextlib import nullcontext
from unittest import skipUnless
from unittest.mock import patch
from uuid import UUID, uuid4

from django.db import connection, transaction
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.utils import timezone

from labops import events
from labops.models import Item, OutboxEvent, Project, User
from labops.publisher_shards import PublisherShardOwner
from labops.worker_metrics import database_processing_budget, database_statement_budget


def proof(name, value):
    directory = os.environ.get('LABOPS_POSTGRES_BINDING_PROOF_EVIDENCE')
    if directory:
        root = Path(__file__).resolve().parents[2]
        value = {**value, 'source_sha256': {
            path: hashlib.sha256((root / path).read_bytes()).hexdigest()
            for path in ('config/settings.py', 'labops/tests/test_postgres_binding.py')}}
        (Path(directory) / (name + '.json')).write_text(
            json.dumps(value, indent=2, sort_keys=True) + '\n')


class PostgresBindingConfigurationTests(SimpleTestCase):
    def configuration(self, mode, binding):
        environment = os.environ.copy()
        for key in ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD',
                    'POSTGRES_HOST', 'POSTGRES_PORT', 'OTEL_EXPORTER_OTLP_ENDPOINT',
                    'DB_SERVER_SIDE_BINDING'):
            environment.pop(key, None)
        environment.update(DJANGO_SETTINGS_MODULE='config.settings', LABOPS_DB_MODE=mode,
            LABOPS_DEBUG='0', LABOPS_SECRET_KEY='binding-configuration-fixture',
            DATABASE_URL='postgresql://binding:synthetic@127.0.0.1:1/unused',
            PYTHONDONTWRITEBYTECODE='1')
        if binding is not None:
            environment['DB_SERVER_SIDE_BINDING'] = binding
        code = """
import json
from django.db import connections
database = connections['default']
result = {'engine': database.settings_dict['ENGINE'],
          'options': database.settings_dict['OPTIONS']}
if database.vendor == 'postgresql':
    params = database.get_connection_params()
    result['cursor_factory'] = params['cursor_factory'].__qualname__
    result['prepare_threshold'] = params['prepare_threshold']
    result['driver_receives_binding_option'] = 'server_side_binding' in params
print(json.dumps(result, sort_keys=True))
"""
        return subprocess.run([sys.executable, '-c', code],
            cwd=Path(__file__).resolve().parents[2], env=environment,
            capture_output=True, text=True, timeout=15)

    def test_postgresql_default_rollback_and_invalid_modes(self):
        observations = []
        for selector, enabled in ((None, True), ('1', True), ('0', False)):
            with self.subTest(selector=selector):
                result = self.configuration('postgres', selector)
                self.assertEqual(result.returncode, 0, result.stderr)
                value = json.loads(result.stdout)
                self.assertIs(value['options']['server_side_binding'], enabled)
                self.assertIn('prepare_threshold', value['options'])
                self.assertIsNone(value['options']['prepare_threshold'])
                self.assertIsNone(value['prepare_threshold'])
                self.assertEqual(value['cursor_factory'], 'ServerBindingCursor' if enabled else 'Cursor')
                self.assertFalse(value['driver_receives_binding_option'])
                observations.append({'selector': selector, **value})
        for selector in ('', '2', 'true', ' 1'):
            with self.subTest(invalid_selector=selector):
                result = self.configuration('postgres', selector)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('DB_SERVER_SIDE_BINDING must be 0 or 1', result.stderr)
                observations.append({'selector': selector, 'rejected': True})
        proof('binding-configuration', {'observations': observations, 'database_connected': False})

    def test_sqlite_options_ignore_postgresql_binding_selector(self):
        result = self.configuration('sqlite-demo', 'invalid-postgresql-selector')
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value['engine'], 'django.db.backends.sqlite3')
        self.assertEqual(value['options'], {'timeout': 30, 'transaction_mode': 'IMMEDIATE'})
        proof('binding-sqlite-configuration', {**value, 'database_connected': False})


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL binding compatibility')
class PostgresBindingLiveTests(TransactionTestCase):
    def assert_cursor(self, database):
        from django.db.backends.postgresql.base import Cursor, ServerBindingCursor
        import psycopg
        database.ensure_connection()
        enabled = database.settings_dict['OPTIONS']['server_side_binding']
        self.assertIs(type(enabled), bool)
        self.assertIsNone(database.settings_dict['OPTIONS']['prepare_threshold'])
        self.assertIsNone(database.connection.prepare_threshold)
        with database.cursor() as cursor:
            expected = ServerBindingCursor if enabled else Cursor
            self.assertIsInstance(cursor.cursor, expected)
            self.assertEqual(isinstance(cursor.cursor, psycopg.ClientCursor), not enabled)
            cursor.execute('SELECT pg_backend_pid()')
            pid = cursor.fetchone()[0]
            kind = type(cursor.cursor).__qualname__
        return {'server_side_binding': enabled, 'cursor_type': kind,
                'prepare_threshold': database.connection.prepare_threshold, 'backend_pid': pid}

    def test_effective_cursors_and_owned_session_copies_keep_binding_mode(self):
        application = self.assert_cursor(connection)
        copied = connection.copy(alias='binding_copy')
        try:
            copied.settings_dict['AUTOCOMMIT'] = True
            copy_value = self.assert_cursor(copied)
            self.assertEqual(copy_value['server_side_binding'], application['server_side_binding'])
            self.assertNotEqual(copy_value['backend_pid'], application['backend_pid'])
        finally:
            copied.close()
        owner = PublisherShardOwner(0, 1)
        try:
            owner.acquire()
            owned = self.assert_cursor(owner._connection)
            owner.assert_owned()
            self.assertEqual(owned['server_side_binding'], application['server_side_binding'])
            self.assertEqual(owned['backend_pid'], owner.backend_pid)
            self.assertNotEqual(owned['backend_pid'], application['backend_pid'])
        finally:
            owner.close()
        with connection.chunked_cursor() as cursor:
            cursor.execute('SELECT %s::integer', [7])
            self.assertEqual(cursor.fetchall(), [(7,)])
            named = {'cursor_type': type(cursor.cursor).__qualname__, 'named': bool(cursor.cursor.name)}
        self.assertTrue(named['named'])
        proof('binding-effective-cursors', {'application': application, 'copy': copy_value,
            'owner': owned, 'owner_closed': owner._connection is None,
            'copy_closed': copied.connection is None, 'named_cursor': named})

    def test_quotes_percent_and_rollback_preserve_typed_values(self):
        effective = self.assert_cursor(connection)
        text = "quotes ' \"; backslash \\; percent %; 中文; -- $99"
        with connection.cursor() as cursor:
            cursor.execute("SELECT %s::text, '%%'::text, (%s::bigint %% %s::bigint)", [text, 17, 5])
            self.assertEqual(cursor.fetchone(), (text, '%', 2))
            wire = cursor.cursor._query.query
            self.assertEqual(b'$1' in wire, effective['server_side_binding'])
        code = 'BINDING-ROLLBACK'
        error = RuntimeError('binding rollback fixture')
        with self.assertRaises(RuntimeError) as raised:
            with transaction.atomic():
                Item.objects.create(code=code, name=text, base_uom='EA')
                self.assertEqual(Item.objects.get(code=code).name, text)
                raise error
        self.assertIs(raised.exception, error)
        self.assertFalse(Item.objects.filter(code=code).exists())
        self.assertFalse(connection.in_atomic_block)
        proof('binding-percent-rollback', {'effective': effective, 'text_roundtrip': True,
            'literal_percent': '%', 'modulo_result': 2, 'rollback_absent': True,
            'server_placeholders_observed': b'$1' in wire})

    def test_uuid_arrays_and_bulk_fixed6_values_preserve_exact_types(self):
        effective = self.assert_cursor(connection)
        high = Decimal('999999999999.999999')
        rows = Item.objects.bulk_create([
            Item(id=UUID(int=301), code='BINDING-MICRO', name='Micro', base_uom='EA', reorder_qty=Decimal('0.000001')),
            Item(id=UUID(int=302), code='BINDING-HIGH', name='High', base_uom='EA', reorder_qty=high)])
        values = dict(Item.objects.filter(pk__in=[r.pk for r in rows]).values_list('id', 'reorder_qty'))
        self.assertEqual(values, {rows[0].pk: Decimal('0.000001'), rows[1].pk: high})
        observations = []
        with connection.cursor() as cursor:
            for array in ([r.pk for r in rows], [str(r.pk) for r in rows], []):
                cursor.execute('SELECT id FROM labops_item WHERE id = ANY(%s::uuid[]) ORDER BY id', [array])
                selected = [row[0] for row in cursor.fetchall()]
                self.assertEqual(selected, [r.pk for r in rows] if array else [])
                self.assertTrue(all(isinstance(value, UUID) for value in selected))
                observations.append({'array_value_type': type(array[0]).__name__ if array else 'empty',
                                     'selected_ids': [str(value) for value in selected]})
            cursor.execute('SELECT reorder_qty FROM labops_item WHERE id = %s', [rows[1].pk])
            self.assertEqual(cursor.fetchone()[0], 999999999999999999)
        self.assertEqual(list(Item.objects.filter(pk__in=[r.pk for r in rows]).order_by('id')
            .values_list('id', 'reorder_qty').iterator(chunk_size=1)),
            [(rows[0].pk, Decimal('0.000001')), (rows[1].pk, high)])
        proof('binding-uuid-fixed6', {'effective': effective, 'arrays': observations,
            'micro_units': 999999999999999999, 'fixed6_roundtrip': str(values[rows[1].pk]),
            'bulk_rows': len(rows), 'named_iterator_rows': len(rows)})

    def test_json_datetime_and_decimal_roundtrip_keep_database_types(self):
        effective = self.assert_cursor(connection)
        instant = timezone.now().replace(microsecond=123456)
        payload = {'unicode': '绑定 % \\ \"', 'nested': [True, None, {'value': '-0.000001'}]}
        event = OutboxEvent.objects.create(event_type='binding.fixture', aggregate_type='binding',
            aggregate_id=uuid4(), dedupe_key=str(uuid4()), payload_json=payload, created_at=instant)
        found = OutboxEvent.objects.get(pk=event.pk)
        self.assertEqual(found.payload_json, payload)
        self.assertEqual(found.created_at, instant)
        self.assertTrue(timezone.is_aware(found.created_at))
        actor = User.objects.create(email='binding@example.test', name='Binding fixture')
        amount = Decimal('9999999999999999.99')
        project = Project.objects.create(code='BINDING-NUMERIC', name='Binding fixture', owner=actor, budget_amount=amount)
        self.assertEqual(Project.objects.get(pk=project.pk).budget_amount, amount)
        proof('binding-json-datetime-decimal', {'effective': effective, 'json_roundtrip': True,
            'datetime': found.created_at.isoformat(), 'datetime_aware': True,
            'decimal_roundtrip': str(amount)})

    @override_settings(EVENT_DB_LOCK_TIMEOUT_MS=111,
        EVENT_RETRY_STATEMENT_TIMEOUT_MS=250, EVENT_RETRY_LOCK_TIMEOUT_MS=77)
    def test_session_budgets_restore_same_backend_after_rollback(self):
        effective = self.assert_cursor(connection)
        read = "SELECT current_setting('statement_timeout'), current_setting('lock_timeout'), pg_backend_pid()"
        apply = "SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)"
        def values():
            with connection.cursor() as cursor:
                cursor.execute(read)
                return cursor.fetchone()
        initial = values()
        try:
            with connection.cursor() as cursor:
                cursor.execute(apply, ['37000', '91'])
            before = values()
            self.assertEqual(before, ('37s', '91ms', effective['backend_pid']))
            with database_statement_budget(2):
                self.assertEqual(values(), ('2s', '111ms', before[2]))
                with self.assertRaisesRegex(RuntimeError, 'processing rollback'):
                    with transaction.atomic():
                        with database_processing_budget():
                            self.assertEqual(values(), ('250ms', '77ms', before[2]))
                            Item.objects.create(code='BINDING-BUDGET', name='Budget', base_uom='EA')
                        self.assertEqual(values(), ('250ms', '77ms', before[2]))
                        raise RuntimeError('processing rollback')
                self.assertEqual(values(), ('2s', '111ms', before[2]))
            self.assertEqual(values(), before)
            self.assertFalse(Item.objects.filter(code='BINDING-BUDGET').exists())
            proof('binding-budgets', {'effective': effective, 'sentinel_before': list(before),
                'sentinel_after': list(values()), 'same_backend': True,
                'transaction_local_until_outer_rollback': True, 'rolled_back_item_absent': True})
        finally:
            with connection.cursor() as cursor:
                cursor.execute(apply, list(initial[:2]))

    def test_tracing_connection_wrapper_retains_binding_roundtrip(self):
        import psycopg
        from opentelemetry.instrumentation.psycopg import DatabaseApiIntegration, PsycopgInstrumentor
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        from django.db.backends.postgresql.base import Cursor, ServerBindingCursor
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        integration = DatabaseApiIntegration('binding-proof', 'postgresql',
            connection_attributes=PsycopgInstrumentor._CONNECTION_ATTRIBUTES, tracer_provider=provider)
        raw = integration.wrapped_connection(psycopg.connect, (), connection.get_connection_params())
        try:
            raw.autocommit = True
            self.assertIsNone(raw.prepare_threshold)
            enabled = connection.settings_dict['OPTIONS']['server_side_binding']
            with raw.cursor() as cursor:
                self.assertIsInstance(cursor, ServerBindingCursor if enabled else Cursor)
                cursor.execute('SELECT %s::text, pg_backend_pid()', ['traced % quote\''])
                result, pid = cursor.fetchone()
                self.assertEqual(result, 'traced % quote\'')
                kind = type(cursor).__qualname__
            spans = exporter.get_finished_spans()
            self.assertEqual(len(spans), 1)
            self.assertEqual(spans[0].name, 'SELECT')
            self.assertFalse(any('parameters' in key for key in spans[0].attributes))
        finally:
            raw.close()
            provider.shutdown()
        proof('binding-tracing', {'server_side_binding': enabled, 'cursor_type': kind,
            'prepare_threshold': None, 'backend_pid': pid, 'span_count': len(spans),
            'roundtrip': True, 'native_connection_closed': raw.closed, 'exporter': 'in-memory'})

    def test_repeated_parameterized_claim_keeps_partial_index_and_no_prepared_cache(self):
        now = timezone.now()
        rows = []
        for index in range(4080):
            changes = {'status': 'PUBLISHED', 'created_at': now - timedelta(hours=1)}
            if 3000 <= index < 4024:
                changes = {'status': 'PENDING', 'created_at': now + timedelta(microseconds=index - 3000)}
            elif index >= 4024:
                controls = (
                    {'transport': 'local', 'status': 'PENDING'},
                    {'status': 'DEAD'},
                    {'status': 'PENDING', 'next_attempt_at': now + timedelta(days=1)},
                    {'status': 'PROCESSING', 'locked_until': now + timedelta(days=1)},
                    {'status': 'PROCESSING', 'locked_until': None},
                    {'status': 'PUBLISHED'},
                    {'status': 'PROCESSING', 'locked_until': now - timedelta(seconds=1)},
                )
                changes = {'created_at': now + timedelta(days=2), **controls[(index - 4024) // 8]}
            row = OutboxEvent(**{'id': UUID(int=10000 + index), 'transport': 'kafka',
                'event_type': 'binding.fixture', 'aggregate_type': 'binding-planner',
                'aggregate_id': UUID(int=20000 + index), 'dedupe_key': 'binding-planner-' + str(index),
                'next_attempt_at': now - timedelta(seconds=1), **changes})
            rows.append(row)
        OutboxEvent.objects.bulk_create(rows, batch_size=256)
        expected = rows[3000].pk
        with connection.cursor() as cursor:
            cursor.execute('ANALYZE labops_outboxevent')
        def nodes(plan):
            result = [(plan['Node Type'], plan.get('Index Name'))]
            for child in plan.get('Plans', []):
                result.extend(nodes(child))
            return result
        for path in ('orm', 'native_postgresql'):
            with self.subTest(claim_path=path):
                captured = []
                expected_start = 'SELECT' if path == 'orm' else 'WITH claimed AS ('
                def observe(execute, sql, params, many, context):
                    if (sql.lstrip().startswith(expected_start) and 'FROM "labops_outboxevent"' in sql
                            and 'FOR UPDATE SKIP LOCKED' in sql):
                        captured.append((sql, params))
                    return execute(sql, params, many, context)
                # Preserve the original SELECT planner proof independently.
                # The native capture also exercises parameterized shard modulo.
                admission = patch.object(events, '_plain_outbox_claim', return_value=False) if path == 'orm' else nullcontext()
                kwargs = {} if path == 'orm' else {'shard_index': 0, 'shard_count': 2}
                if path == 'native_postgresql':
                    self.assertTrue(events._plain_outbox_claim(OutboxEvent.objects))
                with admission, transaction.atomic(), connection.execute_wrapper(observe), \
                        patch.object(events.timezone, 'now', return_value=now):
                    self.assertEqual(events.claim_event(**kwargs).pk, expected)
                    transaction.set_rollback(True)
                self.assertEqual(len(captured), 1)
                sql, params = captured[0]
                self.assertTrue(params, 'Exercise driver binding with actual claim parameters')
                if path == 'native_postgresql':
                    self.assertIn('UPDATE "labops_outboxevent" AS event', sql)
                    self.assertIn('RETURNING event."id"', sql)
                    self.assertIn('%% %s', sql)
                    self.assertTrue(any(type(value) is UUID for value in params))
                observations = []
                for enabled in (False, True):
                    with self.subTest(server_side_binding=enabled):
                        database = connection.copy(alias='binding_planner_' + path + '_' + str(enabled))
                        database.settings_dict['OPTIONS'] = {**database.settings_dict['OPTIONS'],
                            'server_side_binding': enabled, 'prepare_threshold': None}
                        database.settings_dict['AUTOCOMMIT'] = True
                        try:
                            effective = self.assert_cursor(database)
                            # This copy is not registered with Django atomic.
                            # Roll back each native UPDATE, including EXPLAIN
                            # ANALYZE, so every repetition claims the same row.
                            database.set_autocommit(False)
                            with database.cursor() as cursor:
                                for _ in range(8):
                                    cursor.execute(sql, params)
                                    self.assertEqual(cursor.fetchone()[0], expected)
                                    self.assertIsNone(cursor.fetchone())
                                    database.rollback()
                                cursor.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + sql, params)
                                plan = cursor.fetchone()[0]
                                if isinstance(plan, str):
                                    plan = json.loads(plan)
                                shape = nodes(plan[0]['Plan'])
                                self.assertIn('outbox_active_created_id_idx', [name for _, name in shape])
                                database.rollback()
                                cursor.execute('SELECT name FROM pg_prepared_statements')
                                self.assertEqual(cursor.fetchall(), [])
                                cursor.execute('SHOW plan_cache_mode')
                                cache_mode = cursor.fetchone()[0]
                                database.rollback()
                            observations.append({'effective': effective, 'execution_count': 8,
                                'selected_id': str(expected), 'named_prepared_statements': [],
                                'plan_cache_mode': cache_mode, 'plan': plan, 'node_shape': shape,
                                'each_execution_and_explain_rolled_back': True})
                        finally:
                            database.close()
                self.assertEqual(observations[0]['node_shape'], observations[1]['node_shape'])
                found = OutboxEvent.objects.get(pk=expected)
                self.assertEqual(found.status, 'PENDING')
                self.assertIsNone(found.lease_token)
                self.assertIsNone(found.locked_until)
                proof('binding-partial-index' if path == 'orm' else 'binding-native-claim-partial-index', {
                    'claim_path': path, 'fixture_rows': 4080, 'published_history_rows': 3000,
                    'pending_backlog_rows': 1024, 'control_rows': 56, 'normal_planner': True,
                    'actual_claim_sql_sha256': hashlib.sha256(sql.encode()).hexdigest(),
                    'observations': observations, 'equivalent_node_shape': True,
                    'equivalent_selected_id': True, 'claim_mutations_rolled_back': True})
