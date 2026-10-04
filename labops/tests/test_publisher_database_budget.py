"""Single-statement setup preserves each durable operation's session budget."""
from unittest import skipUnless
from unittest.mock import MagicMock, call, patch
from contextlib import contextmanager, nullcontext
from uuid import uuid4

from django.db import connection, DatabaseError, OperationalError, transaction
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops.models import OutboxEvent
from labops.publisher_shards import publisher_shard_owner
from labops.worker_metrics import (database_statement_budget, database_processing_budget,
                                   OperationDeadlineExceeded)


READ = "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')"
SETUP = """WITH previous AS MATERIALIZED (
                SELECT current_setting('statement_timeout') AS statement_value,
                       current_setting('lock_timeout') AS lock_value
            )
            SELECT previous.statement_value, previous.lock_value,
                   set_config('statement_timeout', %s, false),
                   set_config('lock_timeout', %s, false)
            FROM previous"""
SET = "SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)"
LOCAL = "SELECT set_config('statement_timeout', %s, true), set_config('lock_timeout', %s, true)"


@override_settings(EVENT_DB_LOCK_TIMEOUT_MS=111)
class PublisherBudgetStatementTests(SimpleTestCase):
    def database(self, vendor='postgresql'):
        database = MagicMock()
        database.vendor = vendor
        cursor = database.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ('37s', '91ms', '1250ms', '111ms')
        return database, cursor

    def test_single_setup_and_restore_use_original_values_and_session_scope(self):
        database, cursor = self.database()
        with patch('django.db.connection', database):
            with database_statement_budget(1.25):
                self.assertEqual(cursor.execute.call_args_list,
                    [call(SETUP, ['1250', '111'])])
        self.assertEqual(cursor.execute.call_args_list,
            [call(SETUP, ['1250', '111']), call(SET, ['37s', '91ms'])])
        database.close.assert_not_called()

    def test_restore_database_error_closes_connection_without_replacing_body_error(self):
        database, cursor = self.database()
        cursor.execute.side_effect = [None, OperationalError('restore failed')]
        original = RuntimeError('original body failure')
        with patch('django.db.connection', database):
            with self.assertRaises(RuntimeError) as raised:
                with database_statement_budget(2):
                    raise original
        self.assertIs(raised.exception, original)
        self.assertEqual(cursor.execute.call_args_list,
            [call(SETUP, ['2000', '111']), call(SET, ['37s', '91ms'])])
        database.close.assert_called_once_with()

    def test_unknown_setup_execute_or_fetch_preserves_primary_error_even_if_close_fails(self):
        for stage, original in (('execute', OperationalError('ambiguous setup execute')),
                                ('fetch', OperationDeadlineExceeded('setup control exception'))):
            with self.subTest(stage=stage):
                database, cursor = self.database()
                getattr(cursor, stage if stage == 'execute' else 'fetchone').side_effect = original
                database.close.side_effect = RuntimeError('secondary close failure')
                entered = []
                with patch('django.db.connection', database):
                    with self.assertRaises(type(original)) as raised:
                        with database_statement_budget(1.25):
                            entered.append(True)
                self.assertIs(raised.exception, original)
                self.assertEqual(entered, [])
                cursor.execute.assert_called_once_with(SETUP, ['1250', '111'])
                database.close.assert_called_once_with()

    def test_invalid_setup_result_shape_discards_application_connection_without_body_or_retry(self):
        database, cursor = self.database()
        cursor.fetchone.return_value = ('37s', '91ms')
        entered = []
        with patch('django.db.connection', database):
            with self.assertRaises(ValueError):
                with database_statement_budget(1.25):
                    entered.append(True)
        self.assertEqual(entered, [])
        cursor.execute.assert_called_once_with(SETUP, ['1250', '111'])
        database.close.assert_called_once_with()

    def test_non_postgresql_retains_no_query_path_and_original_body_error(self):
        database, _ = self.database('sqlite')
        original = RuntimeError('original body failure')
        with patch('django.db.connection', database):
            with self.assertRaises(RuntimeError) as raised:
                with database_statement_budget(2):
                    raise original
        self.assertIs(raised.exception, original)
        database.cursor.assert_not_called()
        database.close.assert_not_called()

    @override_settings(EVENT_RETRY_STATEMENT_TIMEOUT_MS=250, EVENT_RETRY_LOCK_TIMEOUT_MS=77)
    def test_processing_pair_stays_transaction_local_and_retains_atomic_body(self):
        database, cursor = self.database()
        original = RuntimeError('processing body failed')
        with patch('django.db.connection', database), patch('django.db.transaction.atomic', return_value=nullcontext()) as atomic:
            with self.assertRaises(RuntimeError) as raised:
                with database_processing_budget():
                    raise original
        self.assertIs(raised.exception, original)
        atomic.assert_called_once_with()
        cursor.execute.assert_called_once_with(LOCAL, ['250', '77'])
        database.close.assert_not_called()


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL publisher session budgets')
@override_settings(EVENT_DB_LOCK_TIMEOUT_MS=111)
class PublisherBudgetPostgreSQLTests(TransactionTestCase):
    def native_values(self, database=None):
        # Native reads deliberately stay outside Django's captured statement
        # count, while observing the exact same physical session.
        database = connection if database is None else database
        with database.connection.cursor() as cursor:
            cursor.execute(READ + ', pg_backend_pid()')
            return cursor.fetchone()

    @contextmanager
    def owned_database(self, enabled):
        from django.db.backends.postgresql.base import Cursor, ServerBindingCursor
        database = connection.copy(alias='publisher_budget_' + str(enabled))
        database.settings_dict['OPTIONS'] = {**database.settings_dict['OPTIONS'],
            'server_side_binding': enabled, 'prepare_threshold': None}
        database.settings_dict['AUTOCOMMIT'] = True
        try:
            database.ensure_connection()
            with database.cursor() as cursor:
                self.assertIsInstance(cursor.cursor, ServerBindingCursor if enabled else Cursor)
            self.assertTrue(database.get_autocommit())
            yield database
        finally:
            database.close()

    def test_live_single_setup_restores_nondefault_values_for_both_binding_modes_and_body_errors(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.owned_database(enabled) as database:
                for body_error in (None, RuntimeError('body failed before publication'),
                                   OperationDeadlineExceeded('original durable operation deadline')):
                    with self.subTest(body_error=type(body_error).__name__):
                        with database.cursor() as cursor:
                            cursor.execute(SET, ['37000', '91'])
                        before = self.native_values(database)
                        self.assertEqual(before[:2], ('37s', '91ms'))
                        with patch('django.db.connection', database), CaptureQueriesContext(database) as queries:
                            if body_error is None:
                                with database_statement_budget(1.25):
                                    self.assertEqual(self.native_values(database), ('1250ms', '111ms', before[2]))
                                    self.assertTrue(database.get_autocommit())
                                    self.assertFalse(database.in_atomic_block)
                            else:
                                with self.assertRaises(type(body_error)) as raised:
                                    with database_statement_budget(1.25):
                                        self.assertEqual(self.native_values(database), ('1250ms', '111ms', before[2]))
                                        raise body_error
                                self.assertIs(raised.exception, body_error)
                        self.assertEqual(len(queries), 2)
                        self.assertTrue(queries[0]['sql'].startswith('WITH previous AS MATERIALIZED'))
                        self.assertTrue(queries[1]['sql'].startswith("SELECT set_config('statement_timeout'"))
                        self.assertEqual(self.native_values(database), before)

    def test_nested_session_budgets_restore_inner_to_outer_to_original_without_a_transaction(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.owned_database(enabled) as database:
                for inner_error in (None, RuntimeError('inner budget failed')):
                    with self.subTest(inner_error=inner_error is not None):
                        with database.cursor() as cursor:
                            cursor.execute(SET, ['37000', '91'])
                        before = self.native_values(database)
                        with patch('django.db.connection', database), CaptureQueriesContext(database) as queries:
                            with database_statement_budget(2):
                                outer = ('2s', '111ms', before[2])
                                self.assertEqual(self.native_values(database), outer)
                                with override_settings(EVENT_DB_LOCK_TIMEOUT_MS=77):
                                    if inner_error is None:
                                        with database_statement_budget(.5):
                                            self.assertEqual(self.native_values(database), ('500ms', '77ms', before[2]))
                                    else:
                                        with self.assertRaises(RuntimeError) as raised:
                                            with database_statement_budget(.5):
                                                self.assertEqual(self.native_values(database), ('500ms', '77ms', before[2]))
                                                raise inner_error
                                        self.assertIs(raised.exception, inner_error)
                                self.assertEqual(self.native_values(database), outer)
                                self.assertTrue(database.get_autocommit())
                                self.assertFalse(database.in_atomic_block)
                        self.assertEqual(len(queries), 4)
                        self.assertEqual(self.native_values(database), before)

    def test_session_budget_inside_outer_atomic_and_savepoint_restores_at_each_boundary(self):
        connection.ensure_connection()
        initial = self.native_values()
        try:
            with connection.cursor() as cursor:
                cursor.execute(SET, ['37000', '91'])
            before = self.native_values()
            for rollback in (False, True):
                with self.subTest(outer_rollback=rollback):
                    with transaction.atomic():
                        with database_statement_budget(1.25):
                            self.assertTrue(connection.in_atomic_block)
                            self.assertFalse(connection.get_autocommit())
                            self.assertEqual(self.native_values(), ('1250ms', '111ms', before[2]))
                        self.assertEqual(self.native_values(), before)
                        if rollback:
                            transaction.set_rollback(True)
                    self.assertEqual(self.native_values(), before)
            original = RuntimeError('original inner savepoint body error')
            with transaction.atomic():
                with self.assertRaises(RuntimeError) as raised:
                    with transaction.atomic():
                        with database_statement_budget(.5):
                            self.assertEqual(self.native_values(), ('500ms', '111ms', before[2]))
                            raise original
                self.assertIs(raised.exception, original)
                self.assertEqual(self.native_values(), before)
            self.assertEqual(self.native_values(), before)
        finally:
            with connection.cursor() as cursor:
                cursor.execute(SET, list(initial[:2]))

    def test_setup_second_setter_server_error_rolls_back_then_discards_session_before_body(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.owned_database(enabled) as database:
                with database.cursor() as cursor:
                    cursor.execute(SET, ['37000', '91'])
                before, raw, rolled_back, entered = self.native_values(database), database.connection, [], []
                def observe(execute, sql, params, many, context):
                    try:
                        return execute(sql, params, many, context)
                    except DatabaseError:
                        # Autocommit has rolled back the failed whole statement.
                        # Observe the original session before the helper closes
                        # it; this is not a production restore or business retry.
                        rolled_back.append(self.native_values(database))
                        raise
                with override_settings(EVENT_DB_LOCK_TIMEOUT_MS='invalid'), \
                        patch('django.db.connection', database), database.execute_wrapper(observe), \
                        CaptureQueriesContext(database) as queries:
                    with self.assertRaises(DatabaseError) as raised:
                        with database_statement_budget(1.25):
                            entered.append(True)
                self.assertEqual(raised.exception.__cause__.sqlstate, '22023')
                self.assertEqual(rolled_back, [before])
                self.assertEqual(entered, [])
                self.assertEqual(len(queries), 1)
                self.assertTrue(queries[0]['sql'].startswith('WITH previous AS MATERIALIZED'))
                self.assertIsNone(database.connection)
                self.assertTrue(raw.closed)

    def test_setup_fetch_or_shape_failure_after_actual_apply_closes_only_application_session(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.owned_database(enabled) as database:
                for malformed in (False, True):
                    with self.subTest(malformed_result=malformed):
                        database.ensure_connection()
                        with database.cursor() as cursor:
                            cursor.execute(SET, ['37000', '91'])
                        before, raw = self.native_values(database), database.connection
                        failure, entered, applied = RuntimeError('original post-apply fetch failure'), [], []
                        original_cursor = database.cursor
                        observed_values = self.native_values
                        class SetupCursor:
                            def __init__(self):
                                self.cursor = original_cursor()
                            def __getattr__(self, name):
                                # Preserve driver attributes used by DEBUG SQL
                                # composition before the injected fetch fault.
                                return getattr(self.cursor, name)
                            def __enter__(self):
                                self.cursor.__enter__()
                                return self
                            def __exit__(self, *args):
                                return self.cursor.__exit__(*args)
                            def execute(self, sql, params):
                                result = self.cursor.execute(sql, params)
                                applied.append(observed_values(database))
                                return result
                            def fetchone(self):
                                if malformed:
                                    return self.cursor.fetchone()[:2]
                                raise failure
                        with publisher_shard_owner(0, 1) as owner:
                            owner_raw, owner_pid = owner._raw_connection, owner.backend_pid
                            with patch('django.db.connection', database), \
                                    CaptureQueriesContext(database) as queries, \
                                    patch.object(database, 'cursor', side_effect=SetupCursor):
                                with self.assertRaises(ValueError if malformed else RuntimeError) as raised:
                                    with database_statement_budget(1.25):
                                        entered.append(True)
                            if not malformed:
                                self.assertIs(raised.exception, failure)
                            owner.assert_owned()
                            self.assertIs(owner._raw_connection, owner_raw)
                            self.assertEqual(owner.backend_pid, owner_pid)
                            self.assertFalse(owner_raw.closed)
                        self.assertEqual(applied, [('1250ms', '111ms', before[2])])
                        self.assertEqual(entered, [])
                        self.assertEqual(len(queries), 1)
                        self.assertIsNone(database.connection)
                        self.assertTrue(raw.closed)

    def test_real_statement_timeout_restores_same_session_and_accepts_a_later_query(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.owned_database(enabled) as database:
                with database.cursor() as cursor:
                    cursor.execute(SET, ['37000', '91'])
                before = self.native_values(database)
                with patch('django.db.connection', database):
                    with self.assertRaises(OperationalError) as raised:
                        with database_statement_budget(.05):
                            with database.cursor() as cursor:
                                cursor.execute('SELECT pg_sleep(%s)', [.25])
                self.assertEqual(raised.exception.__cause__.sqlstate, '57014')
                self.assertEqual(self.native_values(database), before)
                with database.cursor() as cursor:
                    cursor.execute('SELECT 1')
                    self.assertEqual(cursor.fetchone(), (1,))

    @override_settings(EVENT_DB_LOCK_TIMEOUT_MS=50)
    def test_real_row_lock_timeout_restores_same_session_and_accepts_a_later_query(self):
        record = OutboxEvent.objects.create(event_type='budget.fixture', transport='kafka',
            aggregate_type='budget', aggregate_id=uuid4(), dedupe_key=str(uuid4()))
        holder = connection.copy(alias='publisher_budget_lock_holder')
        holder.settings_dict['AUTOCOMMIT'] = True
        try:
            holder.ensure_connection()
            holder.set_autocommit(False)
            with holder.cursor() as cursor:
                cursor.execute('SELECT id FROM labops_outboxevent WHERE id = %s FOR UPDATE', [record.pk])
                self.assertEqual(cursor.fetchone()[0], record.pk)
            for enabled in (False, True):
                with self.subTest(server_side_binding=enabled), self.owned_database(enabled) as database:
                    with database.cursor() as cursor:
                        cursor.execute(SET, ['37000', '91'])
                    before = self.native_values(database)
                    with patch('django.db.connection', database):
                        with self.assertRaises(OperationalError) as raised:
                            with database_statement_budget(1):
                                with database.cursor() as cursor:
                                    cursor.execute('SELECT id FROM labops_outboxevent WHERE id = %s FOR UPDATE', [record.pk])
                    self.assertEqual(raised.exception.__cause__.sqlstate, '55P03')
                    self.assertEqual(self.native_values(database), before)
                    with database.cursor() as cursor:
                        cursor.execute('SELECT 1')
                        self.assertEqual(cursor.fetchone(), (1,))
        finally:
            holder.rollback()
            holder.close()

    def test_terminated_owned_backend_restore_closes_and_preserves_original_then_reconnects(self):
        connection.ensure_connection()
        before = self.native_values()
        original = RuntimeError('original body failure after backend termination')
        with self.assertRaises(RuntimeError) as raised:
            with database_statement_budget(2):
                self.assertEqual(self.native_values(), ('2s', '111ms', before[2]))
                try:
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT pg_terminate_backend(pg_backend_pid())')
                except DatabaseError:
                    pass
                raise original
        self.assertIs(raised.exception, original)
        self.assertIsNone(connection.connection)
        connection.ensure_connection()
        after = self.native_values()
        self.assertNotEqual(after[2], before[2])
        self.assertEqual(after[:2], before[:2])

    @override_settings(EVENT_RETRY_STATEMENT_TIMEOUT_MS=250, EVENT_RETRY_LOCK_TIMEOUT_MS=77)
    def test_processing_pair_remains_live_until_outer_commit_and_restores_on_outer_rollback(self):
        connection.ensure_connection()
        initial = self.native_values()
        try:
            for body_error in (None, RuntimeError('processing body failed')):
                with self.subTest(body_error=body_error is not None):
                    with connection.cursor() as cursor:
                        cursor.execute(SET, ['37000', '91'])
                    before = self.native_values()
                    with CaptureQueriesContext(connection) as queries:
                        if body_error is None:
                            with transaction.atomic():
                                with database_processing_budget():
                                    self.assertTrue(connection.in_atomic_block)
                                    self.assertEqual(self.native_values(), ('250ms', '77ms', before[2]))
                                # Releasing the helper's savepoint does not
                                # reset SET LOCAL before the physical commit.
                                self.assertEqual(self.native_values(), ('250ms', '77ms', before[2]))
                        else:
                            with self.assertRaises(RuntimeError) as raised:
                                with transaction.atomic():
                                    with database_processing_budget():
                                        self.assertEqual(self.native_values(), ('250ms', '77ms', before[2]))
                                        raise body_error
                            self.assertIs(raised.exception, body_error)
                    local = [row['sql'] for row in queries if row['sql'].startswith("SELECT set_config('statement_timeout'")]
                    self.assertEqual(len(local), 1)
                    self.assertIn("set_config('lock_timeout'", local[0])
                    self.assertEqual(self.native_values(), before)
                    self.assertFalse(connection.in_atomic_block)
        finally:
            with connection.cursor() as cursor:
                cursor.execute(SET, list(initial[:2]))
