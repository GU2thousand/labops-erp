"""Paired session timeout statements preserve each durable operation's budget."""
from unittest import skipUnless
from unittest.mock import MagicMock, call, patch
from contextlib import nullcontext

from django.db import connection, DatabaseError, OperationalError, transaction
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops.worker_metrics import database_statement_budget, database_processing_budget


READ = "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')"
SET = "SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)"
LOCAL = "SELECT set_config('statement_timeout', %s, true), set_config('lock_timeout', %s, true)"


@override_settings(EVENT_DB_LOCK_TIMEOUT_MS=111)
class PublisherBudgetStatementTests(SimpleTestCase):
    def database(self, vendor='postgresql'):
        database = MagicMock()
        database.vendor = vendor
        cursor = database.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ('37s', '91ms')
        return database, cursor

    def test_paired_read_apply_and_restore_use_original_values_and_session_scope(self):
        database, cursor = self.database()
        with patch('django.db.connection', database):
            with database_statement_budget(1.25):
                self.assertEqual(cursor.execute.call_args_list,
                    [call(READ), call(SET, ['1250', '111'])])
        self.assertEqual(cursor.execute.call_args_list,
            [call(READ), call(SET, ['1250', '111']), call(SET, ['37s', '91ms'])])
        database.close.assert_not_called()

    def test_restore_database_error_closes_connection_without_replacing_body_error(self):
        database, cursor = self.database()
        cursor.execute.side_effect = [None, None, OperationalError('restore failed')]
        original = RuntimeError('original body failure')
        with patch('django.db.connection', database):
            with self.assertRaises(RuntimeError) as raised:
                with database_statement_budget(2):
                    raise original
        self.assertIs(raised.exception, original)
        self.assertEqual(cursor.execute.call_args_list,
            [call(READ), call(SET, ['2000', '111']), call(SET, ['37s', '91ms'])])
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
    def native_values(self):
        # Native reads deliberately stay outside Django's captured statement
        # count, while observing the exact same physical session.
        with connection.connection.cursor() as cursor:
            cursor.execute(READ + ', pg_backend_pid()')
            return cursor.fetchone()

    def test_live_budget_and_three_queries_restore_nondefault_values_on_success_and_body_error(self):
        connection.ensure_connection()
        initial = self.native_values()
        try:
            for body_error in (None, RuntimeError('body failed before publication')):
                with self.subTest(body_error=body_error is not None):
                    with connection.cursor() as cursor:
                        cursor.execute(SET, ['37000', '91'])
                    before = self.native_values()
                    self.assertEqual(before[:2], ('37s', '91ms'))
                    with CaptureQueriesContext(connection) as queries:
                        if body_error is None:
                            with database_statement_budget(2):
                                self.assertEqual(self.native_values(), ('2s', '111ms', before[2]))
                        else:
                            with self.assertRaises(RuntimeError) as raised:
                                with database_statement_budget(2):
                                    self.assertEqual(self.native_values(), ('2s', '111ms', before[2]))
                                    raise body_error
                            self.assertIs(raised.exception, body_error)
                    self.assertEqual(len(queries), 3)
                    self.assertEqual(self.native_values(), before)
        finally:
            with connection.cursor() as cursor:
                cursor.execute(SET, list(initial[:2]))

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
