"""Private controller mechanics; admission fixtures do not prove native admission.

The PostgreSQL cases use ``admission=lambda: True`` only to isolate the
controller. Production producer/call-graph admission belongs to command tests.
"""
from contextlib import contextmanager
import signal
import threading
import time
from unittest import skipUnless
from unittest.mock import Mock, patch

from django.db import connection, OperationalError
from django.db.backends.postgresql.base import Cursor, ServerBindingCursor
from django.test import SimpleTestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops import worker_metrics
from labops.publisher_shards import publisher_shard_owner
from labops.worker_metrics import OperationDeadlineExceeded, database_statement_budget, operation_deadline


READ = "SELECT current_setting('statement_timeout'), current_setting('lock_timeout'), pg_backend_pid()"
SET = "SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)"


class BodyControl(BaseException):
    pass


class PublisherBudgetReuseFallbackTests(SimpleTestCase):
    def controller(self):
        controller = worker_metrics.PublisherBatchBudget(1.25, 3, admission=lambda: False)
        controller.before_deadline()
        return controller

    def test_rejected_admission_preserves_factory_body_suppression_and_exit_once(self):
        controller, calls = self.controller(), []
        first = BodyControl('PRIVATE body')
        @contextmanager
        def manager(seconds):
            calls.append(('enter', seconds))
            try:
                yield
            except BodyControl as error:
                calls.append(error)
            finally:
                calls.append('exit')
        def factory(seconds):
            calls.append(('factory', seconds))
            return manager(seconds)
        class UnavailableConnections:
            def __getitem__(self, alias):
                raise AssertionError('Rejected admission must not resolve a database')
        with patch.object(worker_metrics, 'connections', UnavailableConnections()):
            with controller.record(factory) as token:
                token.retain(True)
                raise first
        controller.finish()
        self.assertEqual(calls, [('factory', 1.25), ('enter', 1.25), first, 'exit'])
        self.assertEqual(controller.counts['fallback_records'], 1)
        self.assertEqual(controller.counts['setup_attempts'], 0)
        self.assertEqual(controller.counts['restore_attempts'], 0)
        self.assertEqual(controller.counts['discard_attempts'], 0)

    def test_rejected_admission_preserves_entry_failure_without_body_or_retry(self):
        controller, calls = self.controller(), []
        first = BodyControl('PRIVATE entry')
        @contextmanager
        def manager(seconds):
            calls.append(seconds)
            raise first
            yield
        with self.assertRaises(BodyControl) as raised:
            with controller.record(manager):
                self.fail('Failed entry must not reach the body')
        self.assertIs(raised.exception, first)
        self.assertEqual(calls, [1.25])
        self.assertEqual(controller.counts['fallback_records'], 1)
        self.assertEqual(controller.counts['setup_attempts'], 0)


@skipUnless(connection.vendor == 'postgresql', 'Private controller requires a real PostgreSQL session')
@override_settings(EVENT_DB_LOCK_TIMEOUT_MS=111)
class PublisherBudgetReusePostgreSQLTests(TransactionTestCase):
    @contextmanager
    def database(self, server_binding, *, connect=True):
        database = connection.copy(alias='default')
        options = dict(database.settings_dict.get('OPTIONS', {}))
        options.pop('pool', None)
        options.update(server_side_binding=server_binding, prepare_threshold=None)
        database.settings_dict['OPTIONS'] = options
        database.settings_dict['AUTOCOMMIT'] = True
        try:
            if connect:
                database.ensure_connection()
                with database.cursor() as cursor:
                    self.assertIs(type(cursor.cursor), ServerBindingCursor if server_binding else Cursor)
                self.seed(database)
            yield database
        finally:
            database.close()

    @contextmanager
    def bind(self, database, mapping=None):
        mapping = {'default': database} if mapping is None else mapping
        with patch.object(worker_metrics, 'connections', mapping), patch('django.db.connection', database):
            yield mapping

    def controller(self):
        # Controller-only mechanics: this does not admit an unknown producer.
        controller = worker_metrics.PublisherBatchBudget(1.25, 3, admission=lambda: True)
        controller.before_deadline()
        return controller

    def seed(self, database, statement='37000', lock='91'):
        with database.cursor() as cursor:
            cursor.execute(SET, [statement, lock])

    def values(self, database):
        # Native reads are outside Django statement-count captures.
        with database.connection.cursor() as cursor:
            cursor.execute(READ)
            return cursor.fetchone()

    def test_consecutive_records_reuse_one_setup_and_restore_nondefault_values(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                before, raw = self.values(database), database.connection
                controller = self.controller()
                with CaptureQueriesContext(database) as queries:
                    for ordinal in range(3):
                        controller.before_deadline()
                        with controller.record(database_statement_budget) as token:
                            self.assertEqual(self.values(database), ('1250ms', '111ms', before[2]))
                            self.assertTrue(database.get_autocommit())
                            self.assertFalse(database.in_atomic_block)
                            token.retain(ordinal < 2)
                    controller.finish()
                    controller.finish()
                self.assertEqual(len(queries), 2)
                self.assertTrue(queries[0]['sql'].startswith('WITH previous AS MATERIALIZED'))
                self.assertTrue(queries[1]['sql'].startswith("SELECT set_config('statement_timeout'"))
                self.assertIs(database.connection, raw)
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts, {
                    'setup_attempts': 1, 'setup_completed': 1, 'reused_records': 2,
                    'restore_attempts': 1, 'restore_completed': 1,
                    'discard_attempts': 0, 'discard_completed': 0,
                    'fallback_records': 0, 'admitted_records': 3})

    def test_first_unopened_record_uses_public_helper_then_next_record_can_admit(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled, connect=False) as database, self.bind(database):
                controller = self.controller()
                self.assertIsNone(database.connection)
                with controller.record(database_statement_budget) as token:
                    self.assertEqual(self.values(database)[:2], ('1250ms', '111ms'))
                    token.retain(True)
                before, raw = self.values(database), database.connection
                self.assertEqual(controller.counts['fallback_records'], 1)
                self.assertEqual(controller.counts['setup_attempts'], 0)
                controller.before_deadline()
                with controller.record(database_statement_budget):
                    self.assertEqual(self.values(database), ('1250ms', '111ms', before[2]))
                self.assertIs(database.connection, raw)
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts['admitted_records'], 1)
                self.assertEqual(controller.counts['setup_completed'], 1)
                self.assertEqual(controller.counts['restore_completed'], 1)

    def test_nested_record_falls_back_without_finishing_outer_budget(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                before = self.values(database)
                controller = self.controller()
                with CaptureQueriesContext(database) as queries:
                    with controller.record(database_statement_budget):
                        expected = ('1250ms', '111ms', before[2])
                        self.assertEqual(self.values(database), expected)
                        with controller.record(database_statement_budget) as token:
                            self.assertEqual(self.values(database), expected)
                            token.retain(True)
                        self.assertEqual(self.values(database), expected)
                self.assertEqual(len(queries), 4)
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts['admitted_records'], 1)
                self.assertEqual(controller.counts['fallback_records'], 1)
                self.assertEqual(controller.counts['setup_completed'], 1)
                self.assertEqual(controller.counts['restore_completed'], 1)

    def test_non_autocommit_session_keeps_public_helper_without_retention(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                before = self.values(database)
                controller = self.controller()
                database.set_autocommit(False)
                try:
                    with controller.record(database_statement_budget) as token:
                        self.assertFalse(database.get_autocommit())
                        self.assertEqual(self.values(database), ('1250ms', '111ms', before[2]))
                        token.retain(True)
                    self.assertEqual(self.values(database), before)
                    self.assertEqual(controller.counts['fallback_records'], 1)
                    self.assertEqual(controller.counts['admitted_records'], 0)
                    self.assertEqual(controller.counts['setup_attempts'], 0)
                finally:
                    database.rollback()
                    database.set_autocommit(True)

    def test_unknown_execute_wrapper_observes_original_two_statement_helper(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                before, seen = self.values(database), []
                controller = self.controller()
                def observe(execute, sql, params, many, context):
                    seen.append(sql)
                    return execute(sql, params, many, context)
                with database.execute_wrapper(observe):
                    with controller.record(database_statement_budget) as token:
                        self.assertEqual(self.values(database), ('1250ms', '111ms', before[2]))
                        token.retain(True)
                self.assertEqual(len(seen), 2)
                self.assertTrue(seen[0].startswith('WITH previous AS MATERIALIZED'))
                self.assertTrue(seen[1].startswith("SELECT set_config('statement_timeout'"))
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts['fallback_records'], 1)
                self.assertEqual(controller.counts['setup_attempts'], 0)

    def test_foreign_record_delegates_without_closing_or_restoring_owner_session(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                before, raw = self.values(database), database.connection
                controller, calls, failures = self.controller(), [], []
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                @contextmanager
                def foreign_factory(seconds):
                    calls.append(('enter', seconds))
                    try:
                        yield
                    finally:
                        calls.append('exit')
                def foreign():
                    try:
                        with controller.record(foreign_factory) as token:
                            calls.append('body')
                            token.retain(True)
                    except BaseException as error:
                        failures.append(error)
                thread = threading.Thread(target=foreign)
                thread.start()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(failures, [])
                self.assertEqual(calls, [('enter', 1.25), 'body', 'exit'])
                self.assertIs(database.connection, raw)
                self.assertFalse(raw.closed)
                self.assertEqual(self.values(database), ('1250ms', '111ms', before[2]))
                controller.finish()
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts['fallback_records'], 1)
                self.assertEqual(controller.counts['discard_attempts'], 0)

    def test_changed_default_wrapper_discards_only_old_session_and_sets_up_new_baseline(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as first, self.database(enabled) as second:
                self.seed(second, '23000', '73')
                old_raw, new_raw, before = first.connection, second.connection, self.values(second)
                mapping = {'default': first}
                with self.bind(first, mapping):
                    controller = self.controller()
                    with controller.record(database_statement_budget) as token:
                        token.retain(True)
                    mapping['default'] = second
                    with patch('django.db.connection', second):
                        controller.before_deadline()
                        with controller.record(database_statement_budget):
                            self.assertEqual(self.values(second), ('1250ms', '111ms', before[2]))
                    self.assertTrue(old_raw.closed)
                    self.assertIs(second.connection, new_raw)
                    self.assertFalse(new_raw.closed)
                    self.assertEqual(self.values(second), before)
                    self.assertEqual(controller.counts['setup_completed'], 2)
                    self.assertEqual(controller.counts['reused_records'], 0)
                    self.assertEqual(controller.counts['discard_completed'], 1)
                    self.assertEqual(controller.counts['restore_completed'], 1)

    def test_reconnected_raw_session_never_receives_old_saved_timeout_values(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                controller, old_raw = self.controller(), database.connection
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                database.close()
                database.ensure_connection()
                self.seed(database, '23000', '73')
                before, replacement = self.values(database), database.connection
                self.assertIsNot(replacement, old_raw)
                controller.before_deadline()
                with controller.record(database_statement_budget):
                    self.assertEqual(self.values(database), ('1250ms', '111ms', before[2]))
                self.assertTrue(old_raw.closed)
                self.assertIs(database.connection, replacement)
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts['setup_completed'], 2)
                self.assertEqual(controller.counts['discard_completed'], 1)
                self.assertEqual(controller.counts['restore_completed'], 1)

    def test_setup_fetch_failure_discards_applied_session_without_body_restore_or_retry(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                controller, raw, entered = self.controller(), database.connection, []
                first = RuntimeError('PRIVATE post-apply fetch')
                cursor_type = raw.cursor_factory
                def fail_fetch(cursor):
                    raise first
                with patch.object(cursor_type, 'fetchone', fail_fetch), CaptureQueriesContext(database) as queries:
                    with self.assertRaises(RuntimeError) as raised:
                        with controller.record(database_statement_budget):
                            entered.append(True)
                self.assertIs(raised.exception, first)
                self.assertEqual(entered, [])
                self.assertEqual(len(queries), 1)
                self.assertTrue(raw.closed)
                self.assertIsNone(database.connection)
                self.assertEqual(controller.counts['setup_attempts'], 1)
                self.assertEqual(controller.counts['setup_completed'], 0)
                self.assertEqual(controller.counts['restore_attempts'], 0)
                self.assertEqual(controller.counts['discard_completed'], 1)

    def test_restore_database_error_discards_without_repeating_body(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                controller, raw, bodies = self.controller(), database.connection, []
                cursor_type, first = raw.cursor_factory, OperationalError('PRIVATE restore')
                original_execute = cursor_type.execute
                def execute(cursor, sql, params=None, **kwargs):
                    if sql.lstrip().startswith("SELECT set_config('statement_timeout'"):
                        raise first
                    return original_execute(cursor, sql, params, **kwargs)
                with patch.object(cursor_type, 'execute', execute):
                    with controller.record(database_statement_budget):
                        bodies.append(True)
                self.assertEqual(bodies, [True])
                self.assertTrue(raw.closed)
                self.assertIsNone(database.connection)
                self.assertEqual(controller.counts['restore_attempts'], 1)
                self.assertEqual(controller.counts['restore_completed'], 0)
                self.assertEqual(controller.counts['discard_completed'], 1)

    def test_ordinary_body_error_wins_over_secondary_restore_error(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                controller, raw = self.controller(), database.connection
                first, secondary = RuntimeError('PRIVATE body'), OSError('PRIVATE cleanup')
                cursor_type, original_execute = raw.cursor_factory, raw.cursor_factory.execute
                def execute(cursor, sql, params=None, **kwargs):
                    if sql.lstrip().startswith("SELECT set_config('statement_timeout'"):
                        raise secondary
                    return original_execute(cursor, sql, params, **kwargs)
                with patch.object(cursor_type, 'execute', execute), self.assertRaises(RuntimeError) as raised:
                    with controller.record(database_statement_budget):
                        raise first
                self.assertIs(raised.exception, first)
                self.assertTrue(raw.closed)
                self.assertEqual(controller.counts['restore_attempts'], 1)
                self.assertEqual(controller.counts['restore_completed'], 0)
                self.assertEqual(controller.counts['discard_completed'], 1)

    def test_business_baseexception_discards_without_restore_sql_and_keeps_identity(self):
        for enabled in (False, True):
            for kind in (BodyControl, OperationDeadlineExceeded):
                with self.subTest(server_side_binding=enabled, control=kind.__name__), self.database(enabled) as database, self.bind(database):
                    controller, raw = self.controller(), database.connection
                    first = kind('PRIVATE control')
                    with CaptureQueriesContext(database) as queries, self.assertRaises(kind) as raised:
                        with controller.record(database_statement_budget) as token:
                            token.retain(True)
                            raise first
                    self.assertIs(raised.exception, first)
                    self.assertEqual(len(queries), 1)
                    self.assertTrue(raw.closed)
                    self.assertEqual(controller.counts['restore_attempts'], 0)
                    self.assertEqual(controller.counts['discard_completed'], 1)

    def test_stop_gap_without_remaining_budget_discards_without_query_or_reconnect(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                controller, raw = self.controller(), database.connection
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                with CaptureQueriesContext(database) as queries:
                    controller.cleanup_gap(0)
                self.assertEqual(len(queries), 0)
                self.assertTrue(raw.closed)
                self.assertIsNone(database.connection)
                self.assertEqual(controller.counts['restore_attempts'], 0)
                self.assertEqual(controller.counts['discard_completed'], 1)

    def test_positive_stop_gap_restores_under_deadline_no_larger_than_remaining(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                before, bounds = self.values(database), []
                controller = self.controller()
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                @contextmanager
                def bounded(seconds):
                    bounds.append(seconds)
                    yield
                with patch.object(worker_metrics, 'operation_deadline', bounded):
                    controller.cleanup_gap(.25)
                self.assertEqual(bounds, [.25])
                self.assertEqual(self.values(database), before)
                self.assertEqual(controller.counts['restore_completed'], 1)
                self.assertEqual(controller.counts['discard_attempts'], 0)

    def test_captured_finish_failure_quarantines_only_old_wrapper_and_preserves_primary(self):
        for enabled in (False, True):
            for kind in (OSError, OperationalError):
                for primary_present in (False, True):
                    with self.subTest(server_side_binding=enabled, finish_error=kind.__name__, primary_present=primary_present), \
                            self.database(enabled) as database, self.database(enabled) as replacement, \
                            self.bind(database) as mapping, publisher_shard_owner(0, 1) as owner:
                        controller, raw = self.controller(), database.connection
                        replacement_raw, replacement_before = replacement.connection, self.values(replacement)
                        owner_raw, owner_pid = owner._raw_connection, owner.backend_pid
                        first, secondary = BodyControl('PRIVATE primary'), kind('PRIVATE finish')
                        with controller.record(database_statement_budget) as token:
                            token.retain(True)
                        native_finish = controller.finish_raw
                        failed_finish = Mock(side_effect=secondary)
                        controller.finish_raw = failed_finish
                        mapping['default'] = replacement
                        try:
                            if primary_present:
                                with self.assertRaises(BodyControl) as raised:
                                    try:
                                        raise first
                                    finally:
                                        controller.cleanup_gap(0, primary=first)
                                self.assertIs(raised.exception, first)
                            else:
                                with self.assertRaises(kind) as raised:
                                    controller.cleanup_gap(0)
                                self.assertIs(raised.exception, secondary)
                            failed_finish.assert_called_once_with()
                            self.assertIsNone(database.connection)
                            self.assertFalse(raw.closed)
                            self.assertIs(replacement.connection, replacement_raw)
                            self.assertFalse(replacement_raw.closed)
                            self.assertEqual(self.values(replacement), replacement_before)
                            owner.assert_owned()
                            self.assertIs(owner._raw_connection, owner_raw)
                            self.assertEqual(owner.backend_pid, owner_pid)
                            self.assertFalse(owner_raw.closed)
                            self.assertEqual(controller.counts['restore_attempts'], 0)
                            self.assertEqual(controller.counts['discard_attempts'], 1)
                            self.assertEqual(controller.counts['discard_completed'], 0)
                            controller.finish()
                            failed_finish.assert_called_once_with()
                        finally:
                            # The injected physical finish deliberately fails;
                            # discharge the original finite native obligation.
                            native_finish()

    def test_same_python_raw_with_replacement_pgconn_finishes_only_captured_handle(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.database(enabled) as replacement, self.bind(database):
                controller, raw = self.controller(), database.connection
                replacement_raw, replacement_before = replacement.connection, self.values(replacement)
                captured_pgconn = raw.pgconn
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                raw.pgconn = replacement_raw.pgconn
                try:
                    with CaptureQueriesContext(database) as queries:
                        controller.finish()
                    self.assertEqual(len(queries), 0)
                    self.assertIs(database.connection, raw)
                    self.assertIs(raw.pgconn, replacement_raw.pgconn)
                    self.assertFalse(raw.closed)
                    self.assertFalse(replacement_raw.closed)
                    self.assertEqual(self.values(database), replacement_before)
                    self.assertEqual(self.values(replacement), replacement_before)
                    self.assertEqual(controller.counts['restore_attempts'], 0)
                    self.assertEqual(controller.counts['discard_attempts'], 1)
                    self.assertEqual(controller.counts['discard_completed'], 1)
                    self.assertEqual(controller.physical_finish_attempts, 1)
                    controller.finish()
                    self.assertEqual(controller.physical_finish_attempts, 1)
                finally:
                    # The replacement still belongs to its original wrapper.
                    raw.pgconn = captured_pgconn

    def test_changed_seconds_or_lock_timeout_ends_retention_before_public_helper(self):
        for enabled in (False, True):
            for changed in ('seconds', 'lock'):
                with self.subTest(server_side_binding=enabled, changed=changed), self.database(enabled) as database, self.bind(database):
                    before, controller = self.values(database), self.controller()
                    with controller.record(database_statement_budget) as token:
                        token.retain(True)
                    controller.before_deadline()
                    if changed == 'seconds':
                        with controller.record(database_statement_budget, seconds=2.0) as token:
                            self.assertEqual(self.values(database), ('2s', '111ms', before[2]))
                            token.retain(True)
                    else:
                        with override_settings(EVENT_DB_LOCK_TIMEOUT_MS=77):
                            with controller.record(database_statement_budget) as token:
                                self.assertEqual(self.values(database), ('1250ms', '77ms', before[2]))
                                token.retain(True)
                    self.assertEqual(self.values(database), before)
                    self.assertEqual(controller.counts['admitted_records'], 1)
                    self.assertEqual(controller.counts['fallback_records'], 1)
                    self.assertEqual(controller.counts['setup_completed'], 1)
                    self.assertEqual(controller.counts['restore_completed'], 1)
                    self.assertEqual(controller.counts['reused_records'], 0)

    def test_expired_cleanup_entry_discards_without_restore_and_preserves_known_primary(self):
        for enabled in (False, True):
            for primary_present in (False, True):
                with self.subTest(server_side_binding=enabled, primary_present=primary_present), self.database(enabled) as database, self.bind(database):
                    controller, raw = self.controller(), database.connection
                    first = RuntimeError('PRIVATE first')
                    control = OperationDeadlineExceeded('PRIVATE cleanup control')
                    with controller.record(database_statement_budget) as token:
                        token.retain(True)
                    @contextmanager
                    def expired(seconds):
                        raise control
                        yield
                    with patch.object(worker_metrics, 'operation_deadline', expired):
                        if primary_present:
                            with self.assertRaises(RuntimeError) as raised:
                                try:
                                    raise first
                                finally:
                                    controller.cleanup_gap(.25, primary=first)
                            self.assertIs(raised.exception, first)
                        else:
                            with self.assertRaises(OperationDeadlineExceeded) as raised:
                                controller.cleanup_gap(.25)
                            self.assertIs(raised.exception, control)
                    self.assertTrue(raw.closed)
                    self.assertEqual(controller.counts['restore_attempts'], 0)
                    self.assertEqual(controller.counts['discard_completed'], 1)

    @skipUnless(hasattr(signal, 'setitimer'), 'Real one-shot POSIX cleanup alarm')
    def test_real_alarm_at_physical_finish_boundary_retries_only_saved_handle_once(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.database(enabled) as replacement, self.bind(database) as mapping:
                previous_handler = signal.getsignal(signal.SIGALRM)
                previous_timer = signal.getitimer(signal.ITIMER_REAL)
                controller, raw = self.controller(), database.connection
                replacement_raw, replacement_before = replacement.connection, self.values(replacement)
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                native_finish, calls, controls = controller.finish_raw, [], []
                def interrupted_finish():
                    calls.append(True)
                    if len(calls) == 1:
                        try:
                            time.sleep(.05)
                        except OperationDeadlineExceeded as error:
                            controls.append(error)
                            raise
                    native_finish()
                controller.finish_raw = interrupted_finish
                mapping['default'] = replacement
                try:
                    with CaptureQueriesContext(database) as queries, self.assertRaises(OperationDeadlineExceeded) as raised:
                        controller.cleanup_gap(.02)
                    self.assertEqual(len(queries), 0)
                    self.assertEqual(calls, [True, True])
                    self.assertEqual(len(controls), 1)
                    self.assertIs(raised.exception, controls[0])
                    self.assertTrue(raw.closed)
                    self.assertIsNone(database.connection)
                    self.assertIs(replacement.connection, replacement_raw)
                    self.assertFalse(replacement_raw.closed)
                    self.assertEqual(self.values(replacement), replacement_before)
                    self.assertEqual(controller.counts['restore_attempts'], 0)
                    self.assertEqual(controller.counts['discard_attempts'], 1)
                    self.assertEqual(controller.counts['discard_completed'], 1)
                    self.assertEqual(controller.physical_finish_attempts, 2)
                    self.assertIs(signal.getsignal(signal.SIGALRM), previous_handler)
                    self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[1], previous_timer[1])
                    if previous_timer[0] == 0:
                        self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)
                finally:
                    native_finish()

    @skipUnless(hasattr(signal, 'setitimer'), 'Real one-shot POSIX cleanup alarm')
    def test_real_alarm_bounds_gap_restore_and_discards_without_retry(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                previous_handler = signal.getsignal(signal.SIGALRM)
                previous_timer = signal.getitimer(signal.ITIMER_REAL)
                controller, raw = self.controller(), database.connection
                with controller.record(database_statement_budget) as token:
                    token.retain(True)
                cursor_type, original_execute = raw.cursor_factory, raw.cursor_factory.execute
                restores = []
                def execute(cursor, sql, params=None, **kwargs):
                    if sql.lstrip().startswith("SELECT set_config('statement_timeout'"):
                        restores.append(True)
                        time.sleep(.05)
                    return original_execute(cursor, sql, params, **kwargs)
                with patch.object(cursor_type, 'execute', execute), self.assertRaises(OperationDeadlineExceeded):
                    controller.cleanup_gap(.02)
                self.assertEqual(restores, [True])
                self.assertTrue(raw.closed)
                self.assertEqual(controller.counts['restore_attempts'], 1)
                self.assertEqual(controller.counts['restore_completed'], 0)
                self.assertEqual(controller.counts['discard_completed'], 1)
                self.assertIs(signal.getsignal(signal.SIGALRM), previous_handler)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[1], previous_timer[1])
                if previous_timer[0] == 0:
                    self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)

    @skipUnless(hasattr(signal, 'setitimer'), 'Real one-shot POSIX alarm')
    def test_real_alarm_during_body_discards_and_restores_previous_timer_handler(self):
        for enabled in (False, True):
            with self.subTest(server_side_binding=enabled), self.database(enabled) as database, self.bind(database):
                previous_handler = signal.getsignal(signal.SIGALRM)
                previous_timer = signal.getitimer(signal.ITIMER_REAL)
                controller, raw = self.controller(), database.connection
                with self.assertRaises(OperationDeadlineExceeded):
                    with operation_deadline(.02):
                        with controller.record(database_statement_budget) as token:
                            token.retain(True)
                            time.sleep(.05)
                self.assertTrue(raw.closed)
                self.assertEqual(controller.counts['restore_attempts'], 0)
                self.assertEqual(controller.counts['discard_completed'], 1)
                self.assertIs(signal.getsignal(signal.SIGALRM), previous_handler)
                self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[1], previous_timer[1])
                if previous_timer[0] == 0:
                    self.assertEqual(signal.getitimer(signal.ITIMER_REAL)[0], 0)
