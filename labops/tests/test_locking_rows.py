"""The lock-only projection must retain actual PostgreSQL row ownership."""
import json
import os
from pathlib import Path
from queue import Queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest import skipUnless
from unittest.mock import call, patch
from uuid import UUID

from django.db import close_old_connections, connection, connections, transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops import locking, models as m
from labops.inventory import services as inventory
from labops.tests.test_acceptance import Fixture


class ProjectKeys:
    def project_keys(self):
        keys = [UUID(int=1), UUID(int=2)]
        for number, key in enumerate(keys, 1):
            m.Project.objects.create(pk=key, code=f'LOCK-{number}',
                name='Lock projection fixture', owner=self.admin)
        return keys

    def assert_key_select(self, sql, model=m.Project):
        table = model._meta.db_table
        self.assertEqual(sql.split(' FROM ', 1)[0], f'SELECT "{table}"."id" AS "pk"')
        self.assertIn(f'FROM "{table}" WHERE "{table}"."id" IN (', sql)
        self.assertIn(' ORDER BY 1 ASC', sql)
        self.assertNotIn('JOIN', sql)
        if connection.vendor == 'postgresql':
            self.assertTrue(sql.endswith(' FOR UPDATE'), sql)


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class RowLockProjectionTests(ProjectKeys, Fixture, TestCase):
    def test_only_primary_keys_are_selected_without_model_hydration_and_are_eager(self):
        keys = self.project_keys()
        requested = [keys[1], UUID(int=3), None, keys[0], keys[1], False, 0, '']
        with patch.object(m.Project, 'from_db', side_effect=AssertionError('Model hydration')), \
             CaptureQueriesContext(connection) as queries:
            result = locking.rows(m.Project, requested)
            self.assertIs(type(result), list)
            self.assertEqual(result, keys)
            self.assertEqual(len(queries), 1, 'Lock query must execute before rows() returns')
        self.assert_key_select(queries[0]['sql'])

    def test_empty_keys_evaluate_to_empty_without_a_database_query(self):
        with CaptureQueriesContext(connection) as queries:
            self.assertEqual(locking.rows(m.Project, [None, False, 0, '']), [])
        self.assertEqual(len(queries), 0)

    def test_command_locks_retains_receipt_ancestor_hierarchy_and_order(self):
        order = self.order(10)
        request = order.lines.get().request_line.request
        m.PurchaseRequest.objects.filter(pk=request.pk).update(project=self.p)
        receipt = self.receipt(order, 1)
        m.RuntimeState.objects.filter(pk=1).update(opening_closed=True)
        with patch.object(locking, 'rows', wraps=locking.rows) as locked, \
             CaptureQueriesContext(connection) as queries:
            self.assertIsNone(locking.command_locks('post_receipt', {'id': receipt.pk}))
        expected = [(m.Project, self.p.pk), (m.PurchaseRequest, request.pk),
                    (m.PurchaseOrder, order.pk), (m.Receipt, receipt.pk)]
        self.assertEqual(locked.call_args_list, [call(model, {key}) for model, key in expected])
        key_queries = [row['sql'] for row in queries if row['sql'].split(' FROM ', 1)[0]
            in {f'SELECT "{model._meta.db_table}"."id" AS "pk"' for model, _ in expected}]
        self.assertEqual(len(key_queries), len(expected))
        for sql, (model, _) in zip(key_queries, expected):
            self.assert_key_select(sql, model)

    def test_issue_draft_retains_document_route_lock_and_project_task_movement_order(self):
        batch = self.stock(20)
        draft = inventory.issue_draft(self.admin, self.issue_data(batch, 3), self.rid)
        other = inventory.issue_draft(self.admin, self.issue_data(batch, 7), self.rid)
        with patch.object(locking, 'rows', wraps=locking.rows) as locked, \
             patch.object(locking, 'advisory', wraps=locking.advisory) as advisory:
            locking.command_locks('issue_draft', {'id': draft.pk,
                'data': {'draft_id': str(other.pk)}})
        advisory.assert_called_once_with('document:' + str(draft.pk))
        self.assertEqual(locked.call_args_list, [call(m.Project, {self.p.pk}),
            call(m.Task, {self.task.pk, None}), call(m.StockMovement, {draft.pk})])

    def test_queue_import_uses_same_eager_pk_locker(self):
        job = m.ImportJob.objects.create(entity_type='items', mode='CREATE',
            file_name='lock.csv', file_sha256='a' * 64, idempotency_key='lock-import')
        with patch.object(m.ImportJob, 'from_db', side_effect=AssertionError('Model hydration')), \
             CaptureQueriesContext(connection) as queries:
            self.assertIsNone(locking.command_locks('queue_import', {'id': job.pk}))
        self.assertEqual(len(queries), 1)
        self.assert_key_select(queries[0]['sql'], m.ImportJob)


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL row-lock proof')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class PostgreSQLRowLockTests(ProjectKeys, Fixture, TransactionTestCase):
    def check_release(self, *, rollback):
        keys = self.project_keys()
        worker_pid = Queue()
        starting_lock = threading.Event()
        requested = [keys[1], None, UUID(int=3), keys[0], keys[1]]

        def contend():
            close_old_connections()
            try:
                with CaptureQueriesContext(connection) as queries, transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL lock_timeout = '5s'")
                        cursor.execute("SET LOCAL statement_timeout = '6s'")
                        cursor.execute('SELECT pg_backend_pid()')
                        worker_pid.put(cursor.fetchone()[0])
                    starting_lock.set()
                    result = locking.rows(m.Project, requested)
                sql = [row['sql'] for row in queries if 'FROM "labops_project"' in row['sql']]
                return result, sql
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                with connection.cursor() as cursor:
                    cursor.execute('SELECT pg_backend_pid()')
                    holder = cursor.fetchone()[0]
                with CaptureQueriesContext(connection) as queries:
                    self.assertEqual(locking.rows(m.Project, requested), keys)
                self.assertEqual(len(queries), 1)
                self.assert_key_select(queries[0]['sql'])
                held_sql = queries[0]['sql']
                waiting = pool.submit(contend)
                contender = worker_pid.get(timeout=3)
                self.assertNotEqual(holder, contender, 'Proof requires independent backend sessions')
                self.assertTrue(starting_lock.wait(3))
                blockers = []
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT pg_blocking_pids(%s)', [contender])
                        blockers = cursor.fetchone()[0]
                    if holder in blockers:
                        break
                    if waiting.done():
                        waiting.result()  # Preserve an actual worker exception if it failed.
                        self.fail('Contender completed while holder transaction remained open')
                    time.sleep(.01)
                self.assertIn(holder, blockers, 'PostgreSQL must report holder blocking contender')
                self.assertFalse(waiting.done())
                if rollback:
                    transaction.set_rollback(True)
            result, sql = waiting.result(timeout=3)
        self.assertEqual(result, keys)
        self.assertEqual(len(sql), 1)
        self.assert_key_select(sql[0])
        evidence = os.environ.get('LABOPS_LOCK_ROWS_PROOF_EVIDENCE')
        if evidence:
            (Path(evidence) / f'row-lock-{ "rollback" if rollback else "commit" }.json').write_text(
                json.dumps({'holder_backend_pid': holder, 'contender_backend_pid': contender,
                    'observed_blocking_backend_pids': blockers,
                    'holder_sql': held_sql, 'contender_sql': sql[0],
                    'returned_ordered_keys': [str(key) for key in result],
                    'released_by': 'ROLLBACK' if rollback else 'COMMIT'}, indent=2) + '\n')

    def test_same_ordered_keys_block_until_holder_commits(self):
        self.check_release(rollback=False)

    def test_same_ordered_keys_block_until_holder_rolls_back(self):
        self.check_release(rollback=True)
