"""Empty ancestor sets avoid ORM construction; actual reads and locks remain."""
from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import call, patch
from uuid import uuid4

from django.core.exceptions import ValidationError
from django.db import connection, router
from django.db.models import QuerySet
from django.db.models.lookups import In
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops import locking, models as m
from labops.inventory import services as inventory
from labops.purchasing import services as purchasing
from labops.tests.test_acceptance import Fixture
from labops.tests.test_locking_rows import ProjectKeys


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class AncestorQueryConstructionTests(ProjectKeys, Fixture, TestCase):
    @contextmanager
    def no_empty_ancestor_queries(self):
        # Observe the unnecessary work as well as SQL: empty IN querysets send
        # no SQL, so a query-count assertion alone cannot detect this regression.
        lookups = {m.Receipt: 'pk__in', m.OrderLine: 'order_id__in',
            m.PurchaseRequest: 'pk__in', m.Task: 'pk__in'}
        built, evaluated = [], []
        original_filter, original_iter = QuerySet.filter, QuerySet.__iter__

        def filtered(queryset, *args, **kwargs):
            key = lookups.get(queryset.model)
            if key in kwargs and isinstance(kwargs[key], set) and not kwargs[key]:
                built.append((queryset.model.__name__, key))
            return original_filter(queryset, *args, **kwargs)

        def iterated(queryset):
            if queryset.model in lookups:
                for lookup in queryset.query.where.children:
                    if isinstance(lookup, In) and isinstance(lookup.rhs, (list, tuple, set)) and not lookup.rhs:
                        evaluated.append(queryset.model.__name__)
            return original_iter(queryset)

        with patch.object(QuerySet, 'filter', filtered), patch.object(QuerySet, '__iter__', iterated):
            yield
        self.assertEqual(built, [], 'Empty ancestor filters must not be constructed')
        self.assertEqual(evaluated, [], 'Empty ancestor querysets must not be evaluated')

    def reads(self, queries, model):
        return [row['sql'] for row in queries if row['sql'].startswith('SELECT ')
            and f'FROM "{model._meta.db_table}"' in row['sql']]

    def project_order(self):
        order = self.order(10)
        request = order.lines.get().request_line.request
        m.PurchaseRequest.objects.filter(pk=request.pk).update(project=self.p)
        return order, request

    def test_public_transfer_has_no_document_ancestor_reads_and_keeps_atomic_effects(self):
        batch = self.stock(10)
        data = {'batch_id': str(batch.pk), 'from_warehouse_id': str(self.wh.pk),
            'to_warehouse_id': str(self.wh2.pk), 'qty': '1.000001'}
        with self.no_empty_ancestor_queries(), patch.object(locking, 'rows', wraps=locking.rows) as locked, \
                CaptureQueriesContext(connection) as queries:
            movement = inventory.transfer(self.store, data, 'ancestor-transfer', self.rid)
        self.assertEqual(locked.call_args_list, [])
        for model in (m.Receipt, m.OrderLine, m.PurchaseRequest, m.Task, m.Project):
            self.assertEqual(self.reads(queries, model), [])
        self.assertEqual(m.StockBalance.objects.get(batch=batch, warehouse=self.wh).on_hand_qty,
            Decimal('8.999999'))
        self.assertEqual(m.StockBalance.objects.get(batch=batch, warehouse=self.wh2).on_hand_qty,
            Decimal('1.000001'))
        self.assertEqual(m.AuditEvent.objects.filter(entity_id=movement.pk, action='POST').count(), 1)
        self.assertEqual(m.OutboxEvent.objects.filter(aggregate_id=movement.pk).count(), 1)
        self.assertEqual(inventory.reconcile(), [])

    def test_public_issue_retains_project_task_lookups_locks_and_opening_gate(self):
        batch = self.stock(10)
        self.assertFalse(m.RuntimeState.objects.get(pk=1).opening_closed)
        with self.no_empty_ancestor_queries(), patch.object(locking, 'rows', wraps=locking.rows) as locked, \
                CaptureQueriesContext(connection) as queries:
            movement = inventory.issue(self.store, self.issue_data(batch, '1.000001'),
                'ancestor-issue', self.rid)
        self.assertEqual(locked.call_args_list, [call(m.Project, {self.p.pk}), call(m.Task, {str(self.task.pk)})])
        for model in (m.Receipt, m.OrderLine, m.PurchaseRequest):
            self.assertEqual(self.reads(queries, model), [])
        project_sql = self.reads(queries, m.Project)
        self.assertEqual(len(project_sql), 1)
        self.assert_key_select(project_sql[0], m.Project)
        task_sql = self.reads(queries, m.Task)
        self.assertEqual(len(task_sql), 3)  # Parent-key lookup, key lock, validated joined read.
        self.assertTrue(any(sql.startswith('SELECT "labops_task"."project_id"') for sql in task_sql))
        self.assertTrue(any('JOIN "labops_project"' in sql for sql in task_sql))
        self.assertTrue(m.RuntimeState.objects.get(pk=1).opening_closed)
        self.assertEqual(movement.status, 'POSTED')
        self.assertEqual(m.StockBalance.objects.get(batch=batch, warehouse=self.wh).on_hand_qty,
            Decimal('8.999999'))
        self.assertEqual(inventory.reconcile(), [])

    def test_receipt_creation_and_posting_follow_newly_discovered_ancestors_in_order(self):
        order, request = self.project_order()
        data = {'order_id': str(order.pk), 'lines': [{'order_line_id': str(order.lines.get().pk),
            'warehouse_id': str(self.wh.pk), 'qty': '1.000001', 'batch_no': uuid4().hex}]}
        with self.no_empty_ancestor_queries(), patch.object(locking, 'rows', wraps=locking.rows) as locked:
            receipt = purchasing.create_receipt(self.store, data, self.rid)
        parent_locks = [call(m.Project, {self.p.pk}), call(m.PurchaseRequest, {request.pk})]
        self.assertEqual(locked.call_args_list, parent_locks + [call(m.PurchaseOrder, {str(order.pk)})])
        with self.no_empty_ancestor_queries(), patch.object(locking, 'rows', wraps=locking.rows) as locked, \
                CaptureQueriesContext(connection) as queries:
            movement = inventory.post_receipt(self.store, receipt.pk,
                {'expected_version': receipt.version}, 'ancestor-receipt', self.rid)
        self.assertEqual(locked.call_args_list, parent_locks + [call(m.PurchaseOrder, {order.pk}),
            call(m.Receipt, {receipt.pk})])
        sql = [row['sql'] for row in queries]
        receipt_parent = next(i for i, value in enumerate(sql)
            if value.startswith('SELECT "labops_receipt"."order_id"'))
        request_parent = next(i for i, value in enumerate(sql)
            if 'FROM "labops_orderline"' in value and 'JOIN "labops_requestline"' in value)
        project_parent = next(i for i, value in enumerate(sql)
            if value.startswith('SELECT "labops_purchaserequest"."project_id"'))
        self.assertLess(receipt_parent, request_parent)
        self.assertLess(request_parent, project_parent)
        for model in (m.Project, m.PurchaseRequest, m.PurchaseOrder, m.Receipt):
            key_sql = [value for value in self.reads(queries, model)
                if value.split(' FROM ', 1)[0] == f'SELECT "{model._meta.db_table}"."id" AS "pk"']
            self.assertEqual(len(key_sql), 1)
            self.assert_key_select(key_sql[0], model)
        receipt.refresh_from_db()
        self.assertEqual(receipt.status, 'POSTED')
        self.assertEqual(movement.receipt_id, receipt.pk)
        self.assertEqual(m.StockBalance.objects.get(batch=receipt.lines.get().batch).on_hand_qty,
            Decimal('1.000001'))
        self.assertEqual(inventory.reconcile(), [])

    def test_missing_nonempty_receipt_still_invokes_managers_routers_and_key_lookup(self):
        missing = uuid4()
        m.RuntimeState.objects.filter(pk=1).update(opening_closed=True)
        with self.no_empty_ancestor_queries(), patch.object(m.Receipt.objects, 'filter',
                wraps=m.Receipt.objects.filter) as filtered, \
                patch.object(router, 'db_for_read', return_value=connection.alias) as read_route, \
                patch.object(router, 'db_for_write', return_value=connection.alias) as write_route, \
                patch.object(locking, 'rows', wraps=locking.rows) as locked, \
                CaptureQueriesContext(connection) as queries:
            self.assertIsNone(locking.command_locks('post_receipt', {'id': missing}))
        self.assertEqual(filtered.call_args_list, [call(pk__in={missing}), call(pk__in={missing})])
        self.assertEqual(locked.call_args_list, [call(m.Receipt, {missing})])
        self.assertTrue(any(item.args[0] is m.Receipt for item in read_route.call_args_list))
        self.assertTrue(any(item.args[0] is m.Receipt for item in write_route.call_args_list))
        receipt_sql = self.reads(queries, m.Receipt)
        self.assertEqual(len(receipt_sql), 2)
        self.assertTrue(receipt_sql[0].startswith('SELECT "labops_receipt"."order_id"'))
        self.assert_key_select(receipt_sql[1], m.Receipt)
        for model in (m.OrderLine, m.PurchaseRequest, m.Task, m.Project):
            self.assertEqual(self.reads(queries, model), [])

    def test_nonempty_malformed_ids_retain_original_queryset_preparation_errors(self):
        m.RuntimeState.objects.filter(pk=1).update(opening_closed=True)
        cases = [('post_receipt', {'id': 'invalid-uuid'}, m.Receipt, 'pk__in'),
            ('create_receipt', {'data': {'order_id': 'invalid-uuid'}}, m.OrderLine, 'order_id__in'),
            ('issue', {'data': {'task_id': 'invalid-uuid'}}, m.Task, 'pk__in')]
        for command, values, model, lookup in cases:
            with self.subTest(command=command):
                with self.assertRaises(ValidationError) as original:
                    model.objects.filter(**{lookup: {'invalid-uuid'}})
                with patch.object(model.objects, 'filter', wraps=model.objects.filter) as filtered:
                    with self.assertRaises(ValidationError) as current:
                        locking.command_locks(command, values)
                self.assertEqual(current.exception.messages, original.exception.messages)
                self.assertEqual(current.exception.code, original.exception.code)
                self.assertEqual(current.exception.params, original.exception.params)
                filtered.assert_called_once_with(**{lookup: {'invalid-uuid'}})

    def test_false_document_id_still_uses_original_uuid_lookup(self):
        m.RuntimeState.objects.filter(pk=1).update(opening_closed=True)
        for ident in (False, 0):
            with self.subTest(ident=ident), \
                    patch.object(m.Receipt.objects, 'filter', wraps=m.Receipt.objects.filter) as filtered, \
                    CaptureQueriesContext(connection) as queries:
                locking.command_locks('post_receipt', {'id': ident})
                receipt_sql = self.reads(queries, m.Receipt)
                self.assertEqual(len(receipt_sql), 1)
                self.assertTrue(receipt_sql[0].startswith('SELECT "labops_receipt"."order_id"'))
                # Ancestor lookup keeps False/0; only the unchanged rows() helper
                # drops falsy keys when it is subsequently asked to lock them.
                self.assertEqual(filtered.call_args_list, [call(pk__in={ident}), call(pk__in=set())])
        with self.assertRaises(ValidationError):
            locking.command_locks('post_receipt', {'id': ''})

    def test_null_document_ids_skip_only_ancestor_queries_with_no_database_effect(self):
        m.RuntimeState.objects.filter(pk=1).update(opening_closed=True)
        cases = [('create_receipt', {'data': {'order_id': None}}),
            ('post_receipt', {'id': None}), ('issue', {'data': {'task_id': None}}),
            ('transfer', {'data': {}})]
        for command, values in cases:
            with self.subTest(command=command), self.no_empty_ancestor_queries(), \
                    patch.object(locking, 'rows', wraps=locking.rows) as locked, \
                    CaptureQueriesContext(connection) as queries:
                locking.command_locks(command, values)
                self.assertEqual(locked.call_args_list, [])
                for model in (m.Receipt, m.OrderLine, m.PurchaseRequest, m.Task, m.Project):
                    self.assertEqual(self.reads(queries, model), [])

    def test_all_seven_lock_models_still_select_only_ordered_existing_keys(self):
        order, request = self.project_order()
        receipt = self.receipt(order, 1)
        movement = inventory.issue(self.store, self.issue_data(self.stock(10), 1), 'all-lock-models', self.rid)
        job = m.ImportJob.objects.create(entity_type='items', mode='CREATE', file_name='lock.csv',
            file_sha256='a' * 64, idempotency_key='ancestor-import')
        records = [self.p, self.task, request, order, receipt, movement, job]
        with CaptureQueriesContext(connection) as queries:
            for record in records:
                model = type(record)
                with patch.object(model, 'from_db', side_effect=AssertionError('Lock-only model hydration')):
                    self.assertEqual(locking.rows(model, [uuid4(), record.pk, record.pk, None]), [record.pk])
        self.assertEqual(len(queries), len(records))
        for row, record in zip(queries, records):
            self.assert_key_select(row['sql'], type(record))
