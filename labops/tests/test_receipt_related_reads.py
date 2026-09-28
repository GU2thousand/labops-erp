"""Receipt creation joins immediate relations without changing its guards."""
from decimal import Decimal
from uuid import uuid4

from django.db import connection
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops.common import BusinessError, obj, snapshot
from labops.models import (AuditEvent, Batch, Item, OrderLine, OutboxEvent,
                           PurchaseOrder, Receipt, ReceiptLine, StockBalance,
                           StockMovement, StockMovementLine, Supplier)
from labops.purchasing import services
from labops.tests.test_acceptance import Fixture


def table_reads(queries, table):
    return [row['sql'] for row in queries
        if row['sql'].lstrip().upper().startswith('SELECT') and f'FROM "{table}"' in row['sql']]


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class ReceiptRelatedReadTests(Fixture, TestCase):
    def data(self, order, line=None, **changes):
        row = {'order_line_id': str((line or order.lines.get()).pk),
            'warehouse_id': str(self.wh.pk), 'qty': '1.000001',
            'batch_no': uuid4().hex}
        row.update(changes)
        return {'order_id': str(order.pk), 'lines': [row]}

    def effects(self):
        return ((Receipt.objects.count(), ReceiptLine.objects.count(), Batch.objects.count(),
            AuditEvent.objects.count(), OutboxEvent.objects.count(),
            StockMovement.objects.count(), StockMovementLine.objects.count()),
            list(StockBalance.objects.order_by('pk').values_list('pk', 'on_hand_qty', 'version')),
            list(PurchaseOrder.objects.order_by('pk').values_list('pk', 'status', 'version')))

    def assert_rejected_without_effects(self, call, code, message, status=422):
        before = self.effects()
        with self.assertRaises(BusinessError) as caught:
            call()
        error = caught.exception
        self.assertEqual((error.code, error.message, error.status), (code, message, status))
        self.assertEqual(self.effects(), before)
        return error

    def test_public_creation_joins_required_relations_without_lazy_reads_or_extra_row_locks(self):
        order = self.order(10)
        data = self.data(order)
        before = (OutboxEvent.objects.count(), StockMovement.objects.count(), StockBalance.objects.count())
        with CaptureQueriesContext(connection) as queries:
            receipt = services.create_receipt(self.store, data, self.rid)
        order_reads = table_reads(queries, 'labops_purchaseorder')
        joined_orders = [sql for sql in order_reads if 'JOIN "labops_supplier"' in sql]
        self.assertEqual(len(joined_orders), 1)
        self.assertNotIn('FOR UPDATE', joined_orders[0])
        order_locks = [sql for sql in order_reads if 'JOIN "labops_supplier"' not in sql]
        self.assertEqual(len(order_locks), 1)
        request_locks = [sql for sql in table_reads(queries, 'labops_purchaserequest')
            if '"labops_purchaserequest"."request_no"' in sql]
        self.assertEqual(len(request_locks), 1)
        if connection.vendor == 'postgresql':
            self.assertIn('FOR UPDATE', order_locks[0])
            self.assertIn('FOR UPDATE', request_locks[0])
        joined_lines = [sql for sql in table_reads(queries, 'labops_orderline')
            if 'JOIN "labops_requestline"' in sql and 'JOIN "labops_item"' in sql]
        self.assertEqual(len(joined_lines), 1)
        self.assertNotIn('FOR UPDATE', joined_lines[0])
        for table in ('labops_supplier', 'labops_requestline', 'labops_item'):
            self.assertEqual(table_reads(queries, table), [])
        # The original exact remaining-quantity aggregate is still executed.
        sums = [sql for sql in table_reads(queries, 'labops_receiptline') if 'SUM(' in sql.upper()]
        self.assertEqual(len(sums), 1)
        fresh = Receipt.objects.get(pk=receipt.pk)
        row = fresh.lines.get()
        self.assertEqual((fresh.status, row.order_line_id, row.qty),
            ('DRAFT', order.lines.get().pk, Decimal('1.000001')))
        self.assertEqual((row.batch.item_id, row.batch.unit_cost), (self.item.pk, Decimal(12)))
        audit = AuditEvent.objects.get(entity_type='receipt', entity_id=receipt.pk, action='LINES_SAVED')
        self.assertEqual(audit.after_json, snapshot(fresh))
        self.assertEqual((OutboxEvent.objects.count(), StockMovement.objects.count(), StockBalance.objects.count()), before)

    def test_related_lookup_and_public_missing_records_keep_obj_errors_and_atomic_rollback(self):
        missing = str(uuid4())
        for model, relations in ((PurchaseOrder, ('supplier',)), (OrderLine, ('request_line__item',))):
            for ident in (None, 'invalid-uuid', missing):
                with self.subTest(model=model.__name__, ident=ident):
                    with self.assertRaises(BusinessError) as original:
                        obj(model, ident)
                    with self.assertRaises(BusinessError) as joined:
                        services._related_obj(model, ident, *relations)
                    expected, actual = original.exception, joined.exception
                    self.assertEqual((actual.code, actual.message, actual.status, actual.field),
                        (expected.code, expected.message, expected.status, expected.field))
        order = self.order(10)
        data = self.data(order)
        message = 'Record not found or no longer available'
        # A malformed order id can fail in the existing ancestor lock lookup;
        # this preserves public 404 checks for absent valid ids and missing ids.
        for ident in (None, missing):
            with self.subTest(order_id=ident):
                self.assert_rejected_without_effects(lambda ident=ident: services.create_receipt(
                    self.store, {**data, 'order_id': ident}, self.rid), 'NOT_FOUND', message, 404)
        for ident in (None, 'invalid-uuid', missing):
            with self.subTest(order_line_id=ident):
                bad = {**data, 'lines': [{**data['lines'][0], 'order_line_id': ident}]}
                self.assert_rejected_without_effects(lambda bad=bad: services.create_receipt(
                    self.store, bad, self.rid), 'NOT_FOUND', message, 404)

    def test_original_authorization_status_supplier_and_wrong_order_before_item_rejections(self):
        order, other = self.order(10), self.order(10)
        data = self.data(order)
        self.assert_rejected_without_effects(lambda: services.create_receipt(self.audit,
            {**data, 'order_id': str(uuid4())}, self.rid), 'FORBIDDEN',
            'Your role does not permit this action', 403)
        Supplier.objects.filter(pk=self.supplier.pk).update(is_active=False)
        PurchaseOrder.objects.filter(pk=order.pk).update(status='DRAFT')
        self.assert_rejected_without_effects(lambda: services.create_receipt(self.store, data, self.rid),
            'ORDER_NOT_CONFIRMED', 'Receipts require a confirmed purchase order')
        PurchaseOrder.objects.filter(pk=order.pk).update(status='CONFIRMED')
        self.assert_rejected_without_effects(lambda: services.create_receipt(self.store,
            {**data, 'lines': [{**data['lines'][0], 'order_line_id': None}]}, self.rid),
            'INACTIVE_SUPPLIER', 'Supplier is inactive')
        Supplier.objects.filter(pk=self.supplier.pk).update(is_active=True)
        Item.objects.filter(pk=self.item.pk).update(is_active=False)
        self.assert_rejected_without_effects(lambda: services.create_receipt(self.store, data, self.rid),
            'INACTIVE_ITEM', 'Item is inactive')
        # This foreign line also has an inactive item and invalid later fields:
        # wrong-order rejection must still precede all of those validations.
        wrong = self.data(order, other.lines.get(), warehouse_id=None, qty='invalid')
        self.assert_rejected_without_effects(lambda: services.create_receipt(self.store, wrong, self.rid),
            'WRONG_ORDER', 'The receipt line does not belong to the selected order')
