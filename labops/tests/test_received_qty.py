import uuid
from decimal import Decimal

from django.test import TestCase, override_settings

from labops.inventory.services import post_receipt, reconcile, reverse
from labops.models import (AuditEvent, Batch, Receipt, ReceiptLine,
                           StockBalance, StockMovement)
from labops.purchasing.services import create_receipt, received_qty
from labops.tests.test_acceptance import Fixture


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class ReceivedQuantityTests(Fixture, TestCase):
    def history(self, order_line, quantity, status='POSTED'):
        receipt=Receipt.objects.create(order=order_line.order,receipt_no=uuid.uuid4().hex,status=status)
        batch=Batch.objects.create(item=self.item,batch_no=uuid.uuid4().hex,origin='PURCHASE',unit_cost=Decimal('1.000001'))
        return ReceiptLine.objects.create(receipt=receipt,line_no=1,order_line=order_line,
            batch=batch,warehouse=self.wh,qty=Decimal(quantity))

    def test_empty_history_returns_decimal_zero(self):
        line=self.order().lines.get()
        total=received_qty(line)
        self.assertIs(type(total),Decimal)
        self.assertEqual(total,Decimal(0))

    def test_exact_micro_units_include_only_posted_rows_for_the_order_line(self):
        line=self.order().lines.get()
        self.history(line,'0.000001')
        self.history(line,'12.345678')
        self.history(line,'9.999999','DRAFT')
        self.history(line,'9.999999','REVERSED')
        other=self.order().lines.get()
        self.history(other,'88.888888')
        total=received_qty(line)
        self.assertIs(type(total),Decimal)
        self.assertEqual(total,Decimal('12.345679'))
        self.assertEqual(received_qty(other),Decimal('88.888888'))

    def test_aggregate_can_exceed_an_individual_fixed6_field_bound(self):
        line=self.order(Decimal('999999999999.999999')).lines.get()
        self.history(line,'999999999999.999999')
        self.history(line,'999999999999.999999')
        # A historical inconsistency must remain visible to the existing limit
        # checks; do not truncate/cast SUM(bigint) back to one field's range.
        self.assertEqual(received_qty(line),Decimal('1999999999999.999998'))

    def test_fractional_over_receipt_creation_rolls_back_without_inventory_change(self):
        order=self.order(Decimal('1.000003'))
        first=self.receipt(order,'0.500001')
        post_receipt(self.store,first.pk,{'expected_version':first.version},'sum-first',self.rid)
        before=(Receipt.objects.count(),Batch.objects.count(),AuditEvent.objects.count(),StockMovement.objects.count())
        self.assertBusiness('OVER_RECEIVED',lambda:self.receipt(order,'0.500003'))
        self.assertEqual((Receipt.objects.count(),Batch.objects.count(),AuditEvent.objects.count(),StockMovement.objects.count()),before)
        self.assertEqual(StockBalance.objects.get().on_hand_qty,Decimal('0.500001'))
        self.assertEqual(reconcile(),[])

    def test_posting_revalidates_pending_draft_after_another_receipt_posts(self):
        order=self.order(Decimal('1.000003'))
        first=self.receipt(order,'0.500002')
        second=self.receipt(order,'0.500002')
        post_receipt(self.store,first.pk,{'expected_version':first.version},'sum-first-draft',self.rid)
        before=(AuditEvent.objects.count(),StockMovement.objects.count())
        self.assertBusiness('OVER_RECEIVED',lambda:post_receipt(self.store,second.pk,
            {'expected_version':second.version},'sum-second-draft',self.rid))
        second.refresh_from_db()
        self.assertEqual(second.status,'DRAFT')
        self.assertFalse(StockMovement.objects.filter(receipt=second).exists())
        self.assertEqual((AuditEvent.objects.count(),StockMovement.objects.count()),before)
        self.assertEqual(StockBalance.objects.get().on_hand_qty,Decimal('0.500002'))
        self.assertEqual(reconcile(),[])

    def test_incoming_multi_line_quantities_share_the_existing_remaining_limit(self):
        order=self.order(Decimal('1.000003'))
        first=self.receipt(order,'0.500001')
        post_receipt(self.store,first.pk,{'expected_version':first.version},'sum-multi-first',self.rid)
        order_line=order.lines.get()
        data={'order_id':str(order.pk),'lines':[{'order_line_id':str(order_line.pk),
            'qty':'0.300002','warehouse_id':str(self.wh.pk),'batch_no':'SUM-MULTI-A'},
            {'order_line_id':str(order_line.pk),'qty':'0.300002',
             'warehouse_id':str(self.wh.pk),'batch_no':'SUM-MULTI-B'}]}
        before=(Receipt.objects.count(),Batch.objects.count(),AuditEvent.objects.count())
        self.assertBusiness('OVER_RECEIVED',lambda:create_receipt(self.store,data,self.rid))
        self.assertEqual((Receipt.objects.count(),Batch.objects.count(),AuditEvent.objects.count()),before)
        self.assertEqual(received_qty(order_line),Decimal('0.500001'))

    def test_exact_order_closure_and_receipt_reversal_update_remaining_quantity(self):
        order=self.order(Decimal('0.000003'))
        first=self.receipt(order,'0.000001')
        post_receipt(self.store,first.pk,{'expected_version':first.version},'sum-close-first',self.rid)
        second=self.receipt(order,'0.000002')
        movement=post_receipt(self.store,second.pk,{'expected_version':second.version},'sum-close-second',self.rid)
        order.refresh_from_db()
        self.assertEqual(order.status,'CLOSED')
        self.assertEqual(received_qty(order.lines.get()),Decimal('0.000003'))
        reverse(self.admin,movement.pk,{'reason':'Fractional receipt correction'},'sum-reverse',self.rid)
        order.refresh_from_db(); second.refresh_from_db()
        self.assertEqual(order.status,'CONFIRMED')
        self.assertEqual(second.status,'REVERSED')
        self.assertEqual(received_qty(order.lines.get()),Decimal('0.000001'))
        self.assertEqual(sum(row.on_hand_qty for row in StockBalance.objects.all()),Decimal('0.000001'))
        self.assertEqual(reconcile(),[])
