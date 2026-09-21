import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import skipUnless
from unittest.mock import patch
from django.db import connection, connections, close_old_connections, transaction
from django.test import TransactionTestCase, override_settings
from labops.tests.test_acceptance import Fixture
from labops.common import BusinessError
from labops.models import *
from labops.inventory.services import issue, reverse, post_receipt, transfer, reconcile
from labops.purchasing.services import request_action
from labops.projects.services import task_action
from labops.events import process_envelope, envelope

@skipUnless(connection.vendor == 'postgresql', 'PostgreSQL concurrency acceptance')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class PostgreSQLConcurrencyTests(Fixture, TransactionTestCase):
    def race(self, functions):
        barrier = threading.Barrier(len(functions))
        def run(fn):
            close_old_connections()
            try:
                barrier.wait(timeout=10)
                return fn()
            except BusinessError as exc: return exc.code
            finally: connections.close_all()
        with ThreadPoolExecutor(max_workers=len(functions)) as pool:
            return list(pool.map(run, functions))
    def batch(self, quantity=10):
        with transaction.atomic(): return self.stock(quantity)
    def test_issue_8_and_6(self):
        batch = self.batch()
        results = self.race([lambda: issue(self.store,self.issue_data(batch,8),'a',self.rid),
                             lambda: issue(self.store,self.issue_data(batch,6),'b',self.rid)])
        self.assertEqual(results.count('INSUFFICIENT_STOCK'),1)
        self.assertIn(StockBalance.objects.get(batch=batch).on_hand_qty,[2,4]); self.assertEqual(reconcile(),[])
    def test_duplicate_key_one_mutation(self):
        batch=self.batch(100)
        results=self.race([lambda:issue(self.store,self.issue_data(batch,8),'same',self.rid) for _ in range(8)])
        self.assertEqual(len({x.pk for x in results}),1)
        self.assertEqual(StockBalance.objects.get(batch=batch).on_hand_qty,92)
        self.assertEqual(StockMovement.objects.filter(type='ISSUE').count(),1)
    def test_key_different_payload_conflicts(self):
        batch=self.batch(100)
        results=self.race([lambda:issue(self.store,self.issue_data(batch,8),'same',self.rid),
                           lambda:issue(self.store,self.issue_data(batch,6),'same',self.rid)])
        self.assertEqual(results.count('IDEMPOTENCY_CONFLICT'),1);self.assertEqual(reconcile(),[])
    def test_api_duplicate_returns_same_result(self):
        batch=self.batch(100); clients=[self.client_for(self.store) for _ in range(4)]
        results=self.race([lambda c=c:self.api_post(c,'stock/issues',self.issue_data(batch,8),'api-same') for c in clients])
        self.assertEqual([r.status_code for r in results],[200]*4)
        self.assertEqual(len({r.json()['data']['id'] for r in results}),1)
        self.assertEqual(StockMovement.objects.filter(idempotency_key='api-same').count(),1)
        self.assertEqual(reconcile(),[])
    def test_approval_race(self):
        request=self.pr()
        data={'expected_version':request.version,'decision':'APPROVE','reason':'Reviewed'}
        result=self.race([lambda:request_action(self.admin,request.id,'decision',data,self.rid),
                          lambda:request_action(self.other,request.id,'decision',data,self.rid)])
        self.assertEqual(result.count('VERSION_CONFLICT'),1)
        self.assertEqual(AuditEvent.objects.filter(entity_id=request.id,action='DECISION').count(),1)
    def test_two_receipts_cannot_exceed_order(self):
        order=self.order(100); a=self.receipt(order,60); b=self.receipt(order,60)
        result=self.race([lambda:post_receipt(self.store,a.id,{'expected_version':a.version},'a',self.rid),
                          lambda:post_receipt(self.store,b.id,{'expected_version':b.version},'b',self.rid)])
        self.assertEqual(result.count('OVER_RECEIVED'),1)
        self.assertEqual(Receipt.objects.filter(status='POSTED').count(),1);self.assertEqual(reconcile(),[])
    def test_duplicate_receipt_different_keys(self):
        order=self.order(); receipt=self.receipt(order,60)
        result=self.race([lambda:post_receipt(self.store,receipt.id,{'expected_version':receipt.version},'a',self.rid),
                          lambda:post_receipt(self.store,receipt.id,{'expected_version':receipt.version},'b',self.rid)])
        self.assertEqual(len({x.pk for x in result}),1);self.assertEqual(reconcile(),[])
    def test_reversal_once_under_race(self):
        batch=self.batch(); original=issue(self.store,self.issue_data(batch,8),'issue',self.rid)
        result=self.race([lambda:reverse(self.admin,original.id,{'reason':'fix'},'a',self.rid),
                          lambda:reverse(self.admin,original.id,{'reason':'fix'},'b',self.rid)])
        self.assertEqual(result.count('ALREADY_REVERSED'),1)
        self.assertEqual(StockBalance.objects.get(batch=batch).on_hand_qty,10);self.assertEqual(reconcile(),[])
    def test_task_close_against_issue(self):
        batch=self.batch()
        result=self.race([lambda:issue(self.store,self.issue_data(batch,8),'issue',self.rid),
                          lambda:task_action(self.admin,self.task.id,'transition',{'expected_version':self.task.version,'target_status':'DONE'},self.rid)])
        self.assertEqual(reconcile(),[])
        self.task.refresh_from_db();self.assertEqual(self.task.status,'DONE')
        self.assertIn(StockBalance.objects.get(batch=batch).on_hand_qty,[2,10])
        self.assertFalse(any(isinstance(x,str) and x!='TASK_NOT_ACTIVE' for x in result))
    def test_reverse_direction_transfers_and_new_balance(self):
        batch=self.batch(100)
        data={'batch_id':str(batch.id),'from_warehouse_id':str(self.wh.id),'to_warehouse_id':str(self.wh2.id),'qty':10}
        results=self.race([lambda:transfer(self.store,data,'a',self.rid),lambda:transfer(self.store,data,'b',self.rid)])
        self.assertTrue(all(isinstance(x,StockMovement) for x in results))
        self.assertEqual(StockBalance.objects.get(batch=batch,warehouse=self.wh2).on_hand_qty,20)
        self.assertEqual(reconcile(),[])
    def test_independent_transfers_progress_while_other_transaction_open(self):
        a=self.batch(100);b=self.batch(100)
        entered=threading.Event();release=threading.Event();real_save=StockBalance.save
        def save(record,*args,**kwargs):
            if record.batch_id==a.id and record.on_hand_qty==90:
                entered.set()
                if not release.wait(10): raise RuntimeError('Timed out waiting for independent write')
            return real_save(record,*args,**kwargs)
        def run(batch,key):
            close_old_connections()
            try:return transfer(self.store,{'batch_id':str(batch.id),'from_warehouse_id':str(self.wh.id),'to_warehouse_id':str(self.wh2.id),'qty':10},key,self.rid)
            finally:connections.close_all()
        with patch.object(StockBalance,'save',save),ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(run,a,'a')
            try:
                self.assertTrue(entered.wait(5))
                second=pool.submit(run,b,'b');self.assertIsInstance(second.result(timeout=3),StockMovement)
            finally:release.set()
            first.result(timeout=5)
        self.assertEqual(reconcile(),[])
    def test_consumer_duplicate_race(self):
        batch=self.batch(10); event=OutboxEvent.objects.get(aggregate_id=StockMovement.objects.get(type='OPENING').id)
        results=self.race([lambda:process_envelope('analytics',envelope(event)) for _ in range(6)])
        self.assertEqual(results.count(True),1)
        self.assertEqual(InventoryProjection.objects.get(batch=batch).quantity,10)
    def test_database_connection_loss_rolls_back_entire_post(self):
        import psycopg
        from django.db import OperationalError
        batch=self.batch(100);before=OutboxEvent.objects.count()
        def disconnect(*args):
            backend=connection.connection.info.backend_pid
            params=connection.get_connection_params();params.pop('context',None);params.pop('cursor_factory',None)
            with psycopg.connect(**params,autocommit=True) as admin:
                admin.execute('SELECT pg_terminate_backend(%s)',[backend])
            with connection.cursor() as cursor:cursor.execute('SELECT 1')
        try:
            with patch('labops.events.emit_inventory',side_effect=disconnect):
                with self.assertRaises(OperationalError):issue(self.store,self.issue_data(batch,8),'disconnect',self.rid)
        finally:connection.close()
        self.assertEqual(StockBalance.objects.get(batch=batch).on_hand_qty,100)
        self.assertEqual(OutboxEvent.objects.count(),before)
        self.assertFalse(StockMovement.objects.filter(type='ISSUE').exists());self.assertEqual(reconcile(),[])
    def test_actual_opposing_transfers_lock_balances_in_same_order(self):
        batch=self.batch(100)
        forward={'batch_id':str(batch.id),'from_warehouse_id':str(self.wh.id),'to_warehouse_id':str(self.wh2.id),'qty':20}
        transfer(self.store,forward,'fund-target',self.rid)
        backward={**forward,'from_warehouse_id':str(self.wh2.id),'to_warehouse_id':str(self.wh.id),'qty':10}
        results=self.race([lambda:transfer(self.store,forward,'outbound',self.rid),lambda:transfer(self.store,backward,'inbound',self.rid)])
        self.assertTrue(all(isinstance(x,StockMovement) for x in results))
        self.assertEqual(StockBalance.objects.get(batch=batch,warehouse=self.wh).on_hand_qty,70)
        self.assertEqual(StockBalance.objects.get(batch=batch,warehouse=self.wh2).on_hand_qty,30)
        self.assertEqual(reconcile(),[])
