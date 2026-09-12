import uuid, json
from decimal import Decimal
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch
from django.test import TestCase,TransactionTestCase,Client
from django.db import connections,close_old_connections,transaction
from django.contrib.auth.models import Group
from django.utils import timezone
from labops.models import *
from labops.common import BusinessError
from labops.catalog.services import write_master
from labops.projects.services import write_project,project_action,write_task,task_action
from labops.purchasing.services import *
from labops.inventory.services import *
from labops.operations.services import *
from labops.queries import costs,inventory_overview

class Fixture:
    def setUp(self):
        RuntimeState.objects.get_or_create(pk=1)
        self.rid='acceptance'; self.today=timezone.localdate()
        self.admin=self.user('admin','ADMIN');self.other=self.user('other','ADMIN');self.buyer=self.user('buyer','BUYER');self.store=self.user('store','STORE');self.tech=self.user('tech','TECH');self.audit=self.user('auditor','AUDITOR')
        self.item=write_master(self.admin,'items',dict(code='reagent-a',name='Test Reagent',base_uom='KIT',reorder_qty=10),self.rid)
        self.wh=write_master(self.admin,'warehouses',dict(code='W1',name='Main'),self.rid)
        self.wh2=write_master(self.admin,'warehouses',dict(code='W2',name='Target'),self.rid)
        self.supplier=write_master(self.admin,'suppliers',dict(code='S1',name='Fictional Supplier'),self.rid)
        self.p=write_project(self.admin,dict(code='P1',name='Project',owner_id=str(self.admin.id)),self.rid)
        self.p=project_action(self.admin,self.p.id,'members',dict(expected_version=self.p.version,user_id=str(self.tech.id)),self.rid)
        self.p=project_action(self.admin,self.p.id,'transition',dict(expected_version=self.p.version,target_status='ACTIVE'),self.rid)
        self.task=write_task(self.admin,dict(project_id=str(self.p.id),title='Task',assignee_id=str(self.tech.id)),self.rid)
        self.task=task_action(self.admin,self.task.id,'transition',dict(expected_version=self.task.version,target_status='IN_PROGRESS'),self.rid)
    def user(self,name,role):
        u=User.objects.create_user(username=name+'@test.local',email=name+'@test.local',name=name,password='TestSecure!2026');u.groups.add(Group.objects.get_or_create(name=role)[0]);return u
    def pr(self,q=100,creator=None):
        creator=creator or self.buyer
        r=write_request(creator,dict(reason='Test purchase',lines=[dict(item_id=str(self.item.id),qty=q,needed_by=str(self.today))]),self.rid)
        return request_action(creator,r.id,'submit',dict(expected_version=r.version),self.rid)
    def approved(self,q=100):
        r=self.pr(q);return request_action(self.admin,r.id,'decision',dict(expected_version=r.version,decision='APPROVE',reason='Approved'),self.rid)
    def order(self,q=100,pr=None):
        r=pr or self.approved(q)
        o=write_order(self.buyer,dict(supplier_id=str(self.supplier.id),lines=[dict(request_line_id=str(r.lines.first().id),qty=q,unit_price=12)]),self.rid)
        return order_action(self.buyer,o.id,'confirm',dict(expected_version=o.version),self.rid)
    def receipt(self,order,q,batch=None):
        return create_receipt(self.store,dict(order_id=str(order.id),lines=[dict(order_line_id=str(order.lines.first().id),qty=q,warehouse_id=str(self.wh.id),batch_no=batch or uuid.uuid4().hex,expires_on=str(self.today+timedelta(days=30)))]),self.rid)
    def stock(self,q=10,expired=False):
        b=Batch.objects.create(item=self.item,batch_no=uuid.uuid4().hex,origin='OPENING',unit_cost=Decimal('12'),expires_on=self.today-timedelta(days=1) if expired else self.today+timedelta(days=30))
        post(self.admin,'OPENING',[line(b,self.wh,Decimal(q))],{},str(uuid.uuid4()),self.rid)
        return b
    def issue_data(self,b,q=7):return dict(task_id=str(self.task.id),lines=[dict(batch_id=str(b.id),warehouse_id=str(self.wh.id),qty=q)])
    def assertBusiness(self,code,fn):
        with self.assertRaises(BusinessError) as ctx: fn()
        self.assertEqual(ctx.exception.code,code)
    def client_for(self,user,csrf=False):
        c=Client(enforce_csrf_checks=csrf);c.force_login(user);return c
    def api_post(self,c,path,body,key=None):return c.post('/api/v1/'+path,json.dumps(body),content_type='application/json',HTTP_IDEMPOTENCY_KEY=key or str(uuid.uuid4()))

class AcceptanceTests(Fixture,TestCase):
    def test_A01_normalized_duplicate(self):
        self.assertBusiness('DUPLICATE_CODE',lambda:write_master(self.admin,'items',dict(code=' REAGENT-A ',name='Duplicate',base_uom='EA'),self.rid))
        self.assertEqual(Item.objects.count(),1)
    def test_A02_self_approval_denied(self):
        r=self.pr(100,self.admin)
        self.assertBusiness('SELF_APPROVAL_DENIED',lambda:request_action(self.admin,r.id,'decision',dict(expected_version=r.version,decision='APPROVE',reason='self'),self.rid))
        r.refresh_from_db();self.assertEqual(r.status,'SUBMITTED')
    def test_A04_over_receipt_rolls_back(self):
        o=self.order(); r=self.receipt(o,60);post_receipt(self.store,r.id,dict(expected_version=r.version),'receive-60',self.rid)
        before=AuditEvent.objects.count()
        self.assertBusiness('OVER_RECEIVED',lambda:self.receipt(o,50))
        self.assertEqual(StockBalance.objects.get(batch=r.lines.first().batch,warehouse=self.wh).on_hand_qty,60)
        self.assertEqual(AuditEvent.objects.count(),before)
    def test_A05_receipt_retry_with_same_and_new_keys(self):
        o=self.order();r=self.receipt(o,60);data=dict(expected_version=r.version)
        m=post_receipt(self.store,r.id,data,'stable',self.rid)
        self.assertEqual(post_receipt(self.store,r.id,data,'stable',self.rid).id,m.id)
        self.assertEqual(post_receipt(self.store,r.id,data,'new-key',self.rid).id,m.id)
        self.assertEqual(StockMovement.objects.filter(type='RECEIPT').count(),1)
        self.assertEqual(StockBalance.objects.get().on_hand_qty,60)
        self.assertBusiness('IDEMPOTENCY_CONFLICT',lambda:post_receipt(self.store,r.id,dict(expected_version=999),'stable',self.rid))
    def test_A07_transfer_atomic_on_destination_failure(self):
        b=self.stock(10);before=StockMovement.objects.count();real=StockBalance.save
        def broken(record,*a,**kw):
            if record.warehouse_id==self.wh2.id and record.on_hand_qty>0: raise RuntimeError('Simulated target write failure')
            return real(record,*a,**kw)
        with patch.object(StockBalance,'save',broken):
            with self.assertRaises(RuntimeError): transfer(self.store,dict(batch_id=str(b.id),from_warehouse_id=str(self.wh.id),to_warehouse_id=str(self.wh2.id),qty=3),'transfer-fail',self.rid)
        self.assertEqual(StockBalance.objects.get(batch=b,warehouse=self.wh).on_hand_qty,10)
        self.assertFalse(StockBalance.objects.filter(batch=b,warehouse=self.wh2).exists())
        self.assertEqual(StockMovement.objects.count(),before)
        self.assertEqual(reconcile(),[])
    def test_A08_expired_and_closed_task_denied(self):
        expired=self.stock(10,True)
        self.assertBusiness('BATCH_EXPIRED',lambda:issue(self.store,self.issue_data(expired),'expired',self.rid))
        b=self.stock(10);task_action(self.admin,self.task.id,'transition',dict(expected_version=self.task.version,target_status='DONE'),self.rid)
        self.assertBusiness('TASK_NOT_ACTIVE',lambda:issue(self.store,self.issue_data(b),'closed-task',self.rid))
        self.assertEqual(sum(x.on_hand_qty for x in StockBalance.objects.all()),20)
    def test_A09_insufficient_receipt_reversal(self):
        o=self.order();r=self.receipt(o,60);m=post_receipt(self.store,r.id,dict(expected_version=r.version),'receive',self.rid)
        issue(self.store,self.issue_data(r.lines.first().batch,10),'issue',self.rid)
        self.assertBusiness('INSUFFICIENT_STOCK',lambda:reverse(self.admin,m.id,dict(reason='return'),'reverse',self.rid))
        r.refresh_from_db();self.assertEqual(r.status,'POSTED');self.assertEqual(StockBalance.objects.get().on_hand_qty,50)
    def test_A10_reversal_once(self):
        b=self.stock(10);m=issue(self.store,self.issue_data(b,7),'use',self.rid)
        reverse(self.admin,m.id,dict(reason='correction'),'undo',self.rid)
        self.assertBusiness('ALREADY_REVERSED',lambda:reverse(self.admin,m.id,dict(reason='again'),'undo2',self.rid))
        self.assertEqual(StockBalance.objects.get().on_hand_qty,10);self.assertEqual(Decimal(costs(self.admin)[0]['material_cost']),0)
    def test_A11_import_resume_skips_succeeded(self):
        raw=b'code,name,base_uom,reorder_qty\nNEW1,One,EA,0\nNEW2,Two,KIT,3\n'
        job=import_preview(self.admin,'items','CREATE','items.csv',raw,{},'job1',self.rid)
        from labops.operations import services as ops
        real=ops.write_master;calls=[0]
        def interrupted(*a,**kw):
            calls[0]+=1
            if calls[0]==2: raise RuntimeError('Simulate worker crash')
            return real(*a,**kw)
        with patch.object(ops,'write_master',interrupted):
            with self.assertRaises(RuntimeError): execute_import(self.admin,job.id,self.rid)
        self.assertEqual(job.rows.filter(status='SUCCEEDED').count(),1)
        execute_import(self.admin,job.id,self.rid);execute_import(self.admin,job.id,self.rid)
        self.assertEqual(Item.objects.filter(code__in=['NEW1','NEW2']).count(),2)
        self.assertEqual(job.rows.filter(status='SUCCEEDED').count(),2)
    def test_A12_optimistic_task_version(self):
        old=self.task.version
        task_action(self.admin,self.task.id,'transition',dict(expected_version=old,target_status='BLOCKED',reason='wait'),self.rid)
        self.assertBusiness('VERSION_CONFLICT',lambda:task_action(self.admin,self.task.id,'transition',dict(expected_version=old,target_status='DONE'),self.rid))
        self.task.refresh_from_db();self.assertEqual(self.task.status,'BLOCKED')
    def test_A13_project_scope_even_known_ids(self):
        outsider=self.user('outsider','TECH');c=self.client_for(outsider)
        for path in ['projects/'+str(self.p.id),'tasks/'+str(self.task.id)]: self.assertEqual(c.get('/api/v1/'+path).status_code,404)
        self.assertEqual(self.api_post(c,'tasks/'+str(self.task.id)+'/transition',dict(expected_version=self.task.version,target_status='DONE')).status_code,404)
    def test_A14_notifications_dedupe(self):
        e=emit('TEST',self.task,'Task','Body',[self.tech.id]);consume_events();e.status='PENDING';e.save();consume_events()
        self.assertEqual(Notification.objects.filter(event=e,user=self.tech).count(),1)
    def test_A15_prd_numeric_ledger_cost(self):
        o=self.order();r=self.receipt(o,60);post_receipt(self.store,r.id,dict(expected_version=r.version),'rcv',self.rid);b=r.lines.first().batch
        m=issue(self.store,self.issue_data(b,10),'iss',self.rid)
        transfer(self.store,dict(batch_id=str(b.id),from_warehouse_id=str(self.wh.id),to_warehouse_id=str(self.wh2.id),qty=5),'tr',self.rid)
        self.assertEqual(StockBalance.objects.get(batch=b,warehouse=self.wh).on_hand_qty,45)
        self.assertEqual(StockBalance.objects.get(batch=b,warehouse=self.wh2).on_hand_qty,5)
        self.assertEqual(Decimal(costs(self.admin)[0]['material_cost']),120)
        reverse(self.admin,m.id,dict(reason='Correction'),'rev',self.rid)
        self.assertEqual(sum(x.on_hand_qty for x in StockBalance.objects.all()),60)
        self.assertEqual(Decimal(costs(self.admin)[0]['material_cost']),0);self.assertEqual(reconcile(),[])
    def test_order_closes_and_reopens_after_reversal(self):
        o=self.order();r1=self.receipt(o,60);post_receipt(self.store,r1.id,dict(expected_version=r1.version),'r1',self.rid)
        r2=self.receipt(o,40);m=post_receipt(self.store,r2.id,dict(expected_version=r2.version),'r2',self.rid)
        o.refresh_from_db();self.assertEqual(o.status,'CLOSED')
        reverse(self.admin,m.id,dict(reason='Wrong batch'),'rev',self.rid);o.refresh_from_db();self.assertEqual(o.status,'CONFIRMED')
    def test_multi_line_issue_all_or_none(self):
        b=self.stock(10);other=self.stock(2);data=self.issue_data(b,5);data['lines']+=self.issue_data(other,4)['lines']
        self.assertBusiness('INSUFFICIENT_STOCK',lambda:issue(self.store,data,'multi',self.rid))
        self.assertEqual(StockBalance.objects.get(batch=b).on_hand_qty,10);self.assertEqual(StockBalance.objects.get(batch=other).on_hand_qty,2)
    def test_exact_six_decimals_and_expiration_day(self):
        m=opening(self.admin,dict(item_id=str(self.item.id),warehouse_id=str(self.wh.id),batch_no='MICRO',unit_cost='0.100001',qty='0.000003',expires_on=str(self.today)),'micro',self.rid)
        b=m.lines.first().batch
        issue(self.store,self.issue_data(b,'0.000001'),'micro-issue',self.rid)
        self.assertEqual(StockBalance.objects.get(batch=b).on_hand_qty,Decimal('0.000002'))
        self.assertEqual(reconcile(),[])
    def test_opening_closed_after_business(self):
        b=self.stock(10);issue(self.store,self.issue_data(b,1),'i',self.rid)
        self.assertBusiness('OPENING_CLOSED',lambda:opening(self.admin,dict(item_id=str(self.item.id),warehouse_id=str(self.wh.id),batch_no='AFTER',unit_cost=1,qty=1),'late',self.rid))
    def test_adjustment_version_and_expired_positive(self):
        b=self.stock(10);balance=StockBalance.objects.get(batch=b)
        adjustment(self.admin,dict(batch_id=str(b.id),warehouse_id=str(self.wh.id),count_qty=9,expected_version=balance.version,reason='Count'),'adj',self.rid)
        self.assertBusiness('VERSION_CONFLICT',lambda:adjustment(self.admin,dict(batch_id=str(b.id),warehouse_id=str(self.wh.id),count_qty=8,expected_version=balance.version,reason='Count'),'adj2',self.rid))
        expired=self.stock(10,True);balance=StockBalance.objects.get(batch=expired)
        self.assertBusiness('BATCH_EXPIRED',lambda:adjustment(self.admin,dict(batch_id=str(expired.id),warehouse_id=str(self.wh.id),count_qty=11,expected_version=balance.version,reason='Count'),'exp-adj',self.rid))
    def test_master_deactivation_and_unit_lock(self):
        self.stock(10)
        self.assertBusiness('STOCK_EXISTS',lambda:write_master(self.admin,'items',dict(expected_version=self.item.version,is_active=False),self.rid,self.item.id))
        self.assertBusiness('UNIT_LOCKED',lambda:write_master(self.admin,'items',dict(expected_version=self.item.version,base_uom='EA'),self.rid,self.item.id))
        self.assertBusiness('STOCK_EXISTS',lambda:write_master(self.admin,'warehouses',dict(expected_version=self.wh.version,is_active=False),self.rid,self.wh.id))
    def test_import_duplicate_version_conflict_and_formula_escape(self):
        bad=import_preview(self.admin,'items','CREATE','dups.csv',b'code,name\na,One\nA,Two\n',{},'bad',self.rid)
        self.assertEqual(bad.status,'INVALID');self.assertEqual(bad.rows.filter(status='INVALID').count(),2)
        raw=b'code,name,base_uom,reorder_qty\nREAGENT-A,New,KIT,3\n'
        job=import_preview(self.admin,'items','UPDATE','update.csv',raw,{},'update',self.rid)
        write_master(self.admin,'items',dict(expected_version=self.item.version,name='Changed'),self.rid,self.item.id)
        job=execute_import(self.admin,job.id,self.rid)
        self.assertEqual(job.status,'FAILED');self.assertEqual(job.rows.first().errors_json[0]['code'],'VERSION_CONFLICT')
        self.assertEqual(csv_safe('=SUM(1,2)'),"'=SUM(1,2)")
    def test_csrf_inactive_session_and_auditor_readonly(self):
        c=self.client_for(self.admin,True)
        self.assertEqual(self.api_post(c,'items',dict(code='X',name='X')).status_code,403)
        c=self.client_for(self.tech);self.tech.is_active=False;self.tech.save()
        self.assertEqual(self.api_post(c,'tasks/'+str(self.task.id)+'/transition',dict(expected_version=self.task.version,target_status='DONE')).status_code,401)
        c=self.client_for(self.audit)
        self.assertEqual(self.api_post(c,'items',dict(code='X',name='X')).status_code,403)
        self.assertEqual(c.get('/api/v1/audit').status_code,200)
    def test_api_lists_details_and_input_contract(self):
        b=self.stock(10);o=self.order();r=self.receipt(o,1);c=self.client_for(self.admin)
        kinds=['items','suppliers','warehouses','projects','tasks','purchase-requests','purchase-orders','receipts','balances','movements','inventory','reports','import-jobs','audit','notifications','events','users','dashboard','me','references','system']
        for k in kinds:
            response=c.get('/api/v1/'+k)
            self.assertEqual(response.status_code,200,k+response.content.decode()[:100])
        self.assertEqual(c.get('/api/v1/items?page_size=101').status_code,400)
        self.assertEqual(c.get('/api/v1/items?sort=password').status_code,400)
        self.assertEqual(c.get('/api/v1/items/'+str(self.item.id)).status_code,200)
        self.assertEqual(c.get('/api/v1/receipts/'+str(r.id)).status_code,200)
        self.assertEqual(c.get('/api/v1/projects/'+str(self.p.id)).status_code,200)
    def test_creation_and_draft_idempotency(self):
        c=self.client_for(self.admin);d=dict(code='RETRY',name='Retry',base_uom='EA')
        a=self.api_post(c,'items',d,'create-stable');b=self.api_post(c,'items',d,'create-stable')
        self.assertEqual(a.status_code,201);self.assertEqual(a.json()['data']['id'],b.json()['data']['id'])
        self.assertEqual(self.api_post(c,'items',{**d,'name':'Changed'},'create-stable').status_code,409)
        batch=self.stock(10);payload=self.issue_data(batch,1)
        a=self.api_post(c,'stock/issues/drafts',payload,'draft-stable');b=self.api_post(c,'stock/issues/drafts',payload,'draft-stable')
        self.assertEqual(a.status_code,201);self.assertEqual(a.json()['data']['id'],b.json()['data']['id'])
        self.assertEqual(StockBalance.objects.get(batch=batch).on_hand_qty,10)

class ConcurrencyTests(Fixture,TransactionTestCase):
    reset_sequences=True
    def concurrent(self,functions):
        barrier=Barrier(len(functions))
        def run(fn):
            close_old_connections();barrier.wait()
            try: fn();return 'OK'
            except BusinessError as e:return e.code
            finally: connections.close_all()
        with ThreadPoolExecutor(max_workers=len(functions)) as pool:return list(pool.map(run,functions))
    def test_A03_concurrent_order_allocation(self):
        r=self.approved(100);lineid=str(r.lines.first().id)
        def order(q): return lambda:write_order(self.buyer,dict(supplier_id=str(self.supplier.id),lines=[dict(request_line_id=lineid,qty=q,unit_price=12)]),self.rid)
        outcomes=self.concurrent([order(70),order(50)])
        self.assertCountEqual(outcomes,['OK','OVER_ORDERED'])
        self.assertLessEqual(sum(x.qty for x in OrderLine.objects.all()),100)
    def test_A06_concurrent_issue(self):
        with transaction.atomic():b=self.stock(10)
        outcomes=self.concurrent([lambda:issue(self.store,self.issue_data(b,7),'concurrent-1',self.rid),lambda:issue(self.store,self.issue_data(b,7),'concurrent-2',self.rid)])
        self.assertCountEqual(outcomes,['OK','INSUFFICIENT_STOCK'])
        self.assertEqual(StockBalance.objects.get(batch=b).on_hand_qty,3);self.assertEqual(reconcile(),[])

class ExtensionTests(Fixture,TestCase):
    def test_A16_sample_transitions_and_events(self):
        from labops.samples.services import create_order,register,transition,order_transition
        test=TestCatalog.objects.create(code='T1',name='Simulated test',sample_type='SERUM')
        o=create_order(self.admin,dict(project_id=str(self.p.id),task_id=str(self.task.id),test_id=str(test.id)),self.rid)
        s=register(self.admin,dict(order_id=str(o.id),barcode='SIM-001'),self.rid)
        self.assertBusiness('INVALID_TRANSITION',lambda:transition(self.admin,s.id,dict(expected_version=s.version,target_status='PROCESSING'),self.rid))
        self.assertEqual(s.events.count(),1)
        self.assertBusiness('DUPLICATE_BARCODE',lambda:register(self.admin,dict(order_id=str(o.id),barcode='sim-001'),self.rid))
        for state in ['RECEIVED','PROCESSING','COMPLETED']:s=transition(self.tech,s.id,dict(expected_version=s.version,target_status=state),self.rid)
        self.assertEqual(s.events.count(),4)
        self.assertEqual(order_transition(self.admin,o.id,dict(expected_version=o.version,target_status='COMPLETED'),self.rid).status,'COMPLETED')
        self.assertEqual(StockMovement.objects.count(),0)
    def test_sample_scope_rejection_and_order_rules(self):
        from labops.samples.services import create_order,register,transition,order_transition
        test=TestCatalog.objects.create(code='T1',name='Simulated test',sample_type='SERUM')
        o=create_order(self.admin,dict(project_id=str(self.p.id),task_id=str(self.task.id),test_id=str(test.id)),self.rid)
        s=register(self.admin,dict(order_id=str(o.id),barcode='SIM-002'),self.rid)
        self.assertBusiness('INVALID_FIELD',lambda:transition(self.tech,s.id,dict(expected_version=s.version,target_status='REJECTED'),self.rid))
        s=transition(self.tech,s.id,dict(expected_version=s.version,target_status='REJECTED',reason='Damaged container'),self.rid)
        self.assertBusiness('SAMPLES_UNFINISHED',lambda:order_transition(self.admin,o.id,dict(expected_version=o.version,target_status='COMPLETED'),self.rid))
        self.assertEqual(order_transition(self.admin,o.id,dict(expected_version=o.version,target_status='CANCELLED'),self.rid).status,'CANCELLED')
        outsider=self.user('outside','TECH');c=self.client_for(outsider)
        self.assertEqual(c.get('/api/v1/samples/'+str(s.id)).status_code,404)
    def test_async_import_contract_and_worker(self):
        job=import_preview(self.admin,'items','CREATE','async.csv',b'code,name,base_uom\nASYNC,Async,EA\n',{},'async',self.rid)
        c=self.client_for(self.admin);r=self.api_post(c,'import-jobs/'+str(job.id)+'/execute',{'confirmed':True})
        self.assertEqual(r.status_code,202);self.assertEqual(r.json()['data']['status'],'RUNNING')
        self.assertFalse(Item.objects.filter(code='ASYNC').exists())
        process_imports();job.refresh_from_db();self.assertEqual(job.status,'COMPLETED');self.assertTrue(Item.objects.filter(code='ASYNC').exists())
    def test_audit_has_transaction_lines(self):
        pr=self.pr()
        entry=AuditEvent.objects.filter(entity_id=pr.id,action='SUBMIT').first()
        self.assertEqual(entry.after_json['lines'][0]['qty'],'100')
