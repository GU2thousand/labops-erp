from datetime import timedelta
from django.core.management.base import BaseCommand,CommandError
from django.contrib.auth.models import Group
from django.utils import timezone
from django.db import transaction
from labops.models import *
from labops.common import ROLES
from labops.catalog.services import write_master
from labops.projects.services import write_project,project_action,write_task,task_action
from labops.purchasing.services import write_request,request_action,write_order,order_action,create_receipt
from labops.inventory.services import opening,post_receipt,issue,transfer
from labops.operations.services import check_alerts,consume_events

class Command(BaseCommand):
    help='Create fictional demo records once; existing records are never overwritten.'
    @transaction.atomic
    def handle(self,*a,**kw):
        if User.objects.exists(): self.stdout.write('Existing users found; seed skipped.'); return
        today=timezone.localdate(); rid='demo-seed'; password='LabOpsDemo!2026'
        users={}
        for handle,name,role in [('admin','林知远','ADMIN'),('reviewer','陈思宁','ADMIN'),('manager','周亦然','MANAGER'),('buyer','许安','BUYER'),('store','陆川','STORE'),('tech','苏禾','TECH'),('auditor','沈清','AUDITOR')]:
            u=User.objects.create_user(username=handle+'@labops.local',email=handle+'@labops.local',name=name,password=password)
            u.groups.add(Group.objects.get_or_create(name=role)[0]); users[handle]=u
        admin=users['admin']; reviewer=users['reviewer']; manager=users['manager']; buyer=users['buyer']; tech=users['tech']; store=users['store']
        warehouses=[]
        for code,name,location in [('WH-01','中央试剂仓','A 栋 · 1F · 常温区'),('WH-02','低温储存仓','A 栋 · B1 · 2–8°C'),('WH-03','实验室耗材仓','B 栋 · 2F'),('WH-QA','隔离待检仓','A 栋 · 1F · 隔离区')]: warehouses.append(write_master(admin,'warehouses',dict(code=code,name=name,location=location),rid))
        suppliers=[]
        for code,name,contact in [('SUP-001','Northstar Bio Supplies','Alex Morgan'),('SUP-002','Evergreen Lab Solutions','Taylor Chen'),('SUP-003','Apex Scientific','Jamie Lee')]: suppliers.append(write_master(admin,'suppliers',dict(code=code,name=name,contact_name=contact,email=code.lower()+'@example.com'),rid))
        items=[]
        entries=[('RG-001','维生素 D 检测试剂盒','KIT',40,0,12),('RG-002','PBS 缓冲液','ML',500,280,0.08),('RG-003','核酸提取试剂盒','KIT',30,18,24),('CS-001','无菌移液枪头 200 μL','EA',500,1800,0.12),('CS-002','离心管 1.5 mL','EA',300,1200,0.25),('RG-004','血清质控标准品','KIT',20,45,32),('CS-003','一次性丁腈手套','EA',200,850,0.18),('RG-005','乙醇 75%','ML',1000,750,0.02),('RG-006','Tris 缓冲试剂','G',100,400,0.65),('CS-004','样本冻存管','EA',200,650,0.5),('RG-007','荧光定量试剂盒','KIT',20,36,48),('CS-005','无菌过滤器','EA',50,120,1.8)]
        for n,(code,name,uom,reorder,stock,cost) in enumerate(entries):
            item=write_master(admin,'items',dict(code=code,name=name,base_uom=uom,reorder_qty=reorder),rid); items.append(item)
            if stock:
                opening(admin,dict(item_id=str(item.id),warehouse_id=str(warehouses[n%3].id),batch_no=f'BT-2609-{n:03}',qty=stock,unit_cost=cost,expires_on=str(today+timedelta(days=12 if n in [2,5,10] else 150+n*7))),f'seed-opening-{n}',rid)
        projects=[]
        for n,(name,code,budget) in enumerate([('微量营养素检测流程优化','PRJ-2026-001',8500),('实验室质控体系升级','PRJ-2026-002',12000),('分子检测方法验证','PRJ-2026-003',6500),('秋季试剂与耗材准备','PRJ-2026-004',4800)]):
            p=write_project(admin,dict(code=code,name=name,owner_id=str(manager.id),budget_amount=budget,start_date=str(today-timedelta(days=12+n)),due_date=str(today+timedelta(days=14+n*8))),rid)
            p=project_action(admin,p.id,'members',dict(expected_version=p.version,user_id=str(tech.id)),rid)
            p=project_action(admin,p.id,'transition',dict(expected_version=p.version,target_status='ACTIVE'),rid); projects.append(p)
        names=[['试剂批次验证与入库','完成首批对照实验','整理实验数据与质控记录','复核营养素检测流程'],['更新标准操作规程','校验质控标准品','完成操作人员培训'],['核酸提取流程验证','检查引物与试剂有效期','整理方法学验证报告'],['盘点低温仓库存','发起季度耗材采购','归档供应商资质']]
        tasks=[]
        for n,p in enumerate(projects):
            for j,title in enumerate(names[n]):
                t=write_task(admin,dict(project_id=str(p.id),title=title,assignee_id=str(tech.id),priority='HIGH' if j==0 else 'NORMAL',due_date=str(today+timedelta(days=-2 if j==1 and n<2 else 5+j))),rid)
                if j<2 or j==3: t=task_action(admin,t.id,'transition',dict(expected_version=t.version,target_status='IN_PROGRESS'),rid)
                if j==0 and n>0: t=task_action(admin,t.id,'transition',dict(expected_version=t.version,target_status='DONE'),rid)
                if j==2 and n==1: t=task_action(admin,t.id,'transition',dict(expected_version=t.version,target_status='BLOCKED',reason='等待新版质控标准品到货'),rid)
                tasks.append(t)
        pr=write_request(buyer,dict(reason='微量营养素检测耗材补充',lines=[dict(item_id=str(items[0].id),qty=100,needed_by=str(today+timedelta(days=7)))]),rid)
        pr=request_action(buyer,pr.id,'submit',dict(expected_version=pr.version),rid)
        pr=request_action(admin,pr.id,'decision',dict(expected_version=pr.version,decision='APPROVE',reason='符合实验计划，同意采购'),rid)
        po=write_order(buyer,dict(supplier_id=str(suppliers[0].id),lines=[dict(request_line_id=str(pr.lines.first().id),qty=100,unit_price=12)]),rid)
        po=order_action(buyer,po.id,'confirm',dict(expected_version=po.version),rid)
        r=create_receipt(store,dict(order_id=str(po.id),lines=[dict(order_line_id=str(po.lines.first().id),warehouse_id=str(warehouses[0].id),qty=60,batch_no='B-001',supplier_lot='NS-2026-V1',expires_on=str(today+timedelta(days=24)))]),rid)
        post_receipt(store,r.id,dict(expected_version=r.version,receipt_id=str(r.id)),'seed-receipt',rid)
        b=r.lines.first().batch
        issue(store,dict(task_id=str(tasks[0].id),lines=[dict(batch_id=str(b.id),warehouse_id=str(warehouses[0].id),qty=10)]),'seed-issue',rid)
        transfer(store,dict(batch_id=str(b.id),from_warehouse_id=str(warehouses[0].id),to_warehouse_id=str(warehouses[2].id),qty=5),'seed-transfer',rid)
        for n,reason in enumerate(['核酸提取试剂补货 · 方法验证','PBS 缓冲液常规补充','乙醇与实验室耗材补货']):
            pr=write_request(buyer,dict(reason=reason,lines=[dict(item_id=str(items[[2,1,7][n]].id),qty=[60,2000,3000][n],needed_by=str(today+timedelta(days=3+n)))]),rid)
            request_action(buyer,pr.id,'submit',dict(expected_version=pr.version),rid)
        check_alerts(); consume_events()
        self.stdout.write('Demo data created. Login: admin@labops.local / LabOpsDemo!2026')
