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
        for handle,name,role in [('admin','Lin Zhiyuan','ADMIN'),('reviewer','Chen Sining','ADMIN'),('manager','Zhou Yiran','MANAGER'),('buyer','Xu An','BUYER'),('store','Lu Chuan','STORE'),('tech','Su He','TECH'),('auditor','Shen Qing','AUDITOR')]:
            u=User.objects.create_user(username=handle+'@labops.local',email=handle+'@labops.local',name=name,password=password)
            u.groups.add(Group.objects.get_or_create(name=role)[0]); users[handle]=u
        admin=users['admin']; reviewer=users['reviewer']; manager=users['manager']; buyer=users['buyer']; tech=users['tech']; store=users['store']
        warehouses=[]
        for code,name,location in [('WH-01','Central Reagent Warehouse','Building A · 1F · Room temperature'),('WH-02','Cold Storage Warehouse','Building A · B1 · 2–8°C'),('WH-03','Lab Consumables Warehouse','Building B · 2F'),('WH-QA','Quarantine Warehouse','Building A · 1F · Quarantine area')]: warehouses.append(write_master(admin,'warehouses',dict(code=code,name=name,location=location),rid))
        suppliers=[]
        for code,name,contact in [('SUP-001','Northstar Bio Supplies','Alex Morgan'),('SUP-002','Evergreen Lab Solutions','Taylor Chen'),('SUP-003','Apex Scientific','Jamie Lee')]: suppliers.append(write_master(admin,'suppliers',dict(code=code,name=name,contact_name=contact,email=code.lower()+'@example.com'),rid))
        items=[]
        entries=[('RG-001','Vitamin D test kit','KIT',40,0,12),('RG-002','PBS buffer solution','ML',500,280,0.08),('RG-003','Nucleic acid extraction kit','KIT',30,18,24),('CS-001','Sterile pipette tips, 200 μL','EA',500,1800,0.12),('CS-002','Centrifuge tubes, 1.5 mL','EA',300,1200,0.25),('RG-004','Serum QC reference material','KIT',20,45,32),('CS-003','Disposable nitrile gloves','EA',200,850,0.18),('RG-005','Ethanol 75%','ML',1000,750,0.02),('RG-006','Tris buffer reagent','G',100,400,0.65),('CS-004','Sample cryovials','EA',200,650,0.5),('RG-007','Quantitative fluorescence assay kit','KIT',20,36,48),('CS-005','Sterile filters','EA',50,120,1.8)]
        for n,(code,name,uom,reorder,stock,cost) in enumerate(entries):
            item=write_master(admin,'items',dict(code=code,name=name,base_uom=uom,reorder_qty=reorder),rid); items.append(item)
            if stock:
                opening(admin,dict(item_id=str(item.id),warehouse_id=str(warehouses[n%3].id),batch_no=f'BT-2609-{n:03}',qty=stock,unit_cost=cost,expires_on=str(today+timedelta(days=12 if n in [2,5,10] else 150+n*7))),f'seed-opening-{n}',rid)
        projects=[]
        for n,(name,code,budget) in enumerate([('Micronutrient Testing Workflow Optimization','PRJ-2026-001',8500),('Laboratory Quality Control Upgrade','PRJ-2026-002',12000),('Molecular Assay Method Validation','PRJ-2026-003',6500),('Autumn Reagent and Consumables Preparation','PRJ-2026-004',4800)]):
            p=write_project(admin,dict(code=code,name=name,owner_id=str(manager.id),budget_amount=budget,start_date=str(today-timedelta(days=12+n)),due_date=str(today+timedelta(days=14+n*8))),rid)
            p=project_action(admin,p.id,'members',dict(expected_version=p.version,user_id=str(tech.id)),rid)
            p=project_action(admin,p.id,'transition',dict(expected_version=p.version,target_status='ACTIVE'),rid); projects.append(p)
        names=[['Validate reagent batches and receive stock','Complete initial control experiments','Organize experiment data and QC records','Review micronutrient testing workflow'],['Update standard operating procedures','Verify QC reference materials','Complete operator training'],['Validate nucleic acid extraction workflow','Check primer and reagent expiry dates','Prepare method validation report'],['Count cold storage stock','Request quarterly consumables','Archive supplier qualifications']]
        tasks=[]
        for n,p in enumerate(projects):
            for j,title in enumerate(names[n]):
                t=write_task(admin,dict(project_id=str(p.id),title=title,assignee_id=str(tech.id),priority='HIGH' if j==0 else 'NORMAL',due_date=str(today+timedelta(days=-2 if j==1 and n<2 else 5+j))),rid)
                if j<2 or j==3: t=task_action(admin,t.id,'transition',dict(expected_version=t.version,target_status='IN_PROGRESS'),rid)
                if j==0 and n>0: t=task_action(admin,t.id,'transition',dict(expected_version=t.version,target_status='DONE'),rid)
                if j==2 and n==1: t=task_action(admin,t.id,'transition',dict(expected_version=t.version,target_status='BLOCKED',reason='Waiting for updated QC reference materials'),rid)
                tasks.append(t)
        pr=write_request(buyer,dict(reason='Restock micronutrient testing consumables',lines=[dict(item_id=str(items[0].id),qty=100,needed_by=str(today+timedelta(days=7)))]),rid)
        pr=request_action(buyer,pr.id,'submit',dict(expected_version=pr.version),rid)
        pr=request_action(admin,pr.id,'decision',dict(expected_version=pr.version,decision='APPROVE',reason='Consistent with the experiment plan; purchase approved'),rid)
        po=write_order(buyer,dict(supplier_id=str(suppliers[0].id),lines=[dict(request_line_id=str(pr.lines.first().id),qty=100,unit_price=12)]),rid)
        po=order_action(buyer,po.id,'confirm',dict(expected_version=po.version),rid)
        r=create_receipt(store,dict(order_id=str(po.id),lines=[dict(order_line_id=str(po.lines.first().id),warehouse_id=str(warehouses[0].id),qty=60,batch_no='B-001',supplier_lot='NS-2026-V1',expires_on=str(today+timedelta(days=24)))]),rid)
        post_receipt(store,r.id,dict(expected_version=r.version,receipt_id=str(r.id)),'seed-receipt',rid)
        b=r.lines.first().batch
        issue(store,dict(task_id=str(tasks[0].id),lines=[dict(batch_id=str(b.id),warehouse_id=str(warehouses[0].id),qty=10)]),'seed-issue',rid)
        transfer(store,dict(batch_id=str(b.id),from_warehouse_id=str(warehouses[0].id),to_warehouse_id=str(warehouses[2].id),qty=5),'seed-transfer',rid)
        for n,reason in enumerate(['Restock nucleic acid extraction reagents · Method validation','PBS buffer replenishment','Restock ethanol and laboratory consumables']):
            pr=write_request(buyer,dict(reason=reason,lines=[dict(item_id=str(items[[2,1,7][n]].id),qty=[60,2000,3000][n],needed_by=str(today+timedelta(days=3+n)))]),rid)
            request_action(buyer,pr.id,'submit',dict(expected_version=pr.version),rid)
        check_alerts(); consume_events()
        self.stdout.write('Demo data created. Login: admin@labops.local / LabOpsDemo!2026')
