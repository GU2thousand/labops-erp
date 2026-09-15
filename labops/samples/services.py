from labops.common import *
from labops.models import LabOrder,Sample,SampleEvent,TestCatalog,Warehouse

def authorize(user,order):
    allow(user,'ADMIN','MANAGER','TECH');project_scope(user,order.project,True);require_open(order.project)
    if not roles(user)&{'ADMIN','MANAGER'}: require(order.task is not None and order.task.assignee_id==user.id,'FORBIDDEN','Only the assigned technician can process this lab order',403)

@atomic_command
def create_order(user,data,rid):
    p=obj(Project,data.get('project_id'));project_scope(user,p,True);require_open(p);allow(user,'ADMIN','MANAGER')
    t=obj(Task,data['task_id']) if data.get('task_id') else None
    if t: require(t.project_id==p.id,'WRONG_PROJECT','The task must belong to the selected project')
    test=obj(TestCatalog,data.get('test_id'));require(test.is_active,'INACTIVE_TEST','Test catalog entry is inactive')
    return new(LabOrder,user,rid,order_no=number('LAB'),test=test,project=p,task=t)

@atomic_command
def register(user,data,rid):
    order=obj(LabOrder,data.get('order_id'));authorize(user,order)
    require(order.status=='OPEN','ORDER_CLOSED','Lab order is already closed')
    barcode=text(data.get('barcode',''),'barcode',64).upper()
    require(not Sample.objects.filter(barcode=barcode).exists(),'DUPLICATE_BARCODE','Sample barcode already exists',409,'barcode')
    warehouse=obj(Warehouse,data['storage_warehouse_id']) if data.get('storage_warehouse_id') else None
    if warehouse: require(warehouse.is_active,'INACTIVE_WAREHOUSE','Storage warehouse is inactive')
    sample=new(Sample,user,rid,order=order,barcode=barcode,collected_at=timezone.now(),storage_warehouse=warehouse)
    SampleEvent.objects.create(sample=sample,actor=user,from_status='',to_status='REGISTERED')
    return sample

@atomic_command
def transition(user,id,data,rid):
    sample=obj(Sample,id);authorize(user,sample.order);version(sample,data)
    require(sample.order.status=='OPEN','ORDER_CLOSED','Lab order is closed')
    target=data.get('target_status');next_states={'REGISTERED':['RECEIVED','REJECTED'],'RECEIVED':['PROCESSING','REJECTED'],'PROCESSING':['COMPLETED']}
    require(target in next_states.get(sample.status,[]),'INVALID_TRANSITION','Samples must proceed through registration, receipt, processing, and completion without skipping states')
    reason=text(data.get('reason',''),'reason',2000,target=='REJECTED')
    before=snapshot(sample);original=sample.status;sample.status=target
    if target=='RECEIVED':sample.received_at=timezone.now()
    save_change(user,sample,rid,before,'SAMPLE_TRANSITION',reason)
    SampleEvent.objects.create(sample=sample,actor=user,from_status=original,to_status=target,reason=reason)
    return sample

@atomic_command
def order_transition(user,id,data,rid):
    order=obj(LabOrder,id);authorize(user,order);version(order,data)
    require(order.status=='OPEN','ORDER_CLOSED','Lab order is closed');target=data.get('target_status')
    require(target in ['COMPLETED','CANCELLED'],'INVALID_TRANSITION','Invalid status')
    if target=='COMPLETED': require(order.samples.filter(status='COMPLETED').exists() and not order.samples.exclude(status__in=['COMPLETED','REJECTED']).exists(),'SAMPLES_UNFINISHED','At least one sample must be completed; all others must be completed or rejected')
    else: require(not order.samples.filter(status__in=['PROCESSING','COMPLETED']).exists(),'SAMPLES_PROCESSING','Cannot cancel with processing or completed samples')
    before=snapshot(order);order.status=target;save_change(user,order,rid,before,'LAB_ORDER_TRANSITION');return order
