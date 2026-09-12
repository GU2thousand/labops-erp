from labops.common import *
from labops.models import LabOrder,Sample,SampleEvent,TestCatalog,Warehouse

def authorize(user,order):
    allow(user,'ADMIN','MANAGER','TECH');project_scope(user,order.project,True);require_open(order.project)
    if not roles(user)&{'ADMIN','MANAGER'}: require(order.task is not None and order.task.assignee_id==user.id,'FORBIDDEN','仅分配实验员可以处理此检测单',403)

@atomic_command
def create_order(user,data,rid):
    p=obj(Project,data.get('project_id'));project_scope(user,p,True);require_open(p);allow(user,'ADMIN','MANAGER')
    t=obj(Task,data['task_id']) if data.get('task_id') else None
    if t: require(t.project_id==p.id,'WRONG_PROJECT','任务必须属于所选项目')
    test=obj(TestCatalog,data.get('test_id'));require(test.is_active,'INACTIVE_TEST','检测目录已停用')
    return new(LabOrder,user,rid,order_no=number('LAB'),test=test,project=p,task=t)

@atomic_command
def register(user,data,rid):
    order=obj(LabOrder,data.get('order_id'));authorize(user,order)
    require(order.status=='OPEN','ORDER_CLOSED','检测单已经关闭')
    barcode=text(data.get('barcode',''),'barcode',64).upper()
    require(not Sample.objects.filter(barcode=barcode).exists(),'DUPLICATE_BARCODE','样本条码已存在',409,'barcode')
    warehouse=obj(Warehouse,data['storage_warehouse_id']) if data.get('storage_warehouse_id') else None
    if warehouse: require(warehouse.is_active,'INACTIVE_WAREHOUSE','存储仓库已停用')
    sample=new(Sample,user,rid,order=order,barcode=barcode,collected_at=timezone.now(),storage_warehouse=warehouse)
    SampleEvent.objects.create(sample=sample,actor=user,from_status='',to_status='REGISTERED')
    return sample

@atomic_command
def transition(user,id,data,rid):
    sample=obj(Sample,id);authorize(user,sample.order);version(sample,data)
    require(sample.order.status=='OPEN','ORDER_CLOSED','检测单已关闭')
    target=data.get('target_status');next_states={'REGISTERED':['RECEIVED','REJECTED'],'RECEIVED':['PROCESSING','REJECTED'],'PROCESSING':['COMPLETED']}
    require(target in next_states.get(sample.status,[]),'INVALID_TRANSITION','样本必须依次登记、接收、处理、完成，不允许跳过状态')
    reason=text(data.get('reason',''),'reason',2000,target=='REJECTED')
    before=snapshot(sample);original=sample.status;sample.status=target
    if target=='RECEIVED':sample.received_at=timezone.now()
    save_change(user,sample,rid,before,'SAMPLE_TRANSITION',reason)
    SampleEvent.objects.create(sample=sample,actor=user,from_status=original,to_status=target,reason=reason)
    return sample

@atomic_command
def order_transition(user,id,data,rid):
    order=obj(LabOrder,id);authorize(user,order);version(order,data)
    require(order.status=='OPEN','ORDER_CLOSED','检测单已关闭');target=data.get('target_status')
    require(target in ['COMPLETED','CANCELLED'],'INVALID_TRANSITION','状态无效')
    if target=='COMPLETED': require(order.samples.filter(status='COMPLETED').exists() and not order.samples.exclude(status__in=['COMPLETED','REJECTED']).exists(),'SAMPLES_UNFINISHED','至少一份样本完成，其他样本必须完成或拒收')
    else: require(not order.samples.filter(status__in=['PROCESSING','COMPLETED']).exists(),'SAMPLES_PROCESSING','存在处理中的或已完成样本，不能取消')
    before=snapshot(order);order.status=target;save_change(user,order,rid,before,'LAB_ORDER_TRANSITION');return order
