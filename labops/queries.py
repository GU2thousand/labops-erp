from collections import defaultdict
from django.db import connection
from django.db.models import F, Sum, DecimalField, ExpressionWrapper, Value, Prefetch, Exists, OuterRef
from django.db.models.functions import Cast
from datetime import timedelta
from labops.common import *
from labops.models import *
from labops.purchasing.services import request_available,received_qty

def serialize(record,detail=False):
    if isinstance(record,dict): return jsonable(record)
    d=snapshot(record)
    if isinstance(record,User): return {'id':str(record.id),'name':record.name,'email':record.email,'roles':list(roles(record)),'is_active':record.is_active,'version':record.version}
    if hasattr(record,'created_by'): d['created_by_name']=record.created_by.name if record.created_by else 'System'
    if isinstance(record,Project):
        ts=list(record.tasks.all()); active=[t for t in ts if t.status!='CANCELLED']; done=sum(t.status=='DONE' for t in active)
        d.update(owner_name=record.owner.name,task_count=len(ts),done_count=done,progress=round(done/len(active)*100) if active else 0)
        if detail: d['members']=[serialize(x) for x in record.members.all()]; d['tasks']=[serialize(x) for x in ts]
    if isinstance(record,Task):
        d.update(project_name=record.project.name,project_code=record.project.code,assignee_name=record.assignee.name if record.assignee else 'Unassigned')
        if detail:
            d['comments']=[{**snapshot(x),'author_name':x.author.name} for x in record.comments.select_related('author').order_by('created_at','id')]
            d['usage']=[serialize(x) for x in record.movement_lines.filter(movement__status='POSTED')]
    if isinstance(record,PurchaseRequest):
        d.update(project_name=record.project.name if record.project else 'General replenishment',approved_by_name=record.approved_by.name if record.approved_by else '',line_count=record.lines.count())
        d['lines']=[{**snapshot(x),'item_name':x.item.name,'item_code':x.item.code,'base_uom':x.item.base_uom,'available_qty':str(request_available(x))} for x in record.lines.select_related('item')]
    if isinstance(record,PurchaseOrder):
        lines=list(record.lines.select_related('request_line__item','request_line__request'))
        d.update(supplier_name=record.supplier.name,total=str(sum((x.qty*x.unit_price for x in lines),Decimal(0))))
        d['lines']=[{**snapshot(x),'item_name':x.request_line.item.name,'item_code':x.request_line.item.code,'base_uom':x.request_line.item.base_uom,'request_no':x.request_line.request.request_no,'received_qty':str(received_qty(x)),'remaining_qty':str(x.qty-received_qty(x))} for x in lines]
    if isinstance(record,Receipt):
        d.update(order_no=record.order.order_no,supplier_name=record.order.supplier.name)
        d['lines']=[{**snapshot(x),'item_name':x.batch.item.name,'batch_no':x.batch.batch_no,'warehouse_name':x.warehouse.name,'expires_on':str(x.batch.expires_on or ''),'supplier_lot':x.batch.supplier_lot} for x in record.lines.select_related('batch__item','warehouse')]
        if hasattr(record,'movement'): d['movement_id']=str(record.movement.id)
    if isinstance(record,StockMovement):
        d['is_reversed']=record._is_reversed if hasattr(record,'_is_reversed') else StockMovement.objects.filter(reversal_of=record).exists()
        lines=record.prefetched_lines if hasattr(record,'prefetched_lines') else record.lines.select_related('batch__item','warehouse','task')
        d['lines']=[serialize(x) for x in lines]
        d['posted_by_name']=record.posted_by.name if record.posted_by else ''
        if record.receipt: d['receipt_no']=record.receipt.receipt_no
        if record.reversal_of: d['original_no']=record.reversal_of.movement_no
    if isinstance(record,StockMovementLine):
        d.update(batch_no=record.batch.batch_no,item_name=record.batch.item.name,base_uom=record.batch.item.base_uom,warehouse_name=record.warehouse.name,task_title=record.task.title if record.task else '',movement_no=record.movement.movement_no,type=record.movement.type)
    if isinstance(record,StockBalance):
        b=record.batch
        expired=bool(b.expires_on and b.expires_on<timezone.localdate())
        d.update(batch_no=b.batch_no,item_id=str(b.item_id),item_name=b.item.name,item_code=b.item.code,base_uom=b.item.base_uom,warehouse_name=record.warehouse.name,expires_on=str(b.expires_on or ''),unit_cost=str(b.unit_cost),supplier_lot=b.supplier_lot,expired=expired,available_qty=str(record.on_hand_qty if not expired and b.item.is_active and record.warehouse.is_active else 0))
    if isinstance(record,AuditEvent): d['actor_name']=record.actor.name if record.actor else 'System'
    if isinstance(record,LabOrder):
        d.update(project_name=record.project.name,test_name=record.test.name,sample_type=record.test.sample_type,task_title=record.task.title if record.task else '',sample_count=record.samples.count())
        if detail: d['samples']=[serialize(x) for x in record.samples.all()]
    if isinstance(record,Sample):
        d.update(order_no=record.order.order_no,project_name=record.order.project.name,test_name=record.order.test.name,sample_type=record.order.test.sample_type,warehouse_name=record.storage_warehouse.name if record.storage_warehouse else '')
        if detail: d['events']=[{**snapshot(e),'actor_name':e.actor.name} for e in record.events.order_by('occurred_at','id')]
    if isinstance(record,ImportJob) and detail: d['rows']=[snapshot(x) for x in record.rows.order_by('row_no')[:5000]]
    return d

def inventory_overview():
    today=timezone.localdate()
    sums=defaultdict(lambda:{'on_hand':Decimal(0),'available':Decimal(0),'expired':Decimal(0),'value':Decimal(0)})
    if connection.vendor == 'postgresql':
        # Fixed6Field is an integer micro-unit. Cast BEFORE multiplication to
        # avoid bigint overflow; divide quantity by 1e6 and value by 1e12.
        with connection.cursor() as cursor:
            cursor.execute("""SELECT b.item_id,
                SUM(s.on_hand_qty::numeric)*0.000001,
                SUM(CASE WHEN (b.expires_on IS NULL OR b.expires_on >= %s) AND i.is_active AND w.is_active THEN s.on_hand_qty::numeric ELSE 0 END)*0.000001,
                SUM(CASE WHEN b.expires_on < %s THEN s.on_hand_qty::numeric ELSE 0 END)*0.000001,
                SUM(s.on_hand_qty::numeric*b.unit_cost::numeric)*0.000000000001
                FROM labops_stockbalance s JOIN labops_batch b ON b.id=s.batch_id
                JOIN labops_item i ON i.id=b.item_id JOIN labops_warehouse w ON w.id=s.warehouse_id
                GROUP BY b.item_id""", [today,today])
            for item_id,on_hand,available,expired,value in cursor:
                sums[item_id]=dict(on_hand=on_hand,available=available,expired=expired,value=value)
    else:
        for b in StockBalance.objects.select_related('batch__item','warehouse'):
            row=sums[b.batch.item_id];row['on_hand']+=b.on_hand_qty;row['value']+=b.on_hand_qty*b.batch.unit_cost
            if b.batch.expires_on and b.batch.expires_on<today:row['expired']+=b.on_hand_qty
            elif b.warehouse.is_active and b.batch.item.is_active:row['available']+=b.on_hand_qty
    result=[]
    for item in Item.objects.select_related('created_by').order_by('code'):
        x=sums[item.id];result.append({**serialize(item),**{k:str(v) for k,v in x.items()},'low_stock':item.is_active and x['available']<item.reorder_qty})
    return result

def costs(user, filters=None):
    filters=filters or {}
    project_qs=visible_projects(user)
    if filters.get('project_id'): project_qs=project_qs.filter(pk=filters['project_id'])
    projects=list(project_qs.select_related('owner','created_by').prefetch_related('tasks')); totals=defaultdict(Decimal)
    lines=StockMovementLine.objects.filter(movement__status='POSTED',task__project__in=projects).select_related('task')
    if filters.get('item_id'): lines=lines.filter(batch__item_id=filters['item_id'])
    if filters.get('from_date'): lines=lines.filter(movement__posted_at__date__gte=day(filters['from_date']))
    if filters.get('to_date'): lines=lines.filter(movement__posted_at__date__lte=day(filters['to_date']))
    if connection.vendor == 'postgresql':
        numeric=DecimalField(max_digits=38,decimal_places=0)
        amount=ExpressionWrapper(-Cast(F('delta_qty'),numeric)*Cast(F('unit_cost'),numeric)*Value(Decimal('0.000000000001')),output_field=DecimalField(max_digits=38,decimal_places=12))
        for row in lines.values('task__project_id').annotate(total=Sum(amount)):
            totals[row['task__project_id']]=row['total']
    else:
        for l in lines: totals[l.task.project_id]-=l.delta_qty*l.unit_cost
    return [{**serialize(p),'material_cost':str(totals[p.id]),'budget_remaining':str(p.budget_amount-totals[p.id]) if p.budget_amount is not None else None} for p in projects]

def dashboard(user):
    today=timezone.localdate(); r=roles(user); overview=inventory_overview()
    ts=Task.objects.filter(project__in=visible_projects(user)); overdue=ts.filter(due_date__lt=today).exclude(status__in=['DONE','CANCELLED'])
    due_batches=StockBalance.objects.filter(on_hand_qty__gt=0,batch__expires_on__gte=today,batch__expires_on__lte=today+timedelta(days=30))
    if r&{'ADMIN','BUYER','STORE','AUDITOR'}: prs=PurchaseRequest.objects.all()
    else: prs=PurchaseRequest.objects.filter(project__in=visible_projects(user))
    pending=prs.filter(status='SUBMITTED')
    return {'metrics':{'pending_requests':pending.count(),'low_stock':sum(x['low_stock'] for x in overview),'expiring_batches':due_batches.values('batch_id').distinct().count(),'overdue_tasks':overdue.count(),'inventory_value':str(sum((Decimal(x['value']) for x in overview),Decimal(0)))},'projects':[serialize(p) for p in visible_projects(user).order_by('-created_at','-id')[:4]],'pending':[serialize(x) for x in pending.order_by('-created_at','-id')[:4]],'alerts':[x for x in overview if x['low_stock']][:4],'tasks':[serialize(x) for x in overdue.select_related('project','assignee').order_by('due_date','id')[:4]],'refreshed_at':timezone.now().isoformat()}


def prepare_queryset(qs, kind):
    fields={f.name for f in qs.model._meta.fields}
    if 'created_by' in fields:qs=qs.select_related('created_by')
    if kind=='movements':
        lines=StockMovementLine.objects.select_related('batch__item','warehouse','task','movement')
        qs=qs.select_related('posted_by','receipt','reversal_of').prefetch_related(Prefetch('lines',queryset=lines,to_attr='prefetched_lines')).annotate(_is_reversed=Exists(StockMovement.objects.filter(reversal_of_id=OuterRef('pk'))))
    if kind=='balances':qs=qs.select_related('batch__item','warehouse')
    if kind=='tasks':qs=qs.select_related('project','assignee')
    if kind=='projects':qs=qs.select_related('owner').prefetch_related('tasks')
    return qs
