from collections import defaultdict
from labops.common import *
from labops.models import *
from labops.purchasing.services import received_qty,order_state

def validate_active(batch,warehouse):
    require(batch.item.is_active,'INACTIVE_ITEM','物料已停用')
    require(warehouse.is_active,'INACTIVE_WAREHOUSE','仓库已停用')
def check_task(user,task,member=False):
    if member: project_scope(user,task.project,True)
    require(task.project.status=='ACTIVE' and task.status=='IN_PROGRESS','TASK_NOT_ACTIVE','只有进行中项目的进行中任务可以领料')
def movement_hash(user,kind,data): return digest({'actor':str(user.id),'kind':kind,'data':data})
def existing(user,key,kind,data):
    key=text(key or '', 'Idempotency-Key',128)
    prior=StockMovement.objects.filter(idempotency_key=key).first()
    if prior:
        require(prior.request_hash==movement_hash(user,kind,data),'IDEMPOTENCY_CONFLICT','相同幂等键已用于其他内容',409)
    return prior

def post(user,kind,lines,data,key,rid,receipt=None,reversal=None,draft=None):
    require(bool(lines),'EMPTY_LINES','库存单据至少需要一条有效明细')
    changes=defaultdict(Decimal)
    for x in lines: changes[(x['batch'].id,x['warehouse'].id)]+=x['delta_qty']
    balances=[]
    for (bid,wid),delta in sorted(changes.items()):
        balance,_=StockBalance.objects.get_or_create(batch_id=bid,warehouse_id=wid)
        require(balance.on_hand_qty+delta>=0,'INSUFFICIENT_STOCK','批次在所选仓库余额不足，整笔操作未执行',422,'qty')
        balances.append((balance,delta))
    m=draft or new(StockMovement,user,rid,movement_no=number('STK'),type=kind)
    before=snapshot(m)
    if draft: m.lines.all().delete()
    m.status='POSTED'; m.receipt=receipt; m.reversal_of=reversal; m.reason=data.get('reason',m.reason); m.idempotency_key=key; m.request_hash=movement_hash(user,kind,data); m.posted_at=timezone.now(); m.posted_by=user
    for n,x in enumerate(lines,1): StockMovementLine.objects.create(movement=m,line_no=n,**x)
    for balance,delta in balances:
        balance.on_hand_qty+=delta; balance.version+=1; balance.save()
    save_change(user,m,rid,before,'POST',m.reason)
    if kind in ['RECEIPT','ISSUE']: RuntimeState.objects.filter(pk=1).update(opening_closed=True)
    return m

def line(batch,warehouse,delta,**other): return dict(batch=batch,warehouse=warehouse,delta_qty=delta,unit_cost=batch.unit_cost,**other)

@atomic_command
def post_receipt(user,id,data,key,rid):
    allow(user,'ADMIN','STORE'); prior=existing(user,key,'RECEIPT',data)
    if prior:
        require(str(prior.receipt_id)==str(id),'IDEMPOTENCY_CONFLICT','相同幂等键对应其他收货单',409); return prior
    receipt=obj(Receipt,id)
    if receipt.status=='POSTED': return receipt.movement
    require(receipt.status=='DRAFT','INVALID_TRANSITION','已冲销收货不能再次过账')
    version(receipt,data)
    po=receipt.order
    require(po.status in ['CONFIRMED','CLOSED'],'ORDER_NOT_CONFIRMED','采购订单已取消或尚未确认')
    require(po.supplier.is_active,'INACTIVE_SUPPLIER','供应商已停用')
    totals=defaultdict(Decimal); lines=[]
    for r in receipt.lines.select_related('batch__item','warehouse','order_line'):
        validate_active(r.batch,r.warehouse)
        totals[r.order_line_id]+=r.qty
        require(totals[r.order_line_id]<=r.order_line.qty-received_qty(r.order_line),'OVER_RECEIVED','收货数量超过当前剩余数量',422,'qty')
        require(r.order_line.order_id==po.id,'WRONG_ORDER','收货行订单不一致')
        lines.append(line(r.batch,r.warehouse,r.qty,receipt_line=r))
    m=post(user,'RECEIPT',lines,data,key,rid,receipt=receipt)
    before=snapshot(receipt); receipt.status='POSTED'; receipt.posted_at=timezone.now(); receipt.posted_by=user
    save_change(user,receipt,rid,before,'POST'); order_state(po)
    from labops.operations.services import emit,stock_recipients
    emit('RECEIPT_POSTED',receipt,'收货已入库',receipt.receipt_no+' · '+po.order_no,stock_recipients())
    return m

def issue_lines(user,data,member=False):
    task=obj(Task,data.get('task_id')); check_task(user,task,member)
    inputs=data.get('lines',[])
    require(isinstance(inputs,list) and 0<len(inputs)<=100,'EMPTY_LINES','领料需要 1 至 100 条明细')
    result=[]
    for x in inputs:
        batch=obj(Batch,x.get('batch_id')); warehouse=obj(Warehouse,x.get('warehouse_id')); validate_active(batch,warehouse)
        require(not batch.expires_on or batch.expires_on>=timezone.localdate(),'BATCH_EXPIRED','所选批次已过期，不能领料',422,'batch_id')
        result.append(line(batch,warehouse,-qty(x.get('qty')),task=task))
    return result

@atomic_command
def issue_draft(user,data,rid,id=None):
    allow(user,'ADMIN','MANAGER','TECH','STORE')
    draft=obj(StockMovement,id) if id else None
    if draft:
        require(draft.created_by_id==user.id,'FORBIDDEN','只能修改自己的草稿',403)
        require(draft.status=='DRAFT' and draft.type=='ISSUE','DOCUMENT_LOCKED','只有领料草稿可以修改')
        version(draft,data)
        if data.get('withdraw'):
            audit(user,draft,'WITHDRAW',rid); draft.lines.all().delete(); draft.delete(); return {'withdrawn':True}
    lines=issue_lines(user,data,member=not bool(roles(user)&{'ADMIN','STORE'}))
    if not draft: draft=new(StockMovement,user,rid,movement_no=number('ISS'),type='ISSUE',reason=text(data.get('reason',''),'reason',2000,False))
    else:
        before=snapshot(draft); draft.lines.all().delete(); save_change(user,draft,rid,before)
    for n,x in enumerate(lines,1): StockMovementLine.objects.create(movement=draft,line_no=n,**x)
    return draft

@atomic_command
def issue(user,data,key,rid):
    allow(user,'ADMIN','STORE'); prior=existing(user,key,'ISSUE',data)
    if prior: return prior
    draft=obj(StockMovement,data['draft_id']) if data.get('draft_id') else None
    if draft:
        require(draft.type=='ISSUE','INVALID_DOCUMENT','只能过账领料需求')
        require(draft.status=='DRAFT','ALREADY_POSTED','该需求已过账',409)
        version(draft,data)
        lines=[]
        for x in draft.lines.select_related('task__project','batch__item','warehouse'):
            check_task(user,x.task); validate_active(x.batch,x.warehouse)
            require(not x.batch.expires_on or x.batch.expires_on>=timezone.localdate(),'BATCH_EXPIRED','批次已过期')
            lines.append(line(x.batch,x.warehouse,x.delta_qty,task=x.task))
    else: lines=issue_lines(user,data)
    return post(user,'ISSUE',lines,data,key,rid,draft=draft)

@atomic_command
def transfer(user,data,key,rid):
    allow(user,'ADMIN','STORE'); prior=existing(user,key,'TRANSFER',data)
    if prior: return prior
    batch=obj(Batch,data.get('batch_id')); source=obj(Warehouse,data.get('from_warehouse_id')); target=obj(Warehouse,data.get('to_warehouse_id')); q=qty(data.get('qty'))
    require(source.id!=target.id,'SAME_WAREHOUSE','来源仓库和目标仓库必须不同')
    validate_active(batch,source); validate_active(batch,target)
    return post(user,'TRANSFER',[line(batch,source,-q,transfer_pair_no=1),line(batch,target,q,transfer_pair_no=1)],data,key,rid)

@atomic_command
def adjustment(user,data,key,rid):
    allow(user,'ADMIN'); prior=existing(user,key,'ADJUSTMENT',data)
    if prior: return prior
    reason=text(data.get('reason',''),'reason',2000)
    batch=obj(Batch,data.get('batch_id')); warehouse=obj(Warehouse,data.get('warehouse_id')); validate_active(batch,warehouse)
    balance=StockBalance.objects.filter(batch=batch,warehouse=warehouse).first()
    require(balance is not None,'BALANCE_NOT_FOUND','请选择已有库存记录进行盘点')
    version(balance,data); count=qty(data.get('count_qty'),'count_qty',False); delta=count-balance.on_hand_qty
    require(delta!=0,'NO_CHANGE','盘点数量与账面相同，无需生成流水')
    if batch.expires_on and batch.expires_on<timezone.localdate(): require(delta<0,'BATCH_EXPIRED','过期批次仅允许负向盘点调整')
    return post(user,'ADJUSTMENT',[line(batch,warehouse,delta)],{**data,'reason':reason},key,rid)

@atomic_command
def opening(user,data,key,rid):
    allow(user,'ADMIN'); prior=existing(user,key,'OPENING',data)
    if prior: return prior
    require(not RuntimeState.objects.get(pk=1).opening_closed,'OPENING_CLOSED','业务收货或领料已启用，不能再录入期初库存')
    item=obj(Item,data.get('item_id')); wh=obj(Warehouse,data.get('warehouse_id'))
    batch_no=text(data.get('batch_no',''),'batch_no',64).upper()
    require(not Batch.objects.filter(item=item,batch_no=batch_no).exists(),'DUPLICATE_BATCH','期初必须使用新的内部批次',409)
    batch=new(Batch,user,rid,item=item,batch_no=batch_no,origin='OPENING',unit_cost=qty(data.get('unit_cost'),'unit_cost',False),expires_on=day(data.get('expires_on'),'expires_on',True))
    validate_active(batch,wh)
    return post(user,'OPENING',[line(batch,wh,qty(data.get('qty')))],data,key,rid)

@atomic_command
def reverse(user,id,data,key,rid):
    allow(user,'ADMIN'); prior=existing(user,key,'REVERSAL',data)
    if prior:
        require(str(prior.reversal_of_id)==str(id),'IDEMPOTENCY_CONFLICT','该幂等键引用其他单据',409); return prior
    original=obj(StockMovement,id)
    require(original.status=='POSTED' and original.type!='REVERSAL','INVALID_REVERSAL','只能冲销原始已过账单据')
    require(not StockMovement.objects.filter(reversal_of=original).exists(),'ALREADY_REVERSED','原单已经冲销，不能再次冲销',409)
    reason=text(data.get('reason',''),'reason',2000)
    lines=[]
    for x in original.lines.select_related('batch','warehouse','task','receipt_line'):
        lines.append(dict(batch=x.batch,warehouse=x.warehouse,delta_qty=-x.delta_qty,unit_cost=x.unit_cost,task=x.task,receipt_line=x.receipt_line,reversal_of_line=x,transfer_pair_no=x.transfer_pair_no))
    m=post(user,'REVERSAL',lines,{**data,'reason':reason},key,rid,reversal=original)
    if original.receipt_id:
        receipt=original.receipt; before=snapshot(receipt); receipt.status='REVERSED'; save_change(user,receipt,rid,before,'REVERSE',reason); order_state(receipt.order)
    return m

def reconcile():
    sums=defaultdict(Decimal)
    for x in StockMovementLine.objects.filter(movement__status='POSTED'): sums[(x.batch_id,x.warehouse_id)]+=x.delta_qty
    problems=[]
    for b in StockBalance.objects.all():
        expected=sums.pop((b.batch_id,b.warehouse_id),Decimal(0))
        if expected!=b.on_hand_qty: problems.append({'batch_id':str(b.batch_id),'warehouse_id':str(b.warehouse_id),'balance':str(b.on_hand_qty),'ledger':str(expected)})
    for (bid,wid),q in sums.items():
        if q: problems.append({'batch_id':str(bid),'warehouse_id':str(wid),'balance':'0','ledger':str(q)})
    return problems
