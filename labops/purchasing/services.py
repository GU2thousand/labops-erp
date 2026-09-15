from collections import defaultdict
from labops.common import *
from labops.models import *

def request_scope(user,request,write=False):
    r=roles(user)
    if r & {'ADMIN','BUYER','STORE'}: return
    if not write and 'AUDITOR' in r: return
    require(request.project_id is not None,'NOT_FOUND','Request not found or access denied',404)
    project_scope(user,request.project,write)

def request_available(line,exclude=None):
    return line.qty-sum((x.qty for x in line.order_lines.exclude(order__status='CANCELLED').exclude(order_id=exclude)),Decimal(0))
def received_qty(line):
    return sum((x.qty for x in line.receipt_lines.filter(receipt__status='POSTED')),Decimal(0))
def order_state(order):
    if order.status in ['CONFIRMED','CLOSED']:
        lines=list(order.lines.all())
        order.status='CLOSED' if lines and all(received_qty(x)==x.qty for x in lines) else 'CONFIRMED'
        order.version+=1; order.save()

@atomic_command
def write_request(user,data,rid,id=None):
    allow(user,'ADMIN','MANAGER','BUYER','STORE','TECH')
    pr=obj(PurchaseRequest,id) if id else None
    if pr:
        request_scope(user,pr,True)
        require(pr.created_by_id==user.id,'FORBIDDEN','You can only edit drafts you created',403)
        version(pr,data); require(pr.status in ['DRAFT','REJECTED'],'DOCUMENT_LOCKED','Only draft or rejected requests can be edited')
    project_id=data.get('project_id',str(pr.project_id) if pr and pr.project_id else '')
    p=obj(Project,project_id) if project_id else None
    if p: project_scope(user,p,True); require_open(p)
    if not p: require(bool(roles(user)&{'ADMIN','BUYER','STORE'}),'FORBIDDEN','Specify a project you can access',403)
    reason=text(data.get('reason',pr.reason if pr else ''),'reason',5000)
    lines=data.get('lines',[])
    require(isinstance(lines,list) and len(lines)<=100,'INVALID_LINES','Maximum lines per request: 100  rows')
    parsed=[]
    for n,x in enumerate(lines,1):
        item=obj(Item,x.get('item_id')); require(item.is_active,'INACTIVE_ITEM','Inactive items cannot be added to a new request')
        parsed.append(dict(line_no=n,item=item,qty=qty(x.get('qty')),needed_by=day(x.get('needed_by'),'needed_by')))
    if pr:
        before=snapshot(pr); pr.project=p; pr.reason=reason; pr.lines.all().delete()
        save_change(user,pr,rid,before)
    else: pr=new(PurchaseRequest,user,rid,request_no=number('PR'),project=p,reason=reason)
    for x in parsed: RequestLine.objects.create(request=pr,**x)
    audit(user,pr,'LINES_SAVED',rid)
    return pr

@atomic_command
def request_action(user,id,action,data,rid):
    pr=obj(PurchaseRequest,id); request_scope(user,pr,True); version(pr,data)
    before=snapshot(pr)
    if action=='submit':
        require(pr.created_by_id==user.id,'FORBIDDEN','You can only submit your own requests',403)
        require(pr.status=='DRAFT','INVALID_TRANSITION','Restore the request to draft first')
        require(pr.lines.exists(),'EMPTY_LINES','Add at least one request line')
        require(not pr.lines.filter(item__is_active=False).exists(),'INACTIVE_ITEM','The request contains inactive items')
        if pr.project: require_open(pr.project)
        pr.status='SUBMITTED'; pr.submitted_at=timezone.now(); pr.approved_by=None; pr.approved_at=None; pr.decision_reason=''
    elif action=='decision':
        allow(user,'ADMIN')
        require(pr.created_by_id!=user.id,'SELF_APPROVAL_DENIED','You cannot approve your own request; ask another administrator',403)
        require(pr.status=='SUBMITTED','INVALID_TRANSITION','The request is not awaiting approval')
        require(data.get('decision') in ['APPROVE','REJECT'],'INVALID_DECISION','Invalid approval decision')
        pr.decision_reason=text(data.get('reason',''),'reason',2000)
        pr.status='APPROVED' if data['decision']=='APPROVE' else 'REJECTED'
        pr.approved_by=user; pr.approved_at=timezone.now()
        from labops.operations.services import emit
        emit('PURCHASE_DECISION',pr,'Purchase request approved' if pr.status=='APPROVED' else 'Purchase request rejected',pr.request_no+' · '+pr.reason,[pr.created_by_id],suffix=str(pr.version))
    elif action=='withdraw':
        require(pr.created_by_id==user.id,'FORBIDDEN','You can only withdraw your own requests',403)
        require(pr.status in ['SUBMITTED','REJECTED'],'INVALID_TRANSITION','Only submitted requests can be withdrawn or rejected requests restored')
        pr.status='DRAFT'; pr.approved_by=None; pr.approved_at=None; pr.decision_reason=''; pr.submitted_at=None
    elif action=='cancel':
        if pr.status=='APPROVED':
            allow(user,'ADMIN')
            require(not OrderLine.objects.filter(request_line__request=pr).exclude(order__status='CANCELLED').exists(),'ORDER_EXISTS','This request has active orders and cannot be cancelled')
        else:
            require(pr.created_by_id==user.id,'FORBIDDEN','You can only cancel your own requests',403)
            require(pr.status in ['DRAFT','REJECTED'],'INVALID_TRANSITION','Cannot cancel in the current status')
        pr.status='CANCELLED'
    else: fail('INVALID_ACTION','Action not found',404)
    save_change(user,pr,rid,before,action.upper())
    return pr

@atomic_command
def write_order(user,data,rid):
    allow(user,'ADMIN','BUYER')
    supplier=obj(Supplier,data.get('supplier_id')); require(supplier.is_active,'INACTIVE_SUPPLIER','Supplier is inactive')
    lines=data.get('lines',[]); require(isinstance(lines,list) and len(lines)<=100,'INVALID_LINES','Maximum lines per order: 100  rows')
    requested=defaultdict(Decimal); parsed=[]
    for n,x in enumerate(lines,1):
        line=obj(RequestLine,x.get('request_line_id'))
        require(line.request.status=='APPROVED','REQUEST_NOT_APPROVED','Orders can only reference approved request lines')
        require(line.item.is_active,'INACTIVE_ITEM','Item is inactive')
        q=qty(x.get('qty')); requested[line.id]+=q
        require(requested[line.id]<=request_available(line),'OVER_ORDERED','Total order quantity exceeds the unallocated request quantity',422,'qty')
        parsed.append(dict(line_no=n,request_line=line,qty=q,unit_price=qty(x.get('unit_price'),'unit_price')))
    po=new(PurchaseOrder,user,rid,order_no=number('PO'),supplier=supplier)
    for x in parsed: OrderLine.objects.create(order=po,**x)
    audit(user,po,'LINES_SAVED',rid)
    return po

@atomic_command
def order_action(user,id,action,data,rid):
    allow(user,'ADMIN','BUYER'); po=obj(PurchaseOrder,id); version(po,data); before=snapshot(po)
    if action=='confirm':
        require(po.status=='DRAFT','INVALID_TRANSITION','Only draft orders can be confirmed')
        require(po.supplier.is_active,'INACTIVE_SUPPLIER','Supplier is inactive')
        require(po.lines.exists(),'EMPTY_LINES','An order needs at least one line')
        for l in po.lines.select_related('request_line__item','request_line__request'):
            require(l.request_line.item.is_active and l.request_line.request.status=='APPROVED','SOURCE_UNAVAILABLE','The request or item is currently unavailable')
        po.status='CONFIRMED'; po.ordered_at=timezone.now()
    elif action=='cancel':
        require(po.status in ['DRAFT','CONFIRMED'],'INVALID_TRANSITION','This order cannot be cancelled in its current status')
        require(not po.receipts.filter(status='POSTED').exists(),'RECEIPT_EXISTS','Reverse all effective receipts before cancelling')
        po.status='CANCELLED'
    else: fail('INVALID_ACTION','Action not found',404)
    save_change(user,po,rid,before,action.upper()); return po

@atomic_command
def create_receipt(user,data,rid):
    allow(user,'ADMIN','STORE')
    po=obj(PurchaseOrder,data.get('order_id'))
    require(po.status in ['CONFIRMED','CLOSED'],'ORDER_NOT_CONFIRMED','Receipts require a confirmed purchase order')
    require(po.supplier.is_active,'INACTIVE_SUPPLIER','Supplier is inactive')
    lines=data.get('lines',[]); require(isinstance(lines,list) and 0<len(lines)<=100,'EMPTY_LINES','Add 1 to 100 receipt lines')
    receipt=new(Receipt,user,rid,receipt_no=number('RCV'),order=po)
    totals=defaultdict(Decimal)
    for n,x in enumerate(lines,1):
        line=obj(OrderLine,x.get('order_line_id')); require(line.order_id==po.id,'WRONG_ORDER','The receipt line does not belong to the selected order')
        item=line.request_line.item; require(item.is_active,'INACTIVE_ITEM','Item is inactive')
        warehouse=obj(Warehouse,x.get('warehouse_id')); require(warehouse.is_active,'INACTIVE_WAREHOUSE','Warehouse is inactive')
        q=qty(x.get('qty')); totals[line.id]+=q
        require(totals[line.id]<=line.qty-received_qty(line),'OVER_RECEIVED','Received quantity exceeds the remaining order quantity',422,'qty')
        batch_no=text(x.get('batch_no',''),'batch_no',64).upper()
        require(not Batch.objects.filter(item=item,batch_no=batch_no).exists(),'DUPLICATE_BATCH','This internal batch already exists for the item',409,'batch_no')
        batch=new(Batch,user,rid,item=item,batch_no=batch_no,supplier_lot=text(x.get('supplier_lot',''),'supplier_lot',128,False),expires_on=day(x.get('expires_on'),'expires_on',True),unit_cost=line.unit_price,origin='PURCHASE')
        ReceiptLine.objects.create(receipt=receipt,line_no=n,order_line=line,batch=batch,warehouse=warehouse,qty=q)
    audit(user,receipt,'LINES_SAVED',rid)
    return receipt
