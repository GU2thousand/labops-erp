from django.core.validators import validate_email
from django.core.exceptions import ValidationError
from django.contrib.auth.models import Group
from django.contrib.auth.password_validation import validate_password
from labops.common import *
from labops.models import Item,Supplier,Warehouse,User,Batch,StockBalance,OrderLine,StockMovementLine
MODELS={'items':Item,'suppliers':Supplier,'warehouses':Warehouse}

def normalized(kind,data):
    require(kind in MODELS,'INVALID_ENTITY','只支持物料、供应商和仓库')
    out={'code':text(data.get('code',''),'code',64).upper(),'name':text(data.get('name',''),'name')}
    active=data.get('is_active',True)
    if isinstance(active,str):
        require(active.lower() in ['true','false','1','0'],'INVALID_FIELD','is_active 必须为 true 或 false',422,'is_active')
        active=active.lower() in ['true','1']
    require(type(active) is bool,'INVALID_FIELD','is_active 必须为布尔值',422,'is_active')
    out['is_active']=active
    if kind=='items':
        out['base_uom']=data.get('base_uom','EA')
        require(out['base_uom'] in ['EA','ML','G','KIT'],'INVALID_UNIT','单位仅支持 EA、ML、G、KIT',422,'base_uom')
        out['reorder_qty']=qty(data.get('reorder_qty',0),'reorder_qty',False)
    if kind=='suppliers':
        out['email']=text(data.get('email',''),'email',254,False)
        if out['email']:
            try: validate_email(out['email'])
            except ValidationError: fail('INVALID_EMAIL','邮箱格式不正确',422,'email')
        out['contact_name']=text(data.get('contact_name',''),'contact_name',160,False)
    if kind=='warehouses': out['location']=text(data.get('location',''),'location',2000,False)
    return out

def validate_master(kind, fields, record=None):
    Model=MODELS[kind]
    require(not Model.objects.filter(code=fields['code']).exclude(pk=record.pk if record else None).exists(),'DUPLICATE_CODE','编码已存在，请使用其他编码',409,'code')
    if not record: return
    if kind=='items':
        if fields['base_uom']!=record.base_uom:
            require(not StockMovementLine.objects.filter(batch__item=record).exists() and not OrderLine.objects.filter(request_line__item=record).exists(),'UNIT_LOCKED','已有交易的物料不能更改基础单位',422,'base_uom')
        if not fields['is_active']:
            require(not StockBalance.objects.filter(batch__item=record,on_hand_qty__gt=0).exists(),'STOCK_EXISTS','物料库存必须为零才能停用')
            require(not OrderLine.objects.filter(request_line__item=record,order__status__in=['DRAFT','CONFIRMED']).exists(),'OPEN_ORDER_EXISTS','物料存在未结订单，不能停用')
    if kind=='warehouses' and not fields['is_active']:
        require(not StockBalance.objects.filter(warehouse=record,on_hand_qty__gt=0).exists(),'STOCK_EXISTS','仓库余额必须为零才能停用')

@atomic_command
def write_master(user,kind,data,rid,id=None):
    allow(user,*(['ADMIN','BUYER'] if kind=='suppliers' else ['ADMIN']))
    require(kind in MODELS,'INVALID_ENTITY','不支持的主数据类型')
    record=obj(MODELS[kind],id) if id else None
    if record: version(record,data)
    fields=normalized(kind,{**(snapshot(record) if record else {}),**data})
    validate_master(kind,fields,record)
    if not record: return new(MODELS[kind],user,rid,**fields)
    before=snapshot(record)
    for k,v in fields.items(): setattr(record,k,v)
    save_change(user,record,rid,before)
    return record

@atomic_command
def write_user(user,data,rid,id=None):
    allow(user,'ADMIN')
    record=obj(User,id) if id else None
    if record: version(record,data)
    before=snapshot(record) if record else None
    selected=data.get('roles',list(roles(record)) if record else [])
    require(isinstance(selected,list) and selected and set(selected)<=set(ROLES),'INVALID_ROLE','请选择有效角色')
    email=text(data.get('email',record.email if record else ''),'email',254).lower()
    try: validate_email(email)
    except ValidationError: fail('INVALID_EMAIL','邮箱格式不正确',422,'email')
    require(not User.objects.filter(email=email).exclude(pk=id).exists(),'DUPLICATE_EMAIL','邮箱已存在',409,'email')
    active=data.get('is_active',record.is_active if record else True)
    require(type(active) is bool,'INVALID_FIELD','is_active 必须为布尔值')
    if record and record.id==user.id:
        require(active and 'ADMIN' in selected,'SELF_DISABLE_DENIED','不能停用自己或移除自己的管理员角色')
    record=record or User()
    record.email=email; record.name=text(data.get('name',record.name),'name'); record.is_active=active
    password=data.get('password')
    if not id: require(bool(password),'PASSWORD_REQUIRED','新用户必须设置密码',422,'password')
    if password:
        try: validate_password(password,record)
        except ValidationError as e: fail('WEAK_PASSWORD','；'.join(e.messages),422,'password')
        record.set_password(password)
    record.version+=1 if id else 0
    record.save()
    record.groups.set([Group.objects.get_or_create(name=x)[0] for x in selected])
    audit(user,record,'USER_UPDATE' if id else 'USER_CREATE',rid,before,','.join(selected))
    return record
