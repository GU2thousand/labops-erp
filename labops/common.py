import json, hashlib, uuid, functools, logging
from decimal import Decimal, InvalidOperation
from datetime import date
from django.db import transaction
from django.core.serializers.json import DjangoJSONEncoder
from django.core.exceptions import ValidationError
from django.utils import timezone
from .models import User, RuntimeState, AuditEvent, CommandResult, Project, Task

ROLES = {'ADMIN':'管理员','MANAGER':'项目经理','BUYER':'采购员','STORE':'库管','TECH':'实验员','AUDITOR':'审计员'}
class BusinessError(Exception):
    def __init__(self, code, message, status=422, field=''):
        self.code,self.message,self.status,self.field = code,message,status,field

def fail(code, message, status=422, field=''): raise BusinessError(code,message,status,field)
def require(condition, code, message, status=422, field=''):
    if not condition: fail(code,message,status,field)
def roles(user): return set(user.groups.values_list('name',flat=True))
def allow(user, *allowed):
    require(user.is_active, 'UNAUTHORIZED','账号已停用',401)
    require(bool(roles(user) & set(allowed)), 'FORBIDDEN','当前角色没有此操作权限',403)
def project_scope(user, project, write=False):
    r = roles(user)
    if 'ADMIN' in r: return
    if not write and 'AUDITOR' in r: return
    require(bool(r & {'MANAGER','TECH'}) and project.members.filter(id=user.id).exists(), 'NOT_FOUND','项目不存在或无权访问',404)
def visible_projects(user):
    if roles(user) & {'ADMIN','AUDITOR'}: return Project.objects.all()
    if roles(user) & {'MANAGER','TECH'}: return Project.objects.filter(members=user).distinct()
    return Project.objects.none()
def require_open(project):
    require(project.status not in ['ARCHIVED','COMPLETED','CANCELLED'],'PROJECT_READ_ONLY','项目已结束，不可修改')
def obj(model, id):
    try: return model.objects.get(pk=id)
    except (model.DoesNotExist, ValueError, ValidationError): fail('NOT_FOUND','记录不存在或已不可用',404)
def text(value, field, max_len=160, required=True):
    require(isinstance(value,str),'INVALID_FIELD',f'{field} 必须是文本',400,field)
    s=value.strip()
    require((not required or bool(s)) and len(s)<=max_len,'INVALID_FIELD',f'{field} 不能为空且最多 {max_len} 字符',422,field)
    return s

def qty(value, field='qty', positive=True):
    try: d=Decimal(str(value))
    except (InvalidOperation, ValueError): fail('INVALID_NUMBER','请输入有效数字',422,field)
    require(d.is_finite() and abs(d)<Decimal('1000000000000') and d==d.quantize(Decimal('0.000001')),'INVALID_NUMBER','最多十二位整数和六位小数',422,field)
    require(d>0 if positive else d>=0,'INVALID_NUMBER','数值必须大于 0' if positive else '数值不能小于 0',422,field)
    return d

def day(value, field='date', optional=False):
    if not value and optional: return None
    try: return date.fromisoformat(value)
    except (ValueError,TypeError): fail('INVALID_DATE','请输入 YYYY-MM-DD 日期',422,field)
def version(record, data):
    require(type(data.get('expected_version')) is int and data['expected_version']==record.version,'VERSION_CONFLICT','记录已更新，请刷新后核对再提交',409,'expected_version')
def jsonable(x): return json.loads(json.dumps(x, cls=DjangoJSONEncoder))
def snapshot(record):
    data=jsonable({f.attname:getattr(record,f.attname) for f in record._meta.fields if f.name not in ['password','last_login']})
    if record.pk and record._meta.model_name in ['purchaserequest','purchaseorder','receipt','stockmovement']:
        data['lines']=[snapshot(x) for x in record.lines.order_by('line_no')]
    return data
def audit(user, record, action, request_id, before=None, reason=''):
    AuditEvent.objects.create(actor=user,entity_type=record._meta.model_name,entity_id=record.id,action=action,before_json=before,after_json=snapshot(record),request_id=request_id,reason=reason)
def save_change(user, record, rid, before, action='UPDATE',reason=''):
    record.version+=1
    if hasattr(record,'updated_by'): record.updated_by=user
    record.save()
    audit(user,record,action,rid,before,reason)
def new(model, user, rid, **fields):
    record=model.objects.create(created_by=user,updated_by=user,**fields)
    audit(user,record,'CREATE',rid)
    return record

def number(prefix): return f'{prefix}-{timezone.localdate():%y%m%d}-{uuid.uuid4().hex[:8].upper()}'
def digest(data): return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',',':'),cls=DjangoJSONEncoder).encode()).hexdigest()
def idempotent(user,key,kind,data,fn):
    key=text(key or '', 'Idempotency-Key',128)
    hashed=digest({'actor':str(user.id),'kind':kind,'data':data})
    cached=CommandResult.objects.filter(key=key).first()
    if cached:
        require(cached.request_hash==hashed,'IDEMPOTENCY_CONFLICT','相同幂等键已用于不同请求',409)
        return cached.result_json
    result=jsonable(fn())
    CommandResult.objects.create(key=key,request_hash=hashed,result_json=result)
    return result

def atomic_command(fn):
    @functools.wraps(fn)
    def wrapped(user,*args,**kwargs):
        with transaction.atomic():
            # One ordered command lock makes cross-module state changes serializable.
            # SQLite uses BEGIN IMMEDIATE; PostgreSQL locks this singleton row.
            RuntimeState.objects.select_for_update().get(pk=1)
            user=User.objects.get(pk=user.pk)
            require(user.is_active,'UNAUTHORIZED','账号已停用',401)
            return fn(user,*args,**kwargs)
    return wrapped
