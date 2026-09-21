import json, hashlib, uuid, functools, logging, inspect, time, random
from contextvars import ContextVar
from django.db import connection, OperationalError
from .locking import advisory, command_locks
from .telemetry import command_span, REPLAYS
from decimal import Decimal, InvalidOperation
from datetime import date
from django.db import transaction
from django.core.serializers.json import DjangoJSONEncoder
from django.core.exceptions import ValidationError
from django.utils import timezone
from .models import User, RuntimeState, AuditEvent, CommandResult, Project, Task

ROLES = {'ADMIN':'Administrator','MANAGER':'Project manager','BUYER':'Purchasing officer','STORE':'Warehouse operator','TECH':'Lab technician','AUDITOR':'Auditor'}
class BusinessError(Exception):
    def __init__(self, code, message, status=422, field=''):
        self.code,self.message,self.status,self.field = code,message,status,field

def fail(code, message, status=422, field=''): raise BusinessError(code,message,status,field)
def require(condition, code, message, status=422, field=''):
    if not condition: fail(code,message,status,field)
def roles(user): return set(user.groups.values_list('name',flat=True))
def allow(user, *allowed):
    require(user.is_active, 'UNAUTHORIZED','Account is inactive',401)
    require(bool(roles(user) & set(allowed)), 'FORBIDDEN','Your role does not permit this action',403)
def project_scope(user, project, write=False):
    r = roles(user)
    if 'ADMIN' in r: return
    if not write and 'AUDITOR' in r: return
    require(bool(r & {'MANAGER','TECH'}) and project.members.filter(id=user.id).exists(), 'NOT_FOUND','Project not found or access denied',404)
def visible_projects(user):
    if roles(user) & {'ADMIN','AUDITOR'}: return Project.objects.all()
    if roles(user) & {'MANAGER','TECH'}: return Project.objects.filter(members=user).distinct()
    return Project.objects.none()
def require_open(project):
    require(project.status not in ['ARCHIVED','COMPLETED','CANCELLED'],'PROJECT_READ_ONLY','The project has ended and cannot be edited')
def obj(model, id):
    try: return model.objects.get(pk=id)
    except (model.DoesNotExist, ValueError, ValidationError): fail('NOT_FOUND','Record not found or no longer available',404)
def text(value, field, max_len=160, required=True):
    require(isinstance(value,str),'INVALID_FIELD',f'{field} must be text',400,field)
    s=value.strip()
    require((not required or bool(s)) and len(s)<=max_len,'INVALID_FIELD',f'{field} is required and must not exceed {max_len} characters',422,field)
    return s

def qty(value, field='qty', positive=True):
    try: d=Decimal(str(value))
    except (InvalidOperation, ValueError): fail('INVALID_NUMBER','Enter a valid number',422,field)
    require(d.is_finite() and abs(d)<Decimal('1000000000000') and d==d.quantize(Decimal('0.000001')),'INVALID_NUMBER','Use at most 12 integer digits and 6 decimal places',422,field)
    require(d>0 if positive else d>=0,'INVALID_NUMBER','Value must be greater than 0' if positive else 'Value must be at least 0',422,field)
    return d

def day(value, field='date', optional=False):
    if not value and optional: return None
    try: return date.fromisoformat(value)
    except (ValueError,TypeError): fail('INVALID_DATE','Enter a date in YYYY-MM-DD format',422,field)
def version(record, data):
    require(type(data.get('expected_version')) is int and data['expected_version']==record.version,'VERSION_CONFLICT','Record has changed. Refresh, review, and submit again',409,'expected_version')
def jsonable(x): return json.loads(json.dumps(x, cls=DjangoJSONEncoder))
def snapshot(record):
    data=jsonable({f.attname:getattr(record,f.attname) for f in record._meta.fields if f.name not in ['password','last_login']})
    if record.pk and record._meta.model_name in ['purchaserequest','purchaseorder','receipt','stockmovement']:
        data['lines']=[snapshot(x) for x in (record.prefetched_lines if hasattr(record,'prefetched_lines') else record.lines.order_by('line_no'))]
    return data
def audit(user, record, action, request_id, before=None, reason=''):
    AuditEvent.objects.create(actor=user,entity_type=record._meta.model_name,entity_id=record.id,action=action,before_json=before,after_json=snapshot(record),request_id=request_id,reason=reason)
def save_change(user, record, rid, before, action='UPDATE',reason=''):
    record.version+=1
    if hasattr(record,'updated_by'): record.updated_by=user
    record.save()
    if record._meta.model_name == 'item':
        from .cache import invalidate_catalog
        transaction.on_commit(invalidate_catalog)
    audit(user,record,action,rid,before,reason)
def new(model, user, rid, **fields):
    record=model.objects.create(created_by=user,updated_by=user,**fields)
    if record._meta.model_name == 'item':
        from .cache import invalidate_catalog
        transaction.on_commit(invalidate_catalog)
    audit(user,record,'CREATE',rid)
    return record

def number(prefix): return f'{prefix}-{timezone.localdate():%y%m%d}-{uuid.uuid4().hex[:8].upper()}'
def digest(data): return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',',':'),cls=DjangoJSONEncoder).encode()).hexdigest()
def idempotent(user,key,kind,data,fn):
    key=text(key or '', 'Idempotency-Key',128)
    hashed=digest({'actor':str(user.id),'kind':kind,'data':data})
    advisory('idempotency:' + key)
    cached=CommandResult.objects.filter(key=key).first()
    if cached:
        require(cached.request_hash==hashed,'IDEMPOTENCY_CONFLICT','This idempotency key was used for a different request',409)
        REPLAYS.inc()
        return cached.result_json
    result=jsonable(fn())
    CommandResult.objects.create(key=key,request_hash=hashed,result_json=result)
    return result

_command_depth = ContextVar('command_depth', default=0)

def atomic_command(fn):
    signature = inspect.signature(fn)
    @functools.wraps(fn)
    def wrapped(user, *args, **kwargs):
        outer = not connection.in_atomic_block
        for attempt in range(3 if outer else 1):
            try:
                with command_span(fn.__name__), transaction.atomic():
                    depth = _command_depth.get()
                    token = _command_depth.set(depth + 1)
                    try:
                        if depth == 0:
                            if connection.vendor == 'postgresql':
                                with connection.cursor() as cursor:
                                    cursor.execute("SET LOCAL lock_timeout = '5s'")
                            exclusive = fn.__name__ in {'write_master', 'write_user'} or getattr(fn, 'catalog_write', False)
                            advisory('catalog-write-gate', shared=not exclusive)
                        values = signature.bind(user, *args, **kwargs).arguments
                        if values.get('key'):
                            advisory('idempotency:' + str(values['key']))
                        command_locks(fn.__name__, values)
                        user = User.objects.get(pk=user.pk)
                        require(user.is_active, 'UNAUTHORIZED', 'Account is inactive', 401)
                        return fn(user, *args, **kwargs)
                    finally:
                        _command_depth.reset(token)
            except OperationalError as exc:
                code = getattr(exc.__cause__, 'sqlstate', None)
                if not outer or code not in {'40P01', '40001'} or attempt == 2:
                    raise
                time.sleep(random.uniform(.01, .04) * (attempt + 1))
    return wrapped
