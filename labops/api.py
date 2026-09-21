import json, uuid, csv, io, hashlib
from datetime import timedelta
from django.http import JsonResponse,HttpResponse
from django.shortcuts import render,redirect
from django.contrib.auth import authenticate,login,logout
from django.views.decorators.csrf import ensure_csrf_cookie
from django.db import IntegrityError,OperationalError
from django.db.models import Q
from django.core.exceptions import ObjectDoesNotExist,ValidationError
from labops.common import *
from labops.models import *
from labops import queries
from labops.catalog import services as catalog
from labops.projects import services as projects
from labops.purchasing import services as purchasing
from labops.inventory import services as inventory
from labops.operations import services as operations
from labops.samples import services as samples

@ensure_csrf_cookie
def login_page(request):
    if request.user.is_authenticated: return redirect('/')
    error=''
    if request.method=='POST':
        email=request.POST.get('email','').strip().lower(); password=request.POST.get('password','')
        from .cache import rate_limit
        try: rate_limit('login',email,10,300)
        except BusinessError as exc: return render(request,'login.html',{'error':exc.message},status=exc.status)
        key=hashlib.sha256(email.encode()).hexdigest(); now=timezone.now()
        attempt,_=LoginAttempt.objects.get_or_create(key=key)
        if attempt.blocked_until and attempt.blocked_until>now: error='Too many sign-in attempts. Wait 5  minutes before retrying'
        else:
            user=authenticate(request,username=email,password=password)
            if user and user.is_active:
                attempt.delete(); login(request,user); return redirect('/')
            attempt.failures+=1
            if attempt.failures>=5: attempt.blocked_until=now+timedelta(minutes=5); attempt.failures=0
            attempt.save(); error='Incorrect email or password, or inactive account'
    return render(request,'login.html',{'error':error})

def logout_page(request):
    if request.method!='POST': return HttpResponse(status=405)
    logout(request); return redirect('/login/')

@ensure_csrf_cookie
def app_page(request,page=''):
    if not request.user.is_authenticated: return redirect('/login/')
    return render(request,'app.html')

def error_response(code,message,status,rid,field=''):
    return JsonResponse({'error':{'code':code,'message':message,'field_errors':{field:message} if field else {}},'request_id':rid},status=status)
def csrf_failure(request,reason=''):
    if request.path.startswith('/api/'): return error_response('CSRF_FAILED','Session verification failed. Refresh the page and try again',403,str(uuid.uuid4()))
    return render(request,'login.html',{'error':'Session expired. Refresh the page and sign in again'},status=403)

MODELS={'items':Item,'suppliers':Supplier,'warehouses':Warehouse,'projects':Project,'tasks':Task,'purchase-requests':PurchaseRequest,'purchase-orders':PurchaseOrder,'receipts':Receipt,'movements':StockMovement,'balances':StockBalance,'import-jobs':ImportJob,'audit':AuditEvent,'notifications':Notification,'events':OutboxEvent,'users':User,'lab-orders':LabOrder,'samples':Sample,'test-catalog':TestCatalog}
SEARCH_FIELDS={'items':['code','name'],'suppliers':['code','name','email'],'warehouses':['code','name','location'],'projects':['code','name'],'tasks':['title','project__name'],'purchase-requests':['request_no','reason'],'purchase-orders':['order_no','supplier__name'],'receipts':['receipt_no','order__order_no'],'movements':['movement_no','reason'],'balances':['batch__batch_no','batch__item__name','batch__item__code'],'import-jobs':['file_name'],'audit':['action','entity_type','request_id'],'notifications':['title','body'],'events':['event_type','last_error'],'users':['name','email'],'lab-orders':['order_no','test__name','project__name'],'samples':['barcode','order__order_no','order__project__name'],'test-catalog':['code','name']}

def scoped(user,kind):
    require(kind in MODELS,'NOT_FOUND','Endpoint not found',404)
    q=MODELS[kind].objects.all(); r=roles(user)
    if kind in ['users','events']: allow(user,'ADMIN')
    if kind in ['audit','import-jobs']: allow(user,'ADMIN','AUDITOR')
    if kind=='projects': q=visible_projects(user)
    if kind=='tasks': q=q.filter(project__in=visible_projects(user))
    if kind=='purchase-requests' and not r&{'ADMIN','BUYER','STORE','AUDITOR'}: q=q.filter(project__in=visible_projects(user))
    if kind in ['purchase-orders','receipts']: allow(user,'ADMIN','BUYER','STORE','AUDITOR')
    if kind=='movements' and not r&{'ADMIN','STORE','AUDITOR'}:
        allow(user,'MANAGER','TECH'); q=q.filter(lines__task__project__in=visible_projects(user)).distinct()
    if kind=='notifications': q=q.filter(user=user)
    if kind=='lab-orders': q=q.filter(project__in=visible_projects(user))
    if kind=='samples': q=q.filter(order__project__in=visible_projects(user))
    return q

def list_records(request,kind):
    user=request.user; params=request.GET; qtext=params.get('q','').strip()
    require(len(qtext)<=160,'INVALID_QUERY','Search text must not exceed 160 characters',400)
    try: page=int(params.get('page',1)); size=int(params.get('page_size',20))
    except ValueError: fail('INVALID_PAGINATION','Pagination parameters must be integers',400)
    require(page>0 and 1<=size<=100,'INVALID_PAGINATION','Page size must be between 1 and 100',400)
    today=timezone.localdate()
    if kind=='inventory':
        data=queries.inventory_overview()
        if qtext: data=[x for x in data if qtext.lower() in (x['code']+x['name']).lower()]
        if params.get('low_stock')=='true': data=[x for x in data if x['low_stock']]
        total=len(data); rows=data[(page-1)*size:page*size]
    elif kind=='reports':
        data=queries.costs(user,params); total=len(data); rows=data[(page-1)*size:page*size]
    else:
        qs=scoped(user,kind)
        if qtext:
            lookup=Q()
            for field in SEARCH_FIELDS[kind]: lookup|=Q(**{field+'__icontains':qtext})
            qs=qs.filter(lookup)
        if params.get('status'):
            require(kind in ['projects','tasks','purchase-requests','purchase-orders','receipts','movements','import-jobs','events','lab-orders','samples'],'INVALID_FILTER','This list does not support status filtering',400)
            require(len(params['status'])<=32,'INVALID_FILTER','Invalid status parameter',400)
            qs=qs.filter(status=params['status'])
        if params.get('is_active'):
            require(kind in ['items','suppliers','warehouses','users'] and params['is_active'] in ['true','false'],'INVALID_FILTER','Invalid active-status filter',400)
            qs=qs.filter(is_active=params['is_active']=='true')
        for field in ['project_id','warehouse_id','batch_id','created_by_id']:
            if params.get(field):
                allowed={'project_id':['tasks','purchase-requests'],'warehouse_id':['balances'],'batch_id':['balances'],'created_by_id':['purchase-requests','movements']}
                require(kind in allowed[field],'INVALID_FILTER','This filter field is not supported by the list',400)
                qs=qs.filter(**{field:params[field]})
        if kind=='tasks' and params.get('overdue')=='true': qs=qs.filter(due_date__lt=today).exclude(status__in=['DONE','CANCELLED'])
        if kind=='balances':
            if params.get('expiring')=='true': qs=qs.filter(on_hand_qty__gt=0,batch__expires_on__gte=today,batch__expires_on__lte=today+timedelta(days=30))
            if params.get('expired')=='true': qs=qs.filter(on_hand_qty__gt=0,batch__expires_on__lt=today)
            if params.get('item_id'): qs=qs.filter(batch__item_id=params['item_id'])
        if kind=='notifications' and params.get('unread')=='true': qs=qs.filter(read_at__isnull=True)
        if kind=='audit':
            if params.get('actor_id'): qs=qs.filter(actor_id=params['actor_id'])
            if params.get('entity_type'): qs=qs.filter(entity_type=text(params['entity_type'],'entity_type',64))
            if params.get('action'): qs=qs.filter(action=text(params['action'],'action',64))
            if params.get('from_date'): qs=qs.filter(created_at__date__gte=day(params['from_date']))
            if params.get('to_date'): qs=qs.filter(created_at__date__lte=day(params['to_date']))
        sort=params.get('sort','-updated_at' if kind=='balances' else ('-date_joined' if kind=='users' else '-created_at'))
        allowed_sorts=['-updated_at','updated_at'] if kind=='balances' else (['-date_joined','date_joined'] if kind=='users' else ['-created_at','created_at'])
        require(sort in allowed_sorts,'INVALID_SORT','Unsupported sort field',400)
        qs=queries.prepare_queryset(qs,kind).order_by(sort,'-id' if sort.startswith('-') else 'id'); total=qs.count(); rows=[queries.serialize(x) for x in qs[(page-1)*size:page*size]]
    return rows,{'page':page,'page_size':size,'total':total}

def references(user,request):
    kind=request.GET.get('kind','items'); qtext=request.GET.get('q','')[:160]
    if kind=='people':
        q=User.objects.filter(is_active=True)
        if qtext: q=q.filter(Q(name__icontains=qtext)|Q(email__icontains=qtext))
        return [{'id':str(x.id),'name':x.name} for x in q.order_by('name','id')[:100]]
    if kind=='items' and not qtext:
        from .cache import cached_catalog
        return cached_catalog(lambda:[queries.serialize(x) for x in Item.objects.filter(is_active=True).select_related('created_by').order_by('code')[:100]])
    q=scoped(user,kind)
    if kind in ['items','suppliers','warehouses']: q=q.filter(is_active=True)
    if kind=='purchase-requests': q=q.filter(status='APPROVED')
    if kind=='purchase-orders': q=q.filter(status__in=['CONFIRMED','CLOSED'])
    if kind=='tasks':
        if roles(user)&{'STORE'}: q=Task.objects.all()
        q=q.filter(status='IN_PROGRESS',project__status='ACTIVE')
    if kind=='balances': q=q.filter(on_hand_qty__gt=0).order_by('batch__expires_on','batch_id','warehouse_id')
    if qtext:
        cond=Q()
        for f in SEARCH_FIELDS[kind]: cond|=Q(**{f+'__icontains':qtext})
        q=q.filter(cond)
    return [queries.serialize(x) for x in q[:100]]

def dispatch(request,route):
    rid=str(uuid.uuid4()); status=200
    try:
        require(request.user.is_authenticated and request.user.is_active,'UNAUTHORIZED','Please sign in',401)
        require(bool(roles(request.user)),'FORBIDDEN','No role has been assigned to this account',403)
        route=route.strip('/'); parts=route.split('/'); kind=parts[0]; uid=parts[1] if len(parts)>1 else None; action=parts[2] if len(parts)>2 else None
        from .cache import rate_limit
        if route=='reports': rate_limit('reports',request.user.pk,60,60)
        if kind=='import-jobs' and request.method=='POST': rate_limit('imports',request.user.pk,10,60)
        if request.method=='GET':
            if route=='me': result=queries.serialize(request.user)
            elif route=='dashboard': result=queries.dashboard(request.user)
            elif route=='references': result=references(request.user,request)
            elif route=='stock/reconcile': allow(request.user,'ADMIN','AUDITOR'); result={'differences':inventory.reconcile()}
            elif route=='system': result={'opening_closed':RuntimeState.objects.get(pk=1).opening_closed,'timezone':'America/New_York','currency':'USD','roles':ROLES}
            elif kind=='import-template':
                allow(request.user,'ADMIN'); require(uid in catalog.MODELS,'INVALID_ENTITY','Template not found',404)
                fields={'items':['code','name','base_uom','reorder_qty','is_active'],'suppliers':['code','name','email','contact_name','is_active'],'warehouses':['code','name','location','is_active']}[uid]
                buf=io.StringIO(); csv.writer(buf).writerow(fields)
                response=HttpResponse('\ufeff'+buf.getvalue(),content_type='text/csv; charset=utf-8'); response['Content-Disposition']=f'attachment; filename="{uid}-template.csv"'; return response
            elif kind=='import-jobs' and action=='errors':
                job=scoped(request.user,kind).filter(pk=uid).first(); require(job is not None,'NOT_FOUND','Import job not found',404)
                buf=io.StringIO(); writer=csv.writer(buf); writer.writerow(['row_no','status','input','errors'])
                for x in job.rows.order_by('row_no'): writer.writerow([x.row_no,x.status,operations.csv_safe(json.dumps(x.input_json,ensure_ascii=False)),operations.csv_safe(json.dumps(x.errors_json,ensure_ascii=False))])
                response=HttpResponse('\ufeff'+buf.getvalue(),content_type='text/csv; charset=utf-8'); response['Content-Disposition']='attachment; filename="import-results.csv"'; return response
            elif uid:
                record=scoped(request.user,kind).filter(pk=uid).first(); require(record is not None,'NOT_FOUND','Record not found or access denied',404); result=queries.serialize(record,True)
            else:
                result,pagination=list_records(request,kind)
                return JsonResponse({'data':result,'pagination':pagination,'request_id':rid})
        else:
            require(request.method in ['POST','PATCH'],'METHOD_NOT_ALLOWED','Unsupported request method',405)
            if kind=='import-jobs' and not uid:
                require(request.method=='POST','METHOD_NOT_ALLOWED','Use POST',405)
                file=request.FILES.get('file'); require(file is not None,'FILE_REQUIRED','Select a CSV file',400)
                result=operations.import_preview(request.user,request.POST.get('entity_type'),request.POST.get('mode'),file.name,file.read(),json.loads(request.POST.get('mapping','{}')),request.headers.get('Idempotency-Key'),rid)
                status=201
            else:
                try: data=json.loads(request.body or '{}')
                except (json.JSONDecodeError,UnicodeDecodeError): fail('INVALID_JSON','Invalid JSON request format',400)
                require(isinstance(data,dict),'INVALID_JSON','Request body must be an object',400)
                user=request.user; key=request.headers.get('Idempotency-Key')
                # Creation commands have stable retries, in addition to inventory posting keys.
                def write():
                    if kind in catalog.MODELS and not action:
                        require((uid and request.method=='PATCH') or (not uid and request.method=='POST'),'METHOD_NOT_ALLOWED','Create with POST; update with PATCH',405)
                        return catalog.write_master(user,kind,data,rid,uid)
                    if kind=='lab-orders': return samples.order_transition(user,uid,data,rid) if action=='transition' else samples.create_order(user,data,rid)
                    if kind=='samples': return samples.transition(user,uid,data,rid) if action=='transition' else samples.register(user,data,rid)
                    if kind=='users' and not action: return catalog.write_user(user,data,rid,uid)
                    if kind=='projects': return projects.project_action(user,uid,action,data,rid) if action else projects.write_project(user,data,rid,uid)
                    if kind=='tasks': return projects.task_action(user,uid,action,data,rid) if action else projects.write_task(user,data,rid,uid)
                    if kind=='purchase-requests': return purchasing.request_action(user,uid,action,data,rid) if action else purchasing.write_request(user,data,rid,uid)
                    if kind=='purchase-orders':
                        if action: return purchasing.order_action(user,uid,action,data,rid)
                        require(not uid,'METHOD_NOT_ALLOWED','Use confirm or cancel after creating an order',405); return purchasing.write_order(user,data,rid)
                    if kind=='receipts':
                        if action=='post': return inventory.post_receipt(user,uid,{**data,'receipt_id':uid},key,rid)
                        require(not uid,'METHOD_NOT_ALLOWED','Posted receipts are read-only',405); return purchasing.create_receipt(user,data,rid)
                    if route=='stock/issues/drafts': return inventory.issue_draft(user,data,rid)
                    if route.startswith('stock/issues/drafts/'): return inventory.issue_draft(user,data,rid,parts[3])
                    if route=='stock/issues': return inventory.issue(user,data,key,rid)
                    if route=='stock/transfers': return inventory.transfer(user,data,key,rid)
                    if route=='stock/adjustments': return inventory.adjustment(user,data,key,rid)
                    if route=='stock/opening': return inventory.opening(user,data,key,rid)
                    if kind=='stock' and uid=='movements' and len(parts)==4 and parts[3]=='reverse': return inventory.reverse(user,parts[2],{**data,'movement_id':parts[2]},key,rid)
                    if kind=='notifications' and action=='read':
                        n=Notification.objects.filter(pk=uid,user=user).first(); require(n is not None,'NOT_FOUND','Notification not found',404); n.read_at=timezone.now(); n.save(update_fields=['read_at']); return n
                    if kind=='events' and action=='retry':
                        allow(user,'ADMIN'); e=obj(OutboxEvent,uid); require(e.status=='DEAD','INVALID_TRANSITION','Only failed events can be requeued'); e.status='PENDING'; e.attempts=0; e.next_attempt_at=timezone.now(); e.save(); audit(user,e,'REQUEUE_EVENT',rid); return e
                    fail('NOT_FOUND','Endpoint not found',404)
                if kind=='import-jobs' and action=='execute':
                    require(data.get('confirmed') is True,'CONFIRM_REQUIRED','Confirm import execution')
                    result=operations.queue_import(user,uid,rid); status=202
                elif route=='operations/check':
                    allow(user,'ADMIN'); operations.check_alerts(); result={'processed':operations.consume_events()}
                else:
                    create=request.method=='POST' and ((not uid and kind not in ['notifications']) or route=='stock/issues/drafts')
                    def perform(actor):
                        nonlocal user
                        user=actor
                        if create:
                            return idempotent(user,key,route,data,lambda:queries.serialize(write(),True))
                        return write()
                    perform.catalog_write = kind in {*catalog.MODELS, 'users'}
                    result=atomic_command(perform)(user)
                    if create: status=201
            result=queries.serialize(result,True) if not isinstance(result,(dict,list)) else result
        return JsonResponse({'data':result,'request_id':rid},status=status)
    except BusinessError as e:
        logging.getLogger('labops').info('request_failed request_id=%s code=%s',rid,e.code)
        return error_response(e.code,e.message,e.status,rid,e.field)
    except (ValidationError,ValueError,TypeError,KeyError,AttributeError) as e:
        logging.getLogger('labops').warning('invalid_request request_id=%s type=%s',rid,type(e).__name__)
        return error_response('INVALID_INPUT','Invalid input format. Check the fields',400,rid)
    except ObjectDoesNotExist: return error_response('NOT_FOUND','Record not found',404,rid)
    except IntegrityError: return error_response('CONFLICT','Duplicate record or relationship constraint conflict. Refresh and review',409,rid)
    except OperationalError:
        logging.getLogger('labops').exception('database_unavailable request_id=%s',rid)
        return error_response('DATABASE_BUSY','Database is busy. Please try again shortly',503,rid)
    except Exception:
        logging.getLogger('labops').exception('request_error request_id=%s',rid)
        return error_response('INTERNAL_ERROR','Operation failed. Contact an administrator with the request ID',500,rid)
