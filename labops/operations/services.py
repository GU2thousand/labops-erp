import csv, io, hashlib
from datetime import timedelta
from django.db.models import Q
from labops.common import *
from labops.models import *
from labops.catalog.services import MODELS, normalized, validate_master, write_master

def stock_recipients(): return list(User.objects.filter(is_active=True,groups__name__in=['ADMIN','STORE']).values_list('id',flat=True).distinct())
def emit(kind,record,title,body,recipients,suffix=''):
    return OutboxEvent.objects.get_or_create(dedupe_key=f'{kind}:{record.id}:{suffix}',defaults=dict(event_type=kind,aggregate_type=record._meta.model_name,aggregate_id=record.id,payload_json={'title':title,'body':body,'recipients':[str(x) for x in set(recipients) if x]}))[0]

def check_alerts():
    from labops.queries import inventory_overview
    today=timezone.localdate()
    with transaction.atomic():
        advisory('daily-alerts')
        recipients=stock_recipients()
        for row in inventory_overview():
            if row['low_stock']: emit('LOW_STOCK',Item.objects.get(pk=row['id']),'Low item stock',f"{row['name']} · Available {row['available']} {row['base_uom']} / Threshold {row['reorder_qty']}",recipients,str(today))
        for b in Batch.objects.filter(expires_on__gte=today,expires_on__lte=today+timedelta(days=30),balances__on_hand_qty__gt=0).distinct():
            emit('EXPIRING',b,'Batch expiring soon',f'{b.batch_no} · {b.expires_on}',recipients,str(today))
        for t in Task.objects.filter(due_date__lt=today).exclude(status__in=['DONE','CANCELLED']).select_related('project'):
            emit('OVERDUE_TASK',t,'Task overdue',t.title,[t.assignee_id,t.project.owner_id],str(today))

def consume_events(limit=100):
    processed=0
    for _ in range(limit):
        now=timezone.now()
        with transaction.atomic():
            e=OutboxEvent.objects.select_for_update(skip_locked=True).filter(transport='local').filter(Q(status='PENDING',next_attempt_at__lte=now)|Q(status='PROCESSING',locked_until__lt=now)).order_by('next_attempt_at','id').first()
            if not e: break
            e.status='PROCESSING'; e.locked_until=now+timedelta(minutes=2); e.save()
            eid=e.id
        try:
            with transaction.atomic():
                e=OutboxEvent.objects.select_for_update().get(pk=eid)
                if e.status!='PROCESSING': continue
                if e.event_type.startswith('inventory.'):
                    from labops.events import envelope, process_envelope
                    for consumer in ['notification', 'analytics']: process_envelope(consumer, envelope(e))
                else:
                    for uid in e.payload_json['recipients']:
                        if User.objects.filter(pk=uid,is_active=True).exists(): Notification.objects.get_or_create(event=e,user_id=uid,defaults={'title':e.payload_json['title'],'body':e.payload_json['body']})
                e.status='PROCESSED'; e.processed_at=timezone.now(); e.locked_until=None; e.last_error=''; e.save(); processed+=1
        except Exception:
            logging.getLogger('labops').exception('outbox_delivery_failed event=%s',eid)
            with transaction.atomic():
                e=OutboxEvent.objects.select_for_update().get(pk=eid)
                e.attempts+=1; e.locked_until=None; e.last_error='Notification delivery failed. Check the worker logs'
                if e.attempts>5: e.status='DEAD'
                else: e.status='PENDING'; e.next_attempt_at=timezone.now()+timedelta(minutes=[1,5,15,60,240][e.attempts-1])
                e.save()
    return processed

@atomic_command
def import_preview(user,kind,mode,name,raw,mapping,key,rid):
    allow(user,'ADMIN')
    require(kind in MODELS and mode in ['CREATE','UPDATE'],'INVALID_IMPORT','Select a valid entity and import mode')
    require(len(raw)<=5*1024*1024,'FILE_TOO_LARGE','CSV file limit: 5 MB',400)
    sha=hashlib.sha256(raw).hexdigest()
    key=text(key or '', 'Idempotency-Key',128)
    old=ImportJob.objects.filter(idempotency_key=key).first()
    if old:
        require(old.file_sha256==sha and old.entity_type==kind and old.mode==mode,'IDEMPOTENCY_CONFLICT','This idempotency key was used for another import',409)
        return old
    try: decoded=raw.decode('utf-8-sig')
    except UnicodeDecodeError: fail('INVALID_ENCODING','Upload a UTF-8 encoded CSV',400)
    try:
        reader=csv.DictReader(io.StringIO(decoded),strict=True)
        require(bool(reader.fieldnames),'INVALID_CSV','CSV is missing column headers',400)
        require(len(set(reader.fieldnames))==len(reader.fieldnames),'INVALID_CSV','CSV contains duplicate column headers',400)
        inputs=[]
        for row in reader:
            require(None not in row and all(v is not None for v in row.values()),'INVALID_CSV','CSV column count does not match the header',400)
            inputs.append(row)
            require(len(inputs)<=5000,'TOO_MANY_ROWS','CSV supports at most 5000 rows',400)
    except csv.Error: fail('INVALID_CSV','CSV format error',400)
    require(bool(inputs),'EMPTY_CSV','CSV has no data rows',400)
    job=new(ImportJob,user,rid,entity_type=kind,mode=mode,file_name=name[:255],file_sha256=sha,status='VALIDATING',total_rows=len(inputs),idempotency_key=key)
    codes=[text({mapping.get(k,k):v for k,v in row.items()}.get('code',''),'code',64,False).upper() for row in inputs]
    counts={c:codes.count(c) for c in set(codes)}
    bad=0
    for n,row in enumerate(inputs,1):
        mapped={mapping.get(k,k):v for k,v in row.items()}
        errors=[]; fields=None; expected=None
        try:
            require(counts[codes[n-1]]==1,'DUPLICATE_CODE','Duplicate code within the same file',422,'code')
            fields=normalized(kind,mapped)
            existing=MODELS[kind].objects.filter(code=fields['code']).first()
            if mode=='CREATE': require(existing is None,'DUPLICATE_CODE','CREATE mode cannot use an existing code',409,'code')
            else:
                require(existing is not None,'NOT_FOUND','UPDATE mode requires an existing code',422,'code')
                expected=existing.version
            validate_master(kind,fields,existing if mode=='UPDATE' else None)
        except BusinessError as e: errors=[{'field':e.field,'code':e.code,'message':e.message}]
        bad+=bool(errors)
        ImportRow.objects.create(job=job,row_no=n,input_json=row,normalized_json=jsonable(fields),errors_json=errors,status='INVALID' if errors else 'READY',expected_version=expected)
    before=snapshot(job); job.status='INVALID' if bad else 'READY'; job.failed_rows=bad; save_change(user,job,rid,before,'VALIDATE_IMPORT')
    return job

def execute_import(user,id,rid):
    allow(user,'ADMIN'); job=obj(ImportJob,id)
    require(job.status in ['READY','RUNNING','COMPLETED','PARTIAL_FAILED','FAILED'],'IMPORT_NOT_READY','Only jobs with all rows passing preflight can run')
    if job.status=='COMPLETED': return job
    with transaction.atomic():
        ImportJob.objects.select_for_update().get(pk=id)
        job=ImportJob.objects.get(pk=id); job.status='RUNNING'; job.started_at=job.started_at or timezone.now(); job.save()
    for row_id in job.rows.exclude(status='SUCCEEDED').order_by('row_no').values_list('id',flat=True):
        try:
            with transaction.atomic():
                advisory('catalog-write-gate')
                row=ImportRow.objects.select_for_update().get(pk=row_id)
                if row.status=='SUCCEEDED': continue
                data=row.normalized_json
                target=None
                if job.mode=='UPDATE':
                    target=MODELS[job.entity_type].objects.filter(code=data['code']).first()
                    require(target is not None,'NOT_FOUND','The target record no longer exists')
                    data={**data,'expected_version':row.expected_version}
                result=write_master(user,job.entity_type,data,rid,id=target.id if target else None)
                row.status='SUCCEEDED'; row.result_id=result.id; row.errors_json=[]; row.save()
                audit(user,row,'IMPORT_ROW',rid)
        except (BusinessError,ValueError,ValidationError) as e:
            with transaction.atomic():
                ImportRow.objects.select_for_update().get(pk=row_id)
                row=ImportRow.objects.get(pk=row_id)
                if row.status=='SUCCEEDED': continue
                row.status='FAILED'; row.errors_json=[{'field':getattr(e,'field',''),'code':getattr(e,'code','INVALID_ROW'),'message':getattr(e,'message','Invalid row data')}]; row.save()
    with transaction.atomic():
        ImportJob.objects.select_for_update().get(pk=id)
        job=ImportJob.objects.get(pk=id); before=snapshot(job)
        job.success_rows=job.rows.filter(status='SUCCEEDED').count(); job.failed_rows=job.total_rows-job.success_rows
        job.status='COMPLETED' if not job.failed_rows else ('PARTIAL_FAILED' if job.success_rows else 'FAILED')
        job.finished_at=timezone.now(); save_change(user,job,rid,before,'EXECUTE_IMPORT')
    return job

def csv_safe(value):
    s=str(value if value is not None else '')
    return "'"+s if s.lstrip().startswith(('=','+','-','−','@','\t','\r')) else s

@atomic_command
def queue_import(user,id,rid):
    allow(user,'ADMIN');job=obj(ImportJob,id)
    require(job.status in ['READY','RUNNING','COMPLETED','PARTIAL_FAILED','FAILED'],'IMPORT_NOT_READY','Correct preflight errors and upload the file again')
    if job.status in ['RUNNING','COMPLETED']:return job
    before=snapshot(job);job.status='RUNNING';job.started_at=job.started_at or timezone.now();job.finished_at=None;save_change(user,job,rid,before,'QUEUE_IMPORT')
    return job

def process_imports():
    for job in ImportJob.objects.filter(status='RUNNING').select_related('created_by').order_by('created_at','id')[:5]:
        try:execute_import(job.created_by,job.id,'import-worker-'+str(job.id))
        except BusinessError:
            with transaction.atomic():
                ImportJob.objects.select_for_update().get(pk=job.pk)
                job=ImportJob.objects.get(pk=job.pk)
                job.status='FAILED';job.failed_rows=job.rows.exclude(status='SUCCEEDED').count();job.success_rows=job.total_rows-job.failed_rows;job.save()
                for row in job.rows.exclude(status='SUCCEEDED'):
                    row.status='FAILED';row.errors_json=[{'field':'','code':'FORBIDDEN','message':'The job creator is inactive or no longer an administrator'}];row.save()
