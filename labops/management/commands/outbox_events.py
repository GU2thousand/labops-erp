import json, uuid
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from labops.models import OutboxEvent,AuditEvent,DeliveryAudit
from labops.common import snapshot

class Command(BaseCommand):
    help='Inspect or requeue DEAD outbox events without changing event identity.'
    def add_arguments(self,p):
        p.add_argument('action',choices=['inspect','retry'])
        p.add_argument('--id')
        p.add_argument('--reason',default='')
        p.add_argument('--actor',default='')
        p.add_argument('--authorization',default='')
    def handle(self,*args,**opts):
        if opts['action']=='inspect':
            self.stdout.write(json.dumps(list(OutboxEvent.objects.filter(status='DEAD').values()),default=str,indent=2));return
        if not all(opts[key].strip() for key in ('id','reason','actor','authorization')):raise CommandError('--id, --reason, --actor and --authorization are required')
        with transaction.atomic():
            event=OutboxEvent.objects.select_for_update().get(pk=opts['id'])
            if event.status!='DEAD':raise CommandError('Only DEAD events may be requeued')
            before=snapshot(event);event.status='PENDING';event.attempts=0;event.next_attempt_at=timezone.now();event.locked_until=None;event.lease_token=None;event.save()
            DeliveryAudit.objects.create(outbox=event,actor_label=opts['actor'],action='REQUEUE_OUTBOX',outcome='success',reason=opts['reason'],before_json=before,after_json=snapshot(event),original_hash=event.payload_hash,authorization_json={'kind':'operator','reference':opts['authorization']})
            AuditEvent.objects.create(entity_type='outboxevent',entity_id=event.id,action='REQUEUE_EVENT',before_json=before,after_json=snapshot(event),reason=opts['reason'],request_id='cli-'+str(uuid.uuid4()))
