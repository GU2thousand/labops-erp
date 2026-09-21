import json, uuid
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from labops.models import OutboxEvent,AuditEvent
from labops.common import snapshot

class Command(BaseCommand):
    help='Inspect or requeue DEAD outbox events without changing event identity.'
    def add_arguments(self,p):
        p.add_argument('action',choices=['inspect','retry'])
        p.add_argument('--id')
        p.add_argument('--reason',default='')
    def handle(self,*args,**opts):
        if opts['action']=='inspect':
            self.stdout.write(json.dumps(list(OutboxEvent.objects.filter(status='DEAD').values()),default=str,indent=2));return
        if not opts['id'] or not opts['reason']:raise CommandError('--id and --reason are required')
        with transaction.atomic():
            event=OutboxEvent.objects.select_for_update().get(pk=opts['id'])
            if event.status!='DEAD':raise CommandError('Only DEAD events may be requeued')
            before=snapshot(event);event.status='PENDING';event.attempts=0;event.next_attempt_at=timezone.now();event.locked_until=None;event.lease_token=None;event.save()
            AuditEvent.objects.create(entity_type='outboxevent',entity_id=event.id,action='REQUEUE_EVENT',before_json=before,after_json=snapshot(event),reason=opts['reason'],request_id='cli-'+str(uuid.uuid4()))
