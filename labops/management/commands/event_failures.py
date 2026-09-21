import json
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from labops.models import FailedDelivery

class Command(BaseCommand):
    help = 'Inspect/retry/resolve parked consumer events. Retry preserves event identity.'
    def add_arguments(self, p):
        p.add_argument('action', choices=['inspect', 'retry', 'resolve'])
        p.add_argument('--id')
        p.add_argument('--reason', default='')
    def handle(self, *args, **options):
        if options['action'] == 'inspect':
            self.stdout.write(json.dumps(list(FailedDelivery.objects.exclude(status='RESOLVED').values()), default=str, indent=2)); return
        if not options['id'] or not options['reason']: raise CommandError('--id and --reason are required')
        with transaction.atomic():
            row = FailedDelivery.objects.select_for_update().get(pk=options['id'])
            if options['action'] == 'retry':
                row.status = 'RETRY'; row.attempts = 0; row.next_attempt_at = timezone.now(); row.dlq_published_at = None
            else: row.status = 'RESOLVED'; row.resolved_at = timezone.now()
            row.resolution_note = options['reason']; row.save()
