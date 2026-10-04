import json
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from labops.models import FailedDelivery
from labops.events import record_audit, canonical_payload_hash


class Command(BaseCommand):
    help = 'Inspect or audit retry/resolve of retained failures. Original content and identity are immutable.'

    def add_arguments(self, p):
        p.add_argument('action', choices=['inspect', 'retry', 'resolve'])
        p.add_argument('--id')
        p.add_argument('--reason', default='')
        p.add_argument('--actor', default='')
        p.add_argument('--authorization', default='')
        p.add_argument('--include-payload', action='store_true', help='Restricted original evidence; omit in ordinary inspection')

    def handle(self, *args, **options):
        if options['action'] == 'inspect':
            fields = ['id', 'consumer_name', 'source_cluster', 'source_generation', 'delivery_key',
                      'status', 'attempts', 'failure_class', 'last_error', 'original_hash', 'created_at',
                      'next_attempt_at', 'resolved_at']
            if options['include_payload']: fields.append('envelope')
            self.stdout.write(json.dumps(list(FailedDelivery.objects.exclude(status='RESOLVED').values(*fields)), default=str, indent=2))
            return
        if not all(options[name].strip() for name in ('id', 'reason', 'actor', 'authorization')):
            raise CommandError('--id, --reason, --actor and --authorization are required for recovery writes')
        with transaction.atomic():
            try: row = FailedDelivery.objects.select_for_update().get(pk=options['id'])
            except (FailedDelivery.DoesNotExist, ValueError) as exc: raise CommandError('Failure not found') from exc
            now = timezone.now()
            if ((row.lease_token and row.locked_until and row.locked_until > now) or
                    (row.dlq_lease_token and row.dlq_locked_until and row.dlq_locked_until > now)):
                raise CommandError('Stop workers and wait for live leases before manual disposition')
            row.lease_token = None; row.locked_until = None
            row.dlq_lease_token = None; row.dlq_locked_until = None
            if row.original_hash and canonical_payload_hash(row.envelope) != row.original_hash:
                raise CommandError('Retained envelope was changed; restore original evidence before disposition')
            before = {'status': row.status, 'attempts': row.attempts}
            if options['action'] == 'retry':
                row.status = 'RETRY'; row.attempts = 0; row.next_attempt_at = timezone.now()
                row.dlq_published_at = None; row.resolved_at = None
            else:
                row.status = 'RESOLVED'; row.resolved_at = timezone.now()
            row.resolution_note = options['reason']; row.save()
            record_audit(row, 'MANUAL_'+options['action'].upper(), before, actor_label=options['actor'],
                         reason=options['reason'], authorization={'reference': options['authorization'], 'kind': 'operator'})
        self.stdout.write(f'{options["action"]}: {row.pk}; original identity retained')
