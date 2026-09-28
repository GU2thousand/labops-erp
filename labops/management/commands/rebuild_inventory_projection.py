from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Sum
from labops.locking import advisory
from labops.models import StockMovementLine, InventoryProjection, OutboxEvent, ProcessedEvent, DeliveryAudit
from labops.events import envelope, canonical_payload_hash


class Command(BaseCommand):
    help = 'Explicitly rebuild analytics from the complete legal ledger under stock-write and analytics gates.'

    def add_arguments(self, p):
        p.add_argument('--actor', default='operator')
        p.add_argument('--reason', default='Explicit ledger projection rebuild')
        p.add_argument('--authorization', default='local maintenance command')

    @transaction.atomic
    def handle(self, *args, **options):
        advisory('catalog-write-gate')
        advisory('analytics-rebuild')
        totals = StockMovementLine.objects.filter(movement__status='POSTED').values('batch_id', 'warehouse_id').annotate(quantity=Sum('delta_qty'))
        InventoryProjection.objects.all().delete()
        InventoryProjection.objects.bulk_create([InventoryProjection(**row) for row in totals], batch_size=1000)
        checkpointed = 0
        for event in OutboxEvent.objects.filter(event_type__startswith='inventory.').iterator(chunk_size=1000):
            digest = canonical_payload_hash(envelope(event))
            marker, created = ProcessedEvent.objects.get_or_create(consumer_name='analytics', event_id=event.id,
                                                                   defaults={'payload_hash': digest})
            if not created:
                if marker.payload_hash and marker.payload_hash != digest: raise ValueError('Checkpoint content conflict')
                if not marker.payload_hash:
                    marker.payload_hash = digest; marker.save(update_fields=['payload_hash'])
            checkpointed += 1
        DeliveryAudit.objects.create(actor_label=options['actor'], action='REBUILD_PROJECTION', outcome='success',
            reason=options['reason'], before_json=None,
            after_json={'balances': InventoryProjection.objects.count(), 'checkpointed_events': checkpointed},
            authorization_json={'kind': 'operator', 'reference': options['authorization']})
        self.stdout.write(f'Rebuilt {InventoryProjection.objects.count()} balances; checkpointed {checkpointed} original inventory events.')
