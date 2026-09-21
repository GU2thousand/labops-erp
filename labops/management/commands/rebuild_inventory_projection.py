from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Sum
from labops.locking import advisory
from labops.models import StockMovementLine,InventoryProjection,OutboxEvent,ProcessedEvent

class Command(BaseCommand):
    help='Rebuild analytics from the complete ledger; briefly pauses stock writes and analytics consumption.'
    @transaction.atomic
    def handle(self,*args,**options):
        advisory('catalog-write-gate')
        advisory('analytics-rebuild')
        totals=StockMovementLine.objects.filter(movement__status='POSTED').values('batch_id','warehouse_id').annotate(quantity=Sum('delta_qty'))
        InventoryProjection.objects.all().delete()
        InventoryProjection.objects.bulk_create([InventoryProjection(**row) for row in totals],batch_size=1000)
        events=OutboxEvent.objects.filter(event_type__startswith='inventory.')
        ProcessedEvent.objects.bulk_create([ProcessedEvent(consumer_name='analytics',event_id=eid) for eid in events.values_list('pk',flat=True)],ignore_conflicts=True,batch_size=1000)
        self.stdout.write(f'Rebuilt {InventoryProjection.objects.count()} balances; existing inventory events are checkpointed.')
