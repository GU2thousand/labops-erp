import os,json,time,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
os.environ.setdefault('DJANGO_SETTINGS_MODULE','config.settings')
import django
django.setup()
from labops.models import OutboxEvent,ProcessedEvent,InventoryProjection,StockBalance,FailedDelivery
from labops.inventory.services import reconcile
for attempt in range(60):
    events=OutboxEvent.objects.filter(transport='kafka')
    count=events.count()
    pending=events.exclude(status='PUBLISHED').count()
    consumers={c:ProcessedEvent.objects.filter(consumer_name=c,event_id__in=events.values('id')).count() for c in ['notification','analytics']}
    balances={(str(b.batch_id),str(b.warehouse_id)):b.on_hand_qty for b in StockBalance.objects.all()}
    projections={(str(b.batch_id),str(b.warehouse_id)):b.quantity for b in InventoryProjection.objects.all()}
    mismatches=[k for k in balances.keys()|projections.keys() if balances.get(k,0)!=projections.get(k,0)]
    if count>0 and pending==0 and all(x==count for x in consumers.values()) and not mismatches:break
    time.sleep(2)
result={'events':count,'pending':pending,'consumers':consumers,'projection_mismatches':mismatches,'ledger_differences':reconcile(),'failed_deliveries':FailedDelivery.objects.exclude(status='RESOLVED').count()}
print(json.dumps(result))
assert count>0 and pending==0 and all(x==count for x in consumers.values()),result
assert not mismatches and not result['ledger_differences'] and not result['failed_deliveries'],result
