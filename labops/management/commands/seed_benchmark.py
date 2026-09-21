"""Deterministic, isolated benchmark data; never clears existing business data."""
import uuid
from datetime import timedelta
from decimal import Decimal
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import Group
from django.db import transaction
from django.utils import timezone
from labops.models import *

def uid(kind, index): return uuid.uuid5(uuid.NAMESPACE_URL, f'labops-benchmark-v1/{kind}/{index}')

class Command(BaseCommand):
    help = 'Seed an EMPTY benchmark database (1000 items, 10000 batches/orders, 100000 ledger lines).'
    def add_arguments(self,p):
        p.add_argument('--items',type=int,default=1000)
        p.add_argument('--batches',type=int,default=10000)
        p.add_argument('--lines',type=int,default=100000)
        p.add_argument('--orders',type=int,default=10000)
    @transaction.atomic
    def handle(self,*args,**o):
        if Item.objects.exists() or User.objects.exists():raise CommandError('Requires an empty isolated database; refuses to overwrite data.')
        ni,nb,nl,no=o['items'],o['batches'],o['lines'],o['orders']
        if not(0<ni<=nb<=nl and nl%nb==0 and nl//nb<1000 and no>0):raise CommandError('Require items <= batches <= lines; lines divisible by batches, fewer than 1000 lines per batch.')
        today=timezone.localdate();timestamp=timezone.now()
        admin=User.objects.create_user(id=uid('user',0),username='benchmark@labops.local',email='benchmark@labops.local',name='Benchmark Admin',password='BenchmarkLocal!2026')
        buyer=User.objects.create_user(id=uid('user',1),username='buyer@benchmark.local',email='buyer@benchmark.local',name='Benchmark Buyer',password='BenchmarkLocal!2026')
        for user,role in [(admin,'ADMIN'),(buyer,'BUYER')]:user.groups.add(Group.objects.get_or_create(name=role)[0])
        supplier=Supplier.objects.create(id=uid('supplier',0),code='BENCH',name='Benchmark Supplier',created_by=admin)
        warehouse=Warehouse.objects.create(id=uid('warehouse',0),code='BENCH',name='Benchmark Warehouse',created_by=admin)
        p=Project.objects.create(id=uid('project',0),code='BENCH',name='Benchmark Project',owner=admin,status='ACTIVE',created_by=admin)
        ProjectMember.objects.create(project=p,user=admin)
        task=Task.objects.create(id=uid('task',0),project=p,title='Benchmark Task',status='IN_PROGRESS',assignee=admin,created_by=admin)
        Item.objects.bulk_create([Item(id=uid('item',i),code=f'BENCH-{i:05}',name=f'Benchmark Item {i}',base_uom='EA',reorder_qty=10,created_by=admin) for i in range(ni)],batch_size=1000)
        Batch.objects.bulk_create([Batch(id=uid('batch',i),item_id=uid('item',i%ni),batch_no=f'BENCH-{i:06}',unit_cost=Decimal('12.345678'),expires_on=today+timedelta(days=365),origin='OPENING',created_by=admin) for i in range(nb)],batch_size=1000)
        per=nl//nb
        for start in range(0,nl,5000):
            movements=[];lines=[]
            for i in range(start,min(start+5000,nl)):
                opening=i%per==0;mid=uid('movement',i);bid=uid('batch',i//per)
                movements.append(StockMovement(id=mid,movement_no=f'BENCH-{i:07}',type='OPENING' if opening else 'ISSUE',status='POSTED',posted_at=timestamp,created_by=admin))
                lines.append(StockMovementLine(id=uid('line',i),movement_id=mid,line_no=1,batch_id=bid,warehouse=warehouse,delta_qty=1000 if opening else -1,unit_cost=Decimal('12.345678'),task=None if opening else task))
            StockMovement.objects.bulk_create(movements,batch_size=1000);StockMovementLine.objects.bulk_create(lines,batch_size=1000)
        StockBalance.objects.bulk_create([StockBalance(batch_id=uid('batch',i),warehouse=warehouse,on_hand_qty=1001-per) for i in range(nb)],batch_size=1000)
        for start in range(0,no,1000):
            indices=range(start,min(start+1000,no))
            PurchaseRequest.objects.bulk_create([PurchaseRequest(id=uid('request',i),request_no=f'BENCH-PR-{i:06}',reason='Benchmark replenishment',status='APPROVED',created_by=buyer,approved_by=admin) for i in indices])
            RequestLine.objects.bulk_create([RequestLine(id=uid('request-line',i),request_id=uid('request',i),line_no=1,item_id=uid('item',i%ni),qty=100,needed_by=today) for i in indices])
            PurchaseOrder.objects.bulk_create([PurchaseOrder(id=uid('order',i),order_no=f'BENCH-PO-{i:06}',supplier=supplier,status='CONFIRMED',created_by=buyer) for i in indices])
            OrderLine.objects.bulk_create([OrderLine(id=uid('order-line',i),order_id=uid('order',i),line_no=1,request_line_id=uid('request-line',i),qty=100,unit_price=Decimal('12.345678')) for i in indices])
        RuntimeState.objects.update_or_create(pk=1,defaults={'opening_closed':True})
        self.stdout.write(f'Seed v1: items={ni} batches={nb} ledger_lines={nl} requests={no} orders={no}')
