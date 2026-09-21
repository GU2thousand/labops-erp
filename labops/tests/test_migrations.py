from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase
from django.core.management import call_command
from decimal import Decimal

class UpgradeMigrationTests(TransactionTestCase):
    def test_existing_ledger_and_local_outbox_survive_upgrade(self):
        executor=MigrationExecutor(connection)
        before=[('labops','0003_laborder_sample_sampleevent_testcatalog_and_more')]
        latest=[('labops','0004_reliable_events')]
        try:
            executor.migrate(before)
            apps=executor.loader.project_state(before).apps
            item=apps.get_model('labops','Item').objects.create(code='LEGACY',name='Legacy item',base_uom='EA')
            wh=apps.get_model('labops','Warehouse').objects.create(code='LEGACY',name='Legacy warehouse')
            batch=apps.get_model('labops','Batch').objects.create(item_id=item.pk,batch_no='LEGACY',unit_cost=Decimal('1.000001'),origin='OPENING')
            movement=apps.get_model('labops','StockMovement').objects.create(movement_no='LEGACY',type='OPENING',status='POSTED')
            apps.get_model('labops','StockMovementLine').objects.create(movement_id=movement.pk,line_no=1,batch_id=batch.pk,warehouse_id=wh.pk,delta_qty=Decimal('0.000003'),unit_cost=Decimal('1.000001'))
            apps.get_model('labops','StockBalance').objects.create(batch_id=batch.pk,warehouse_id=wh.pk,on_hand_qty=Decimal('0.000003'))
            event=apps.get_model('labops','OutboxEvent').objects.create(event_type='LEGACY_NOTICE',aggregate_type='stockmovement',aggregate_id=movement.pk,dedupe_key='legacy')
            executor=MigrationExecutor(connection);executor.migrate(latest)
            from labops.models import StockBalance,InventoryProjection,OutboxEvent
            self.assertEqual(StockBalance.objects.get(batch_id=batch.pk).on_hand_qty,Decimal('0.000003'))
            self.assertEqual(OutboxEvent.objects.get(pk=event.pk).transport,'local')
            call_command('rebuild_inventory_projection',verbosity=0)
            self.assertEqual(InventoryProjection.objects.get(batch_id=batch.pk).quantity,Decimal('0.000003'))
        finally:MigrationExecutor(connection).migrate(latest)
