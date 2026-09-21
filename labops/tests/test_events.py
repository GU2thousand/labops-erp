from datetime import timedelta
from unittest.mock import patch
from django.test import TestCase, override_settings
from django.utils import timezone
from labops.tests.test_acceptance import Fixture
from labops.models import *
from labops.inventory.services import issue
from labops.events import *

@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class EventTests(Fixture, TestCase):
    def event(self):
        self.stock(10)
        return OutboxEvent.objects.get(event_type='inventory.opening.posted')
    def test_independent_consumers_and_duplicate(self):
        event=self.event()
        for name in ['analytics','notification']:
            self.assertTrue(process_envelope(name,envelope(event)))
            self.assertFalse(process_envelope(name,envelope(event)))
        self.assertEqual(ProcessedEvent.objects.filter(event_id=event.pk).count(),2)
        self.assertEqual(InventoryProjection.objects.get().quantity,10)
    def test_consumer_effect_failure_rolls_back_dedupe(self):
        event=self.event()
        with patch.object(InventoryProjection,'save',side_effect=RuntimeError('crash')):
            with self.assertRaises(RuntimeError):process_envelope('analytics',envelope(event))
        self.assertFalse(ProcessedEvent.objects.exists());self.assertFalse(InventoryProjection.objects.exists())
        self.assertTrue(process_envelope('analytics',envelope(event)))
    def test_outbox_rolls_back_with_business(self):
        batch=self.stock(10);before=OutboxEvent.objects.count()
        with patch('labops.events.emit_inventory',side_effect=RuntimeError('crash')):
            with self.assertRaises(RuntimeError):issue(self.store,self.issue_data(batch,3),'failed',self.rid)
        self.assertEqual(OutboxEvent.objects.count(),before)
        self.assertEqual(StockBalance.objects.get().on_hand_qty,10)
        self.assertFalse(StockMovement.objects.filter(type='ISSUE').exists())
    def test_publish_ack_then_crash_redelivers_same_id(self):
        event=self.event();event.transport='kafka';event.save()
        with patch('labops.events.send') as send_mock:
            with self.assertRaises(RuntimeError):publish_one(None,after_send=lambda:(_ for _ in ()).throw(RuntimeError('crash')))
            event.refresh_from_db();self.assertEqual(event.status,'PENDING')
            event.next_attempt_at=timezone.now();event.save()
            self.assertTrue(publish_one(None))
            self.assertEqual(send_mock.call_args_list[0].args[3]['event_id'],send_mock.call_args_list[1].args[3]['event_id'])
        event.refresh_from_db();self.assertEqual(event.status,'PUBLISHED')
    def test_expired_lease_is_recovered(self):
        event=self.event();event.transport='kafka';event.status='PROCESSING';event.locked_until=timezone.now()-timedelta(seconds=1);event.save()
        self.assertEqual(claim_event().id,event.id)
    def test_poison_message_retry_dlq_and_manual_replay(self):
        event=self.event();bad={**envelope(event),'schema_version':999}
        self.assertFalse(deliver('analytics',bad,'topic:0:1'))
        for _ in range(4):
            FailedDelivery.objects.update(next_attempt_at=timezone.now());retry_deliveries()
        row=FailedDelivery.objects.get();self.assertEqual(row.status,'DEAD')
        self.assertFalse(ProcessedEvent.objects.exists())
        with patch('labops.events.send') as send_mock:
            publish_dlq(None);self.assertEqual(send_mock.call_count,1)
        row.envelope=envelope(event);row.status='RETRY';row.next_attempt_at=timezone.now();row.save();retry_deliveries()
        row.refresh_from_db();self.assertEqual(row.status,'RESOLVED');self.assertEqual(InventoryProjection.objects.get().quantity,10)
    def test_later_aggregate_version_waits_for_earlier(self):
        event=self.event();event.transport='kafka';event.status='DEAD';event.save()
        later=OutboxEvent.objects.create(event_type=event.event_type,transport='kafka',aggregate_id=event.aggregate_id,aggregate_type=event.aggregate_type,aggregate_version=event.aggregate_version+1,dedupe_key='later')
        self.assertIsNone(claim_event())
        event.status='PUBLISHED';event.save();self.assertEqual(claim_event().pk,later.pk)

    def test_projection_rebuild_checkpoints_old_events(self):
        from django.core.management import call_command
        event=self.event()
        call_command('rebuild_inventory_projection',verbosity=0)
        self.assertEqual(InventoryProjection.objects.get().quantity,10)
        self.assertFalse(process_envelope('analytics',envelope(event)))
        batch=Batch.objects.get();movement=issue(self.store,self.issue_data(batch,3),'next',self.rid)
        process_envelope('analytics',envelope(OutboxEvent.objects.get(aggregate_id=movement.id)))
        self.assertEqual(InventoryProjection.objects.get().quantity,7)

from django.test import TransactionTestCase
from django.db import connection
from unittest import skipUnless

@skipUnless(connection.vendor=='postgresql','PostgreSQL deferred constraints')
class DeferredConstraintRetryTests(TransactionTestCase):
    def test_invalid_foreign_key_advances_retry_and_reaches_dead(self):
        import uuid
        event={'event_id':str(uuid.uuid4()),'schema_version':1,'event_type':'inventory.issue.posted',
               'aggregate_id':str(uuid.uuid4()),'aggregate_version':1,'payload':{'lines':[
               {'batch_id':str(uuid.uuid4()),'warehouse_id':str(uuid.uuid4()),'delta_qty':'-1'}]}}
        self.assertFalse(deliver('analytics',event,'missing-fk:0:0'))
        for attempts in range(2,6):
            FailedDelivery.objects.update(next_attempt_at=timezone.now())
            self.assertEqual(retry_deliveries(),1)
            row=FailedDelivery.objects.get();self.assertEqual(row.attempts,attempts)
        self.assertEqual(row.status,'DEAD');self.assertFalse(ProcessedEvent.objects.exists())
