import copy
import io
import json
import uuid
from datetime import timedelta
from unittest.mock import patch
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings
from django.db import connection, OperationalError
from unittest import skipIf
from django.utils import timezone
from labops.tests.test_acceptance import Fixture
from labops.models import OutboxEvent, ProcessedEvent, InventoryProjection, FailedDelivery, DeliveryAudit
from labops.events import envelope, raw_envelope, process_envelope, publish_one, deliver, retry_deliveries, publish_dlq, claim_dlq, LeaseLost, BrokerDeliveryError, broker_error, classify_failure
from labops.event_schema import canonical_payload_hash


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class EventReliabilityTests(Fixture, TestCase):
    def event(self):
        self.stock(10)
        return OutboxEvent.objects.get(event_type='inventory.opening.posted')

    def test_broker_errors_have_sanitized_authorization_categories(self):
        from confluent_kafka import KafkaError
        error = broker_error(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, 'secret-should-not-escape'))
        self.assertEqual(classify_failure(error), 'authorization')
        self.assertEqual(str(error), f'broker_authorization_failed:kafka_code={KafkaError.TOPIC_AUTHORIZATION_FAILED}')
        self.assertNotIn('secret', str(error))

    def test_jsonb_unrepresentable_poison_is_retained_without_nul(self):
        self.assertFalse(deliver('analytics', {'invalid': 'NUL\x00'}, 'topic:0:8'))
        row = FailedDelivery.objects.get()
        self.assertEqual(row.status, 'DEAD')
        self.assertIn('invalid_payload_base64', row.envelope)
        self.assertEqual(row.original_hash, canonical_payload_hash(row.envelope))

    def test_json_null_poison_is_durably_wrapped(self):
        self.assertFalse(deliver('analytics', None, 'topic:0:9'))
        row = FailedDelivery.objects.get()
        self.assertEqual(row.envelope, {'invalid_payload': None})
        self.assertEqual(row.status, 'DEAD')

    def test_new_inventory_contract_and_hash_saved_atomically(self):
        event = self.event()
        self.assertEqual(event.payload_hash, canonical_payload_hash(raw_envelope(event)))
        self.assertEqual(len(event.payload_hash), 64)

    def test_same_event_id_different_payload_is_quarantined(self):
        event = self.event(); original = envelope(event)
        self.assertTrue(process_envelope('analytics', original))
        changed = copy.deepcopy(original); changed['payload']['lines'][0]['delta_qty'] = '99'
        self.assertFalse(deliver('analytics', changed, 'source:0:3'))
        row = FailedDelivery.objects.get(); self.assertEqual(row.status, 'DEAD')
        self.assertEqual(row.original_hash, canonical_payload_hash(changed))
        self.assertEqual(InventoryProjection.objects.get().quantity, 10)
        self.assertEqual(ProcessedEvent.objects.count(), 1)
        self.assertEqual(row.audit_entries.get().action, 'PARK')

    def test_retained_record_absent_from_restored_business_outbox_is_quarantined(self):
        original = envelope(self.event()); original['event_id'] = str(uuid.uuid4())
        self.assertFalse(deliver('analytics', original, 'record:0:1'))
        self.assertFalse(ProcessedEvent.objects.exists()); self.assertFalse(InventoryProjection.objects.exists())
        self.assertEqual(FailedDelivery.objects.get().last_error, 'missing_business_event')

    def test_failure_coordinates_are_namespaced_by_cluster_and_generation(self):
        bad = {'schema_version': 999}
        for cluster, generation in [('a', '1'), ('a', '2'), ('b', '1')]:
            self.assertFalse(deliver('analytics', bad, 'topic:0:0', source_cluster=cluster, source_generation=generation))
        self.assertEqual(FailedDelivery.objects.count(), 3)

    def test_reused_coordinate_conflict_retains_both_observations(self):
        self.assertFalse(deliver('analytics', {'bad': 1}, 'topic:0:0'))
        self.assertFalse(deliver('analytics', {'bad': 2}, 'topic:0:0'))
        self.assertEqual(FailedDelivery.objects.count(), 2)
        self.assertEqual(FailedDelivery.objects.get(delivery_key='topic:0:0').envelope, {'bad': 1})
        conflict = FailedDelivery.objects.exclude(delivery_key='topic:0:0').get()
        self.assertEqual(conflict.envelope, {'bad': 2}); self.assertEqual(conflict.status, 'DEAD')
        self.assertEqual(conflict.audit_entries.get().action, 'SOURCE_CONFLICT')

    def test_stale_owner_after_ack_cannot_mark_new_owner_published(self):
        event = self.event(); event.transport='kafka'; event.save()
        def owner_replaced():
            OutboxEvent.objects.filter(pk=event.id).update(lease_token=uuid.uuid4(), locked_until=timezone.now()+timedelta(minutes=1))
        with patch('labops.events.send'):
            with self.assertRaises(LeaseLost): publish_one(None, after_send=owner_replaced)
        event.refresh_from_db(); self.assertEqual(event.status, 'PROCESSING'); self.assertIsNone(event.published_at)

    def assert_shard_owner_loss_preserves_lease(self, failure_at):
        from labops.publisher_shards import ShardOwnershipLost
        event = self.event(); event.transport = 'kafka'; event.save()
        observations = []
        def ownership_check():
            current = OutboxEvent.objects.get(pk=event.pk)
            observations.append((current.lease_token, current.locked_until))
            if len(observations) == failure_at:
                raise ShardOwnershipLost('Owner session ended')
        with patch('labops.events.send') as send:
            with self.assertRaises(ShardOwnershipLost):
                publish_one(None, ownership_check=ownership_check)
        self.assertEqual(send.call_count, failure_at - 1)
        event.refresh_from_db()
        self.assertEqual(event.status, 'PROCESSING')
        self.assertEqual((event.lease_token, event.locked_until), observations[-1])
        self.assertEqual(event.attempts, 0)
        self.assertIsNone(event.published_at)

    def test_shard_owner_loss_before_send_preserves_lease(self):
        self.assert_shard_owner_loss_preserves_lease(1)

    def test_shard_owner_loss_after_ack_preserves_ambiguous_lease(self):
        self.assert_shard_owner_loss_preserves_lease(2)

    def test_publisher_database_failure_preserves_claim_without_retry_rewrite(self):
        event = self.event(); event.transport = 'kafka'; event.save()
        with patch('labops.events.send', side_effect=OperationalError('Disconnected database')):
            with self.assertRaises(OperationalError): publish_one(None)
        event.refresh_from_db()
        self.assertEqual(event.status, 'PROCESSING'); self.assertEqual(event.attempts, 0)
        self.assertIsNotNone(event.lease_token); self.assertIsNotNone(event.locked_until)
        self.assertIsNone(event.published_at)

    def test_retry_database_failure_rolls_back_attempt_and_audit(self):
        value = envelope(self.event())
        with patch('labops.events.process_envelope', side_effect=RuntimeError('Temporary dependency')):
            deliver('analytics', value, 'db-retry:0:1')
        row = FailedDelivery.objects.get()
        FailedDelivery.objects.filter(pk=row.pk).update(next_attempt_at=timezone.now())
        before_audits = row.audit_entries.count(); before_attempts = row.attempts
        with patch('labops.events.process_envelope', side_effect=OperationalError('Disconnected database')):
            with self.assertRaises(OperationalError): retry_deliveries()
        row.refresh_from_db()
        self.assertEqual(row.status, 'RETRY'); self.assertEqual(row.attempts, before_attempts)
        self.assertIsNone(row.lease_token); self.assertEqual(row.audit_entries.count(), before_audits)
        self.assertFalse(ProcessedEvent.objects.exists())

    def test_dlq_database_failure_retains_claim_without_compensating_write(self):
        deliver('analytics', {'invalid': True}, 'db-dlq:0:1')
        row = FailedDelivery.objects.get(); before_audits = row.audit_entries.count()
        with patch('labops.events.send', side_effect=OperationalError('Disconnected database')):
            with self.assertRaises(OperationalError): publish_dlq(None)
        row.refresh_from_db()
        self.assertEqual(row.status, 'DEAD'); self.assertEqual(row.dlq_attempts, 0)
        self.assertIsNotNone(row.dlq_lease_token); self.assertIsNotNone(row.dlq_locked_until)
        self.assertIsNone(row.dlq_published_at); self.assertEqual(row.audit_entries.count(), before_audits)

    def test_retry_preserves_original_payload_and_audits_dependency_recovery(self):
        event = self.event(); value = envelope(event)
        with patch('labops.events.process_envelope', side_effect=RuntimeError('dependency unavailable')):
            self.assertFalse(deliver('analytics', value, 'topic:0:4'))
        row=FailedDelivery.objects.get(); self.assertEqual(row.status,'RETRY')
        FailedDelivery.objects.filter(pk=row.id).update(next_attempt_at=timezone.now())
        self.assertEqual(retry_deliveries(),1)
        row.refresh_from_db(); self.assertEqual(row.status,'RESOLVED'); self.assertEqual(row.envelope,value)
        self.assertEqual(row.audit_entries.count(),2); self.assertEqual(InventoryProjection.objects.get().quantity,10)

    @skipIf(connection.vendor == "postgresql", "Production immutable trigger rejects stored-payload corruption")
    def test_changed_retry_payload_is_dead_without_effect(self):
        event = self.event(); value = envelope(event)
        with patch('labops.events.process_envelope', side_effect=RuntimeError()): deliver('analytics',value,'topic:0:5')
        row=FailedDelivery.objects.get(); row.envelope['payload']['lines'][0]['delta_qty']='11'
        row.next_attempt_at=timezone.now(); row.save()
        retry_deliveries(); row.refresh_from_db()
        self.assertEqual(row.status,'DEAD'); self.assertEqual(row.last_error,'payload_conflict')
        self.assertFalse(ProcessedEvent.objects.exists()); self.assertFalse(InventoryProjection.objects.exists())

    def test_dlq_claim_has_single_live_owner_and_same_delivery_id(self):
        deliver('analytics', {'invalid': True}, 'topic:0:6')
        row=claim_dlq(); self.assertIsNotNone(row); self.assertIsNone(claim_dlq())
        FailedDelivery.objects.filter(pk=row.id).update(dlq_locked_until=timezone.now()-timedelta(seconds=1))
        with patch('labops.events.send') as sent:
            self.assertEqual(publish_dlq(None),1)
            self.assertEqual(sent.call_args.args[3]['delivery_id'],str(row.id))
        row.refresh_from_db(); self.assertIsNotNone(row.dlq_published_at); self.assertIsNone(row.dlq_lease_token)

    def test_replay_dry_run_is_read_only_and_execute_dedupes(self):
        source = self.event()
        event = OutboxEvent.objects.create(event_type=source.event_type, aggregate_type=source.aggregate_type,
            aggregate_id=source.aggregate_id, aggregate_version=source.aggregate_version + 1,
            payload_json=source.payload_json, dedupe_key='legacy-null-hash', payload_hash=None)
        args={'consumer':'analytics','start':(event.created_at-timedelta(seconds=1)).isoformat(),
              'end':(event.created_at+timedelta(seconds=1)).isoformat(),'event_id':[str(event.id)],'rate':1000,'stdout':io.StringIO()}
        before_audit_count = DeliveryAudit.objects.count()
        call_command('replay_events',**args)
        event.refresh_from_db(); self.assertIsNone(event.payload_hash)
        self.assertFalse(ProcessedEvent.objects.exists()); self.assertEqual(DeliveryAudit.objects.count(), before_audit_count)
        args.update(execute=True,actor='operator@example.test',reason='Restore drill',authorization='change-123')
        with patch('labops.management.commands.replay_events.time.sleep'):
            call_command('replay_events',**args);call_command('replay_events',**args)
        self.assertEqual(InventoryProjection.objects.get().quantity,10)
        self.assertEqual(DeliveryAudit.objects.filter(action='REPLAY').count(),2)

    def test_manual_disposition_recovers_expired_lease_with_append_only_audit(self):
        deliver('analytics',{'invalid':True},'topic:0:7');row=FailedDelivery.objects.get()
        row.dlq_lease_token=uuid.uuid4();row.dlq_locked_until=timezone.now()-timedelta(seconds=1);row.save()
        with self.assertRaises(CommandError):call_command('event_failures','retry',id=str(row.id),reason='retry',stdout=io.StringIO())
        call_command('event_failures','retry',id=str(row.id),reason='Dependency fixed',actor='operator',authorization='change-456',stdout=io.StringIO())
        row.refresh_from_db();self.assertIsNone(row.dlq_lease_token);self.assertEqual(row.status,'RETRY')
        audit=DeliveryAudit.objects.get(action='MANUAL_RETRY');self.assertEqual(audit.actor_label,'operator')
        with self.assertRaises(ValueError):audit.save()
        with self.assertRaises(ValueError):DeliveryAudit.objects.filter(id=audit.id).update(reason='overwritten')
