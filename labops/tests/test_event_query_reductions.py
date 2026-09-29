"""Single-event query reductions retain exact, atomic database effects."""
import copy
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from unittest import skipUnless
from unittest.mock import patch

from django.db import connection, connections, close_old_connections, IntegrityError, transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from labops import events
from labops.event_schema import EventValidationError, canonical_payload_hash
from labops.inventory.services import issue
from labops.models import InventoryProjection, Notification, OutboxEvent, ProcessedEvent, User
from labops.tests.test_acceptance import Fixture


def reads(queries, table):
    return [row['sql'] for row in queries if row['sql'].lstrip().upper().startswith('SELECT')
            and f'FROM "{table}"' in row['sql']]


class EventQueryFixture(Fixture):
    def event(self, recipients=None, *, legacy=False):
        recipients = recipients if recipients is not None else [self.admin.pk, self.other.pk, self.store.pk]
        with transaction.atomic(), patch('labops.operations.services.stock_recipients', return_value=recipients):
            if legacy:
                # A genuinely legacy NULL hash is inserted, never edited after
                # immutable checksum initialization (including PostgreSQL).
                with patch.object(events, 'envelope', side_effect=events.raw_envelope):
                    batch = self.stock('10.000001')
            else:
                batch = self.stock('10.000001')
        return batch, OutboxEvent.objects.get(aggregate_id=batch.movement_lines.get().movement_id)


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'], EVENT_TRANSPORT='kafka')
class EventQueryReductionTests(EventQueryFixture, TestCase):
    def test_one_active_snapshot_bulk_insert_preserves_existing_notification_metadata(self):
        User.objects.filter(pk=self.tech.pk).update(is_active=False)
        _, event = self.event([str(self.admin.pk).upper(), self.other.pk, self.store.pk, self.tech.pk, uuid.uuid4()])
        read_at = timezone.now()
        prior = Notification.objects.create(event=event, user=self.other, title='Prior title', body='Prior body', read_at=read_at)
        prior_pk = prior.pk
        with CaptureQueriesContext(connection) as queries:
            self.assertTrue(events.process_envelope('notification', events.envelope(event)))
        self.assertEqual(len(reads(queries, 'labops_user')), 1)
        self.assertEqual(len(reads(queries, 'labops_notification')), 2)
        inserts = [row['sql'] for row in queries if row['sql'].startswith('INSERT INTO "labops_notification"')]
        self.assertEqual(len(inserts), 1)
        self.assertEqual(set(Notification.objects.filter(event=event).values_list('user_id', flat=True)),
                         {self.admin.pk, self.other.pk, self.store.pk})
        prior.refresh_from_db()
        self.assertEqual((prior.pk, prior.title, prior.body, prior.read_at),
                         (prior_pk, 'Prior title', 'Prior body', read_at))
        self.assertFalse(events.process_envelope('notification', events.envelope(event)))
        self.assertEqual(Notification.objects.filter(event=event).count(), 3)

    def test_no_eligible_recipient_and_canonical_duplicates_do_not_create_notifications(self):
        User.objects.filter(pk=self.tech.pk).update(is_active=False)
        _, event = self.event([self.tech.pk, uuid.uuid4()])
        with CaptureQueriesContext(connection) as queries:
            self.assertTrue(events.process_envelope('notification', events.envelope(event)))
        self.assertEqual(len(reads(queries, 'labops_user')), 1)
        self.assertEqual(len(reads(queries, 'labops_notification')), 0)
        self.assertFalse(Notification.objects.filter(event=event).exists())
        changed = copy.deepcopy(events.envelope(event))
        changed['payload']['recipients'] = [str(self.admin.pk), str(self.admin.pk).upper()]
        with self.assertRaises(EventValidationError):
            events.process_envelope('notification', changed)
        self.assertEqual(ProcessedEvent.objects.filter(event_id=event.pk).count(), 1)

    def test_unrelated_notification_primary_key_collision_rolls_back_marker_and_all_new_rows(self):
        _, event = self.event()
        prior = Notification.objects.create(event=event, user=self.other, title='Preserve', body='Existing')
        real_bulk = Notification.objects.bulk_create
        def collide(rows, *args, **kwargs):
            rows[0].pk = prior.pk
            return real_bulk(rows, *args, **kwargs)
        with patch.object(Notification.objects, 'bulk_create', side_effect=collide), \
                patch.object(events, '_notification_unique_conflict', wraps=events._notification_unique_conflict) as classify:
            with self.assertRaises(IntegrityError):
                events.process_envelope('notification', events.envelope(event))
        self.assertEqual(classify.call_count, 1)
        self.assertFalse(ProcessedEvent.objects.filter(event_id=event.pk).exists())
        self.assertEqual(list(Notification.objects.filter(event=event).values_list('pk', flat=True)), [prior.pk])
        prior.refresh_from_db()
        self.assertEqual((prior.title, prior.body), ('Preserve', 'Existing'))
        self.assertTrue(events.process_envelope('notification', events.envelope(event)))
        self.assertEqual(Notification.objects.filter(event=event).count(), 3)

    def test_missing_bulk_effect_is_detected_before_deduplication_commit(self):
        _, event = self.event()
        with patch.object(Notification.objects, 'bulk_create', side_effect=lambda rows: rows):
            with self.assertRaises(IntegrityError):
                events.process_envelope('notification', events.envelope(event))
        self.assertFalse(ProcessedEvent.objects.filter(event_id=event.pk).exists())
        self.assertFalse(Notification.objects.filter(event=event).exists())
        self.assertTrue(events.process_envelope('notification', events.envelope(event)))

    def test_projection_single_locked_read_keeps_exact_created_existing_and_duplicate_effects(self):
        batch, event = self.event()
        with CaptureQueriesContext(connection) as first:
            self.assertTrue(events.process_envelope('analytics', events.envelope(event)))
        movement = issue(self.store, self.issue_data(batch, '2.000001'), 'projection-next', self.rid)
        next_event = OutboxEvent.objects.get(aggregate_id=movement.pk)
        with CaptureQueriesContext(connection) as second:
            self.assertTrue(events.process_envelope('analytics', events.envelope(next_event)))
        for queries in (first, second):
            projection_reads = reads(queries, 'labops_inventoryprojection')
            self.assertEqual(len(projection_reads), 1)
            if connection.vendor == 'postgresql':
                self.assertIn('FOR UPDATE', projection_reads[0])
        self.assertEqual(InventoryProjection.objects.get(batch=batch, warehouse=self.wh).quantity, Decimal('8'))
        self.assertFalse(events.process_envelope('analytics', events.envelope(next_event)))
        self.assertEqual(InventoryProjection.objects.get(batch=batch, warehouse=self.wh).quantity, Decimal('8'))

    def test_original_validation_and_legacy_hash_initialization_precede_digest_equality(self):
        _, event = self.event(legacy=True)
        self.assertIsNone(event.payload_hash)
        value = events.raw_envelope(event)
        with patch.object(events, 'canonical_payload_hash', wraps=canonical_payload_hash) as hashed:
            self.assertTrue(events.process_envelope('notification', value))
        self.assertEqual(hashed.call_count, 2)
        event.refresh_from_db()
        self.assertEqual(event.payload_hash, canonical_payload_hash(value))
        changed = copy.deepcopy(value)
        changed['payload']['body'] += ' changed'
        with self.assertRaises(events.PayloadConflict):
            events.process_envelope('analytics', changed)
        self.assertFalse(ProcessedEvent.objects.filter(consumer_name='analytics', event_id=event.pk).exists())
        self.assertFalse(InventoryProjection.objects.exists())


@skipUnless(connection.vendor == 'postgresql', 'PostgreSQL consumer concurrency acceptance')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'], EVENT_TRANSPORT='kafka')
class PostgreSQLEventQueryConcurrencyTests(EventQueryFixture, TransactionTestCase):
    def worker(self, function):
        close_old_connections()
        try:
            return function()
        finally:
            connections.close_all()

    def test_same_event_dedupe_and_expected_notification_unique_race_preserve_existing_row(self):
        _, event = self.event()
        value = events.envelope(event)
        entered, release = threading.Event(), threading.Event()
        barrier = threading.Barrier(4)
        real_bulk = Notification.objects.bulk_create
        observed = []
        real_classify = events._notification_unique_conflict
        def bulk(rows, *args, **kwargs):
            entered.set()
            if not release.wait(10):
                raise RuntimeError('External notification writer did not complete')
            return real_bulk(rows, *args, **kwargs)
        def classify(error):
            observed.append(getattr(getattr(error.__cause__, 'diag', None), 'constraint_name', None))
            return real_classify(error)
        def consume():
            barrier.wait(timeout=10)
            return events.process_envelope('notification', value)
        with patch.object(Notification.objects, 'bulk_create', side_effect=bulk), \
                patch.object(events, '_notification_unique_conflict', side_effect=classify), \
                ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self.worker, consume) for _ in range(4)]
            try:
                self.assertTrue(entered.wait(10))
                read_at = timezone.now()
                prior = Notification.objects.create(event=event, user=self.admin,
                    title='Concurrent existing title', body='Concurrent existing body', read_at=read_at)
            finally:
                release.set()
            results = [future.result(timeout=10) for future in futures]
        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 3)
        self.assertEqual(observed, ['notification_unique'])
        self.assertEqual(ProcessedEvent.objects.filter(consumer_name='notification', event_id=event.pk).count(), 1)
        self.assertEqual(Notification.objects.filter(event=event).count(), 3)
        prior.refresh_from_db()
        self.assertEqual((prior.title, prior.body, prior.read_at),
                         ('Concurrent existing title', 'Concurrent existing body', read_at))

    def test_distinct_events_create_one_projection_and_preserve_exact_commutative_sum(self):
        batch, event = self.event()
        movement = issue(self.store, self.issue_data(batch, '2.000001'), 'concurrent-projection-next', self.rid)
        values = [events.envelope(event), events.envelope(OutboxEvent.objects.get(aggregate_id=movement.pk))]
        barrier = threading.Barrier(2)
        def consume(value):
            barrier.wait(timeout=10)
            return events.process_envelope('analytics', value)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [future.result(timeout=10) for future in
                [pool.submit(self.worker, lambda value=value: consume(value)) for value in values]]
        self.assertEqual(results, [True, True])
        self.assertEqual(InventoryProjection.objects.filter(batch=batch, warehouse=self.wh).count(), 1)
        self.assertEqual(InventoryProjection.objects.get(batch=batch, warehouse=self.wh).quantity, Decimal('8'))
        self.assertEqual(ProcessedEvent.objects.filter(consumer_name='analytics').count(), 2)
        self.assertEqual([events.process_envelope('analytics', value) for value in values], [False, False])
        self.assertEqual(InventoryProjection.objects.get(batch=batch, warehouse=self.wh).quantity, Decimal('8'))
