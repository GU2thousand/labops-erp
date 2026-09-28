"""Posting reuses persisted lines without changing audit or event contracts."""
from decimal import Decimal
from unittest.mock import patch

from django.db import connection, transaction
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext

from labops.common import snapshot
from labops.event_schema import canonical_payload_hash
from labops import events
from labops.inventory import services as inventory
from labops.models import AuditEvent, OutboxEvent, StockBalance, StockMovement
from labops.tests.test_acceptance import Fixture


def table_reads(queries, table):
    return [row['sql'] for row in queries
        if row['sql'].lstrip().upper().startswith('SELECT') and f'FROM "{table}"' in row['sql']]


@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class InventoryPostReadTests(Fixture, TestCase):
    def precise_stock(self):
        movement = inventory.opening(self.admin, {
            'item_id': str(self.item.pk), 'warehouse_id': str(self.wh.pk),
            'batch_no': 'EXACT-POST', 'qty': '10.000000', 'unit_cost': '0.100000'},
            'precise-opening', self.rid)
        return movement.lines.get().batch

    def transfer_data(self, batch):
        return {'batch_id': str(batch.pk), 'from_warehouse_id': str(self.wh.pk),
            'to_warehouse_id': str(self.wh2.pk), 'qty': '1.230000'}

    def assert_persisted_audit_and_event(self, movement):
        fresh = StockMovement.objects.get(pk=movement.pk)
        audit = AuditEvent.objects.get(entity_type='stockmovement', entity_id=movement.pk, action='POST')
        event = OutboxEvent.objects.get(aggregate_id=movement.pk, event_type__startswith='inventory.')
        self.assertEqual(audit.after_json, snapshot(fresh))
        expected = [{'batch_id': str(row.batch_id), 'warehouse_id': str(row.warehouse_id),
            'delta_qty': str(row.delta_qty), 'unit_cost': str(row.unit_cost)}
            for row in fresh.lines.order_by('line_no')]
        self.assertEqual(event.payload_json['lines'], expected)
        self.assertEqual([{key: row[key] for key in expected[0]} for row in audit.after_json['lines']], expected)
        self.assertEqual(event.payload_hash, canonical_payload_hash(events.envelope(event)))
        return audit, event

    def test_one_database_line_list_serves_audit_and_one_argument_emitter_with_exact_hash_and_order(self):
        batch = self.precise_stock()
        observed = []
        original_save, original_emit = inventory.save_change, events.emit_inventory
        def save(user, movement, *args, **kwargs):
            observed.append(('audit', movement.prefetched_lines))
            return original_save(user, movement, *args, **kwargs)
        def emit(movement):
            observed.append(('event', movement.prefetched_lines))
            return original_emit(movement)
        with patch.object(inventory, 'save_change', save), patch.object(events, 'emit_inventory', emit), \
                CaptureQueriesContext(connection) as queries:
            movement = inventory.transfer(self.store, self.transfer_data(batch), 'precise-transfer', self.rid)
        self.assertEqual([name for name, _ in observed], ['audit', 'event'])
        self.assertIs(observed[0][1], observed[1][1])
        self.assertEqual([row.line_no for row in observed[0][1]], [1, 2])
        self.assertEqual([str(row.delta_qty) for row in observed[0][1]], ['-1.23', '1.23'])
        self.assertEqual([str(row.unit_cost) for row in observed[0][1]], ['0.1', '0.1'])
        self.assertFalse(hasattr(movement, 'prefetched_lines'))
        # CREATE and before snapshots still query their empty lines; the third
        # read supplies both the POST snapshot and immutable event payload.
        self.assertEqual(len(table_reads(queries, 'labops_stockmovementline')), 3)
        audit, _ = self.assert_persisted_audit_and_event(movement)
        created = AuditEvent.objects.get(entity_type='stockmovement', entity_id=movement.pk, action='CREATE')
        self.assertEqual(audit.before_json, created.after_json)
        self.assertEqual(audit.before_json['status'], 'DRAFT')
        self.assertEqual(audit.before_json['lines'], [])
        self.assertEqual(inventory.reconcile(), [])

    def test_existing_and_new_balances_each_have_one_read_and_keep_exact_defaults_and_versions(self):
        batch = self.precise_stock()
        source = StockBalance.objects.get(batch=batch, warehouse=self.wh)
        original_version = source.version
        self.assertFalse(StockBalance.objects.filter(batch=batch, warehouse=self.wh2).exists())
        with CaptureQueriesContext(connection) as queries:
            inventory.transfer(self.store, self.transfer_data(batch), 'balance-reads', self.rid)
        reads = table_reads(queries, 'labops_stockbalance')
        self.assertEqual(len(reads), 2)
        if connection.vendor == 'postgresql':
            self.assertTrue(all('FOR UPDATE' in sql for sql in reads), reads)
        source.refresh_from_db()
        target = StockBalance.objects.get(batch=batch, warehouse=self.wh2)
        self.assertEqual(source.on_hand_qty, Decimal('8.77'))
        self.assertEqual(source.version, original_version + 1)
        self.assertEqual(target.on_hand_qty, Decimal('1.23'))
        self.assertEqual(target.version, 2)
        self.assertNotEqual(source.pk, target.pk)
        self.assertEqual(inventory.reconcile(), [])

    def test_public_draft_post_keeps_before_lines_and_uses_replacement_posted_rows(self):
        batch = self.precise_stock()
        draft = inventory.issue_draft(self.store, self.issue_data(batch, '1.000000'), self.rid)
        before = snapshot(draft)
        old_ids = {row['id'] for row in before['lines']}
        movement = inventory.issue(self.store, {'draft_id': str(draft.pk), 'expected_version': draft.version},
            'post-public-draft', self.rid)
        self.assertFalse(hasattr(movement, 'prefetched_lines'))
        audit, _ = self.assert_persisted_audit_and_event(movement)
        self.assertEqual(audit.before_json, before)
        self.assertEqual(audit.before_json['status'], 'DRAFT')
        self.assertEqual(audit.after_json['status'], 'POSTED')
        self.assertTrue(old_ids.isdisjoint(row['id'] for row in audit.after_json['lines']))

    def test_emitter_fallback_after_post_reads_database_and_keeps_existing_immutable_event(self):
        batch = self.precise_stock()
        movement = inventory.transfer(self.store, self.transfer_data(batch), 'fallback', self.rid)
        original = OutboxEvent.objects.get(aggregate_id=movement.pk)
        with CaptureQueriesContext(connection) as queries:
            event = events.emit_inventory(movement)
        self.assertEqual(len(table_reads(queries, 'labops_stockmovementline')), 1)
        self.assertEqual(event.pk, original.pk)
        self.assertEqual(event.payload_json, original.payload_json)
        self.assertEqual(event.payload_hash, original.payload_hash)
        self.assertFalse(hasattr(movement, 'prefetched_lines'))

    def test_fresh_movement_emitter_failure_removes_attribute_and_rolls_back_every_effect(self):
        batch = self.precise_stock()
        before_audits, before_events = AuditEvent.objects.count(), OutboxEvent.objects.count()
        captured = []
        original = RuntimeError('emission failed after immutable event insertion')
        original_emit = events.emit_inventory
        def fail(movement):
            captured.append(movement)
            original_emit(movement)
            raise original
        with patch.object(events, 'emit_inventory', fail):
            with self.assertRaises(RuntimeError) as caught:
                inventory.issue(self.store, self.issue_data(batch, '1.000000'), 'failed-new', self.rid)
        self.assertIs(caught.exception, original)
        self.assertEqual(len(captured), 1)
        self.assertFalse(hasattr(captured[0], 'prefetched_lines'))
        self.assertFalse(StockMovement.objects.filter(idempotency_key='failed-new').exists())
        self.assertEqual(AuditEvent.objects.count(), before_audits)
        self.assertEqual(OutboxEvent.objects.count(), before_events)
        self.assertEqual(StockBalance.objects.get(batch=batch, warehouse=self.wh).on_hand_qty, Decimal('10'))
        self.assertEqual(inventory.reconcile(), [])

    def test_audit_failure_removes_attribute_and_rolls_back_new_movement_and_balance(self):
        batch = self.precise_stock()
        before_audits, before_events = AuditEvent.objects.count(), OutboxEvent.objects.count()
        captured = []
        original = ValueError('audit failure')
        def fail(user, movement, *args, **kwargs):
            self.assertTrue(hasattr(movement, 'prefetched_lines'))
            captured.append(movement)
            raise original
        with patch.object(inventory, 'save_change', fail):
            with self.assertRaises(ValueError) as caught:
                inventory.issue(self.store, self.issue_data(batch, '1.000000'), 'failed-audit', self.rid)
        self.assertIs(caught.exception, original)
        self.assertFalse(hasattr(captured[0], 'prefetched_lines'))
        self.assertFalse(StockMovement.objects.filter(idempotency_key='failed-audit').exists())
        self.assertEqual(AuditEvent.objects.count(), before_audits)
        self.assertEqual(OutboxEvent.objects.count(), before_events)
        self.assertEqual(StockBalance.objects.get(batch=batch, warehouse=self.wh).on_hand_qty, Decimal('10'))

    def post_same_draft_instance(self, draft, batch, key):
        return inventory.post(self.store, 'ISSUE', [inventory.line(batch, self.wh, -Decimal('1.000000'), task=self.task)],
            self.issue_data(batch, '1.000000'), key, self.rid, draft=draft)

    def test_prior_draft_attribute_is_restored_exactly_after_success(self):
        batch = self.precise_stock()
        draft = inventory.issue_draft(self.store, self.issue_data(batch, '1.000000'), self.rid)
        prior = list(draft.lines.order_by('line_no'))
        draft.prefetched_lines = prior
        before = snapshot(draft)
        with transaction.atomic():
            movement = self.post_same_draft_instance(draft, batch, 'existing-prefetch')
        self.assertIs(movement, draft)
        self.assertIs(movement.prefetched_lines, prior)
        audit, _ = self.assert_persisted_audit_and_event(movement)
        self.assertEqual(audit.before_json, before)

    def test_prior_draft_attribute_is_restored_exactly_and_deleted_rows_return_after_failure(self):
        batch = self.precise_stock()
        draft = inventory.issue_draft(self.store, self.issue_data(batch, '1.000000'), self.rid)
        prior = list(draft.lines.order_by('line_no'))
        draft.prefetched_lines = prior
        before = snapshot(draft)
        before_audits, before_events = AuditEvent.objects.count(), OutboxEvent.objects.count()
        original = RuntimeError('draft event failure')
        original_emit = events.emit_inventory
        def fail(movement):
            self.assertIsNot(movement.prefetched_lines, prior)
            original_emit(movement)
            raise original
        with patch.object(events, 'emit_inventory', fail):
            with self.assertRaises(RuntimeError) as caught:
                with transaction.atomic():
                    self.post_same_draft_instance(draft, batch, 'failed-draft')
        self.assertIs(caught.exception, original)
        self.assertIs(draft.prefetched_lines, prior)
        self.assertEqual(snapshot(StockMovement.objects.get(pk=draft.pk)), before)
        self.assertEqual(AuditEvent.objects.count(), before_audits)
        self.assertEqual(OutboxEvent.objects.count(), before_events)
        self.assertEqual(StockBalance.objects.get(batch=batch, warehouse=self.wh).on_hand_qty, Decimal('10'))
        self.assertEqual(inventory.reconcile(), [])
