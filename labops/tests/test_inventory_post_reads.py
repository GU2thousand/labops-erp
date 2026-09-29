"""Posting reuses persisted lines without changing audit or event contracts."""
from decimal import Decimal
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

from django.db import connection, transaction
from django.test import TestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from labops.common import BusinessError, obj, snapshot
from labops.event_schema import canonical_payload_hash
from labops import events
from labops.inventory import services as inventory
from labops.models import AuditEvent, Batch, Item, OutboxEvent, Receipt, StockBalance, StockMovement, StockMovementLine, Supplier, Task
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

    def assert_rejected_without_effects(self, call, code, status=422):
        before = (StockMovement.objects.count(), StockMovementLine.objects.count(),
            AuditEvent.objects.count(), OutboxEvent.objects.count(),
            list(StockBalance.objects.order_by('pk').values_list('pk', 'on_hand_qty', 'version')))
        with self.assertRaises(BusinessError) as caught:
            call()
        self.assertEqual((caught.exception.code, caught.exception.status), (code, status))
        after = (StockMovement.objects.count(), StockMovementLine.objects.count(),
            AuditEvent.objects.count(), OutboxEvent.objects.count(),
            list(StockBalance.objects.order_by('pk').values_list('pk', 'on_hand_qty', 'version')))
        self.assertEqual(after, before)
        return caught.exception

    def test_public_reads_join_only_required_relations_without_lazy_reads_or_extra_row_locks(self):
        batch = self.precise_stock()
        with CaptureQueriesContext(connection) as issue_queries:
            inventory.issue(self.store, self.issue_data(batch, '1.000000'), 'joined-issue', self.rid)
        task_reads = [sql for sql in table_reads(issue_queries, 'labops_task') if 'JOIN "labops_project"' in sql]
        batch_reads = [sql for sql in table_reads(issue_queries, 'labops_batch') if 'JOIN "labops_item"' in sql]
        self.assertEqual(len(task_reads), 1)
        self.assertEqual(len(batch_reads), 1)
        self.assertTrue(all('FOR UPDATE' not in sql for sql in task_reads + batch_reads))
        # The original project lock remains; its former lazy FK read is absent.
        project_reads = table_reads(issue_queries, 'labops_project')
        self.assertEqual(len(project_reads), 1)
        if connection.vendor == 'postgresql':
            self.assertIn('FOR UPDATE', project_reads[0])
        self.assertEqual(table_reads(issue_queries, 'labops_item'), [])

        with CaptureQueriesContext(connection) as transfer_queries:
            inventory.transfer(self.store, self.transfer_data(batch), 'joined-transfer', self.rid)
        batch_reads = [sql for sql in table_reads(transfer_queries, 'labops_batch') if 'JOIN "labops_item"' in sql]
        self.assertEqual(len(batch_reads), 1)
        self.assertNotIn('FOR UPDATE', batch_reads[0])
        self.assertEqual(table_reads(transfer_queries, 'labops_item'), [])

        receipt = self.receipt(self.order(10), '1.000000')
        with CaptureQueriesContext(connection) as receipt_queries:
            inventory.post_receipt(self.store, receipt.pk, {'expected_version': receipt.version}, 'joined-receipt', self.rid)
        receipt_reads = [sql for sql in table_reads(receipt_queries, 'labops_receipt')
            if 'JOIN "labops_purchaseorder"' in sql and 'JOIN "labops_supplier"' in sql]
        self.assertEqual(len(receipt_reads), 1)
        self.assertNotIn('FOR UPDATE', receipt_reads[0])
        # The original ancestor-order lock remains; neither parent has a lazy read.
        order_reads = table_reads(receipt_queries, 'labops_purchaseorder')
        self.assertEqual(len(order_reads), 1)
        if connection.vendor == 'postgresql':
            self.assertIn('FOR UPDATE', order_reads[0])
        self.assertEqual(table_reads(receipt_queries, 'labops_supplier'), [])
        self.assertEqual(inventory.reconcile(), [])

    def test_related_lookup_retains_obj_errors_public_missing_records_and_authorization(self):
        batch = self.precise_stock()
        for model, relations in ((Task, ('project',)), (Batch, ('item',)), (Receipt, ('order__supplier',))):
            for ident in (None, 'invalid-uuid', str(uuid4())):
                with self.subTest(model=model.__name__, ident=ident):
                    with self.assertRaises(BusinessError) as original:
                        obj(model, ident)
                    with self.assertRaises(BusinessError) as joined:
                        inventory._related_obj(model, ident, *relations)
                    self.assertEqual((joined.exception.code, joined.exception.status, joined.exception.message),
                        (original.exception.code, original.exception.status, original.exception.message))
        missing = str(uuid4())
        # Malformed document/task UUIDs can be rejected earlier by the unchanged
        # locking wrapper. Well-formed absent IDs reach the identical 404 boundary.
        calls = [
            lambda: inventory.issue(self.store, {**self.issue_data(batch, 1), 'task_id': missing}, 'missing-task', self.rid),
            lambda: inventory.issue(self.store, self.issue_data(type('BatchId', (), {'id': missing})(), 1), 'missing-batch', self.rid),
            lambda: inventory.transfer(self.store, {**self.transfer_data(batch), 'batch_id': missing}, 'missing-transfer-batch', self.rid),
            lambda: inventory.post_receipt(self.store, missing, {'expected_version': 1}, 'missing-receipt', self.rid),
        ]
        for call in calls:
            error = self.assert_rejected_without_effects(call, 'NOT_FOUND', 404)
            self.assertEqual(error.message, 'Record not found or no longer available')
        self.assert_rejected_without_effects(lambda: inventory.issue(self.audit, self.issue_data(batch, 1), 'forbidden', self.rid), 'FORBIDDEN', 403)
        outsider = self.user('outside-project', 'TECH')
        self.assert_rejected_without_effects(lambda: inventory.issue_draft(outsider, self.issue_data(batch, 1), self.rid), 'NOT_FOUND', 404)

    def test_joined_relations_keep_closed_expired_and_inactive_rejections_without_effects(self):
        batch = self.precise_stock()
        receipt = self.receipt(self.order(10), '1.000000')
        Task.objects.filter(pk=self.task.pk).update(status='DONE')
        self.assert_rejected_without_effects(lambda: inventory.issue(self.store, self.issue_data(batch, 1), 'closed', self.rid), 'TASK_NOT_ACTIVE')
        Task.objects.filter(pk=self.task.pk).update(status='IN_PROGRESS')
        Item.objects.filter(pk=self.item.pk).update(is_active=False)
        self.assert_rejected_without_effects(lambda: inventory.issue(self.store, self.issue_data(batch, 1), 'inactive-issue', self.rid), 'INACTIVE_ITEM')
        self.assert_rejected_without_effects(lambda: inventory.transfer(self.store, self.transfer_data(batch), 'inactive-transfer', self.rid), 'INACTIVE_ITEM')
        Item.objects.filter(pk=self.item.pk).update(is_active=True)
        Batch.objects.filter(pk=batch.pk).update(expires_on=timezone.localdate() - timedelta(days=1))
        self.assert_rejected_without_effects(lambda: inventory.issue(self.store, self.issue_data(batch, 1), 'expired', self.rid), 'BATCH_EXPIRED')
        Supplier.objects.filter(pk=self.supplier.pk).update(is_active=False)
        self.assert_rejected_without_effects(lambda: inventory.post_receipt(self.store, receipt.pk, {'expected_version': receipt.version}, 'inactive-supplier', self.rid), 'INACTIVE_SUPPLIER')
        self.assertEqual(inventory.reconcile(), [])
