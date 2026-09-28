"""Publisher claims filter the dependency expression without selecting its value."""
import copy
import json
import os
from pathlib import Path
from queue import Queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest import skipUnless
from unittest.mock import patch
from uuid import UUID, uuid4

from django.db import close_old_connections, connection, connections, transaction
from django.test import TestCase, TransactionTestCase, override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from labops import events
from labops.event_schema import EventValidationError, canonical_payload_hash
from labops.models import OutboxEvent


class ClaimFixture:
    def row(self, *, legacy=False, **changes):
        aggregate = changes.pop('aggregate_id', uuid4())
        defaults = dict(event_type='inventory.issue.posted', transport='kafka',
            aggregate_type='stockmovement', aggregate_id=aggregate,
            dedupe_key=str(uuid4()), payload_json={
                '_trace_context': {}, 'movement_id': str(aggregate), 'movement_type': 'ISSUE',
                'title': 'Issue posted', 'body': 'Claim fixture', 'recipients': [str(uuid4())],
                'lines': [{'batch_id': str(uuid4()), 'warehouse_id': str(uuid4()),
                    'delta_qty': '-0.000001', 'unit_cost': '0.1'}]})
        defaults.update(changes)
        record = OutboxEvent(**defaults)
        if not legacy:
            record.payload_hash = canonical_payload_hash(events.raw_envelope(record))
        record.save()
        return record

    def fields(self, record):
        return {field.attname: copy.deepcopy(getattr(record, field.attname))
                for field in record._meta.concrete_fields}

    def claim(self, **kwargs):
        with CaptureQueriesContext(connection) as queries:
            record = events.claim_event(**kwargs)
        selects = [row['sql'] for row in queries
            if row['sql'].lstrip().startswith('SELECT') and 'FROM "labops_outboxevent"' in row['sql']]
        self.assertEqual(len(selects), 1)
        return record, selects[0]

    def assert_filter_only(self, sql):
        self.assertNotIn('EXISTS', sql.split(' FROM "labops_outboxevent"', 1)[0])
        self.assertNotIn(' AS "blocked"', sql)
        self.assertEqual(sql.count('EXISTS('), 1)
        self.assertIn('NOT EXISTS(', sql)
        self.assertIn('ORDER BY "labops_outboxevent"."created_at" ASC, "labops_outboxevent"."id" ASC LIMIT 1', sql)
        if connection.vendor == 'postgresql':
            self.assertTrue(sql.endswith(' FOR UPDATE SKIP LOCKED'), sql)

    def backend_pid(self):
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            return cursor.fetchone()[0]

    def evidence(self, name, value):
        directory = os.environ.get('LABOPS_PUBLISHER_CLAIM_PROOF_EVIDENCE')
        if directory:
            (Path(directory) / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


@override_settings(EVENT_LEASE_SECONDS=60)
class PublisherClaimReadTests(ClaimFixture, TestCase):
    def test_filter_alias_omits_selected_exists_and_keeps_all_persisted_fields(self):
        record = self.row(attempts=2, last_error='Prior failure')
        before = self.fields(record)
        now = timezone.now()
        with patch.object(events.timezone, 'now', return_value=now):
            claimed, sql = self.claim()
        self.assert_filter_only(sql)
        self.assertIsInstance(claimed, OutboxEvent)
        self.assertFalse(hasattr(claimed, 'blocked'))
        expected = {**before, 'status': 'PROCESSING', 'lease_token': claimed.lease_token,
                    'locked_until': now + timedelta(seconds=60)}
        self.assertIsInstance(claimed.lease_token, UUID)
        self.assertEqual(self.fields(claimed), expected)
        self.assertEqual(self.fields(OutboxEvent.objects.get(pk=record.pk)), expected)
        self.evidence('claim-projection.json', {'sql': sql,
            'selected_field_names': list(expected), 'unused_blocked_attribute_present': False,
            'persisted_fields_match': True})

    def test_only_due_kafka_candidates_are_claimed_in_created_at_then_id_order(self):
        now = timezone.now()
        excluded = [self.row(transport='local'), self.row(status='DEAD'),
            self.row(status='PUBLISHED'), self.row(next_attempt_at=now + timedelta(seconds=1)),
            self.row(status='PROCESSING', locked_until=now + timedelta(seconds=1)),
            self.row(status='PROCESSING', locked_until=None)]
        before = {row.pk: self.fields(row) for row in excluded}
        later = self.row(id=UUID(int=2), created_at=now, next_attempt_at=now)
        first = self.row(id=UUID(int=1), created_at=now, next_attempt_at=now)
        oldest = self.row(created_at=now - timedelta(seconds=1), next_attempt_at=now)
        with patch.object(events.timezone, 'now', return_value=now):
            self.assertEqual([events.claim_event().pk for _ in range(3)],
                             [oldest.pk, first.pk, later.pk])
            self.assertIsNone(events.claim_event())
        for row in excluded:
            self.assertEqual(self.fields(OutboxEvent.objects.get(pk=row.pk)), before[row.pk])

    def test_each_unpublished_earlier_version_blocks_until_published_with_same_aggregate_scope(self):
        now = timezone.now()
        for status in ('PENDING', 'PROCESSING', 'DEAD'):
            with self.subTest(earlier_status=status):
                earlier = self.row(status=status, next_attempt_at=now + timedelta(days=1),
                    locked_until=now + timedelta(days=1))
                later = self.row(aggregate_id=earlier.aggregate_id, aggregate_version=2)
                self.assertIsNone(events.claim_event())
                OutboxEvent.objects.filter(pk=earlier.pk).update(status='PUBLISHED')
                self.assertEqual(events.claim_event().pk, later.pk)
        # Local transport and another aggregate type do not block this scope.
        local = self.row(transport='local', status='DEAD')
        later = self.row(aggregate_id=local.aggregate_id, aggregate_version=2)
        self.assertEqual(events.claim_event().pk, later.pk)
        foreign = self.row(aggregate_type='another-type', status='DEAD')
        later = self.row(aggregate_id=foreign.aggregate_id, aggregate_version=2)
        self.assertEqual(events.claim_event().pk, later.pk)

    def test_processing_reclaim_retains_strict_expiry_boundary_and_replaces_only_lease(self):
        now = timezone.now()
        original_token = uuid4()
        record = self.row(status='PROCESSING', lease_token=original_token,
            locked_until=now, attempts=3, last_error='Prior failure')
        before = self.fields(record)
        with patch.object(events.timezone, 'now', return_value=now):
            self.assertIsNone(events.claim_event())
        reclaimed_at = now + timedelta(microseconds=1)
        with patch.object(events.timezone, 'now', return_value=reclaimed_at):
            claimed = events.claim_event()
        self.assertEqual(claimed.pk, record.pk)
        self.assertNotEqual(claimed.lease_token, original_token)
        self.assertEqual(self.fields(claimed), {**before, 'lease_token': claimed.lease_token,
            'locked_until': reclaimed_at + timedelta(seconds=60)})

    def test_publish_keeps_exact_envelope_and_initializes_legacy_hash_without_content_change(self):
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                record = self.row(legacy=legacy)
                value = events.raw_envelope(record)
                expected_hash = canonical_payload_hash(value)
                before = copy.deepcopy(record.payload_json)
                if legacy:
                    self.assertIsNone(record.payload_hash)
                with patch.object(events, 'send') as send:
                    self.assertTrue(events.publish_one(None))
                self.assertEqual(send.call_count, 1)
                self.assertEqual(send.call_args.args[2:],
                    (f'{record.aggregate_type}:{record.aggregate_id}', value))
                record.refresh_from_db()
                self.assertEqual((record.status, record.attempts), ('PUBLISHED', 0))
                self.assertEqual(record.payload_json, before)
                self.assertEqual(record.payload_hash, expected_hash)
                self.assertIsNone(record.lease_token)
                self.assertIsNone(record.locked_until)

    def test_schema_failure_never_sends_and_preserves_payload_hash_while_marking_dead(self):
        record = self.row(payload_json={'malformed': True})
        before = (copy.deepcopy(record.payload_json), record.payload_hash)
        with patch.object(events, 'send') as send:
            with self.assertRaises(EventValidationError):
                events.publish_one(None)
        send.assert_not_called()
        record.refresh_from_db()
        self.assertEqual((record.status, record.attempts), ('DEAD', 1))
        self.assertEqual((record.payload_json, record.payload_hash), before)
        self.assertIsNone(record.lease_token)

    def test_shard_selection_keeps_payload_and_existing_publisher_shard_annotation(self):
        zero = self.row(aggregate_id=UUID('00000000-0000-0000-0000-000000000001'))
        one = self.row(aggregate_id=UUID('00000001-0000-0000-0000-000000000001'))
        first, sql = self.claim(shard_index=1, shard_count=2)
        self.assertEqual(first.pk, one.pk)
        self.assertEqual(first.payload_json, one.payload_json)
        self.assertFalse(hasattr(first, 'blocked'))
        if connection.vendor == 'postgresql':
            self.assertEqual(first.publisher_shard, 1)
            self.assertIn(' AS "publisher_shard"', sql)
            self.assert_filter_only(sql)
        second = events.claim_event(shard_index=0, shard_count=2)
        self.assertEqual(second.pk, zero.pk)
        self.assertIsNone(events.claim_event(shard_index=1, shard_count=2))


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL SKIP LOCKED claims')
@override_settings(EVENT_LEASE_SECONDS=60)
class PublisherClaimPostgreSQLTests(ClaimFixture, TransactionTestCase):
    def test_real_skip_locked_claims_disjoint_rows_while_first_transaction_open(self):
        now = timezone.now()
        oldest = self.row(created_at=now - timedelta(seconds=1))
        next_row = self.row(created_at=now)
        def contend():
            close_old_connections()
            try:
                pid = self.backend_pid()
                claimed, sql = self.claim()
                return pid, claimed, sql
            finally:
                connections.close_all()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with transaction.atomic():
                holder_pid = self.backend_pid()
                holder, holder_sql = self.claim()
                self.assertEqual(holder.pk, oldest.pk)
                contender_pid, contender, contender_sql = pool.submit(contend).result(timeout=3)
                self.assertNotEqual(holder_pid, contender_pid)
                self.assertEqual(contender.pk, next_row.pk)
                self.assertNotEqual(holder.lease_token, contender.lease_token)
                self.assert_filter_only(holder_sql)
                self.assert_filter_only(contender_sql)
        self.assertIsNone(events.claim_event())
        self.assertEqual(set(OutboxEvent.objects.values_list('status', flat=True)), {'PROCESSING'})
        self.evidence('claim-skip-locked-disjoint.json', {'holder_backend_pid': holder_pid,
            'contender_backend_pid': contender_pid, 'holder_event_id': str(holder.pk),
            'contender_event_id': str(contender.pk), 'holder_sql': holder_sql,
            'contender_sql': contender_sql, 'contender_completed_before_holder_commit': True})

    def check_outer_release(self, *, rollback):
        record = self.row()
        observed = Queue()
        release = threading.Event()
        def contend():
            close_old_connections()
            try:
                pid = self.backend_pid()
                first, first_sql = self.claim()
                visible = OutboxEvent.objects.get(pk=record.pk)
                observed.put((pid, first, visible.status, visible.lease_token, first_sql))
                if not release.wait(5):
                    raise TimeoutError('Holder did not finish its transaction')
                with transaction.atomic():
                    # NOWAIT proves the physical row lock released, even when
                    # the committed lease correctly makes another claim ineligible.
                    released = OutboxEvent.objects.select_for_update(nowait=True).get(pk=record.pk)
                    after = (released.status, released.lease_token)
                second, second_sql = self.claim()
                return after, second, second_sql
            finally:
                connections.close_all()
        with ThreadPoolExecutor(max_workers=1) as pool:
            try:
                with transaction.atomic():
                    holder_pid = self.backend_pid()
                    holder, holder_sql = self.claim()
                    future = pool.submit(contend)
                    contender_pid, first, visible_status, visible_token, first_sql = observed.get(timeout=3)
                    self.assertNotEqual(holder_pid, contender_pid)
                    self.assertIsNone(first, 'SKIP LOCKED must skip the held claim row')
                    self.assertEqual((visible_status, visible_token), ('PENDING', None))
                    self.assertFalse(future.done())
                    if rollback:
                        transaction.set_rollback(True)
            finally:
                release.set()
            after, second, second_sql = future.result(timeout=3)
        if rollback:
            self.assertEqual(after, ('PENDING', None))
            self.assertEqual(second.pk, record.pk)
            self.assertNotEqual(second.lease_token, holder.lease_token)
        else:
            self.assertEqual(after, ('PROCESSING', holder.lease_token))
            self.assertIsNone(second)
        for sql in (holder_sql, first_sql, second_sql):
            self.assert_filter_only(sql)
        self.evidence(f'claim-outer-{"rollback" if rollback else "commit"}.json', {
            'holder_backend_pid': holder_pid, 'contender_backend_pid': contender_pid,
            'held_row_skipped': first is None, 'uncommitted_lease_invisible': visible_token is None,
            'nowait_row_lock_succeeded_after_release': True,
            'released_by': 'ROLLBACK' if rollback else 'COMMIT',
            'row_status_after_release': after[0], 'reclaimed_after_release': second is not None,
            'holder_sql': holder_sql, 'contender_sql_before_release': first_sql,
            'contender_sql_after_release': second_sql})

    def test_claim_lease_commits_and_row_lock_releases_after_outer_commit(self):
        self.check_outer_release(rollback=False)

    def test_claim_lease_rolls_back_and_row_is_claimable_after_outer_rollback(self):
        self.check_outer_release(rollback=True)
