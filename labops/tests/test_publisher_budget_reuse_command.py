"""Real PG command lifecycle with an explicit controller-only admission seam.

The delivery fixture is unknown to production admission. Forcing admission here
isolates command ordering and cleanup; separate native-identity tests prove the
real production gate, and hosted Kafka validation proves actual broker delivery.
"""
from contextlib import ExitStack
import signal
import time
from unittest import skipUnless
from unittest.mock import patch

from django.core.management import call_command
from django.db import connection
from django.db.models.query import QuerySet
from django.test import TransactionTestCase, override_settings

from labops import events
from labops.management.commands import publish_events as command
from labops.models import OutboxEvent
from labops.publisher_shards import PublisherShardOwner, ShardOwnershipLost
from labops.tests.test_diagnostic_profile import DeliveryClientFixture
from labops.tests.test_publisher_claim_reads import ClaimFixture
from labops.tests.test_publisher_budget_reuse_admission import ordinary_connection_capability
from labops.worker_metrics import OperationDeadlineExceeded, operation_deadline


class LifecycleClient(DeliveryClientFixture):
    def purge(self, *args, **kwargs):
        self.calls.append(('purge', args, kwargs))


@skipUnless(connection.vendor == 'postgresql', 'Real PG command budget lifecycle')
@override_settings(WORKER_METRICS_ENABLED=False, EVENT_LEASE_SECONDS=60)
class PublisherBudgetCommandPostgreSQLTests(ClaimFixture, TransactionTestCase):
    def values(self):
        with connection.connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('statement_timeout'), current_setting('lock_timeout'), pg_backend_pid()")
            return cursor.fetchone()

    def make_rows(self, count):
        rows = [self.row() for _ in range(count)]
        with connection.cursor() as cursor:
            cursor.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)", ['37000', '91'])
        return rows

    def seam(self, stack):
        # Real publish_one/claims/leases/fenced writes; deterministic delivery.
        stack.enter_context(ordinary_connection_capability())
        stack.enter_context(patch('confluent_kafka.Producer', LifecycleClient))
        class ControllerOnlyAdmission:
            aliases = command._budget_aliases()
            def plain(self, *_args):
                return True
        stack.enter_context(patch.object(command, '_BUDGET_ADMISSION', ControllerOnlyAdmission()))

    def receipt(self, captured):
        lines = [record.getMessage() for record in captured.records
                 if record.getMessage().startswith('publisher_budget_reuse schema=1 ')]
        self.assertEqual(len(lines), 1)
        return dict(item.split('=', 1) for item in lines[0].split()[1:])

    def test_three_records_keep_owner_lease_ack_writeback_order_and_one_budget_pair(self):
        rows = self.make_rows(3)
        before, raw, steps = self.values(), connection.connection, []
        original_owner, original_claim = PublisherShardOwner.assert_owned, events.claim_event
        original_owned, original_send = events.owned_event, events.send
        original_update = QuerySet.update
        def owned(owner):
            result = original_owner(owner)
            steps.append('owner')
            return result
        def claim(**kwargs):
            steps.append('claim')
            return original_claim(**kwargs)
        def lease(row):
            steps.append('lease')
            return original_owned(row)
        def send(*args, **kwargs):
            self.assertTrue(connection.get_autocommit())
            self.assertFalse(connection.in_atomic_block)
            steps.append('send')
            result = original_send(*args, **kwargs)
            steps.append('ack')
            return result
        def update(queryset, **kwargs):
            result = original_update(queryset, **kwargs)
            if kwargs.get('status') == 'PUBLISHED':
                steps.append('writeback')
            return result
        with ExitStack() as stack:
            self.seam(stack)
            stack.enter_context(patch.object(PublisherShardOwner, 'assert_owned', owned))
            stack.enter_context(patch.object(events, 'claim_event', claim))
            stack.enter_context(patch.object(events, 'owned_event', lease))
            stack.enter_context(patch.object(events, 'send', send))
            stack.enter_context(patch.object(QuerySet, 'update', update))
            with self.assertLogs('labops', level='INFO') as logs:
                call_command('publish_events', limit=3)
        # One owner acquisition assertion, then the original three checks per row.
        self.assertEqual(steps, ['owner'] + ['owner', 'claim', 'owner', 'lease', 'send', 'ack', 'owner', 'writeback'] * 3)
        self.assertEqual(OutboxEvent.objects.filter(pk__in=[row.pk for row in rows], status='PUBLISHED').count(), 3)
        self.assertIs(connection.connection, raw)
        self.assertEqual(self.values(), before)
        receipt = self.receipt(logs)
        self.assertEqual({key: receipt[key] for key in ('outcome', 'returned_true', 'setup_completed', 'reused_records', 'restore_completed')},
                         {'outcome': 'full', 'returned_true': '3', 'setup_completed': '1', 'reused_records': '2', 'restore_completed': '1'})

    def test_real_sigterm_finishes_current_ack_writeback_restores_then_stops(self):
        rows = self.make_rows(2)
        before, sends = self.values(), []
        original_send = events.send
        def send(*args, **kwargs):
            result = original_send(*args, **kwargs)
            sends.append(True)
            signal.raise_signal(signal.SIGTERM)
            return result
        with ExitStack() as stack:
            self.seam(stack)
            stack.enter_context(patch.object(events, 'send', send))
            with self.assertLogs('labops', level='INFO') as logs:
                call_command('publish_events', loop=True, limit=3)
        self.assertEqual(sends, [True])
        states = list(OutboxEvent.objects.filter(pk__in=[row.pk for row in rows]).values_list('status', flat=True))
        self.assertEqual(sorted(states), ['PENDING', 'PUBLISHED'])
        self.assertEqual(self.values(), before)
        receipt = self.receipt(logs)
        self.assertEqual(receipt['outcome'], 'stopped')
        self.assertEqual(receipt['returned_true'], '1')
        self.assertEqual(receipt['restore_completed'], '1')
        self.assertEqual(receipt['reused_records'], '0')

    def test_next_outer_owner_loss_restores_before_purge_without_claim_or_reacquire(self):
        rows = self.make_rows(2)
        before, checks, broker = self.values(), [], []
        original_owner = PublisherShardOwner.assert_owned
        def owned(owner):
            checks.append(True)
            if len(checks) == 5:  # acquire, first outer+two inner, next outer
                raise ShardOwnershipLost('Injected next-record owner loss')
            return original_owner(owner)
        original_producer = events.producer
        def producer():
            client = original_producer()
            broker.append(client)
            return client
        with ExitStack() as stack:
            stack.enter_context(patch.object(command, 'producer', producer))
            self.seam(stack)
            stack.enter_context(patch.object(PublisherShardOwner, 'assert_owned', owned))
            with self.assertLogs('labops', level='INFO') as logs, self.assertRaises(ShardOwnershipLost):
                call_command('publish_events', loop=True, limit=3)
        self.assertEqual(len(checks), 5)
        self.assertEqual(self.values(), before)
        states = list(OutboxEvent.objects.filter(pk__in=[row.pk for row in rows]).values_list('status', flat=True))
        self.assertEqual(sorted(states), ['PENDING', 'PUBLISHED'])
        native = broker[0].client
        self.assertEqual(sum(name == 'flush' for name, _, _ in native.calls), 1)  # first send ACK; no final drain
        self.assertEqual(sum(name == 'purge' for name, _, _ in native.calls), 1)
        receipt = self.receipt(logs)
        self.assertEqual(receipt['outcome'], 'failed')
        self.assertEqual(receipt['restore_completed'], '1')
        self.assertEqual(receipt['returned_true'], '1')

    def test_real_second_record_deadline_discards_and_leaves_committed_unused_claim(self):
        rows = self.make_rows(2)
        raw, sends, deadlines = connection.connection, [], []
        original_send = events.send
        def send(*args, **kwargs):
            sends.append(True)
            if len(sends) == 2:
                time.sleep(.3)
            return original_send(*args, **kwargs)
        def deadline(seconds):
            deadlines.append(seconds)
            return operation_deadline(.1 if len(deadlines) == 2 else 30)
        with ExitStack() as stack:
            stack.enter_context(patch.object(command, 'operation_deadline', deadline))
            self.seam(stack)
            stack.enter_context(patch.object(events, 'send', send))
            with self.assertLogs('labops', level='INFO') as logs, self.assertRaises(OperationDeadlineExceeded):
                call_command('publish_events', limit=3)
        self.assertEqual(len(sends), 2)
        self.assertTrue(raw.closed)
        self.assertIsNone(connection.connection)
        states = list(OutboxEvent.objects.filter(pk__in=[row.pk for row in rows]).values_list('status', flat=True))
        self.assertEqual(sorted(states), ['PROCESSING', 'PUBLISHED'])
        receipt = self.receipt(logs)
        self.assertEqual(receipt['outcome'], 'failed')
        self.assertEqual(receipt['returned_true'], '1')
        self.assertEqual(receipt['restore_attempts'], '0')
        self.assertEqual(receipt['discard_completed'], '1')
