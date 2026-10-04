from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.test import TransactionTestCase
from django.core.management import call_command
from decimal import Decimal

class UpgradeMigrationTests(TransactionTestCase):
    def test_existing_ledger_and_local_outbox_survive_upgrade(self):
        executor=MigrationExecutor(connection)
        before=[('labops','0003_laborder_sample_sampleevent_testcatalog_and_more')]
        latest=executor.loader.graph.leaf_nodes('labops')
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


import importlib
import json
import os
from pathlib import Path
from queue import Queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest import skipUnless
from unittest.mock import patch
from uuid import uuid4

from django.db import DatabaseError, close_old_connections, connections, models, transaction
from django.db.models import F
from django.db.utils import NotSupportedError


index_migration = importlib.import_module('labops.migrations.0007_outbox_active_ordered_index')


class ActiveIndexMigrationFixture(TransactionTestCase):
    before = [('labops', '0006_event_immutability')]
    after = [('labops', '0007_outbox_active_ordered_index')]

    def setUp(self):
        super().setUp()
        self.temporary_index = False
        MigrationExecutor(connection).migrate(self.before)
        executor = MigrationExecutor(connection)
        self.before_state = executor.loader.project_state(self.before)
        self.after_state = executor.loader.project_state(self.after)
        self.operation = index_migration.Migration.operations[0]
        self.model = self.after_state.apps.get_model('labops', 'OutboxEvent')

    def tearDown(self):
        try:
            if self.temporary_index:
                applied = ('labops', self.after[0][1]) in MigrationExecutor(connection).loader.applied_migrations
                self.drop_fixture_index()
                if applied:
                    with connection.schema_editor(atomic=False) as editor:
                        self.operation.database_forwards('labops', editor, self.before_state, self.after_state)
            MigrationExecutor(connection).migrate(self.after)
        finally:
            super().tearDown()

    def row(self, **changes):
        return self.model.objects.create(event_type='LEGACY_NOTICE', transport='kafka',
            aggregate_type='migration-fixture', aggregate_id=uuid4(), dedupe_key=str(uuid4()),
            **changes)

    def indexes(self):
        with connection.cursor() as cursor:
            return connection.introspection.get_constraints(cursor, index_migration.TABLE_NAME)

    def catalog(self):
        with connection.schema_editor(atomic=False) as editor:
            return index_migration.index_catalog(editor)

    def drop_fixture_index(self):
        # Only these tests create this name in their disposable test database.
        quote = connection.ops.quote_name
        with connection.cursor() as cursor:
            if connection.vendor == 'postgresql':
                cursor.execute(f'DROP INDEX CONCURRENTLY {quote(index_migration.SCHEMA_NAME)}.{quote(index_migration.INDEX_NAME)}')
            else:
                cursor.execute(f'DROP INDEX {quote(index_migration.INDEX_NAME)}')
        self.temporary_index = False

    def fixture_index(self, index):
        with connection.schema_editor(atomic=False) as editor:
            editor.add_index(self.model, index)
        self.temporary_index = True

    def evidence(self, name, value):
        directory = os.environ.get('LABOPS_OUTBOX_INDEX_PROOF_EVIDENCE')
        if directory:
            (Path(directory) / name).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')

    def pid(self):
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_backend_pid()')
            return cursor.fetchone()[0]


class OutboxActiveIndexMigrationTests(ActiveIndexMigrationFixture):
    def test_model_state_and_vendor_sql_match_the_partial_ordered_index(self):
        from labops.models import OutboxEvent
        runtime = next(i for i in OutboxEvent._meta.indexes if i.name == index_migration.INDEX_NAME)
        historical = next(i for i in self.model._meta.indexes if i.name == index_migration.INDEX_NAME)
        self.assertEqual(runtime.deconstruct(), historical.deconstruct())
        self.assertEqual(self.operation.index.deconstruct(), historical.deconstruct())
        self.assertFalse(index_migration.Migration.atomic)
        self.assertFalse(self.operation.atomic)
        original_table = self.model._meta.db_table
        with connection.schema_editor(collect_sql=True, atomic=False) as editor:
            self.operation.database_forwards('labops', editor, self.before_state, self.after_state)
            forward = editor.collected_sql[-1]
            self.operation.database_backwards('labops', editor, self.after_state, self.before_state)
            reverse = editor.collected_sql[-1]
        self.assertEqual(self.model._meta.db_table, original_table)
        self.assertIn('("created_at", "id")', forward)
        self.assertIn("'kafka'", forward)
        self.assertIn("'PENDING'", forward)
        self.assertIn("'PROCESSING'", forward)
        self.assertNotIn('IF NOT EXISTS', forward)
        if connection.vendor == 'postgresql':
            self.assertTrue(forward.startswith('CREATE INDEX CONCURRENTLY "outbox_active_created_id_idx" ON "public"."labops_outboxevent"'))
            self.assertEqual(reverse, 'DROP INDEX CONCURRENTLY "public"."outbox_active_created_id_idx";')
        else:
            self.assertNotIn('CONCURRENTLY', forward + reverse)

    def test_forward_reverse_forward_preserves_data_and_every_existing_index(self):
        record = self.row(payload_json={'synthetic': 'retained'}, attempts=3)
        original_fields = {f.attname: getattr(record, f.attname) for f in record._meta.concrete_fields}
        before = self.indexes()
        observed = []
        for target, present in [(self.after, True), (self.before, False), (self.after, True)]:
            MigrationExecutor(connection).migrate(target)
            indexes = self.indexes()
            self.assertEqual(index_migration.INDEX_NAME in indexes, present)
            for name, definition in before.items():
                self.assertEqual(indexes[name], definition)
            fresh = self.model.objects.get(pk=record.pk)
            self.assertEqual({f.attname: getattr(fresh, f.attname) for f in fresh._meta.concrete_fields}, original_fields)
            catalog = self.catalog() if connection.vendor == 'postgresql' else None
            if present:
                self.assertEqual(indexes[index_migration.INDEX_NAME]['columns'], ['created_at', 'id'])
                self.assertFalse(indexes[index_migration.INDEX_NAME]['unique'])
                if catalog:
                    with connection.schema_editor(atomic=False) as editor:
                        index_migration.require_owned_index(editor, index_migration.target_table(editor))
            observed.append({'target': target[0][1], 'index_present': present,
                'old_indexes_preserved': True, 'fields_preserved': True, 'catalog': catalog})
        self.evidence('outbox-index-roundtrip.json', {'observations': observed,
            'old_index_names': sorted(before), 'event_id': str(record.pk)})

    def test_same_name_collision_aborts_without_reusing_or_deleting_it(self):
        record = self.row()
        wrong = models.Index(fields=['id'], name=index_migration.INDEX_NAME)
        self.fixture_index(wrong)
        before = self.indexes()[index_migration.INDEX_NAME]
        with self.assertRaises((RuntimeError, DatabaseError)):
            MigrationExecutor(connection).migrate(self.after)
        self.assertEqual(self.indexes()[index_migration.INDEX_NAME], before)
        self.assertTrue(self.model.objects.filter(pk=record.pk).exists())
        self.assertNotIn(tuple(self.after[0]), MigrationExecutor(connection).loader.applied_migrations)
        self.drop_fixture_index()
        MigrationExecutor(connection).migrate(self.after)

    def test_router_denial_keeps_state_but_emits_no_database_operation(self):
        with patch.object(self.operation, 'allow_migrate_model', return_value=False), \
                connection.schema_editor(collect_sql=True, atomic=False) as editor:
            self.operation.database_forwards('labops', editor, self.before_state, self.after_state)
            self.operation.database_backwards('labops', editor, self.after_state, self.before_state)
            self.assertEqual(editor.collected_sql, [])
        self.assertTrue(any(i.name == index_migration.INDEX_NAME for i in self.model._meta.indexes))


@skipUnless(connection.vendor == 'postgresql', 'Actual PostgreSQL concurrent index migration')
class OutboxActiveIndexPostgreSQLTests(ActiveIndexMigrationFixture):
    def test_postgresql_rejects_atomic_execution_before_ddl(self):
        with transaction.atomic(), connection.schema_editor(atomic=False) as editor:
            with self.assertRaises(NotSupportedError):
                self.operation.database_forwards('labops', editor, self.before_state, self.after_state)
            with self.assertRaises(NotSupportedError):
                self.operation.database_backwards('labops', editor, self.after_state, self.before_state)
        self.assertIsNone(self.catalog())
        MigrationExecutor(connection).migrate(self.after)
        self.assertTrue(self.catalog()[5])

    def wait_for_pending_ddl(self, builder_pid, future, forward):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if future.done():
                future.result()
                self.fail('Concurrent index DDL completed before the held transaction released')
            with connection.cursor() as cursor:
                if forward:
                    cursor.execute('''SELECT phase, relid, index_relid
                        FROM pg_stat_progress_create_index WHERE pid = %s''', [builder_pid])
                else:
                    cursor.execute('''SELECT wait_event, state FROM pg_stat_activity
                        WHERE pid = %s AND state = 'active' AND wait_event_type = 'Lock'
                          AND query LIKE 'DROP INDEX CONCURRENTLY %%' ''', [builder_pid])
                progress = cursor.fetchone()
            catalog = self.catalog()
            if progress and catalog and not catalog[5]:
                return {'progress': list(progress), 'catalog': catalog}
            threading.Event().wait(.02)
        self.fail('Expected blocked concurrent index DDL was not observed')

    def controlled_ddl(self, *, forward, cancel=False):
        if not forward:
            MigrationExecutor(connection).migrate(self.after)
        row = self.row()
        release = threading.Event()
        held = Queue()
        started = Queue()
        def holder():
            close_old_connections()
            try:
                with transaction.atomic():
                    if forward:
                        self.model.objects.filter(pk=row.pk).update(attempts=F('attempts') + 1)
                    else:
                        self.model.objects.filter(pk=row.pk).exists()
                    held.put(self.pid())
                    if not release.wait(12):
                        raise TimeoutError('Index fixture holder was not released')
            finally:
                connections.close_all()
        def builder():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '15s'")
                started.put(self.pid())
                MigrationExecutor(connection).migrate(self.after if forward else self.before)
                return True
            finally:
                connections.close_all()
        def observe_written(ident):
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '3s'")
                return self.pid(), self.model.objects.filter(pk=ident).exists()
            finally:
                connections.close_all()
        with ThreadPoolExecutor(max_workers=3) as pool:
            hold_future = pool.submit(holder)
            ddl_future = None
            try:
                holder_pid = held.get(timeout=3)
                ddl_future = pool.submit(builder)
                builder_pid = started.get(timeout=3)
                self.assertNotEqual(builder_pid, holder_pid)
                waiting = self.wait_for_pending_ddl(builder_pid, ddl_future, forward)
                if forward:
                    self.temporary_index = True
                result = {'holder_backend_pid': holder_pid, 'builder_backend_pid': builder_pid,
                    'forward': forward, **waiting}
                if cancel:
                    with connection.cursor() as cursor:
                        cursor.execute('SELECT pg_cancel_backend(%s)', [builder_pid])
                        self.assertTrue(cursor.fetchone()[0])
                    with self.assertRaises(DatabaseError) as caught:
                        ddl_future.result(timeout=4)
                    result['cancelled_sqlstate'] = getattr(caught.exception.__cause__, 'sqlstate', None)
                    self.assertEqual(result['cancelled_sqlstate'], '57014')
                else:
                    writer_pid = self.pid()
                    self.assertNotIn(writer_pid, [holder_pid, builder_pid])
                    with transaction.atomic():
                        written = self.row()
                    self.assertTrue(connection.get_autocommit())
                    self.assertFalse(connection.in_atomic_block)
                    observer_pid, observed = pool.submit(observe_written, written.pk).result(timeout=4)
                    self.assertNotIn(observer_pid, [holder_pid, builder_pid, writer_pid])
                    self.assertTrue(observed, 'An independent backend must see the committed writer row')
                    self.assertFalse(ddl_future.done(), 'Writer must commit while index DDL remains blocked')
                    result.update(writer_backend_pid=writer_pid,
                        writer_committed_while_ddl_pending=True, written_event_id=str(written.pk),
                        writer_autocommit_after_commit=connection.get_autocommit(),
                        writer_in_atomic_block_after_commit=connection.in_atomic_block,
                        observer_backend_pid=observer_pid, observer_saw_committed_write=observed)
                    release.set()
                    self.assertTrue(ddl_future.result(timeout=4))
                    self.temporary_index = False
                return result
            finally:
                release.set()
                hold_future.result(timeout=4)
                if ddl_future is not None and not ddl_future.done():
                    ddl_future.result(timeout=4)

    def test_concurrent_forward_and_reverse_allow_a_distinct_writer_to_commit(self):
        forward = self.controlled_ddl(forward=True)
        reverse = self.controlled_ddl(forward=False)
        self.assertIsNone(self.catalog())
        MigrationExecutor(connection).migrate(self.after)
        self.evidence('outbox-index-concurrent-writer.json', {'observations': [forward, reverse]})

    def test_cancelled_build_retains_invalid_index_and_retry_fails_closed(self):
        cancelled = self.controlled_ddl(forward=True, cancel=True)
        catalog = self.catalog()
        self.assertIsNotNone(catalog)
        self.assertFalse(catalog[5])
        self.assertNotIn(tuple(self.after[0]), MigrationExecutor(connection).loader.applied_migrations)
        with self.assertRaisesMessage(RuntimeError, 'name already exists'):
            MigrationExecutor(connection).migrate(self.after)
        self.assertEqual(self.catalog(), catalog)
        self.drop_fixture_index()
        MigrationExecutor(connection).migrate(self.after)
        self.assertTrue(self.catalog()[5])
        self.evidence('outbox-index-interrupted-build.json', {**cancelled,
            'invalid_catalog_retained': catalog, 'retry_rejected_without_deletion': True,
            'explicit_owned_fixture_cleanup_then_migration_succeeded': True})

    def test_reverse_rejects_missing_or_mismatched_index_without_dropping_it(self):
        observations = []
        wrong_indexes = [
            models.Index(fields=['id', 'created_at'], name=index_migration.INDEX_NAME,
                condition=models.Q(transport='kafka') & models.Q(status__in=['PENDING', 'PROCESSING'])),
            models.Index(fields=['created_at', 'id'], name=index_migration.INDEX_NAME,
                condition=models.Q(transport='local') & models.Q(status__in=['PENDING', 'PROCESSING'])),
        ]
        for wrong in [None, *wrong_indexes]:
            with self.subTest(index=None if wrong is None else wrong.deconstruct()):
                if wrong is not None:
                    self.fixture_index(wrong)
                before = self.catalog()
                with connection.schema_editor(atomic=False) as editor:
                    with self.assertRaisesMessage(RuntimeError, 'catalog does not match'):
                        self.operation.database_backwards('labops', editor, self.after_state, self.before_state)
                self.assertEqual(self.catalog(), before)
                observations.append({'kind': 'missing' if wrong is None else 'mismatched',
                    'catalog_preserved': True, 'catalog': before})
                if wrong is not None:
                    self.drop_fixture_index()
        self.evidence('outbox-index-reverse-guard.json', {'observations': observations})
