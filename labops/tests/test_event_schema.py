import copy
import json
import math
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from unittest import skipUnless

from django.db import connection, DatabaseError, IntegrityError, transaction
from django.db.migrations.executor import MigrationExecutor
from django.test import SimpleTestCase, TestCase, TransactionTestCase

from labops.event_schema import (
    EventValidationError, MAX_EVENT_BYTES, canonical_json_bytes,
    canonical_payload_hash, validate_inventory_envelope,
)
from django.utils import timezone
from jsonschema import Draft202012Validator, ValidationError as JSONSchemaValidationError
from labops.models import DeliveryAudit, FailedDelivery, OutboxEvent, ProcessedEvent


SCHEMAS = Path(__file__).resolve().parents[2] / 'schemas'


def valid_event():
    return json.loads((SCHEMAS / 'fixtures/inventory-v1/issue.json').read_text())


class InventoryContractTests(SimpleTestCase):
    def test_every_supported_movement_fixture_is_accepted(self):
        paths = sorted((SCHEMAS / 'fixtures/inventory-v1').glob('*.json'))
        self.assertEqual(len(paths), 6)
        for path in paths:
            with self.subTest(kind=path.stem):
                value = json.loads(path.read_text())
                self.assertIs(validate_inventory_envelope(value), value)

    def test_existing_v1_short_fixed6_and_optional_trace_carriers(self):
        event = valid_event()
        event.pop('trace_context')
        event['payload'].pop('_trace_context')
        event['payload']['lines'][0].update(delta_qty='-3', unit_cost='10')
        self.assertIs(validate_inventory_envelope(event), event)

    def test_schema_documents_and_dlq_fixture_retain_poison_identity(self):
        for path in SCHEMAS.rglob('*.schema.json'):
            schema = json.loads(path.read_text())
            self.assertEqual(schema['$schema'], 'https://json-schema.org/draft/2020-12/schema')
            Draft202012Validator.check_schema(schema)
        fixture = json.loads((SCHEMAS / 'fixtures/dlq-v1/schema-rejected.json').read_text())
        self.assertEqual(fixture['original_hash'], canonical_payload_hash(fixture['event']))
        with self.assertRaises(EventValidationError):
            validate_inventory_envelope(fixture['event'])

    def test_actual_json_schemas_validate_every_contract_fixture(self):
        validator = Draft202012Validator(json.loads((SCHEMAS / 'inventory/v1/envelope.schema.json').read_text()))
        for path in sorted((SCHEMAS / 'fixtures/inventory-v1').glob('*.json')):
            with self.subTest(kind=path.stem):
                validator.validate(json.loads(path.read_text()))
        dlq = Draft202012Validator(json.loads((SCHEMAS / 'dlq/v1/envelope.schema.json').read_text()))
        for path in sorted((SCHEMAS / 'fixtures/dlq-v1').glob('*.json')):
            dlq.validate(json.loads(path.read_text()))

    def test_schema_and_runtime_both_reject_trailing_lf_decimals_and_utc_time(self):
        validator = Draft202012Validator(json.loads((SCHEMAS / 'inventory/v1/envelope.schema.json').read_text()))
        for field in ('delta_qty', 'unit_cost'):
            event = valid_event()
            event['payload']['lines'][0][field] += '\n'
            with self.subTest(field=field), self.assertRaises(JSONSchemaValidationError):
                validator.validate(event)
            with self.assertRaises(EventValidationError):
                validate_inventory_envelope(event)
        event = valid_event(); event['occurred_at'] += '\n'
        with self.assertRaises(JSONSchemaValidationError):
            validator.validate(event)
        dlq = Draft202012Validator(json.loads((SCHEMAS / 'dlq/v1/envelope.schema.json').read_text()))
        fixture = json.loads((SCHEMAS / 'fixtures/dlq-v1/schema-rejected.json').read_text())
        fixture['original_hash'] += '\n'
        with self.assertRaises(JSONSchemaValidationError):
            dlq.validate(fixture)

    def test_invalid_scalar_versions_and_types_are_rejected(self):
        mutations = []
        for field in ('event_id', 'aggregate_id'):
            for value in (None, '', 'bad', 'a' * 36, 1, True, str(uuid.uuid4()).replace('-', ''), [], {}):
                mutations.append((field, value))
        for field in ('schema_version', 'aggregate_version'):
            for value in (None, False, True, 0, -1, 1.0, '1', 2147483648, [], {}):
                mutations.append((field, value))
        mutations += [('schema_version', 2), ('event_type', 'inventory.unknown.posted'),
                      ('event_type', []), ('event_type', {}), ('event_type', False),
                      ('aggregate_type', 'batch'), ('aggregate_type', None)]
        for field, value in mutations:
            with self.subTest(field=field, value=value):
                event = valid_event()
                event[field] = value
                with self.assertRaises(EventValidationError):
                    validate_inventory_envelope(event)

    def test_invalid_fixed6_matrix(self):
        values = (None, True, 1, 1.5, '', 'NaN', '-NaN', 'Infinity', '-Infinity',
                  '1e2', '1E-6', ' 1', '1 ', '+1', '.1', '1.', '01', '00.1',
                  '1.0000001', '1000000000000', '-1000000000000', '1_000',
                  '١', '0x10', [], {})
        tested = 0
        for field in ('delta_qty', 'unit_cost'):
            for value in values:
                with self.subTest(field=field, value=value):
                    event = valid_event()
                    event['payload']['lines'][0][field] = value
                    with self.assertRaises(EventValidationError):
                        validate_inventory_envelope(event)
                    tested += 1
        for zero in ('0', '-0', '0.000000', '-0.000000'):
            event = valid_event()
            event['payload']['lines'][0]['delta_qty'] = zero
            with self.assertRaises(EventValidationError):
                validate_inventory_envelope(event)
        event = valid_event()
        event['payload']['lines'][0]['unit_cost'] = '-0.000001'
        with self.assertRaises(EventValidationError):
            validate_inventory_envelope(event)
        self.assertGreaterEqual(tested, 50)

    def test_fixed6_extremes_remain_exact(self):
        for delta in ('999999999999.999999', '-999999999999.999999', '0.000001', '-0.000001'):
            event = valid_event()
            event['payload']['lines'][0].update(delta_qty=delta, unit_cost='999999999999.999999')
            validate_inventory_envelope(event)

    def test_occurrence_time_requires_real_explicit_utc(self):
        for bad in ('2026-09-27', '2026-09-27T12:00:00', '2026-09-27T12:00:00-04:00',
                    '2026-09-27T12:00:00+01:00', '2026-02-30T12:00:00Z',
                    '2026-09-27T25:00:00Z', '2026-09-27T12:00:60Z',
                    '2026-09-27 12:00:00Z', '2026-09-27T12:00:00.1234567Z', None, True):
            with self.subTest(timestamp=bad):
                event = valid_event()
                event['occurred_at'] = bad
                with self.assertRaises(EventValidationError):
                    validate_inventory_envelope(event)

    def test_shape_limits_and_semantic_mismatches(self):
        events = []
        base = valid_event()
        for field in base:
            event = copy.deepcopy(base); del event[field]
            if field != 'trace_context':
                events.append(event)
        event = valid_event(); event['unknown'] = 'future'; events.append(event)
        event = valid_event(); event['payload']['unknown'] = 'future'; events.append(event)
        event = valid_event(); event['payload']['movement_id'] = str(uuid.uuid4()); events.append(event)
        event = valid_event(); event['payload']['movement_type'] = 'OPENING'; events.append(event)
        event = valid_event(); event['payload']['recipients'] *= 2; events.append(event)
        event = valid_event(); event['payload']['recipients'] = [str(uuid.uuid4()) for _ in range(1001)]; events.append(event)
        event = valid_event(); event['payload']['lines'] = []; events.append(event)
        event = valid_event(); event['payload']['lines'] *= 1001; events.append(event)
        event = valid_event(); event['payload']['lines'][0]['unknown'] = 1; events.append(event)
        event = valid_event(); event['payload']['lines'][0]['batch_id'] = 'bad'; events.append(event)
        event = valid_event(); event['payload']['title'] = 'x' * 161; events.append(event)
        event = valid_event(); event['payload']['body'] = 'x' * 4097; events.append(event)
        event = valid_event(); event['payload']['body'] = 'NUL\x00'; events.append(event)
        event = valid_event(); event['trace_context'] = {'x': 1}; events.append(event)
        event = valid_event(); event['trace_context'] = {'x': 'y' * 2049}; events.append(event)
        event = valid_event(); event['trace_context'] = {str(x): 'y' for x in range(33)}; events.append(event)
        event = valid_event(); event['payload']['_trace_context'] = []; events.append(event)
        for event in events:
            with self.subTest(event=event):
                with self.assertRaises(EventValidationError):
                    validate_inventory_envelope(event)

    def test_size_limit_counts_utf8_bytes_and_boundary(self):
        event = valid_event()
        event['payload']['body'] = '库存' * 100
        size = len(canonical_json_bytes(event))
        validate_inventory_envelope(event, max_bytes=size)
        with self.assertRaises(EventValidationError) as raised:
            validate_inventory_envelope(event, max_bytes=size - 1)
        self.assertEqual(raised.exception.code, 'size')
        self.assertLess(size, MAX_EVENT_BYTES)

    def test_hash_is_key_order_stable_and_includes_all_content(self):
        event = valid_event()
        reordered = dict(reversed(list(event.items())))
        self.assertEqual(canonical_payload_hash(event), canonical_payload_hash(reordered))
        for field, value in (('occurred_at', '2026-09-27T13:00:00Z'),
                             ('aggregate_version', 2), ('trace_context', {})):
            changed = copy.deepcopy(event); changed[field] = value
            self.assertNotEqual(canonical_payload_hash(event), canonical_payload_hash(changed))
        changed = copy.deepcopy(event); changed['payload']['_trace_context'] = {}
        self.assertNotEqual(canonical_payload_hash(event), canonical_payload_hash(changed))
        changed = copy.deepcopy(event); changed['payload']['lines'][0]['delta_qty'] = '-1.000002'
        self.assertNotEqual(canonical_payload_hash(event), canonical_payload_hash(changed))

    def test_checksum_normalizes_numeric_semantics_without_rewriting_input(self):
        value = {'nested': [1e20, 1e30, -0.0, 1.5, 1e-20]}
        self.assertEqual(canonical_payload_hash(value), canonical_payload_hash(
            {'nested': [10**20, 10**30, 0, 1.5, 1e-20]}))
        self.assertEqual(canonical_json_bytes(value), b'{"nested":[1e+20,1e+30,-0.0,1.5,1e-20]}')
        event = valid_event(); event['aggregate_version'] = 1.0
        with self.assertRaises(EventValidationError):
            validate_inventory_envelope(event)

    def test_arbitrary_finite_json_poison_is_hashable_but_nonfinite_rejected(self):
        for value in (None, True, False, 42, 'invalid JSON record', [], {'raw': 'not-json'}):
            self.assertEqual(len(canonical_payload_hash(value)), 64)
        for value in (math.nan, math.inf, -math.inf, {'x': math.nan}, {'x': object()}, '\ud800'):
            with self.assertRaises(EventValidationError):
                canonical_payload_hash(value)


class DeliveryAuditContractTests(TestCase):
    def test_public_audit_mutation_apis_are_append_only(self):
        audit = DeliveryAudit.objects.create(actor_label='operator:contract-test',
            action='REPLAY', outcome='DRY_RUN', reason='contract check',
            authorization_json={'scope': 'test'})
        audit.reason = 'changed'
        for action in (lambda: audit.save(), lambda: audit.delete(),
                       lambda: DeliveryAudit.objects.filter(pk=audit.pk).update(reason='changed'),
                       lambda: DeliveryAudit.objects.filter(pk=audit.pk).delete(),
                       lambda: DeliveryAudit.objects.bulk_update([audit], ['reason']),
                       lambda: DeliveryAudit.objects.bulk_create([audit], update_conflicts=True,
                           update_fields=['reason'], unique_fields=['id'])):
            with self.assertRaisesMessage(ValueError, 'append-only'):
                action()
        audit.refresh_from_db()
        self.assertEqual(audit.reason, 'contract check')
        with self.assertRaises(IntegrityError), transaction.atomic():
            DeliveryAudit(id=audit.id, action='REPLAY', outcome='CHANGED', reason='pk-overwrite').save()
        audit.refresh_from_db()
        self.assertEqual(audit.reason, 'contract check')

    def test_delivery_identity_scopes_identical_offsets_to_source_generation(self):
        first = FailedDelivery.objects.create(consumer_name='analytics', delivery_key='topic:0:1',
            source_cluster='cluster-a', source_generation='generation-a')
        second = FailedDelivery.objects.create(consumer_name='analytics', delivery_key='topic:0:1',
            source_cluster='cluster-a', source_generation='generation-b')
        self.assertNotEqual(first.pk, second.pk)
        with self.assertRaises(IntegrityError), transaction.atomic():
            FailedDelivery.objects.create(consumer_name='analytics', delivery_key='topic:0:1',
                source_cluster='cluster-a', source_generation='generation-a')


class EventContractMigrationTests(TransactionTestCase):
    before = [('labops', '0004_reliable_events')]
    latest = [('labops', '0006_event_immutability')]

    def test_legacy_rows_are_hashed_without_changing_content_or_route(self):
        try:
            MigrationExecutor(connection).migrate(self.before)
            executor = MigrationExecutor(connection)
            apps = executor.loader.project_state(self.before).apps
            envelope = valid_event()
            outbox = apps.get_model('labops', 'OutboxEvent').objects.create(
                id=envelope['event_id'], event_type=envelope['event_type'],
                aggregate_type=envelope['aggregate_type'], aggregate_id=envelope['aggregate_id'],
                payload_json=envelope['payload'], dedupe_key='legacy-inventory', transport='local')
            apps.get_model('labops', 'ProcessedEvent').objects.create(consumer_name='analytics', event_id=outbox.id)
            apps.get_model('labops', 'ProcessedEvent').objects.create(consumer_name='analytics', event_id=uuid.uuid4())
            failed = apps.get_model('labops', 'FailedDelivery').objects.create(
                consumer_name='analytics', delivery_key='topic:0:1', envelope={'poison': True})
            numeric_poison = {'nested': [1.0, -0.0, 1e20, 1e30]}
            numeric_failed = apps.get_model('labops', 'FailedDelivery').objects.create(
                consumer_name='analytics', delivery_key='topic:0:2', envelope=numeric_poison)
            MigrationExecutor(connection).migrate(self.latest)

            from labops.models import ProcessedEvent
            retained = OutboxEvent.objects.get(pk=outbox.pk)
            self.assertEqual(retained.payload_json, envelope['payload'])
            self.assertEqual(retained.transport, 'local')
            frozen = {**envelope, 'occurred_at': retained.created_at.isoformat()}
            self.assertEqual(retained.payload_hash, canonical_payload_hash(frozen))
            self.assertEqual(ProcessedEvent.objects.get(event_id=outbox.id).payload_hash, retained.payload_hash)
            self.assertEqual(ProcessedEvent.objects.filter(payload_hash__isnull=True).count(), 1)
            self.assertEqual(FailedDelivery.objects.get(pk=failed.pk).original_hash,
                             canonical_payload_hash({'poison': True}))
            numeric_retained = FailedDelivery.objects.get(pk=numeric_failed.pk)
            self.assertEqual(numeric_retained.original_hash, canonical_payload_hash(numeric_poison))
            self.assertEqual(numeric_retained.original_hash, canonical_payload_hash(numeric_retained.envelope))
            self.assertTrue(DeliveryAudit.objects.filter(action='SCHEMA_BACKFILL').exists())
        finally:
            MigrationExecutor(connection).migrate(self.latest)

    def test_historical_duplicate_inventory_versions_abort_without_deletion(self):
        historical = None
        try:
            MigrationExecutor(connection).migrate(self.before)
            executor = MigrationExecutor(connection)
            historical = executor.loader.project_state(self.before).apps.get_model('labops', 'OutboxEvent')
            aggregate_id = uuid.uuid4()
            for key in ('duplicate-a', 'duplicate-b'):
                historical.objects.create(event_type='inventory.issue.posted', aggregate_type='stockmovement',
                    aggregate_id=aggregate_id, aggregate_version=1, dedupe_key=key)
            with self.assertRaisesMessage(RuntimeError, 'data audit failed'):
                MigrationExecutor(connection).migrate(self.latest)
            self.assertEqual(historical.objects.filter(aggregate_id=aggregate_id).count(), 2)
        finally:
            # Explicit test-fixture cleanup; the migration never repairs or deletes.
            if historical is not None:
                historical.objects.filter(dedupe_key__in=('duplicate-a', 'duplicate-b')).delete()
            MigrationExecutor(connection).migrate(self.latest)


@skipUnless(connection.vendor == 'postgresql', 'PostgreSQL retained-content triggers')
class PostgreSQLImmutabilityTests(TestCase):
    def outbox(self, *, hashed=True):
        event = valid_event()
        event['event_id'] = str(uuid.uuid4())
        return OutboxEvent.objects.create(id=event['event_id'],
            event_type=event['event_type'], aggregate_type=event['aggregate_type'],
            aggregate_id=event['aggregate_id'], aggregate_version=event['aggregate_version'],
            schema_version=event['schema_version'], created_at=datetime.fromisoformat(event['occurred_at']),
            payload_json=event['payload'], payload_hash=canonical_payload_hash(event) if hashed else None,
            dedupe_key=str(uuid.uuid4()))

    def test_outbox_identity_and_content_mutation_rejected_but_delivery_state_allowed(self):
        row = self.outbox()
        for field, value in (
            ('id', uuid.uuid4()), ('event_type', 'inventory.opening.posted'), ('schema_version', 2),
            ('aggregate_type', 'batch'), ('aggregate_id', uuid.uuid4()), ('aggregate_version', 2),
            ('created_at', timezone.now()), ('payload_json', {'changed': True}),
            ('payload_hash', 'a' * 64), ('payload_hash', None), ('payload_hash', ''),
        ):
            with self.subTest(field=field), self.assertRaises(DatabaseError), transaction.atomic():
                OutboxEvent.objects.filter(pk=row.pk).update(**{field: value})
        OutboxEvent.objects.filter(pk=row.pk).update(status='PROCESSING', transport='kafka',
            lease_token=uuid.uuid4(), locked_until=timezone.now() + timedelta(seconds=60))
        row.refresh_from_db()
        self.assertEqual(row.status, 'PROCESSING')
        with self.assertRaises(DatabaseError), transaction.atomic(), connection.cursor() as cursor:
            cursor.execute('UPDATE labops_outboxevent SET payload_hash = %s WHERE id = %s', ['b' * 64, row.pk])

    def test_failed_delivery_retains_source_and_content_but_allows_retry_updates(self):
        poison = {'poison': True}
        row = FailedDelivery.objects.create(consumer_name='analytics', delivery_key='topic:0:1',
            source_cluster='cluster', source_generation='generation', envelope=poison,
            original_hash=canonical_payload_hash(poison))
        for field, value in (
            ('id', uuid.uuid4()), ('consumer_name', 'notification'), ('source_cluster', 'other-cluster'),
            ('source_generation', 'other-generation'), ('delivery_key', 'topic:0:2'),
            ('created_at', timezone.now()), ('envelope', {'poison': False}), ('original_hash', 'a' * 64),
        ):
            with self.subTest(field=field), self.assertRaises(DatabaseError), transaction.atomic():
                FailedDelivery.objects.filter(pk=row.pk).update(**{field: value})
        FailedDelivery.objects.filter(pk=row.pk).update(status='DEAD', attempts=2,
            last_error='permanent schema failure', dlq_lease_token=uuid.uuid4(),
            dlq_locked_until=timezone.now() + timedelta(seconds=60))
        row.refresh_from_db()
        self.assertEqual(row.attempts, 2)
        with self.assertRaises(DatabaseError), transaction.atomic(), connection.cursor() as cursor:
            cursor.execute('UPDATE labops_faileddelivery SET envelope = %s::jsonb WHERE id = %s',
                           [json.dumps({'changed': True}), row.pk])

    def test_processed_markers_reject_identity_mutations(self):
        row = ProcessedEvent.objects.create(consumer_name='analytics', event_id=uuid.uuid4(), payload_hash='a' * 64)
        for field, value in (('id', uuid.uuid4()), ('consumer_name', 'notification'),
                             ('event_id', uuid.uuid4()), ('created_at', timezone.now()), ('payload_hash', 'b' * 64)):
            with self.subTest(field=field), self.assertRaises(DatabaseError), transaction.atomic():
                ProcessedEvent.objects.filter(pk=row.pk).update(**{field: value})

    def test_unassigned_legacy_hashes_can_be_filled_once(self):
        outbox = self.outbox(hashed=False)
        OutboxEvent.objects.filter(pk=outbox.pk).update(payload_hash='a' * 64)
        failed = FailedDelivery.objects.create(consumer_name='analytics', delivery_key='topic:0:1')
        FailedDelivery.objects.filter(pk=failed.pk).update(original_hash=canonical_payload_hash({}))
        processed = ProcessedEvent.objects.create(consumer_name='analytics', event_id=uuid.uuid4())
        ProcessedEvent.objects.filter(pk=processed.pk).update(payload_hash='b' * 64)
        outbox.refresh_from_db(); failed.refresh_from_db(); processed.refresh_from_db()
        self.assertEqual(outbox.payload_hash, 'a' * 64)
        self.assertEqual(failed.original_hash, canonical_payload_hash({}))
        self.assertEqual(processed.payload_hash, 'b' * 64)

    def test_opaque_json_numeric_checksum_survives_jsonb_round_trip(self):
        value = {'nested': [1e20, 1e30, -0.0, 1.5, 1e-20]}
        digest = canonical_payload_hash(value)
        row = FailedDelivery.objects.create(consumer_name='analytics', delivery_key='numeric-poison:0:1',
            envelope=value, original_hash=digest, failure_class='permanent', status='DEAD')
        row.refresh_from_db()
        self.assertIs(type(row.envelope['nested'][0]), int)
        self.assertIs(type(row.envelope['nested'][1]), int)
        self.assertEqual(canonical_payload_hash(row.envelope), digest)

    def test_raw_sql_audit_changes_rejected(self):
        row = DeliveryAudit.objects.create(actor_label='test', action='REPLAY', outcome='DRY_RUN')
        with self.assertRaises(DatabaseError), transaction.atomic(), connection.cursor() as cursor:
            cursor.execute('UPDATE labops_deliveryaudit SET reason = %s WHERE id = %s', ['changed', row.pk])
        with self.assertRaises(DatabaseError), transaction.atomic(), connection.cursor() as cursor:
            cursor.execute('DELETE FROM labops_deliveryaudit WHERE id = %s', [row.pk])
