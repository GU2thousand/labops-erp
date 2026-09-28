"""At-least-once transport, immutable contracts and atomic database effects.

A database lease fences database writes, never a stale Kafka producer. Duplicate
broker records retain their event ID and are harmless to this database's effects.
"""
import json
import base64
import logging
import inspect
from pathlib import Path
import sqlite3
import time
import uuid
from datetime import timedelta
from decimal import Decimal
from django.conf import settings
from django.db import transaction, connection, connections, router, DEFAULT_DB_ALIAS, DatabaseError, IntegrityError, OperationalError
from django.db import models
from django.db.models import Exists, OuterRef, Q
from django.db.models.base import ModelBase
from django.db.models.manager import Manager
from django.db.models.query import QuerySet, ModelIterable
from django.db.models.signals import pre_init, post_init, pre_save, post_save
from django.db.models import base as model_base, manager as model_manager, query as model_query, fields as model_fields
from django.db.models.query_utils import DeferredAttribute
from django.utils.functional import cached_property
from django.db.backends.postgresql import base as postgres_base, operations as postgres_operations, compiler as postgres_compiler
from django.db.backends.base import operations as base_operations
from django.db.models.sql import compiler as sql_compiler, subqueries as sql_subqueries, query as sql_query
from django.db.models.expressions import RawSQL
from django.utils import timezone
from .locking import advisory
from .telemetry import tracer, traced_consumer
from opentelemetry.propagate import inject, extract
from opentelemetry import trace
from .models import OutboxEvent, ProcessedEvent, InventoryProjection, FailedDelivery, Notification, User, DeliveryAudit, StockMovement
from .event_schema import validate_inventory_envelope, canonical_payload_hash, EventValidationError
from .kafka_config import producer_config, source_identity
from .worker_metrics import EVENTS, PUBLISH_ACK, EFFECT_LATENCY, LEASE_REJECTIONS, schema_rejected

log = logging.getLogger('labops')

# References are capabilities, not a cached eligibility decision. Every marker
# operation rechecks the effective model/manager/fields/listeners below.
_MARKER_MODEL_METHODS = {name: inspect.getattr_static(models.Model, name) for name in (
    '__new__', '__init__', '__getattribute__', '__setattr__', 'from_db', 'save', 'save_base',
    '_prepare_related_fields_for_save', '_save_parents', '_validate_force_insert', '_save_table', '_do_insert', '_is_pk_set',
    '_get_pk_val')}
_MARKER_MANAGER_METHODS = {name: inspect.getattr_static(Manager, name) for name in (
    'get_queryset', 'get_or_create', 'get', 'create', '_insert')}
_MARKER_QUERY_METHODS = {name: inspect.getattr_static(QuerySet, name) for name in (
    '__init__', 'get_or_create', 'get', 'create', '_extract_model_params',
    '_insert', 'filter', '_clone', 'db')}
_MARKER_FIELD_METHODS = {kind: {name: inspect.getattr_static(kind, name) for name in (
    'deconstruct', 'get_default', 'pre_save', 'get_db_prep_save',
    'get_db_prep_value', 'get_prep_value', 'get_pk_value_on_save',
    'to_python', 'value_from_object', '_get_default')}
    for kind in (models.UUIDField, models.DateTimeField, models.CharField)}
_MARKER_DEFAULTS = (uuid.uuid4, timezone.now)
_MARKER_SOURCE_FILES = {module.__name__: str(Path(module.__file__).resolve()) for module in (
    model_base, model_manager, model_query, model_fields, uuid, timezone,
    postgres_base, postgres_operations, postgres_compiler, base_operations,
    sql_compiler, sql_subqueries, sql_query)}
_MARKER_INSERT_METHODS = tuple((kind, name, module, inspect.getattr_static(kind, name)) for kind, names, module in (
    (sql_subqueries.InsertQuery, ('__init__', 'insert_values'), 'django.db.models.sql.subqueries'),
    (sql_subqueries.InsertQuery, ('get_compiler',), 'django.db.models.sql.query'),
    (postgres_compiler.SQLInsertCompiler, ('as_sql', 'execute_sql', 'prepare_value', 'pre_save_val', 'field_as_sql'), 'django.db.models.sql.compiler'),
    (postgres_compiler.SQLInsertCompiler, ('assemble_as_sql',), 'django.db.backends.postgresql.compiler'),
    (postgres_operations.DatabaseOperations, ('compiler', 'insert_statement'), 'django.db.backends.base.operations'),
    (postgres_operations.DatabaseOperations, ('quote_name',), 'django.db.backends.postgresql.operations'),
) for name in names)


def _marker_callable(descriptor):
    if isinstance(descriptor, (classmethod, staticmethod)):
        return descriptor.__func__
    if isinstance(descriptor, property):
        return descriptor.fget
    if isinstance(descriptor, cached_property):
        return descriptor.func
    return descriptor


_MARKER_CALLABLE_STATES = {id(descriptor): (descriptor, _marker_callable(descriptor).__code__,
        _marker_callable(descriptor).__module__, _marker_callable(descriptor).__qualname__)
    for descriptor in (*_MARKER_MODEL_METHODS.values(), *_MARKER_MANAGER_METHODS.values(),
        *_MARKER_QUERY_METHODS.values(), *(method for methods in _MARKER_FIELD_METHODS.values() for method in methods.values()),
        *_MARKER_DEFAULTS, *(method for _, _, _, method in _MARKER_INSERT_METHODS))
    if hasattr(_marker_callable(descriptor), '__code__')}


def _marker_original_callable(descriptor, module, filename_module, name):
    state = _MARKER_CALLABLE_STATES.get(id(descriptor))
    function = _marker_callable(descriptor)
    code = getattr(function, '__code__', None)
    return (state is not None and state[0] is descriptor and code is state[1]
            and function.__module__ == state[2] == module and function.__qualname__ == state[3]
            and function.__qualname__.split('.')[-1] == name
            and code.co_filename == _MARKER_SOURCE_FILES[filename_module])


def _marker_framework_original():
    # A wrapper installed before events imports must not become a "plain"
    # reference merely because it is the effective function at import time.
    for name, method in _MARKER_MODEL_METHODS.items():
        if name in {'__new__', '__getattribute__', '__setattr__'}:
            if method is not inspect.getattr_static(object, name):
                return False
        elif not _marker_original_callable(method, 'django.db.models.base', 'django.db.models.base', name):
            return False
    for name, method in _MARKER_MANAGER_METHODS.items():
        module = 'django.db.models.manager' if name == 'get_queryset' else 'django.db.models.query'
        if not _marker_original_callable(method, module, 'django.db.models.manager', name):
            return False
    if any(not _marker_original_callable(method, 'django.db.models.query', 'django.db.models.query', name)
           for name, method in _MARKER_QUERY_METHODS.items()):
        return False
    if any(not _marker_original_callable(method, 'django.db.models.fields', 'django.db.models.fields', name)
           for methods in _MARKER_FIELD_METHODS.values() for name, method in methods.items()):
        return False
    if any(inspect.getattr_static(kind, name) is not method
           or not _marker_original_callable(method, module, module, name)
           for kind, name, module, method in _MARKER_INSERT_METHODS):
        return False
    return (_marker_original_callable(_MARKER_DEFAULTS[0], 'uuid', 'uuid', 'uuid4')
            and _marker_original_callable(_MARKER_DEFAULTS[1], 'django.utils.timezone', 'django.utils.timezone', 'now'))


def _plain_processed_marker(manager):
    """Unknown extensions retain the complete ordinary ORM lifecycle."""
    try:
        if (connection.vendor != 'postgresql' or connection.alias != DEFAULT_DB_ALIAS
                or settings.DATABASE_ROUTERS or router.routers):
            return False
        if not _marker_framework_original():
            return False
        database = connections[DEFAULT_DB_ALIAS]
        if (connection.settings_dict['ENGINE'] != 'django.db.backends.postgresql'
                or type(database) is not postgres_base.DatabaseWrapper
                or type(connection.ops) is not postgres_operations.DatabaseOperations
                or connection.ops.compiler('SQLInsertCompiler') is not postgres_compiler.SQLInsertCompiler):
            return False
        options = database.settings_dict['OPTIONS']
        if 'isolation_level' in options or 'options' in options:
            return False
        if database.connection is not None and (
                database.isolation_level != postgres_base.IsolationLevel.READ_COMMITTED
                or database.connection.isolation_level not in (None, postgres_base.IsolationLevel.READ_COMMITTED)):
            return False
        if type(ProcessedEvent) is not ModelBase or any(
                inspect.getattr_static(ProcessedEvent, name) is not method
                for name, method in _MARKER_MODEL_METHODS.items()):
            return False
        if inspect.getattr_static(ModelBase, '__call__') is not inspect.getattr_static(type, '__call__'):
            return False
        meta = ProcessedEvent._meta
        if (meta.db_table != 'labops_processedevent' or meta.proxy or meta.parents or meta.swapped
                or meta.concrete_model is not ProcessedEvent or meta.unique_together):
            return False
        if (type(manager) is not Manager or manager.model is not ProcessedEvent
                or manager._db is not None or manager._hints
                or manager._queryset_class is not QuerySet
                or any(inspect.getattr_static(manager, name) is not method
                       for name, method in _MARKER_MANAGER_METHODS.items())):
            return False
        for other in (ProcessedEvent._base_manager, ProcessedEvent._default_manager):
            if (type(other) is not Manager or other.model is not ProcessedEvent
                    or other._db is not None or other._hints or other._queryset_class is not QuerySet
                    or any(inspect.getattr_static(other, name) is not method
                           for name, method in _MARKER_MANAGER_METHODS.items())):
                return False
        if any(inspect.getattr_static(QuerySet, name) is not method
               for name, method in _MARKER_QUERY_METHODS.items()):
            return False
        queryset = manager.get_queryset()
        if (type(queryset) is not QuerySet or queryset.model is not ProcessedEvent
                or queryset._db is not None or queryset._hints or queryset._for_write
                or queryset._iterable_class is not ModelIterable):
            return False
        expected = (
            ('id', models.UUIDField, {'primary_key': True, 'default': _MARKER_DEFAULTS[0], 'editable': False, 'serialize': False}),
            ('created_at', models.DateTimeField, {'default': _MARKER_DEFAULTS[1], 'db_index': True}),
            ('consumer_name', models.CharField, {'max_length': 64}),
            ('event_id', models.UUIDField, {}),
            ('payload_hash', models.CharField, {'max_length': 64, 'null': True, 'blank': True}),
        )
        fields = tuple(meta.local_concrete_fields)
        if tuple(meta.concrete_fields) != fields or len(fields) != len(expected):
            return False
        for field, (name, kind, options) in zip(fields, expected):
            if (type(field) is not kind or field.name != name or field.attname != name
                    or field.column != name or field.model is not ProcessedEvent
                    or field.generated or field.db_returning or hasattr(field, 'get_placeholder')
                    or any(inspect.getattr_static(type(field) if method_name == '_get_default' else field, method_name) is not method
                           for method_name, method in _MARKER_FIELD_METHODS[kind].items())):
                return False
            if field.deconstruct()[3] != options:
                return False
            descriptor = inspect.getattr_static(ProcessedEvent, name)
            if type(descriptor) is not DeferredAttribute or descriptor.field is not field:
                return False
            # Field caches the callable when get_default is first used.
            if '_get_default' in field.__dict__:
                default = {'id': _MARKER_DEFAULTS[0], 'created_at': _MARKER_DEFAULTS[1],
                           'consumer_name': str, 'event_id': model_fields.return_None,
                           'payload_hash': model_fields.return_None}[name]
                if field.__dict__['_get_default'] is not default:
                    return False
        if uuid.uuid4 is not _MARKER_DEFAULTS[0] or timezone.now is not _MARKER_DEFAULTS[1]:
            return False
        constraints = meta.constraints
        if meta.db_returning_fields:
            return False
        if len(constraints) != 1 or type(constraints[0]) is not models.UniqueConstraint:
            return False
        constraint = constraints[0]
        if (constraint.name != 'consumer_event_unique' or constraint.fields != ('consumer_name', 'event_id')
                or constraint.condition is not None or constraint.deferrable is not None
                or constraint.expressions or constraint.include or constraint.opclasses
                or constraint.nulls_distinct is not None):
            return False
        return not any(signal.has_listeners(ProcessedEvent) for signal in (pre_init, post_init, pre_save, post_save))
    except Exception:
        # Admission failure cannot replace the business operation/error. Deadlines
        # derive from BaseException and continue to leave the worker boundary.
        return False


def _processed_marker(consumer, event_id, digest):
    manager = ProcessedEvent.objects
    lookup = {'consumer_name': consumer, 'event_id': event_id}
    if (type(consumer) is not str or consumer not in {'notification', 'analytics'}
            or type(event_id) is not uuid.UUID or type(digest) is not str
            or len(digest) != 64 or any(character not in '0123456789abcdef' for character in digest)
            or not _plain_processed_marker(manager)):
        return manager.get_or_create(**lookup, defaults={'payload_hash': digest})
    try:
        # Keep Django's insertion savepoint so ANY IntegrityError can recover
        # through a fresh target lookup on a usable transaction.
        with transaction.atomic():
            marker = ProcessedEvent(**lookup, payload_hash=digest)
            fields = ProcessedEvent._meta.local_concrete_fields
            values = [field.get_db_prep_save(field.pre_save(marker, True), connection) for field in fields]
            quote = connection.ops.quote_name
            columns = ', '.join(quote(field.column) for field in fields)
            with connection.cursor() as cursor:
                cursor.execute(f'INSERT INTO {quote(ProcessedEvent._meta.db_table)} ({columns}) '
                    f'VALUES (%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT {quote("consumer_event_unique")} '
                    f'DO NOTHING RETURNING {quote("id")}', values)
                created = cursor.fetchone() is not None
            if created:
                marker._state.db = DEFAULT_DB_ALIAS
                marker._state.adding = False
                return marker, True
    except IntegrityError:
        try:
            return manager.get(**lookup), False
        except ProcessedEvent.DoesNotExist:
            pass
        raise
    # Read Committed may have waited for a row outside the INSERT snapshot.
    # A separate statement observes that committed arbiter row.
    try:
        return manager.get(**lookup), False
    except ProcessedEvent.DoesNotExist as exc:
        # DO NOTHING produces no SQL exception to preserve. A concurrently
        # deleted target still fails permanently, like ORM insert recovery.
        raise IntegrityError('Processed marker disappeared after unique conflict') from exc


class PayloadConflict(EventValidationError):
    def __init__(self):
        super().__init__('Event identity already has different immutable content', 'payload_conflict')


class LeaseLost(RuntimeError):
    pass


class BrokerDeliveryError(RuntimeError):
    def __init__(self, code, failure_class='transient', kafka_error_code=None):
        # Only the numeric protocol/local code crosses this diagnostic boundary.
        # Broker text may include payloads, endpoints or authentication details.
        self.kafka_error_code = kafka_error_code if type(kafka_error_code) is int else None
        message = code if self.kafka_error_code is None else f'{code}:kafka_code={self.kafka_error_code}'
        super().__init__(message)
        self.code, self.failure_class = code, failure_class


def broker_error(error):
    from confluent_kafka import KafkaError
    actual = error.code() if error is not None and hasattr(error, 'code') else None
    auth = {getattr(KafkaError, name, None) for name in (
        'TOPIC_AUTHORIZATION_FAILED', 'GROUP_AUTHORIZATION_FAILED', 'CLUSTER_AUTHORIZATION_FAILED',
        'SASL_AUTHENTICATION_FAILED', '_AUTHENTICATION', '_SSL')}
    if actual is not None and actual in auth:
        return BrokerDeliveryError('broker_authorization_failed', 'authorization', actual)
    if actual == getattr(KafkaError, 'MSG_SIZE_TOO_LARGE', None):
        return BrokerDeliveryError('broker_message_size', 'permanent', actual)
    return BrokerDeliveryError('broker_delivery_failed', kafka_error_code=actual)



def raw_envelope(event):
    return {'event_id': str(event.id), 'event_type': event.event_type,
            'aggregate_type': event.aggregate_type, 'aggregate_id': str(event.aggregate_id),
            'aggregate_version': event.aggregate_version, 'schema_version': event.schema_version,
            'occurred_at': event.created_at.isoformat(),
            'trace_context': event.payload_json.get('_trace_context', {}) if isinstance(event.payload_json, dict) else {},
            'payload': event.payload_json}


def emit_inventory(movement):
    from .operations.services import stock_recipients
    carrier = {}; inject(carrier)
    lines = movement.prefetched_lines if hasattr(movement, 'prefetched_lines') else movement.lines.order_by('line_no')
    payload = {
        '_trace_context': carrier,
        'movement_id': str(movement.id), 'movement_type': movement.type,
        'title': f'{movement.type.title()} posted', 'body': movement.movement_no,
        'recipients': [str(x) for x in stock_recipients()],
        'lines': [{'batch_id': str(x.batch_id), 'warehouse_id': str(x.warehouse_id),
                   'delta_qty': str(x.delta_qty), 'unit_cost': str(x.unit_cost)}
                  for x in lines],
    }
    event, created = OutboxEvent.objects.get_or_create(dedupe_key=f'inventory:{movement.id}', defaults={
        'event_type': f'inventory.{movement.type.lower()}.posted', 'aggregate_type': 'stockmovement',
        'aggregate_id': movement.id, 'aggregate_version': movement.version,
        'payload_json': payload, 'transport': settings.EVENT_TRANSPORT,
    })
    envelope(event)  # schema validation and immutable checksum are in the inventory transaction
    return event


def envelope(event):
    value = raw_envelope(event)
    validate_inventory_envelope(value, max_bytes=settings.EVENT_MAX_PAYLOAD_BYTES)
    digest = canonical_payload_hash(value)
    if event.payload_hash and event.payload_hash != digest:
        raise PayloadConflict()
    if not event.payload_hash:
        # Legacy rows receive their checksum once; migration backfills known rows.
        changed = OutboxEvent.objects.filter(pk=event.pk).filter(Q(payload_hash='') | Q(payload_hash__isnull=True)).update(payload_hash=digest)
        if not changed:
            current = OutboxEvent.objects.get(pk=event.pk).payload_hash
            if current != digest: raise PayloadConflict()
        event.payload_hash = digest
    return value


def _notification_unique_conflict(error):
    """Only the event/user insertion race permits the existing-row fallback."""
    cause = error.__cause__
    if connection.vendor == 'postgresql':
        return getattr(getattr(cause, 'diag', None), 'constraint_name', None) == 'notification_unique'
    if connection.vendor == 'sqlite':
        return (getattr(cause, 'sqlite_errorcode', None) == sqlite3.SQLITE_CONSTRAINT_UNIQUE
                and str(cause) == 'UNIQUE constraint failed: labops_notification.event_id, labops_notification.user_id')
    return False


def _notify_inventory(event_id, payload):
    # Eligibility is one event-local active-user query snapshot. These rows are
    # not user authorization for inventory commands, which retain their locks.
    recipients = [uuid.UUID(value) for value in payload['recipients']]
    active = set(User.objects.filter(pk__in=recipients, is_active=True).values_list('id', flat=True))
    if not active:
        return
    existing = set(Notification.objects.filter(event_id=event_id, user_id__in=active)
                   .values_list('user_id', flat=True))
    missing = [uid for uid in recipients if uid in active and uid not in existing]
    if missing:
        try:
            with transaction.atomic():
                Notification.objects.bulk_create([
                    Notification(event_id=event_id, user_id=uid, title=payload['title'], body=payload['body'])
                    for uid in missing])
        except IntegrityError as exc:
            if not _notification_unique_conflict(exc):
                raise
            for uid in missing:
                Notification.objects.get_or_create(event_id=event_id, user_id=uid,
                    defaults={'title': payload['title'], 'body': payload['body']})
    completed = set(Notification.objects.filter(event_id=event_id, user_id__in=active)
                    .values_list('user_id', flat=True))
    if completed != active:
        raise IntegrityError('Notification recipients did not complete')


@traced_consumer
def process_envelope(consumer, event):
    """Only effects in this PostgreSQL database are atomic with deduplication."""
    if consumer not in {'notification', 'analytics'}:
        raise ValueError('Unknown consumer')
    validate_inventory_envelope(event, max_bytes=settings.EVENT_MAX_PAYLOAD_BYTES)
    digest = canonical_payload_hash(event)
    eid = uuid.UUID(event['event_id'])
    payload = event['payload']
    with transaction.atomic():
        if consumer == 'analytics': advisory('analytics-rebuild', shared=True)
        # Inventory events do not contain a complete business command. Unknown
        # IDs, missing master data and records beyond restored ledger watermarks
        # must never invent an inventory truth from retained Kafka deltas.
        original = OutboxEvent.objects.filter(pk=eid, event_type__startswith='inventory.').first()
        if original is None or not StockMovement.objects.filter(pk=event['aggregate_id'], status='POSTED').exists():
            raise EventValidationError('Original posted movement and inventory outbox are required', 'missing_business_event')
        envelope(original)  # verifies/backfills the immutable original checksum
        if original.payload_hash != digest:
            raise PayloadConflict()
        marker, created = _processed_marker(consumer, eid, digest)
        if not created:
            if marker.payload_hash and marker.payload_hash != digest: raise PayloadConflict()
            if not marker.payload_hash:
                marker.payload_hash = digest; marker.save(update_fields=['payload_hash'])
            transaction.on_commit(lambda: EVENTS.labels(consumer, 'duplicate').inc())
            return False
        if consumer == 'notification':
            _notify_inventory(eid, payload)
        else:
            # Deltas commute; retries may arrive after later events. Projection
            # is eventually consistent and never authorizes inventory writes.
            deltas = {}
            for line in payload['lines']:
                key = (str(uuid.UUID(line['batch_id'])), str(uuid.UUID(line['warehouse_id'])))
                delta = Decimal(line['delta_qty'])
                deltas[key] = deltas.get(key, Decimal(0)) + delta
            for (bid, wid), delta in sorted(deltas.items()):
                advisory(f'projection:{bid}:{wid}')
                projection, _ = InventoryProjection.objects.select_for_update().get_or_create(batch_id=bid, warehouse_id=wid)
                projection.quantity += delta
                projection.save(update_fields=['quantity'])
        connection.check_constraints()
        committed_at = original.created_at
        def observed():
            EVENTS.labels(consumer, 'success').inc()
            EFFECT_LATENCY.labels(consumer).observe(max(0, (timezone.now()-committed_at).total_seconds()))
        transaction.on_commit(observed)
        return True


def claim_event(*, shard_index=0, shard_count=1):
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError('Invalid publisher shard')
    now = timezone.now()
    earlier = OutboxEvent.objects.filter(transport='kafka', aggregate_type=OuterRef('aggregate_type'),
        aggregate_id=OuterRef('aggregate_id'), aggregate_version__lt=OuterRef('aggregate_version')).exclude(status='PUBLISHED')
    with transaction.atomic():
        events = (OutboxEvent.objects.select_for_update(skip_locked=True).filter(transport='kafka')
            .filter(Q(status='PENDING', next_attempt_at__lte=now) | Q(status='PROCESSING', locked_until__lt=now))
            .alias(blocked=Exists(earlier)).filter(blocked=False).order_by('created_at', 'id'))
        # Stable shard = first 32 UUID bits modulo count. PostgreSQL filters
        # before locking, so shards do not acquire one another's candidates.
        if shard_count > 1 and connection.vendor == 'postgresql':
            events = events.annotate(publisher_shard=RawSQL(
                "(('x' || substr(replace(aggregate_id::text, '-', ''), 1, 8))::bit(32)::bigint %% %s)",
                (shard_count,))).filter(publisher_shard=shard_index)
            event = events.first()
        elif shard_count > 1:
            # SQLite demo has no concurrent publisher guarantee.
            event = next((row for row in events.iterator(chunk_size=100)
                          if int(row.aggregate_id.hex[:8], 16) % shard_count == shard_index), None)
        else: event = events.first()
        if event is None: return None
        if event.status == 'PROCESSING': EVENTS.labels('publisher', 'lease_expired').inc()
        event.status = 'PROCESSING'; event.lease_token = uuid.uuid4()
        event.locked_until = now + timedelta(seconds=settings.EVENT_LEASE_SECONDS)
        event.save(update_fields=['status', 'lease_token', 'locked_until'])
        return event


def producer(role='publisher'):
    from confluent_kafka import Producer
    # Transport callbacks expose only bounded internal categories. Authentication
    # failures can otherwise surface on delivery as a generic message timeout.
    class Client:
        last_security_error = None
        def error(self, error):
            classified = broker_error(error)
            if classified.failure_class == 'authorization': self.last_security_error = classified
        def __getattr__(self, name): return getattr(self.client, name)
    wrapped = Client()
    config = producer_config(role); config['error_cb'] = wrapped.error
    wrapped.client = Producer(config)
    return wrapped


def send(producer, topic, key, value):
    results = []; started = time.monotonic()
    if hasattr(producer, 'last_security_error'): producer.last_security_error = None
    worker = 'dlq' if topic == settings.KAFKA_DLQ_TOPIC else 'publisher'
    encoded = json.dumps(value, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
    limit = settings.KAFKA_DLQ_MESSAGE_MAX_BYTES if worker == 'dlq' else settings.KAFKA_MESSAGE_MAX_BYTES
    if len(encoded) > limit:
        raise EventValidationError('Broker message byte limit exceeded', 'size')
    with tracer.start_as_current_span('outbox.publish', context=extract(value.get('trace_context', {})), kind=trace.SpanKind.PRODUCER):
        deadline = time.monotonic() + settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS
        while True:
            try:
                producer.produce(topic, key=key, value=encoded, on_delivery=lambda err, msg: results.append(err))
                break
            except BufferError:
                if time.monotonic() >= deadline: raise RuntimeError('Producer queue budget exceeded')
                producer.poll(min(.1, max(0, deadline-time.monotonic())))
        remaining = producer.flush(settings.KAFKA_PUBLISH_FLUSH_SECONDS)
    if remaining or not results or results[0] is not None:
        # Error details may contain credentials or plaintext payloads. Client
        # security settings and broker-side reason remain in access-controlled diagnostics.
        EVENTS.labels(worker, 'failure').inc()
        security = getattr(producer, 'last_security_error', None)
        if security: raise security
        raise broker_error(results[0] if results else None)
    PUBLISH_ACK.labels(worker).observe(time.monotonic()-started)


def owned_event(event):
    required = settings.KAFKA_PUBLISH_FLUSH_SECONDS + settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS + settings.EVENT_PUBLISH_DB_BUDGET_SECONDS
    return OutboxEvent.objects.filter(pk=event.pk, status='PROCESSING', lease_token=event.lease_token,
                                     locked_until__gt=timezone.now()+timedelta(seconds=required)).exists()


def publish_one(producer, after_send=None, *, shard_index=0, shard_count=1, ownership_check=None):
    event = claim_event(shard_index=shard_index, shard_count=shard_count)
    if event is None: return False
    try:
        value = envelope(event)
        if ownership_check: ownership_check()
        if not owned_event(event): raise LeaseLost('Publish owner expired before send')
        send(producer, settings.KAFKA_TOPIC, f'{event.aggregate_type}:{event.aggregate_id}', value)
        if after_send: after_send()
        if ownership_check: ownership_check()
        changed = OutboxEvent.objects.filter(pk=event.pk, status='PROCESSING', lease_token=event.lease_token,
                                            locked_until__gt=timezone.now()).update(
            status='PUBLISHED', published_at=timezone.now(), locked_until=None, lease_token=None, last_error='')
        if changed != 1:
            LEASE_REJECTIONS.labels('publisher').inc()
            log.warning('publisher_stale_write_rejected', extra={'event_id': str(event.id)})
            raise LeaseLost('Expired publish owner cannot mark delivery')
        EVENTS.labels('publisher', 'success').inc()
    except Exception as exc:
        from .publisher_shards import ShardOwnershipLost
        if isinstance(exc, ShardOwnershipLost) or (isinstance(exc, DatabaseError) and not isinstance(exc, IntegrityError)):
            # Preserve an ambiguous claim until expiry. Owner loss exits and
            # purges without a final flush; an application DB failure discards
            # that connection and rechecks the dedicated owner before reuse.
            if isinstance(exc, ShardOwnershipLost): LEASE_REJECTIONS.labels('publisher').inc()
            raise
        attempts = event.attempts + 1; retry = settings.EVENT_RETRY_SECONDS
        failure_class = classify_failure(exc)
        changed = OutboxEvent.objects.filter(pk=event.pk, status='PROCESSING', lease_token=event.lease_token).update(
            status='DEAD' if failure_class != 'transient' or attempts > len(retry) else 'PENDING', attempts=attempts,
            next_attempt_at=timezone.now()+timedelta(seconds=retry_delay(attempts, str(event.id))),
            locked_until=None, lease_token=None, last_error=safe_error(exc))
        if not changed and not isinstance(exc, LeaseLost): LEASE_REJECTIONS.labels('publisher').inc()
        raise
    return True


def classify_failure(exc):
    if isinstance(exc, BrokerDeliveryError): return exc.failure_class
    if isinstance(exc, (EventValidationError, IntegrityError, KeyError, TypeError, ValueError)): return 'permanent'
    if isinstance(exc, PermissionError): return 'authorization'
    return 'transient'


def safe_error(exc):
    # A fixed code is safe for durable operator-visible diagnostics; original
    # payload stays permission-restricted in FailedDelivery, never log labels.
    return str(exc) if isinstance(exc, BrokerDeliveryError) else getattr(exc, 'code', None) or type(exc).__name__


def retry_delay(attempts, identity):
    delays = settings.EVENT_RETRY_SECONDS
    base = delays[min(max(0, attempts-1), len(delays)-1)]
    # Deterministic bounded jitter gives reproducible drills without synchronized storms.
    fraction = int(uuid.uuid5(uuid.NAMESPACE_OID, f'{identity}:{attempts}').hex[:8], 16) / 0xffffffff
    return base * (1 + settings.EVENT_RETRY_JITTER * fraction)


def record_audit(row, action, before, *, actor_label='system', reason='', outcome='success', authorization=None):
    return DeliveryAudit.objects.create(delivery=row, actor_label=actor_label, action=action, outcome=outcome,
        reason=reason, before_json=before, after_json={'status': row.status, 'attempts': row.attempts},
        original_hash=row.original_hash, authorization_json=authorization or {'kind': 'service', 'scope': action})


def retained_evidence(event):
    """JSONB cannot store NUL strings; retain canonical original bytes losslessly."""
    if not isinstance(event, dict): return {'invalid_payload': event}
    stack = [event]
    while stack:
        node = stack.pop()
        if isinstance(node, str) and '\x00' in node:
            from .event_schema import canonical_json_bytes
            original = canonical_json_bytes(event)
            return {'invalid_payload_base64': base64.b64encode(original).decode(),
                    'evidence_encoding': 'canonical-json-with-nul'}
        if isinstance(node, dict): stack.extend(node.keys()); stack.extend(node.values())
        elif isinstance(node, list): stack.extend(node)
    return event


def deliver(consumer, event, delivery_key, *, source_cluster=None, source_generation=None):
    try:
        process_envelope(consumer, event)
    except Exception as exc:
        if isinstance(exc, EventValidationError): schema_rejected(exc.code)
        cluster, generation = source_identity()
        failure_class = classify_failure(exc)
        retained = retained_evidence(event)
        digest = canonical_payload_hash(retained)
        # This transaction is the offset acknowledgement boundary. A PostgreSQL
        # outage rolls it back and propagates; the consumer must not commit.
        with transaction.atomic():
            row, created = FailedDelivery.objects.get_or_create(consumer_name=consumer, delivery_key=delivery_key,
                source_cluster=source_cluster or cluster, source_generation=str(source_generation or generation),
                defaults={'envelope': retained, 'original_hash': digest,
                    'failure_class': failure_class, 'status': 'RETRY' if failure_class == 'transient' else 'DEAD',
                    'last_error': safe_error(exc),
                    'next_attempt_at': timezone.now()+timedelta(seconds=retry_delay(1, delivery_key))})
            if not created and row.original_hash and row.original_hash != digest:
                # A reused source generation is an operator error. Retain the
                # original row and a visible DEAD conflict with stable hash suffix.
                conflict, conflict_created = FailedDelivery.objects.get_or_create(
                    consumer_name=consumer, source_cluster=source_cluster or cluster,
                    source_generation=str(source_generation or generation),
                    delivery_key=f'{delivery_key}:conflict:{digest}',
                    defaults={'envelope': retained, 'original_hash': digest, 'failure_class': 'permanent',
                              'status': 'DEAD', 'last_error': 'source_coordinate_conflict'})
                if conflict_created:
                    DeliveryAudit.objects.create(delivery=conflict, actor_label='system', action='SOURCE_CONFLICT',
                        outcome='quarantined', reason='Source coordinate content changed',
                        before_json={'original_delivery': str(row.id), 'original_hash': row.original_hash},
                        after_json={'source_coordinates': delivery_key, 'incoming_hash': digest}, original_hash=digest,
                        authorization_json={'kind': 'service', 'scope': 'consumer.quarantine'})
                transaction.on_commit(lambda: EVENTS.labels(consumer, 'dead').inc())
            if created: record_audit(row, 'PARK', {}, reason=safe_error(exc), outcome='parked')
        outcome = 'parked' if failure_class == 'transient' else 'dead'
        transaction.on_commit(lambda: EVENTS.labels(consumer, outcome).inc())
        return False
    return True


def set_retry_timeouts():
    if connection.vendor == 'postgresql':
        with connection.cursor() as cursor:
            cursor.execute("SELECT set_config('lock_timeout', %s, true)", [f'{settings.EVENT_RETRY_LOCK_TIMEOUT_MS}ms'])
            cursor.execute("SELECT set_config('statement_timeout', %s, true)", [f'{settings.EVENT_RETRY_STATEMENT_TIMEOUT_MS}ms'])


def retry_deliveries(limit=100):
    count = 0
    for _ in range(limit):
        with transaction.atomic():
            set_retry_timeouts()
            row = FailedDelivery.objects.select_for_update(skip_locked=True).filter(
                status='RETRY', next_attempt_at__lte=timezone.now()).order_by('next_attempt_at', 'id').first()
            if row is None: break
            before = {'status': row.status, 'attempts': row.attempts}
            row.lease_token = uuid.uuid4(); row.locked_until = timezone.now()+timedelta(seconds=settings.EVENT_LEASE_SECONDS)
            try:
                if row.original_hash and canonical_payload_hash(row.envelope) != row.original_hash: raise PayloadConflict()
                process_envelope(row.consumer_name, row.envelope)
                row.status = 'RESOLVED'; row.resolved_at = timezone.now(); outcome = 'success'
            except Exception as exc:
                if isinstance(exc, DatabaseError) and not isinstance(exc, IntegrityError):
                    # Roll back this retry transaction and discard the failed
                    # application connection in the command's outer boundary.
                    raise
                row.attempts += 1; row.last_error = safe_error(exc); row.failure_class = classify_failure(exc)
                if row.failure_class != 'transient' or row.attempts > len(settings.EVENT_RETRY_SECONDS): row.status = 'DEAD'
                else: row.next_attempt_at = timezone.now()+timedelta(seconds=retry_delay(row.attempts, str(row.id)))
                outcome = 'dead' if row.status == 'DEAD' else 'failure'
            row.lease_token = None; row.locked_until = None
            row.save(); record_audit(row, 'RETRY', before, reason=row.last_error)
            transaction.on_commit(lambda outcome=outcome: EVENTS.labels('retry', outcome).inc())
            count += 1
    return count


def claim_dlq():
    now = timezone.now()
    with transaction.atomic():
        row = FailedDelivery.objects.select_for_update(skip_locked=True).filter(
            status='DEAD', dlq_published_at__isnull=True, dlq_next_attempt_at__lte=now).filter(
            Q(dlq_lease_token__isnull=True) | Q(dlq_locked_until__lt=now)).order_by('created_at', 'id').first()
        if row is None: return None
        row.dlq_lease_token = uuid.uuid4(); row.dlq_locked_until = now+timedelta(seconds=settings.EVENT_LEASE_SECONDS)
        row.save(update_fields=['dlq_lease_token', 'dlq_locked_until'])
        return row


def publish_dlq(producer, limit=100):
    # DLQ is a mirror. PostgreSQL failure rows and append-only audit are truth.
    # Ack then crash can produce duplicates bearing the same stable delivery ID.
    count = 0
    for _ in range(limit):
        row = claim_dlq()
        if row is None: break
        try:
            value = {'schema_version': 1, 'delivery_id': str(row.id), 'consumer_name': row.consumer_name,
                'source_cluster': row.source_cluster, 'source_generation': row.source_generation,
                'delivery_key': row.delivery_key, 'original_hash': row.original_hash,
                'event': row.envelope, 'attempts': row.attempts, 'error': row.last_error, 'failure_class': row.failure_class}
            if not FailedDelivery.objects.filter(pk=row.pk, dlq_lease_token=row.dlq_lease_token,
                    dlq_locked_until__gt=timezone.now()+timedelta(seconds=settings.KAFKA_PUBLISH_FLUSH_SECONDS)).exists():
                raise LeaseLost('DLQ owner expired before send')
            send(producer, settings.KAFKA_DLQ_TOPIC, str(row.id), value)
            with transaction.atomic():
                locked = FailedDelivery.objects.select_for_update().get(pk=row.pk)
                if locked.dlq_lease_token != row.dlq_lease_token or locked.dlq_locked_until <= timezone.now():
                    LEASE_REJECTIONS.labels('dlq').inc(); raise LeaseLost('Expired DLQ owner cannot mark delivery')
                locked.dlq_published_at = timezone.now(); locked.dlq_lease_token = None; locked.dlq_locked_until = None
                locked.save(update_fields=['dlq_published_at', 'dlq_lease_token', 'dlq_locked_until'])
                record_audit(locked, 'PUBLISH_DLQ', {'status': 'DEAD'}, reason='Acknowledged stable delivery ID')
            EVENTS.labels('dlq', 'success').inc(); count += 1
        except DatabaseError:
            # A post-ack DB failure is ambiguous. Leave the durable claim until
            # natural expiry; a replacement mirror retains its delivery ID.
            raise
        except Exception as exc:
            FailedDelivery.objects.filter(pk=row.pk, dlq_lease_token=row.dlq_lease_token).update(
                dlq_lease_token=None, dlq_locked_until=None, dlq_attempts=row.dlq_attempts+1,
                dlq_next_attempt_at=timezone.now()+timedelta(seconds=retry_delay(row.dlq_attempts+1, str(row.id))))
            EVENTS.labels('dlq', 'failure').inc()
            log.warning('dlq_delivery_failed', extra={'delivery_id': str(row.id), 'error_code': safe_error(exc)})
            # One poison DLQ row never blocks subsequent eligible rows.
    return count
