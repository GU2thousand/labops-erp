"""At-least-once transport, atomic per-consumer database effects, durable retries."""
import json
import uuid
from datetime import timedelta
from decimal import Decimal
from django.conf import settings
from django.db import transaction, connection
from django.db.models import Exists, OuterRef, Q
from django.utils import timezone
from .locking import advisory
from .telemetry import tracer, traced_consumer
from opentelemetry.propagate import inject, extract
from opentelemetry import trace
from .models import OutboxEvent, ProcessedEvent, InventoryProjection, FailedDelivery, Notification, User


def emit_inventory(movement):
    from .operations.services import stock_recipients
    carrier = {}; inject(carrier)
    payload = {
        '_trace_context': carrier,
        'movement_id': str(movement.id), 'movement_type': movement.type,
        'title': f'{movement.type.title()} posted', 'body': movement.movement_no,
        'recipients': [str(x) for x in stock_recipients()],
        'lines': [{'batch_id': str(x.batch_id), 'warehouse_id': str(x.warehouse_id),
                   'delta_qty': str(x.delta_qty), 'unit_cost': str(x.unit_cost)}
                  for x in movement.lines.order_by('line_no')],
    }
    return OutboxEvent.objects.get_or_create(dedupe_key=f'inventory:{movement.id}', defaults={
        'event_type': f'inventory.{movement.type.lower()}.posted', 'aggregate_type': 'stockmovement',
        'aggregate_id': movement.id, 'aggregate_version': movement.version,
        'payload_json': payload, 'transport': settings.EVENT_TRANSPORT,
    })[0]


def envelope(event):
    return {'event_id': str(event.id), 'event_type': event.event_type,
            'aggregate_type': event.aggregate_type, 'aggregate_id': str(event.aggregate_id),
            'aggregate_version': event.aggregate_version, 'schema_version': event.schema_version,
            'occurred_at': event.created_at.isoformat(), 'trace_context': event.payload_json.get('_trace_context',{}), 'payload': event.payload_json}


@traced_consumer
def process_envelope(consumer, event):
    """Only effects in this PostgreSQL database are atomic with deduplication."""
    if consumer not in {'notification', 'analytics'}:
        raise ValueError('Unknown consumer')
    if event.get('schema_version') != 1 or event.get('event_type') not in {
        f'inventory.{kind}.posted' for kind in ['opening', 'receipt', 'issue', 'transfer', 'adjustment', 'reversal']
    }:
        raise ValueError('Unsupported event schema or type')
    eid = uuid.UUID(event['event_id'])
    uuid.UUID(event['aggregate_id'])
    if not isinstance(event['aggregate_version'], int) or event['aggregate_version'] < 1:
        raise ValueError('Invalid aggregate version')
    payload = event['payload']
    with transaction.atomic():
        if consumer == 'analytics': advisory('analytics-rebuild', shared=True)
        _, created = ProcessedEvent.objects.get_or_create(consumer_name=consumer, event_id=eid)
        if not created:
            return False
        if consumer == 'notification':
            for uid in payload['recipients']:
                if User.objects.filter(pk=uid, is_active=True).exists():
                    Notification.objects.get_or_create(event_id=eid, user_id=uid,
                        defaults={'title': payload['title'], 'body': payload['body']})
        else:
            # Deltas commute; retries may arrive after later events. This projection
            # is eventually consistent and never authorizes inventory writes.
            deltas = {}
            for line in payload['lines']:
                key = (str(uuid.UUID(line['batch_id'])), str(uuid.UUID(line['warehouse_id'])))
                delta = Decimal(line['delta_qty'])
                if not delta.is_finite(): raise ValueError('Invalid quantity')
                deltas[key] = deltas.get(key, Decimal(0)) + delta
            for (bid, wid), delta in sorted(deltas.items()):
                advisory(f'projection:{bid}:{wid}')
                projection, _ = InventoryProjection.objects.get_or_create(batch_id=bid, warehouse_id=wid)
                projection = InventoryProjection.objects.select_for_update().get(pk=projection.pk)
                projection.quantity += delta
                projection.save(update_fields=['quantity'])
        # Django PostgreSQL FKs are initially deferred. Surface violations in
        # this savepoint so the retry owner can persist attempts rather than
        # repeatedly failing only at its outer commit.
        connection.check_constraints()
        return True


def claim_event():
    now = timezone.now()
    # A dead/leased earlier version blocks later versions of that aggregate.
    earlier = OutboxEvent.objects.filter(transport='kafka', aggregate_type=OuterRef('aggregate_type'),
        aggregate_id=OuterRef('aggregate_id'), aggregate_version__lt=OuterRef('aggregate_version')).exclude(status='PUBLISHED')
    with transaction.atomic():
        event = (OutboxEvent.objects.select_for_update(skip_locked=True).filter(transport='kafka')
            .filter(Q(status='PENDING', next_attempt_at__lte=now) | Q(status='PROCESSING', locked_until__lt=now))
            .annotate(blocked=Exists(earlier)).filter(blocked=False).order_by('created_at', 'id').first())
        if not event: return None
        event.status = 'PROCESSING'
        event.lease_token = uuid.uuid4()
        event.locked_until = now + timedelta(seconds=60)
        event.save(update_fields=['status', 'lease_token', 'locked_until'])
        return event


def producer():
    from confluent_kafka import Producer
    return Producer({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
        'enable.idempotence': True, 'acks': 'all', 'delivery.timeout.ms': 10000,
        'request.timeout.ms': 5000})


def send(producer, topic, key, value):
    results = []
    with tracer.start_as_current_span('outbox.publish',context=extract(value.get('trace_context',{})),kind=trace.SpanKind.PRODUCER):
        producer.produce(topic, key=key, value=json.dumps(value).encode(), on_delivery=lambda err, msg: results.append(err))
        remaining = producer.flush(12)
    if remaining or not results or results[0] is not None:
        raise RuntimeError('Broker did not acknowledge delivery')


def publish_one(producer, after_send=None):
    event = claim_event()
    if event is None: return False
    try:
        send(producer, settings.KAFKA_TOPIC, f'{event.aggregate_type}:{event.aggregate_id}', envelope(event))
        if after_send: after_send()  # fault injection: acknowledgement before DB mark
        OutboxEvent.objects.filter(pk=event.pk, lease_token=event.lease_token).update(
            status='PUBLISHED', published_at=timezone.now(), locked_until=None, lease_token=None, last_error='')
    except Exception as exc:
        attempts = event.attempts + 1
        retry = settings.EVENT_RETRY_SECONDS
        OutboxEvent.objects.filter(pk=event.pk, lease_token=event.lease_token).update(
            status='DEAD' if attempts > len(retry) else 'PENDING', attempts=attempts,
            next_attempt_at=timezone.now()+timedelta(seconds=retry[min(attempts-1, len(retry)-1)]),
            locked_until=None, lease_token=None, last_error=str(exc)[:1000])
        raise
    return True


def deliver(consumer, event, delivery_key):
    try:
        process_envelope(consumer, event)
    except Exception as exc:
        # Persist before acknowledging the broker; if this write fails the offset
        # must remain uncommitted and Kafka will redeliver.
        FailedDelivery.objects.get_or_create(consumer_name=consumer, delivery_key=delivery_key,
            defaults={'envelope': event, 'last_error': str(exc)[:1000],
                      'next_attempt_at': timezone.now()+timedelta(seconds=settings.EVENT_RETRY_SECONDS[0])})
        return False
    return True


def retry_deliveries(limit=100):
    count = 0
    for _ in range(limit):
        with transaction.atomic():
            row = FailedDelivery.objects.select_for_update(skip_locked=True).filter(
                status='RETRY', next_attempt_at__lte=timezone.now()).order_by('next_attempt_at', 'id').first()
            if row is None: break
            try:
                process_envelope(row.consumer_name, row.envelope)
                row.status = 'RESOLVED'; row.resolved_at = timezone.now()
            except Exception as exc:
                row.attempts += 1; row.last_error = str(exc)[:1000]
                delays = settings.EVENT_RETRY_SECONDS
                if row.attempts > len(delays): row.status = 'DEAD'
                else: row.next_attempt_at = timezone.now()+timedelta(seconds=delays[row.attempts-1])
            row.save(); count += 1
    return count


def publish_dlq(producer):
    # Duplicate DLQ publication after a crash is expected. Stable delivery_id is
    # retained; replays use the original event_id, never a newly minted one.
    for row in FailedDelivery.objects.filter(status='DEAD', dlq_published_at__isnull=True)[:100]:
        send(producer, settings.KAFKA_DLQ_TOPIC, str(row.id), {
            'delivery_id': str(row.id), 'consumer_name': row.consumer_name,
            'event': row.envelope, 'attempts': row.attempts, 'error': row.last_error})
        FailedDelivery.objects.filter(pk=row.pk, status='DEAD').update(dlq_published_at=timezone.now())
