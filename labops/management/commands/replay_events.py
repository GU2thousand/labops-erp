"""Database-local replay of retained original outbox; never reset running Kafka offsets."""
import json
import time
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from labops.events import raw_envelope, envelope, process_envelope, canonical_payload_hash, safe_error
from labops.event_schema import validate_inventory_envelope
from labops.models import OutboxEvent, DeliveryAudit, ProcessedEvent


class Command(BaseCommand):
    help = 'Dry-run or audit bounded replay into one consumer, preserving event IDs and dedupe.'

    def add_arguments(self, p):
        p.add_argument('--consumer', required=True, choices=['notification', 'analytics'])
        p.add_argument('--start', required=True, help='Inclusive UTC ISO timestamp')
        p.add_argument('--end', required=True, help='Exclusive UTC ISO timestamp')
        p.add_argument('--limit', type=int, default=100)
        p.add_argument('--rate', type=float, default=10)
        p.add_argument('--event-id', action='append', default=[])
        p.add_argument('--actor', default='')
        p.add_argument('--reason', default='')
        p.add_argument('--authorization', default='')
        p.add_argument('--execute', action='store_true')
        p.add_argument('--dry-run', action='store_true', help='Default; cannot combine with --execute')

    def handle(self, *args, **options):
        import math
        if options['execute'] and options['dry_run']: raise CommandError('Choose dry-run or execute')
        if not 1 <= options['limit'] <= 100000 or not math.isfinite(options['rate']) or not 0 < options['rate'] <= 1000:
            raise CommandError('limit must be 1..100000 and rate finite 0..1000 events/s')
        try:
            start, end = [datetime.fromisoformat(options[key].replace('Z', '+00:00')) for key in ('start', 'end')]
            if any(value.utcoffset() is None or value.utcoffset().total_seconds() != 0 for value in (start, end)) or start >= end:
                raise ValueError()
        except ValueError as exc: raise CommandError('A valid increasing UTC range is required') from exc
        if options['execute'] and not all(options[name].strip() for name in ('actor', 'reason', 'authorization')):
            raise CommandError('--actor, --reason and --authorization are required for replay writes')
        rows = OutboxEvent.objects.filter(event_type__startswith='inventory.', created_at__gte=start, created_at__lt=end)
        if options['event_id']: rows = rows.filter(pk__in=options['event_id'])
        rows = rows.order_by('created_at', 'id')
        total = rows.count(); summary = {'dry_run': not options['execute'], 'consumer': options['consumer'],
            'range': {'start': start.isoformat(), 'end': end.isoformat()}, 'matched': total,
            'limit': options['limit'], 'selected': 0, 'effects': 0, 'duplicates': 0, 'failed': 0, 'results': []}
        for event in rows[:options['limit']]:
            summary['selected'] += 1
            marker = ProcessedEvent.objects.filter(consumer_name=options['consumer'], event_id=event.id).first()
            before = {'processed': marker is not None}
            try:
                value = envelope(event) if options['execute'] else raw_envelope(event)
                validate_inventory_envelope(value, max_bytes=settings.EVENT_MAX_PAYLOAD_BYTES)
                digest = canonical_payload_hash(value)
                if event.payload_hash and event.payload_hash != digest: raise ValueError('Immutable content mismatch')
                if marker and marker.payload_hash and marker.payload_hash != digest: raise ValueError('Dedupe marker content mismatch')
                if options['execute']:
                    with transaction.atomic():
                        created = process_envelope(options['consumer'], value)
                        result = 'effect' if created else 'duplicate'
                        DeliveryAudit.objects.create(outbox=event, actor_label=options['actor'], action='REPLAY',
                            outcome=result, reason=options['reason'], before_json=before,
                            after_json={'processed': True, 'consumer': options['consumer']}, original_hash=digest,
                            authorization_json={'kind': 'operator', 'reference': options['authorization'], 'range': summary['range']})
                    summary['effects' if created else 'duplicates'] += 1
                else: result = 'would_dedupe' if before['processed'] else 'would_process'
            except Exception as exc:
                result = 'failed'; summary['failed'] += 1
                if options['execute']:
                    DeliveryAudit.objects.create(outbox=event, actor_label=options['actor'], action='REPLAY',
                        outcome='failed', reason=options['reason'], before_json=before,
                        after_json={'error_code': safe_error(exc), 'consumer': options['consumer']},
                        original_hash=event.payload_hash, authorization_json={'kind': 'operator', 'reference': options['authorization']})
            summary['results'].append({'event_id': str(event.id), 'outcome': result, 'original_hash': event.payload_hash})
            if options['execute']: time.sleep(1/options['rate'])
        self.stdout.write(json.dumps(summary, default=str, indent=2))
        if summary['failed']: raise CommandError(f'{summary["failed"]} retained events could not be replayed; inspect audit')
