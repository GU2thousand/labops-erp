"""Inventory v1 contract and immutable, whole-envelope SHA256 identity.

Canonical JSON uses sorted keys, compact separators and UTF-8 with no NaN/Infinity.
Checksums also normalize integral JSON floats and signed numeric zero to their
decimal integer value, matching PostgreSQL JSONB numeric semantics. Every field,
including both trace carriers, participates. Retrying retains the original event;
changing any value requires a new event. This is not RFC 8785/JCS.
"""
import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

MAX_EVENT_BYTES = 256 * 1024
MAX_LINES = 1000
MAX_RECIPIENTS = 1000
MAX_VERSION = 2_147_483_647
INVENTORY_KINDS = ('opening', 'receipt', 'issue', 'transfer', 'adjustment', 'reversal')
INVENTORY_EVENT_TYPES = frozenset(f'inventory.{kind}.posted' for kind in INVENTORY_KINDS)
UUID_PATTERN = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
FIXED6_PATTERN = r'^-?(?:0|[1-9][0-9]{0,11})(?:\.[0-9]{1,6})?$'
UTC_PATTERN = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$'


class EventValidationError(ValueError):
    def __init__(self, message, code='schema'):
        super().__init__(message)
        self.code = code


def canonical_json_bytes(value):
    """Finite JSON only; usable for primitive/raw poison evidence as well as events."""
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                          allow_nan=False).encode('utf-8')
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise EventValidationError('Envelope must be finite, UTF-8 JSON', 'json') from exc


def canonical_payload_hash(value):
    """Hash complete JSON content with JSONB-stable numeric semantics.

    Normalize only for hashing; ingress still validates original Python types.
    Decimal(str(float)) preserves the JSON numeric value of shortest rendering,
    unlike int(float), which exposes binary approximation for values such as 1e30.
    """
    def normalize(item):
        if type(item) is float and math.isfinite(item):
            number = Decimal(str(item))
            return int(number) if number == number.to_integral_value() else item
        if type(item) is dict:
            return {key: normalize(child) for key, child in item.items()}
        if type(item) is list:
            return [normalize(child) for child in item]
        return item
    try:
        normalized = normalize(value)
    except RecursionError as exc:
        raise EventValidationError('Envelope must be finite, acyclic JSON', 'json') from exc
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _fail(message, code='schema'):
    raise EventValidationError(message, code)


def _object(value, name, required, optional=()):
    if type(value) is not dict:
        _fail(f'{name} must be an object')
    if not all(type(key) is str for key in value):
        _fail(f'{name} keys must be strings')
    missing = set(required) - value.keys()
    if missing:
        _fail(f'{name} is missing {", ".join(sorted(missing))}')
    unknown = value.keys() - set(required) - set(optional)
    if unknown:
        _fail(f'{name} has unsupported fields: {", ".join(sorted(unknown))}')


def _string(value, name, maximum, minimum=0):
    if type(value) is not str or not minimum <= len(value) <= maximum:
        _fail(f'{name} must be a string of {minimum}..{maximum} characters')
    if '\x00' in value:
        _fail(f'{name} must not contain NUL')


def _uuid(value, name):
    _string(value, name, 36, 36)
    if not re.fullmatch(UUID_PATTERN, value):
        _fail(f'{name} must be a UUID string')
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise EventValidationError(f'{name} must be a UUID string') from exc


def _version(value, name):
    # bool is deliberately rejected despite being an int subclass in Python.
    if type(value) is not int or not 1 <= value <= MAX_VERSION:
        _fail(f'{name} must be an integer in 1..{MAX_VERSION}')


def _fixed6(value, name, *, nonnegative=False, nonzero=False):
    if type(value) is not str or not re.fullmatch(FIXED6_PATTERN, value):
        _fail(f'{name} must be a plain Fixed6 decimal string')
    number = Decimal(value)
    if not number.is_finite() or abs(number * 1_000_000) >= 10**18:
        _fail(f'{name} exceeds NUMERIC(18,6)')
    if nonnegative and number < 0:
        _fail(f'{name} must be nonnegative')
    if nonzero and number == 0:
        _fail(f'{name} must be nonzero')
    return number


def _trace_context(value, name):
    if type(value) is not dict or len(value) > 32:
        _fail(f'{name} must be a map with at most 32 string entries')
    for key, item in value.items():
        _string(key, f'{name} key', 64, 1)
        _string(item, f'{name}.{key}', 2048)


def validate_inventory_envelope(event, *, max_bytes=MAX_EVENT_BYTES):
    """Validate before creating any dedupe marker/effect; return the same object."""
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError('max_bytes must be a positive integer')
    encoded = canonical_json_bytes(event)
    if len(encoded) > max_bytes:
        _fail(f'Envelope exceeds {max_bytes} UTF-8 bytes', 'size')
    _object(event, 'envelope', ('event_id', 'event_type', 'aggregate_type', 'aggregate_id',
                               'aggregate_version', 'schema_version', 'occurred_at', 'payload'),
            ('trace_context',))
    _uuid(event['event_id'], 'event_id')
    aggregate_id = _uuid(event['aggregate_id'], 'aggregate_id')
    _version(event['schema_version'], 'schema_version')
    if event['schema_version'] != 1:
        _fail('Unsupported inventory schema_version', 'schema_version')
    _version(event['aggregate_version'], 'aggregate_version')
    _string(event['event_type'], 'event_type', 64, 1)
    if event['event_type'] not in INVENTORY_EVENT_TYPES:
        _fail('Unsupported inventory event_type', 'event_type')
    if event['aggregate_type'] != 'stockmovement':
        _fail('aggregate_type must be stockmovement')
    occurred = event['occurred_at']
    if type(occurred) is not str or not re.fullmatch(UTC_PATTERN, occurred):
        _fail('occurred_at must be an ISO-8601 UTC timestamp')
    try:
        parsed = datetime.fromisoformat(occurred.replace('Z', '+00:00'))
    except ValueError as exc:
        raise EventValidationError('occurred_at is not a valid timestamp') from exc
    if parsed.utcoffset() != timedelta(0):
        _fail('occurred_at must use UTC')
    if 'trace_context' in event:
        _trace_context(event['trace_context'], 'trace_context')
    payload = event['payload']
    _object(payload, 'payload', ('movement_id', 'movement_type', 'title', 'body', 'recipients', 'lines'),
            ('_trace_context',))
    if _uuid(payload['movement_id'], 'payload.movement_id') != aggregate_id:
        _fail('movement_id must match aggregate_id')
    if payload['movement_type'] != event['event_type'].split('.')[1].upper():
        _fail('movement_type must match event_type')
    _string(payload['title'], 'payload.title', 160, 1)
    _string(payload['body'], 'payload.body', 4096)
    recipients = payload['recipients']
    if type(recipients) is not list or len(recipients) > MAX_RECIPIENTS:
        _fail(f'recipients must be a list of at most {MAX_RECIPIENTS} UUIDs')
    recipient_ids = [_uuid(value, 'recipient') for value in recipients]
    if len(set(recipient_ids)) != len(recipient_ids):
        _fail('recipients must not contain duplicate UUIDs')
    lines = payload['lines']
    if type(lines) is not list or not 1 <= len(lines) <= MAX_LINES:
        _fail(f'lines must have 1..{MAX_LINES} entries')
    for index, line in enumerate(lines):
        name = f'payload.lines[{index}]'
        _object(line, name, ('batch_id', 'warehouse_id', 'delta_qty', 'unit_cost'))
        _uuid(line['batch_id'], f'{name}.batch_id')
        _uuid(line['warehouse_id'], f'{name}.warehouse_id')
        _fixed6(line['delta_qty'], f'{name}.delta_qty', nonzero=True)
        _fixed6(line['unit_cost'], f'{name}.unit_cost', nonnegative=True)
    if '_trace_context' in payload:
        _trace_context(payload['_trace_context'], 'payload._trace_context')
    return event
