"""Qualify delivery evidence for every requested event and inventory consumer.

The caller owns JSONL streaming and artifact persistence. Repeated observations
of one Kafka coordinate never prove receipt of another published record. Only
the selected inventory topic and source identity can satisfy this contract.
"""
from collections import Counter
from collections.abc import Iterable, Mapping
import math
import re
from uuid import UUID


CONSUMERS = ('notification', 'analytics')
SOURCE_IDENTIFIER = re.compile(r'[A-Za-z0-9_.-]{1,96}\Z')
TOPIC_IDENTIFIER = re.compile(r'[A-Za-z0-9_.-]{1,249}\Z')
CANONICAL_INTEGER = re.compile(r'(?:0|[1-9][0-9]*)\Z')
MAX_OFFSET = 2 ** 63 - 1
OFFSET_INVALID = -1001


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _identifier(value, pattern, description):
    _check(isinstance(value, str) and bool(pattern.fullmatch(value)),
           'Missing or malformed ' + description)
    return value


def _event_id(value):
    _check(isinstance(value, str), 'Expected event IDs must be canonical UUID strings')
    try:
        canonical = str(UUID(value))
    except (ValueError, AttributeError) as exc:
        raise ValueError('Expected event IDs must be canonical UUID strings') from exc
    _check(canonical == value, 'Expected event IDs must be canonical UUID strings')
    return value


def _source_coordinates(value):
    _check(isinstance(value, str), 'Missing or malformed required delivery key')
    pieces = value.split(':')
    _check(len(pieces) == 5, 'Required delivery key must identify cluster, generation, topic, partition and offset')
    cluster, generation, topic, partition, offset = pieces
    _identifier(cluster, SOURCE_IDENTIFIER, 'delivery source cluster')
    _identifier(generation, SOURCE_IDENTIFIER, 'delivery source generation')
    _identifier(topic, TOPIC_IDENTIFIER, 'delivery topic')
    _check(bool(CANONICAL_INTEGER.fullmatch(partition)) and len(partition) <= 10,
           'Required delivery partition must be a canonical nonnegative integer')
    _check(bool(CANONICAL_INTEGER.fullmatch(offset)) and len(offset) <= 19,
           'Required delivery offset must be a canonical nonnegative integer')
    partition, offset = int(partition), int(offset)
    _check(partition <= 2 ** 31 - 1 and offset <= MAX_OFFSET,
           'Required delivery coordinates exceed Kafka integer bounds')
    return cluster, generation, topic, partition, offset


def _raw_observation(row):
    # Preserve the writer's evidence fields, never arbitrary fields from an
    # envelope or producer configuration. Optional provenance is caller-supplied.
    result = {field: row[field] for field in ('event_id', 'consumer', 'delivery_key', 'result')}
    for field in ('received_at', 'completed_at'):
        if field in row:
            value = row[field]
            finite = False
            if type(value) in (int, float):
                try:
                    finite = math.isfinite(value)
                except OverflowError:
                    pass
            _check(finite and value >= 0, 'Malformed required delivery observation timestamp')
            result[field] = value
    if 'log_file' in row:
        _check(isinstance(row['log_file'], str) and bool(row['log_file'])
               and all(character.isprintable() for character in row['log_file']),
               'Malformed required delivery log provenance')
        result['log_file'] = row['log_file']
    if 'line_number' in row:
        _check(type(row['line_number']) is int and row['line_number'] >= 1,
               'Malformed required delivery line provenance')
        result['line_number'] = row['line_number']
    return result


def _acknowledged_coordinates(required_coordinates, ids, topic, partitions):
    if required_coordinates is None:
        return {event_id: set() for event_id in ids}
    _check(isinstance(required_coordinates, Mapping)
           and set(required_coordinates) == set(ids),
           'Acknowledged coordinates must map exactly the requested event IDs')
    result = {}
    owners = {}
    for event_id in ids:
        records = required_coordinates[event_id]
        _check(isinstance(records, (tuple, list)) and bool(records),
               'Each requested event must have a nonempty list of acknowledged coordinates')
        coordinates = set()
        for record in records:
            _check(isinstance(record, Mapping) and set(record) == {'partition', 'offset'},
                   'Acknowledged coordinate must contain exactly partition and offset')
            partition, offset = record['partition'], record['offset']
            _check(type(partition) is int and partition in partitions,
                   'Acknowledged inventory partition must be integer 0, 1 or 2')
            _check(type(offset) is int and 0 <= offset <= MAX_OFFSET,
                   'Acknowledged inventory offset must be a nonnegative Kafka integer')
            coordinate = (topic, partition, offset)
            _check(coordinate not in coordinates,
                   'Duplicate acknowledged coordinate for a requested event')
            _check(coordinate not in owners,
                   'One acknowledged Kafka coordinate claims multiple event IDs')
            coordinates.add(coordinate)
            owners[coordinate] = event_id
        result[event_id] = coordinates
    return result


def _coordinate_records(coordinates):
    return [{'topic': topic, 'partition': partition, 'offset': offset}
            for topic, partition, offset in sorted(coordinates)]


def acknowledged_offsets_coverage(snapshot, required_coordinates):
    """Prove both durable group cursors have passed every new producer ACK.

    Kafka committed offsets identify the next record to read, so each relevant
    partition must be at least its highest acknowledged offset plus one. Other
    partitions need not have zero lag. Missing coverage returns ``passed=False``;
    malformed snapshot or ACK identity / coordinates raises ValueError.
    """
    _check(isinstance(required_coordinates, Mapping) and bool(required_coordinates),
           'Durable offset coverage requires nonempty acknowledged event coordinates')
    ids = tuple(_event_id(value) for value in required_coordinates)
    # This helper compares only partition / offset; the receipt contract binds
    # these same ACK records to the configured main topic and source identity.
    acknowledged = _acknowledged_coordinates(required_coordinates, ids, None, (0, 1, 2))
    maximum = {}
    for records in acknowledged.values():
        for _topic, partition, offset in records:
            maximum[partition] = max(offset, maximum.get(partition, offset))
    _check(isinstance(snapshot, Mapping) and set(snapshot) == set(CONSUMERS),
           'Committed offset snapshot must identify both inventory consumer groups exactly')
    raw_next_offsets = {}
    for consumer in CONSUMERS:
        offsets = snapshot[consumer]
        _check(isinstance(offsets, Mapping) and set(offsets) == {'0', '1', '2'},
               'Committed offset snapshot must contain exactly inventory partitions 0, 1 and 2')
        raw_next_offsets[consumer] = {}
        for partition in ('0', '1', '2'):
            offset = offsets[partition]
            _check(type(offset) is int and (offset == OFFSET_INVALID or 0 <= offset <= MAX_OFFSET),
                   'Committed next offset must be a nonnegative Kafka integer or OFFSET_INVALID')
            raw_next_offsets[consumer][partition] = offset
    checks = []
    missing = []
    for consumer in CONSUMERS:
        for partition, maximum_ack in sorted(maximum.items()):
            observed = raw_next_offsets[consumer][str(partition)]
            required = maximum_ack + 1
            check = {'consumer': consumer, 'partition': partition,
                     'maximum_acknowledged_offset': maximum_ack,
                     'required_next_offset': required, 'observed_next_offset': observed,
                     'passed': observed >= required}
            checks.append(check)
            if not check['passed']:
                missing.append(dict(check))
    return {'passed': not missing, 'consumers': list(CONSUMERS),
            'raw_next_offsets': raw_next_offsets,
            'required_max_acknowledged_offsets': {str(partition): offset
                                                for partition, offset in sorted(maximum.items())},
            'required_next_offsets': {str(partition): offset + 1
                                      for partition, offset in sorted(maximum.items())},
            'acknowledged_event_count': len(ids),
            'acknowledged_broker_record_count': sum(len(records) for records in acknowledged.values()),
            'expected_consumer_partition_checks': len(checks),
            'covered_consumer_partition_checks': len(checks) - len(missing),
            'checks': checks, 'missing': missing}


def qualify_delivery_observations(rows, event_ids, expected_topic, expected_per_pair,
                                  *, expected_source_identity=None,
                                  expected_partitions=(0, 1, 2), required_coordinates=None):
    """Return auditable per-event / consumer coverage, including shortfalls.

    Each pair requires at least ``expected_per_pair`` distinct successful Kafka
    topic / partition / offset coordinates. The expected denominator remains
    exact, and any excess is reported. Missing coverage returns ``passed=False``
    for a caller's bounded wait; malformed required evidence raises ValueError.
    Unrelated event IDs, topics and source identities cannot increase coverage.
    When ``required_coordinates`` maps IDs to producer-acknowledged partition /
    offset pairs, both consumers must observe every one of those coordinates.
    Historical at-least-once duplicates cannot substitute for the new ACKs.
    The input iterable is consumed once, without materializing unrelated rows.
    """
    _check(isinstance(rows, Iterable) and not isinstance(rows, (str, bytes, Mapping)),
           'Delivery observations must be an iterable of rows')
    _check(isinstance(event_ids, Iterable) and not isinstance(event_ids, (str, bytes, Mapping)),
           'Expected event IDs must be a collection')
    ids = tuple(_event_id(value) for value in event_ids)
    _check(bool(ids) and len(set(ids)) == len(ids),
           'Expected event IDs must be nonempty and distinct')
    wanted = set(ids)
    topic = _identifier(expected_topic, TOPIC_IDENTIFIER, 'expected inventory topic')
    _check(type(expected_per_pair) is int and expected_per_pair >= 1,
           'Expected distinct deliveries per event / consumer must be a positive integer')
    _check(isinstance(expected_partitions, (tuple, list))
           and tuple(expected_partitions) == (0, 1, 2)
           and all(type(value) is int for value in expected_partitions),
           'Expected inventory partitions must be exactly 0, 1 and 2')
    source_identity = None
    if expected_source_identity is not None:
        _check(isinstance(expected_source_identity, (tuple, list))
               and len(expected_source_identity) == 2,
               'Expected source identity must contain cluster and generation')
        source_identity = tuple(_identifier(value, SOURCE_IDENTIFIER, 'expected source identity')
                                for value in expected_source_identity)
    acknowledged = _acknowledged_coordinates(required_coordinates, ids, topic, expected_partitions)

    observed = {(event_id, consumer): {'coordinates': set(), 'observations': [],
                                     'successful_rows': 0}
                for event_id in ids for consumer in CONSUMERS}
    coordinate_owners = {}
    ignored = Counter()
    total = 0
    required_rows = 0
    for row in rows:
        total += 1
        _check(isinstance(row, Mapping), 'Delivery observation must be an object')
        event_id = row.get('event_id')
        if not isinstance(event_id, str) or event_id not in wanted:
            ignored['unrelated_event_id'] += 1
            continue
        cluster, generation, actual_topic, partition, offset = _source_coordinates(row.get('delivery_key'))
        if actual_topic != topic:
            ignored['unrelated_topic'] += 1
            continue
        if source_identity is not None and (cluster, generation) != source_identity:
            ignored['unrelated_source_identity'] += 1
            continue
        consumer = row.get('consumer')
        _check(isinstance(consumer, str) and consumer in CONSUMERS,
               'Required delivery observation has an unknown consumer identity')
        _check(partition in expected_partitions,
               'Required inventory delivery partition must be 0, 1 or 2')
        _check(type(row.get('result')) is bool,
               'Required delivery observation result must be boolean')
        pair = observed[(event_id, consumer)]
        pair['observations'].append(_raw_observation(row))
        required_rows += 1
        coordinate = (actual_topic, partition, offset)
        owner_key = (cluster, generation, coordinate)
        prior_owner = coordinate_owners.setdefault(owner_key, event_id)
        _check(prior_owner == event_id,
               'One required Kafka coordinate claims multiple event IDs')
        if row['result']:
            pair['successful_rows'] += 1
            pair['coordinates'].add(coordinate)

    pairs = []
    missing = []
    for event_id in ids:
        for consumer in CONSUMERS:
            pair = observed[(event_id, consumer)]
            count = len(pair['coordinates'])
            missing_acknowledged = acknowledged[event_id] - pair['coordinates']
            passed = count >= expected_per_pair and not missing_acknowledged
            record = {'event_id': event_id, 'consumer': consumer, 'passed': passed,
                      'expected_distinct_coordinates': expected_per_pair,
                      'distinct_coordinates': count,
                      'shortfall': max(0, expected_per_pair - count),
                      'excess_distinct_coordinates': max(0, count - expected_per_pair),
                      'raw_observation_count': len(pair['observations']),
                      'successful_observation_count': pair['successful_rows'],
                      'repeated_successful_coordinate_observations': pair['successful_rows'] - count,
                      'coordinates': _coordinate_records(pair['coordinates']),
                      'required_acknowledged_coordinates': _coordinate_records(acknowledged[event_id]),
                      'missing_acknowledged_coordinates': _coordinate_records(missing_acknowledged),
                      'expected_acknowledged_coordinates': len(acknowledged[event_id]),
                      'observed_acknowledged_coordinates': len(acknowledged[event_id]) - len(missing_acknowledged),
                      'raw_observations': pair['observations']}
            pairs.append(record)
            if not passed:
                missing.append({'event_id': event_id, 'consumer': consumer, 'shortfall': record['shortfall'],
                                'missing_acknowledged_coordinates': record['missing_acknowledged_coordinates']})
    denominator = len(ids) * len(CONSUMERS) * expected_per_pair
    qualified = sum(min(pair['distinct_coordinates'], expected_per_pair) for pair in pairs)
    return {'passed': not missing, 'topic': topic,
            'source_identity': {'cluster': source_identity[0], 'generation': source_identity[1]}
                               if source_identity is not None else None,
            'consumers': list(CONSUMERS), 'partitions': list(expected_partitions),
            'expected_event_count': len(ids), 'expected_pair_count': len(pairs),
            'expected_distinct_coordinates_per_pair': expected_per_pair,
            'expected_total_distinct_coordinates': denominator,
            'qualified_total_distinct_coordinates': qualified,
            'observed_total_distinct_coordinates': sum(pair['distinct_coordinates'] for pair in pairs),
            'requires_acknowledged_coordinates': required_coordinates is not None,
            'expected_acknowledged_coordinate_observations':
                sum(pair['expected_acknowledged_coordinates'] for pair in pairs),
            'observed_acknowledged_coordinate_observations':
                sum(pair['observed_acknowledged_coordinates'] for pair in pairs),
            'covered_pair_count': len(pairs) - len(missing),
            'total_observation_rows': total, 'required_stream_observation_rows': required_rows,
            'ignored_observation_rows': total - required_rows,
            'ignored_observation_reasons': dict(ignored), 'missing_pairs': missing, 'pairs': pairs}
