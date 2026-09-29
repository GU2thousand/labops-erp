"""Predeclared validation pools; production consumer semantics remain unchanged."""
from copy import deepcopy
import json
from pathlib import Path
import re


if __package__:
    from .writer_topology import DEFAULT_WRITER_PRESET, WRITER_PRESETS, writer_profile
else:
    from writer_topology import DEFAULT_WRITER_PRESET, WRITER_PRESETS, writer_profile


PRESETS = ('single', 'notification-dual')
DEFAULT_PRESET = 'single'


def topology_profile(preset=DEFAULT_PRESET, *, diagnostic_profile=False,
                     writer_topology=DEFAULT_WRITER_PRESET):
    if preset not in PRESETS or type(diagnostic_profile) is not bool:
        raise ValueError('Invalid consumer topology request')
    if diagnostic_profile and preset != DEFAULT_PRESET:
        raise ValueError('Nondefault consumer topology requires function profiling OFF')
    writers = writer_profile(writer_topology, diagnostic_profile=diagnostic_profile)
    return {'version': 'consumer-topology-v1', 'preset': preset, 'writer_lanes': writers['lanes'],
        'publisher_members': 1, 'notification_members': 2 if preset == 'notification-dual' else 1,
        'analytics_members': 1, 'inventory_partitions': 3,
        'ack_policy': 'original per-record durable database transaction then immediate synchronous offset commit',
        'selection': 'explicit before setup; no runtime adaptation'}


def consumer_roles(name, preset=DEFAULT_PRESET):
    profile = topology_profile(preset)
    if name not in ('notification', 'analytics'):
        raise ValueError('Unknown logical consumer')
    return tuple(name if slot == 0 else name + '-' + str(slot)
                 for slot in range(profile[name + '_members']))


def worker_roles(preset=DEFAULT_PRESET):
    return ('publisher', *consumer_roles('notification', preset), 'analytics', 'retry', 'dlq')


def freeze_topology(path, run_id, preset=DEFAULT_PRESET, *, diagnostic_profile=False,
                    writer_topology=DEFAULT_WRITER_PRESET):
    profile = topology_profile(preset, diagnostic_profile=diagnostic_profile, writer_topology=writer_topology)
    value = {'run_id': run_id, **profile}
    path = Path(path)
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise ValueError('Frozen consumer topology changed')
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x') as output:
            json.dump(value, output, sort_keys=True)
            output.write('\n')
    return profile


def assignment_result(description, *, topic, client_ids, stable_state):
    """Reject malformed/foreign membership and any incomplete partition union."""
    expected = tuple(client_ids)
    if (not 1 <= len(expected) <= 3
            or any(type(value) is not str or re.fullmatch(r'acceptance-[1-9][0-9]*', value) is None
                   for value in expected) or len(set(expected)) != len(expected)):
        raise ValueError('Expected distinct owned client IDs')
    if getattr(description, 'state', None) != stable_state:
        return False
    members = getattr(description, 'members', None)
    if not isinstance(members, (list, tuple)) or len(members) != len(expected):
        return False
    seen_clients, coordinates, rows = set(), set(), []
    for member in members:
        client_id = getattr(member, 'client_id', None)
        if type(client_id) is not str or client_id not in expected or client_id in seen_clients:
            return False
        seen_clients.add(client_id)
        parts = getattr(getattr(member, 'assignment', None), 'topic_partitions', None)
        if not isinstance(parts, (list, tuple)) or not parts:
            return False
        assignments = []
        for part in parts:
            number = getattr(part, 'partition', None)
            coordinate = (getattr(part, 'topic', None), number)
            if (type(number) is not int or number not in range(3) or coordinate[0] != topic
                    or coordinate in coordinates):
                return False
            coordinates.add(coordinate)
            assignments.append({'topic': topic, 'partition': number})
        rows.append({'client_id': client_id, 'assignments': assignments})
    if coordinates != {(topic, number) for number in range(3)}:
        return False
    result = {'state': str(description.state), 'member_count': len(rows),
              'client_ids': list(expected), 'members': sorted(rows, key=lambda row: row['client_id'])}
    if len(rows) == 1:
        result.update(client_id=rows[0]['client_id'], assignments=deepcopy(rows[0]['assignments']))
    return result


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preset', choices=PRESETS, default=DEFAULT_PRESET)
    parser.add_argument('--writer-topology', choices=WRITER_PRESETS, default=DEFAULT_WRITER_PRESET)
    parser.add_argument('--diagnostic-profile', choices=('true', 'false'), default='false')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        freeze_topology(args.output, args.run_id, args.preset,
                        diagnostic_profile=args.diagnostic_profile == 'true', writer_topology=args.writer_topology)
    except ValueError as error:
        parser.error(str(error))
