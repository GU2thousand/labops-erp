"""Closed acceptance writer requests; no application or runtime dependencies."""
from collections.abc import Mapping
import json
import os
from pathlib import Path


WRITER_PRESETS = ('writers-4', 'writers-6')
DEFAULT_WRITER_PRESET = 'writers-4'
WRITER_TOPOLOGY_VERSION = 'writer-topology-v1'
_LANES = {'writers-4': 4, 'writers-6': 6}
_FIELDS = ('writer_topology', 'writer_topology_version')


def writer_profile(preset=DEFAULT_WRITER_PRESET, *, diagnostic_profile=False):
    if type(preset) is not str or preset not in WRITER_PRESETS or type(diagnostic_profile) is not bool:
        raise ValueError('Invalid writer topology request')
    if diagnostic_profile and preset != DEFAULT_WRITER_PRESET:
        raise ValueError('Nondefault writer topology requires function profiling OFF')
    lanes = _LANES[preset]
    return {'version': WRITER_TOPOLOGY_VERSION, 'preset': preset, 'lanes': lanes,
        'cycle_length': 4, 'queue_capacity': 4, 'result_capacity': 16,
        'assignment': f'(global_index//4)%{lanes}', 'position': 'global_index%4',
        'selection': 'explicit before setup; no runtime adaptation'}


def resolve_profile_writer(value):
    """Validate paired metadata; a missing pair is the original four writers.

    This never adds fields to a legacy profile. Numeric journals and retained
    origin plans therefore retain their original serialized shape and hashes.
    """
    present = [name in value if isinstance(value, Mapping) else hasattr(value, name)
               for name in _FIELDS]
    if any(present) and not all(present):
        raise ValueError('Writer topology requires both selector and version')
    if not any(present):
        return DEFAULT_WRITER_PRESET
    get = value.__getitem__ if isinstance(value, Mapping) else lambda name: getattr(value, name)
    preset, version = (get(name) for name in _FIELDS)
    if type(version) is not str or version != WRITER_TOPOLOGY_VERSION:
        raise ValueError('Invalid writer topology version')
    if isinstance(value, Mapping):
        profiling = value.get('diagnostic_profile_enabled', value.get('diagnostic_profile', False))
    else:
        profiling = getattr(value, 'diagnostic_profile_enabled', getattr(value, 'diagnostic_profile', False))
    writer_profile(preset, diagnostic_profile=profiling)
    return preset


def generator_roles(preset=DEFAULT_WRITER_PRESET):
    return tuple(f'generator-{lane}' for lane in range(writer_profile(preset)['lanes']))


def freeze_writer_topology(path, run_id, preset=DEFAULT_WRITER_PRESET, *, diagnostic_profile=False):
    profile = writer_profile(preset, diagnostic_profile=diagnostic_profile)
    value = {'run_id': run_id, **profile}
    path = Path(path)
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise ValueError('Frozen writer topology changed')
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x') as output:
            json.dump(value, output, sort_keys=True, allow_nan=False)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
    return profile


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preset', choices=WRITER_PRESETS, default=DEFAULT_WRITER_PRESET)
    parser.add_argument('--diagnostic-profile', choices=('true', 'false'), default='false')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        freeze_writer_topology(args.output, args.run_id, args.preset,
                               diagnostic_profile=args.diagnostic_profile == 'true')
    except ValueError as error:
        parser.error(str(error))
