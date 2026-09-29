"""One explicit native-scoped diagnostic subprocess; no acceptance/profiler."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from types import FunctionType

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()
from django.core.management import call_command
from labops import events, publisher_observation as observation
from labops.management.commands import publish_events as command


def run_publisher(output, options, _current=observation.current,
        _code=observation.current.__code__, _defaults=observation.current.__defaults__,
        _globals=observation.current.__globals__, _state=observation.__dict__,
        _kind=observation.NativePublisherObservation, _type=type, _dict=dict,
        _function=FunctionType, _getprofile=sys.getprofile, _gettrace=sys.gettrace, _policy=command._BUDGET_ADMISSION, _send=events.send):
    # Defaults hold definition-time native helper references, not admission data.
    if (_type(_current) is not _function or _current.__code__ is not _code
            or _current.__defaults__ is not _defaults or _current.__globals__ is not _globals
            or _current.__kwdefaults__ is not None or _current.__dict__
            or _dict.get(_state, 'current') is not _current
            or _current(_current, bootstrap=True) is not _kind
            or command._BUDGET_ADMISSION is not _policy or events.send is not _send
            or _getprofile() is not None or _gettrace() is not None):
        raise RuntimeError('NativeObservationCapabilityOrProfilerRefused')
    owned = _kind()
    owned.bind(_policy, _send)
    _dict.__setitem__(_state, 'ACTIVE', owned)
    primary = None
    try:
        if _current(_current) is not owned:
            raise RuntimeError('NativeObservationCapabilityRefused')
        return call_command('publish_events', **options)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            intact = (_type(_current) is _function and _current.__code__ is _code
                and _current.__defaults__ is _defaults and _current.__globals__ is _globals
                and _current.__kwdefaults__ is None and not _current.__dict__
                and _dict.get(_state, 'current') is _current)
            valid = intact and _current(_current) is owned
            result = owned.close() if valid else {'mode': 'native-scoped', 'complete': False,
                'status': 'OBSERVED_PARTIAL', 'qualification_admissible': False,
                'reason': 'observation_capability_or_owner_changed'}
            if _dict.get(_state, 'ACTIVE') is owned:
                _dict.__setitem__(_state, 'ACTIVE', None)
            sources = ('labops/publisher_observation.py', 'labops/events.py',
                'labops/management/commands/publish_events.py', 'labops/worker_metrics.py',
                'benchmarks/events/native_publisher.py')
            value = {'schema_version': 1, 'observation_only': True, 'diagnostic_only': True,
                'qualification_admissible': False, 'capacity_accepted': False,
                'function_profile_requested': False, 'function_graph_status': 'NOT_REQUESTED',
                'publisher_observation': result,
                'source_sha256': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources}}
            with Path(output).open('x') as stream:
                json.dump(value, stream, indent=2, allow_nan=False)
                stream.write('\n')
        except BaseException:
            # Secondary diagnostics cannot replace the original control/error.
            if primary is None:
                raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--metrics-port', type=int, default=None)
    args = parser.parse_args()
    if not args.output.name.startswith('publisher-profile-') or args.output.suffix != '.json':
        parser.error('Output must be an authored publisher-profile JSON filename')
    run_publisher(args.output, {'loop': True, 'limit': 500, 'metrics_port': args.metrics_port})


if __name__ == '__main__':
    main()
