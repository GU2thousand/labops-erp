"""Owned validation bootstrap; the real production management command is intact."""
from contextlib import contextmanager
from functools import wraps
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def install_publisher_hooks(profile):
    from labops import events, worker_metrics
    from labops.management.commands import publish_events as command
    from labops.publisher_shards import PublisherShardOwner
    if profile.profiler is None:
        return
    try:
        if (command.publish_one is not events.publish_one
                or command.database_statement_budget is not worker_metrics.database_statement_budget):
            raise RuntimeError('UnexpectedPublisherAlias')
        expected = [(command, 'publish_one', 'publish_one_composite', events.publish_one),
            (events, 'claim_event', 'claim_commit_composite', events.claim_event),
            (events, 'envelope', 'envelope_composite', events.envelope),
            (events, 'owned_event', 'lease_check', events.owned_event),
            (events, 'send', 'send_composite', events.send),
            (PublisherShardOwner, 'assert_owned', 'shard_ownership', PublisherShardOwner.assert_owned),
            (worker_metrics.StopController, 'wait', 'idle_wait', worker_metrics.StopController.wait)]
        for target, name, phase, original in expected:
            # Only these pinned repository functions may be intercepted. The
            # source hashes are exported by the sanitized function graph.
            filename = Path(original.__code__.co_filename).resolve()
            if not filename.is_relative_to(ROOT / 'labops'):
                raise RuntimeError('UnexpectedPublisherSource')
        for target, name, phase, original in expected:
            profile.hook(target, name, phase, expected=original, ordinal=name == 'publish_one')
        original = command.database_statement_budget

        @wraps(original)
        @contextmanager
        def budget(*args, **kwargs):
            manager = original(*args, **kwargs)
            with profile.phase('budget_setup_composite'):
                value = manager.__enter__()
            try:
                yield value
            except BaseException:
                info = sys.exc_info()
                with profile.phase('budget_restore_composite'):
                    suppressed = manager.__exit__(*info)
                if not suppressed:
                    raise
            else:
                with profile.phase('budget_restore_composite'):
                    manager.__exit__(None, None, None)

        command.database_statement_budget = budget
        profile.hooks.append((command, 'database_statement_budget', original, budget, True))
    except BaseException as error:
        profile.record_error('publisher_hook_admission', error)
        profile.restore()


def run_publisher(output, options, *, call_command=None, profile_factory=None):
    from benchmarks.events.diagnostic_profile import CPUProfile, _safe_name
    profile = (profile_factory or CPUProfile)('publisher')
    try:
        install_publisher_hooks(profile)
        if call_command is None:
            from django.core.management import call_command
        with profile.call('publisher_lifecycle'):
            return call_command('publish_events', **options)
    finally:
        # close contains diagnostic failures; it cannot replace the real
        # management command's first error or stop its own normal shutdown.
        coverage, close_error = None, None
        try:
            coverage = profile.close(output)
        except BaseException as error:
            close_error = error
            try:
                profile.error('publisher_close', error)
            except BaseException:
                pass  # Missing output remains incomplete in the coordinator.
        try:
            if close_error is not None or not isinstance(coverage, dict) or coverage.get('complete') is not True:
                metadata = {'kind': 'publisher_diagnostic_profile', 'status': 'INCOMPLETE'}
                if isinstance(coverage, dict):
                    metadata['coverage'] = coverage
                if close_error is not None:
                    metadata['error_type'] = _safe_name(type(close_error).__name__)
                # This bootstrap is used only for an enabled diagnostic run.
                # Emit no exception text, paths, business data or raw stats.
                print(json.dumps(metadata, sort_keys=True, allow_nan=False), file=sys.stderr, flush=True)
        except BaseException:
            pass  # A failed diagnostic log sink cannot replace business work.


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--metrics-port', type=int, default=None)
    args = parser.parse_args()
    if not args.output.name.startswith('publisher-profile-') or args.output.suffix != '.json':
        parser.error('Output must be an authored publisher-profile JSON filename')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    import django
    django.setup()
    run_publisher(args.output, {'loop': True, 'limit': 500, 'metrics_port': args.metrics_port})


if __name__ == '__main__':
    main()
