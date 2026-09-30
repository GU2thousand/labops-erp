"""Bounded, offline analysis of one native publisher profile.

This checks profile data only. It does not verify run/source identity, journals,
physical PostgreSQL effects, resources, or any capacity/production gate.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

STAGES = frozenset((
    'admission_call', 'claim_native_admission', 'claim_composite',
    'atomic_claim_execute', 'claim_object_construct', 'claim_physical_commit',
    'envelope', 'lease_check', 'lease_check_execute', 'send', 'delivery_ack',
    'publication_writeback', 'publication_writeback_execute',
    'application_other_execute',
))
PUBLISHED = STAGES - {'application_other_execute'}
EMPTY = frozenset(('admission_call', 'claim_native_admission', 'claim_composite',
                   'atomic_claim_execute', 'claim_physical_commit'))
LIMITATIONS = (
    'Profile structure only; run/source identity, workload, resources, journals, '
    'physical business effects and cleanup are UNVERIFIED.',
    'All phase timings are inclusive and overlap. Do not sum phases, subtract '
    'children, add marginal quantiles or infer exclusive CPU/causal savings.',
    'Selection and lease update share one atomic CTE; their server costs cannot '
    'be separated. Claim fetch/conversion outside from_db is unmeasured.',
    'delivery_ack duration measures the marker after the original callback, '
    'not broker ACK latency or callback arrival time.',
    'Clock CPU is only an instrumentation overhead lower bound. Total observer '
    'overhead and PostgreSQL/client clock calibration are UNKNOWN.',
    'Capacity/full/production qualification is always false.',
)
COUNTERS = ('attempts_seen', 'sampled_attempts', 'overflow_attempts',
            'overflow_records', 'clock_read_calls', 'clock_read_thread_cpu_ns',
            'clock_failures', 'hook_installs', 'hook_restores', 'hook_failures',
            'ack_late', 'ack_nonowner', 'ack_unavailable')
ZERO_COUNTERS = ('overflow_attempts', 'overflow_records', 'clock_failures',
                 'hook_failures', 'ack_late', 'ack_nonowner', 'ack_unavailable')


def integer(value):
    return type(value) is int and 0 <= value <= 2**63 - 1


def distribution(values):
    values = sorted(values)
    n = len(values)
    return {'n': n, 'mean': statistics.fmean(values) if n else None,
            'min': values[0] if n else None, 'max': values[-1] if n else None,
            **{name: values[math.ceil(fraction * n) - 1] if n else None
               for name, fraction in (('p50', .5), ('p95', .95), ('p99', .99))}}


def analyze(profile):
    if type(profile) is not dict or type(profile.get('publisher_observation')) is not dict:
        raise ValueError('ProfileShapeInvalid')
    observation = profile['publisher_observation']
    attempts, records = observation.get('attempts'), observation.get('records')
    if (type(attempts) is not list or type(records) is not list
            or len(attempts) > 4096 or len(records) > 131072
            or any(type(row) is not dict for row in attempts + records)):
        raise ValueError('ObservationShapeOrBoundInvalid')
    issues = []
    if not (type(profile.get('schema_version')) is int
            and type(observation.get('schema_version')) is int
            and profile.get('schema_version') == observation.get('schema_version') == 1
            and observation.get('mode') == 'native-scoped'
            and profile.get('observation_only') is True
            and profile.get('diagnostic_only') is True
            and profile.get('function_profile_requested') is False
            and profile.get('qualification_admissible') is False
            and profile.get('capacity_accepted') is False
            and observation.get('qualification_admissible') is False
            and observation.get('function_profile_requested') is False
            and observation.get('function_graph_status') == 'NOT_REQUESTED'
            and observation.get('nested_times_additive') is False):
        issues.append('DiagnosticSchemaOrFlagsInvalid')
    if observation.get('complete') is not True or observation.get('status') != 'COMPLETE_DIAGNOSTIC':
        issues.append('ObserverReportedPartial')
    amap, rmap = {}, {}
    for rows, key, index in ((attempts, 'attempt_id', amap), (records, 'id', rmap)):
        for row in rows:
            identity = row.get(key)
            if not integer(identity) or identity in index:
                issues.append('InvalidOrDuplicate_' + key)
            else:
                index[identity] = row
    grouped, timed = defaultdict(list), defaultdict(list)
    invalid_records = 0
    for row in records:
        attempt = amap.get(row.get('attempt_id')) if integer(row.get('attempt_id')) else None
        parent = rmap.get(row.get('parent_id')) if integer(row.get('parent_id')) else None
        stage = row.get('stage')
        if (not isinstance(stage, str) or stage not in STAGES or attempt is None
                or row.get('event_id') != attempt.get('event_id')
                or (row.get('parent_id') is not None and (parent is None
                    or not integer(row.get('id')) or row['parent_id'] >= row['id']
                    or parent.get('attempt_id') != row.get('attempt_id')))):
            invalid_records += 1
        if attempt is not None:
            grouped[row['attempt_id']].append(row)
        if (isinstance(stage, str) and stage in STAGES
                and integer(row.get('wall_ns')) and integer(row.get('thread_cpu_ns'))):
            timed[stage].append(row)
    if invalid_records:
        issues.append('InvalidRecordIdentityStageOrParent')
    deficits = []
    event_counts, nonempty_events = Counter(), set()
    for token in attempts:
        rows = grouped.get(token.get('attempt_id'), ()) if integer(token.get('attempt_id')) else ()
        counts = Counter(row.get('stage') for row in rows if type(row.get('stage')) is str)
        empty = token.get('empty_claim') is True
        required = EMPTY if empty else PUBLISHED
        reasons = []
        if any(counts[name] != 1 for name in required):
            reasons.append('MissingOrDuplicateBoundary')
        if set(counts) != required:
            reasons.append('UnexpectedBoundary')
        if any(row.get('complete') is not True or row.get('outcome') != 'returned'
               or not integer(row.get('wall_ns')) or not integer(row.get('thread_cpu_ns'))
               for row in rows):
            reasons.append('IncompleteOrInvalidTiming')
        acks = [row for row in rows if row.get('stage') == 'delivery_ack']
        if (empty and acks) or (not empty and (len(acks) != 1
                or acks[0].get('delivery_success') is not True
                or acks[0].get('boundary') != 'native_callback_after_original_callback')):
            reasons.append('NativeCallbackMarkerInvalid')
        if not (token.get('empty_claim') is empty
                and token.get('publish_result_observed') is True
                and token.get('publish_result') is (False if empty else True)
                and token.get('complete') is True
                and token.get('outcome') == 'returned'
                and token.get('temporary_hooks_restored') is True
                and token.get('claim_path') == 'native_postgresql'
                and type(token.get('guard_results')) is list
                and len(token['guard_results']) == 1 and token['guard_results'][0] is True
                and type(token.get('native_claim_results')) is list
                and len(token['native_claim_results']) == 1 and token['native_claim_results'][0] is True):
            reasons.append('OriginalReturnAdmissionOrRestorationInvalid')
        event = token.get('event_id')
        if not empty:
            if type(event) is not str or not event:
                reasons.append('PublishedEventIdentityMissing')
            else:
                nonempty_events.add(event)
                if token.get('publish_result_observed') is True and token.get('publish_result') is True:
                    event_counts[event] += 1
        if reasons:
            deficits.append({'attempt_id': token.get('attempt_id'), 'reasons': reasons,
                             'stage_counts': dict(counts)})
    if deficits:
        issues.append('PartialAttempts')
    if any(count > 1 for count in event_counts.values()):
        issues.append('DuplicatePublishedEventIdentity')
    boundary_counts = Counter(row.get('stage') for row in records if type(row.get('stage')) is str)
    if dict(boundary_counts) != observation.get('boundaries'):
        issues.append('BoundarySummaryMismatch')
    counters = {name: observation.get(name) for name in COUNTERS}
    if (not all(integer(value) for value in counters.values())
            or counters['attempts_seen'] != counters['sampled_attempts']
            or counters['sampled_attempts'] != len(attempts)
            or any(counters[name] != 0 for name in ZERO_COUNTERS)
            or counters['hook_installs'] != counters['hook_restores']
            or observation.get('recording_failed') is not False
            or observation.get('errors') != [] or not attempts):
        issues.append('RawCountersMissingOverflowOrError')
    return {
        'schema_version': 1,
        'status': 'PROFILE_DATA_COMPLETE' if not issues else 'PROFILE_DATA_PARTIAL',
        'scope': 'profile_only', 'diagnostic_only': True,
        'qualification_admissible': False, 'capacity_accepted': False,
        'full_qualified': False, 'production_qualified': False,
        'issues': sorted(set(issues)), 'partial_attempts': deficits,
        'denominators': {'attempts': len(attempts), 'records': len(records),
            'unique_published_event_ids': len(event_counts),
            'unique_nonempty_event_ids': len(nonempty_events),
            'published_true': sum(row.get('publish_result_observed') is True
                                  and row.get('publish_result') is True for row in attempts),
            'literal_false': sum(row.get('publish_result_observed') is True
                                  and row.get('publish_result') is False for row in attempts),
            'empty_claim': sum(row.get('empty_claim') is True for row in attempts),
            'unobserved_return': sum(row.get('publish_result_observed') is not True for row in attempts),
            'guard_false_or_unknown': sum(not (type(row.get('guard_results')) is list
                and len(row['guard_results']) == 1 and row['guard_results'][0] is True) for row in attempts),
            'native_false_or_unknown': sum(not (type(row.get('native_claim_results')) is list
                and len(row['native_claim_results']) == 1 and row['native_claim_results'][0] is True) for row in attempts),
            'fallback_or_unknown_path': sum(row.get('claim_path') != 'native_postgresql' for row in attempts),
            'invalid_records': invalid_records, 'partial_attempts': len(deficits)},
        'raw_counters': counters, 'raw_boundary_counts': dict(boundary_counts),
        'phases': {name: {'rows': len(rows),
            'complete_rows': sum(row.get('complete') is True for row in rows),
            'inclusive_wall_ns': distribution([row['wall_ns'] for row in rows]),
            'inclusive_thread_cpu_ns': distribution([row['thread_cpu_ns'] for row in rows])}
            for name, rows in sorted(timed.items())},
        'limitations': list(LIMITATIONS),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    source = args.profile.resolve(strict=True)
    if source.stat().st_size > 96 * 1024 * 1024:
        parser.error('Profile exceeds 96 MiB')
    if args.output.resolve() == source:
        parser.error('Output must be separate from the profile')
    raw = source.read_bytes()
    if len(raw) > 96 * 1024 * 1024:
        parser.error('Profile exceeds 96 MiB')
    def reject_constant(value):
        raise ValueError('NonfiniteJSON')
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('DuplicateJSONKey')
            value[key] = item
        return value
    result = analyze(json.loads(raw, parse_constant=reject_constant,
                                object_pairs_hook=unique_object))
    if source.read_bytes() != raw:
        raise ValueError('ProfileChangedDuringAnalysis')
    result['input'] = {'name': source.name, 'bytes': len(raw),
                       'sha256': hashlib.sha256(raw).hexdigest()}
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2, allow_nan=False)
        stream.write('\n')
    print(json.dumps({'status': result['status'], 'capacity_accepted': False}))
    return 0 if not result['issues'] else 2


if __name__ == '__main__':
    sys.exit(main())
