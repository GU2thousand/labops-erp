"""Offline data integrity and CLI failure behavior; no Django/database imports."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from benchmarks.events.analyze_native_observation import (
    COUNTERS, EMPTY, PUBLISHED, analyze, main,
)


def fixture(empty=False):
    event = None if empty else 'event-1'
    token = {'attempt_id': 1, 'event_id': event, 'empty_claim': empty,
        'publish_result_observed': True, 'publish_result': not empty,
        'outcome': 'returned', 'temporary_hooks_restored': True,
        'claim_path': 'native_postgresql', 'guard_results': [True],
        'native_claim_results': [True], 'complete': True}
    records = [{'id': i, 'attempt_id': 1, 'event_id': event,
        'parent_id': None, 'stage': stage, 'complete': True,
        'outcome': 'returned', 'wall_ns': 100, 'thread_cpu_ns': 20}
        for i, stage in enumerate(sorted(EMPTY if empty else PUBLISHED), 1)]
    for row in records:
        if row['stage'] == 'delivery_ack':
            row.update(delivery_success=True,
                       boundary='native_callback_after_original_callback')
    observation = {'schema_version': 1, 'mode': 'native-scoped',
        'qualification_admissible': False, 'nested_times_additive': False,
        'complete': True, 'status': 'COMPLETE_DIAGNOSTIC',
        'function_profile_requested': False, 'function_graph_status': 'NOT_REQUESTED',
        'attempts': [token], 'records': records,
        'boundaries': dict(Counter(row['stage'] for row in records)),
        **dict.fromkeys(COUNTERS, 0), 'recording_failed': False, 'errors': []}
    observation.update(attempts_seen=1, sampled_attempts=1)
    return {'schema_version': 1, 'observation_only': True,
        'diagnostic_only': True, 'function_profile_requested': False,
        'qualification_admissible': False, 'capacity_accepted': False,
        'publisher_observation': observation}


class NativeObservationAnalysisTests(unittest.TestCase):
    def test_complete_published_and_empty_data_remain_nonqualifying(self):
        for empty in (False, True):
            value = analyze(fixture(empty))
            self.assertEqual(value['status'], 'PROFILE_DATA_COMPLETE')
            self.assertEqual(value['denominators']['empty_claim'], int(empty))
            for field in ('qualification_admissible', 'capacity_accepted',
                          'full_qualified', 'production_qualified'):
                self.assertIs(value[field], False)

    def test_forged_complete_does_not_hide_missing_lease_boundary(self):
        profile = fixture()
        rows = profile['publisher_observation']['records']
        rows[:] = [row for row in rows if row['stage'] != 'lease_check_execute']
        value = analyze(profile)
        self.assertEqual(value['status'], 'PROFILE_DATA_PARTIAL')
        self.assertEqual(value['denominators']['records'], 12)
        self.assertIn('MissingOrDuplicateBoundary', value['partial_attempts'][0]['reasons'])
        self.assertIn('BoundarySummaryMismatch', value['issues'])

    def test_ack_failure_and_wrong_callback_boundary_remain_partial(self):
        for field, changed in (('delivery_success', False), ('boundary', 'broker_ack')):
            profile = fixture()
            row = next(row for row in profile['publisher_observation']['records']
                       if row['stage'] == 'delivery_ack')
            row[field] = changed
            value = analyze(profile)
            self.assertIn('NativeCallbackMarkerInvalid', value['partial_attempts'][0]['reasons'])

    def test_integer_truthiness_does_not_qualify_original_admission(self):
        for field in ('guard_results', 'native_claim_results'):
            profile = fixture()
            profile['publisher_observation']['attempts'][0][field] = [1]
            self.assertEqual(analyze(profile)['status'], 'PROFILE_DATA_PARTIAL')

    def test_overflow_unknown_and_unrestored_hooks_are_not_discarded(self):
        for field, value in (('overflow_records', 8), ('ack_unavailable', 1),
                             ('hook_restores', None)):
            profile = fixture()
            profile['publisher_observation'][field] = value
            result = analyze(profile)
            self.assertEqual(result['raw_counters'][field], value)
            self.assertIn('RawCountersMissingOverflowOrError', result['issues'])

    def test_boolean_nan_negative_and_missing_times_do_not_enter_distributions(self):
        for value in (True, float('nan'), -1, None):
            profile = fixture()
            profile['publisher_observation']['records'][0]['wall_ns'] = value
            result = analyze(profile)
            self.assertEqual(result['status'], 'PROFILE_DATA_PARTIAL')
            self.assertNotIn(profile['publisher_observation']['records'][0]['stage'], result['phases'])

    def test_duplicate_and_cross_attempt_parent_are_partial(self):
        for mutation in ('duplicate', 'cross_parent'):
            profile = fixture()
            observation = profile['publisher_observation']
            if mutation == 'duplicate':
                observation['records'].append(deepcopy(observation['records'][0]))
            else:
                observation['records'][0]['parent_id'] = observation['records'][-1]['id']
            self.assertEqual(analyze(profile)['status'], 'PROFILE_DATA_PARTIAL')

    def test_invalid_record_catalog_preserves_raw_denominator(self):
        profile = fixture()
        profile['publisher_observation']['records'][0]['stage'] = []
        result = analyze(profile)
        self.assertEqual(result['denominators']['records'], 13)
        self.assertEqual(result['denominators']['invalid_records'], 1)

    def test_nested_timings_remain_separate_and_input_is_unchanged(self):
        profile = fixture()
        before = deepcopy(profile)
        value = analyze(profile)
        self.assertEqual(profile, before)
        self.assertEqual(value['phases']['send']['inclusive_wall_ns']['p95'], 100)
        self.assertNotIn('total_wall_ns', value)
        self.assertTrue(any('not broker ACK latency' in line for line in value['limitations']))

    def test_shape_and_bounds_fail_before_creating_a_report(self):
        for changed in (None, {'publisher_observation': {}},
                        {'publisher_observation': {'attempts': [{}] * 4097, 'records': []}}):
            with self.assertRaises(ValueError):
                analyze(changed)

    def test_cli_preserves_input_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / 'profile.json', root / 'analysis.json'
            source.write_text(json.dumps(fixture()))
            before = source.read_bytes()
            self.assertEqual(main(['--profile', str(source), '--output', str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertEqual(result['input']['sha256'], hashlib.sha256(before).hexdigest())
            self.assertEqual(source.read_bytes(), before)
            with self.assertRaises(FileExistsError):
                main(['--profile', str(source), '--output', str(output)])
            with self.assertRaises(SystemExit):
                main(['--profile', str(source), '--output', str(source)])
            self.assertEqual(source.read_bytes(), before)

    def test_partial_cli_returns_two_and_retains_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / 'profile.json', root / 'analysis.json'
            profile = fixture()
            profile['publisher_observation']['overflow_attempts'] = 12
            source.write_text(json.dumps(profile))
            self.assertEqual(main(['--profile', str(source), '--output', str(output)]), 2)
            self.assertEqual(json.loads(output.read_text())['raw_counters']['overflow_attempts'], 12)

    def test_nonfinite_json_does_not_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / 'profile.json', root / 'analysis.json'
            source.write_text('{"unexpected":NaN}')
            with self.assertRaises(ValueError):
                main(['--profile', str(source), '--output', str(output)])
            self.assertFalse(output.exists())

    def test_duplicate_json_keys_do_not_create_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / 'profile.json', root / 'analysis.json'
            source.write_text('{"schema_version":1,"schema_version":2}')
            with self.assertRaisesRegex(ValueError, 'DuplicateJSONKey'):
                main(['--profile', str(source), '--output', str(output)])
            self.assertFalse(output.exists())

    def test_reported_partial_and_boolean_schema_cannot_qualify_profile(self):
        for target, field, value in (('profile', 'schema_version', True),
                                    ('observer', 'complete', False)):
            profile = fixture()
            (profile if target == 'profile' else profile['publisher_observation'])[field] = value
            self.assertEqual(analyze(profile)['status'], 'PROFILE_DATA_PARTIAL')

    def test_original_partial_attempt_is_not_promoted_by_matching_boundaries(self):
        for value in (False, None):
            profile = fixture()
            profile['publisher_observation']['attempts'][0]['complete'] = value
            self.assertEqual(analyze(profile)['status'], 'PROFILE_DATA_PARTIAL')

    def test_nonempty_failed_attempt_is_not_counted_as_published(self):
        profile = fixture()
        token = profile['publisher_observation']['attempts'][0]
        token.update(publish_result=False, claim_path='orm_or_unknown', guard_results=[False])
        value = analyze(profile)
        self.assertEqual(value['status'], 'PROFILE_DATA_PARTIAL')
        self.assertEqual(value['denominators']['published_true'], 0)
        self.assertEqual(value['denominators']['unique_published_event_ids'], 0)
        self.assertEqual(value['denominators']['unique_nonempty_event_ids'], 1)
        self.assertEqual(value['denominators']['literal_false'], 1)
        self.assertEqual(value['denominators']['guard_false_or_unknown'], 1)
        self.assertEqual(value['denominators']['fallback_or_unknown_path'], 1)
