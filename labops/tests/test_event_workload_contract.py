import copy
import json
from django.test import SimpleTestCase

from benchmarks.events.workload_contract import qualify_fault_workload


class FaultWorkloadContractTests(SimpleTestCase):
    def workload(self, count=30000, rate=50, elapsed=600):
        return {'input': count, 'completed_commands': count, 'elapsed_seconds': elapsed,
                'target_rate': rate, 'actual_command_rate': count / elapsed,
                'schedule_lateness_seconds': max(0, elapsed - count / rate)}

    def qualify(self, workload=None, **overrides):
        values = {'tier': 'full', 'requested_events': 30000, 'configured_rate': 50,
                  'actual_input_count': 30000, 'requested_min_outage_seconds': 600,
                  'measured_outage_seconds': 650}
        values.update(overrides)
        return qualify_fault_workload(workload or self.workload(), **values)

    def test_default_and_exact_five_percent_boundary_qualify(self):
        for elapsed in (600, 630):
            with self.subTest(elapsed=elapsed):
                result = self.qualify(self.workload(elapsed=elapsed))
                self.assertTrue(result['passed'])
                self.assertTrue(result['capacity_qualified'])
                self.assertEqual(result['qualification'], 'FULL_CAPACITY_QUALIFIED')
                self.assertEqual(result['frozen_gate']['maximum_generation_seconds'], 630)
                self.assertEqual(result['frozen_gate']['maximum_schedule_lateness_seconds'], 30)
                self.assertEqual(result['observed']['generation_elapsed_seconds'], elapsed)

    def test_slow_generation_fails_with_measurements_retained(self):
        result = self.qualify(self.workload(elapsed=630.001))
        self.assertFalse(result['passed'])
        self.assertIn('full_generation_window', result['failure_codes'])
        self.assertIn('full_actual_rate_lower_bound', result['failure_codes'])
        self.assertEqual(result['observed']['generation_elapsed_seconds'], 630.001)
        self.assertFalse(result['capacity_qualified'])

    def test_fast_pacing_fails_frozen_upper_bound(self):
        result = self.qualify(self.workload(elapsed=570))
        self.assertFalse(result['passed'])
        self.assertIn('full_actual_rate_upper_bound', result['failure_codes'])
        self.assertEqual(result['frozen_gate']['maximum_actual_command_rate'], 52.5)

    def test_exact_requested_counts_and_external_ids_are_required(self):
        cases = [(self.workload(), {'actual_input_count': 29999}),
                 ({**self.workload(), 'completed_commands': 29999}, {}),
                 ({**self.workload(), 'input': 30001}, {}),
                 (self.workload(count=29999), {'requested_events': 29999, 'actual_input_count': 29999})]
        for workload, overrides in cases:
            with self.subTest(overrides=overrides, input=workload['input']):
                result = self.qualify(workload, **overrides)
                self.assertFalse(result['passed'])
        self.assertIn('full_requested_events_minimum', self.qualify(cases[-1][0], **cases[-1][1])['failure_codes'])

    def test_larger_predeclared_denominator_has_derived_fixed_window(self):
        result = self.qualify(self.workload(count=36000, elapsed=756), requested_events=36000,
                              actual_input_count=36000, measured_outage_seconds=780)
        self.assertTrue(result['passed'])
        self.assertEqual(result['requested']['input_events'], 36000)
        self.assertEqual(result['frozen_gate']['expected_generation_seconds'], 720)
        self.assertEqual(result['frozen_gate']['maximum_generation_seconds'], 756)
        self.assertEqual(result['frozen_gate']['maximum_schedule_lateness_seconds'], 36)

    def test_fractional_denominator_boundaries_do_not_fail_from_float_rounding(self):
        for count in range(30000, 30101):
            elapsed = (count / 50) * 1.05
            with self.subTest(count=count):
                result = self.qualify(self.workload(count=count, elapsed=elapsed), requested_events=count,
                                      actual_input_count=count, measured_outage_seconds=elapsed + 1)
                self.assertTrue(result['passed'], result['failure_codes'])
                self.assertEqual(result['frozen_gate']['numeric_comparison_ulps'], 4)
                outside = self.qualify(self.workload(count=count, elapsed=elapsed + .001), requested_events=count,
                                       actual_input_count=count, measured_outage_seconds=elapsed + 1)
                self.assertFalse(outside['passed'])
                self.assertIn('full_generation_window', outside['failure_codes'])

    def test_full_target_rate_cannot_be_lowered(self):
        result = self.qualify(self.workload(rate=49), configured_rate=49)
        self.assertFalse(result['passed'])
        self.assertIn('full_configured_rate', result['failure_codes'])

    def test_reported_rate_and_lateness_must_match_recomputed_observations(self):
        for overrides in ({'actual_command_rate': 50}, {'schedule_lateness_seconds': 0}, {'target_rate': 49}):
            with self.subTest(overrides=overrides):
                result = self.qualify({**self.workload(elapsed=630), **overrides})
                self.assertFalse(result['passed'])
                self.assertTrue(any('matches' in code for code in result['failure_codes']))

    def test_nan_inf_bool_and_zero_observations_fail_with_json_safe_evidence(self):
        for key in ('elapsed_seconds', 'actual_command_rate', 'schedule_lateness_seconds'):
            for value in (float('nan'), float('inf'), True, -1):
                with self.subTest(key=key, value=value):
                    result = self.qualify({**self.workload(), key: value})
                    self.assertFalse(result['passed'])
                    self.assertIn('workload.' + key, result['invalid_values'])
                    json.dumps(result, allow_nan=False)
        self.assertFalse(self.qualify({**self.workload(), 'elapsed_seconds': 0})['passed'])

    def test_outage_minimum_and_actual_are_distinct_and_generation_is_contained(self):
        result = self.qualify(requested_min_outage_seconds=300, measured_outage_seconds=620)
        self.assertTrue(result['passed'])
        self.assertEqual(result['requested']['minimum_outage_seconds'], 300)
        self.assertEqual(result['observed']['measured_outage_seconds'], 620)
        too_short = self.qualify(requested_min_outage_seconds=600, measured_outage_seconds=599)
        self.assertFalse(too_short['passed'])
        self.assertIn('outage_reached_requested_minimum', too_short['failure_codes'])
        impossible = self.qualify(requested_min_outage_seconds=300, measured_outage_seconds=350)
        self.assertFalse(impossible['passed'])
        self.assertIn('outage_contains_generation_window', impossible['failure_codes'])

    def test_smoke_preserves_counts_and_rates_without_claiming_capacity(self):
        result = self.qualify(self.workload(count=20, rate=10, elapsed=8), tier='smoke',
                              requested_events=20, configured_rate=10, actual_input_count=20,
                              requested_min_outage_seconds=5, measured_outage_seconds=9)
        self.assertTrue(result['passed'])
        self.assertFalse(result['capacity_qualified'])
        self.assertEqual(result['qualification'], 'SMOKE_CAPACITY_UNQUALIFIED')
        self.assertEqual(result['observed']['reported_actual_command_rate'], 2.5)
        self.assertEqual(result['observed']['reported_schedule_lateness_seconds'], 6)
        self.assertIsNone(result['checks']['full_generation_window'])
        self.assertFalse(result['frozen_gate']['applied'])
        failed = self.qualify(self.workload(count=20, rate=10, elapsed=8), tier='smoke',
                              requested_events=20, configured_rate=10, actual_input_count=19,
                              requested_min_outage_seconds=5, measured_outage_seconds=9)
        self.assertFalse(failed['passed'])

    def test_supplied_extra_thresholds_do_not_override_frozen_gate_or_mutate_input(self):
        workload = {**self.workload(elapsed=700), 'maximum_generation_seconds': 99999, 'tolerance': .9}
        original = copy.deepcopy(workload)
        result = self.qualify(workload, measured_outage_seconds=800)
        self.assertFalse(result['passed'])
        self.assertEqual(result['frozen_gate']['maximum_generation_seconds'], 630)
        self.assertEqual(workload, original)

    def test_invalid_configuration_counts_outage_and_tier_fail_as_evidence(self):
        for overrides in ({'tier': []}, {'configured_rate': float('nan')},
                          {'requested_events': True}, {'actual_input_count': 30000.0},
                          {'requested_min_outage_seconds': float('inf')},
                          {'measured_outage_seconds': float('nan')}, {'measured_outage_seconds': -1}):
            with self.subTest(overrides=overrides):
                result = self.qualify(**overrides)
                self.assertFalse(result['passed'])
                json.dumps(result, allow_nan=False)

    def test_missing_measurements_fail_without_substituting_successful_defaults(self):
        result = qualify_fault_workload({}, tier='full', requested_events=30000, configured_rate=50,
                                       actual_input_count=30000, requested_min_outage_seconds=600,
                                       measured_outage_seconds=650)
        self.assertFalse(result['passed'])
        self.assertIsNone(result['observed']['generation_elapsed_seconds'])
        self.assertIsNone(result['observed']['reported_actual_command_rate'])
        self.assertIn('exact_input_and_completed_denominator', result['failure_codes'])

    def test_unrepresentable_numeric_input_still_produces_json_safe_failure(self):
        enormous = 10 ** 5000
        for field in ('requested_events', 'configured_rate', 'actual_input_count',
                      'requested_min_outage_seconds', 'measured_outage_seconds'):
            with self.subTest(field=field):
                result = self.qualify(**{field: enormous})
                self.assertFalse(result['passed'])
                self.assertEqual(result['invalid_values'][field]['type'], 'int')
                self.assertEqual(result['invalid_values'][field]['value'], '[unrepresentable scalar]')
                json.dumps(result, allow_nan=False)
