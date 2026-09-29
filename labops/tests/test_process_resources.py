"""Pure exact-PID process resource contracts; no process discovery or database."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.events.process_resources import (
    GENERATOR_ROLES, PROCESS_ROLES, ProcessResources, own_process_snapshot,
    process_resources_profile, sanitize_process_resources, summarize_process_resources,
)
from benchmarks.events.consumer_topology import worker_roles
from benchmarks.events.writer_topology import generator_roles


def stat_text(pid, *, start=42, user=20, system=10, rss=8, comm='PRIVATE ) process (name)'):
    fields = ['0'] * 23
    fields[0], fields[11], fields[12], fields[19], fields[21] = 'R', str(user), str(system), str(start), str(rss)
    return f'{pid} ({comm}) ' + ' '.join(fields)


class ProcessFixture:
    def __init__(self, *, required_workers=('publisher',), stopped=(), consumer_topology='single',
                 writer_topology='writers-4'):
        self.files, self.reads, self.clock_value = {}, [], 100.0
        self.consumer_topology = consumer_topology
        self.writer_topology = writer_topology
        self.generator_roles = generator_roles(writer_topology)
        self.pids = {role: 1001 + index for index, role in enumerate(self.generator_roles + worker_roles(consumer_topology))}
        self.catalog = ProcessResources(required_roles=self.generator_roles + tuple(required_workers),
            known_stopped_roles=stopped, consumer_topology=consumer_topology, writer_topology=writer_topology,
            read_text=self.read, clock_ticks=100, page_size=4096,
            monotonic=self.clock)

    def clock(self):
        self.clock_value += .001
        return self.clock_value

    def read(self, path):
        path = str(path)
        self.reads.append(path)
        value = self.files[path]
        if isinstance(value, BaseException):
            raise value
        if callable(value):
            return value()
        return value

    def set(self, role, **kwargs):
        pid = self.pids[role]
        self.files[f'/proc/{pid}/stat'] = stat_text(pid, **kwargs)

    def child(self, role):
        with patch('benchmarks.events.process_resources.os.getpid', return_value=self.pids[role]):
            return own_process_snapshot(read_text=self.read, clock_ticks=100,
                page_size=4096, monotonic=self.clock)

    def start(self, role):
        self.set(role)
        self.catalog.register(role, self.pids[role])
        self.catalog.ready(role, child_snapshot=self.child(role) if role in self.generator_roles else None)

    def start_all(self):
        for role in self.catalog.profile()['required_roles']:
            if role not in self.catalog.profile()['known_stopped_roles']:
                self.start(role)

    def finish(self, role, *, user=30, system=15):
        self.set(role, user=user, system=system)
        receipt = self.child(role)
        result = self.catalog.finalize(role, receipt, exitcode=0, cleaned=True)
        del self.files[f'/proc/{self.pids[role]}/stat']
        return result

    def finish_generators(self):
        for role in self.generator_roles:
            self.finish(role)

    def clean(self):
        return sanitize_process_resources(self.catalog.sample(), consumer_topology=self.consumer_topology,
                                          writer_topology=self.writer_topology)


class ProcessResourceTests(unittest.TestCase):
    def test_six_writers_require_independent_extra_generator_receipts_and_counters(self):
        fixture = ProcessFixture(required_workers=('notification', 'notification-1'),
            consumer_topology='notification-dual', writer_topology='writers-6')
        fixture.start_all()
        sample = fixture.clean()
        fixture.finish_generators()
        final = fixture.clean()
        result = summarize_process_resources([sample], final, consumer_topology='notification-dual',
            writer_topology='writers-6', expected_profile=fixture.catalog.profile())
        self.assertTrue(result['collection_complete'])
        self.assertEqual(result['generator_complete_roles'], list(generator_roles('writers-6')))
        self.assertEqual(set(result['generator_cpu_deltas']), set(generator_roles('writers-6')))
        self.assertEqual(result['profile']['consumer_topology']['writer_lanes'], 6)
        rows = {row['role']: row for row in final['processes']}
        self.assertEqual(len({rows[role]['pid'] for role in generator_roles('writers-6')}), 6)
        for role in ('generator-4', 'generator-5'):
            self.assertEqual(rows[role]['ready_snapshot']['source'], 'child_origin')
            self.assertEqual(rows[role]['final_snapshot']['source'], 'child_origin')
            self.assertAlmostEqual(result['generator_cpu_deltas'][role]['user_cpu_seconds'], .1)
            self.assertAlmostEqual(result['generator_cpu_deltas'][role]['system_cpu_seconds'], .05)

    def test_six_default_role_contract_cannot_shrink_to_four_generators(self):
        profile = process_resources_profile(writer_topology='writers-6')
        self.assertEqual(profile['required_roles'], list(generator_roles('writers-6')))
        with self.assertRaises(ValueError):
            process_resources_profile(GENERATOR_ROLES, writer_topology='writers-6')
        fixture = ProcessFixture(writer_topology='writers-6')
        value = fixture.catalog.sample()
        value['profile']['required_roles'] = list(GENERATOR_ROLES)
        self.assertFalse(sanitize_process_resources(value, writer_topology='writers-6')['observed'])
        self.assertFalse(summarize_process_resources([value], value,
            writer_topology='writers-6')['collection_complete'])
        value['profile'].pop('required_roles')
        self.assertFalse(sanitize_process_resources(value, writer_topology='writers-6')['observed'])

    def test_six_catalog_cannot_be_relabelled_or_omit_an_extra_owned_generator(self):
        fixture = ProcessFixture(writer_topology='writers-6')
        value = fixture.catalog.sample()
        self.assertFalse(sanitize_process_resources(value)['observed'])
        for role in ('generator-4', 'generator-5'):
            with self.subTest(role=role):
                missing = {**value, 'processes': [row for row in value['processes'] if row['role'] != role]}
                self.assertFalse(sanitize_process_resources(missing, writer_topology='writers-6')['observed'])
        value['profile']['consumer_topology']['writer_lanes'] = 4
        self.assertFalse(sanitize_process_resources(value, writer_topology='writers-6')['observed'])

    def test_extra_generator_requires_child_origin_ready_and_final_receipts(self):
        fixture = ProcessFixture(writer_topology='writers-6')
        fixture.set('generator-4')
        fixture.catalog.register('generator-4', fixture.pids['generator-4'])
        with self.assertRaisesRegex(ValueError, 'originate in the child'):
            fixture.catalog.ready('generator-4')
        fixture.catalog.ready('generator-4', fixture.child('generator-4'))
        raw = fixture.catalog.sample()
        row = next(row for row in raw['processes'] if row['role'] == 'generator-4')
        row['ready_snapshot']['source'] = 'kernel_proc_stat'
        self.assertFalse(sanitize_process_resources(raw, writer_topology='writers-6')['observed'])
        with self.assertRaisesRegex(ValueError, 'Generator cannot be excluded'):
            fixture.catalog.finalize('generator-4', exitcode=0, cleaned=True, known_stopped=True)
        with self.assertRaises(ValueError):
            fixture.catalog.register('generator-6', 9000)

    def test_extra_generator_missing_close_or_actual_reap_never_qualifies(self):
        for boundary in ('missing_final', 'not_cleaned', 'not_reaped', 'nonzero_exit'):
            with self.subTest(boundary=boundary):
                fixture = ProcessFixture(writer_topology='writers-6')
                fixture.start_all()
                live = fixture.clean()
                for role in generator_roles('writers-6'):
                    if role != 'generator-5':
                        fixture.finish(role)
                fixture.set('generator-5', user=30, system=15)
                fixture.catalog.finalize('generator-5',
                    None if boundary == 'missing_final' else fixture.child('generator-5'),
                    exitcode=None if boundary == 'not_reaped' else (-9 if boundary == 'nonzero_exit' else 0),
                    cleaned=boundary != 'not_cleaned')
                report = summarize_process_resources([live], fixture.clean(), writer_topology='writers-6')
                self.assertFalse(report['collection_complete'])
                self.assertNotIn('generator-5', report['generator_complete_roles'])
                self.assertIsNone(report['generator_cpu_deltas']['generator-5']['user_cpu_seconds'])
                self.assertIn('GeneratorReadyFinalReapProofIncomplete', report['error_type_counts'])

    def test_dual_second_member_is_required_and_independently_measured(self):
        fixture = ProcessFixture(required_workers=('notification', 'notification-1'), consumer_topology='notification-dual')
        fixture.start_all()
        sample = fixture.clean()
        fixture.finish_generators()
        final = fixture.clean()
        result = summarize_process_resources([sample], final, consumer_topology='notification-dual')
        self.assertTrue(result['collection_complete'])
        self.assertIn('notification-1', result['worker_complete_roles'])
        rows = {row['role']: row for row in sample['processes']}
        self.assertNotEqual(rows['notification']['pid'], rows['notification-1']['pid'])
        self.assertNotEqual(rows['notification']['start_time_ticks'], None)
        final['processes'] = [row for row in final['processes'] if row['role'] != 'notification-1']
        self.assertFalse(summarize_process_resources([sample], final, consumer_topology='notification-dual')['collection_complete'])

    def test_dual_catalog_cannot_be_relabelled_as_single_or_omit_slot_one(self):
        fixture = ProcessFixture(required_workers=('notification', 'notification-1'), consumer_topology='notification-dual')
        value = fixture.catalog.sample()
        self.assertFalse(sanitize_process_resources(value)['observed'])
        value['processes'] = [row for row in value['processes'] if row['role'] != 'notification-1']
        self.assertFalse(sanitize_process_resources(value, consumer_topology='notification-dual')['observed'])

    def test_dual_callback_cannot_downgrade_independently_frozen_required_members(self):
        fixture = ProcessFixture(required_workers=('notification', 'notification-1'), consumer_topology='notification-dual')
        expected = fixture.catalog.profile()
        fixture.start_all()
        value = fixture.catalog.sample()
        value['profile']['required_roles'].remove('notification-1')
        for row in value['processes']:
            if row['role'] == 'notification-1':
                row.update(generation=None, stage='planned', applicable=False, pid=None,
                    start_time_ticks=None, value=None, registration_snapshot=None, ready_snapshot=None)
        self.assertFalse(sanitize_process_resources(value, consumer_topology='notification-dual',
            expected_profile=expected)['observed'])
        self.assertFalse(summarize_process_resources([value], value, consumer_topology='notification-dual',
            expected_profile=expected)['collection_complete'])

    def test_dual_member_rejects_foreign_role_and_duplicate_live_pid(self):
        fixture = ProcessFixture(required_workers=('notification', 'notification-1'), consumer_topology='notification-dual')
        fixture.start('notification')
        with self.assertRaises(ValueError):
            fixture.catalog.register('notification-1', fixture.pids['notification'])
        with self.assertRaises(ValueError):
            fixture.catalog.register('notification-2', 8888)
    def test_profile_names_all_planned_roles_and_precise_spans_without_wall_cpu_sum(self):
        profile = process_resources_profile(GENERATOR_ROLES + ('publisher',))
        self.assertEqual(profile['planned_roles'], list(PROCESS_ROLES))
        self.assertIn('before exit', profile['generator_delta_span'])
        self.assertTrue(profile['cpu_deltas_overlap_wall'])
        self.assertFalse(profile['sum_cpu_deltas_is_wall_time'])

    def test_planned_roles_are_not_live_observations_or_fabricated_zeros(self):
        fixture = ProcessFixture()
        snapshot = fixture.clean()
        self.assertTrue(snapshot['observed'])
        self.assertEqual(fixture.reads, [])
        self.assertTrue(all(row['stage'] == 'planned' and not row['applicable']
            and row['value'] is None for row in snapshot['processes']))
        report = summarize_process_resources([snapshot], snapshot)
        self.assertTrue(report['live_sample_coverage_complete'])
        self.assertFalse(report['collection_complete'])
        self.assertEqual(report['generator_complete_roles'], [])

    def test_exact_pid_stat_reads_allow_complex_comm_but_never_emit_it(self):
        fixture = ProcessFixture()
        fixture.start('generator-0')
        snapshot = fixture.clean()
        row = snapshot['processes'][0]
        self.assertEqual(row['value']['user_cpu_seconds'], .2)
        self.assertEqual(row['value']['system_cpu_seconds'], .1)
        self.assertEqual(row['value']['rss_bytes'], 32768)
        self.assertEqual(set(fixture.reads), {'/proc/1001/stat'})
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    def test_canonical_string_pid_and_start_are_accepted_bools_and_malformed_values_rejected(self):
        fixture = ProcessFixture()
        fixture.set('generator-0')
        fixture.catalog.register('generator-0', '1001', '42')
        for value in (True, False, 0, -1, 1.5, '001', '../1001', str(1 << 64)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ProcessFixture().catalog.register('generator-0', value)
        for value in (True, -1, 1.5, '042', str(1 << 64)):
            with self.subTest(start=value), self.assertRaises(ValueError):
                ProcessFixture().catalog.register('generator-0', 1001, value)

    def test_unknown_duplicate_role_and_owned_pid_are_rejected(self):
        fixture = ProcessFixture()
        fixture.start('generator-0')
        with self.assertRaises(ValueError): fixture.catalog.register('secret-role', 9999)
        with self.assertRaises(ValueError): fixture.catalog.register('generator-0', 1001)
        with self.assertRaises(ValueError): fixture.catalog.register('publisher', 1001)

    def test_pid_reuse_returns_unknown_and_does_not_rebind_start_identity(self):
        fixture = ProcessFixture()
        fixture.start('generator-0')
        fixture.set('generator-0', start=99)
        row = fixture.clean()['processes'][0]
        self.assertEqual(row['start_time_ticks'], 42)
        self.assertEqual(row['value']['status'], 'unavailable')
        self.assertIsNone(row['value']['user_cpu_seconds'])
        self.assertEqual(row['value']['error_type'], 'ValueError')

    def test_pid_mismatch_and_racing_start_identity_are_unknown(self):
        for kind in ('pid', 'race'):
            with self.subTest(kind=kind):
                fixture = ProcessFixture()
                fixture.start('generator-0')
                if kind == 'pid':
                    fixture.files['/proc/1001/stat'] = stat_text(9999)
                else:
                    values = iter([stat_text(1001), stat_text(1001, start=99)])
                    fixture.files['/proc/1001/stat'] = lambda: next(values)
                self.assertEqual(fixture.clean()['processes'][0]['value']['status'], 'unavailable')

    def test_zombie_process_does_not_qualify_as_live_numeric_observation(self):
        fixture = ProcessFixture()
        fixture.start('generator-0')
        fixture.files['/proc/1001/stat'] = stat_text(1001).replace(' R ', ' Z ')
        row = fixture.clean()['processes'][0]
        self.assertEqual(row['value']['status'], 'unavailable')
        self.assertIsNone(row['value']['rss_bytes'])

    def test_initial_missing_read_remains_unknown_after_later_identity_registration(self):
        fixture = ProcessFixture()
        fixture.files['/proc/1001/stat'] = PermissionError('PRIVATE startup')
        fixture.catalog.register('generator-0', 1001)
        fixture.set('generator-0')
        fixture.catalog.ready('generator-0', fixture.child('generator-0'))
        clean = fixture.clean()
        self.assertTrue(clean['observed'])
        row = clean['processes'][0]
        self.assertEqual(row['start_time_ticks'], 42)
        self.assertEqual(row['registration_snapshot']['status'], 'unavailable')
        self.assertIsNone(row['registration_snapshot']['start_time_ticks'])
        self.assertTrue(row['errors'])

    def test_missing_permission_and_malformed_stat_preserve_unknown_and_sanitized_errors(self):
        for value in (FileNotFoundError('PRIVATE'), PermissionError('PRIVATE'), stat_text(1001, user='nan'),
                      stat_text(1001, rss=-1), stat_text(1001, system=1 << 64)):
            with self.subTest(value=type(value).__name__):
                fixture = ProcessFixture()
                fixture.start('generator-0')
                fixture.files['/proc/1001/stat'] = value
                snapshot = fixture.clean()
                self.assertEqual(snapshot['processes'][0]['value']['status'], 'unavailable')
                self.assertNotIn('PRIVATE', json.dumps(snapshot))

    def test_reset_counter_remains_unknown_even_if_a_later_sample_recovers(self):
        fixture = ProcessFixture()
        fixture.start_all()
        first = fixture.clean()
        fixture.set('generator-0', user=2)
        bad = fixture.clean()
        fixture.set('generator-0', user=25)
        good = fixture.clean()
        fixture.finish_generators()
        report = summarize_process_resources([first, bad, good], fixture.clean())
        self.assertFalse(report['live_sample_coverage_complete'])
        self.assertFalse(report['collection_complete'])
        self.assertIsNone(bad['processes'][0]['value']['user_cpu_seconds'])

    def test_repeated_unavailable_reads_are_bounded_and_counted_without_silent_recovery(self):
        fixture = ProcessFixture()
        fixture.start('generator-0')
        fixture.files['/proc/1001/stat'] = PermissionError('PRIVATE unavailable')
        for _ in range(100):
            snapshot = fixture.clean()
        row = snapshot['processes'][0]
        self.assertEqual(len(row['errors']), 1)
        self.assertEqual(row['errors'][0]['occurrences'], 100)
        self.assertIsNone(row['value']['user_cpu_seconds'])

    def test_four_ready_final_receipts_and_actual_cleanup_reap_qualify_scoped_measurements(self):
        fixture = ProcessFixture()
        fixture.start_all()
        live = fixture.clean()
        fixture.finish_generators()
        final = fixture.clean()
        report = summarize_process_resources([live], final)
        self.assertTrue(report['collection_complete'])
        self.assertEqual(report['generator_complete_roles'], list(GENERATOR_ROLES))
        self.assertEqual(report['worker_complete_roles'], ['publisher'])
        self.assertAlmostEqual(report['generator_cpu_deltas']['generator-0']['user_cpu_seconds'], .1)
        for row in final['processes'][:4]:
            self.assertFalse(row['applicable'])
            self.assertIsNone(row['value'])
            self.assertEqual(row['final_snapshot']['source'], 'child_origin')
        self.assertNotIn('cpu_total_seconds', report)

    def test_actual_reap_and_success_code_are_required_not_cleanup_declaration_alone(self):
        for code in (None, 1, -9):
            with self.subTest(code=code):
                fixture = ProcessFixture()
                fixture.start_all()
                live = fixture.clean()
                for role in GENERATOR_ROLES:
                    fixture.catalog.finalize(role, fixture.child(role), cleaned=True, exitcode=code)
                self.assertFalse(summarize_process_resources([live], fixture.clean())['collection_complete'])

    def test_final_receipt_identity_source_missing_numeric_and_reset_are_rejected(self):
        for change in ({'pid': 9999}, {'start_time_ticks': 99}, {'source': 'kernel_proc_stat'},
                       {'rss_bytes': None}, {'system_cpu_seconds': float('inf')}, {'user_cpu_seconds': .01}):
            with self.subTest(change=change):
                fixture = ProcessFixture()
                fixture.start('generator-0')
                receipt = fixture.child('generator-0') | change
                with self.assertRaises(ValueError):
                    fixture.catalog.finalize('generator-0', receipt, cleaned=True, exitcode=0)
                self.assertTrue(fixture.clean()['processes'][0]['errors'])

    def test_generator_ready_requires_child_message_and_summary_rejects_kernel_substitute(self):
        fixture = ProcessFixture()
        fixture.set('generator-0')
        fixture.catalog.register('generator-0', 1001)
        with self.assertRaises(ValueError):
            fixture.catalog.ready('generator-0')
        fixture = ProcessFixture()
        fixture.start_all()
        live = fixture.clean()
        fixture.finish_generators()
        final = fixture.clean()
        final['processes'][0]['ready_snapshot']['source'] = 'kernel_proc_stat'
        report = summarize_process_resources([live], final)
        self.assertFalse(report['collection_complete'])
        self.assertNotIn('generator-0', report['generator_complete_roles'])

    def test_final_missing_child_receipt_cannot_use_cached_live_counter(self):
        fixture = ProcessFixture()
        fixture.start_all()
        live = fixture.clean()
        for role in GENERATOR_ROLES:
            fixture.catalog.finalize(role, cleaned=True, exitcode=0)
            del fixture.files[f'/proc/{fixture.pids[role]}/stat']
        report = summarize_process_resources([live], fixture.clean())
        self.assertFalse(report['collection_complete'])
        self.assertTrue(all(value is None for value in report['generator_cpu_deltas']['generator-0'].values()))

    def test_intentionally_stopped_owned_worker_is_explicit_unmeasured_not_zero(self):
        fixture = ProcessFixture(required_workers=('publisher', 'analytics'), stopped=('analytics',))
        fixture.start_all()
        live = fixture.clean()
        fixture.finish_generators()
        final = fixture.clean()
        report = summarize_process_resources([live], final)
        self.assertTrue(report['collection_complete'])
        row = next(row for row in final['processes'] if row['role'] == 'analytics')
        self.assertTrue(row['known_stopped'])
        self.assertIsNone(row['pid'])
        self.assertIsNone(row['value'])

    def test_unexpected_worker_exit_and_explicit_restarts_are_distinct(self):
        fixture = ProcessFixture()
        fixture.start_all()
        live = fixture.clean()
        fixture.catalog.finalize('publisher', cleaned=True, exitcode=1)
        fixture.finish_generators()
        self.assertFalse(summarize_process_resources([live], fixture.clean())['collection_complete'])
        fixture.pids['publisher'] = 7777
        fixture.set('publisher', start=88)
        fixture.catalog.register('publisher', 7777, 88)
        fixture.catalog.ready('publisher')
        restarted = fixture.clean()
        rows = [row for row in restarted['processes'] if row['role'] == 'publisher']
        self.assertEqual([row['generation'] for row in rows], [0, 1])
        self.assertEqual(rows[1]['start_time_ticks'], 88)
        self.assertEqual(rows[0]['exitcode'], 1)

    def test_partial_live_failure_is_not_forgiven_by_union_of_other_samples(self):
        fixture = ProcessFixture()
        fixture.start_all()
        first = fixture.clean()
        bad = json.loads(json.dumps(first))
        bad['processes'][0]['value'] = None
        fixture.finish_generators()
        report = summarize_process_resources([first, bad, first], fixture.clean())
        self.assertFalse(report['collection_complete'])
        self.assertIn('generator-0', report['observed_live_roles'])
        self.assertIn('RequiredLiveProcessCounterUnavailable', report['error_type_counts'])

    def test_whitelist_drops_arbitrary_secrets_fields_and_returned_data_is_a_copy(self):
        fixture = ProcessFixture()
        fixture.start_all()
        raw = fixture.catalog.sample()
        raw['environment'] = 'PRIVATE_SECRET'
        raw['profile']['commandline'] = 'PRIVATE_SECRET'
        raw['processes'][0]['path'] = 'PRIVATE_SECRET'
        raw['processes'][0]['value']['sql'] = 'PRIVATE_SECRET'
        clean = sanitize_process_resources(raw)
        self.assertTrue(clean['observed'])
        self.assertNotIn('PRIVATE', json.dumps(clean))
        raw['processes'][0]['stage'] = 'planned'
        self.assertEqual(fixture.catalog.sample()['processes'][0]['stage'], 'running')

    def test_bad_profile_duplicate_missing_roles_and_retained_scalar_after_failure_do_not_qualify(self):
        fixture = ProcessFixture()
        fixture.start_all()
        raw = fixture.catalog.sample()
        for modified in (raw | {'profile': {}}, raw | {'processes': raw['processes'][:-1]},
                         raw | {'processes': raw['processes'] + [raw['processes'][0]]}):
            self.assertFalse(sanitize_process_resources(modified)['observed'])
        raw['processes'][0]['value']['status'] = 'unavailable'
        row = sanitize_process_resources(raw)['processes'][0]
        self.assertIsNone(row['value']['user_cpu_seconds'])
        self.assertIsNone(row['value']['rss_bytes'])

    def test_callback_boundary_rejects_receipt_as_live_sample_and_duplicate_pid_identity(self):
        fixture = ProcessFixture()
        fixture.start_all()
        raw = fixture.catalog.sample()
        raw['processes'][0]['value']['source'] = 'child_origin'
        self.assertFalse(sanitize_process_resources(raw)['observed'])
        raw = fixture.catalog.sample()
        first, duplicate = raw['processes'][:2]
        duplicate['pid'], duplicate['start_time_ticks'] = first['pid'], first['start_time_ticks']
        for field in ('value', 'registration_snapshot', 'ready_snapshot'):
            duplicate[field]['pid'], duplicate[field]['start_time_ticks'] = first['pid'], first['start_time_ticks']
        self.assertFalse(sanitize_process_resources(raw)['observed'])

    def test_callback_identity_switch_counter_reset_and_cached_clock_cannot_qualify(self):
        for kind in ('identity', 'counter', 'cached'):
            with self.subTest(kind=kind):
                fixture = ProcessFixture()
                fixture.start_all()
                first, second = fixture.clean(), fixture.clean()
                row = second['processes'][0]
                if kind == 'identity':
                    row['pid'] = 9999
                    for field in ('value', 'registration_snapshot', 'ready_snapshot'):
                        row[field]['pid'] = 9999
                elif kind == 'counter':
                    row['value']['user_cpu_seconds'] = .01
                else:
                    row['value']['captured_monotonic'] = first['processes'][0]['value']['captured_monotonic']
                fixture.finish_generators()
                report = summarize_process_resources([first, second], fixture.clean())
                self.assertFalse(report['collection_complete'])
                self.assertFalse(report['live_sample_coverage_complete'])
                self.assertTrue(all(value is None for value in report['generator_cpu_deltas']['generator-0'].values()))

    def test_callback_child_final_cannot_reset_preceding_live_cpu_but_allows_later_live_read(self):
        for order in ('before_final', 'after_final'):
            with self.subTest(order=order):
                fixture = ProcessFixture()
                fixture.start_all()
                live = fixture.clean()
                fixture.finish_generators()
                final = fixture.clean()
                observed = live['processes'][0]['value']
                child_final = final['processes'][0]['final_snapshot']
                observed['user_cpu_seconds'] = child_final['user_cpu_seconds'] + .2
                observed['captured_monotonic'] = child_final['captured_monotonic'] + (
                    .001 if order == 'after_final' else -.001)
                report = summarize_process_resources([live], final)
                self.assertEqual(report['collection_complete'], order == 'after_final')
                if order == 'before_final':
                    self.assertIsNone(report['generator_cpu_deltas']['generator-0']['user_cpu_seconds'])

    def test_callback_unavailable_child_final_preserves_unknown_without_summary_exception(self):
        fixture = ProcessFixture()
        fixture.start_all()
        live = fixture.clean()
        fixture.finish_generators()
        final = fixture.catalog.sample()
        final['processes'][0]['final_snapshot']['status'] = 'unavailable'
        final = sanitize_process_resources(final)
        self.assertTrue(final['observed'])
        report = summarize_process_resources([live], final)
        self.assertFalse(report['collection_complete'])
        self.assertIsNone(report['generator_cpu_deltas']['generator-0']['user_cpu_seconds'])
        self.assertNotIn('generator-0', report['generator_complete_roles'])

    def test_symlink_cannot_redirect_exact_registered_pid_even_inside_allowed_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'1001').mkdir()
            (root/'9999').mkdir()
            (root/'9999'/'stat').write_text(stat_text(1001))
            (root/'1001'/'stat').symlink_to(root/'9999'/'stat')
            catalog = ProcessResources(proc_root=root, clock_ticks=100, page_size=4096)
            observation = catalog.register('generator-0', 1001)
            self.assertEqual(observation['status'], 'unavailable')
            self.assertEqual(observation['error_type'], 'ValueError')

    def test_child_helper_missing_own_proc_preserves_unavailability(self):
        snapshot = own_process_snapshot(read_text=lambda path: (_ for _ in ()).throw(PermissionError('PRIVATE')),
            clock_ticks=100, page_size=4096)
        self.assertEqual(snapshot['status'], 'unavailable')
        self.assertIsNone(snapshot['start_time_ticks'])
        self.assertIsNone(snapshot['rss_bytes'])
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    def test_invalid_units_never_silently_use_default_or_accept_bool(self):
        for unit in (True, 0, -1, 1.5, '001'):
            with self.subTest(unit=unit), self.assertRaises(ValueError):
                ProcessResources(clock_ticks=unit, page_size=4096)


if __name__ == '__main__':
    unittest.main()
