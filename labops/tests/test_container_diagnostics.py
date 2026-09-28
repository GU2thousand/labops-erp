"""Pure container resource evidence tests; commands and Linux files are injected."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from benchmarks.events.container_diagnostics import (
    ContainerResources, SAFE_INSPECT_FORMAT, parse_flat_counters, parse_io_stat,
    parse_cgroup_path, parse_proc_stat,
)


PROJECT = 'labops_events_test'
SERVICE = 'redpanda-0'
CONTAINER_ID = 'a' * 64
PID = 4321
CGROUP_ROOT = '/test-cgroup'
CGROUP_PATH = CGROUP_ROOT + '/system.slice/docker-' + CONTAINER_ID + '.scope'


def proc_stat(*, pid=PID, start=123456, user=250, system=125, rss=7):
    fields = ['S'] + ['0'] * 49
    for index, value in ((11, user), (12, system), (19, start), (21, rss)):
        fields[index] = str(value)
    return str(pid) + ' (redpanda worker ) (nested name)) ' + ' '.join(fields) + '\n'


def inspection():
    return {'container_id': CONTAINER_ID, 'name': '/' + PROJECT + '-' + SERVICE + '-1',
            'status': 'running', 'pid': PID, 'project': PROJECT, 'service': SERVICE,
            'nano_cpus': 1000000000, 'cpu_quota': 100000, 'cpu_period': 100000,
            'memory_bytes': 134217728, 'cpuset_cpus': '0-1'}


class LinuxFixture:
    def __init__(self):
        self.inspection = inspection()
        self.compose_calls = []
        self.commands = []
        self.reads = []
        self.files = {
            f'/proc/{PID}/stat': proc_stat(),
            f'/proc/{PID}/cgroup': '0::/system.slice/docker-' + CONTAINER_ID + '.scope\n',
            f'/proc/{PID}/status': 'Name:\tredpanda\nCpus_allowed_list:\t0-1\n',
            f'/proc/{PID}/io': 'rchar: 100\nwchar: 200\nsyscr: 3\nsyscw: 4\nread_bytes: 4096\nwrite_bytes: 8192\ncancelled_write_bytes: 0\n',
            CGROUP_PATH + '/cpu.max': '100000 100000\n',
            CGROUP_PATH + '/memory.max': '134217728\n',
            CGROUP_PATH + '/cpuset.cpus.effective': '0-1\n',
            CGROUP_PATH + '/cpu.stat': 'usage_usec 900\nuser_usec 600\nsystem_usec 300\nnr_periods 10\nnr_throttled 2\nthrottled_usec 100\n',
            CGROUP_PATH + '/io.stat': '8:0 rbytes=4096 wbytes=8192 rios=1 wios=2\n',
            CGROUP_PATH + '/memory.current': '65536\n',
            CGROUP_PATH + '/memory.events': 'low 0\nhigh 1\nmax 2\noom 0\noom_kill 0\n',
        }

    def compose(self, *args):
        self.compose_calls.append(args)
        return CONTAINER_ID + '\n'

    def command(self, argv):
        self.commands.append(list(argv))
        return json.dumps(self.inspection)

    def read_text(self, path):
        path = str(path)
        self.reads.append(path)
        value = self.files.get(path, FileNotFoundError('PRIVATE missing file: ' + path))
        if isinstance(value, BaseException):
            raise value
        return value

    def discover(self):
        return ContainerResources.from_compose(
            self.compose, services=(SERVICE,), command_callback=self.command,
            expected_project=PROJECT, read_text=self.read_text,
            cgroup_root=CGROUP_ROOT, clock_ticks=100, page_size=4096)


class ContainerDiagnosticsTests(unittest.TestCase):
    def test_flat_counters_parse_exact_nonnegative_integers(self):
        self.assertEqual(parse_flat_counters('usage_usec 900\nuser_usec 600\nsystem_usec 300\n'),
                         {'usage_usec': 900, 'user_usec': 600, 'system_usec': 300})

    def test_flat_counters_reject_duplicates_negative_and_malformed_rows(self):
        for text in ('usage_usec 1\nusage_usec 2\n', 'usage_usec -1\n',
                     'usage_usec 1.5\n', 'usage_usec NaN\n', 'usage_usec\n',
                     'usage_usec 1 extra\n'):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    parse_flat_counters(text)

    def test_io_stat_preserves_devices_and_actual_counters(self):
        self.assertEqual(parse_io_stat('8:0 rbytes=4096 wbytes=8192 rios=1 wios=2\n8:16 rbytes=0 wbytes=10\n'),
                         {'8:0': {'rbytes': 4096, 'wbytes': 8192, 'rios': 1, 'wios': 2},
                          '8:16': {'rbytes': 0, 'wbytes': 10}})

    def test_io_stat_rejects_duplicate_devices_counters_and_malformed_values(self):
        for text in ('8:0 rbytes=1\n8:0 wbytes=2\n', '8:0 rbytes=1 rbytes=2\n',
                     '8:0 rbytes=-1\n', '8:0 rbytes=1.5\n', 'not-a-device rbytes=1\n',
                     '8:0 arbitrary\n'):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    parse_io_stat(text)

    def test_unified_cgroup_path_is_anchored_below_configured_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'cgroup'
            root.mkdir()
            target = root / 'system.slice' / 'docker-test.scope'
            target.mkdir(parents=True)
            self.assertEqual(parse_cgroup_path('5:cpu:/legacy\n0::/system.slice/docker-test.scope\n', root),
                             target.resolve())

    def test_cgroup_path_rejects_root_traversal_deleted_and_multiple_unified_rows(self):
        for text in ('0::/\n', '0::relative\n', '0::/../outside\n', '0::/a/../../outside\n',
                     '0::/docker/id (deleted)\n', '0::/a\n0::/b\n',
                     '0::/docker/secret\x00suffix\n', '5:cpu:/legacy\n'):
            with self.subTest(text=repr(text)):
                with self.assertRaises(ValueError):
                    parse_cgroup_path(text, '/test-cgroup')

    def test_cgroup_path_rejects_symlink_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'cgroup'
            outside = Path(directory) / 'outside'
            root.mkdir()
            outside.mkdir()
            (root / 'escaped').symlink_to(outside, target_is_directory=True)
            with self.assertRaises(ValueError):
                parse_cgroup_path('0::/escaped/container\n', root)

    def test_proc_stat_handles_embedded_parentheses_and_reports_cpu_rss_units(self):
        result = parse_proc_stat(proc_stat(), clock_ticks=100, page_size=4096)
        self.assertEqual(result['start_time_ticks'], 123456)
        self.assertEqual(result['user_cpu_seconds'], 2.5)
        self.assertEqual(result['system_cpu_seconds'], 1.25)
        self.assertEqual(result['rss_bytes'], 28672)

    def test_proc_stat_rejects_truncated_negative_and_malformed_numbers(self):
        for text in ('4321 (truncated) S 0\n', proc_stat(user=-1), proc_stat(system=-1),
                     proc_stat(start=-1), proc_stat(rss=-1), proc_stat(user='NaN')):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    parse_proc_stat(text, clock_ticks=100, page_size=4096)

    def test_discovery_uses_only_exact_safe_inspection_fields(self):
        fixture = LinuxFixture()
        fixture.inspection['Env'] = ['TOP_SECRET=password']
        fixture.inspection['Config'] = {'Env': ['TOP_SECRET=token']}
        resources = fixture.discover()
        self.assertEqual(fixture.compose_calls, [('ps', '--all', '-q', SERVICE)])
        self.assertEqual(fixture.commands[0], ['docker', 'inspect', '--format', SAFE_INSPECT_FORMAT, CONTAINER_ID])
        self.assertNotIn('.Config.Env', SAFE_INSPECT_FORMAT)
        self.assertNotIn('json .Config.Labels', SAFE_INSPECT_FORMAT)
        self.assertNotIn('password', SAFE_INSPECT_FORMAT.lower())
        self.assertNotIn('token', SAFE_INSPECT_FORMAT.lower())
        self.assertNotIn('TOP_SECRET', json.dumps(resources.profile()))
        self.assertNotIn('TOP_SECRET', json.dumps(resources.snapshot()))

    def test_profile_freezes_actual_cgroup_limits_and_affinity_as_a_deep_copy(self):
        fixture = LinuxFixture()
        fixture.files[CGROUP_PATH + '/cpu.max'] = '50000 100000\n'
        fixture.files[CGROUP_PATH + '/memory.max'] = '67108864\n'
        resources = fixture.discover()
        profile = resources.profile()
        row = profile['containers'][SERVICE]
        self.assertEqual(row['identity'], inspection())
        self.assertEqual(row['cgroup_membership']['value'], CGROUP_PATH)
        self.assertEqual(row['limits']['cpu_max'], {'status': 'available', 'value': {
            'quota_usec': 50000, 'period_usec': 100000, 'quota_cpus': .5, 'unlimited': False}})
        self.assertEqual(row['limits']['memory_max'], {'status': 'available', 'value': {
            'bytes': 67108864, 'unlimited': False}})
        for name in ('cpuset_cpus_effective', 'process_affinity'):
            self.assertEqual(row['limits'][name], {'status': 'available', 'value': {
                'cpu_ids': [0, 1], 'cpu_count': 2}})
        row['limits']['cpu_max']['value']['quota_usec'] = 1
        fixture.files[CGROUP_PATH + '/cpu.max'] = '200000 100000\n'
        self.assertEqual(resources.profile()['containers'][SERVICE]['limits']['cpu_max']['value']['quota_usec'],
                         50000, 'Frozen discovery must not change through copies or later kernel reads')

    def test_unlimited_limits_are_explicit_and_not_zero_or_unavailable(self):
        fixture = LinuxFixture()
        fixture.files[CGROUP_PATH + '/cpu.max'] = 'max 100000\n'
        fixture.files[CGROUP_PATH + '/memory.max'] = 'max\n'
        limits = fixture.discover().profile()['containers'][SERVICE]['limits']
        self.assertEqual(limits['cpu_max'], {'status': 'available', 'value': {
            'quota_usec': None, 'period_usec': 100000, 'quota_cpus': None, 'unlimited': True}})
        self.assertEqual(limits['memory_max'], {'status': 'available', 'value': {
            'bytes': None, 'unlimited': True}})

    def test_malformed_limits_are_unavailable_and_never_defaulted_to_zero(self):
        for name, filename, text in (('cpu_max', 'cpu.max', '100000 0\n'),
                                     ('cpu_max', 'cpu.max', '-1 100000\n'),
                                     ('memory_max', 'memory.max', '-1\n'),
                                     ('cpuset_cpus_effective', 'cpuset.cpus.effective', '3-1\n')):
            with self.subTest(name=name, text=text):
                fixture = LinuxFixture()
                fixture.files[CGROUP_PATH + '/' + filename] = text
                limits = fixture.discover().profile()['containers'][SERVICE]['limits']
                self.assertEqual(limits[name], {'status': 'unavailable', 'error_type': 'ValueError'})

    def test_snapshot_reports_actual_cgroup_and_process_counters(self):
        fixture = LinuxFixture()
        resources = fixture.discover()
        fixture.files[CGROUP_PATH + '/cpu.stat'] = 'usage_usec 1000\nuser_usec 700\nsystem_usec 300\n'
        snapshot = resources.snapshot()
        row = snapshot['containers'][SERVICE]
        self.assertEqual(row['status'], 'available')
        files = row['cgroup_v2']['files']
        self.assertEqual(files['cpu_stat'], {'status': 'available', 'value': {
            'usage_usec': 1000, 'user_usec': 700, 'system_usec': 300}})
        self.assertEqual(files['memory_current'], {'status': 'available', 'value': 65536})
        self.assertEqual(files['io_stat']['value']['8:0']['wbytes'], 8192)
        self.assertEqual(files['memory_events']['value']['high'], 1)
        self.assertEqual(row['process']['stat']['value']['rss_bytes'], 28672)
        self.assertEqual(row['process']['stat']['value']['user_cpu_seconds'], 2.5)
        self.assertEqual(row['process']['io']['value']['write_bytes'], 8192)
        self.assertEqual(row['cpu_total_seconds'], .001)
        self.assertEqual(row['memory_usage_bytes'], 65536)
        self.assertEqual(row['process_cpu_total_seconds'], 3.75)
        self.assertEqual(row['process_rss_bytes'], 28672)
        self.assertEqual(row['io_read_bytes'], 4096)
        self.assertEqual(row['io_write_bytes'], 8192)
        self.assertEqual(row['memory_high_events'], 1)
        self.assertNotEqual(row['cpu_total_seconds'], row['process_cpu_total_seconds'])
        self.assertNotEqual(row['memory_usage_bytes'], row['process_rss_bytes'])
        json.dumps(snapshot, allow_nan=False)

    def test_file_permission_and_missing_errors_are_unavailable_without_raw_messages(self):
        fixture = LinuxFixture()
        resources = fixture.discover()
        fixture.files[CGROUP_PATH + '/cpu.stat'] = PermissionError('TOP_SECRET permission error')
        fixture.files[CGROUP_PATH + '/memory.current'] = FileNotFoundError('TOP_SECRET missing file')
        snapshot = resources.snapshot()
        files = snapshot['containers'][SERVICE]['cgroup_v2']['files']
        self.assertEqual(files['cpu_stat'], {'status': 'unavailable', 'error_type': 'PermissionError'})
        self.assertEqual(files['memory_current'], {'status': 'unavailable', 'error_type': 'FileNotFoundError'})
        row = snapshot['containers'][SERVICE]
        self.assertNotIn('cpu_total_seconds', row, 'A main-PID CPU value cannot qualify container CPU')
        self.assertNotIn('memory_usage_bytes', row, 'A main-PID RSS value cannot qualify container memory')
        self.assertEqual(row['process_cpu_total_seconds'], 3.75)
        self.assertEqual(row['process_rss_bytes'], 28672)
        self.assertNotIn('TOP_SECRET', json.dumps(snapshot))
        self.assertNotIn('PRIVATE', json.dumps(snapshot))

    def test_discovery_identity_mismatch_never_reads_container_resource_files(self):
        for field, value in (('container_id', 'b' * 64), ('project', 'other-project'),
                             ('service', 'other-service'), ('name', '/unrelated-container'),
                             ('status', 'exited'), ('pid', 0)):
            with self.subTest(field=field):
                fixture = LinuxFixture()
                fixture.inspection[field] = value
                resources = fixture.discover()
                snapshot = resources.snapshot()
                row = snapshot['containers'][SERVICE]
                self.assertEqual(row['status'], 'unavailable')
                self.assertEqual(row['error_type'], 'ValueError')
                self.assertEqual(fixture.reads, [], 'Identity mismatch must not sample another process')

    def test_verified_stopped_container_never_fabricates_resource_samples(self):
        for status in ('exited', 'created'):
            with self.subTest(status=status):
                fixture = LinuxFixture()
                fixture.inspection.update(status=status, pid=0)
                resources = fixture.discover()
                self.assertTrue(resources.profile()['containers'][SERVICE]['known_stopped'])
                row = resources.snapshot()['containers'][SERVICE]
                self.assertEqual(row['status'], 'known_stopped')
                self.assertTrue(row['known_stopped'])
                self.assertEqual(row['identity'], fixture.inspection)
                self.assertEqual(row['process']['status'], 'unavailable')
                self.assertEqual(row['cgroup_v2']['status'], 'unavailable')
                for measurement in row['cgroup_v2']['files'].values():
                    self.assertEqual(measurement['status'], 'unavailable')
                    self.assertNotIn('value', measurement)
                for name in ('cpu_total_seconds', 'memory_usage_bytes',
                             'process_cpu_total_seconds', 'process_rss_bytes'):
                    self.assertNotIn(name, row)
                self.assertEqual(fixture.reads, [], 'A verified stopped container has no live PID to sample')

    def test_malformed_stopped_pid_and_running_pid_zero_are_unavailable(self):
        for status, pid in (('exited', PID), ('exited', -1), ('exited', True),
                            ('created', '0'), ('running', 0)):
            with self.subTest(status=status, pid=pid):
                fixture = LinuxFixture()
                fixture.inspection.update(status=status, pid=pid)
                row = fixture.discover().snapshot()['containers'][SERVICE]
                self.assertEqual(row['status'], 'unavailable')
                self.assertFalse(row.get('known_stopped', False))
                self.assertEqual(row['process']['status'], 'unavailable')
                self.assertEqual(row['cgroup_v2']['status'], 'unavailable')
                self.assertEqual(fixture.reads, [])

    def test_compose_or_inspect_failure_is_never_inferred_to_mean_stopped(self):
        for stage in ('compose', 'inspect'):
            with self.subTest(stage=stage):
                fixture = LinuxFixture()

                def fail(*args):
                    raise RuntimeError('TOP_SECRET discovery failure')

                if stage == 'compose':
                    fixture.compose = fail
                else:
                    fixture.command = fail
                row = fixture.discover().snapshot()['containers'][SERVICE]
                self.assertEqual(row['status'], 'unavailable')
                self.assertFalse(row.get('known_stopped', False))
                self.assertEqual(row['process']['status'], 'unavailable')
                self.assertEqual(row['cgroup_v2']['status'], 'unavailable')
                self.assertEqual(fixture.reads, [])
                self.assertNotIn('TOP_SECRET', json.dumps(row))

    def test_reused_pid_does_not_attribute_new_process_cpu_or_rss(self):
        fixture = LinuxFixture()
        resources = fixture.discover()
        fixture.files[f'/proc/{PID}/stat'] = proc_stat(start=123457, user=9999, rss=9999)
        row = resources.snapshot()['containers'][SERVICE]
        self.assertEqual(row['status'], 'unavailable')
        self.assertEqual(row['process']['status'], 'unavailable')
        self.assertEqual(row['cgroup_v2']['status'], 'unavailable')
        encoded = json.dumps(row)
        self.assertNotIn('rss_bytes', encoded)
        self.assertNotIn('user_cpu_seconds', encoded)
        self.assertNotIn('9999', encoded)

    def test_proc_pid_mismatch_does_not_attribute_foreign_process(self):
        fixture = LinuxFixture()
        resources = fixture.discover()
        fixture.files[f'/proc/{PID}/stat'] = proc_stat(pid=PID + 1)
        row = resources.snapshot()['containers'][SERVICE]
        self.assertEqual(row['status'], 'unavailable')
        self.assertEqual(row['process']['status'], 'unavailable')
        self.assertEqual(row['cgroup_v2']['status'], 'unavailable')
        self.assertNotIn('rss_bytes', json.dumps(row))

    def test_changed_cgroup_membership_is_not_sampled_as_original_container(self):
        fixture = LinuxFixture()
        resources = fixture.discover()
        fixture.files[f'/proc/{PID}/cgroup'] = '0::/docker/' + 'b' * 64 + '\n'
        fixture.reads.clear()
        row = resources.snapshot()['containers'][SERVICE]
        self.assertEqual(row['cgroup_v2']['status'], 'unavailable')
        self.assertIsNone(row['cgroup_v2']['path'])
        for measurement in row['cgroup_v2']['files'].values():
            self.assertEqual(measurement['status'], 'unavailable')
            self.assertNotIn('value', measurement)
        self.assertTrue(all(path.startswith('/proc/') for path in fixture.reads),
                        'Membership changes must suppress both original and foreign cgroup reads')


if __name__ == '__main__':
    unittest.main()
