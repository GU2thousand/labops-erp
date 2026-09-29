"""Pure admission and fixed-file profile fixtures; no Docker or services."""
from copy import deepcopy
import ast
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.events.runner_profile import (
    build_runner_profile, cgroup_cpu_profile, own_cgroup_path,
    parse_cpu_max, parse_cpu_set, select_runner, selected_image_profile,
)


SHA = 'a' * 40
PROJECT = 'labops_events_test'
CONTAINER_ID = 'b' * 64
IMAGE_ID = 'sha256:' + 'c' * 64


class Files:
    def __init__(self):
        self.reads = []
        self.files = {'/proc/self/cgroup': '0::/user.slice/runner.scope\n',
            '/sys/fs/cgroup/user.slice/runner.scope/cpuset.cpus.effective': '0-3\n',
            '/sys/fs/cgroup/user.slice/runner.scope/cpu.max': 'max 100000\n',
            '/sys/fs/cgroup/user.slice/cpu.max': '200000 100000\n'}

    def read(self, path):
        self.reads.append(str(path))
        value = self.files.get(str(path), FileNotFoundError('PRIVATE absent file'))
        if isinstance(value, Exception):
            raise value
        return value


def profile(files=None, **changes):
    files = files or Files()
    arguments = {'choice': 'arm64', 'event_name': 'workflow_dispatch',
        'requested_label': 'ubuntu-24.04-arm', 'runner_arch': 'ARM64',
        'source_sha': SHA, 'checkedout_sha': SHA, 'machine': 'aarch64', 'system': 'Linux',
        'python_version': '3.12.12', 'cpu_count': lambda: 4,
        'affinity_getter': lambda pid: {0, 1, 2, 3}, 'read_text': files.read}
    arguments.update(changes)
    return build_runner_profile(**arguments)


def images():
    container = {'container_id': CONTAINER_ID, 'image_id': IMAGE_ID,
        'project': PROJECT, 'service': 'postgres', 'declared_image_ref': 'postgres:17.7',
        'Env': ['PASSWORD=PRIVATE'], 'password': 'PRIVATE'}
    image = {'image_id': IMAGE_ID, 'repo_digests': ['postgres@sha256:' + 'd' * 64],
        'architecture': 'arm64', 'os': 'linux', 'image_version_label': None,
        'Env': ['PASSWORD=PRIVATE'], 'Labels': {'unrelated': 'PRIVATE'}}
    return container, image


def collector_function():
    # Execute the production pure callback function without importing the
    # collector's HTTP/runtime dependencies. The injected command callback is
    # the only inspection boundary; no Docker process is launched by fixtures.
    source = Path(__file__).resolve().parents[2] / 'infra/events/validation/collect.py'
    tree = ast.parse(source.read_text())
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == 'selected_service_images')
    namespace = {'json': json, 're': re, 'selected_image_profile': selected_image_profile}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), namespace)
    return namespace['selected_service_images']


class CollectorImages:
    def __init__(self, failed_role=None):
        roles = ('redpanda-0', 'redpanda-1', 'redpanda-2', 'postgres', 'kafka-exporter', 'prometheus')
        self.containers = {str(index + 1) * 64: {'container_id': str(index + 1) * 64,
            'project': PROJECT, 'service': role, 'declared_image_ref': 'official/' + role + ':fixed',
            'image_id': 'sha256:' + ('a' if role.startswith('redpanda') else str(index + 1)) * 64}
            for index, role in enumerate(roles)}
        self.role_ids = {row['service']: identity for identity, row in self.containers.items()}
        self.calls = []
        self.failed_role = failed_role

    def run(self, argv):
        self.calls.append(list(argv))
        if argv[:3] == ['docker', 'compose', 'ps']:
            return {'exit_code': 0, 'output': self.role_ids[argv[-1]] + '\n', 'stderr': ''}
        if argv[:3] == ['docker', 'container', 'inspect']:
            return {'exit_code': 0, 'output': json.dumps(self.containers[argv[3]]), 'stderr': ''}
        if argv[:3] == ['docker', 'image', 'inspect']:
            image_id = argv[3]
            if self.failed_role and image_id == self.containers[self.role_ids[self.failed_role]]['image_id']:
                return {'exit_code': 1, 'output': '\n',
                        'stderr': 'template parsing error: PRIVATE inspection failure'}
            # Native nil-label fixtures render valid JSON with a null optional
            # version, retaining identity/platform rather than skipping image.
            return {'exit_code': 0, 'output': json.dumps({'image_id': image_id,
                'repo_digests': ['official/image@' + image_id], 'architecture': 'arm64',
                'os': 'linux', 'image_version_label': None}), 'stderr': ''}
        raise AssertionError('Fixture attempted an unexpected command')


class RunnerProfileTests(unittest.TestCase):
    def test_automatic_events_have_one_fixed_x64_selection(self):
        for event in ('push', 'pull_request'):
            for choice in (None, '', 'x64'):
                self.assertEqual(select_runner(choice, event),
                    {'choice': 'x64', 'label': 'ubuntu-24.04', 'runner_arch': 'X64'})
            with self.assertRaises(ValueError):
                select_runner('arm64', event)

    def test_native_arm_profile_freezes_observations_without_physical_core_inference(self):
        files = Files()
        observed = profile(files)
        self.assertTrue(observed['admission_passed'])
        self.assertEqual(observed['source_git_sha'], SHA)
        self.assertEqual(observed['requested_runner']['label'], 'ubuntu-24.04-arm')
        self.assertEqual(observed['logical_cpu_count']['value'], 4)
        self.assertEqual(observed['cpu_affinity']['value']['logical_cpu_ids'], [0, 1, 2, 3])
        self.assertEqual(observed['cgroup_cpu']['effective_visible_quota_cpu_equivalents'], 2)
        self.assertIsNone(observed['physical_core_count'])
        self.assertEqual(set(files.reads), set(files.files))
        self.assertNotIn('PRIVATE', json.dumps(observed, allow_nan=False))

    def test_manual_empty_arbitrary_or_nonstandard_choices_fail_before_kernel_reads(self):
        for choice in ('', None, 'ARM64', 'ubuntu-24.04-arm', 'self-hosted', 'gpu'):
            files = Files()
            with self.subTest(choice=choice), self.assertRaises(ValueError):
                profile(files, choice=choice)
            self.assertEqual(files.reads, [])

    def test_label_architecture_hosting_visibility_and_source_mismatch_fail_before_reads(self):
        invalid = [{'requested_label': 'ubuntu-latest'}, {'runner_arch': 'X64'},
            {'machine': 'x86_64'}, {'runner_environment': 'self-hosted'},
            {'runner_os': 'macOS'}, {'system': 'Darwin'},
            {'repository_visibility': 'private'}, {'checkedout_sha': 'b' * 40},
            {'source_sha': 'arbitrary'}, {'event_name': 'pull_request_target'}]
        for changes in invalid:
            files = Files()
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                profile(files, **changes)
            self.assertEqual(files.reads, [])

    def test_native_x64_is_admitted_on_automatic_default(self):
        observed = profile(choice='', event_name='pull_request', requested_label='ubuntu-24.04',
                           runner_arch='X64', machine='x86_64')
        self.assertEqual(observed['requested_runner']['choice'], 'x64')
        self.assertTrue(observed['admission_passed'])

    def test_cpu_max_preserves_finite_fraction_and_unlimited_without_false_zero(self):
        self.assertEqual(parse_cpu_max('150000 100000\n')['quota_cpu_equivalents'], 1.5)
        unlimited = parse_cpu_max('max 100000\n')
        self.assertTrue(unlimited['unlimited'])
        self.assertIsNone(unlimited['quota_microseconds'])
        self.assertIsNone(unlimited['quota_cpu_equivalents'])
        for text in ('100000', 'max 0', '0 100000', '-1 100000', 'NaN 100000',
                     '100000 100000 extra', '18446744073709551616 1', '1 0'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_cpu_max(text)

    def test_cpu_set_grammar_does_not_duplicate_logical_cpus(self):
        self.assertEqual(parse_cpu_set('0-3,8,10-11\n'), [0, 1, 2, 3, 8, 10, 11])
        for text in ('', '-1', '0,0', '0-2,2-3', '3-1', '0-1 trailing', '0-1000000'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse_cpu_set(text)

    def test_own_membership_rejects_traversal_extra_rows_and_non_kernel_paths(self):
        self.assertEqual(str(own_cgroup_path('0::/\n')), '/')
        for text in ('0::relative\n', '0::/../etc\n', '0::/a/../../etc\n',
                     '0::/a\n0::/b\n', '0::/a (deleted)\n', '0::/a\x00b\n',
                     '5:cpu:/legacy\n', '0::/a//b\n'):
            with self.subTest(text=repr(text)), self.assertRaises(ValueError):
                own_cgroup_path(text)

    def test_unknown_quota_or_affinity_does_not_become_unlimited_or_zero(self):
        files = Files()
        files.files['/sys/fs/cgroup/user.slice/runner.scope/cpu.max'] = PermissionError('PRIVATE quota')
        observed = profile(files, cpu_count=lambda: None,
                           affinity_getter=lambda pid: (_ for _ in ()).throw(PermissionError('PRIVATE affinity')))
        self.assertEqual(observed['logical_cpu_count']['status'], 'unavailable')
        self.assertIsNone(observed['logical_cpu_count']['value'])
        self.assertEqual(observed['cpu_affinity']['status'], 'unavailable')
        self.assertIsNone(observed['cpu_affinity']['value'])
        quota = observed['cgroup_cpu']
        self.assertFalse(quota['visible_quota_complete'])
        self.assertFalse(quota['no_finite_visible_quota'])
        self.assertIsNone(quota['effective_visible_quota_cpu_equivalents'])
        self.assertEqual(quota['minimum_observed_quota_cpu_equivalents'], 2)
        self.assertNotIn('PRIVATE', json.dumps(observed))

    def test_cgroup_path_symlink_escape_is_not_read(self):
        with tempfile.TemporaryDirectory() as directory:
            root, outside = Path(directory) / 'root', Path(directory) / 'outside'
            root.mkdir()
            outside.mkdir()
            (root / 'escape').symlink_to(outside, target_is_directory=True)
            reads = []

            def read(path):
                reads.append(str(path))
                return '0::/escape\n'

            with patch('benchmarks.events.runner_profile.CGROUP_ROOT', root):
                observed = cgroup_cpu_profile(read)
            self.assertEqual(observed['status'], 'unavailable')
            self.assertEqual(reads, ['/proc/self/cgroup'])

    def test_selected_image_whitelist_retains_actual_identity_not_private_extra_fields(self):
        container, image = images()
        observed = selected_image_profile(container, image, PROJECT, 'postgres')
        self.assertEqual(observed['selected_image_id'], IMAGE_ID)
        self.assertEqual(observed['architecture'], 'arm64')
        self.assertEqual(observed['declared_image_ref'], 'postgres:17.7')
        self.assertIsNone(observed['image_version_label'])
        self.assertIsNone(observed['actual_binary_version'])
        self.assertIsNone(observed['child_manifest_digest'])
        self.assertNotIn('PRIVATE', json.dumps(observed))

    def test_selected_image_rejects_unrelated_container_and_mismatched_or_malformed_digest(self):
        container, image = images()
        for area, key, value in [('container', 'project', 'other'),
                                ('container', 'service', 'other'),
                                ('container', 'container_id', 'bad'),
                                ('image', 'image_id', 'sha256:' + 'e' * 64),
                                ('image', 'repo_digests', ['invalid']),
                                ('image', 'architecture', 'unknown')]:
            candidate_container, candidate_image = deepcopy(container), deepcopy(image)
            (candidate_container if area == 'container' else candidate_image)[key] = value
            with self.subTest(area=area, key=key), self.assertRaises(ValueError):
                selected_image_profile(candidate_container, candidate_image, PROJECT, 'postgres')

    def test_collector_nil_optional_versions_preserve_all_six_identities_and_cached_image_reads(self):
        fixture = CollectorImages()
        observed = collector_function()(['docker', 'compose'], fixture.run, PROJECT)
        self.assertEqual(set(observed), set(fixture.role_ids))
        for role, row in observed.items():
            self.assertEqual(row['status'], 'available')
            self.assertEqual(row['architecture'], 'arm64')
            self.assertEqual(row['os'], 'linux')
            self.assertEqual(row['selected_image_id'], fixture.containers[fixture.role_ids[role]]['image_id'])
            self.assertIsNone(row['image_version_label'])
            self.assertIsNone(row['actual_binary_version'])
            self.assertIsNone(row['child_manifest_digest'])
        inspected = [argv[3] for argv in fixture.calls if argv[:3] == ['docker', 'image', 'inspect']]
        self.assertEqual(len(inspected), 4)
        self.assertEqual(len(set(inspected)), 4)
        self.assertNotIn('PRIVATE', json.dumps(observed))

    def test_collector_does_not_waive_a_real_image_inspection_failure(self):
        fixture = CollectorImages(failed_role='kafka-exporter')
        observed = collector_function()(['docker', 'compose'], fixture.run, PROJECT)
        self.assertEqual(observed['kafka-exporter'], {'status': 'unavailable', 'error_type': 'ValueError'})
        self.assertEqual(observed['postgres']['status'], 'available')
        self.assertIsNone(observed['postgres']['image_version_label'])
        self.assertNotIn('PRIVATE', json.dumps(observed))


if __name__ == '__main__':
    unittest.main()
