"""Stdlib-only admission and evidence for fixed standard hosted Linux runners.

Only the calling process's cgroup membership and fixed CPU-limit files are read.
Logical CPUs, affinity and visible quotas do not establish physical core count.
Sources: GitHub runner/variable references and Linux cgroup-v2 cpu.max docs.
"""
import argparse
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import subprocess


PROFILE_VERSION = 'standard-hosted-runner-v1'
RUNNERS = {'x64': ('ubuntu-24.04', 'X64', {'x86_64', 'amd64'}),
           'arm64': ('ubuntu-24.04-arm', 'ARM64', {'aarch64', 'arm64'})}
CGROUP_ROOT = Path('/sys/fs/cgroup')
UINT64_MAX = 2 ** 64 - 1


def select_runner(choice, event_name):
    """Reject arbitrary labels, manual empty choices and non-x64 automatic jobs."""
    if event_name not in ('push', 'pull_request', 'workflow_dispatch'):
        raise ValueError('Unsupported workflow event')
    if event_name != 'workflow_dispatch':
        if choice not in (None, '', 'x64'):
            raise ValueError('Automatic acceptance requires x64')
        choice = 'x64'
    if choice not in RUNNERS:
        raise ValueError('Runner choice must be x64 or arm64')
    return {'choice': choice, 'label': RUNNERS[choice][0],
            'runner_arch': RUNNERS[choice][1]}


def _positive_integer(token):
    if not isinstance(token, str) or not re.fullmatch(r'[0-9]+', token):
        raise ValueError('Expected an unsigned integer')
    value = int(token)
    if not 0 < value <= UINT64_MAX:
        raise ValueError('CPU limit must be a positive uint64')
    return value


def parse_cpu_max(text):
    fields = text.split()
    if len(fields) != 2:
        raise ValueError('cpu.max must contain quota and period')
    quota = None if fields[0] == 'max' else _positive_integer(fields[0])
    period = _positive_integer(fields[1])
    return {'quota_microseconds': quota, 'period_microseconds': period,
            'quota_cpu_equivalents': quota / period if quota is not None else None,
            'unlimited': quota is None}


def parse_cpu_set(text):
    result = set()
    for token in text.strip().split(','):
        match = re.fullmatch(r'([0-9]+)(?:-([0-9]+))?', token)
        if not match:
            raise ValueError('Invalid effective CPU set')
        first = int(match[1])
        last = int(match[2]) if match[2] else first
        if first > last or last > 1048575 or last - first > 65535:
            raise ValueError('Effective CPU range exceeds the profile bound')
        values = set(range(first, last + 1))
        if values & result:
            raise ValueError('Duplicate effective CPU set entry')
        result.update(values)
    return sorted(result)


def own_cgroup_path(text):
    rows = [line for line in text.splitlines() if line.startswith('0::')]
    if len(rows) != 1:
        raise ValueError('Expected one own cgroup v2 membership')
    raw = rows[0][3:]
    if not raw.startswith('/') or len(raw) > 4096 or any(ord(char) < 32 for char in raw):
        raise ValueError('Invalid own cgroup path')
    if raw != '/':
        segments = raw[1:].split('/')
        if len(segments) > 64 or any(part in ('', '.', '..') or not re.fullmatch(r'[A-Za-z0-9_.:@-]+', part)
               for part in segments):
            raise ValueError('Unsafe own cgroup path')
    return PurePosixPath(raw)


def selected_image_profile(container, image, expected_project, expected_service):
    """Whitelist evidence for a Compose container's already-selected image ID."""
    if container.get('project') != expected_project or container.get('service') != expected_service:
        raise ValueError('Container is outside the isolated project/service')
    container_id = container.get('container_id')
    image_id = container.get('image_id')
    if not isinstance(container_id, str) or not re.fullmatch(r'[a-f0-9]{64}', container_id) \
            or not isinstance(image_id, str) or not re.fullmatch(r'sha256:[a-f0-9]{64}', image_id) \
            or image.get('image_id') != image_id:
        raise ValueError('Selected image identity does not match the owned container')
    digests = image.get('repo_digests')
    if digests is not None and (not isinstance(digests, list) or any(
            not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9._:/-]+@sha256:[a-f0-9]{64}', value)
            for value in digests)):
        raise ValueError('Malformed image repository digest evidence')
    architecture, image_os = image.get('architecture'), image.get('os')
    if architecture not in ('amd64', 'arm64') or image_os != 'linux':
        raise ValueError('Unexpected selected image platform')
    reference = container.get('declared_image_ref')
    if not isinstance(reference, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,511}', reference):
        raise ValueError('Invalid declared image reference')
    version = image.get('image_version_label')
    if version is not None and (not isinstance(version, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}', version)):
        version = None
    return {'status': 'available', 'container_id': container_id,
            'project': expected_project, 'service': expected_service,
            'declared_image_ref': reference, 'selected_image_id': image_id,
            'selected_image_id_kind': 'selected image configuration digest',
            'repo_digests': digests, 'architecture': architecture, 'os': image_os,
            'image_version_label': version, 'actual_binary_version': None,
            'repo_digest_kind': 'Docker RepoDigests; index or manifest kind not classified',
            'child_manifest_digest': None,
            'limitations': 'No pull, registry resolution or binary version inference was performed.'}


def _read_text(path):
    # Fixed kernel files are small; avoid unbounded reads of a substituted file.
    with Path(path).open() as source:
        text = source.read(65537)
    if len(text) > 65536:
        raise ValueError('Kernel profile file exceeds the read bound')
    return text


def _capture(operation):
    try:
        return {'status': 'available', 'value': operation()}
    except Exception as error:
        return {'status': 'unavailable', 'value': None,
                'error_type': type(error).__name__}


def cgroup_cpu_profile(read_text=_read_text):
    """Read own membership, own effective cpuset and ancestor fixed cpu.max files."""
    membership = _capture(lambda: own_cgroup_path(read_text('/proc/self/cgroup')))
    if membership['status'] != 'available':
        return {**membership, 'scope': 'visible cgroup v2 hierarchy'}
    relative = membership['value']
    root = CGROUP_ROOT.resolve()
    own = (root / str(relative).lstrip('/')).resolve()
    try:
        own.relative_to(root)
    except ValueError:
        return {'status': 'unavailable', 'error_type': 'ValueError', 'value': None,
                'scope': 'visible cgroup v2 hierarchy'}
    cpuset = _capture(lambda: parse_cpu_set(read_text(own / 'cpuset.cpus.effective')))
    levels = []
    current = own
    # cpu.max exists on non-root cgroups. The root has no local cpu.max;
    # unknown or inaccessible non-root limits never become unlimited/zero.
    while current != root:
        limit = _capture(lambda path=current: parse_cpu_max(read_text(path / 'cpu.max')))
        levels.append({'relative_path': '/' + str(current.relative_to(root)), **limit})
        current = current.parent
    finite = [level['value']['quota_cpu_equivalents'] for level in levels
              if level['status'] == 'available' and not level['value']['unlimited']]
    complete = all(level['status'] == 'available' for level in levels)
    return {'status': 'available', 'scope': 'visible cgroup v2 hierarchy',
            'own_relative_path': str(relative), 'cpu_max_levels': levels,
            'effective_cpuset': cpuset,
            'visible_quota_complete': complete,
            'minimum_observed_quota_cpu_equivalents': min(finite) if finite else None,
            'effective_visible_quota_cpu_equivalents': min(finite) if finite and complete else None,
            'no_finite_visible_quota': complete and not finite,
            'root_cpu_max_not_applicable': True}


def _affinity(getter):
    cpus = sorted(getter(0))
    if not cpus or any(type(cpu) is not int or cpu < 0 for cpu in cpus) or len(set(cpus)) != len(cpus):
        raise ValueError('Invalid logical CPU affinity')
    return {'logical_cpu_ids': cpus, 'logical_cpu_count': len(cpus)}


def build_runner_profile(choice, event_name, requested_label, runner_arch,
                         source_sha, checkedout_sha, *,
                         runner_environment='github-hosted', runner_os='Linux',
                         repository_visibility='public', machine=None, system=None,
                         python_version=None, cpu_count=os.cpu_count,
                         affinity_getter=None, read_text=_read_text):
    selection = select_runner(choice, event_name)
    machine = platform.machine() if machine is None else machine
    system = platform.system() if system is None else system
    if requested_label != selection['label'] or runner_arch != selection['runner_arch']:
        raise ValueError('Declared runner label or actual runner architecture mismatches selection')
    if not isinstance(machine, str) or machine.lower() not in RUNNERS[selection['choice']][2]:
        raise ValueError('Actual platform architecture mismatches selection')
    if runner_environment != 'github-hosted' or runner_os != 'Linux' or system != 'Linux':
        raise ValueError('Standard GitHub-hosted Linux runner required')
    if repository_visibility != 'public':
        raise ValueError('The frozen comparison requires a public repository')
    if not isinstance(source_sha, str) or not re.fullmatch(r'(?:[a-f0-9]{40}|[a-f0-9]{64})', source_sha) \
            or source_sha != checkedout_sha:
        raise ValueError('Checked-out source revision differs from the workflow source')
    if affinity_getter is None:
        affinity_getter = getattr(os, 'sched_getaffinity', None)
    affinity = _capture(lambda: _affinity(affinity_getter))
    logical = _capture(cpu_count)
    if logical['value'] is None:
        logical = {'status': 'unavailable', 'value': None, 'error_type': 'UnknownCPUCount'}
    elif type(logical['value']) is not int or logical['value'] <= 0:
        logical = {'status': 'unavailable', 'value': None, 'error_type': 'InvalidCPUCount'}
    return {'version': PROFILE_VERSION, 'admission_passed': True,
            'phase': 'before dependencies, Docker and business commands',
            'requested_runner': selection, 'event_name': event_name,
            'actual_runner_arch': runner_arch, 'actual_machine': machine,
            'actual_system': system, 'runner_environment': runner_environment,
            'repository_visibility': repository_visibility,
            'source_git_sha': checkedout_sha, 'workflow_source_sha': source_sha,
            'python_version': platform.python_version() if python_version is None else python_version,
            'logical_cpu_count': logical, 'cpu_affinity': affinity,
            'cgroup_cpu': cgroup_cpu_profile(read_text),
            'physical_core_count': None, 'physical_core_count_status': 'not measured',
            'limitations': ['Logical CPU count and affinity do not establish physical core count.',
                            'Cgroup limits cover only the visible own-process hierarchy.']}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--choice', required=True)
    parser.add_argument('--event-name', required=True)
    parser.add_argument('--requested-label', required=True)
    parser.add_argument('--runner-arch', required=True)
    parser.add_argument('--runner-environment', required=True)
    parser.add_argument('--runner-os', required=True)
    parser.add_argument('--repository-visibility', required=True)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    try:
        revision = subprocess.run(['git', 'rev-parse', 'HEAD'], check=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  text=True).stdout.strip()
        profile = build_runner_profile(args.choice, args.event_name, args.requested_label,
            args.runner_arch, args.source_sha, revision,
            runner_environment=args.runner_environment, runner_os=args.runner_os,
            repository_visibility=args.repository_visibility)
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('x') as output:
            json.dump(profile, output, indent=2, sort_keys=True, allow_nan=False)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        # Never emit subprocess stderr, paths from a permission exception or
        # environment contents. Failed admission occurs before any Docker step.
        parser.exit(2, 'Runner profile admission failed (' + type(error).__name__ + ').\n')
    print('Frozen runner profile: ' + profile['requested_runner']['label'])


if __name__ == '__main__':
    main()
