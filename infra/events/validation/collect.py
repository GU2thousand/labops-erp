"""Capture config fingerprints and actual cluster metadata, excluding secrets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
import requests
from wait_ready import load_env


def network_identity(command, project):
    """Inspect only network identity fields, never container Env or full config."""
    def inspect(argv):
        result = subprocess.run(argv, text=True, capture_output=True)
        return {'exit_code': result.returncode, 'output': result.stdout, 'stderr': result.stderr}
    container_format = ('{"container_id":{{json .Id}},"name":{{json .Name}},'
        '"status":{{json .State.Status}},'
        '"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
        '"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
        '"networks":{{json .NetworkSettings.Networks}}}')
    services = {}
    for name in ('redpanda-0', 'redpanda-1', 'redpanda-2'):
        ids = inspect([*command, 'ps', '--all', '--quiet', name])
        rows = []
        for container in ids['output'].splitlines() if ids['exit_code'] == 0 else []:
            result = inspect(['docker', 'container', 'inspect', container, '--format', container_format])
            rows.append(json.loads(result['output']) if result['exit_code'] == 0 else result)
        services[name] = {'container_ids_result': ids, 'containers': rows}
    network_format = ('{"network_id":{{json .Id}},"name":{{json .Name}},'
        '"driver":{{json .Driver}},"ipam_config":{{json .IPAM.Config}},'
        '"containers":{{json .Containers}}}')
    network = inspect(['docker', 'network', 'inspect', project + '_default', '--format', network_format])
    return {'observed_at': time.time(), 'compose_project': project,
        'bridge': json.loads(network['output']) if network['exit_code'] == 0 else network,
        'brokers': services, 'inspection_scope': 'network IDs, aliases, IPAM and runtime addresses only'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--network-snapshot-only', action='store_true',
        help='Capture identity before/after fault injection without running the full final collector')
    parser.add_argument('--network-stage', default='collection',
        help='Unique evidence label, for example before, quorum-after or collection')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', args.network_stage):
        parser.error('--network-stage must be 1-48 lowercase letters, digits, underscore or hyphen')
    env = load_env(args.env_file)
    sensitive = []
    secrets_path = Path(args.env_file).parent / 'secrets.json'
    if secrets_path.exists():
        sensitive.extend(json.loads(secrets_path.read_text()).values())
    sensitive.extend(value for key, value in env.items() if any(word in key for word in ['PASSWORD', 'TOKEN', 'SECRET']))
    bootstrap = env.get('RP_BOOTSTRAP_USER', '').split(':')
    if len(bootstrap) >= 2:
        sensitive.append(bootstrap[1])
    sensitive = sorted({value for value in sensitive if isinstance(value, str) and value}, key=len, reverse=True)
    def redact(value):
        if isinstance(value, str):
            for secret in sensitive:
                value = value.replace(secret, '[REDACTED]')
            return value
        if isinstance(value, dict):
            return {key: redact(item) for key, item in value.items()}
        if isinstance(value, list):
            return [redact(item) for item in value]
        return value
    destination = args.evidence_dir
    destination.mkdir(parents=True, exist_ok=True)
    (destination / 'logs').mkdir(exist_ok=True)
    (destination / 'metrics').mkdir(exist_ok=True)
    command = ['docker', 'compose', '--env-file', args.env_file, '-f', 'infra/events/validation/compose.yaml', '--profile', 'metrics']
    def run(argv):
        result = subprocess.run(argv, text=True, capture_output=True)
        return redact({'exit_code': result.returncode, 'output': result.stdout, 'stderr': result.stderr})
    network_path = destination / ('network-identity-' + args.network_stage + '.json')
    with network_path.open('x') as output:
        output.write(json.dumps(redact(network_identity(command, env['LABOPS_VALIDATION_PROJECT'])), indent=2) + '\n')
    if args.network_snapshot_only:
        print(json.dumps({'run_id': args.run_id, 'network_evidence': str(network_path)}))
        return
    # Only image IDs and repo digests are inspected, never container Env.
    image = run(['docker', 'image', 'inspect', env['REDPANDA_IMAGE'], '--format', '{{json .RepoDigests}}'])
    commit = run(['git', 'rev-parse', 'HEAD'])['output'].strip()
    manifest = {'run_id': args.run_id, 'commit': commit, 'github_run_id': os.getenv('GITHUB_RUN_ID'),
        'github_run_attempt': os.getenv('GITHUB_RUN_ATTEMPT'), 'github_run_url': f'https://github.com/{os.getenv("GITHUB_REPOSITORY")}/actions/runs/{os.getenv("GITHUB_RUN_ID")}' if os.getenv('GITHUB_RUN_ID') else None,
        'broker_image': env['REDPANDA_IMAGE'], 'actual_image_digests': image,
        'python': platform.python_version(), 'platform': platform.platform(),
        'host_cpu_count': os.cpu_count(), 'docker_resources': run(['docker', 'info', '--format', '{{json .NCPU}} {{json .MemTotal}}']),
        'scope': 'three broker processes on one host; independent AZ and production unverified',
        'config_sha256': {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in [Path('infra/events/validation/compose.yaml'), Path('infra/events/topics.json'), *Path(args.env_file).parent.glob('*.yaml')]},
        'clients': run(['python', '-c', 'import confluent_kafka; print(confluent_kafka.version()); print(confluent_kafka.libversion())']),
        'durability': {'developer_mode': False, 'unsafe_bypass_fsync': False, 'write_caching_default': 'disabled', 'overprovisioned': True, 'host_checks': False}}
    collection_path = destination / 'collection-manifest.json'
    with collection_path.open('x') as output:
        output.write(json.dumps(redact(manifest), indent=2) + '\n')
    primary = destination / 'manifest.json'
    if not primary.exists():
        harness = destination / 'harness-manifest.json'
        combined = json.loads(harness.read_text()) if harness.exists() else {}
        combined['infrastructure_collection'] = manifest
        with primary.open('x') as output:
            output.write(json.dumps(redact(combined), indent=2) + '\n')
    logs = run([*command, 'logs', '--no-color'])
    (destination / 'logs' / 'brokers-postgres.log').write_text(logs['output'] + logs['stderr'])
    (destination / 'containers.json').write_text(json.dumps(run([*command, 'ps', '--all', '--format', 'json']), indent=2) + '\n')
    errors = []
    session = requests.Session()
    session.auth = (env['KAFKA_ADMIN_USERNAME'], env['KAFKA_ADMIN_PASSWORD'])
    session.verify = env['KAFKA_SSL_CA_LOCATION']
    for node, port in enumerate([19644, 29644, 39644]):
        for endpoint, name in [('/v1/cluster/health_overview', 'health'), ('/v1/cluster_config', 'cluster-config'), ('/public_metrics', 'metrics')]:
            try:
                response = session.get(f'https://127.0.0.1:{port}' + endpoint, timeout=5, allow_redirects=False)
                response.raise_for_status()
                content = response.text
                if name == 'cluster-config':
                    configuration = response.json()
                    allowed = ['kafka_enable_authorization', 'admin_api_require_auth', 'auto_create_topics_enabled', 'internal_topic_replication_factor', 'group_topic_partitions', 'write_caching_default', 'enable_idempotence', 'storage_min_free_bytes']
                    content = json.dumps({key: configuration.get(key) for key in allowed}, indent=2)
                target = destination / ('metrics' if name == 'metrics' else '.') / f'redpanda-{node}-{name}.txt'
                target.write_text(redact(content) + '\n')
            except Exception as exc:
                errors.append({'node': node, 'endpoint': endpoint, 'error_class': type(exc).__name__})
    try:
        exporter = requests.get('http://127.0.0.1:19308/metrics', timeout=5, allow_redirects=False)
        exporter.raise_for_status()
        (destination / 'metrics' / 'kafka-exporter.txt').write_text(redact(exporter.text))
    except Exception as exc:
        errors.append({'endpoint': 'kafka-exporter', 'error_class': type(exc).__name__})
    try:
        prometheus = requests.get('http://127.0.0.1:19091/api/v1/targets', timeout=5, allow_redirects=False)
        prometheus.raise_for_status()
        (destination / 'metrics' / 'prometheus-targets.json').write_text(json.dumps(redact(prometheus.json()), indent=2) + '\n')
        alerts = requests.get('http://127.0.0.1:19091/api/v1/alerts', timeout=5, allow_redirects=False)
        alerts.raise_for_status()
        (destination / 'metrics' / 'prometheus-alerts-after.json').write_text(json.dumps(redact(alerts.json()), indent=2) + '\n')
    except Exception as exc:
        errors.append({'endpoint': 'prometheus', 'error_class': type(exc).__name__})
    (destination / 'collection-errors.json').write_text(json.dumps(errors, indent=2) + '\n')
    # Apply the same explicit credential redaction to every text evidence file,
    # including worker logs and failure summaries captured by the harness.
    text_suffixes = {'.json', '.jsonl', '.log', '.md', '.csv', '.prom', '.txt', '.sql'}
    for path in destination.rglob('*'):
        if path.is_file() and path.suffix in text_suffixes:
            original = path.read_text()
            protected = redact(original)
            if protected != original:
                path.write_text(protected)
    print(json.dumps({'run_id': args.run_id, 'evidence_dir': str(destination), 'collection_errors': len(errors), 'private_keys_uploaded': False}))


if __name__ == '__main__':
    main()
