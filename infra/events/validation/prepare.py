"""Generate fresh disposable validation credentials, TLS and broker config.

Never run this against retained production data. Output contains secrets and is
excluded from Git and evidence uploads. Regeneration refuses an existing run.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import subprocess

ROOT = Path(__file__).resolve().parents[3]
IMAGE = 'docker.redpanda.com/redpandadata/redpanda:v26.2.2@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96'
# Frozen before worker startup. Keep natural persisted retries within the
# 15-minute recovery window and enough attempts for the 10-minute outage.
# This CI policy does not measure the production default's longer backoffs.
VALIDATION_RETRY_SECONDS = [15, 30] + [60] * 22
VALIDATION_RETRY_JITTER = .2


def validate_ipv4_prefix(value):
    if not re.fullmatch(r'[0-9]{1,3}(?:\.[0-9]{1,3}){2}', value):
        raise ValueError('validation IPv4 prefix must contain three decimal octets')
    network = ipaddress.IPv4Network(value + '.0/24')
    if not any(network.subnet_of(ipaddress.IPv4Network(private))
               for private in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')):
        raise ValueError('validation subnet must be within an RFC1918 private range')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--ipv4-prefix', default=os.getenv('LABOPS_VALIDATION_IPV4_PREFIX', '10.243.77'),
        help='Unused RFC1918 three-octet /24 prefix; Docker refuses overlapping existing pools')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', args.run_id):
        parser.error('run-id must be 1-48 lowercase ASCII letters, digits, _ or -')
    try:
        ipv4_prefix = validate_ipv4_prefix(args.ipv4_prefix)
    except ValueError as exc:
        parser.error(str(exc))
    output = Path(__file__).resolve().parent / 'generated'
    if output.exists():
        parser.error('generated already exists; use another checkout or explicitly preserve/remove the prior disposable run')
    output.mkdir(mode=0o700)
    passwords = {name: secrets.token_urlsafe(32) for name in ['admin', 'publisher', 'notification', 'analytics', 'dlq', 'exporter', 'replay']}
    db_password = secrets.token_urlsafe(32)
    (output / 'secrets.json').write_text(json.dumps(passwords, indent=2) + '\n')
    tls_directory = output / 'tls'
    tls_directory.mkdir(mode=0o755)
    tls_directory.chmod(0o755)
    def openssl(*arguments):
        subprocess.run(['openssl', *arguments], cwd=tls_directory, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-days', '2', '-subj', '/CN=LabOps disposable validation CA', '-keyout', 'ca.key', '-out', 'ca.crt')
    openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-days', '2', '-subj', '/CN=Wrong validation CA', '-keyout', 'wrong-ca.key', '-out', 'wrong-ca.crt')
    for node in range(3):
        name = f'redpanda-{node}'
        openssl('req', '-newkey', 'rsa:2048', '-nodes', '-sha256', '-subj', f'/CN={name}', '-keyout', f'{name}.key', '-out', f'{name}.csr')
        extension = tls_directory / f'{name}.ext'
        extension.write_text(f'subjectAltName=DNS:{name},DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
        openssl('x509', '-req', '-in', f'{name}.csr', '-CA', 'ca.crt', '-CAkey', 'ca.key', '-CAcreateserial', '-days', '2', '-sha256', '-extfile', extension.name, '-out', f'{name}.crt')
        def tls(listener):
            return {'name': listener, 'enabled': True, 'require_client_auth': False, 'cert_file': f'/etc/labops-tls/{name}.crt', 'key_file': f'/etc/labops-tls/{name}.key', 'truststore_file': '/etc/labops-tls/ca.crt'}
        config = {'redpanda': {
            'data_directory': '/var/lib/redpanda/data', 'node_id': node,
            'developer_mode': False, 'empty_seed_starts_cluster': False,
            'seed_servers': [{'host': {'address': f'redpanda-{i}', 'port': 33145}} for i in range(3)],
            'rpc_server': {'address': '0.0.0.0', 'port': 33145},
            'advertised_rpc_api': {'address': name, 'port': 33145},
            'kafka_api': [{'name': 'internal', 'address': '0.0.0.0', 'port': 9092, 'authentication_method': 'sasl'}, {'name': 'external', 'address': '0.0.0.0', 'port': 19092, 'authentication_method': 'sasl'}],
            'advertised_kafka_api': [{'name': 'internal', 'address': name, 'port': 9092}, {'name': 'external', 'address': '127.0.0.1', 'port': 19092 + node * 10000}],
            'kafka_api_tls': [tls('internal'), tls('external')],
            'admin': [{'name': 'admin', 'address': '0.0.0.0', 'port': 9644}], 'admin_api_tls': [tls('admin')],
            'rack': f'same-host-process-{node}',
        }, 'rpk': {'overprovisioned': True, 'tune_network': False, 'tune_disk_scheduler': False, 'tune_disk_nomerges': False, 'tune_disk_write_cache': False, 'tune_cpu': False}}
        # JSON is a strict YAML subset; keep prepare.py free of PyYAML dependency.
        (output / f'{name}.yaml').write_text(json.dumps(config, indent=2) + '\n')
    bootstrap = {'superusers': ['admin'], 'kafka_enable_authorization': True,
        'admin_api_require_auth': True, 'auto_create_topics_enabled': False,
        'default_topic_replications': 3, 'internal_topic_replication_factor': 3,
        'group_topic_partitions': 3, 'write_caching_default': 'disabled',
        'enable_idempotence': True, 'storage_min_free_bytes': 268435456,
        'enable_metrics_reporter': False, 'enable_consumer_group_metrics': ['group', 'partition', 'consumer_lag']}
    (output / 'bootstrap.yaml').write_text(json.dumps(bootstrap, indent=2) + '\n')
    prefix = 'labops.' + args.run_id
    values = {'LABOPS_VALIDATION_PROJECT': 'labops_events_' + args.run_id,
        'LABOPS_VALIDATION_IPV4_PREFIX': ipv4_prefix,
        'REDPANDA_IMAGE': IMAGE, 'RP_BOOTSTRAP_USER': f'admin:{passwords["admin"]}:SCRAM-SHA-256',
        'POSTGRES_PASSWORD': db_password, 'LABOPS_DB_MODE': 'postgres',
        'DATABASE_URL': f'postgresql://labops:{db_password}@127.0.0.1:55434/labops_events',
        'LABOPS_SECRET_KEY': secrets.token_urlsafe(48), 'LABOPS_DEBUG': '1',
        'LABOPS_EVENT_TRANSPORT': 'kafka', 'KAFKA_BOOTSTRAP_SERVERS': '127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092',
        'EVENT_RETRY_SECONDS': ','.join(map(str, VALIDATION_RETRY_SECONDS)),
        'EVENT_RETRY_JITTER': str(VALIDATION_RETRY_JITTER),
        'KAFKA_TOPIC': prefix + '.inventory.v1', 'KAFKA_DLQ_TOPIC': prefix + '.inventory.dlq.v1',
        'KAFKA_GROUP_PREFIX': prefix, 'KAFKA_SOURCE_CLUSTER_ID': 'validation.' + args.run_id,
        'KAFKA_SOURCE_STREAM_GENERATION': '1', 'KAFKA_SECURITY_PROTOCOL': 'SASL_SSL',
        'KAFKA_SASL_MECHANISM': 'SCRAM-SHA-256', 'KAFKA_SSL_CA_LOCATION': str(tls_directory / 'ca.crt'),
        'KAFKA_SASL_USERNAME': 'publisher', 'KAFKA_SASL_PASSWORD': passwords['publisher'],
        'KAFKA_ADMIN_USERNAME': 'admin', 'KAFKA_ADMIN_PASSWORD': passwords['admin'],
        'KAFKA_ADMIN_URL': 'https://127.0.0.1:19644',
        'KAFKA_ADMIN_TRUSTED_URLS': 'https://127.0.0.1:19644,https://127.0.0.1:29644,https://127.0.0.1:39644',
        'KAFKA_EXPORTER_PASSWORD': passwords['exporter'],
        'METRICS_TOKEN': secrets.token_urlsafe(32), 'REDIS_URL': '', 'OTEL_EXPORTER_OTLP_ENDPOINT': ''}
    (output / 'client.env').write_text(''.join(f'{key}={value}\n' for key, value in values.items()))
    metrics_directory = output / 'metrics'
    metrics_directory.mkdir(mode=0o755)
    metrics_directory.chmod(0o755)
    (metrics_directory / 'targets.json').write_text('[]\n')
    (metrics_directory / 'targets.json').chmod(0o644)
    (output / 'metrics-token').write_text(values['METRICS_TOKEN'])
    # The token is owner-private through generated/ on the host, and mounted as
    # one read-only file for the unprivileged Prometheus UID in its container.
    (output / 'metrics-token').chmod(0o644)
    prometheus = {'global': {'scrape_interval': '5s', 'evaluation_interval': '5s'},
        'rule_files': ['/etc/prometheus/alerts.yml'],
        'scrape_configs': [
            {'job_name': 'redpanda', 'scheme': 'https', 'metrics_path': '/public_metrics',
             'tls_config': {'ca_file': '/etc/labops-ca/ca.crt'},
             'static_configs': [{'targets': ['127.0.0.1:19644', '127.0.0.1:29644', '127.0.0.1:39644']}]},
            {'job_name': 'kafka-exporter', 'static_configs': [{'targets': ['127.0.0.1:19308']}]},
            {'job_name': 'labops-events-workers', 'metrics_path': '/metrics',
             'authorization': {'credentials_file': '/etc/labops-runtime/metrics-token'},
             'file_sd_configs': [{'files': ['/etc/labops-targets/targets.json'], 'refresh_interval': '5s'}]}]}
    (output / 'prometheus.yaml').write_text(json.dumps(prometheus, indent=2) + '\n')
    # Brokers read only their own key/cert and the public CA through individual
    # file mounts. The generated directory itself remains owner-only on the host.
    for path in output.iterdir():
        if path.is_file():
            path.chmod(0o644 if path.suffix == '.yaml' or path.name == 'metrics-token' else 0o600)
    for path in tls_directory.iterdir():
        path.chmod(0o644 if path.suffix == '.crt' or path.name.endswith('.key') and path.name.startswith('redpanda-') else 0o600)
    print(json.dumps({'run_id': args.run_id, 'image': IMAGE, 'generated': str(output), 'credentials': 'excluded from Git and evidence', 'cluster_scope': 'three processes on one Docker host'}))


if __name__ == '__main__':
    main()
