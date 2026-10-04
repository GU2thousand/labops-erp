"""Validated librdkafka allowlist. Secrets are never included in errors or logs."""
import math
import re
from pathlib import Path

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

ROLES = {'publisher', 'notification', 'analytics', 'dlq', 'admin', 'exporter', 'replay'}
PROTOCOLS = {'PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'}
MECHANISMS = {'SCRAM-SHA-256', 'SCRAM-SHA-512', 'PLAIN'}
IDENTIFIER = re.compile(r'^[A-Za-z0-9_.-]{1,96}$')
COMMON_ALLOWLIST = frozenset({
    'bootstrap.servers', 'security.protocol', 'ssl.ca.location',
    'enable.ssl.certificate.verification', 'ssl.endpoint.identification.algorithm',
    'sasl.mechanism', 'sasl.username', 'sasl.password', 'allow.auto.create.topics',
    'socket.timeout.ms',
})
PRODUCER_ALLOWLIST = COMMON_ALLOWLIST | {
    'enable.idempotence', 'acks', 'delivery.timeout.ms', 'request.timeout.ms',
    'retries', 'retry.backoff.ms', 'queue.buffering.max.messages',
    'queue.buffering.max.kbytes', 'max.in.flight.requests.per.connection',
    'message.max.bytes',
}
CONSUMER_ALLOWLIST = COMMON_ALLOWLIST | {
    'group.id', 'enable.auto.commit', 'enable.auto.offset.store', 'auto.offset.reset',
    'max.poll.interval.ms', 'session.timeout.ms', 'heartbeat.interval.ms',
    'isolation.level', 'check.crcs',
}


def _error(message):
    raise ImproperlyConfigured(message)


def read_secret_file(path, setting):
    try:
        value = Path(path).read_text().rstrip('\r\n')
    except (OSError, UnicodeError):
        _error(f'{setting} must reference a readable UTF-8 secret file')
    if not value or '\n' in value or '\r' in value or '\x00' in value:
        _error(f'{setting} must contain one nonempty secret')
    return value


def _identifier(value, setting):
    value = str(value)
    if not IDENTIFIER.fullmatch(value):
        _error(f'{setting} must contain 1-96 letters, numbers, dots, underscores or hyphens')
    return value


def source_identity():
    return (_identifier(settings.KAFKA_SOURCE_CLUSTER_ID, 'KAFKA_SOURCE_CLUSTER_ID'),
            _identifier(settings.KAFKA_SOURCE_STREAM_GENERATION, 'KAFKA_SOURCE_STREAM_GENERATION'))


def source_key(topic, partition, offset):
    cluster, generation = source_identity()
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,249}', topic) or partition < 0 or offset < 0:
        _error('Invalid Kafka source coordinates')
    return f'{cluster}:{generation}:{topic}:{partition}:{offset}'


def consumer_group(name):
    if name not in {'notification', 'analytics'}:
        _error('Unknown Kafka consumer identity')
    prefix = _identifier(settings.KAFKA_GROUP_PREFIX, 'KAFKA_GROUP_PREFIX')
    return f'{prefix}.{name}.v1'


def _positive(name, allow_zero=False):
    value = getattr(settings, name)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
        _error(f'{name} must be positive' if not allow_zero else f'{name} must not be negative')
    return value


def validate_runtime():
    source_identity()
    _identifier(settings.KAFKA_GROUP_PREFIX, 'KAFKA_GROUP_PREFIX')
    for name in ('KAFKA_DELIVERY_TIMEOUT_MS', 'KAFKA_REQUEST_TIMEOUT_MS',
                 'KAFKA_PRODUCER_RETRIES', 'KAFKA_RETRY_BACKOFF_MS',
                 'KAFKA_QUEUE_MAX_MESSAGES', 'KAFKA_QUEUE_MAX_KBYTES',
                 'KAFKA_PUBLISH_FLUSH_SECONDS', 'EVENT_PUBLISH_DB_BUDGET_SECONDS',
                 'EVENT_LEASE_SECONDS', 'KAFKA_MAX_POLL_INTERVAL_MS',
                 'KAFKA_SESSION_TIMEOUT_MS', 'KAFKA_HEARTBEAT_INTERVAL_MS',
                 'KAFKA_SOCKET_TIMEOUT_MS', 'KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS',
                 'EVENT_DB_LOCK_TIMEOUT_MS', 'WORKER_SHUTDOWN_TIMEOUT_SECONDS'):
        _positive(name)
    _positive('KAFKA_PRODUCER_QUEUE_WAIT_SECONDS', allow_zero=True)
    if settings.KAFKA_REQUEST_TIMEOUT_MS > settings.KAFKA_DELIVERY_TIMEOUT_MS:
        _error('KAFKA_REQUEST_TIMEOUT_MS must not exceed KAFKA_DELIVERY_TIMEOUT_MS')
    if settings.KAFKA_PUBLISH_FLUSH_SECONDS < settings.KAFKA_DELIVERY_TIMEOUT_MS / 1000:
        _error('KAFKA_PUBLISH_FLUSH_SECONDS must cover KAFKA_DELIVERY_TIMEOUT_MS')
    budget = (settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS + settings.KAFKA_PUBLISH_FLUSH_SECONDS
              + settings.EVENT_PUBLISH_DB_BUDGET_SECONDS)
    if settings.EVENT_LEASE_SECONDS <= budget:
        _error('EVENT_LEASE_SECONDS must exceed queue wait + publish flush + database write budget')
    if settings.WORKER_SHUTDOWN_TIMEOUT_SECONDS < settings.KAFKA_PUBLISH_FLUSH_SECONDS:
        _error('WORKER_SHUTDOWN_TIMEOUT_SECONDS must cover the publish flush budget')
    if settings.WORKER_SHUTDOWN_TIMEOUT_SECONDS < settings.KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS + settings.KAFKA_SOCKET_TIMEOUT_MS / 1000 + 1:
        _error('WORKER_SHUTDOWN_TIMEOUT_SECONDS must cover consumer processing + commit + close safety budget')
    poll_budget = settings.KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS * 1000 + settings.KAFKA_SOCKET_TIMEOUT_MS + 6000
    if settings.KAFKA_MAX_POLL_INTERVAL_MS <= poll_budget:
        _error('KAFKA_MAX_POLL_INTERVAL_MS must exceed processing + commit + polling safety budget')
    if settings.KAFKA_HEARTBEAT_INTERVAL_MS * 3 > settings.KAFKA_SESSION_TIMEOUT_MS:
        _error('KAFKA_HEARTBEAT_INTERVAL_MS must not exceed one third of session timeout')
    if not settings.EVENT_RETRY_SECONDS or any(isinstance(x, bool) or not isinstance(x, int) or x < 1 for x in settings.EVENT_RETRY_SECONDS):
        _error('EVENT_RETRY_SECONDS must contain positive backoff delays')
    if not math.isfinite(settings.EVENT_RETRY_JITTER) or not 0 <= settings.EVENT_RETRY_JITTER <= 1:
        _error('EVENT_RETRY_JITTER must be between zero and one')
    for name in ('EVENT_RETRY_LOCK_TIMEOUT_MS', 'EVENT_RETRY_STATEMENT_TIMEOUT_MS',
                 'EVENT_MAX_PAYLOAD_BYTES', 'KAFKA_MESSAGE_MAX_BYTES', 'KAFKA_DLQ_MESSAGE_MAX_BYTES'):
        _positive(name)
    if settings.EVENT_MAX_PAYLOAD_BYTES >= settings.KAFKA_MESSAGE_MAX_BYTES:
        _error('EVENT_MAX_PAYLOAD_BYTES must be below KAFKA_MESSAGE_MAX_BYTES')
    if settings.KAFKA_DLQ_MESSAGE_MAX_BYTES <= settings.KAFKA_MESSAGE_MAX_BYTES * 4 / 3 + 8192:
        _error('KAFKA_DLQ_MESSAGE_MAX_BYTES must cover base64 raw poison and envelope overhead')
    if settings.EVENT_RETRY_STATEMENT_TIMEOUT_MS > settings.KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS * 1000:
        _error('EVENT_RETRY_STATEMENT_TIMEOUT_MS must not exceed consumer processing budget')


def common_config(role):
    if role not in ROLES:
        _error('Unknown Kafka client identity')
    validate_runtime()
    servers = settings.KAFKA_BOOTSTRAP_SERVERS
    if not isinstance(servers, str) or not servers or any(
            not re.fullmatch(r'(?:[A-Za-z0-9_.-]+|\[[0-9a-fA-F:]+\]):[0-9]{1,5}', entry.strip())
            or not 1 <= int(entry.rsplit(':', 1)[1]) <= 65535 for entry in servers.split(',')):
        _error('KAFKA_BOOTSTRAP_SERVERS must be comma-separated host:port addresses')
    protocol = settings.KAFKA_SECURITY_PROTOCOL.upper()
    if protocol not in PROTOCOLS:
        _error('Unsupported KAFKA_SECURITY_PROTOCOL')
    if settings.KAFKA_REQUIRE_SECURITY:
        if protocol != 'SASL_SSL':
            _error('KAFKA_REQUIRE_SECURITY requires SASL_SSL')
        if settings.KAFKA_SOURCE_CLUSTER_ID == 'dev-local':
            _error('Secure Kafka requires an explicit non-development source cluster identity')
    config = {'bootstrap.servers': servers, 'security.protocol': protocol,
              'allow.auto.create.topics': False, 'socket.timeout.ms': settings.KAFKA_SOCKET_TIMEOUT_MS}
    if protocol in {'SSL', 'SASL_SSL'}:
        ca = settings.KAFKA_SSL_CA_LOCATION
        if not ca or not Path(ca).is_file():
            _error('KAFKA_SSL_CA_LOCATION must reference a trusted CA file')
        config.update({'ssl.ca.location': ca, 'enable.ssl.certificate.verification': True,
                       'ssl.endpoint.identification.algorithm': 'https'})
    if protocol.startswith('SASL'):
        mechanism = settings.KAFKA_SASL_MECHANISM.upper()
        if mechanism not in MECHANISMS:
            _error('Unsupported KAFKA_SASL_MECHANISM')
        username = getattr(settings, f'KAFKA_{role.upper()}_SASL_USERNAME', '') or settings.KAFKA_SASL_USERNAME
        role_file = getattr(settings, f'KAFKA_{role.upper()}_SASL_PASSWORD_FILE', '')
        role_password = getattr(settings, f'KAFKA_{role.upper()}_SASL_PASSWORD', '')
        if settings.KAFKA_REQUIRE_SECURITY and (not getattr(settings, f'KAFKA_{role.upper()}_SASL_USERNAME', '') or not role_file):
            _error('Secure Kafka requires an explicit role username and password secret file')
        secret_file = role_file or (settings.KAFKA_SASL_PASSWORD_FILE if not role_password else '')
        password = read_secret_file(secret_file, 'Kafka password file') if secret_file else role_password or settings.KAFKA_SASL_PASSWORD
        if not username or not password:
            _error('SASL requires a username and secret for the selected Kafka identity')
        config.update({'sasl.mechanism': mechanism, 'sasl.username': username, 'sasl.password': password})
    return config


def producer_config(role='publisher'):
    config = common_config(role)
    config.update({'enable.idempotence': True, 'acks': 'all',
                   'delivery.timeout.ms': settings.KAFKA_DELIVERY_TIMEOUT_MS,
                   'request.timeout.ms': settings.KAFKA_REQUEST_TIMEOUT_MS,
                   'retries': settings.KAFKA_PRODUCER_RETRIES,
                   'retry.backoff.ms': settings.KAFKA_RETRY_BACKOFF_MS,
                   'queue.buffering.max.messages': settings.KAFKA_QUEUE_MAX_MESSAGES,
                   'queue.buffering.max.kbytes': settings.KAFKA_QUEUE_MAX_KBYTES,
                   'message.max.bytes': settings.KAFKA_DLQ_MESSAGE_MAX_BYTES if role == 'dlq' else settings.KAFKA_MESSAGE_MAX_BYTES,
                   'max.in.flight.requests.per.connection': 5})
    assert config.keys() <= PRODUCER_ALLOWLIST
    return config


def consumer_config(name):
    config = common_config(name)
    config.update({'group.id': consumer_group(name), 'enable.auto.commit': False,
                   'enable.auto.offset.store': False, 'auto.offset.reset': 'earliest',
                   'max.poll.interval.ms': settings.KAFKA_MAX_POLL_INTERVAL_MS,
                   'session.timeout.ms': settings.KAFKA_SESSION_TIMEOUT_MS,
                   'heartbeat.interval.ms': settings.KAFKA_HEARTBEAT_INTERVAL_MS,
                   'isolation.level': 'read_committed', 'check.crcs': True})
    assert config.keys() <= CONSUMER_ALLOWLIST
    return config
