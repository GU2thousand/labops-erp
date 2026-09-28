"""Credential-safe negative probes for the isolated SASL/TLS acceptance cluster.

``run_security_probes(configs, topic, run_id, evidence_dir)`` accepts ordinary
librdkafka dictionaries under ``publisher``, ``notification``, and ``admin``.
Optional top-level settings are ``ca_path``, ``probe_timeout`` (seconds), and
``broker_log_reader(case_name, start_utc, end_utc)``. The latter returns broker
log text captured only within that probe's time window, for protocols where the
client sees a disconnect while the server records the authentication rejection.
The caller owns the legitimate topic and grants notification access to
``labops.<run_id>.notification.v1``. Never run this against a production topic:
an ACL regression may allow the deliberately forbidden write/create operation.

No connection configuration is included in the report. A timeout or an arbitrary
transport failure is inconclusive, rather than evidence of a security denial.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import re
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


# Public test certificate only; its private key is deliberately not retained.
# Using a valid, unrelated CA tests server-certificate verification at handshake
# time, rather than passing because the client could not parse a broken PEM.
_UNTRUSTED_CA = """-----BEGIN CERTIFICATE-----
MIIDRzCCAi+gAwIBAgIUD70PBVmgNogKFX94l1GxGD7jpTowDQYJKoZIhvcNAQEL
BQAwMjEwMC4GA1UEAwwnTGFiT3BzIEludGVudGlvbmFsbHkgVW50cnVzdGVkIFBy
b2JlIENBMCAXDTI2MDkyODAxMTg1NFoYDzIxMjYwOTA0MDExODU0WjAyMTAwLgYD
VQQDDCdMYWJPcHMgSW50ZW50aW9uYWxseSBVbnRydXN0ZWQgUHJvYmUgQ0EwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC1x/95ldIn0r27UbOJUbw0EILN
ERKEgAw/OUvwPwKehbp25cXGeYfApFksxmtQIJ59aIigq68ijkR2wPGvCKY/54yD
GOU43Qy2sE30JJ/5JwgMrITb/rB5bnjVelcMfZ3lBxzO8fM7ZgaIuRw/NW10kWHV
e3SxSlFGoYSt5gePrQ41hXMCFOZqD0pYZV/SmFE//Jept5xyAlYQf7eGVHx++1XQ
b4DGvNrEeZ4CEbhlZDP7BfLPh5WZdXzVz+nKJ1CoewVliOKPPCtIFGoomYM45gaQ
DJVvA5Q384rbpuiWoOlQc+o1VjFvbmH5bqFVbYsO5a7aURC+ZM9m8NUiZZovAgMB
AAGjUzBRMB0GA1UdDgQWBBTGqQK7WXzKtpl60KTvEvjK3RXfpjAfBgNVHSMEGDAW
gBTGqQK7WXzKtpl60KTvEvjK3RXfpjAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3
DQEBCwUAA4IBAQCRKyBH4uiFOwm0rweus4ZeUbKEgVF7KRSQgxpme39OuHnu0HFc
Lqgtr5+vFUoeCKORs9FWZLQLqhSB5FpVZE9zPAZ4qAkMWrs0eEQ3Zr4ELzOUr5Vr
MfKK5EJxrcPSzzIUc5/61lIoTIjXulEOVQkzbEHAcqi7UTJuVFt1fRw+Y/CsTZ62
Wv7YC/bwfclCNvcFz6soGceGzVtAGvd4r/KLeFmuN+8idWhJx3eGVkjI/2+9r8qX
3FNToE+wGqB1vdq3U+8UlG/1Puy2xHXm7Mynz8wHA2hzNJwhry4FHjnxsg6mqc/+
JtLN7YxVwkQQF5SIEsHpo21YWaIH/2m1fewo
-----END CERTIFICATE-----
"""

_AUTHENTICATION_CODES = {
    KafkaError._AUTHENTICATION, KafkaError.SASL_AUTHENTICATION_FAILED,
    KafkaError.ILLEGAL_SASL_STATE, KafkaError.UNSUPPORTED_SASL_MECHANISM,
}
_AUTHORIZATION_CODES = {
    KafkaError.TOPIC_AUTHORIZATION_FAILED, KafkaError.GROUP_AUTHORIZATION_FAILED,
    KafkaError.CLUSTER_AUTHORIZATION_FAILED,
    KafkaError.TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
}
_CONNECTION_KEYS = {
    "bootstrap.servers", "security.protocol", "client.id", "api.version.request",
    "broker.version.fallback", "enable.ssl.certificate.verification",
}


def _category(code, message):
    if code in _AUTHENTICATION_CODES:
        return "authentication"
    if code == KafkaError._SSL:
        return "ssl"
    if code in _AUTHORIZATION_CODES:
        return "authorization"
    # Some clients wrap explicit authentication errors in _TRANSPORT. Their
    # speculative "broker might require SASL authentication" disconnect hint
    # does not establish why a connection was closed and must never pass.
    if code == KafkaError._TRANSPORT and re.search(
        r"SASL authentication failed|authentication failed|illegal SASL state",
        message, re.IGNORECASE,
    ) and not re.search(r"might|possibly|may (?:be|require)", message, re.IGNORECASE):
        return "authentication_protocol_rejection"
    return "other"


class _Observations:
    def __init__(self, secrets):
        self.secrets = sorted(set(secrets), key=len, reverse=True)
        self.items = []

    def redact(self, value):
        value = str(value)
        for secret in self.secrets:
            if secret:
                value = value.replace(secret, "[redacted]")
        value = re.sub(
            r"(?i)((?:sasl[._])?(?:password|username)\s*[=:]\s*)[^\s,;]+",
            r"\1[redacted]", value,
        )
        return value[:1200]

    def error(self, error, source="error_callback"):
        if isinstance(error, KafkaException) and error.args:
            error = error.args[0]
        code = error.code() if isinstance(error, KafkaError) else None
        message = str(error)
        item = {"source": source, "error_code": code,
                "error_name": error.name() if isinstance(error, KafkaError) else type(error).__name__,
                "category": _category(code, message), "message": self.redact(message)}
        if item not in self.items:
            self.items.append(item)

    def note(self, source, message):
        self.items.append({"source": source, "category": "observation",
                           "message": self.redact(message)})


def _connection_config(raw, ca_path, observations):
    config = {key: value for key, value in raw.items()
              if key in _CONNECTION_KEYS or key.startswith(("sasl.", "ssl.", "socket."))}
    if ca_path:
        config["ssl.ca.location"] = str(ca_path)
    config.update({"error_cb": observations.error, "log_level": 0,
                   "enable.ssl.certificate.verification": True,
                   "ssl.endpoint.identification.algorithm": "https"})
    return config


def _metadata(config, topic, observations, timeout):
    client = None
    try:
        client = AdminClient(config)
        metadata = client.list_topics(topic=topic, timeout=timeout)
        entry = metadata.topics.get(topic)
        if entry is None:
            observations.note("metadata", "Legitimate topic was absent from metadata")
            return False
        if entry.error is not None:
            observations.error(entry.error, "topic_metadata")
            return False
        return bool(entry.partitions)
    except Exception as exc:
        observations.error(exc, "metadata_exception")
        return False
    finally:
        if client is not None:
            # error_cb is served by poll, even when metadata itself timed out.
            try:
                client.poll(0.2)
            except Exception as exc:
                observations.error(exc, "metadata_poll")


def _group_access(config, topic, group, observations, timeout):
    client = None
    assigned = []
    try:
        config = {**config, "group.id": group, "enable.auto.commit": False,
                  "enable.auto.offset.store": False, "auto.offset.reset": "earliest",
                  "allow.auto.create.topics": False,
                  "session.timeout.ms": 6000, "heartbeat.interval.ms": 2000}
        client = Consumer(config)
        client.subscribe([topic], on_assign=lambda _, partitions: assigned.extend(partitions))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = client.poll(min(0.5, max(0, deadline - time.monotonic())))
            if message is not None and message.error():
                observations.error(message.error(), "consumer_poll")
            if any(item["category"] in {"authorization", "authentication", "ssl"}
                   for item in observations.items):
                return False
            if assigned:
                observations.note("consumer_assignment", "Group received a partition assignment")
                return True
        observations.note("probe_deadline", "No partition assignment before probe deadline")
        return False
    except Exception as exc:
        observations.error(exc, "consumer_exception")
        return False
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                observations.error(exc, "consumer_close")


def _forbidden_write(config, topic, run_id, observations, timeout):
    delivered = []
    client = None

    def delivery(error, message):
        if error is not None:
            observations.error(error, "delivery_callback")
        else:
            delivered.append({"partition": message.partition(), "offset": message.offset()})

    try:
        # Do not enable idempotence for the read-only principal: the probe must
        # exercise topic WRITE, rather than fail on an unrelated cluster ACL.
        config = {**config, "enable.idempotence": False, "acks": "all",
                  "delivery.timeout.ms": int(timeout * 1000),
                  "request.timeout.ms": min(5000, int(timeout * 1000)),
                  "allow.auto.create.topics": False}
        client = Producer(config)
        client.produce(topic, partition=0, key="security-probe",
                       value=json.dumps({"security_probe": "forbidden_write", "run_id": run_id}),
                       on_delivery=delivery)
        remaining = client.flush(timeout + 1)
        if remaining:
            observations.note("producer_flush", "Message remained unacknowledged at probe deadline")
        for coordinates in delivered:
            observations.note("unexpected_delivery", json.dumps(coordinates, sort_keys=True))
        return bool(delivered)
    except Exception as exc:
        observations.error(exc, "producer_exception")
        return False


def _forbidden_create(config, admin_config, topic, observations, timeout):
    created = False
    try:
        client = AdminClient(config)
        future = client.create_topics([NewTopic(topic, num_partitions=1, replication_factor=3)],
                                      request_timeout=timeout, operation_timeout=timeout)[topic]
        try:
            future.result(timeout + 1)
            created = True
            observations.note("unexpected_topic_creation", "Application principal created the forbidden topic")
        finally:
            client.poll(0.2)
    except Exception as exc:
        observations.error(exc, "create_topic_exception")
    if created:
        # Only a fresh topic whose successful creation this invocation observed.
        try:
            admin = AdminClient(admin_config)
            admin.delete_topics([topic], request_timeout=timeout)[topic].result(timeout + 1)
            observations.note("cleanup", "Removed the unexpectedly created disposable topic")
        except Exception as exc:
            observations.error(exc, "cleanup_exception")
    return created


def run_security_probes(configs, topic, run_id, evidence_dir):
    """Run all six probes, write security-probes.json, and return its dictionary.

    ``passed`` requires healthy controls and specific denial evidence for every
    case. ``denial_confirmed`` is false for constructor errors and bare timeouts.
    Authentication cases require authentication/SSL evidence; ACL cases require
    their specific authorization code, so incorrect credentials cannot pass ACL.
    The report intentionally contains neither usernames nor passwords/configs.
    """
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("run_id must be a short isolated resource identifier")
    if not isinstance(topic, str) or not topic:
        raise ValueError("A pre-created legitimate topic is required")
    roles = ("publisher", "notification", "admin")
    for role in roles:
        raw = configs.get(role)
        if not isinstance(raw, dict) or raw.get("security.protocol", "").upper() != "SASL_SSL":
            raise ValueError(f"{role} must supply a SASL_SSL librdkafka configuration")
        if not raw.get("sasl.username") or not raw.get("sasl.password"):
            raise ValueError(f"{role} must supply its own SASL credentials")
    timeout = max(4.0, min(60.0, float(configs.get("probe_timeout", 12))))
    ca_path = configs.get("ca_path")
    secrets = [str(value) for role in roles for key, value in configs[role].items()
               if any(part in key.lower() for part in ("password", "username", "key.pem")) and value]
    directory = Path(evidence_dir)
    directory.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    report = {"test": "kafka_security_negative_probes", "run_id": run_id,
              "denial_rule": "Specific authentication, SSL, or required authorization evidence; timeout alone fails",
              "controls": [], "cases": []}

    control_observations = _Observations(secrets)
    admin_config = _connection_config(configs["admin"], ca_path, control_observations)
    admin_ok = _metadata(admin_config, topic, control_observations, timeout)
    report["controls"].append({"name": "admin_legitimate_topic", "passed": admin_ok,
                               "observations": control_observations.items})
    control_observations = _Observations(secrets)
    notification_config = _connection_config(configs["notification"], ca_path, control_observations)
    group_ok = _group_access(notification_config, topic, f"labops.{run_id}.notification.v1",
                             control_observations, timeout)
    report["controls"].append({"name": "notification_permitted_group", "passed": group_ok,
                               "observations": control_observations.items})

    def case(name, expected, action):
        observations = _Observations(secrets)
        case_started = time.monotonic()
        unexpected_success = bool(action(observations))
        matched = [item for item in observations.items if (
            item.get("error_code") in expected if isinstance(expected, set)
            else item["category"] in expected)]
        report["cases"].append({"name": name, "denial_confirmed": bool(matched) and not unexpected_success,
                                "unexpected_success": unexpected_success,
                                "duration_seconds": round(time.monotonic() - case_started, 3),
                                "observations": observations.items})

    def omitted_credentials(observations):
        config = _connection_config(configs["publisher"], ca_path, observations)
        for key in list(config):
            if key.startswith("sasl."):
                config.pop(key)
        config["security.protocol"] = "SSL"
        observations.note("probe_method", "TLS connection omitted SASL on the SASL-required listener")
        window_start = datetime.now(timezone.utc).isoformat()
        accepted = _metadata(config, topic, observations, timeout)
        window_end = datetime.now(timezone.utc).isoformat()
        read_logs = configs.get("broker_log_reader")
        if callable(read_logs):
            try:
                # The caller must return logs scoped to this invocation's
                # cluster and time window, never historical authentication
                # failures. Only definitive server diagnostics are retained.
                logs = read_logs("anonymous", window_start, window_end)
                if not isinstance(logs, str):
                    raise TypeError("broker_log_reader must return scoped log text")
                for line in logs.splitlines():
                    if re.search(
                        r"Unexpected request during authentication:\s*3\b|"
                        r"Failed authentication.*(?:METADATA|during SASL handshake)|"
                        r"illegal SASL state", line, re.IGNORECASE,
                    ):
                        observations.items.append({
                            "source": "broker_log", "error_code": None,
                            "error_name": "BROKER_AUTHENTICATION_REJECTION",
                            "category": "authentication_protocol_rejection",
                            "probe_window_start_utc": window_start,
                            "probe_window_end_utc": window_end,
                            "message": observations.redact(line),
                        })
            except Exception as exc:
                observations.error(exc, "broker_log_reader_exception")
        return accepted

    case("anonymous", ("authentication", "authentication_protocol_rejection"), omitted_credentials)

    wrong_password = "invalid-security-probe-" + uuid.uuid4().hex
    secrets.append(wrong_password)
    case("wrong_password", ("authentication",), lambda obs: _metadata(
        {**_connection_config(configs["publisher"], ca_path, obs), "sasl.password": wrong_password},
        topic, obs, timeout))

    wrong_ca = directory / "security-probe-untrusted-ca.crt"
    wrong_ca.write_text(_UNTRUSTED_CA)

    def untrusted_certificate(observations):
        config = _connection_config(configs["publisher"], wrong_ca, observations)
        config.pop("ssl.ca.pem", None)
        return _metadata(config, topic, observations, timeout)

    case("wrong_ca", ("ssl",), untrusted_certificate)
    case("forbidden_consumer_group", {KafkaError.GROUP_AUTHORIZATION_FAILED},
         lambda obs: _group_access(_connection_config(configs["notification"], ca_path, obs),
                                   topic, f"other.{run_id}", obs, timeout))
    case("forbidden_topic_write", {KafkaError.TOPIC_AUTHORIZATION_FAILED},
         lambda obs: _forbidden_write(_connection_config(configs["notification"], ca_path, obs),
                                      topic, run_id, obs, timeout))
    create_topic = f"other.{run_id}.security-create-{uuid.uuid4().hex[:10]}"
    case("forbidden_create_topic", {KafkaError.TOPIC_AUTHORIZATION_FAILED, KafkaError.CLUSTER_AUTHORIZATION_FAILED},
         lambda obs: _forbidden_create(_connection_config(configs["publisher"], ca_path, obs),
                                       _connection_config(configs["admin"], ca_path, obs),
                                       create_topic, obs, timeout))
    report.update(passed=all(control["passed"] for control in report["controls"])
                  and all(item["denial_confirmed"] for item in report["cases"]),
                  passed_cases=sum(item["denial_confirmed"] for item in report["cases"]),
                  total_cases=6, duration_seconds=round(time.monotonic() - started, 3))
    output = directory / "security-probes.json"
    temporary = output.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(output)
    return report
