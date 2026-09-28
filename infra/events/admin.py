#!/usr/bin/env python3
"""Provision users/least-privilege ACLs and create-and-verify declared topics.

This tool requires an administration identity. It never repairs existing topic
configuration, deletes ACLs, or resets group offsets. Reports contain no secrets.
See production/README.md and production/rf1-migration.md before a live change.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.parse import quote, urlparse

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (
    AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
    AdminClient, ConfigResource, NewTopic, ResourcePatternType, ResourceType,
)


class ReconcileError(RuntimeError):
    """A safety check failed or actual state differs from the declaration."""


NAME = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")
ROLES = ("publisher", "notification", "analytics", "dlq", "replay", "exporter")


def load_env(path: str | None) -> None:
    """Read plain KEY=value without shell evaluation; process env wins."""
    if not path:
        return
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise ReconcileError("Invalid env file entry (expected KEY=value)")
        if value[:1] in ("'", '"'):
            if len(value) < 2 or value[-1] != value[0]:
                raise ReconcileError("Invalid quoted env file value")
            value = value[1:-1]
        os.environ.setdefault(key, value)


def validated_name(value: str, label: str) -> str:
    if not NAME.fullmatch(value) or value in (".", ".."):
        raise ReconcileError(f"Invalid {label}; use a Kafka-safe resource name")
    return value


def client_config(args: argparse.Namespace) -> dict:
    protocol = os.environ.get("KAFKA_ADMIN_SECURITY_PROTOCOL",
                              os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")).upper()
    if protocol != "SASL_SSL" and not (args.development_rf1 and protocol == "PLAINTEXT"):
        raise ReconcileError("Admin requires SASL_SSL; PLAINTEXT is allowed only with --development-rf1")
    bootstrap = args.bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    if not bootstrap:
        raise ReconcileError("KAFKA_BOOTSTRAP_SERVERS is required")
    conf = {"bootstrap.servers": bootstrap, "security.protocol": protocol,
            "client.id": "labops-events-admin", "socket.timeout.ms": 10000}
    if protocol == "SASL_SSL":
        username = os.environ.get("KAFKA_ADMIN_USERNAME", "")
        password = os.environ.get("KAFKA_ADMIN_PASSWORD", "")
        mechanism = os.environ.get("KAFKA_ADMIN_SASL_MECHANISM",
                                   os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"))
        if not username or not password:
            raise ReconcileError("KAFKA_ADMIN_USERNAME and KAFKA_ADMIN_PASSWORD are required")
        if mechanism not in ("SCRAM-SHA-256", "SCRAM-SHA-512"):
            raise ReconcileError("Admin authentication must use SCRAM-SHA-256 or SCRAM-SHA-512")
        conf.update({"sasl.username": username, "sasl.password": password,
                     "sasl.mechanism": mechanism, "enable.ssl.certificate.verification": True,
                     "ssl.endpoint.identification.algorithm": "https"})
        ca = os.environ.get("KAFKA_ADMIN_SSL_CA_LOCATION",
                            os.environ.get("KAFKA_SSL_CA_LOCATION", ""))
        if ca:
            if not Path(ca).is_file():
                raise ReconcileError("Configured Kafka CA file does not exist")
            conf["ssl.ca.location"] = ca
    return conf


def declarations(args: argparse.Namespace) -> list[dict]:
    document = json.loads(Path(args.topics_file).read_text())
    if document.get("schema_version") != 1:
        raise ReconcileError("Unknown topic declaration schema")
    result = []
    for item in document["topics"]:
        result.append({**item, "name": validated_name(
            os.environ.get(item["name_env"], item["default_name"]), "topic"),
            "replication_factor": 1 if args.development_rf1 else item["replication_factor"]})
    if len({item["name"] for item in result}) != len(result):
        raise ReconcileError("Inventory and DLQ topics must have different names")
    if args.verify_offsets:
        result.extend({**item, "replication_factor": 1 if args.development_rf1 else item["replication_factor"]}
                      for item in document["internal_topics"])
    return result


def snapshot_topics(admin: AdminClient, wanted: list[dict]) -> tuple[dict, list[str]]:
    # Request all metadata: list_topics(topic=...) may trigger broker auto-creation.
    metadata = admin.list_topics(timeout=15)
    actual, missing = {}, []
    resources = []
    for spec in wanted:
        name = spec["name"]
        topic = metadata.topics.get(name)
        if topic is None or (topic.error and topic.error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART):
            missing.append(name)
            continue
        if topic.error:
            raise ReconcileError(f"Could not inspect topic {name}: {topic.error.name()}")
        partitions = []
        for number, part in sorted(topic.partitions.items()):
            if part.error:
                raise ReconcileError(f"Could not inspect {name}/{number}: {part.error.name()}")
            partitions.append({"partition": number, "leader": part.leader,
                               "replicas": sorted(part.replicas), "isr": sorted(part.isrs)})
        actual[name] = {"partitions": partitions, "config": {}}
        if spec.get("config"):
            resources.append(ConfigResource(ResourceType.TOPIC, name))
    if resources:
        for resource, future in admin.describe_configs(resources, request_timeout=15).items():
            values = future.result(timeout=20)
            spec = next(item for item in wanted if item["name"] == resource.name)
            actual[resource.name]["config"] = {
                key: {"value": values[key].value, "source": str(values[key].source)}
                if key in values else {"value": None, "source": "MISSING"}
                for key in spec["config"]}
    return actual, missing


def mismatches(wanted: list[dict], actual: dict, *, full_isr: bool = False) -> list[str]:
    errors = []
    for spec in wanted:
        name = spec["name"]
        if name not in actual:
            continue
        seen = actual[name]
        if "partitions" in spec and len(seen["partitions"]) != spec["partitions"]:
            errors.append(f"{name}: partition count differs from {spec['partitions']}")
        for part in seen["partitions"]:
            if len(part["replicas"]) != spec["replication_factor"] or len(set(part["replicas"])) != len(part["replicas"]):
                errors.append(f"{name}/{part['partition']}: replicas differ from RF{spec['replication_factor']}")
            if full_isr and (part["leader"] < 0 or part["isr"] != part["replicas"]):
                errors.append(f"{name}/{part['partition']}: incomplete ISR or no leader")
        for key, expected in spec.get("config", {}).items():
            if str(seen["config"].get(key, {}).get("value")) != str(expected):
                errors.append(f"{name}: {key} differs from declared {expected}")
    return errors


def topic_reconcile(args: argparse.Namespace, report: dict) -> None:
    admin = AdminClient(client_config(args))
    wanted = declarations(args)
    actual, missing = snapshot_topics(admin, wanted)
    report.update({"declared": wanted, "actual": actual, "created": []})
    errors = mismatches(wanted, actual, full_isr=args.require_full_isr)
    if errors:
        report["mismatches"] = errors
        raise ReconcileError("Existing topic mismatch; follow production/rf1-migration.md. No topics modified.")
    internal = [item["name"] for item in wanted if item.get("verify_only") and item["name"] in missing]
    if internal:
        raise ReconcileError("Internal offsets topic missing; start an authorized consumer and commit an offset, then verify again")
    if args.verify_only and missing:
        raise ReconcileError("Declared topics missing: " + ", ".join(missing))
    if missing:
        requests = [NewTopic(item["name"], num_partitions=item["partitions"],
                             replication_factor=item["replication_factor"], config=item["config"])
                    for item in wanted if item["name"] in missing]
        for name, future in admin.create_topics(requests, request_timeout=15, operation_timeout=15).items():
            try:
                future.result(timeout=20)
                report["created"].append(name)
            except KafkaException as exc:
                # A concurrent administrator may have created it. Actual state is
                # still validated below; a matching name is never sufficient.
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
    deadline = time.monotonic() + args.wait_seconds
    while True:
        actual, missing = snapshot_topics(admin, wanted)
        errors = mismatches(wanted, actual, full_isr=args.require_full_isr)
        report["actual"] = actual
        report["mismatches"] = errors + [f"{name}: missing" for name in missing]
        if not report["mismatches"]:
            break
        if time.monotonic() >= deadline:
            raise ReconcileError("Topic verification failed; inspect mismatches in the report")
        time.sleep(1)


def load_identities(path: str | None) -> dict[str, str]:
    if not path:
        raise ReconcileError("--identities secrets.json is required")
    file = Path(path)
    if file.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise ReconcileError("Identity secret file must be private (chmod 600)")
    data = json.loads(file.read_text())
    if not isinstance(data, dict) or not all(isinstance(k, str) and isinstance(v, str) and v for k, v in data.items()):
        raise ReconcileError("Identity secret file must map usernames to nonempty passwords")
    for role in ("admin", *ROLES):
        if role not in data:
            raise ReconcileError(f"Identity secret file missing {role}")
    for username in data:
        validated_name(username, "username")
    return data


def acl_key(acl: AclBinding) -> tuple:
    return (acl.restype.name, acl.name, acl.resource_pattern_type.name,
            acl.principal, acl.host, acl.operation.name, acl.permission_type.name)


def acl_document(acl: AclBinding) -> dict:
    values = acl_key(acl)
    return dict(zip(("resource_type", "name", "pattern", "principal", "host", "operation", "permission"), values))


def expected_acls(args: argparse.Namespace) -> list[AclBinding]:
    topics = declarations(args)
    inventory, dlq = topics[0]["name"], topics[1]["name"]
    # Preserve the configured bytes exactly, matching kafka_config.consumer_group.
    prefix = os.environ.get("KAFKA_GROUP_PREFIX", "labops")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", prefix):
        raise ReconcileError("Group prefix must contain 1-96 letters, numbers, dots, underscores or hyphens")
    bindings = []

    def allow(role: str, resource: ResourceType, name: str, operations: tuple,
              pattern: ResourcePatternType = ResourcePatternType.LITERAL) -> None:
        for operation in operations:
            bindings.append(AclBinding(resource, name, pattern, "User:" + role,
                                       "*", operation, AclPermissionType.ALLOW))

    for role, topic in (("publisher", inventory), ("dlq", dlq)):
        allow(role, ResourceType.TOPIC, topic, (AclOperation.WRITE, AclOperation.DESCRIBE))
        # ResourceType.BROKER (Kafka resource type 4) represents CLUSTER ACLs;
        # the protocol-defined cluster name is kafka-cluster, not a broker ID.
        allow(role, ResourceType.BROKER, "kafka-cluster", (AclOperation.IDEMPOTENT_WRITE,))
    for role in ("notification", "analytics"):
        allow(role, ResourceType.TOPIC, inventory, (AclOperation.READ, AclOperation.DESCRIBE))
        allow(role, ResourceType.GROUP, prefix + "." + role + ".v1",
              (AclOperation.READ, AclOperation.DESCRIBE))
    allow("replay", ResourceType.TOPIC, inventory, (AclOperation.READ, AclOperation.DESCRIBE))
    allow("replay", ResourceType.GROUP, prefix + ".replay.",
          (AclOperation.READ, AclOperation.DESCRIBE), ResourcePatternType.PREFIXED)
    for topic in (inventory, dlq):
        allow("exporter", ResourceType.TOPIC, topic, (AclOperation.DESCRIBE,))
    allow("exporter", ResourceType.GROUP, prefix + ".", (AclOperation.DESCRIBE,), ResourcePatternType.PREFIXED)
    allow("exporter", ResourceType.BROKER, "kafka-cluster", (AclOperation.DESCRIBE,))
    return bindings


def read_application_acls(admin: AdminClient) -> list[AclBinding]:
    result = []
    for role in ROLES:
        selector = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                                    "User:" + role, None, AclOperation.ANY, AclPermissionType.ANY)
        result.extend(admin.describe_acls(selector, request_timeout=15).result(timeout=20))
    # Wildcard principal grants also apply to each application identity. Refuse
    # them rather than claiming that per-principal ACLs alone prove least privilege.
    selector = AclBindingFilter(ResourceType.ANY, None, ResourcePatternType.ANY,
                                "User:*", None, AclOperation.ANY, AclPermissionType.ANY)
    result.extend(admin.describe_acls(selector, request_timeout=15).result(timeout=20))
    return result


def acl_reconcile(args: argparse.Namespace, report: dict) -> None:
    load_identities(args.identities)
    admin = AdminClient(client_config(args))
    wanted = expected_acls(args)
    expected = {acl_key(acl) for acl in wanted}
    actual = read_application_acls(admin)
    observed = {acl_key(acl) for acl in actual}
    extras = observed - expected
    report.update({"declared": [acl_document(acl) for acl in wanted],
                   "actual": [acl_document(acl) for acl in actual], "created": [],
                   "unexpected": [dict(zip(("resource_type", "name", "pattern", "principal", "host", "operation", "permission"), key))
                                  for key in sorted(extras)]})
    if extras:
        raise ReconcileError("Unexpected application or wildcard-principal ACLs; review/remove explicitly before reconcile")
    missing = [acl for acl in wanted if acl_key(acl) not in observed]
    if args.verify_only and missing:
        raise ReconcileError("Required application ACLs are missing")
    if missing:
        for acl, future in admin.create_acls(missing, request_timeout=15).items():
            future.result(timeout=20)
            report["created"].append(acl_document(acl))
    actual = read_application_acls(admin)
    report["actual"] = [acl_document(acl) for acl in actual]
    if {acl_key(acl) for acl in actual} != expected:
        raise ReconcileError("ACL verification failed after provision")


def users_reconcile(args: argparse.Namespace, report: dict) -> None:
    import requests

    users = load_identities(args.identities)
    # Validate shared TLS/SCRAM configuration even though user creation uses HTTP.
    config = client_config(args)
    url = os.environ.get("KAFKA_ADMIN_URL", "").rstrip("/")
    parsed = urlparse(url)
    if (parsed.scheme != "https" or parsed.username or parsed.password or not parsed.hostname
            or parsed.query or parsed.fragment):
        raise ReconcileError("KAFKA_ADMIN_URL must be HTTPS without embedded credentials")
    session = requests.Session()
    session.auth = (config["sasl.username"], config["sasl.password"])
    session.verify = config.get("ssl.ca.location", True)
    session.headers["Content-Type"] = "application/json"
    response = session.get(url + "/v1/security/users", timeout=15, allow_redirects=False)
    if not 200 <= response.status_code < 300:
        raise ReconcileError(f"Admin user listing failed with HTTP {response.status_code}")
    existing = response.json()
    if not isinstance(existing, list) or not all(isinstance(user, str) for user in existing):
        raise ReconcileError("Unexpected Admin API user list response")
    report.update({"created": [], "existing": [], "rotated": []})
    # Rotate the connected administrator last so its old Basic credentials do
    # not invalidate the remaining provisioning operations mid-run.
    for username in sorted(users, key=lambda name: (name == config["sasl.username"], name)):
        password = users[username]
        exists = username in existing
        if exists and not args.rotate_existing:
            report["existing"].append(username)
            continue
        if args.verify_only:
            if not exists:
                raise ReconcileError(f"Required user missing: {username}")
            continue
        body = {"password": password, "algorithm": config["sasl.mechanism"]}
        if exists:
            response = session.put(url + "/v1/security/users/" + quote(username, safe=""), json=body,
                                   timeout=15, allow_redirects=False)
        else:
            response = session.post(url + "/v1/security/users", json={"username": username, **body},
                                    timeout=15, allow_redirects=False)
        if not 200 <= response.status_code < 300:
            # Never log the response body: upstream error responses can echo input.
            raise ReconcileError(f"Admin user operation failed for {username}: HTTP {response.status_code}")
        report["rotated" if exists else "created"].append(username)
        if username == config["sasl.username"]:
            session.auth = (username, password)
    final = session.get(url + "/v1/security/users", timeout=15, allow_redirects=False)
    if not 200 <= final.status_code < 300 or not set(users).issubset(set(final.json())):
        raise ReconcileError("Admin API user existence verification failed")
    report["credentials_verified"] = False
    report["note"] = "Existence verification only; execute positive/negative client authentication checks separately."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("topics", "acls", "users"):
        sub = commands.add_parser(command)
        sub.add_argument("--env-file")
        sub.add_argument("--bootstrap", help="Kafka bootstrap addresses; credentials are read only from environment")
        sub.add_argument("--topics-file", default=str(Path(__file__).with_name("topics.json")))
        sub.add_argument("--identities", help="Private JSON mapping identity names to passwords")
        sub.add_argument("--report", help="JSON report path; contains declarations and observed state, never secrets")
        sub.add_argument("--verify-only", action="store_true")
        sub.add_argument("--development-rf1", action="store_true", help="Explicit single-broker development mode; never production")
        sub.add_argument("--verify-offsets", action="store_true", help="Require broker-created __consumer_offsets RF to match")
        sub.add_argument("--require-full-isr", action="store_true", help="Require every declared partition to have a leader and all replicas in ISR")
        sub.add_argument("--wait-seconds", type=int, default=30)
        sub.add_argument("--rotate-existing", action="store_true", help="Explicitly rotate existing SCRAM passwords (users only)")
    args = parser.parse_args()
    if args.wait_seconds < 0 or args.wait_seconds > 120:
        parser.error("--wait-seconds must be in 0..120")
    if args.rotate_existing and args.command != "users":
        parser.error("--rotate-existing is valid only for users")
    return args


def main() -> int:
    args = parse_args()
    report = {"command": args.command, "status": "FAILED",
              "mode": "development-rf1" if args.development_rf1 else "replicated-secure",
              "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    code = 1
    try:
        load_env(args.env_file)
        {"topics": topic_reconcile, "acls": acl_reconcile, "users": users_reconcile}[args.command](args, report)
        report["status"] = "PASSED"
        code = 0
    except ReconcileError as exc:
        report["error"] = str(exc)
    except KafkaException as exc:
        report["error"] = "Kafka request failed: " + exc.args[0].name()
    except Exception as exc:
        # Generic third-party exceptions can contain sensitive request/config data.
        report["error"] = "Administration operation failed: " + type(exc).__name__
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.report:
        output = Path(args.report)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered)
    sys.stdout.write(rendered)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
