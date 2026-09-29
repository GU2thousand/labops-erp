# Versions and compatibility evidence

Version review: 2026-09-27. Recheck support before a deployment at a later date.

| Component | Frozen baseline | Selected upgrade | Required execution evidence |
|---|---|---|---|
| Python | Docker `python:3.12-slim` | Python 3.12 | Exact `sys.version` and application image digest per run; base tag is not an immutable runtime identity |
| Django | `5.2.17` | `5.2.17` | Lockfile/requirements hash and test revision |
| confluent-kafka | `2.15.1` | `2.15.1` | `confluent_kafka.version()` plus runtime `libversion()` |
| librdkafka | Bundled/runtime dependent | Runtime bundled with the installed client wheel | Collect `libversion()`; do not invent a version from the package number |
| Redpanda/rpk | `25.1.9` | `26.2.2` | Broker/rpk version, image digest and cluster feature/config report |
| PostgreSQL | `17-alpine` image tag | PostgreSQL 17 | `SHOW server_version`, image digest, backup restore and concurrency tests |
| JSON inventory | Existing envelope v1 | Inventory v1 and explicit DLQ v1 | Fixture/schema SHA256 and compatibility test result |

Pinned Redpanda image:

```text
docker.redpanda.com/redpandadata/redpanda:v26.2.2@sha256:468bd13a9f2bd24794cb7fddc867c767fb1008b9a07b297b89fde48c564d7d96
```

Official release notes list 26.2.2 as released on 2026-08-21 and the 26.2 line as
supported through 2027-07-28. The 25.1 line ended support on 2026-04-07.
[Redpanda releases](https://docs.redpanda.com/streaming/current/reference/releases/redpanda/).

Record actual runtime versions, not only these intended selections:

```sh
python -c 'import sys,confluent_kafka; print(sys.version); print(confluent_kafka.version()); print(confluent_kafka.libversion())'
docker compose --profile events exec redpanda rpk version
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SHOW server_version"'
```

## Retained broker data and upgrade path

The RF3 validation cluster is initialized with fresh disposable volumes directly
at 26.2.2. Do **not** reuse a retained 25.1 data volume with that image.

A retained 25.1 deployment needs a separate vendor-compatible rolling upgrade
plan, tested backup/restore, selected supported intermediate patches and evidence
at every feature boundary: `25.1 -> 25.2 -> 25.3 -> 26.1 -> 26.2`. Upgrade one broker
at a time, verify health and replication after each step, and complete each
feature boundary before proceeding. Recheck intermediate support/licensing and
release incompatibilities with the operator. The source plan is not permission
to run obsolete brokers indefinitely.

Do not promise an in-place feature downgrade after finalization. The 26.2
deferred-finalization route requires the applicable Enterprise capability and
license; absent verified support, use the tested restore/replacement-cluster path.
Application schema cutover and broker feature upgrade have separate rollback
boundaries. [Official upgrade guidance](https://docs.redpanda.com/streaming/current/upgrade/rolling-upgrade/).

The upstream Confluent current API page identified itself as 2.15.0 when reviewed;
the repository pins 2.15.1. Use installed-client tests for delivery callbacks,
synchronous commit, rebalance and close semantics. A current documentation page
is not proof that every newer API is available in the pinned wheel.
[Confluent Python API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).
