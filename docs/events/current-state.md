# Frozen current state and upgrade boundary

Baseline repository: [GU2thousand/labops-erp](https://github.com/GU2thousand/labops-erp).
Baseline commit: `cdda597d77fd008aa4053b301c07bac2d04bb153`.
Review date: 2026-09-27. This file describes that commit, before the event upgrade;
it is not a claim that the new configuration has run in production.

| Component | Fact at the frozen commit | Upgrade requirement |
|---|---|---|
| Database | PostgreSQL 17 development/CI; SQLite demo compatibility | Ledger, balances, audit and outbox remain one PostgreSQL transaction |
| Broker | Compose `events` profile, Redpanda `v25.1.9`, one broker | Supported pinned patch; fresh RF3 validation; separately operated production deployment |
| Topics | Inventory and DLQ, three partitions, RF1 | RF3 validation and explicit migration of retained RF1 topics |
| Topic init | Creates missing topics and skips existing ones | Verify partitions, replicas, retention, delete policy, message limit, write caching and internal offsets allocation |
| Transport | Default `local`; inventory emitter stores selected route on each row | Enabling a profile alone does not select Kafka; historic rows retain their route |
| Event scope | `inventory.{opening,receipt,issue,transfer,adjustment,reversal}.posted` | Preserve scope; ordinary alerts and imports keep their existing workflow |
| Publisher | Idempotent producer, `acks=all`; one delivery/flush acknowledgement per row | Bounded queue/flush/shutdown budget, shared TLS/SASL settings, ack-only writeback |
| Claiming | `SKIP LOCKED`, random lease token, 60-second lease | Expired-token write rejection and explicit stale-sender limitations |
| Ordering | Key is `stockmovement:<movement UUID>`; earlier unpublished aggregate versions block later ones | No batch/warehouse total order; current analytics deltas commute |
| Consumers | `labops.notification.v1`, `labops.analytics.v1`; auto commit/store disabled | Effects or durable failure record before synchronous offset commit |
| Dedupe | Unique `(consumer_name,event_id)` within one database | Add whole-envelope checksums; conflicting content must be isolated |
| Failure source | Consumer plus topic/partition/offset string | Add source cluster and stream generation to survive same-name topic recreation |
| Retry | Durable FailedDelivery, 60/300/900/3600-second schedule | Classify errors, jitter, isolated retry and DLQ workers, append-only operator audit |
| DLQ | DEAD record mirror published to DLQ | Stable delivery ID and claim/writeback; PostgreSQL remains the recovery record |
| Projection | Rebuild from POSTED ledger; checkpoint existing inventory outbox for analytics | Explicit stopped-write recovery with retained master data/outbox/dedupe |
| Operations | `process_events` also runs imports and daily alerts | Continue it when Kafka is enabled |
| Metrics | Durable backlog/failure gauges, Kafka exporter, OTel; standalone worker counters absent from web scrape | Independent worker scrapes and freshness/parked/DEAD indicators |
| CI | PostgreSQL and SQLite regressions, Compose checks; separate historical fault results | Current immutable real RF3 execution evidence, distinct reduced/full/staging status |

Frozen source links:
[Compose](https://github.com/GU2thousand/labops-erp/blob/cdda597d77fd008aa4053b301c07bac2d04bb153/compose.yaml),
[events](https://github.com/GU2thousand/labops-erp/blob/cdda597d77fd008aa4053b301c07bac2d04bb153/labops/events.py),
[models](https://github.com/GU2thousand/labops-erp/blob/cdda597d77fd008aa4053b301c07bac2d04bb153/labops/models.py),
[consumer](https://github.com/GU2thousand/labops-erp/blob/cdda597d77fd008aa4053b301c07bac2d04bb153/labops/management/commands/consume_kafka.py),
[rebuild](https://github.com/GU2thousand/labops-erp/blob/cdda597d77fd008aa4053b301c07bac2d04bb153/labops/management/commands/rebuild_inventory_projection.py).

## Facts, assumptions, and unknowns

The source facts above were inspected at the fixed commit. Historical
`benchmarks/results/` and `VALIDATION.md` belong to their own stated revisions and
workloads; retain those records without relabeling them as this upgrade's result.

The selected implementation candidate is Redpanda `26.2.2`, Python `3.12`, and
`confluent-kafka==2.15.1`; see [compatibility](compatibility.md) for image identity,
runtime collection and broker upgrade limits. JSON inventory v1 is retained.

Real production average/peak event rate, payload distribution, consumer time,
database growth, disk budget, SLA, production endpoints, cloud authorization,
fault domains and backup/PITR capability have not been supplied or measured.
The plan's 50 events/s and retention periods are explicit starting assumptions,
not measurements. See [capacity](capacity.md).

No public production deployment or independent-AZ acceptance follows from this
baseline inspection. Phase 0-6 completion depends on the evidence gates in
[acceptance](acceptance.md), including raw counts and restore watermarks.
