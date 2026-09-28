# Acceptance status and evidence gates

Status on 2026-09-27: implementation and acceptance tooling are being delivered;
the full upgrade plan's Phase 0-6 acceptance is **pending executed evidence**.
Production endpoints, cloud authorization, budget, real load and independent
failure-domain deployment have not been supplied. No staging HA or production
release is asserted by this file.

## Phase status must separate delivery from execution

| Phase | Delivery | Evidence required before completion |
|---|---|---|
| 0 | Frozen baseline/catalog, versions, capacity assumptions, recovery ADRs | Actual workload sizing/DB growth, reviewed production RPO/RTO/budget; unknowns explicitly retained |
| 1 | Shared security config, fresh RF3 environment, topic/ACL verification, production reference | Executed positive/negative auth, actual RF3/internal offsets/config reports, RF1 migration if retained data exists |
| 2 | JSON v1 schema/fixtures, validation, immutable hashes | Current-revision fixtures, malformed/oversize rejection, hash conflict and compatibility results |
| 3 | Outbox indexes/constraints, ownership/shards/writeback boundaries | Concurrent owners, stale token, ack/crash and original-ID duplicate evidence with per-consumer effect counts |
| 4 | Independent retry/DLQ, lifecycle handling, audit/replay | DB persistence/offset boundary, durable restart, poison isolation, audited retry/resolve/replay results |
| 5 | Worker metrics, dashboard/rules, lag versus parked/DEAD | Real scrape/freshness/alert fire and recovery, latency/capacity and telemetry-overhead measurements |
| 6 | Real RF3 harness/workflow and restore/cutover runbooks | Full matrix/counts/durations, isolated restore watermarks, independent-host/AZ staging and recorded cutover observation |

Current cloud/local execution results must be added with their exact commit/run
URL and artifact hashes. Do not mark a phase accepted solely because its files
exist, unit tests pass or historical results are checked in.

## Execution tiers

| Tier | What it can establish | What it cannot establish |
|---|---|---|
| Schema/config/unit regression | Contract validation, migration drift, controlled transaction assertions | Live protocol/failover, long-run capacity |
| Single broker development | Local event effects and at-least-once recovery | RF3 quorum or independent-host HA |
| Reduced RF3 CI smoke | Current real TLS/SCRAM, replication/config and fault plumbing, its exact small denominators | 30-minute/90,000-event or 20-repetition targets |
| Full RF3 same-host CI | Requested count/duration and process-fault matrix if all actual thresholds pass | Independent-host/AZ failures or production capacity |
| Independent-domain staging | Actual host/AZ/volume/network and restore behavior at frozen thresholds | Production release unless separately deployed/observed |
| Production release | Named deployed cluster/config/images, rollout time and post-release observations | Unperformed staging drills or guarantees outside the measured boundary |

The automatic workflow uses reduced inputs (60 events, 10/s, zero minimum steady
duration and one crash repetition); manual full inputs request 90,000 events,
50/s, 1,800s and 20 repetitions. Inspect actual fault windows and denominators:
requesting full inputs does not prove every broker/consumer outage lasted the
plan's five/ten minutes or every acceptance scenario was executed.

Both CI tiers explicitly freeze `EVENT_RETRY_SECONDS=15,30` followed by twenty-two
`60`-second delays, with deterministic jitter from the base through 20% above it,
before any worker starts. Broker outage recovery waits for these durable due
timestamps and natural leases; it never resets outbox status, leases or due times.
The artifact records the exact policy. Its maximum scheduled retry delay is 72
seconds and its minimum complete retry horizon exceeds the 600-second outage.
This is a validation policy, not evidence for default production retry timing.
The application's default `60,300,900,3600` schedule can delay an eligible retry
up to 4,320 seconds; for both outbox publication and FailedDelivery consumer
retries, its four delays can total 5,832 seconds before the fifth failed attempt
becomes DEAD, in addition to processing time. A 900-second drain
claim therefore requires a separately frozen compatible policy or an explicitly
audited operator recovery. Production policy and recovery SLA remain deployment
decisions requiring executed staging evidence.

## Required scenarios and frozen targets

Use [capacity](capacity.md) for latency/retention/RPO assumptions. Preserve all
attempts, committed commands, broker records, effects, errors and unfinished work.

| Scenario | Full-plan denominator/window | Pass boundary |
|---|---|---|
| Steady workload | 90,000 unique legal inventory events, 50/s for 30min | Each consumer 90,000/90,000 effect/dedupe; zero lost/extra; p95 <=5s and p99 <=15s |
| Duplicate delivery | 10,000 IDs republished twice, 20,000 duplicate records | Zero additional effects, original hash/ID retained |
| Publisher failures | Ack-before/ack-after-writeback, lease expiry, stale owner recovery, each 20 | No lost outbox; zero stale successful writeback; zero extra consumer effects |
| Consumer failures | Both consumers, before DB commit/after commit-before offset, each 20 | Same source redelivery; zero duplicated effects; no offset past unpersisted boundary |
| Broker failures | RF3, one broker down 5min; separate quorum loss | No acknowledged-record loss; no false publish success without quorum; durable outbox |
| Analytics pause | 10min while 50/s, 30,000 target inputs | Notification independent; drain <=15min after return; zero reconciliation mismatch |
| All-broker pause | 10min while 50/s, 30,000 target inputs | Business commit denominator retained; audited requeue if needed; drain <=15min |
| PostgreSQL failure | Business commit, consumer effect and FailedDelivery persistence boundaries | Atomic rollback/commit; offset not advanced when failure cannot persist |
| Poison/schema | 100 cases including type/version/JSON/decimal/size | Every applicable case isolated; zero partial effects; healthy traffic continues |
| Security negative | Anonymous, password, CA, foreign group/topic, write/create | All denied; outbox retained; no secret exposure |
| Rebalance/shutdown | Same group 1->3->1, SIGTERM/lost, each 20 | No permanent lost work/extra effects; bounded recorded close/commit behavior |
| Restore/retention | Isolated DB/config/offset restore, expired history, same-name topic recreation | Legal ledger/projection zero mismatch; original-ID replay; actual RPO/RTO/lost history |

Application-oversize messages below broker max should reach consumer isolation.
Broker-oversize rejection is producer evidence and cannot be counted as a consumer
DLQ case. Offset lag zero does not count parked RETRY/DEAD as successful business
effects. External notifications are outside the database-effect guarantee.

## Immutable run package

For each run write a new unique directory and preserve failures as well as passes:

```text
evidence/<run-id>/
  manifest.json            # commit/images/runtime/host/config hashes/scope
  workload.json            # seed/schema hash/payload samples/input/fault schedule
  events.jsonl             # original IDs/timestamps/source/effects/outcomes
  errors.jsonl             # every failure/timeout/parked/uncompleted observation
  offsets-before.json
  offsets-after.json
  reconciliation.json      # ledger/balance/projection/notification/dedupe counts/hashes
  latency.csv              # raw timestamps and incomplete markers
  metrics/
  logs/
  summary.md               # thresholds/raw denominators/results/limits/restore range
```

Artifact name/run ID and hashes identify retained output; immutable publication
storage must be selected by the deployment operator. Secret JSON, env files,
passwords, private keys and unrestricted payload dumps do not belong in shared
Actions artifacts. Preserve authorized original poison evidence separately.

The final acceptance report must name every failed/unexecuted gate and distinguish
current runs from historical evidence. Only after mandatory implementation **and**
real acceptance gates pass may the Kafka event upgrade be called complete.
Optional Apache Kafka/MSK Phase 7 remains separate and is not required for the
first Redpanda release.
