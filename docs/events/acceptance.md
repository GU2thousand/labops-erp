# Acceptance status and evidence gates

Status on 2026-09-28: implementation and acceptance tooling are being delivered;
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

Full mode requires `--drain-timeout 900` exactly; a larger timeout does not qualify
the frozen recovery target. It also requires the requested duplicate-event count
to be no greater than the steady-event count. Reject an impossible full duplicate
request rather than reducing its denominator to the available IDs.

Full mode defaults to one shared fault input of `N=30,000` committed inventory
events at 50 events/s for each applicable broker/analytics outage scenario. An
explicit larger shared `N` is allowed only when frozen before the run; full mode
requires `N >= 30,000` and does not lower any input denominator. The nominal
generation window is `N/50` seconds, or 600 seconds at the default. The
single-broker argument of 300 seconds is
a **requested minimum**, not an exact five-minute outage: generating that entire
shared batch while the broker is stopped normally makes the measured outage
approximately 600 seconds plus command/probe/observation overhead. Record both
the requested minimum and measured actual outage, with their clock boundaries.
Do not report an exact five-minute result or quietly reduce the 30,000-event
denominator to fit the shorter requested hold.

The full fault generation workload has the same frozen 5% window qualification
as steady generation; it must not silently become a much slower test with the
same successful event count. Report requested/committed/observed input counts,
configured rate, measured generation elapsed time, actual command rate and
schedule lateness for every fault batch. The full qualification requires:

- Requested input, reported input, completed commands and observed input count
  all equal the frozen `N`; configured/reported target rate is exactly 50/s.
- Finite, positive generation elapsed time no greater than `1.05*N/50`; reported
  lateness agrees with `max(0, elapsed-N/50)` and is no greater than `0.05*N/50`.
- Reported actual command rate agrees with `N/elapsed` and is between
  `50/1.05` (approximately 47.619/s) and the separately frozen upper bound 52.5/s.
- Requested minimum outage and measured actual outage are finite and retained
  independently; actual outage must be at least the greater of its requested
  minimum and measured generation elapsed time. The requested minimum is not an
  extra hold added after generation.

At the default 30,000 inputs the generation window is at most 630 seconds and
lateness at most 30 seconds. These thresholds are frozen before execution, not
adjusted after a failing observation. A reduced smoke batch must still report
finite, internally consistent measurements and its exact requested count, but
its qualification is `SMOKE_CAPACITY_UNQUALIFIED` with
`capacity_qualified=false`; it does not pass the full capacity gate. Full evidence
records `FULL_CAPACITY_QUALIFIED` or `FULL_CAPACITY_FAILED`, with every check,
failure code and malformed/nonfinite value retained in JSON-safe form. Four
floating-point ULPs at the operand scale cover representation equality at a
numeric boundary; they do not add a timing or percentage allowance. Changing
frozen inputs or qualification requires a separately identified run.

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

## Frozen business execution profile and known results

Capacity scenarios use the separately declared `parallel-lanes-v1` execution
profile: four FIFO worker lanes, each with queue capacity four. Assign an entire
four-command cycle to lane `(global_index//4)%4`; command kind follows
`global_index%4` as receipt, issue, transfer, reversal. Each complete cycle retains
that exact mix and its own batch/issue state. Global indices and lane state persist
across batches. Small fault fixtures explicitly use serial execution with the
same global-index/lane mapping, identified as `serial_fault_fixture` in their
workload result. A small drill may split a cycle across calls; its persistent lane
state completes the original cycle rather than resetting the command mix.

Each lane uses independent project, task, approved material request, purchase
order and order-line records created through the ordinary application services.
Each cycle creates its own inventory batch. The application retains its normal
transactions and business locks; concurrency does not bypass them or substitute
synthetic event inserts. The actor, item and source/target warehouse context are
shared read-only; business aggregates and per-cycle batch balances belong to
their lane. `business-lane-topology.json` records the actual project/task/order/
order-line IDs, shared context and lock scope.

One scheduler applies the requested **global** rate. At 50/s, 12.5/s per lane is a
nominal average, not four independent 50/s schedulers or a 200/s workload. Bounded
queues apply backpressure; each queue's four slots exclude its in-flight command.
For each capacity batch, elapsed time starts before
per-lane journal-batch setup and ends only after actual business commits,
observations, connection cleanup, worker completion and joins, and journal
finalization. Initial project/task/request/order fixture setup precedes that
capacity clock and is excluded. Report completed commands divided by that elapsed time and
the corresponding schedule lateness. Enqueuing 50 commands/s alone cannot pass
the frozen generation-rate gate.

The shared incremental journal uses an `RLock`; each lane receives its own batch
under the original scenario label. Retain requested/attempted/committed/
identified/unattempted counts, per-lane outcomes and scheduled/started/completed/
cancelled/unscheduled indices. A first failure stops scheduling and new command
starts; already-started commands finish before cleanup. Every lane closes its
own database connection and signals explicit completion, and every worker is
joined before the original failure or interruption is rethrown. A passing final
report requires every recorded generation topology to have `passed=true`, as
well as all mandatory scenarios and journal/database/effect gates. A partial topology or cleanup failure
cannot be hidden by a successful queue schedule or completed subset.

Freeze `generation-execution-profile.json` exclusively and fsync it before
environment loading or harness setup, alongside the numeric requested profile.
Retain actual `business-lane-topology.json`, per-batch
`generation-topology-*.json` and incremental `generation-schedule-*.jsonl`.
The per-batch topology/schedule files cover concurrent capacity batches; small
serial drills identify their execution mode in the workload result. The actual
artifacts distinguish the declared profile from the topology and
business work that really ran. A change from serial to concurrent execution
requires a new identified run; it does not change an earlier run's outcome.

The hosted serial probe [Actions run 36375853169](https://github.com/GU2thousand/labops-erp/actions/runs/36375853169),
at commit `dee603f649a097ebfcf78b3f6ad83fda937df654`, requested 3,000 commands at
50/s. It completed all 3,000 in **155.793580068s**, an actual **19.25624919/s**,
with **95.793580068s** of schedule lateness: the frozen 50/s rate gate **failed**.
The same run reconciled all 3,000 inventory events and both consumers' effects,
with no ledger/balance/projection mismatch; analytics p99 was 1.02153s and
notification p99 1.03008s. Those correctness/latency results remain valid for that
measured workload, but do not pass the throughput gate or unexecuted fault matrix.
Retained `report.json` SHA-256 is
`74e375ba7b5453c69aecc3c6c93c6b21fdf9c1ba9b3a910b25562c09f3083375`;
the failing report remains unchanged.

The new four-lane profile has no claimed hosted/full 50/s pass in this document.
A local SQL profile of 200 application-service commands is implementation
profiling, not an RF3 workload or hosted/production acceptance result. Full RF3,
independent-domain staging and production acceptance remain pending executed
evidence at their frozen denominators and clock boundaries.

## Required scenarios and frozen targets

Use [capacity](capacity.md) for latency/retention/RPO assumptions. Preserve all
attempts, committed commands, broker records, effects, errors and unfinished work.

| Scenario | Full-plan denominator/window | Pass boundary |
|---|---|---|
| Steady workload | 90,000 unique legal inventory events, 50/s for 30min | Each consumer 90,000/90,000 effect/dedupe; zero lost/extra; p95 <=5s and p99 <=15s |
| Duplicate delivery | 10,000 IDs republished twice, 20,000 duplicate records | Zero additional effects, original hash/ID retained |
| Publisher failures | Ack-before/ack-after-writeback, lease expiry, stale owner recovery, each 20 | No lost outbox; zero stale successful writeback; zero extra consumer effects |
| Consumer failures | Both consumers, before DB commit/after commit-before offset, each 20 | Same source redelivery; zero duplicated effects; no offset past unpersisted boundary |
| Broker failures | RF3, requested single-broker minimum 5min; shared full input 30,000 at 50/s extends actual outage to approximately 10min plus overhead; separate quorum loss | Requested minimum and actual window recorded separately; no acknowledged-record loss; no false publish success without quorum; durable outbox |
| Analytics pause | 10min while 50/s, 30,000 target inputs | Notification independent; drain <=15min after return; zero reconciliation mismatch |
| All-broker pause | 10min while 50/s, 30,000 target inputs | Business commit denominator retained; audited requeue if needed; drain <=15min |
| PostgreSQL failure | Business commit, consumer effect and FailedDelivery persistence boundaries | Atomic rollback/commit; offset not advanced when failure cannot persist |
| Poison/schema | 100 cases including type/version/JSON/decimal/size | Every applicable case isolated; zero partial effects; healthy traffic continues |
| Security negative | Anonymous, password, CA, foreign group/topic, write/create | All denied; outbox retained; no secret exposure |
| Rebalance/shutdown | Same group 1->3->1, SIGTERM/lost, each 20 | No permanent lost work/extra effects; bounded recorded close/commit behavior |
| Restore/retention | Isolated DB/config/offset restore, expired history, same-name topic recreation | Legal ledger/projection zero mismatch; original-ID replay; actual RPO/RTO/lost history |

The full analytics catch-up budget is 900 seconds and starts immediately **before**
starting the replacement analytics consumer, so process startup/group recovery
and effect drain consume that same window. Its final blocking database
predicate must finish within that same frozen window: starting a count/query
before the deadline and obtaining a successful answer after it is a failure.
Measure completion after the predicate returns, retain the window start and
actual elapsed time, and keep unfinished work in the denominator. Broker recovery
likewise includes network/cluster-health recovery from the recorded process-start
boundary rather than granting a fresh drain budget after health polling.

## Duplicate and exporter proof boundaries

The duplicate drill publishes two **new broker records** for every selected
original event and retains each producer acknowledgement's source/topic/partition/
offset. Both notification and analytics must demonstrate the two exact new
acknowledged coordinates for every event; the original delivery or repeated log
observations of one coordinate cannot substitute for a new duplicate record.
Delivery proof binds the configured source cluster/generation and main inventory
topic. The total distinct-coordinate coverage includes the original delivery
plus both new publications for each event/consumer pair.

Receipt proof and committed-offset proof share one frozen 180-second observation
window after duplicate publication. Each consumer group's committed **next**
offset must be strictly greater than every new acknowledged offset on its relevant
partition. Count a proof only after its final blocking request completes inside
the remaining original window. Take the zero-extra-effects comparison only after
both proofs pass; compare notification/dedupe counts and hashes and projection
hash against the pre-duplicate snapshot. Receipt alone or a count unchanged before
offset completion does not establish that both duplicates were safely processed.

Exporter coverage uses this run's exact notification and analytics group IDs and
main inventory topic: two groups times partitions `0`, `1`, `2`, giving six
required finite, nonnegative lag samples. Another run's groups, the DLQ topic,
missing partitions, duplicate required coordinates, NaN/Infinity or malformed
exposition cannot satisfy the gate. Preserve accepted raw sample lines and
ignored/total sample denominators alongside broker count and RF3 replica checks.
Six lag samples, even all zero, do not replace durable RETRY/DEAD or effect checks.

Application-oversize messages below broker max should reach consumer isolation.
Broker-oversize rejection is producer evidence and cannot be counted as a consumer
DLQ case. Offset lag zero does not count parked RETRY/DEAD as successful business
effects. External notifications are outside the database-effect guarantee.

## Immutable run package

For each run write a new unique directory and preserve failures as well as passes:

```text
evidence/<run-id>/
  requested-profile.json   # exclusively created numeric request before env/setup
  generation-execution-profile.json # frozen lane/cycle/queue/rate execution model
  business-lane-topology.json # actual independent aggregates and shared context
  generation-topology-*.json # per-batch actual lane counts, journal batches/results
  generation-schedule-*.jsonl # incremental scheduler/worker observations
  generation-journal.jsonl # incrementally flushed attempts/commit/ID transitions
  generation-summary.json # requested/attempted/committed/failure/pending totals
  startup-failure.json     # conditional env/setup failure and unobserved DB state
  manifest.json            # commit/images/runtime/host/config hashes/scope
  harness-manifest.json    # tier/retry policy/runtime identities
  workload.json            # seed/schema hash/payload samples/input/fault schedule
  events.jsonl             # original IDs/timestamps/source/effects/outcomes
  errors.jsonl             # every failure/timeout/parked/uncompleted observation
  duplicate-requests.json
  duplicate-publications.jsonl
  duplicate-offset-observations.jsonl
  delivery-proof-*.json    # required IDs/source/new ACK coordinates, pass or fail
  *-workload-qualification.json
  *-recovery-window.json
  offsets-before.json
  offsets-after.json
  reconciliation.json      # ledger/balance/projection/notification/dedupe counts/hashes
  final-outbox-state.json  # actual isolated DB rows/status/hash/lease; if observed
  final-processed-state.json
  final-inventory-state.json # observed flags, incomplete IDs, accounting conflicts
  final-offsets.json       # best-effort final durable cursors, even on failure
  final-reconciliation.json
  latency.csv              # raw timestamps and incomplete markers
  report.json              # FAIL stays fail; production_ready=false
  metrics/
    exporter-observations.jsonl
    exporter-accepted.prom
    exporter-lag-coverage.json
  logs/
  summary.md               # thresholds/raw denominators/results/limits/restore range
```

The numeric requested profile is exclusively created and fsynced after CLI
validation but **before** loading the generated environment or constructing the
harness. A startup failure therefore retains its original request and marks the
database as unobserved. Never derive requested counts from `events.jsonl`, completed
effects or whichever scenarios happened to run.

The execution profile is likewise frozen before environment/setup. Actual lane
and per-batch topology evidence must match that profile; generation completion
and the final report include all topology pass/failure outcomes. The package
retains a failed or partial schedule, not just successful event-log entries.

For each generation batch, record requested and attempted counts before the
outer business transaction; record committed movement identity immediately after
the transaction returns, before timestamp/outbox/event-log observation. Record
the original event identity separately when observed. Preserve failures before
commit, post-commit observation failures, identified events, unattempted commands,
pending commits and pending observations. A commit journal transition is flushed
and fsynced, but the filesystem journal and PostgreSQL are not atomic; an exit or
filesystem failure between them requires final database reconciliation.

On both success and failure, stop supervised workers and attempt to export actual
outbox/processed rows, per-consumer incomplete IDs, committed offsets and
ledger/balance/projection/notification reconciliation. Reconcile actual committed
IDs/movements against both the journal and event log, retaining missing/conflicting
sets. A failed database/offset/reconciliation query is **unknown/unobserved** with
its safe error classification; it never becomes zero pending work, zero loss or
a passing denominator. Partial generation and secondary evidence errors remain
visible. A failing `report.json` causes a nonzero CLI exit even when no earlier
scenario raised. If finalization fails while another exception is already active,
preserve the original exception rather than replace it with the collection error.

Artifact name/run ID and hashes identify retained output; immutable publication
storage must be selected by the deployment operator. Secret JSON, env files,
passwords, private keys and unrestricted payload dumps do not belong in shared
Actions artifacts. Preserve authorized original poison evidence separately.

The journal/profile store only allowlisted numeric parameters, authored scenario
codes, original UUIDs and exception class names; they exclude raw business payload,
connection strings, environment values and credentials. Final state exports keep
IDs/status/checksums instead of payload/recipient dumps. The collector masks known
generated credentials in text artifacts, including worker logs/failure summaries,
before upload; review collection errors and redaction outcomes before sharing.
Secret maps, generated env files, tokens and private keys remain excluded. An
isolated synthetic database dump still contains users, outbox and notification
data and requires restricted artifact access; text credential masking does not
make such a binary backup safe for public publication.

The final acceptance report must name every failed/unexecuted gate and distinguish
current runs from historical evidence. Only after mandatory implementation **and**
real acceptance gates pass may the Kafka event upgrade be called complete.
Optional Apache Kafka/MSK Phase 7 remains separate and is not required for the
first Redpanda release.
