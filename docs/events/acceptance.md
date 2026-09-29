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

Automatic jobs and the manual default use `runner_arch=x64` on
`ubuntu-24.04`. The manual comparison permits only `arm64` on
`ubuntu-24.04-arm`; arbitrary labels, architecture mismatches and private or
self-hosted runner contexts fail admission before Docker. GitHub documents both
as [standard runners for public repositories](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-public-repositories),
whose standard usage is free. This comparison keeps the same application,
workload, default four-writer profile and acceptance gates. It does not establish
independent-host HA or explain performance differences by architecture alone.

Before dependencies and services, `runner-profile.json` freezes the selected
label, actual `RUNNER_ARCH` and platform architecture, Python version, source
revision, logical CPU count, affinity and visible own-process cgroup CPU limits.
Missing counters remain unknown. Logical CPUs and runner specifications do not
prove physical core count; that field remains unmeasured. After execution, the
collector records the IDs, architectures and repository digests of the images
actually selected by the six validation services, without an additional pull.
Architecture-specific child digests may differ beneath a shared image manifest.
The ARM 3,000-event/50-per-second/60-second comparison explicitly enables
diagnostics; its inclusive elapsed and latency gates remain unchanged. The prior
x64 rate and latency failure remains a separate retained result. The first ARM
[run 36387008788](https://github.com/GU2thousand/labops-erp/actions/runs/36387008788),
at `c5ee5ed` with unchanged `605c9d3` application code, also **failed**: all 3,000
commands committed in 76.823346875 seconds (39.050628774/s), exceeding the
63-second generation window. Analytics p95/p99 were 10.015/10.957 seconds and
notification p95/p99 were 20.895/21.169 seconds; latency qualification failed.
Both consumers completed all original IDs with exact effects and no ledger or
projection mismatch. The new image observer retained four of six image rows;
PostgreSQL and exporter inspection failed on missing optional label maps, while
the existing run-scoped Compose image evidence independently retained all six
native ARM identities. Keep those observer failures intact. The guarded optional
label lookup leaves a missing version as null. Subsequent balance/line read and
publisher pacing changes require a new run; none predicts 50/s capacity.

Those bounded changes at `b10f8bc` were tested by ARM
[run 36389116383](https://github.com/GU2thousand/labops-erp/actions/runs/36389116383),
which also **failed**: 3,000 commands in 67.823740143 seconds (44.232299689/s),
analytics p95/p99 6.059978/6.247530 seconds and notification p95/p99
20.226638/20.515744 seconds. All original effects completed with exact counts,
and all six selected-image observations established native ARM identities.
The rate and latency gates remain unmet; those outcomes are preserved.

Further query changes keep one business command and one consumed event per
transaction. Inventory reads join only the already-required task/project,
batch/item and receipt/order/supplier relations; the existing separate row locks,
advisory locks, lookup errors and validations stay in place. Analytics obtains
the projection with one locked `get_or_create` under its existing advisory lock.
Original envelopes still validate and initialize or verify their immutable hash
before comparing that checksum with the incoming hash.

The internal `locking.rows` helper now eagerly selects only primary keys. Its
two command-lock callers discard the result; truthy-key filtering, deduplication,
primary-key ordering, `FOR UPDATE`, ancestor lock order and transaction lifetime
remain the same. The focused PostgreSQL regression scope includes two independent
backend sessions that observe blocking through `pg_blocking_pids` until the
holder commits or rolls back, plus the existing receipt, inventory, allocation,
draft-posting and rollback checks. SQLite checks query shape and compatibility;
it cannot establish PostgreSQL row-lock behavior. The observed writer profile
motivates removing model hydration, but does not predict CPU savings or 50/s
capacity. A separate unprofiled acceptance run is still required.

PostgreSQL uses Django's `server_side_binding=True` cursor option with psycopg 3.
`prepare_threshold=None` remains explicit: this change does not enable automatic
prepared-statement caching. `DB_SERVER_SIDE_BINDING` accepts only `1` (the
default) or `0`; an invalid value fails PostgreSQL configuration before connection.
For compatibility rollback, set `DB_SERVER_SIDE_BINDING=0` and restart every web,
publisher and consumer process so new connections use client binding. SQLite demo
options are unchanged. URL options do not override this selector. See the
[Django parameter-binding option](https://docs.djangoproject.com/en/5.2/ref/databases/#server-side-parameters-binding)
and [psycopg prepared-statement control](https://www.psycopg.org/psycopg3/docs/advanced/prepare.html).

The focused PostgreSQL proof must record actual application, named and dedicated
ownership cursors; quote/percent, UUID-array, JSON, timezone, decimal and Fixed6
round trips; transaction rollback and timeout restoration; and the tracing wrapper
with an in-memory exporter. It captures the actual parameterized claim SQL and
compares both binding modes under the normal planner after eight executions,
including use of `outbox_active_created_id_idx`, selected IDs and empty named
prepared-statement catalogs. Existing migration/concurrent-claim, ledger/report
and recovery tests must also pass. These compatibility checks make no CPU-saving,
latency or capacity claim; fresh hosted acceptance keeps the same workload and
thresholds.

Function profiling remains disabled by default. Own-thread diagnostic attribution
requires the classic callback profiler engine to be observed on the actual host;
the configured CPU timer is unchanged. An unsupported engine yields incomplete
profiling evidence while business execution continues without profiling.

The `1cea77d` publisher diagnostic
[run 36459580248](https://github.com/GU2thousand/labops-erp/actions/runs/36459580248)
records 518 `claim_event` calls at
1.611524912 own-thread CPU seconds and 6.332475470 wall seconds. This is the
composite claim transaction, including lookup, decoding, lease update and commit;
it does not isolate the correlated SQL predicate's cost. Its unused `blocked`
annotation is now an alias: the same earlier-version `EXISTS` still filters
eligibility, while its value is no longer selected onto the returned model.
Payload fields, shard annotation, due/lease boundaries, ordering and
`FOR UPDATE SKIP LOCKED` remain unchanged. A focused PostgreSQL 17 proof must
establish actual disjoint claims and outer commit/rollback behavior; this narrow
query change makes no CPU-saving, latency or capacity claim.

Notification eligibility is one event-local active-user query snapshot
(PostgreSQL's configured default is READ COMMITTED). Missing and inactive users
are excluded. Existing notifications retain their UUID, title, body and read
timestamp. Missing rows use a plain bulk insert inside a savepoint; only the
named event/user uniqueness race permits the existing `get_or_create` fallback.
Other primary-key, foreign-key or constraint failures roll back the marker and
effects. Exact eligible-recipient completion is checked in the same transaction.
This groups rows within one event, and does not batch consumed events or offsets.
These changes require another live probe and do not predict 50/s capacity.

The subsequent ARM [run 36391294683](https://github.com/GU2thousand/labops-erp/actions/runs/36391294683)
at `e23246a` also **failed**: all 3,000 commands/effects completed, but the
77.073496419-second window (38.923886153/s) exceeded 63 seconds. Analytics
p95/p99 were 6.49619/6.68469 seconds and notification p95/p99 were
7.45375/7.62372 seconds. Both p99 results met 15 seconds; both p95 results
exceeded five seconds. Exact counts and complete native diagnostics do not
replace those failed rate and latency gates.

PostgreSQL timeout queries retain their original scope and budgets. The
publisher/DLQ helper now materializes both previous session values before
applying both settings in one statement, then restores both saved values in
the original second statement. The earlier paired implementation used three
statements per operation; this single setup change removes one round trip.
Any setup execute/fetch/result failure discards the application session with
unknown settings and preserves the original exception; the body is not entered.
The independent ownership session is untouched. The existing restore database-
error path still closes the application connection. See the
[setup hypothesis and required proof](../publisher-budget-setup-hypothesis.md).
Database-effect workers set both transaction-
local values in one statement; the original atomic context remains, and SET
LOCAL lasts until the outer transaction commits or rolls back. Publisher owner
checks, lease checks, per-message broker ACK, conditional writeback and consumer
offset/transaction boundaries are unchanged. The two `create_receipt` joined
lookups load only supplier and request-line/item data used by existing checks.
They add no joined row locks and retain the same lookup errors and guards.
Receipt SUMs remain independent reads; no totals cache is introduced. These
query reductions require fresh acceptance and do not forecast throughput.

Runtime diagnostics are an explicit opt-in with `--runtime-diagnostics`.
The default CLI and automatic 60-event smoke leave diagnostics disabled. The
manual workflow's `runtime_diagnostics` boolean also defaults to `false`; the
planned 512-event diagnostic, 3,000-event probe and full runs explicitly set it
to `true` or pass the CLI flag. The requested profile freezes that choice before
environment/setup. Disabling diagnostics changes instrumentation scope; it does
not change the independently frozen writer topology, queue capacity four, global
target rate, 5% generation window or any business/fault denominator. No execution
mode or writer count is selected by the diagnostics flag: process/thread selection
follows the separately frozen count policy below, including when diagnostics are
disabled.

When disabled, the report records `runtime_diagnostics_enabled=false`,
`runtime_diagnostics_applicable=false`, `runtime_diagnostics_status=NOT_REQUESTED`
and `runtime_diagnostics_complete=null`. This is no claim of collected or complete
measurements; an empty diagnostic list cannot become successful numeric coverage.
When requested, the frozen execution profile records
`runtime_diagnostics.enabled`, `.applicable` and `.request_status`; its request
status is `REQUESTED` or `NOT_REQUESTED`. The initial
`runtime-resource-profile.json` uses `REQUESTED_NOT_STARTED` when enabled;
overall diagnostic qualification uses `COMPLETE` or `INCOMPLETE` with retained
summaries and errors. Each batch's `collection_complete` remains separate: a
qualified fault batch may still have incomplete numeric resource coverage.

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

## Frozen writer topology

`--writer-topology` and the manual workflow's `writer_topology` input accept only
`writers-4` (default) or `writers-6`. Automatic jobs retain `writers-4`; eight
writers and arbitrary lane counts are not admitted. Six writers are the selected
new acceptance candidate, **unqualified until actual PostgreSQL integration and
native RF3 evidence pass**. Selection or unit tests do not establish capacity.

New requested profiles record both `writer_topology` and
`writer_topology_version=writer-topology-v1`, including the explicit default.
Legacy profiles resolve to four only when both fields are absent, preserving
their original serialized shape and digest semantics. If either field is
present, both must validate; unknown IDs, partial pairs, wrong versions or a
different selection in a dependent profile fail admission. The selection is
frozen in `writer-topology.json` and jointly validated with the consumer profile
before environment loading, services or database setup. The workflow runs the
standard-library topology helpers immediately after checkout, before Python
setup. The existing runner hardware/runtime admission follows Python setup and
still precedes dependency installation or Docker. Function profiling ON with
`writers-6` is rejected before setup; its four-writer
attribution scope is not expanded. Runtime diagnostics remain an independent
predeclared choice.

Let `L` be four or six from that admitted profile. Requested, execution, consumer,
business-lane, origin and process-resource evidence must agree on `L`; the
consumer profile's existing `writer_lanes` field now records this selected
count. Consumer membership remains independently frozen. Six writers may compose
with either `single` or `notification-dual`, without changing publisher count,
consumer group membership, inventory partitions or per-record synchronous ACK.
Each applicable native diagnostic sample must cover every selected generator,
including `generator-4` and `generator-5` for six. Optional thread observations
use the same selected `paced-business-lane-0..L-1` role set. A missing extra lane,
shrunk observer profile, reused identity or incomplete close/reap/session proof
cannot become complete numeric coverage.

The capacity request remains one global 50/s on the same native
four-logical-CPU runner and original effective service limits. A 3,000-command
probe must finish within the original inclusive 63 seconds and retain 3,000
complete durable latency samples per logical consumer, p95 <=5s and p99 <=15s.
Exact effects, security, accounting, fault, cleanup and full-tier gates remain
unchanged. Correctness smoke at global 10/s cannot qualify 50/s capacity. The
following is a request template for the reviewed implementation, not an executed
or passing result; `reviewed-ref` must identify that exact source:

```sh
gh workflow run events-validation.yml --repo GU2thousand/labops-erp --ref reviewed-ref \
  -f tier=smoke -f events=3000 -f rate=50 -f duration=60 -f fault_repetitions=1 \
  -f runner_arch=arm64 -f writer_topology=writers-6 -f consumer_topology=notification-dual \
  -f runtime_diagnostics=true -f diagnostic_profile=false
```

The corresponding acceptance CLI selection is `--writer-topology writers-6`
with `--consumer-topology notification-dual`; all original numeric, outage and drain
arguments remain explicit for the requested tier. Returning to four requires a
new explicit invocation and its own evidence. Never relabel a failed six-writer
run, change its denominator or carry earlier-source results to this candidate.

## Frozen consumer topology

`--consumer-topology` accepts `single` (default) or the explicit
`notification-dual` preset. `consumer-topology.json` freezes profile version
`consumer-topology-v1` before environment/setup; the workflow freezes it before
services. Both presets compose the selected writer count `L` with one publisher,
one analytics member and the existing three inventory partitions. The dual preset
starts
`notification` and `notification-1` in the same existing notification group.
Production consumption retains one durable database transaction followed by an
immediate synchronous offset commit for each record. Function profiling ON with
the nondefault preset is rejected before setup; runtime diagnostics are a
separate, predeclared choice.

Readiness requires STABLE membership with exactly the owned
`acceptance-<PID>` client IDs, nonempty disjoint assignments and the complete
topic/partition union `0,1,2`. Dual notification assignments therefore have sizes
one and two; analytics retains all three. After the broker observation, the
harness rechecks each member's live PID, generation and process start identity
within the original deadline. Every generation has distinct log, delivery and
metrics identities. `.started`/`.closed` receipts bind the actual PostgreSQL
backend and application name to that process. Their startup query warms the owning
connection for both presets outside generation; this is no cold-start
measurement. A close receipt covers the owning thread only; after reaping,
separate settlement checks all sessions in the exact owned application namespace.
Cleanup retains every started child, exit/signal outcome and secondary error,
including partial pool startup failures.

Exclusive fault, retry, database and restore drills stop every owned supervised
or temporary member of the affected group before their targeted child runs.
Recovery restores the selected pool and exact readiness. The mandatory
rebalance remains the original temporary **1→3→1** drill, followed by restoration
to notification two/analytics one for the dual preset; it is not replaced with
2→3→2. Per-member delivery files are joined by logical consumer, original event
and broker coordinate, retaining process/log provenance. Worker count does not
change effect or latency denominators; a delivery log still precedes ACK and
cannot replace committed-offset evidence.

The dual preset requires its own executed evidence and supplies no capacity
claim for a newly selected writer profile. The preceding `single` capacity
[run 36486112533](https://github.com/GU2thousand/labops-erp/actions/runs/36486112533)
at `cc4569ac` remains **FAIL**: 3,000 commands took 75.694751744 seconds
(39.632866624/s), and notification p95 was 8.656019926 seconds. A fresh unprofiled
dual run must retain the same native four-logical-CPU runner/resource limits,
global 50/s, 3,000-command maximum 63-second window, p95 ≤5s and p99 ≤15s,
3,000 durable latency samples per logical consumer with none missing, exact
6,000 logical consumer/event effects and the fixture's 9,000
notification rows. All errors, consistency, security, fault and full-tier gates
remain unchanged. Extra members establish no expected speedup, same-baseline
code gain, independent-host HA or production release.

## Frozen business execution profile and known results

Before environment/setup, the CLI freezes the selected writer topology and an
automatic execution policy: capacity-scenario batches of **512 or more commands**
use exactly `L` fresh `spawn` children (`spawn-lanes-v1`); smaller capacity batches
use `L` FIFO thread lanes (`parallel-lanes-v1`). Thus 512/3,000-command probes and
full
90,000 steady/30,000 fault inputs select spawn; default 60-command steady and
20-command fault inputs retain threads. Both modes have `L` FIFO worker lanes,
each with queue capacity four, and one global output/IPC credit limit of 16.
Input queue slots total 16 for four lanes or 24 for six; output credits do not
increase with writer count. Assign an entire
four-command cycle to lane `(global_index//4)%L`; command kind follows
`global_index%4` as receipt, issue, transfer, reversal. Each complete cycle retains
that exact mix and its own batch/issue state. Global indices and lane state persist
across batches. Small fault fixtures explicitly use serial execution with the
same global-index/lane mapping, identified as `serial_fault_fixture` in their
workload result. A small drill may split a cycle across calls; its persistent lane
state completes the original cycle rather than resetting the command mix.

Spawn result frames retain the existing 60,000-byte protocol limit and carry
complete cleanup metadata in one atomic datagram. Before spawning, both owned
socket endpoints use per-socket send capacity 60,000 bytes and receive capacity
120,000 bytes, with both endpoints registered for cleanup before option setup.
This corrects a harness transport mismatch: the first local six-writer PostgreSQL
regression's 48-command spawn batch committed all 48 but failed to deliver cleanup
receipts; its later continuation was unreached. The mixed-mode test committed
transfer 26 and cancelled reversal 27. Retained sequence numbers and finalized journals locate the failure
after owning cleanup returned; the exact failed frame and errno were not captured.
A separate native macOS synthetic probe observed default 2,048-byte send and
4,096-byte receive buffers, `EMSGSIZE` for a 4,096-byte send, and exact 4,096- and
60,000-byte round trips with the configured buffers. Receive capacity 60,000 alone
failed that boundary probe, so the bounded 120,000-byte receive setting includes
datagram/address overhead. The probe changed no global kernel setting and does
not establish database, workload or capacity acceptance. The same 16 global
output credits, four input slots per lane, service CPU/memory limits and required
close/reap/session checks remain. See
[Apple's socket buffer options](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setsockopt.2.html)
and [XNU's local datagram buffer implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/uipc_usrreq.c).

The historical four-writer spawn probe at `605c9d3` failed: 3,000 commands took
93.519 seconds (32.079/s), with analytics p99 18.297 seconds and notification p99
25.105 seconds. All 3,000 original events and both consumers completed with exact
effects and no ledger mismatch; this does not satisfy the rate or latency gates
and is not evidence for the six-writer candidate.

Each lane uses independent project, task, approved material request, purchase
order and order-line records created through the ordinary application services.
Each cycle creates its own inventory batch. The application retains its normal
transactions and business locks; concurrency does not bypass them or substitute
synthetic event inserts. The actor, item and source/target warehouse context are
shared read-only; business aggregates and per-cycle batch balances belong to
their lane. `business-lane-topology.json` records the actual project/task/order/
order-line IDs, shared context and lock scope.

Increasing `L` creates more legal project/task/order/order-line fixtures and
distributes posted receipt history and same-order work over more aggregates.
Each lane retains the original ordered quantity formula
`(events + fault_events*4 + fault_repetitions*20 + 1000)*4`; it is not divided
by `L`. Shared actor/item/warehouses and receipt/issue/transfer/reversal quantities
remain unchanged. This changes data, contention, total ordered ceiling and
connection/setup footprint. A passing six-writer run would qualify that declared
profile; comparing it with four does not isolate a pure concurrency gain. No
CPU, SQL, ACK or throughput improvement is forecast.

One scheduler applies the requested **global** rate once. At 50/s, `50/L` per lane
is nominal metadata (12.5/s for four, approximately 8.333/s for six), not an
independent pacing clock in each child. Bounded
queues apply backpressure; each queue's four slots exclude its in-flight command.
For each capacity batch, elapsed starts before lane-batch or spawn-plan setup and
includes actual business commits/observations, owning database close and worker
completion. Threads include joins; spawn additionally includes spawn/init, origin
persistence, reaping and PostgreSQL session settlement. Enabled diagnostic discovery and cleanup also consume that
same clock. Spawn pacing begins after all `L` READY handshakes; readiness
overhead still consumes the unchanged completion window. Initial shared business
fixture setup precedes this clock and is excluded; record its increased cost
separately for the new profile. Report completed commands divided by that elapsed time and
the corresponding schedule lateness. Enqueuing 50 commands/s alone cannot pass
the frozen generation-rate gate.

Thread mode's shared incremental journal uses an `RLock`; each lane receives its own batch
under the original scenario label. Retain requested/attempted/committed/
identified/unattempted counts, per-lane outcomes and scheduled/started/completed/
cancelled/unscheduled indices. A first failure stops scheduling and new command
starts. Thread in-flight commands finish before owning connection cleanup and
joins. Spawn failure cleanup is bounded: in-flight work may finish during an
up-to-30-second drain, then SIGTERM/5-second grace and forced stop if necessary. Reap and
session settlement remain required; a forced stop cannot imply rollback or
complete cleanup. A passing final
report requires every recorded generation topology to have `passed=true`, as
well as all mandatory scenarios and journal/database/effect gates. A partial topology or cleanup failure
cannot be hidden by a successful queue schedule or completed subset.

Each spawn child exclusively owns its origin journal. ATTEMPT and COMMIT
transitions are flushed and fsynced; identification/event observations are
flushed, with the origin prefix fsynced on completion/failure/finalization.
`CompositeGenerationJournal` unions thread and child reservations/counts without
rewriting child evidence. A lost IPC result is `COMMIT_UNKNOWN` until retained
origin evidence or exact legal movement/outbox reconciliation resolves it after
the owned PostgreSQL sessions settle. Missing/conflicting facts stay unknown or
failed; they never become zero commits. Reconciliation does not automatically
replay a command or turn an interrupted batch into a pass.

Private child bootstrap carries the actual owning Django database configuration,
including its `NAME`, and the frozen `EVENT_TRANSPORT`, inventory topic, source
cluster and stream generation. It does not select a database from an unrelated
environment fallback. Artifacts retain safe identities/digests and authored
namespace context, excluding credentials. Child failures cross IPC as sanitized
stage/class/outcome evidence; raw messages/tracebacks and bootstrap credentials
are not artifact content. Database reconciliation establishes legal row/outbox
facts; configuration context alone does not establish broker source identity.

Freeze `generation-execution-profile.json` exclusively and fsync it before
environment loading or harness setup, alongside the numeric requested profile.
Retain actual `business-lane-topology.json`, per-batch
`generation-topology-*.json` and incremental `generation-schedule-*.jsonl`.
The per-batch topology/schedule files cover concurrent capacity batches; small
serial drills identify their execution mode in the workload result. The actual
artifacts distinguish the declared profile from the topology and
business work that really ran. A change from serial to concurrent execution
requires a new identified run; it does not change an earlier run's outcome.
The frozen policy and each batch's actual selected mode must agree; spawning is
independent of the diagnostics opt-in and is not a response to observed speed.

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

The hosted four-lane probe [Actions run 36377715335](https://github.com/GU2thousand/labops-erp/actions/runs/36377715335),
at commit `9e0d324e1e123f90501e6572cf5f8b162db2a0e1`, completed all 3,000 commands
in **131.664182009s**, an actual **22.785240103/s**, with **71.664182009s** of
schedule lateness. The unchanged 50/s generation gate **failed**. All 3,000
outboxes published; both consumers completed 3,000 IDs, with exactly 6,000 dedupe
markers and 9,000 notifications, no missing IDs or reconciliation mismatches.
Analytics p99 was 1.06097s and notification p99 1.09924s. The retained failing
`report.json` SHA-256 is
`bc0fe1a1d7851ce34df03b55d0a881092530d46ed3e48e3499a5cd97ca615eb9`.
These results establish correctness and latency at the measured workload;
they do not meet the requested rate. CPU/GIL, SQL waits and host contention were
not measured by that run, so no specific saturation cause is asserted.

The hosted 512-event diagnostic [Actions run 36380529863](https://github.com/GU2thousand/labops-erp/actions/runs/36380529863),
at commit `63f10f50ee544d68f37c1811634784cc4d79f230`, completed all 512 commands
in **24.396559664s**, an actual **20.986565608/s** against the unchanged 50/s
target, with **14.156559664s** of lateness. The rate gate **failed**. All 512
outboxes published; both consumers completed 512 IDs, with exactly 1,024 dedupe
markers and 1,536 notifications and zero reconciliation mismatches. The retained
failing `report.json` SHA-256 is
`a131b79e61a1dc612778f78234317ca1ac06e81b3727e689504951d03c04925a`.
Container CPU collection was incomplete: all six roles reported `cpu_stat`
`ValueError` in each of 24 samples, giving **144 failed role/sample observations**.
The historical artifact did not retain raw `cpu.stat` text, so the exact failing
input cannot be reconstructed. It does not establish CPU/GIL or database
saturation, complete resource coverage, or full capacity acceptance.

The earlier automatic 60-event run [Actions run 36380519394](https://github.com/GU2thousand/labops-erp/actions/runs/36380519394),
at commit `b9f574e91c55a2ed5c7ca7f61d56c40042fbceb8`, used diagnostics before the
new opt-in default. Its inclusive **7.133691250s** generation clock included
**1.171047299s** of resource discovery, producing **8.410792940/s** against 10/s
and a failing rate gate. All 60 event/effect IDs reconciled, but diagnostic CPU
coverage was incomplete. Retained `report.json` SHA-256 is
`355777361ce898ab35f3afa84edb62848bfc95b71712bf63ef0412a7f7bbe0fa`.
The new default does not remove discovery time from that historical result or
relabel either failed run as a pass.

At source head `f3cb2e11ab859b4fcd51a232054c8d145b089fba`, the
[default smoke run 36381732572](https://github.com/GU2thousand/labops-erp/actions/runs/36381732572)
ran PR checkout `35d00e2df5b71a8819ff212e488869578003991d` and passed **30/30**
recorded cases with 173 unique inventory events; diagnostics were `NOT_REQUESTED`.
Its `report.json` SHA-256 is
`ff2c545b4899791c51d9ea163b29d610ddc51289794a7f4177e7f396919bb569`.
The same source's [512-command thread diagnostic 36381740390](https://github.com/GU2thousand/labops-erp/actions/runs/36381740390)
completed in **14.897411965s**, an actual **34.368385677/s** against 50/s, so
throughput remained **FAIL**. All 512 published/effect IDs were complete; its
14 samples contained 84 valid CPU/memory role observations (six roles each),
with diagnostic collection/lifecycle complete. The failing report SHA-256 is
`f7f8c06f15ddeaf26d82de4f5e9d34f3c99038995aa6b23abe2286d99813cdb4`.
These are thread-mode results, not evidence for the new spawn execution policy.

The four-lane profile has no claimed hosted/full 50/s pass in this document.
A local SQL profile of 200 application-service commands is implementation
profiling, not an RF3 workload or hosted/production acceptance result. Full RF3,
independent-domain staging and production acceptance remain pending executed
evidence at their frozen denominators and clock boundaries.

### Measurement-only capacity diagnosis

An explicitly enabled bounded diagnostic keeps the selected writer topology,
queue capacity four, legal business services/locks, global 50/s target and 5%
completion-window gate. A
512-event request is a diagnostic input, not the 90,000-event acceptance target.
It may fail the rate gate and still retain useful raw measurements. A diagnostic
does not qualify a slower input rate as 50/s or establish long-run capacity.

The independent `--diagnostic-profile` opt-in and workflow `diagnostic_profile`
input default to false; the existing runtime diagnostics policy is unchanged.
A manual 512-command function-profile request at 50/s uses the default four spawn
lanes and queue capacity four; a nondefault writer or consumer profile with
function profiling ON is rejected before setup. Enabling profiling always sets
`qualification_admissible=false`,
even if business/count/rate/latency gates pass or profiling errors trigger a
fallback. Profile startup, export, cleanup and join consume the elapsed clock;
each generator must persist its required profile before owning database cleanup
and join, and publisher lifecycle output/shutdown consumes total elapsed time.
Missing profile evidence after SIGKILL remains incomplete.

The profiler accumulates cProfile own-thread CPU for each of the four generators
and one real publisher lifecycle. It exports full sanitized function/caller JSON
with hash-safe paths/callers, never raw pstats, SQL, arguments, locals or environment
variables. Fixed phases report wall and thread CPU; nested CPU is not additive,
and own-thread CPU excludes other threads, background work and librdkafka. The
publisher send phase combines encode, produce, flush and callbacks; the inline
conditional mark remains an unclassified residual. The business-code baseline
remains `06c`: its actual 3,000-command run took **78.187971851s** at
**38.369073004/s**, with notification p95 **6.843565941s**; rate and notification
p95 were **FAIL**. Profiling supplies diagnosis, without a performance forecast
or full-capacity qualification.

When diagnostics are enabled, the frozen execution profile declares the
one-second resource observer and its sample/request bounds before setup. The
initial `runtime-resource-profile.json` records the requested/not-started scope;
each enabled batch's `runtime-resource-profile-NNN.json` records
actual isolated-container identities and effective CPU/memory limits without
environment variables. Each concurrent batch retains
`runtime-diagnostics-NNN.json`: process user/system CPU versus wall time, safe
host/process resources, PostgreSQL state/wait/blocking-backend samples and
container CPU/memory/I/O counters when the host exposes their Linux cgroups.
Unavailable counters and failed samples are explicit unknowns. A final artifact
does not substitute zero for an unavailable measurement or infer a cause from
`cpu_count` alone.

Thread `command-diagnostics.jsonl` and enabled spawn
`origins/.../command-diagnostics.jsonl` link every attempted command's global index, lane
and kind to wall/thread/process CPU, SQL client-call count/wall time and physical
commit count/wall time. SQL text, bind parameters, credentials and endpoint
configuration are excluded. Client SQL wall time includes network/client
decoding and Python scheduling; it is not pure PostgreSQL server execution time
or a direct GIL-wait measurement. Per-command process CPU includes all concurrent
threads and must not be summed as independent lane CPU.

Enabled spawn diagnostics register only the exact owned generator and current
Kafka-worker PIDs, validating process start identity before CPU/RSS attribution;
they do not scan arbitrary processes. `process-resource-profile-NNN.json`
freezes the managed-process role/sampling policy. Actual PID/start identities
remain in runtime diagnostic samples and topology/schedule receipts. Generator
READY counters are child-origin receipts checked against a fresh live kernel
read; final receipts validate the same registered process identity and preceding
counters. Their CPU delta covers READY to the pre-exit final receipt,
excluding pre-READY init and later IPC/reap; the capacity elapsed still includes
both. A retained final receipt is not a post-exit live kernel observation.
CPU includes each process's threads and excludes descendants; RSS is approximate
current residency. Missing/reused/stopped identities remain explicit unknowns.

All enabled fresh Docker/process discovery, observer startup/sampling, joined
owning-thread connection and sampler cleanup, required raw artifact
persistence and summary calculation consume the generation clock along with the business work. The
final topology metadata rewrite reports the already measured completion boundary.
Record
observer overhead separately; it receives no extra capacity allowance. The
observer uses its own bounded database connection and performs read-only
statistics queries. Before each capacity batch, resource discovery freezes the
current isolated process identities, including explicitly inspected stopped
roles during broker fault batches. Steady diagnostic collection requires its
finite numeric CPU/database/resource observations and `collection_complete=true`;
an incomplete steady sample
cannot support a passing measurement. Fault batches preserve optional missing
resource observations and coverage limits without invalidating otherwise valid
business/fault results. Every enabled batch still requires
`lifecycle_complete=true`; missing fault resource coverage cannot excuse failed
observer startup, owning cleanup, joins or persistence. Such a failure fails
the affected scenario's topology and final report. Parent-side failures retain
their original exception and durable partial journal even if diagnostic
collection also fails; child failures retain their sanitized authored evidence.
Preserve the
original failed probes unchanged when running a new diagnostic revision.

The counter parser now accepts valid dotted numeric keys, including
`core_sched.force_idle_usec` emitted by the official
[Linux v6.17 cgroup implementation](https://github.com/torvalds/linux/blob/v6.17/kernel/cgroup/rstat.c#L700-L740),
while retaining strict two-token, duplicate-key and unsigned-integer checks.
That correction establishes parser compatibility with the declared grammar;
it does not recover the omitted historical input or prove online CPU coverage.
The retained `f3cb2e1` thread diagnostic subsequently established numeric coverage
for its measured samples. New spawn CPU coverage and capacity remain unproved.
Diagnosis remains separate from the full 90,000-event capacity gate; reduced
smoke inputs remain capacity-unqualified and all full denominators/windows stay
unchanged.

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
  requested-profile.json   # numeric request and writer ID/version before env/setup
  writer-topology.json     # exclusively frozen writer preset/version/L/constraints
  generation-execution-profile.json # frozen selected lanes/cycle/queue/rate model
  consumer-topology.json   # preset/member/partition/ACK contract and selected writer_lanes
  worker-processes.jsonl   # role/generation/PID/start/client/log/delivery/metrics identity
  consumer-pool-readiness.jsonl # exact owned STABLE assignment and live-process proof
  group-assignment-observations.jsonl # raw membership, failures and deadline outcomes
  worker-close-observations.jsonl # owning close receipts, exits and intentional faults
  worker-session-settlement.jsonl # exact owned PostgreSQL application namespace
  business-lane-topology.json # actual independent aggregates and shared context
  generation-topology-*.json # per-batch actual lane counts, journal batches/results
  generation-schedule-*.jsonl # incremental scheduler/worker observations
  process-generation-plan-*.json # selected spawn reservations and safe scope
  process-resource-profile-*.json # enabled managed-process role/sampling policy
  origin-reconciliation.jsonl # conditional settled database facts/unknowns
  origins/process_batch_NNN_lane_L/
    origin-plan.json
    origin-journal.jsonl     # exclusive child attempt/commit/observation evidence
    events.jsonl
    command-diagnostics.jsonl # enabled child command measurements
  runtime-resource-profile.json # requested/not-started or not-requested scope
  runtime-resource-profile-*.json # enabled batch's fresh scoped discovery
  runtime-diagnostics-*.json # enabled raw samples, unknowns and lifecycle/coverage
  command-diagnostics.jsonl # enabled command timings/physical commit observations
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

The execution profile is likewise frozen before environment/setup. New requests
retain both validated writer ID/version fields across dependent profiles; only
an absent legacy pair resolves to default four. Actual lane
and per-batch topology evidence must match that profile; generation completion
and the final report include all topology pass/failure outcomes. The package
retains a failed or partial schedule, not just successful event-log entries.
The requested profile includes the `runtime_diagnostics_enabled` boolean. A
disabled run retains `NOT_REQUESTED`/not-applicable status and does not fabricate
raw diagnostic artifacts or complete numeric collection.

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

Migration `0007_outbox_active_ordered_index` adds the nonunique
`outbox_active_created_id_idx` on `(created_at, id)` for Kafka PENDING/PROCESSING
rows. It preserves every existing index and the publisher's due, lease,
dependency, shard and row-lock predicates. PostgreSQL creation and removal run
concurrently outside a transaction; SQLite uses its normal partial-index DDL.
Concurrent creation can wait for older transactions and adds database/WAL work;
the index alone establishes no workload rate or latency result.

A same-name object, including an invalid interrupted build or a valid build
whose migration record was not written, stops migration rather than being
silently reused or deleted. Inspect the public index's target table OID,
definition and validity before recovery. For a verified leftover from this
migration, explicitly drop that index concurrently, then rerun the migration;
preserve a different same-name object and resolve the collision separately.
Binary rollback can retain this additive index. Database rollback to `0006`
removes only the matching expected index concurrently; a missing or mismatched
catalog definition stops rollback. It does not remove existing indexes, change
business data, or alter acceptance gates.

Consumer deduplication retains its insertion savepoint. On the ordinary
PostgreSQL five-field `ProcessedEvent` model, it inserts with the specific
`consumer_event_unique` arbiter and reads an existing marker in a fresh statement
when the insert does nothing. Any actual insertion `IntegrityError` rolls back
that savepoint before the fresh lookup; if the pair is absent, the original
exception is rethrown. If a concurrently deleted pair disappears after
`DO NOTHING`, there is no original SQL exception: the consumer raises a new
`IntegrityError` to retain permanent failure classification and durable parking.

Admission is checked on every operation. Custom model/base lifecycle,
managers/querysets, fields/defaults/descriptors, routing, backend insert behavior,
or effective global/sender init/save listeners use the original ORM
`get_or_create` once. Canonical callable provenance and current code identities
also reject ordinary extensions installed before or after module import. The
insert path accepts only the existing two consumer names, UUID identity and
canonical checksum shape. Legacy NULL/empty hashes still backfill through the
original ORM save. Notification insertion, business validation/reads, analytics
locks and Fixed6 effects, constraint validation, outer durable transaction,
processing limits, partition ownership and per-record synchronous offset commit
remain unchanged. This is one fewer normal new-marker SELECT, not a measured
throughput or latency improvement; the frozen acceptance gates still apply.
The optimized path uses the managed default Read Committed session. Configured
isolation/startup overrides and nonstandard wrapper/native isolation metadata
fall back without an admission query. Arbitrary SQL `SET TRANSACTION` or session
isolation changes outside the managed worker contract are not established by
these metadata checks and are outside this optimized-path support boundary.
