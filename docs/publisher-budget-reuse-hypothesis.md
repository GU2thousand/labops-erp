# Command-local publisher session-budget reuse hypothesis

This is one selected production hypothesis. It retains the application session's
statement and lock timeout budget across consecutive successful records in an
ordinary `publish_events` batch. It does not change the public
`database_statement_budget(seconds)` API, DLQ publishing, or arbitrary helper
callers. Its performance effect is unmeasured until exact-source hosted runs.

The prior observed setup and restore durations identify a cost to investigate;
inclusive diagnostic durations are not a savings forecast. The prior capacity
failure remains a failure and is not superseded by implementation or tests.

## Explicit behavior change

The application session GUCs remain visible between consecutive record deadline
contexts. Intermediate setup/restore statements and their failure sites disappear
on an admitted span. This is a controlled command policy, not transparent
per-record helper equivalence. No business transaction spans Kafka or multiple
records. No claim, ownership, lease, delivery, ACK, or writeback operation is
removed, retried, or reordered.

Every record still starts its own original `operation_deadline`, then checks the
dedicated shard owner before accessing the application session. `publish_one`
retains both inner owner checks, its lease test, synchronous broker send/ACK,
and fenced final update. Only an exact `True` return can retain a budget.
The current batch has at most 500 records and ends before the next heartbeat.
Limits outside 2..500 use the public per-record helper.

## Admission and captured identity

`PublisherBudgetAdmission` captures static function/code/descriptor capabilities
when the command imports. Each record checks their current identities without
SQL or a second call to native claim admission. Unknown command aliases,
producer/public method extensions, model/query lifecycle or field converters,
JSON encoders/decoders, signal listeners, backend operation overrides, and
execution wrappers use the public helper. The supported broker is the original
Client wrapper around the real confluent C Producer, with its original internal
error callback and delivery callback construction.

Tracing admission is deliberately restricted to the default ProxyTracer with no
installed provider or real tracer, its empty NoOpTracer, the standard
tracecontext+baggage CompositePropagator, and an empty standard context runtime.
Custom processors, propagators, active contexts, SDK tracing, and backend
instrumentation use the public helper. An empty OTLP environment variable alone
does not establish this admission. Profiling/phase-observation wrappers replace
command aliases and therefore retain the original per-record path; the command
emits one `publisher_budget_reuse_unsupported` message for this case. Such
diagnostics do not measure the optimized path.

`PublisherBatchBudget` requires the exact standard default PostgreSQL wrapper,
the exact psycopg Connection, the same physical raw object and backend PID,
the owning main thread, autocommit, an idle driver transaction, no atomic or
rollback state, no connection pooling/options extensions, no health-check
reconnect path, and standard row/cursor factories. Driver adapters are checked
against a finite frozen standard registry, including optimized binary adapters,
plus the standard Django timezone loader structure. No adapter factory is called
by admission or restoration. Unknown adapters and notice/notify callbacks fall
back. The existing deployment contract requires direct PostgreSQL or session
pooling; a remote transaction pool cannot be identified by local metadata.

An unopened application session uses the public helper for that record. Once a
subsequent record has a known live session it can start a retained span. A new
wrapper/raw/PID starts from its own saved GUC baseline; saved values are never
applied to a replacement backend. Current budget parameters are compared at each
record; changed parameters end retention and use the literal helper arguments.

## Cleanup boundaries

Setup is the existing explicit MATERIALIZED read-before-set CTE. Setup execute,
fetch, or result-shape failure discards the captured session without a body,
retry, or restore from unknown values. Restore checks captured identity before
cursor creation and again before its setters. Health checks are excluded so a
cursor cannot silently reconnect between those checks.

The last, empty, failed, or stop-requested record restores inside its own record
deadline. An outer owner failure before entering the next budget record also
finishes the previous capture inside that record's deadline, then preserves the
existing stale-owner purge branch. Cleanup never reacquires ownership.

A stop between record deadlines is handled by batch-final cleanup before idle,
heartbeat, final broker flush/purge, or shutdown. That cleanup uses a deadline no
larger than both the database budget and shutdown time remaining. Zero remaining
time or an already raised control exception discards the captured session without
restore SQL. Discard invokes only the captured idempotent native `PGconn.finish`,
not a mutable wrapper/Python raw close callback. It quarantines only the matching
old wrapper and marks the raw object closed only if it still refers to that
physical handle, even when finish fails. A replacement raw/PGconn session and the
dedicated owner remain untouched. No SQL, reconnect, or new deadline is added.
A physical finish failure is visible as incomplete discard;
it does not establish that the old backend closed. Existing primary errors win.
If a one-shot control interrupts immediately before the native finish enters,
one additional idempotent call to that same captured local handle is attempted.
The first control still propagates; no SQL, backend replacement, new deadline,
or unbounded cleanup loop is introduced. A native return does not independently
establish server-side session settlement.

The first known body/owner/control exception wins over secondary restore, close,
or receipt errors. A newly raised control exception with no primary propagates.
Database restore errors discard, preserving the public helper's close-on-database
error behavior. Ordinary non-database restore errors on a successful record
propagate. No new operation time is granted to an expired record.

## Runtime receipt

Each batch with a private exact True return or a reused record, and anomalous
failed/stopped/partial batches with a private setup attempt, emits at most one
payload-free INFO message named `publisher_budget_reuse`, with fixed `schema=1` and outcome
`full`, `empty`, `partial`, `stopped`, or `failed`.

| Field | Meaning |
| --- | --- |
| `setup_attempts`, `setup_completed` | Private setup paths attempted / completed with known saved values |
| `reused_records` | Admitted record bodies entered with a retained capture |
| `admitted_records` | Private record bodies entered after successful setup or reuse |
| `returned_true`, `returned_false` | Exact literal returns observed from those private bodies |
| `restore_attempts`, `restore_completed` | Captured-session restore paths attempted / completed |
| `discard_attempts`, `discard_completed` | Discard operations attempted / native physical finish returned |
| `physical_finish_attempts` | Calls attempted on the saved native handle, including the single control cleanup retry |
| `fallback_records` | Records delegated to the public helper in that batch |

All counts are integers and describe executed paths, not intended eligibility.
Entered bodies may fail before a return. A setup attempt may fail before a body.
No payload, event ID, DSN, backend PID, credential, per-event log, extra SQL, or
high-cardinality metric is added. All-fallback and ordinary empty idle batches emit no receipt;
absence or missing/truncated worker logs cannot be treated as a measured zero.
Receipts must be bound to exact source, hosted run/job, worker log, and generator
lifecycle. They prove an observed path; they do not independently prove complete
log retention, durable success, latency acceptance, or RF3.
The message size and frequency are bounded. Its logger handler wall time is not
separately deadline-bounded; this does not assert arbitrary handlers are
nonblocking. Receipt errors preserve an existing primary exception, while a new
control exception without a primary propagates.

## Required proof groups

Tests are authored for root-owned serial execution, not claimed passed here.
Controller-only tests using a supplied admission callback are labeled as such;
they do not prove real producer admission.
Positive policy fixtures use the actual StopController and the captured ordinary
`ensure_connection` descriptor: Django's test-only inherited connection wrapper
is temporarily removed from these fixtures. A persistent instance cursor is
removed only when it is the exact original bound method left by Django test
teardown, then the same object is restored even after a fixture error. Unknown
cursor callbacks remain in place and are refused without invocation. There is
no production exemption for test wrappers or unknown connection capabilities.

- Real PostgreSQL, both client/server binding modes: nondefault GUC values,
  exact raw/PID identity, one setup across consecutive admitted records and one
  final restore, native positive admission and empty native claim, and literal
  fallback for unopened/nested/non-autocommit/unknown-wrapper paths.
- Setup fetch/shape failure, restore errors, exact primary identity, physical
  close failure/quarantine, replacement wrapper/raw/health-check boundaries,
  parameter changes, and no restore onto replacement sessions.
- Real one-shot SIGALRM during a body/restore, bounded stop-gap cleanup,
  SIGTERM/owner-loss ordering, no retry/reacquire, unchanged unused-claim leases,
  broker ACK and fenced writeback order, and existing arbitrary-helper tests.
- Static callback fallback before added SQL/getters: read conversion/JSON,
  no-op tracer/runtime instances, propagators/signals, broker extensions,
  function wrappers/default mutations, driver/backend extensions.

After affected and full actual PostgreSQL checks, run unchanged hosted regression,
full-stack fault validation, and default RF3 correctness smoke on the exact frozen
source. Reduced default RF3 topology is not capacity qualification. Preserve
failed runs, raw denominators, and receipt/log coverage limits. The unchanged
formal six-writer dual-stream workload and 30-row matrix remain separate capacity
acceptance requirements, distinct from diagnostic or unit-test success.
