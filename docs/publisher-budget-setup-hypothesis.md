# Single-statement publisher timeout setup hypothesis

This L2 candidate combines the application connection's prior-timeout read and
session-timeout setters into one PostgreSQL statement. The shared
`database_statement_budget` helper now executes one setup statement and the
existing restore statement, rather than two setup statements and one restore.
Publisher and DLQ users keep their per-record scopes. No throughput or latency
improvement is established by reducing this statement count.

## Evidence and measurement limits

The first uninstrumented six-writer, dual-notification capacity attempt at
`1cc66e1b929b067e5804d1e666c74cb88d2477e9`,
[run 36529451258](https://github.com/GU2thousand/labops-erp/actions/runs/36529451258),
failed its business latency gates. Generation was 61.406261179 seconds against
63, but notification p95/p99 was 9.552397966/9.875026941 seconds and analytics
was 19.425207853/19.827255011 seconds against the unchanged 5/15-second gates.
All 3,000 commands committed and published, both consumers persisted 3,000
effects, and reconciliation had zero mismatches. One case failed and 29 were
unreached. That original failure remains evidence and must not be retried to
select a passing result.

Source inspection identifies three owner-validation SQL statements and three
timeout statements per successful ordinary publisher operation. The outer
owner validation and timeout setup/restoration are outside the finite
`publish_one` event observation. The first capacity run had publisher
observation, function profiling and runtime diagnostics OFF; its retained
broker metrics do not measure these boundaries. Application `published_at`
values are publication-mark timestamps, not physical publication commits or
broker ACK timestamps. A later diagnostic's actual native-path receipts are
separate from this failed uninstrumented attempt, and inclusive diagnostic
spans cannot be added as independent costs.

The hypothesis is limited: one fewer application-connection round trip per
attempt may reduce complete publisher-loop time. Setup also has one fewer
implicit autocommit transaction. Their cost and causal effect on the capacity
gates must be measured; no numerical saving is forecast. Consumer changes,
owner-check caching, batching and altered budgets are outside this candidate.

## Setup and failure behavior

The PostgreSQL setup uses a single-row `previous AS MATERIALIZED` CTE to
capture both old settings before the outer SELECT applies the two new values.
The four returned fields contain old statement/lock settings followed by the
two setter results; Python retains the first two for exact restoration.
Timeout values remain driver-bound parameters, including their existing
millisecond conversion. Session scope remains `set_config(..., false)`.

PostgreSQL documents [MATERIALIZED as forcing separate CTE calculation](https://www.postgresql.org/docs/17/queries-with.html#QUERIES-WITH-CTE-MATERIALIZATION)
and [set_config's session and transaction scopes](https://www.postgresql.org/docs/17/functions-admin.html#FUNCTIONS-ADMIN-SET).
The explicit materialization is necessary: placing reads and setters in an
ordinary target list would rely on expression evaluation order. The two
setters address different settings and need no ordering relative to each
other. The setup statement starts under the previously active server timeout;
this change does not claim the new timeout bounds setup itself.

The combined statement introduces a budget-integrity failure case: a network
or control exception can follow server-side application of settings before
Python has received both original values. Any setup cursor/execute/fetch/result
shape failure therefore discards the application connection and rethrows the
original `BaseException`, even if close raises a secondary error. This helper
does not enter the body or retry setup. The existing command loop retains its
logging, wait and application reconnect behavior after `DatabaseError`. This
setup-integrity safeguard is deliberately stronger than the old setup-error
path. It does not close or reacquire the independently owned publisher-shard
session.

A PostgreSQL server error in autocommit aborts the entire setup statement; the
tests observe old values on the same raw backend before the helper discards it.
An explicit caller transaction must unwind after an error; reading a broken
transaction is not proof of restoration. A closed setup session is not reported
as a preserved backend or as restoration of its prior custom settings.

After successful setup, the restore SQL, saved exact values and existing body
exception behavior remain unchanged. A restore `DatabaseError` closes the
application connection without replacing an active body error. The non-
PostgreSQL path remains query-free. Transaction-local processing budgets remain
in their original atomic helper. The helper adds no transaction spanning Kafka.

The publisher's outer, pre-send and post-ACK ownership checks, event lease/token
fences, native claim admission, immediate producer flush/delivery ACK,
conditional publication writeback, worker deadline and shutdown behavior remain
in their existing call sites.

## Required validation and qualification

The affected tests exercise the real PostgreSQL backend in both current client
and server binding modes. They check nondefault `37s`/`91ms` originals,
fractional `1250ms` setup, one setup plus one restore, exact old values and the
same backend on success/body errors, nested session budgets, outer atomic and
savepoint completion/rollback, and original deadline-control exceptions.

Failure cases include an actual invalid second setter (SQLSTATE 22023), an
actual successful setup followed by fetch failure or a malformed result,
application-session discard while the dedicated owner remains held, and the
existing terminated-backend restore proof. Real `pg_sleep` statement timeout
(57014) and a competing row-lock timeout (55P03) verify that the active budgets
still cancel operations and allow exact restoration followed by another query.
Owned connection copies and held locks are always closed/released.

These are required source-bound correctness checks; authoring them does not
constitute a passing execution. After all writers freeze, retain the affected
and full PostgreSQL results with before/after hashes. Complete hosted regression,
full-stack runtime and RF3 correctness for the frozen revision.

A same-topology finite diagnostic must record the deadline context around
outer owner validation, setup, `publish_one` and restoration, actual native/ORM
claim path and the correct dedicated versus application connection. Heartbeat,
loop bookkeeping, idle wait and shutdown remain outside that measured context. Keep its
instrumentation overhead explicit and qualification false. Then perform the
next declared uninstrumented six-writer/dual capacity attempt once, preserving
the first failure, every reached/unreached denominator, unchanged thresholds,
ACK guarantees and PR draft status until all required gates pass.
