# Finite publisher phase observation

This is an L1 diagnostic design. It does not change inventory, publisher or
consumer behavior, qualify capacity, or replace the RF3 matrix. Existing
function-profiling admission still requires four writers and one notification
consumer. The new `publisher_observation` workflow input selects a separate
finite observer, with function profiling OFF.

The first declared experiment uses one publisher, `writers-6`,
`notification-dual` plus one analytics consumer, the standard public
`ubuntu-24.04-arm` runner (four CPU), 3,000 legal inventory commands at 50/s,
60 seconds, runtime diagnostics ON, and an at most 900-second drain. Broker,
PostgreSQL and client versions remain the source-pinned validation versions.
The existing four-operation business cycle, lane assignment, seed fixtures,
durable input journal and generation clock remain in force. The runner-profile,
consumer-topology and writer-topology artifacts freeze actual resources and
configuration. Source revision and observer source hashes are persisted before
execution. No prior failed artifact is overwritten.

The observer records bounded synthetic event UUIDs and absolute epoch,
monotonic and own-thread CPU clocks around existing calls. For the ordinary ORM
path it separates claim SELECT execution, model construction, lease write,
physical transaction commit, the original delivery callback, and final
publication UPDATE. The single-record PostgreSQL CTE path instead records
combined atomic claim driver execution inside a raw-materialization composite;
that composite also includes SQL construction, atomic entry/exit and commit.
It does not infer separate SELECT, lease-write or model-conversion timings.
On PostgreSQL with the validated native helper present, the observer leaves
model lifecycle methods untouched so it cannot force the production admission
to fall back. No additional production admission call is made at bootstrap.
Only an actual helper invocation marks a native attempt. An ORM fallback on
that strategy retains its missing model boundaries and remains INCOMPLETE.
Query execution does not isolate server execution from network or driver work;
fetch and unobserved Python residuals retain their explicit limits. Parent IDs preserve
nested intervals: inclusive stages must not be summed as independent costs.
Schema 2 samples the exact command `operation_deadline` context: its original
factory, entry, body and exit. The outer ownership assertion and timeout-budget
setup/restoration belong to that scope, along with `publish_one`, its two inner
ownership checks, claim and delivery. This scope excludes heartbeat, loop
bookkeeping, idle wait and producer shutdown; it is not the entire publisher
cycle. Calls made directly to `publish_one` retain a separately labelled legacy
scope. Deadline attempts have their own monotonic ordinal and unique attempt ID;
the function profiler's publish-only counter remains unchanged. Event identity
is bound after claim and backfilled onto already recorded deadline, owner and
budget rows without loading deferred fields.

The observer records budget setup/restore driver calls on the application
connection and ownership driver calls on the owner's existing dedicated
PostgreSQL connection. It does not establish ownership, connect, reconnect or
issue extra statements. Counts come from actual execute callbacks, including
failed calls; setup's statement count is not assumed. Unknown owner connections
and context managers delegate unchanged and report unavailable boundaries.
Unknown context managers use Python's original `with` protocol. Known standard
generator context managers cache their type-level exit before entering, preserve
suppression and exceptions, and invoke the original factory/entry/exit once.
The production deadline control propagates even when its one-shot signal lands
in an observer clock or error sink. An entered context still exits once if setup
measurement finalization or restoration measurement entry is interrupted;
secondary recording controls preserve an already known body exception.
Failed setup, owner loss, deadline failures, empty claims and ORM fallbacks remain
explicit bounded outcomes. A successful application return cannot hide a failed
budget restoration execute. SQL fetching, connection initialization and server
cost remain outside separate execute measurements. Unattributed residuals remain
explicit.

Each sample exports `publish_result` as the original exact boolean or null and
`publish_result_status` as `not_invoked`, `no_return`, `observed_bool` or
`unsupported_nonbool`. It never invokes an unknown result's truthiness. The
sample's `outcome` describes the enclosing context, independently of that
publication result. `published_native_postgresql` / `published_orm` require the
original True plus successful required boundaries and context cleanup. False
after a nonempty claim is `publish_returned_false` and incomplete; an empty
claim's original False remains `empty_claim`. Unsupported nonboolean returns
are `publish_result_unavailable`. A True with missing publication boundaries
is `publication_incomplete`; a failed restore remains `budget_restore_failed`
even when the original budget context catches the database error and returns.

The finite limits are 4,096 attempts and 131,072 records. The previous 65,536
record limit could overflow a 3,000-event probe after adding deadline, budget and
three ownership SQL boundaries. Both overflows remain visible and make coverage
incomplete; no extra workload is executed to compensate. The added clocks,
wrappers and records have unmeasured total instrumentation overhead. Inclusive
parent/child times still must not be added.

The top-level `writer_roles=4` field is the function profiling guard template.
It does not describe an observation-only run's actual writer count. The observer
exports `actual_topology=NOT_OBSERVED_IN_PROFILE`; runner, writer and consumer
topology manifests are the authoritative evidence. This clarification does not
change function profiling's existing four-writer/single-consumer protections.
Only admitted repository/model/owned-client call sites are instrumented.
Unknown call sites, missing IDs, overflow, clock failures and restoration errors
remain visible and prevent complete-observation claims. Function graphs are
NOT_REQUESTED and their coverage is never reported as a complete profile.

Authenticated worker metrics are sampled every two seconds before generation
through backlog drain, with one final sample before workers stop. Each raw
sample has its own file, timestamp, byte count and hash. Failed scrapes retain
their worker and time denominator. The observer exports clock overhead counts;
the entire observation and metric-sampling overhead remains part of this
diagnostic. No unobserved counterfactual overhead or causal speedup is claimed.

The probe uses the real harness's drain, reconciliation, native synchronous
consumer ACKs, worker cleanup and final offsets. Its steady rate and latency
gates remain 63 seconds and per-consumer p95/p99 of 5/15 seconds. Gate failures
are retained even when observation completes. All fault cases are intentionally
unexecuted and explicitly excluded from qualification. `report.json` always
has `qualification_admissible=false` and `passed=false`; a diagnostic workflow
can succeed only for complete observation and durable final accounting. The
dedicated `publisher-probe-result.json` distinguishes that result from capacity.

Use the resulting same-topology stage distributions to choose one minimal
change with a measurable hypothesis. Re-run affected real PostgreSQL tests,
regression, full stack and RF3 correctness for its SHA before an unprofiled
3,000-event, 30-case capacity run. Do not lower gates, weaken fencing or ACKs,
compare a four-writer composite as a six-writer stage, or retry a business
capacity failure to select a passing result.
