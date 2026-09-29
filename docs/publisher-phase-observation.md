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
The observer samples inside `publish_one`; its outer ownership assertion,
timeout-budget setup/restoration, idle wait and producer shutdown are outside
that event scope. They remain unmeasured in this mode and cannot be folded into
claim or ACK cost. Unattributed residuals remain explicit.
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
