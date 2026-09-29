# Single-record PostgreSQL claim hypothesis

The selected L2 change replaces the ordinary PostgreSQL claim's SELECT and
lease UPDATE with one locked-candidate CTE and UPDATE RETURNING. It claims one
event at a time and keeps the transaction commit before the existing publisher
can send. Capacity remains unqualified until the new revision passes the
unprofiled six-writer, dual-notification smoke and all 30 cases.

## Evidence and limits

[The finite observation](https://github.com/GU2thousand/labops-erp/actions/runs/36524035293)
used `aa985e982a4d12c26dc261aca98fa69faaa2a3c3`, one publisher,
`writers-6`, `notification-dual`, one analytics consumer and the standard
four-CPU `ubuntu-24.04-arm` runner. It generated 3,000 legal commands at a
requested 50/s for 60 seconds, retained runtime metrics through drain and
disabled function profiling. Its fixed-clock observer remains overhead in the
measurement; this is not an uninstrumented baseline or a capacity result.

All 3,000 committed event IDs align with claim, native broker delivery ACK and
publication writeback. Both consumers completed 3,000 durable effects,
notification rows totalled 9,000, and ledger/balance/projection reconciliation
had zero mismatches. There were 3,011 returned publisher and claim attempts,
including 11 empty claims. The observation completed with no missing IDs,
overflow or clock failures; the function graph was NOT_REQUESTED.

| Measured boundary | Samples | Mean wall time |
| --- | ---: | ---: |
| Entire `publish_one` | 3,011 | 22.930880 ms |
| Claim and physical commit | 3,011 | 10.116900 ms |
| Claim SELECT driver execution | 3,011 | 2.200731 ms |
| Model construction | 3,000 | 0.045794 ms |
| Lease write composite | 3,000 | 2.141432 ms |
| Lease UPDATE driver execution, inside the lease composite | 3,000 | 1.241896 ms |
| Physical claim commit, inside the claim composite | 3,011 | 1.213439 ms |
| Claim residual after the union of direct child spans | 3,000 | 4.532312 ms |
| Send, including native delivery ACK validation | 3,000 | 3.152627 ms |
| Final publication UPDATE, including driver autocommit | 3,000 | 1.873403 ms |

These nested boundaries must not be added together. SELECT execution includes
client/server/network work and excludes fetch and conversion. The residual
includes observer and unsplit framework work; its own-thread CPU mean is
2.238192 ms and cannot be equated with removable cost. Clock reads alone cost
0.164307 own-thread CPU seconds across 81,088 reads; total instrumentation and
metric-sampling overhead is unmeasured. Outer ownership assertions, statement
budget setup/restore, idle wait and shutdown are outside the event observation.

Generation took 69.480980 seconds against the unchanged 63-second gate.
Notification p95/p99 was 23.825723/24.179293 seconds and analytics was
24.887103/25.294274 seconds against 5/15 seconds. No fault case ran in this
finite diagnostic. Its successful workflow means complete diagnostic
accounting, while `qualification_admissible=false` and `passed=false` remain.
The original earlier capacity failure is retained separately and is not
replaced by this observation.

## Testable change

The claim is the largest measured substage. A single atomic statement may
remove one query round trip and repeated ORM query/update preparation.
No numerical saving or causal speedup is promised. The next capacity result
will keep the same topology, runner, workload, thresholds and images while
disabling both function profiling and publisher observation.

The validation-only PostgreSQL, exporter and Prometheus references now pin
their registry index digests. Each registry ARM64 config digest was verified
against the actual image ID retained by run 36524035293 before this freeze;
all three are identical. Redpanda already uses the source-pinned image lock.
This freezes the same images rather than changing the measured image contents.
The Compose CPU/memory limits and standard runner selection stay in force.

The CTE filters Kafka transport, due PENDING or strictly expired
PROCESSING leases, the same aggregate/type predecessor dependency and the
publisher shard before sorting by `created_at, id`, limiting to one and
locking with SKIP LOCKED. The UPDATE changes only status, token and lease and
returns all persisted columns. Django's standard raw model iterable applies
the field converters and materializes a complete model. The existing shard
annotation remains; the temporary prior-status annotation is removed before
return. There is no batch, extra fetch, send, poll, flush or business retry.

Unknown model/field/manager/queryset extensions, lifecycle methods/signals,
routers and non-PostgreSQL configurations use the existing ORM path before
native execution. Native SQL, conversion and commit errors propagate without
an ORM retry. Owner and event-token fencing, broker delivery ACK validation,
publication writeback, consumer persistence/offset ACK and worker shutdown
remain in their existing paths.

A normal empty native attempt creates and discards one random UUID before
execution; the ORM creates a token only for a selected row. A patched UUID
source selects the ORM path and preserves its extension call counts. No token
is persisted for an empty native result.

The expired-lease diagnostic counter has one explicit error-path difference:
the native path learns the previous PROCESSING status only from a successful
RETURNING row, then increments before commit. A later rollback or commit
failure can still leave that diagnostic increment, as in the ORM path. An
UPDATE failure before a row is returned cannot increment it; the ORM path can
increment before its separate lease UPDATE fails. This counter is not the
claim authority, does not mask the error and does not alter recovery.

The diagnostic observer must record the path actually executed. For native
claims it avoids the model hooks that would force extension fallback, and
reports the combined atomic SQL execution plus raw materialization composite.
It does not invent separate SELECT, lease-write or model-conversion timings
for the CTE. Physical commit, actual native ACK and final autocommit remain
separate measured boundaries. Unknown or incomplete paths stay INCOMPLETE.

## Required validation and failure handling

Real PostgreSQL checks cover typed full rows and legacy hashes, strict expiry,
predecessor order, shard filtering before locks, disjoint SKIP LOCKED claims,
outer commit/rollback visibility, extension fallback, trigger/conversion and
commit errors, and no send after a failed claim. Existing owner-loss,
unused-claim, exception, SIGTERM/SIGKILL, resource-close, inventory-lock,
deduplication and reconciliation checks remain required for the exact source.

After independent review and affected PostgreSQL tests, freeze the candidate
SHA and run its complete hosted regression, full-stack runtime and RF3 smoke.
All three workflows explicitly check out the PR head SHA for PR events and
`github.sha` for push/dispatch, so the checked source agrees with the candidate
revision. Final main still requires its own post-merge run.
Then execute the declared uninstrumented six-writer, dual-notification capacity
smoke. Retain the first failure and all reached/unreached denominators; do not
retry a business rate/latency failure to select a passing run. Keep PR #2 draft
until all required cases and gates pass. Only then merge and qualify final
main before the full workload, independent hosts/AZ, target PostgreSQL PITR
and cutover stages.
