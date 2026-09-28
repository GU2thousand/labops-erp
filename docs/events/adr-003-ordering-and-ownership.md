# ADR 003: Movement ordering, leased ownership, and legal duplicates

Status: accepted, with an explicit stale-sender window. Decision date: 2026-09-27.

Kafka keys are `stockmovement:<movement UUID>`. `aggregate_version` is the version
of that movement. It is not a sequence for a batch, warehouse or the whole
inventory ledger. Earlier unpublished versions of the same aggregate block later
versions; unrelated aggregates may publish concurrently. Keep three partitions
until measured capacity or hotspot evidence justifies a reviewed mapping change.

Claims use short PostgreSQL transactions, `SKIP LOCKED`, random lease tokens and
bounded leases. Publishing occurs outside the transaction. Ownership is checked
before send; the lease exceeds queue wait, flush acknowledgement and database
write budget. Ack-only writeback matches the active token and checks affected row
count. A replaced/expired owner cannot mark a new owner's row published.

Optional publisher shards deterministically assign movement UUIDs. Each publisher
holds a PostgreSQL session advisory lock for its shard on a dedicated physical
connection, separate from application transaction/reconnect handling. Before
producing and again after broker acknowledgement, it checks the original backend
PID and the granted `ExclusiveLock` in `pg_locks`; a different backend or missing
lock is ownership loss. The loop exits on session loss and does not silently
reacquire. It purges locally queued/in-flight producer records instead of flushing
additional stale sends. Purging cannot retract a record already accepted by the
broker or guarantee that an in-flight request will not be accepted.

Dedicated-session loss is a nonzero process exit, including with `--loop`.
Recovery requires a **new publisher process started by its supervisor** after
PostgreSQL is healthy; the old loop does not resume or reacquire ownership.
The new process takes a new session/shard lock and recovers eligible outbox rows
using their original IDs and normal lease expiry. Failed business connections
are closed before later attempts so a broken application connection does not
prevent checking the dedicated ownership session. Do not clear leases, markers
or failed rows to speed restart. Consumers similarly exit if PostgreSQL cannot
commit an effect or durable failure record; their supervisor restarts the process,
which reuses the same group and replays the uncommitted broker offset.

The root Compose application services use `restart: unless-stopped`. A
Kubernetes Deployment or equivalent production supervisor must restart failed
worker processes and report failures/restarts. A worker intentionally stopped
for maintenance requires an explicit start; restoring PostgreSQL alone does not
restart a manually stopped worker. Include process exit/restart and natural
lease recovery in outage acceptance evidence, separate from resuming a process
that stayed alive during a broker-only pause.

Use a direct PostgreSQL connection or session pooling for this ownership
connection. PgBouncer transaction pooling is incompatible with session advisory
ownership. The shard count and index are both part of the lock key and UUID
routing: **stop all publishers** and finish the drained cutover before changing
shard count, because differently sized shard assignments use different locks and
can otherwise overlap. Record the old/new count and watermark. SQLite supports
only one publisher process with shard count 1; larger counts are refused, and its
process-local guard provides no exclusion between separate processes. Shard
ownership is database-side scheduling, not hard Kafka producer fencing.

## Stale send and crash boundary

A database lease cannot revoke a Kafka request already queued or transmitted.
An owner may lose its lease after the pre-send check, publish a stale record and
fail database writeback. An acknowledged publish followed by a process crash also
permits another owner to publish the same original ID/content. Producer
idempotence reduces some retries within a producer session; it does not bridge
PostgreSQL and broker into one atomic transaction or fence all restarted owners.

The required recovery behavior is zero lost committed outbox events and zero
duplicate **database effects**, while recording legal duplicate broker records.
Consumers dedupe by original ID plus whole-envelope hash. Current analytics
deltas commute, and retry notifications may arrive after later notifications;
there is no strict total-order claim after retries or restore.

Future noncommutative event types need a stable business `aggregate_sequence`
allocated within the command transaction, gap/out-of-order detection and parked
processing that does not advance dependent effects. Hard producer fencing would
require a reviewed per-shard transactional producer design, verified against the
selected broker/client; it still would not make PostgreSQL exactly once with
Kafka. Do not reuse the present movement version as that future sequence.

Single-row callback/flush remains the initial acknowledgement strategy. Measure
its throughput first. Any later bounded batching must map each callback to its
outbox ID, mark only acknowledged rows, preserve immutable content and honor a
finite shutdown budget. Required crash drills include ack-before-writeback,
expired owner, owner recovery and both consumer transaction/offset boundaries.
