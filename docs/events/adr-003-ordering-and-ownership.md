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

Optional publisher shards deterministically assign movement UUIDs. Supervise one
active owner per shard assignment; change shard count at a drained boundary and
record it. A shard assignment is application scheduling, not broker fencing.

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
