# ADR 001: PostgreSQL ledger and retained relations define recovery truth

Status: accepted implementation boundary; production recovery performance remains
unmeasured. Decision date: 2026-09-27.

StockMovement, its lines, balances, business audit and original inventory
OutboxEvent commit in one PostgreSQL transaction. Kafka delivery is asynchronous
and at least once. Broker outage cannot reverse a committed command or make a
client believe a committed inventory command was rejected merely because an
outbox publisher is unavailable.

StockBalance and the posted ledger authorize stock commands. InventoryProjection
is an eventual read model; it never authorizes an issue. Notification and
analytics effects commit atomically with `(consumer_name,event_id)` and its
immutable checksum in this same database. This establishes at most one database
effect per consumer/event, not cross-system exactly once.

## Recovery decision

The inventory envelope contains deltas and notification recipients, not complete
business commands, purchase allocations, master data, authorization decisions or
all ledger relations. Therefore retained Kafka records cannot reconstruct business
truth lost after an older PostgreSQL snapshot.

Prefer PostgreSQL PITR to a documented business watermark. If only an older dump
is available, report the lost committed business range and actual RPO. Isolate
post-watermark broker records for investigation; never blindly apply their deltas
as proof of a missing legal ledger mutation.

Restore retained Batch/Warehouse/User/master records together with ledger,
OutboxEvent, ProcessedEvent, FailedDelivery, DeliveryAudit and notification state.
Rebuild analytics from the restored valid POSTED ledger, then checkpoint retained
inventory outbox IDs/checksums represented in that ledger. Resume later valid
events only across an explicit consistent watermark. Notification recovery uses
original outbox/user relations and dedupe state. Missing original outbox or
master references is an isolation condition.

FailedDelivery plus append-only DeliveryAudit are the durable failure/recovery
record. DLQ is a mirrored inspection stream with potentially duplicate records;
its loss is not permission to discard database recovery state.

## Consequences

Projection rebuild is an explicit maintenance command behind stopped writes and
analytics coordination, never a startup hook. Retention is initially conservative:
retain all dedupe/outbox/audit records until a tested replay window and audited
cleanup policy exist. Topic/group offsets are separate recovery evidence; resetting
them does not undo a database effect. Kafka retention expiry does not prevent
rebuilding analytics from the ledger, but missing outbox/user/history can prevent
notification recovery. External notification providers will require a separate
outbox, provider idempotency key and receipt strategy.

Execute the [restore runbook](restore.md) in an isolated environment before an
actual cutover. A PostgreSQL dump restore test is not a PITR deployment or an
independent-AZ disaster-recovery measurement.
