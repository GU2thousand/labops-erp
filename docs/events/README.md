# LabOps inventory event operations

PostgreSQL remains the inventory system of record. Kafka-compatible transport
delivers the six posted movement events to independent notification and analytics
consumers. Imports and ordinary alerts continue through `process_events`.

Start with the [frozen baseline](current-state.md), [event catalog](event-catalog.md),
[capacity assumptions](capacity.md), and [version matrix](compatibility.md).
The design decisions cover [business truth and recovery](adr-001-recovery-truth.md),
[immutable contracts](adr-002-immutable-contracts.md), and
[publisher ordering and ownership](adr-003-ordering-and-ownership.md).

For execution use the [security runbook](security.md),
[operator commands](operator-commands.md), [cutover](cutover.md), and
[restore](restore.md). [Acceptance](acceptance.md) records evidence requirements
and the remaining staging and production gates. An implemented harness or a
passing reduced CI run alone does not establish every plan threshold.

The [disposable RF3 environment](../../infra/events/validation/README.md) is three
broker processes on one host. Its process-failure evidence must be labeled
separately from independent-host/AZ staging and production observations.
