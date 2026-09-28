# Inventory v1 event catalog

All six types describe an already POSTED StockMovement. The ledger mutation,
balances, business audit and inventory outbox commit together. Consumers never
use these messages to authorize a new stock issue or to recreate missing ledger
truth after a database restore.

| Event type | `movement_type` | Business trigger | Analytics effect |
|---|---|---|---|
| `inventory.opening.posted` | `OPENING` | Initial stock document is posted | Apply its signed line deltas |
| `inventory.receipt.posted` | `RECEIPT` | Goods receipt is posted | Apply positive receipt deltas |
| `inventory.issue.posted` | `ISSUE` | Authorized issue is posted | Apply negative issue deltas |
| `inventory.transfer.posted` | `TRANSFER` | Warehouse transfer is posted | Apply source and destination deltas |
| `inventory.adjustment.posted` | `ADJUSTMENT` | Authorized count adjustment is posted | Apply signed count differences |
| `inventory.reversal.posted` | `REVERSAL` | A legal reversal document is posted | Apply reversal deltas; original event remains immutable |

Every event supports notification delivery to the retained recipient user IDs.
The current effect is a row in the same database, unique by `(event_id,user_id)`;
inactive or absent users are not notified. This is not external email delivery.

## Envelope and validation

Topic defaults are `labops.inventory.v1` and `labops.inventory.dlq.v1`; isolated
runs override topic/group prefixes. Key: `stockmovement:<aggregate_id>`.

| Field | Meaning and v1 validation |
|---|---|
| `event_id` | UUID of original OutboxEvent; stable for every publish/replay |
| `event_type` | One of the six catalog entries; no unknown type |
| `aggregate_type` | Exactly `stockmovement` |
| `aggregate_id` | UUID of the movement; equals `payload.movement_id` |
| `aggregate_version` | Positive integer movement version, maximum 2,147,483,647; not warehouse sequence |
| `schema_version` | Exactly integer `1`; booleans and unknown versions rejected |
| `occurred_at` | Original outbox timestamp, ISO-8601 UTC (`Z` or `+00:00`), at most six fractional digits |
| `trace_context` | Known optional string map; up to 32 entries, 64-character nonempty keys and 2,048-character values |
| `payload.movement_id` | UUID of the retained movement |
| `payload.movement_type` | Uppercase catalog kind, matching event type |
| `payload.title`, `body` | Title 1-160 characters; body at most 4,096; no NUL |
| `payload.recipients` | At most 1,000 unique UUID strings |
| `payload.lines` | 1-1,000 entries; each has batch/warehouse UUID, delta and unit cost |
| `payload.lines[].delta_qty` | Nonzero finite plain decimal string, at most six fractional digits, `NUMERIC(18,6)` range |
| `payload.lines[].unit_cost` | Nonnegative finite decimal string, same range |
| `payload._trace_context` | Known optional retained trace carrier, same map limits |

Default application maximum is 262,144 UTF-8 bytes, subject to the configured
`EVENT_MAX_PAYLOAD_BYTES`; an application cap remains distinct from broker message
limits. Unknown envelope/payload/line fields are rejected. NaN, Infinity,
scientific decimal notation, excess precision, duplicate recipients and malformed
UUIDs are rejected before any consumer marker or effect.

The complete retained JSON envelope is hashed, including both trace carriers and
all known optional fields. See [immutable-contract ADR](adr-002-immutable-contracts.md).
Schema files and fixtures are executable contracts, while this table explains
their business meaning.

## Dependencies and failure recovery

Each consumer verifies the original inventory OutboxEvent and immutable checksum.
Analytics needs retained Batch and Warehouse relations. Notification needs the
original outbox, retained users and its dedupe/notification state. Missing original
outbox or invalid analytics FK relations are isolated rather than promoted into
new business facts. Notification skips absent/inactive users; restore preflight
must verify the intended recipient/user history before accepting that behavior
as a complete notification recovery.

Failure classes are transient dependencies, permanent schema/data errors, and
authorization errors. FailedDelivery records retain the original content/hash,
consumer, source cluster, stream generation and topic/partition/offset identity.
Only a committed effect or a committed failure record permits offset commit.

DLQ v1 is an inspection mirror containing stable `delivery_id`, consumer/source
identity, original hash and envelope, attempt/error/classification information.
Duplicate DLQ records are possible after acknowledgement and before database
writeback. The FailedDelivery and append-only DeliveryAudit rows are the recovery
truth. Raw poison payload and recipient/trace fields need restricted access.
