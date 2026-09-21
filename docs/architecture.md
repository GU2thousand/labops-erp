# LabOps transaction and event architecture

PostgreSQL owns business state, the stock ledger, command results, audit records, and the durable outbox. Kafka/Redpanda transports inventory events; Redis is optional acceleration and admission control. Neither authorizes stock changes. The local Compose topology is a development and validation environment, not a high-availability deployment.

Operational commands and recovery procedures are in [operations.md](operations.md). Validation results belong in the repository's validation report; the presence of a test or configuration is not evidence that a production failure scenario has been exercised.

## Business invariants

A posted inventory movement, its lines, all affected balances, its audit records, and its inventory outbox event commit in one database transaction. A failed line, insufficient balance, failed audit write, or failed outbox insertion rolls back the command. Posted lines are immutable through the application: corrections create a reversal with opposite deltas. Database uniqueness restricts a movement to one reversal and a receipt to one posting movement.

For each `(batch, warehouse)`, the authoritative balance equals the sum of posted ledger deltas. A balance has a unique row and cannot be negative. Transfers debit and credit in one transaction. Issues require an active project and an in-progress task; expired batches cannot be issued. Purchase allocation and receipt posting recheck remaining quantities while holding the corresponding request/order locks. Project and task transitions share those locks with dependent commands.

`reconcile_stock` compares ledger totals and balances. On PostgreSQL the comparison is a single statement, so both sides use one MVCC snapshot even while writers commit. `InventoryProjection` is a separate, eventually consistent analytics view; it is never used to decide whether stock can be issued.

These guarantees apply to the application's command paths. Ad hoc SQL, direct ORM mutations, migrations, and new services must preserve the same invariants and lock protocol. Database constraints provide a final defense for specific relationships and quantities; they do not encode the entire business workflow.

## Transactions and lock ordering

The application uses PostgreSQL's normal `READ COMMITTED` isolation, with explicit locks around multi-statement decisions. It does not enable database-wide serializable isolation. SQLite remains an explicitly selected demo backend using `BEGIN IMMEDIATE`; its behavior is not a substitute for PostgreSQL concurrency validation.

`atomic_command()` establishes the transaction, sets a five-second transaction-local lock timeout before acquiring the catalog gate, refreshes the actor, and checks that the account is still active. Nested service calls use savepoints within the owning transaction. Transaction-scoped advisory locks are released automatically on commit or rollback.

Every normal business command takes the catalog gate in shared mode. Catalog and user writes take it exclusively, including each import row's catalog update. This prevents item deactivation, unit changes, user changes, and dependent transactions from validating against incompatible concurrent state. It permits unrelated business commands to run together, but an exclusive catalog operation briefly blocks all such commands. It is a deliberate, remaining global coordination point.

The normal acquisition order is:

1. Shared or exclusive catalog gate.
2. Idempotency key, when the command has one.
3. Opening-stock state row, while opening stock is still available.
4. Draft document advisory lock, when editing or posting an issue draft.
5. Project, task, purchase request, purchase order, receipt, and movement rows, in that order; multiple rows of one type are sorted by primary key.
6. Balance advisory locks, sorted by `(batch_id, warehouse_id)`, followed by the corresponding balance row locks.

A balance advisory lock also covers the initially missing row, before `get_or_create`; locking only an existing row would leave concurrent creation unprotected. Physical-count adjustments acquire the balance lock before reading the quantity used to compute their delta. Draft edits lock the document selected by the route ID; an unrelated `draft_id` field in the request cannot redirect that lock.

Opening stock and the first receipt or issue coordinate through `RuntimeState`. Once opening stock closes, ordinary receipts and issues no longer update or lock that singleton on every posting. The close is rolled back if the triggering business command fails.

Project locks also protect project membership/status checks and sample workflows. Consequently, issues against different tasks or batches within the same project still serialize at the project row. This is more conservative than balance-only locking. Independent transfers do not need a project lock and can progress on different balances. Claims of universal parallelism across all orders, tasks, or batches would be inaccurate.

Only the outer transaction owner retries PostgreSQL deadlocks (`40P01`) and serialization failures (`40001`), with jitter and at most three total attempts. Nested calls propagate the error to that owner. A lock timeout or database connection failure is not automatically retried by this mechanism; the API returns a database-busy response. The five-second timeout bounds an individual lock wait, not total request duration. Background jobs that manage their own transactions do not all inherit `atomic_command()`'s timeout and retry policy.

The implementation combines application checks with unique/check constraints. Version comparisons are performed after the relevant business locks have been acquired. Adding another writer requires reviewing its entire read/check/write sequence and lock order, not merely adding a `version` field.

## Quantities, costs, and command identity

`Fixed6Field` stores quantities and unit costs as integer micro-units. It rejects non-finite values, more than six fractional digits, and scaled values outside the field's range. Model reads return `Decimal`; stock updates do not use binary floating point. A persisted quantity of `1.250000` is stored as `1250000`.

PostgreSQL overview queries cast integer operands to `numeric` before multiplication to avoid overflowing `bigint`. Quantities are multiplied by the exact numeric constant `0.000001`, and quantity-times-unit-cost values by `0.000000000001`. This avoids PostgreSQL numeric division choosing too few fractional places for a large result. Quantity/cost column precision and derived report precision are different concerns; changing aggregation code must retain the scaling and check the supported numeric range.

Creation commands use `CommandResult`: a stable key identifies a request hash containing actor, route/kind, and request data. The key is locked before lookup. Matching retries return the stored result; a conflicting hash is rejected. Inventory postings additionally retain a unique movement idempotency key and request hash. Receipt and reversal relationships supply separate uniqueness safeguards. Import previews have their own durable key, and import execution skips successful rows.

The idempotency records live in the same database transaction as their effects. If a response is lost after commit, a retry with the same identity can find the committed result. Callers must preserve keys and payloads when retrying. These records currently have no automatic expiry; deleting them changes the replay guarantee. Not every API mutation is a replayable creation command: version checks and valid state transitions still govern ordinary edits.

## Synchronous audit and durable events

Business audit records are written synchronously in the business transaction. The event pipeline is not the sole audit trail. The inventory outbox row is created in that same transaction, with a deduplication key based on the posted movement ID.

The version-1 event envelope contains:

- `event_id`, `event_type`, and `schema_version`.
- `aggregate_type`, `aggregate_id`, and `aggregate_version`.
- `occurred_at` and propagated `trace_context`.
- A payload containing the movement identity/type, notification recipients/text, and lines with batch, warehouse, quantity delta, and unit cost.

The aggregate is a stock movement. A normal posted movement currently emits one inventory event; its version must not be interpreted as a globally ordered inventory sequence. Quantities and costs travel as decimal strings.

Each row retains the transport chosen when it was created. `local` events are processed by the operations worker; `kafka` inventory events use the publisher and independent notification/analytics consumer groups. Changing the environment setting does not migrate existing events. Existing non-inventory alerts and workflow notifications continue through the local outbox worker.

## Publication, duplicates, and ordering

The publisher claims eligible Kafka outbox rows using `SELECT FOR UPDATE SKIP LOCKED`, records a random lease token, and commits a 60-second lease before contacting the broker. Completion or failure updates are conditional on that token, preventing a stale worker from replacing a newer lease owner's database state. Expired claims can be recovered.

The producer enables Kafka idempotence and requests acknowledgements from all configured replicas. The supplied single-broker development configuration has only one replica; `acks=all` does not turn it into replicated storage.

A crash after broker acknowledgement and before `PUBLISHED` is committed causes the same event ID to be published again. An expired publisher can also send after a replacement has acquired its lease. Database fencing protects outbox state, not the broker from late duplicate sends. Delivery is at least once, not end-to-end exactly once.

The broker key is `aggregate_type:aggregate_id`. The publisher prevents a known later version of an aggregate from being claimed while an earlier Kafka outbox version is unpublished, including when the earlier version is DEAD. This is not a global ordering guarantee. Duplicate publication, old workers, consumer retries, and manual replay can place an old event after a newer one. An earlier version that has never been inserted cannot be discovered by the claim query.

## Consumer effects, retries, and DLQ

Notification and analytics use independent consumer groups. Each database effect is committed together with a `ProcessedEvent` record whose unique key is `(consumer_name, event_id)`. A duplicate for the same consumer performs no second effect; another consumer can process the same event independently. PostgreSQL deferred constraints are explicitly checked inside the consumer transaction/savepoint so invalid references become a caught delivery failure, including when processing occurs inside a retry transaction.

Notification effects create database notifications. Analytics sums each event's deltas into projection rows under sorted projection locks. Deltas commute, so late retries do not require a contiguous event version or strict processing order. Temporary projection values can lag the ledger or be negative if debits arrive before the corresponding credits; they must not be displayed as an authoritative available balance.

The consumer disables automatic offset commit/store. It commits the broker offset only after either successful database processing or durable insertion of a `FailedDelivery` row. If persisting that failure fails, the offset is not committed. A crash after database commit but before offset commit leads to redelivery and deduplication.

Failed deliveries are keyed by consumer and broker topic/partition/offset. The retry worker locks eligible rows with `SKIP LOCKED`, attempts the original envelope, and persists attempts and errors. Scheduled delays are 60, 300, 900, and 3600 seconds. Exhausted consumer deliveries enter DEAD and are copied to the DLQ with a stable delivery ID and original event identity. Publisher failures use the same delay sequence but remain DEAD outbox rows; they are distinct from consumer DLQ records.

DLQ publication can itself duplicate after a crash. The database failure row is the recovery record. Manual retry preserves the event ID; manual resolution records an operator decision without applying the event. Parking a failed message allows the broker offset to advance, so zero consumer lag does not mean every business effect succeeded. Monitor unresolved database deliveries as well.

Atomic consumer effects cover this database only. Notification foreign keys and analytics references assume consumers use the same application database and retained source records. External email, webhooks, or another database would need their own delivery/idempotency design.

## Existing database cutover

Adding an empty analytics table to an existing ledger would omit historical balances. During an upgrade, back up and stop writers/consumers as described in the operations guide, migrate, run `rebuild_inventory_projection`, and reconcile before resuming work.

The rebuild takes the catalog gate exclusively, then an exclusive analytics-rebuild gate. Normal analytics processing takes the latter gate in shared mode. Inside one transaction the rebuild replaces projection rows with totals from the complete posted ledger and checkpoints all existing inventory event IDs as processed for analytics. Thus pending historical messages do not apply the same deltas again. Notification consumption is not checkpointed by this operation.

The command can be repeated, but it is an explicit maintenance operation and its locks can pause work for the duration of the rebuild. Recovery from a database snapshot also requires an explicit Kafka offset/retention plan: restoring a database does not rewind committed broker offsets. Do not delete source outbox identities or processed-event history without designing the resulting replay behavior.

## Redis and observability

The optional Redis cache stores the unfiltered active-item reference list under a generation key with a 60-second TTL. Item mutations increment the generation after database commit. Cache errors fall back to PostgreSQL; failed invalidation can leave an old cached result until expiry. The TTL is a cache-entry lifetime, not a linearizable read guarantee. All stock validation reads PostgreSQL regardless of cache state.

Configured Redis rate limiting uses an atomic token-bucket Lua script and Redis server time. Exhaustion returns 429; a Redis error returns 503 for protected login, import, and report operations. An unset `REDIS_URL` disables this optional limiter, while existing database login protection remains. Redis outages therefore have different behavior for cached reads and protected requests.

HTTP, database statement, command, cache, replay, and rate-limit metrics use bounded labels. Durable gauges report outbox backlog, failed deliveries, and reconciliation. The reconciliation gauge performs a ledger scan, which must be included in capacity planning. Web Gunicorn counters can aggregate through Prometheus multiprocess files; standalone worker counters are not automatically included in that web scrape.

When OTLP export is configured, Django and psycopg instrumentation and explicit command spans produce traces. The originating trace carrier is stored in the durable inventory event. Publisher and consumer spans extract that carrier, preserving correlation across worker processes and delayed retries. They are related to the originating context; the consumer span is not necessarily a child of the particular publish attempt. The collector exports traces to Jaeger; Prometheus stores metrics, not traces. The supplied Jaeger storage is in memory and does not provide durable trace retention.

## Verification boundaries and assumptions

PostgreSQL acceptance tests cover competing issues, duplicate keys, conflicting payloads, approvals, over-receipts, duplicate posting, reversals, task closing against issue, opposing transfers, independent transfer progress, duplicate consumers, and draft edits against posting. Consumer tests also cover deferred foreign-key failure advancing through retries. These targeted tests support the implemented invariants; they do not prove every possible interleaving.

Material limits remain:

- Long-running maintenance and imports, full database/broker outages, broker retention expiry, consumer rebalances, and lease expiry overlapping network stalls require operational fault exercises at the intended deployment scale.
- Live projection rebuild concurrent with every writer/consumer combination is not an exhaustive tested protocol. The documented cutover stops those processes, in addition to the application gates.
- Correctness assumes all business writers use the shared lock protocol and all consumers preserve original event identities. New mutable relationships or new command entry points require a fresh lock review.
- Large-project contention, catalog-gate waits, analytics backlog, and reconciliation scrape cost must be measured with representative data and workloads.
- Local containers, configured dashboards, and unit fault injection do not establish high availability, external-effect exactly-once delivery, production throughput, or recovery-time objectives.
