# Inventory event cutover and rollback

This is a stopped-write inventory cutover. Application schema changes, broker
feature upgrades and RF1 migrations are separate changes with separate rollback
boundaries. First complete an isolated [restore rehearsal](restore.md) and retain
its actual RPO/RTO, reconciliation and unavailable-history range.

Use the exact configuration/profile and service supervisor of the target
deployment. The examples refer to the repository's development Compose stack;
the disposable RF3 stack contains brokers/PostgreSQL and uses host worker
processes. Neither example implicitly provisions production.

## Preflight and drain with the old route still working

1. Record deployed application commit/image, broker versions/digests, migration
   state, Kafka source cluster/generation, topic/group names, ACL policy, payload
   schema/hash and configuration fingerprints. Freeze the capacity and acceptance
   thresholds before execution. Confirm compatible clients, trusted certificates,
   credentials, disk headroom and actual RF3/offset replicas at the destination.
2. Freeze inventory writers and any import tasks that can post stock. Wait for
   in-flight business transactions. Record UTC freeze time, posted movement/event
   counts and ordered original-ID/checksum watermark. Keep the operations worker
   available for unrelated imports/alerts under the cutover policy; do not assume
   that stopping all workers is harmless.
3. While old settings and services still work, drain **both** historical routes.
   Local inventory rows are handled by `process_events`; Kafka rows by publisher,
   notification/analytics consumers and retry/DLQ workers. The current transport
   setting cannot re-route existing rows. Observe Kafka PENDING/PROCESSING/DEAD,
   local unprocessed inventory, per-consumer missing effects, RETRY/DEAD and lag.
4. Drain normal rows. List every remaining RETRY/DEAD separately with original
   hash/source/age and assigned recovery owner. Lag zero is insufficient. If old
   services cannot finish a row, include that row in the bounded recovery plan;
   do not silently mark it processed or switch its transport.
5. Pause publisher, both consumers, retry and DLQ after the drain. Quiesce
   inventory-producing work in the operations worker for the watermark snapshot,
   then keep unrelated operations available according to the maintenance policy.
   Wait for active leases and in-flight effects to finish. Capture final offsets,
   log-start/end offsets, retention boundary and cluster/topic/ACL configuration.

Useful inspections and worker stop points (verify exact service names against
`docker compose --profile events config --services`):

```sh
docker compose exec web python manage.py outbox_events inspect
docker compose exec web python manage.py event_failures inspect
docker compose --profile events stop publisher notification-consumer analytics-consumer retry-worker dlq-publisher
```

For a host-supervised deployment, stop the corresponding `retry_events` and
`publish_dlq` processes too.
Do not run a projection rebuild while writers or the analytics consumer are
operating normally.

## Snapshot, migrate, verify

Take the [restricted PostgreSQL backup and watermark exports](restore.md) while
writers/effects are stopped. Include retained outbox, processed, failed, audits,
users/master data and notifications. Export broker configuration/offsets before
performing the separate [broker/RF migration](../../infra/events/production/rf1-migration.md).
Preserve old data and evidence at every broker feature boundary.

Apply compatible additive database migrations and the reviewed application:

```sh
docker compose run --rm migrate
docker compose run --rm --no-deps web python manage.py rebuild_inventory_projection \
  --actor 'oncall@example.invalid' --reason 'Frozen ledger cutover checkpoint' \
  --authorization 'CUTOVER-123'
docker compose run --rm --no-deps web python manage.py reconcile_stock
```

Verify ledger/balance zero mismatch, rebuilt projection versus the legal ledger,
retained original hashes and per-consumer markers. Review migration legacy-hash
counts/conflicts and replay dry-run. Verify topics, full replica assignments,
internal offsets and ACLs using the [security commands](security.md). Never run
RF1 development override for production acceptance.

Set `LABOPS_EVENT_TRANSPORT=kafka` explicitly for future inventory rows. Install
role-specific secrets, broker source identity/generation and group prefix. A
cluster switch/recreated stream requires a new source identity or generation;
same names and offset numbers are not the same source. Migrate source/watermark
mapping through a reviewed procedure, not a bulk status/transport rewrite.

## Canary and reopen

Start consumers, retry/DLQ and publisher with bounded supervision, and continue
the operations worker. Reopen only the canary writer, post a lawful inventory
command with its idempotency key, and verify original outbox, broker ack, both
dedupe/effects, no unexpected parking and reconciliation. Observe worker/exporter
freshness, broker replication/quorum, error rates and business-effect latency.

Expand writer access only after canary evidence and restore/cutover owner review.
Record actual freeze/resume UTC times, duration, counts, offsets, unresolved rows,
configuration/image identities and post-cutover observation window. A working
canary is not the full 90,000-event matrix or production HA certification.

## Rollback

Pause new publisher/consumers/retry/DLQ, freeze inventory writes, preserve new and
old broker/database state, and reconcile at the last accepted business watermark.
Restore a compatible application/configuration and continue each retained route.
Switching `LABOPS_EVENT_TRANSPORT=local` affects only newly created rows; existing
Kafka rows and parked failures still need their corresponding workers.

After v2/new-topic emission, an old consumer may be incompatible. Retain that
stream, resume only compatible flows, and execute the recorded recovery mapping.
Broker rollback after feature finalization may require tested replacement/restore.
Offset reset cannot undo committed effects. Never delete volumes, clear dedupe,
rewrite immutable messages, reuse old stream identity after recreation, or invent
new IDs to force replay.
