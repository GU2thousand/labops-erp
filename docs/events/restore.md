# PostgreSQL and event restore rehearsal

Run first in an isolated database/cluster with no production consumer group and
no external notification connection. Retain a unique non-overwritten evidence
directory, the original data and all failed attempts. A checked-in restore command
is a procedure; only its executed result establishes recoverability or RPO/RTO.

## Choose a business watermark

Prefer tested PostgreSQL PITR from base backup plus archived WAL to a precise
transaction/business watermark. `pg_dump` is a logical snapshot and cannot be
combined with WAL replay to become PITR. This repository does not automatically
configure a managed PITR service. Document the deployment's actual base/WAL
archive, restore target/timeline, last known committed stock command and retained
relations. [PostgreSQL 17 PITR](https://www.postgresql.org/docs/17/continuous-archiving.html).

If recovering an older logical snapshot, quantify committed commands/events
missing after it and the actual lost business interval. Kafka delta records alone
cannot reconstruct that inventory truth. Isolate post-watermark messages whose
original ledger/outbox/master relations are absent; do not apply them to fabricate
legitimate balances. A broker RF3 cluster does not establish database RPO zero.

## Capture restricted backup and recovery context

Freeze stock writes and stop effects using [cutover](cutover.md). Export all data
together: users, items, batches, warehouses, purchase/project relations, posted
movements/lines, StockBalance, OutboxEvent, ProcessedEvent, FailedDelivery,
DeliveryAudit, business audits and notifications. The whole-database dump retains
these relationships; a broker-only backup does not.

The following local example writes a new restricted directory and refuses to
overwrite a prior run. Replace the run ID with an unused incident-specific value:

```sh
umask 077
RESTORE_RUN_ID=restore-20260927-01
mkdir -p backups
chmod 700 backups
mkdir "backups/$RESTORE_RUN_ID"
docker compose exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' \
  > "backups/$RESTORE_RUN_ID/labops.dump"
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT clock_timestamp(), pg_current_wal_lsn(), current_database(), version()"' \
  > "backups/$RESTORE_RUN_ID/database-watermark.txt"
docker compose exec web python manage.py reconcile_stock \
  > "backups/$RESTORE_RUN_ID/reconcile-before.txt"
docker compose exec web python manage.py outbox_events inspect \
  > "backups/$RESTORE_RUN_ID/dead-outbox.json"
docker compose exec web python manage.py event_failures inspect \
  > "backups/$RESTORE_RUN_ID/failed-deliveries.json"
```

Exit status and dump size must be checked; a created file alone is not a valid backup. Copy
the encrypted backup and manifest outside the source host/volume using the
deployment's approved backup store. `backup_database` is SQLite-only.
[PostgreSQL dump reference](https://www.postgresql.org/docs/17/app-pgdump.html).

Separately capture a canonical export/hash/count of posted ledger, balances,
projections, original outbox ID/hash/status/transport, processed markers,
notifications and all failed/audit rows at the frozen boundary. Capture the exact
application commit/image, migrations, schema/fixtures hash, topic configs and
replica IDs/racks, source cluster/generation, log-start/end offsets and committed
offsets for every business group. A timestamp alone is not a cross-database/broker
atomic watermark; stopped writes/effects and the retained ID mapping establish
the boundary.

Use a private version-matched `rpk` admin profile for broker inspection:

```sh
rpk cluster info
rpk topic describe "$KAFKA_TOPIC" -p
rpk topic describe "$KAFKA_DLQ_TOPIC" -p
rpk group describe "$KAFKA_GROUP_PREFIX.notification.v1"
rpk group describe "$KAFKA_GROUP_PREFIX.analytics.v1"
python infra/events/admin.py topics --env-file /run/secrets/kafka-admin.env \
  --verify-only --verify-offsets --require-full-isr \
  --report "backups/$RESTORE_RUN_ID/topics-and-offset-replicas.json"
```

Export the command results into restricted evidence. Do not dump secret-bearing
environment/configurations or private keys into an Actions artifact. Store only
configuration fingerprints, allowed fields, certificate fingerprints and secret
manager version references in a shared evidence package.

## Restore a logical snapshot into a new database

These local commands create a new isolated database; they must not target the
active database. Confirm `labops_restore_20260927_01` is unused and leave active
services connected to their original database. A managed PITR restore uses its
tested provider/DBA procedure instead and records target timeline/LSN/time.

```sh
docker compose exec -T postgres sh -c 'createdb -U "$POSTGRES_USER" labops_restore_20260927_01'
docker compose exec -T postgres sh -c 'pg_restore -U "$POSTGRES_USER" --dbname=labops_restore_20260927_01 --exit-on-error --no-owner' \
  < "backups/$RESTORE_RUN_ID/labops.dump"
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore_20260927_01 \
  web python manage.py migrate --noinput
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore_20260927_01 \
  web python manage.py reconcile_stock
```

The deployment uses discrete PostgreSQL environment settings, so set the restore
database consistently for every isolated command. If your deployment uses a
`DATABASE_URL` without discrete settings, point that URL at the new database and
verify the selected database before a write. Do not start its workers against
live topic/groups by inheriting an unreviewed environment.

Compare full row counts/hashes to the backup watermark; verify FK integrity and
all original outbox/hash/marker/audit relations. Missing data after an older
snapshot remains actual RPO loss even if a broker record still exists.

## Rebuild, checkpoint, and bounded replay

Rebuild from the restored legal ledger while restored writers/analytics are
stopped. The command checkpoints original retained inventory IDs/checksums for
analytics and appends a rebuild audit:

```sh
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore_20260927_01 \
  web python manage.py rebuild_inventory_projection \
  --actor 'oncall@example.invalid' --reason 'Isolated restored ledger checkpoint' \
  --authorization 'RESTORE-123'
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore_20260927_01 \
  web python manage.py reconcile_stock
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore_20260927_01 \
  web python manage.py replay_events --consumer analytics \
  --start 2026-09-27T00:00:00Z --end 2026-09-28T00:00:00Z \
  --limit 100 --rate 10 --actor 'oncall@example.invalid' \
  --reason 'Check rebuilt retained IDs dedupe' --authorization 'RESTORE-123' --dry-run
```

Review selection against the frozen business watermark and retained relations,
then use the same bounded selection with `--execute` if required. Replayed
checkpointed analytics events must dedupe and never double-count. Compare the
rebuilt projection to the legal posted ledger, separately from StockBalance
reconciliation. Notification recovery preserves its restored dedupe/effect rows;
review originals/users and test bounded replay independently.

Do not globally reset a live production group's offsets. A replacement cluster
has new source coordinates; retain original IDs and create a new source
identity/generation with an explicit reviewed mapping. If broker history has
expired, ledger rebuild still restores analytics; absent notification outbox or
external delivery receipts remains an unavailable-history limitation. Retain
quarantined missing-relation messages and the lost range for investigation.

## Acceptance and promotion

Record restore start/end, restored watermark, intended target, actual RPO loss,
RTO, all failed attempts, lag/parked/DEAD counts, retention bounds and unknown or
unrecoverable IDs/ranges. Require zero legal ledger/balance and ledger/projection
mismatch, no duplicate notification/analytics effects and preserved audit history.
Only then use [cutover](cutover.md) to select the restored database consistently,
start scoped workers, perform a canary and reopen writes. Preserve the old data
until observation and incident review complete; never delete broker volumes or
processed markers to make a result appear clean.
