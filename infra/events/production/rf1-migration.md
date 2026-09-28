# Existing RF1 topics and retained broker data

This runbook is an explicit administrator operation. `admin.py topics` detects
RF/config/partition mismatch and refuses to mutate existing topics. Never reuse
the single-broker development data volume as a new v26.2 validation cluster.

## Upgrade the retained broker cluster first

Inventory the actual broker binary, active feature version, volumes, topics,
offsets and PostgreSQL business watermarks. Take and test supported backups on
an isolated environment. For a retained 25.1 cluster, obtain vendor guidance for
the unsupported starting release and the supported sequential path
25.1 → 25.2 → 25.3 → 26.1 → 26.2. Select/pin the appropriate patch at each step;
roll one broker at a time, complete health/compatibility checks, and preserve
evidence before moving to the next feature version. Do not skip feature versions
or directly attach old volumes to a new image. The official [upgrade
guide](https://docs.redpanda.com/streaming/current/upgrade/rolling-upgrade/)
defines finalize and downgrade restrictions; deferred finalization requires
checking version support and Enterprise authorization. Rollback after feature
activation may require restoring a backup into a compatible replacement cluster.

## Inventory/DLQ RF1 → RF3 in the same cluster

1. Confirm three healthy brokers on three real fault domains, adequate spare
   disk and matching version. Export topic configs, assignments, group offsets,
   application source-cluster identity and a verified PostgreSQL backup/PITR
   watermark. Record counts of retained broker records and unresolved RETRY/DEAD.
2. Freeze inventory writes and wait for in-flight transactions, drain existing
   published/local backlog, and pause publisher/consumers/retry/DLQ. Keep the
   operations worker for unrelated imports/alerts within the cutover policy.
   Export final watermarks; never clear dedupe rows or rewrite event IDs.
3. Use the version-matched `rpk` administrator profile with TLS/SCRAM configured
   in a private file. Review/dry-run the user-topic change, then apply deliberately:

   ```sh
   rpk topic alter-config "$KAFKA_TOPIC" "$KAFKA_DLQ_TOPIC" --set replication.factor=3 --dry
   rpk topic alter-config "$KAFKA_TOPIC" "$KAFKA_DLQ_TOPIC" --set replication.factor=3
   rpk topic alter-config "$KAFKA_TOPIC" --set cleanup.policy=delete --set retention.ms=2592000000 --set retention.bytes=-1 --set max.message.bytes=1048576 --set write.caching=false
   rpk topic alter-config "$KAFKA_DLQ_TOPIC" --set cleanup.policy=delete --set retention.ms=7776000000 --set retention.bytes=-1 --set max.message.bytes=2097152 --set write.caching=false
   ```

   Redpanda documents the [explicit replication-factor
   change](https://docs.redpanda.com/streaming/current/develop/manage-topics/config-topics/).
   Preserve the source and outcome of these commands; wait for all partition
   movements and full replication. Inspect actual replica IDs and rack placements,
   not only the requested RF. RF cannot exceed broker count.
4. Run `admin.py topics --verify-only --verify-offsets --require-full-isr` with
   the administration env file and preserve its report. Verify ledger, balances,
   projection and consumer dedupe hashes at the frozen business watermark. Resume
   consumers then publisher, run a canary inventory operation and reconcile before
   reopening writes. Record actual downtime, effect latency and any parked work.

## Internal offsets RF1

Set `internal_topic_replication_factor=3` before any consumer commits on a fresh
cluster. Changing that cluster default does not prove an existing internal topic
has migrated. The checker verifies every actual `__consumer_offsets` partition.
Official [topic configuration
guidance](https://docs.redpanda.com/streaming/current/reference/properties/topic-properties/)
warns against altering internal topic properties. Do not run the user-topic
`alter-config` instructions against `__consumer_offsets`.

If offsets is already RF1, obtain the vendor-supported migration procedure for
that exact version, or prepare a replacement RF3 cluster. For replacement,
retain original IDs and verified source identity/watermark mapping; new broker
coordinates are a new source stream. Explicitly export/translate offset and
dedupe boundaries or use audited replay groups. Do not delete/recreate the
internal topic or reset running business groups to earliest. Complete independent
restore/replay verification and a final cutover before retiring old data.

Partition-count mismatch is also a deliberate migration. More partitions change
key-to-partition routing and cannot restore earlier ordering. Prefer a new
versioned topic with an audited shadow consumer and boundary rather than an
automatic increase.

## Rollback

Pause new workers, preserve both broker and database data, and reconcile at the
last accepted watermark. Restore a compatible application and continue draining
the original transport for retained outbox rows. Switching the setting to local
affects only newly created events. Offset reset cannot undo database effects.
Use tested restore/replacement for incompatible broker data; do not remove
volumes, discard failed records, clear processed markers, or create new IDs to
force processing.
