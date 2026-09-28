# Single broker development

The root `compose.yaml` `events` profile is a lightweight local RF1 environment.
It pins a supported Redpanda patch and immutable multiarchitecture digest; it
does not establish replication, security, or production high availability.
Inventory transport still defaults to `local`: set `LABOPS_EVENT_TRANSPORT=kafka`
when testing Kafka. Existing outbox rows retain their recorded transport.

The `topics` service declares and verifies partition count, retention and delete
policy using an explicit `--development-rf1` option. Configuration drift fails
startup; it is never silently skipped. `publisher`, `retry-worker`,
`dlq-publisher`, both independent consumers and the operations `worker` serve
distinct responsibilities. Enable `observability` to scrape each events worker
on its private port 9100. Credentials, TLS and RF3 are exercised by the isolated
validation profile, not inferred from this development profile.

Never attach a retained older-version broker volume to the upgraded image.
Backup and use the official one-feature-version-at-a-time upgrade procedure in
`infra/events/production/README.md`.

## Starting a new empty development cluster

The v26.2 broker uses a new `redpanda-26-2-data` named volume. The former
`redpanda-data` volume remains preserved and is never attached to v26.2. Startup
refuses to run until `LABOPS_EVENTS_FRESH_CLUSTER_ACK` is exactly
`accept-new-empty-development-cluster`. This acknowledgement permits an empty
development cluster; it does not migrate old Kafka records or offsets.

Before acknowledging a previously used development environment:

1. Freeze inventory writes. Drain the old local/Kafka backlog using the old
   compatible broker and workers; separately inventory RETRY/DEAD rows.
2. Stop old event workers, back up the business database, and export old topic
   retention bounds, topic configuration and group offsets. Preserve the old
   broker volume and image. Do not use `down --volumes` on that project.
3. Choose a new `KAFKA_SOURCE_CLUSTER_ID` (for example `labops-dev-26-2`) and
   `KAFKA_GROUP_PREFIX`, and set stream generation `1`. This makes new source
   coordinates distinct from the old cluster's offsets. Existing failure rows
   retain their source identity and original event IDs.
4. Decide explicitly how the retained original outbox/events will reach the
   new cluster. A `PUBLISHED` row does not become `PENDING` on restart; use the
   audited replay/migration runbook and original IDs after dry-run review.
   Pending rows can drain using their recorded transport after the boundary.
5. Acknowledge the empty development cluster, start topics/workers, reconcile
   ledger and projection, then resume a canary inventory command. Keep the
   operations worker running for imports and alerts outside the write freeze.

For a brand-new disposable development database with no existing broker history,
record that empty starting state, choose its source identity and acknowledge it.
Use the supported sequential broker upgrade or replacement-cluster migration
runbook when retaining broker history; the fresh development path is not an
in-place v25.1 → v26.2 upgrade.
