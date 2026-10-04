# Event security and rotation runbook

The repository's production directory is a deployment reference. A live cluster,
production secrets, network policy and operator authorization must be supplied
and verified before describing this as a production security deployment.

## Provision and verify

Use TLS/SASL SCRAM with hostname/certificate verification, independent identities
and private Kafka/Admin/RPC/metrics listeners. Place trusted CA and role passwords
in a secret manager or read-only secret files. `kafka_config.py` uses an allowlist
and validates publish/lease/poll budgets; arbitrary librdkafka options are not
passed through. Never print passwords, client config or rendered container Env.

| Identity | Allowed scope | Deliberately excluded |
|---|---|---|
| publisher | Inventory Write/Describe, cluster IdempotentWrite | Read, arbitrary topic creation/admin, DLQ write |
| notification | Inventory Read/Describe, own exact notification group | Analytics/foreign groups, write/admin |
| analytics | Inventory Read/Describe, own exact analytics group | Notification/foreign groups, write/admin |
| dlq | DLQ Write/Describe, cluster IdempotentWrite | Inventory write, DLQ read, admin |
| replay | Inventory Read/Describe and reviewed replay-group prefix | Resetting live business groups/admin |
| exporter | Declared-topic Describe, scoped group Describe and cluster metadata discovery | Read/write/create/admin |
| retry | PostgreSQL failure/dedupe/effect access | Broker credential unnecessary |
| admin | Provisioning/config/ACL operations | Application runtime use |

Database-local `replay_events` requires reviewed database/operator access; the
broker replay identity above is reserved for isolated broker-read workflows.
DLQ inspection needs a separately provisioned restricted reader; application
worker ACLs do not grant it. Recipient IDs, trace context, bodies, raw poison
bytes and backups remain restricted data. Metrics labels exclude IDs/barcodes,
raw payloads and free-form exceptions.

The administration tool accepts a protected plain KEY=value file without shell
evaluation. Use the role-name secret map documented in
[production provisioning](../../infra/events/production/README.md):

```sh
python infra/events/admin.py users --env-file /run/secrets/kafka-admin.env \
  --identities /run/secrets/secrets.json --report evidence/users.json
python infra/events/admin.py topics --env-file /run/secrets/kafka-admin.env \
  --report evidence/topics.json
python infra/events/admin.py acls --env-file /run/secrets/kafka-admin.env \
  --identities /run/secrets/secrets.json --report evidence/acls.json
python infra/events/admin.py topics --env-file /run/secrets/kafka-admin.env \
  --verify-only --verify-offsets --require-full-isr \
  --report evidence/topics-after-consumption.json
```

The internal offsets topic exists only after an authorized consumer commits.
Set its RF3 cluster default before first use, then verify actual assignments.
Existing topic/config mismatches fail instead of being silently changed. Extra
application or wildcard-principal ACL grants also fail reconciliation. User
existence alone does not verify a password; connect as each identity and run the
permitted and denied operation probes. Include anonymous, wrong password, wrong
CA/hostname, foreign group/topic, unauthorized write and topic-create cases.
Retain broker/client results and outbox persistence evidence, excluding secrets.

## SCRAM credential rotation

For a disposable acceptance run, a fully coordinated rotation can explicitly
use `users --rotate-existing`. That tool rotates every identity in the required
secret map; it is unsuitable for silently rotating all production roles at once.

For production, stage the new secret, drain/stop the affected role within its
shutdown budget, use the authenticated Admin API to rotate that role, restart
with the secret-manager version and test allowed/denied operations. Preserve the
old secret only in the incident's authorized secret manager for rollback. Do not
change envelope IDs/hashes or queue routes during credential rotation. Rotate
admin last with a second tested administrator session so the sole access path is
not invalidated. Record role, secret version reference, actor/change reference,
times and outcomes; never retain secret values in artifacts.

## CA and certificate rotation

Distribute an old+new CA trust bundle to clients first. Issue new broker
certificates whose SANs match all advertised endpoints, roll one broker at a time,
and verify full replication, quorum and new TLS connections after every step.
Restart clients with the final certificate chain; remove old roots only when all
clients and brokers are verified. Record fingerprints/expiry, not private keys.
The disposable validation CA lasts two days and cannot be reused as production PKI.

Retain private-network controls during rotation; do not bypass certificate or
hostname verification to restore availability. Broker acknowledgements that fail
must leave the durable outbox available for retry. Relevant official guidance:
[authentication](https://docs.redpanda.com/streaming/current/manage/security/authentication/)
and [TLS](https://docs.redpanda.com/streaming/current/manage/security/encryption/).

## RF1 migration and disk policy

Use the explicit [retained-data migration procedure](../../infra/events/production/rf1-migration.md).
The topic verifier does not migrate RF1 topics; modifying only create-time defaults
leaves their existing replicas unchanged. Inventory/DLQ may use the version-matched
administrator `rpk topic alter-config ... --set replication.factor=3` after reviewed
dry-run and backup. Internal offsets requires vendor-supported migration or an
isolated replacement-cluster plan; never delete/recreate it as a shortcut.

Verify actual replica IDs and rack/AZ placement. Three same-host replicas prove
neither host nor AZ tolerance. Keep delete cleanup for deltas; compaction by
movement key can discard required history. Freeze retention, max-message and disk
headroom thresholds from measured size/rate; monitor full disk, replication health,
CA expiry, missing worker/exporter metrics and authorization errors.
