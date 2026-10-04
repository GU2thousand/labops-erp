# Production deployment reference

This directory is a deployment/configuration reference. The three-broker
`validation` environment demonstrates protocol and process failure behavior on
one host. It does not establish independent-host/AZ availability or claim a live
production deployment. Application upgrades and broker data upgrades have separate
rollback boundaries.

## Deployment and durability

Use a managed Redpanda deployment or the official [Kubernetes deployment
workflow](https://docs.redpanda.com/streaming/current/deploy/redpanda/kubernetes/)
or [production readiness
workflow](https://docs.redpanda.com/streaming/current/deploy/redpanda/manual/production/production-readiness/).
Pin Redpanda v26.2.2 to the manifest in `image-lock.json`; pin the official Operator
and Helm chart version during platform provisioning and store their rendered
configuration and digests in deployment evidence. These platform versions are
not selected or deployed by this reference.

Place three brokers on three independent hosts/failure domains. Use durable
per-broker volumes with tested fsync behavior and sufficient capacity for RF3
retention plus catch-up/recovery. Enable production mode (`developer_mode=false`,
no overprovisioned mode), run the supported node autotuner, and verify system
checks. Three containers on one host remain a validation setup. Set each broker's
rack from its actual failure domain; for Kubernetes use the official
[rack awareness
configuration](https://docs.redpanda.com/streaming/current/manage/kubernetes/k-rack-awareness/)
and node anti-affinity. Export partition replica assignments and verify each
partition spans the intended racks; a rack label alone proves no placement.

`cluster-properties.yaml` declares a starting policy. The
[cluster properties](https://docs.redpanda.com/streaming/current/reference/properties/cluster-properties/)
include `internal_topic_replication_factor=3`, explicit authorization, disabled
auto-creation, and `write_caching_default=disabled`. Inventory and DLQ additionally
declare `write.caching=false` in `../topics.json`; Redpanda's Raft majority
acknowledgment with caching disabled is the intended persistence boundary.
Do not translate Apache Kafka's ISR/min.insync.replicas policy into a Redpanda
claim. Record actual successful producer acknowledgments and recovery evidence.

Each advertised broker/RPC identity must keep a stable address through supported
restarts. Use the platform's supported stable node endpoints and verify their
identity after each restart. In a custom container deployment, assign stable
per-broker private addresses: a hostname alone does not protect against a peer
caching an old address that Docker has reassigned to another broker. The RF3 CI
profile uses a dedicated network and fixed broker addresses for this reason.
Before returning to service, check all brokers' cluster health, no down nodes,
no leaderless/under-replicated partitions, and actual topic leaders/replicas;
counting reachable endpoints or metadata entries is insufficient. The developer
Compose profile has one broker and does not validate this multi-node boundary.

Configure and validate `internal_topic_replication_factor=3` before any consumer
group first commits. The administration tool only inspects `__consumer_offsets`
after a consumer has created it; it never creates or modifies internal topics.
Existing RF1 data requires the explicit [migration runbook](rf1-migration.md).

## Provisioning and least privilege

Kafka and Admin API listeners use TLS with server certificate SANs matching every
advertised hostname/IP. Kafka clients require SASL_SSL/SCRAM and certificate plus
hostname validation. Restrict Kafka, Admin API, inter-broker RPC and metrics ports
to private network policies/security groups; enable TLS for replication links as
well. Production certificates and private keys must come from the platform's
secret manager, not the ephemeral validation CA.

Set `authentication_method: sasl` on every Kafka listener in each broker's
configuration, with matching TLS entries. The cluster reference uses explicit
`kafka_enable_authorization=true`; leave legacy global `enable_sasl` unset. The
[authentication guide](https://docs.redpanda.com/streaming/current/manage/security/authentication/)
requires choosing one configuration method. Cluster-wide write caching is
`disabled`, so later topic overrides cannot silently weaken fsync durability.

Bootstrap a fresh cluster's `admin` account using the documented
[`RP_BOOTSTRAP_USER`](https://docs.redpanda.com/streaming/current/reference/environment-variables/)
secret at initial startup; `superusers` names alone do not create credentials.
Remove that bootstrap environment value from regular deployment configuration
after provisioning. Separate the Admin API/TLS/SCRAM identity from every worker.
Use private `secrets.json` (mode 0600) mapping the names `admin`, `publisher`,
`notification`, `analytics`, `dlq`, `replay`, and `exporter` to independent random
passwords, and a private env file with the following values:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=broker-a.private:9093,broker-b.private:9093,broker-c.private:9093
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SSL_CA_LOCATION=/run/secrets/kafka-ca.crt
KAFKA_ADMIN_USERNAME=admin
KAFKA_ADMIN_PASSWORD=<secret-manager-reference-value>
KAFKA_ADMIN_URL=https://broker-a.private:9644
KAFKA_TOPIC=labops.inventory.v1
KAFKA_DLQ_TOPIC=labops.inventory.dlq.v1
KAFKA_GROUP_PREFIX=labops
```

Run with the pinned Python requirements from repository root, using env files
mounted at runtime rather than passwords in shell command arguments:

```sh
python infra/events/admin.py users --env-file /run/secrets/kafka-admin.env --identities /run/secrets/secrets.json --report evidence/users.json
python infra/events/admin.py topics --env-file /run/secrets/kafka-admin.env --report evidence/topics.json
python infra/events/admin.py acls --env-file /run/secrets/kafka-admin.env --identities /run/secrets/secrets.json --report evidence/acls.json
# After an authorized consumer has committed an offset:
python infra/events/admin.py topics --env-file /run/secrets/kafka-admin.env --verify-only --verify-offsets --require-full-isr --report evidence/topics-after-consumption.json
```

`users` preserves existing passwords unless `--rotate-existing` is explicit.
User existence does not verify supplied credentials; acceptance must connect as
each application identity. The topic command creates missing topics and then
compares every partition's actual replicas, partition count, and declared config.
Any existing mismatch fails before modifying topics. It refuses to replace data
or silently accept RF1. `--development-rf1` is an explicitly labeled override for
a single-broker development profile, never a production acceptance option.

The [Kafka ACL
API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
creates missing ACLs, reads them back, and rejects additional grants to these
application principals or the wildcard principal. No worker is a superuser.
The role policy is:

| Identity | Topic permissions | Group/cluster permissions |
| --- | --- | --- |
| publisher | Write/Describe inventory only | Cluster IdempotentWrite |
| notification | Read/Describe inventory | Read/Describe exactly `<prefix>.notification.v1` |
| analytics | Read/Describe inventory | Read/Describe exactly `<prefix>.analytics.v1` |
| dlq | Write/Describe DLQ only | Cluster IdempotentWrite |
| replay | Read/Describe inventory | Read/Describe `<prefix>.replay.` group prefix |
| exporter | Describe inventory and DLQ | Describe `<prefix>.` group prefix; Cluster Describe for metadata/group discovery |

The retry scheduler only accesses PostgreSQL and needs no broker identity.
Exporter filters must use the same topic/group scope as the ACLs. Cluster Describe
can expose group names through discovery; unrelated groups still cannot be
described or consumed. No application identity may Create, Alter, Delete, change
ACLs, reset another group's offsets, or read the DLQ. If a separate DLQ reader is
needed, provision another audited role. Credential testing must include anonymous,
wrong password/CA, foreign group, foreign topic read/write, and topic creation.

## Capacity, rotation, and operations

Estimate per-broker retained bytes from measured event rates and encoded payload
size, then add DLQ retention, internal topics, segment overhead, recovery headroom
and uneven-partition margin. RF3 distributes three copies across the cluster.
Initial inventory retention is 30 days and DLQ retention 90 days with no byte-based
early truncation (`retention.bytes=-1`). The 1 MiB/2 MiB topic batch limits are
separate from the smaller application envelope limit. Freeze capacity against
the actual workload before provisioning disks; these values are policy starting
points, not measured sizing.

The reference disk policy alerts at 20% free and rejects producers at 5 GiB
remaining. Review both thresholds against the selected disk sizes and incident
response time. Monitor all brokers, under-replication/leaderless partitions,
replication lag, quorum, disk free bytes, certificate expiration, client errors
and exporter scrape failure using the official [disk
management](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/disk-utilization/)
metrics. A full disk blocks publication; business outbox state must remain intact.

For SCRAM rotation, stage fresh secrets in the secret manager, drain/restart one
affected worker role at a time, explicitly update that role via the authenticated
Admin API, reconnect using the new secret, and prove permitted and denied
operations. The tool's `users --rotate-existing` rotates every listed identity;
use a separately reviewed direct role rotation for rolling production changes.
Coordinate admin rotation last with a second tested administrator session; do
not invalidate the sole access path mid-operation.

For CA rotation, first distribute a trust bundle containing old and new CA roots
to clients, then issue matching broker certificates. Follow the official
[TLS/restart
procedure](https://docs.redpanda.com/streaming/current/manage/security/encryption/),
rolling one broker at a time and verifying cluster health/full ISR after each.
Validate new connections with hostname verification, restart clients with the
final trust chain, then remove old roots after all connections and consumers
are verified. Store certificate fingerprints/expiry and operation outcomes,
never keys or passwords, in evidence.

Export configuration, topic assignments, consumer offsets and PostgreSQL
business/outbox/dedupe/failed/audit watermarks before changes. Broker backup does
not replace PostgreSQL PITR; replay cannot recreate missing business commands
or master data. Test restore on an isolated cluster and preserve the actual
RPO/RTO and unavailable history window.
