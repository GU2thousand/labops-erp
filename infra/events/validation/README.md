# Isolated RF3 validation

The complete acceptance workflow runs on a Linux Docker host. Prometheus uses
that host's loopback to scrape the independent worker processes. On macOS,
container startup/configuration checks remain useful, while the GitHub-hosted
Linux run supplies the complete cluster and metrics acceptance evidence.

This creates a fresh cluster and database, with separate TLS/SCRAM identities.
It is a same-host protocol and process-failure test, not a production HA or AZ
deployment. `developer_mode=false`, `unsafe_bypass_fsync=false` and cluster-wide
`write_caching_default=disabled` preserve the fsync acknowledgement boundary.
CPU affinity and host checks are explicitly relaxed for CI; production must pass
the official host readiness checks and use separate failure domains.

```sh
python infra/events/validation/prepare.py --run-id "ci123"
set -a
. infra/events/validation/generated/client.env
set +a
docker compose --env-file infra/events/validation/generated/client.env -f infra/events/validation/compose.yaml up -d
python infra/events/validation/wait_ready.py --env-file infra/events/validation/generated/client.env
python infra/events/admin.py users --identities infra/events/validation/generated/secrets.json --report infra/events/validation/evidence/users.json
python infra/events/admin.py topics --report infra/events/validation/evidence/topics.json
python infra/events/admin.py acls --identities infra/events/validation/generated/secrets.json --report infra/events/validation/evidence/acls.json
docker compose --env-file infra/events/validation/generated/client.env -f infra/events/validation/compose.yaml --profile metrics up -d kafka-exporter prometheus
python benchmarks/events/acceptance.py --run-id ci123 --tier smoke --events 60 --rate 10 --duration 0 --fault-repetitions 1 --evidence-dir infra/events/validation/evidence/ci123
```

Generated credentials and private keys are excluded from Git and artifacts.
The generated `KAFKA_ADMIN_TRUSTED_URLS` explicitly trusts the three HTTPS Admin
origins, including the initial `KAFKA_ADMIN_URL`. User provisioning validates each
307's trusted origin, unchanged path and single positive `redirect` counter before
trying another configured endpoint. This handles the [Redpanda 26.2.2 redirect
heuristic](https://github.com/redpanda-data/redpanda/blob/v26.2.2/src/v/redpanda/admin/server.cc#L937-L1113),
which preserves the incoming port even when Docker maps each broker to a different
host port. Automatic redirects and ambient proxy/CA/netrc settings stay disabled;
TLS, authentication and other
failures stop routing. Each operation attempts each configured origin at most
once within a cumulative 15-second monotonic budget and reuses the last successful
endpoint. Requests timeouts cannot forcibly preempt DNS or a trickling peer;
the budget bounds new attempts and rejects late responses. Failed run evidence
must remain retained independently of any later successful provisioning run.

The generated env file freezes a validation retry schedule of `15,30` followed
by twenty-two `60`-second delays and jitter in `[1,1.2]`. Natural broker recovery
is measured under this recorded policy; default production retry timing is
unmeasured. No broker fault resets retained outbox status, due times or leases.

Each broker keeps its private address `.10/.11/.12` on the dedicated default
`10.243.77.0/24` bridge through abrupt stop/start. `prepare.py --ipv4-prefix`
accepts another unused RFC1918 three-octet prefix and freezes it in the env file.
Docker IPAM refuses an overlapping existing pool; do not remove another project's
network to work around that refusal. Choose an unused prefix instead. Parallel
projects also need distinct host ports or separate Docker daemons. Each broker
fault retains before/after network identities and fails on an address/node change.
Recovery additionally requires healthy views from all three brokers and actual
RF3 leaders/full ISR for inventory and DLQ. Cluster readiness has its existing
180-second deadline and counts within the total 900-second recovery/drain window.
TLS certificates last two days and are for disposable runs only. Preserve logs
and the raw evidence before `docker compose ... down -v`; cleanup applies only to
the generated Compose project. Never reuse retained v25.1 data volumes with this
image; use the sequential upgrade and restore procedure in the production
runbook. Existing topic configuration mismatches fail validation and require an
explicit, audited migration.

The automatic CI input uses the reduced `smoke` tier. The manual workflow's
`full` tier freezes `90000` events, `50` events/sec, `1800` seconds and the full
fault inputs documented in `benchmarks/events/README.md`. Measured generation
must fit the fixed 5% window tolerance, and all expected effects must reconcile;
missing those gates fails acceptance. Shared CI runner throughput is not a
production capacity estimate. This workflow runs for pull requests and pushes
to `main`; it has no scheduled recurring execution.

The workflow retains a distinct case and denominator for each executed fault.
Snapshot restore uses a frozen business watermark and preserves dedupe/effect
state. PITR to an earlier business snapshot, insufficient broker retention,
same-name topic recreation and independent-host/AZ failure require their own
case evidence. The Linux metrics profile additionally supports an actual
broker-scrape alert incident with firing/recovery evidence. These remain
unverified until a successful run is attached. `promtool` firing and recovery
tests prove rule evaluation against supplied series; the real worker/exporter
metrics case proves endpoint and metric coverage. Neither alone substitutes
for those live incident and recovery exercises.

Publisher crash drills accelerate a disposable lease timestamp and record that
fixture separately; they do not measure waiting for the configured production
lease TTL. Retry recovery fixtures similarly advance their due time only after
checking the original persisted state. Broker outage drain keeps the actual
stored retry schedules and lease deadlines, with no blanket status reset.
