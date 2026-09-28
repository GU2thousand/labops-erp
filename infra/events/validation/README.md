# Isolated RF3 validation

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
docker compose --env-file infra/events/validation/generated/client.env -f infra/events/validation/compose.yaml --profile metrics up -d kafka-exporter
python benchmarks/events/acceptance.py --run-id ci123 --events 60 --rate 10 --duration 0 --fault-repetitions 1 --evidence-dir infra/events/validation/evidence/ci123
```

Generated credentials and private keys are excluded from Git and artifacts.
TLS certificates last two days and are for disposable runs only. Preserve logs
and the raw evidence before `docker compose ... down -v`; cleanup applies only to
the generated Compose project. Never reuse retained v25.1 data volumes with this
image; use the sequential upgrade and restore procedure in the production
runbook. Existing topic configuration mismatches fail validation and require an
explicit, audited migration.

The automatic CI input is deliberately reduced. Manual `90000` events, `50`
events/sec, `1800` seconds and `20` fault repetitions request the plan's workload;
the resulting measurements and fault denominators determine completion. Shared
CI runner throughput is not a production capacity estimate.

The workflow retains a distinct case and denominator for each executed fault.
Snapshot restore uses a frozen business watermark and preserves dedupe/effect
state. PITR to an earlier business snapshot, insufficient broker retention,
same-name topic recreation, independent-host/AZ failure and actual live
Prometheus alert firing/recovery are separate mandatory staging validations.
They remain unverified until their own evidence is attached. `promtool` firing
and recovery tests prove rule evaluation against supplied series; the real
worker/exporter metrics case proves endpoint and metric coverage. Neither
substitutes for those live incident and recovery exercises.
