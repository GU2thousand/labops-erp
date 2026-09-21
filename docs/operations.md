# LabOps operations

This Compose stack is a local development and repeatable validation environment. PostgreSQL is the system of record. The default stack runs PostgreSQL, a one-shot migration service, a two-worker Gunicorn web service, and the operations worker. Named volumes preserve data across container replacement. Host ports bind to loopback only.

## Start the baseline

```sh
cp .env.example .env
# Set LABOPS_SECRET_KEY, METRICS_TOKEN and GRAFANA_ADMIN_PASSWORD in .env.
# A suitable secret can be generated with: openssl rand -hex 32
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 migrate web worker
```

Open http://127.0.0.1:8001. PostgreSQL listens on host port 55433. Set `POSTGRES_PORT` and `WEB_PORT` in `.env` to change these ports. The container database port remains 5432. Credentials containing URL-special characters are safe in `POSTGRES_PASSWORD` because the application receives discrete PostgreSQL settings.

Migrations execute once in the `migrate` service before application processes start; workers never race to migrate or seed. No demo accounts or records are created automatically. For a new, disposable database only:

```sh
docker compose exec web python manage.py seed_demo
```

The seed creates fictional records and known development credentials (`admin@labops.local` / `LabOpsDemo!2026`). It skips databases containing users. Do not use seeded accounts for real data. For a real installation create the initial administrator through your account provisioning procedure instead.

`LABOPS_DEBUG=1` enables local HTTP session cookies; switching it to `0` enables secure cookies and requires HTTPS termination and an explicit host allowlist. Replace all example secrets before sharing an environment. This stack does not supply TLS, high availability, or remote access.

## Existing database upgrade and projection cutover

Take and test a backup first. Stop application writers and event consumers during the cutover. Run migrations and rebuild the analytics projection from the authoritative posted ledger before restarting normal work:

```sh
docker compose stop web worker publisher notification-consumer analytics-consumer
docker compose run --rm migrate
docker compose run --rm --no-deps web python manage.py rebuild_inventory_projection
docker compose run --rm --no-deps web python manage.py reconcile_stock
docker compose up -d
```

If event profiles are enabled, include the same profile flags used for normal startup in the final `up` command. The rebuild atomically records existing inventory events as processed by analytics, preventing historical events from double-counting the rebuilt projection. It briefly locks writes and analytics updates. It is an explicit cutover operation, never an automatic container startup step. It is harmless on an empty database. Keep the pre-upgrade backup until acceptance and reconciliation are complete.

## Kafka / Redpanda events

In `.env`, set `LABOPS_EVENT_TRANSPORT=kafka`, then enable the full profile:

```sh
docker compose --profile events up -d --build
docker compose --profile events logs --tail=100 topics publisher notification-consumer analytics-consumer
docker compose exec redpanda rpk group describe labops.notification.v1 --brokers redpanda:9092
docker compose exec redpanda rpk group describe labops.analytics.v1 --brokers redpanda:9092
```

The profile creates inventory and DLQ topics with three partitions and starts independent notification and analytics consumer groups. The normal operations worker stays active for imports, alerts, and locally routed events. Each event retains the transport selected at creation; changing the setting does not move existing rows to another transport. Keep the publisher and consumers running until existing Kafka work drains before disabling the profile. Leave the transport `local` when the profile is not running, or events will accumulate in the outbox.

Internal clients use `redpanda:9092`; host clients use `127.0.0.1:19093`. The single broker uses replication factor one and is appropriate for local tests, not replicated production durability. Publisher retries and consumer deduplication provide at-least-once delivery. A broker acknowledgement followed by process failure can produce duplicate events; the database uniqueness key `(consumer_name, event_id)` prevents duplicate database effects for each consumer.

## Inspect and replay failed deliveries

```sh
docker compose exec web python manage.py outbox_events inspect
docker compose exec web python manage.py outbox_events retry --id EVENT_UUID --reason 'Broker restored after outage'
docker compose exec web python manage.py event_failures inspect
docker compose exec web python manage.py event_failures retry --id DELIVERY_UUID --reason 'Underlying data or consumer issue corrected'
docker compose exec web python manage.py event_failures resolve --id DELIVERY_UUID --reason 'Documented operator disposition'
docker compose exec redpanda rpk topic consume labops.inventory.dlq.v1 --brokers redpanda:9092 --num 1
```

Publisher events that exhaust retries enter DEAD and remain durable. Use `outbox_events retry` after correcting the fault; it records an audit reason. An administrator can also retry through `POST /api/v1/events/{id}/retry`. These are publisher outbox rows, separate from consumer failed deliveries.

For consumer failures, `retry` preserves the original event identity and schedules the durable failed delivery for the publisher retry loop. `resolve` records an operator decision and does not apply the event. Keep the reason meaningful. Do not create new event IDs to force replay: that bypasses deduplication. A DEAD delivery is copied to the DLQ topic for inspection; the database failed-delivery record is the operator recovery record.

## Optional Redis

Set `REDIS_URL=redis://redis:6379/0` and start with `--profile redis`. Combine this flag with other enabled profiles. Host Redis port defaults to 16380.

Catalog cache reads fail open to PostgreSQL if Redis is unavailable. Cache invalidation occurs after commit, and the 60-second TTL bounds stale catalog data if invalidation fails. Stock validation always reads PostgreSQL. With Redis configured, rate limiting for login, imports and reports fails closed: Redis failure returns HTTP 503, while an exhausted bucket returns 429. With `REDIS_URL` empty, Redis rate limiting is disabled; existing database login protection remains. Restore Redis before retrying operations, or deliberately unset the URL and recreate application services to disable that optional protection. `noeviction` prevents memory pressure from silently evicting active limiter keys.

## Metrics and traces

Set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` to enable HTTP OTLP export, then start with `--profile observability`. An empty endpoint disables tracing. All profiles can be combined:

```sh
docker compose --profile events --profile redis --profile observability up -d --build
```

- Grafana: http://127.0.0.1:13000, user `admin`, password from `.env`.
- Prometheus: http://127.0.0.1:19090.
- Jaeger: http://127.0.0.1:16687.

Grafana automatically provisions Prometheus and Jaeger data sources and the **LabOps Operations** dashboard. It displays request and database latency, request errors, idempotency replays, outbox delay, consumer failures, reconciliation, cache results, rate-limit rejections, and Kafka consumer lag. Traces flow from each application process through the OTel collector into Jaeger; Prometheus does not store traces. Local Jaeger trace storage is in memory and resets on container replacement. Prometheus retains metrics for 15 days on its volume. Rules evaluate availability, ledger mismatch, outbox delay, failed delivery and sustained Kafka lag above 100 messages; configure an Alertmanager separately if external notifications are desired.

Prometheus sends the shared `METRICS_TOKEN` as a bearer token to `/metrics`. Avoid putting that token into browser query strings. Gunicorn workers share Prometheus counters through per-container multiprocess files, cleared once when the master starts. Scrapes combine those counters with database-derived gauges. The `/metrics` reconciliation gauge scans the ledger, so account for its cost when profiling large datasets. Process-local counters from standalone workers are not aggregated into the web endpoint; durable backlog/failure gauges do cover their database state. Redpanda `/public_metrics` and the Kafka exporter are scraped when the events profile is running; their targets are intentionally down otherwise. The events profile includes pinned `danielqsj/kafka-exporter:v1.9.0`, a 128 MiB-limited internal service on port 9308. It reads committed offsets and partition high watermarks through the broker protocol for `labops.*` consumer groups, including disconnected groups. No exporter port is exposed on the host. See the [exporter documentation](https://github.com/danielqsj/kafka_exporter/tree/v1.9.0) for metric semantics.

The dashboard sums `kafka_consumergroup_lag` by consumer group and topic and shows group members, scrape health, broker discovery and lag sample age. Sample age measures scrape freshness, not event processing latency. New groups appear only after committing offsets; missing samples do not mean zero lag. Use the exporter health panel to distinguish a scrape failure from a drained queue, and `rpk group describe` to cross-check individual partitions. The high-lag alert intentionally does not fire when the optional event profile is disabled. Kafka lag measures unconsumed broker records; events parked in durable retries or DLQ are tracked separately by `consumer_failures`, so zero Kafka lag alone does not establish successful business processing.

## Back up and restore PostgreSQL

Backups contain application data and must be stored with restricted access outside the container volume. The worker's SQLite backup command does not back up PostgreSQL. A PostgreSQL backup can run while normal writes continue:

```sh
mkdir -p backups
chmod 700 backups
umask 077
docker compose exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom' > backups/labops.dump
```

Verify by restoring into a separate scratch database. These commands create `labops_restore`; choose an unused name if one already exists:

```sh
docker compose exec -T postgres sh -c 'createdb -U "$POSTGRES_USER" labops_restore'
docker compose exec -T postgres sh -c 'pg_restore -U "$POSTGRES_USER" --dbname=labops_restore --exit-on-error --no-owner' < backups/labops.dump
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore web python manage.py migrate --noinput
docker compose run --rm --no-deps -e POSTGRES_DB=labops_restore web python manage.py reconcile_stock
```

Inspect restored application records before accepting the backup. Schedule backups and define retention and recovery objectives for the deployment. For a real recovery, stop all writers and consumers, restore into a fresh database, verify it, update `POSTGRES_DB` consistently, then recreate the application services. When restoring an older database snapshot while Kafka retains newer offsets, explicitly plan consumer offset rewind and event replay; a database restore alone is not a complete event-system recovery. Keep database and event retention long enough for that procedure. Never use `docker compose down -v` unless intentionally deleting all local database and broker data.

## Validation and fault drills

CI runs migrations, migration-drift detection, system checks and the full suite against PostgreSQL 17. A separate SQLite demo job runs its supported regression suite and seeded reconciliation. PostgreSQL concurrency tests explicitly skip SQLite and should not be reported as SQLite concurrency validation.

Use a disposable database for fault drills: pause the broker and verify business transactions commit while the outbox grows; resume it and verify backlog drains. Stop a consumer, inspect group lag, restart it, then reconcile. Replay a duplicate and verify no extra effects. Stop Redis and verify catalog fallback plus the documented 503 behavior. Kill a publisher after acknowledgement and before its database update to exercise duplicate publication. Record actual results separately from the existence of configuration and test code.


### Consumer process-death drill

The checked-in harness kills the real `consume_kafka` process with SIGKILL after it reaches a controlled boundary. It covers both notification and analytics consumers, before the database commit and after commit but before broker offset acknowledgement. It runs the production command and delivery functions; test-only wrappers signal the boundary to the parent process. The parent kills the child externally, restarts the same consumer group and proves the identical topic/partition/offset is replayed without duplicate database effects.

Run against local development services with a PostgreSQL role that can create databases:

```sh
export DRILL_DATABASE_URL=postgresql://labops:labops-local@127.0.0.1:55433/postgres
.venv/bin/python benchmarks/consumer_crash_drill.py --bootstrap-servers 127.0.0.1:19093
```

The harness creates a uniquely named `labops_phase8_*` database and isolated broker topics/groups, then cleans up only those generated resources. It refuses non-loopback database/broker endpoints. It does not stop the main web service or its consumers. Results are written to `benchmarks/results/consumer-crash.json`. Check the exit status and all assertions; merely receiving the SIGKILL signal is not a successful recovery test. This is single-node process recovery, not replicated failover or an external email/API exactly-once guarantee.


### Full PostgreSQL service-outage drill

Use a dedicated disposable PostgreSQL container, not the Compose database used by the preview or another application. The script stops the entire server, so every database on that server is interrupted. It requires an exact container-name acknowledgement, rejects Compose services and non-loopback database URLs, verifies the mapped PostgreSQL port, and creates its own unique scratch database.

For the dedicated local container used in this validation:

```sh
DATABASE_URL=postgresql://labops:labops-local@127.0.0.1:55432/postgres \
  .venv/bin/python benchmarks/postgres_outage_drill.py \
  --container labops-upgrade-postgres \
  --confirm-stop-container labops-upgrade-postgres --port 8003
```

The named container must already exist; substitute your dedicated container and its mapped port. The harness starts an isolated two-worker Gunicorn instance, signs in with generated demo data, stops PostgreSQL, verifies stable HTTP 503 errors, then restarts it. It compares all application table hashes before retrying the same stock command twice and checking one mutation plus reconciliation. It removes its generated database and restores the container's initial running state. Results are written to `benchmarks/results/postgres-outage.json`; a local `.server.log` is diagnostic output, not a credential or response-body archive.

Database connectivity failures return `DATABASE_UNAVAILABLE`, `Retry-After: 5`, and `Cache-Control: no-store`. The response covers API dispatch and database-backed authentication/session loading in views. Retry mutations with the same idempotency key: a connection loss around commit can leave the client uncertain whether its original command committed. Do not invent a new key merely because the first response was unavailable. The outage test above verifies an already-stopped server; the independent connection-termination test separately exercises rollback during posting.
