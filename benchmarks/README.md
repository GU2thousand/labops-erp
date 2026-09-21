# LabOps benchmark evidence

Measured locally on September 21, 2026. Results are observations, not production capacity guarantees.

## Environment and fixture

- Apple M4, 16 GiB host memory; Docker allocated 8 CPUs and approximately 3.83 GiB RAM. PostgreSQL 17 runs in Docker; Django/Python 3.12 and Gunicorn run on the host for these measurements.
- Fixture v1 uses deterministic UUID5 identifiers and fixed quantities: 1,000 items, 10,000 batches, 100,000 posted ledger lines, 10,000 purchase requests and 10,000 orders. Dates are relative to the seed execution day.
- The load runner clones a clean seeded template for every tier; mutations within a tier are expected. Every disposable clone is reconciled before removal.
- Cache, Redis admission control, broker consumers and OTLP export are disabled for the HTTP benchmark. No analytics reads authorize inventory writes. Ordinary metrics instrumentation remains enabled in the upgraded code.

## Authenticated handler comparison

Django test client requests execute the real authentication, middleware, routing and database code in one process, without a network server. Three warmups and 30 measured requests per endpoint. Counts include session/auth queries. This measures the complete handler; it is not concurrent HTTP throughput.

| Endpoint | Before p50 / p95 (ms) | After p50 / p95 (ms) | Queries before → after |
|---|---:|---:|---:|
| `inventory` | 822.96 / 895.86 | 131.15 / 170.91 | 1005 → 5 |
| `reports` | 1849.91 / 2061.63 | 255.00 / 485.01 | 9 → 7 |
| `movements` | 80.11 / 110.66 | 36.57 / 45.34 | 86 → 7 |

The baseline is the original query implementation; the upgraded path includes database aggregation, eager loading and new metrics instrumentation. PostgreSQL inventory/cost expressions multiply by exact decimal scale factors, including a large-value micro-unit regression test. The movement list prefetches lines and annotates reversal existence. Improvements are not attributed to an unmeasured index: this iteration primarily reduces query count, ORM materialization and Python aggregation.

Raw [before](results/before.json) and [after](results/after.json) contain samples and real `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plans. Identical SQL is grouped with an occurrence count, retaining its first measured plan; synthetic session tokens are redacted. Query plans are collected after the timing samples. Sequential scans can be appropriate for a full-table aggregate.

## Real Gunicorn / k6 mixed workload

Two Gunicorn workers, four threads each; 30 seconds per tier with a 50 ms iteration pause. Approximate operation selection: 60% inventory reads, 15% request creation, 10% approval workflows, 10% receipt/issue workflows, 5% reports. An approval or receipt workflow generates multiple HTTP requests, so these are operation proportions, not request proportions. Setup authenticates separate admin/buyer sessions and validates fixtures. Script exceptions count as business failures. The summary includes setup traffic and graceful iteration completion.

| VUs | HTTP requests/s | p50 (ms) | p95 (ms) | p99 (ms) | HTTP / business error rate | Ledger reconciliation |
|---:|---:|---:|---:|---:|---:|---|
| 20 | 23.19 | 855.52 | 1681.88 | 2318.93 | 0.00% / 0.00% | OK |
| 50 | 39.87 | 1131.59 | 1926.53 | 2262.80 | 0.00% / 0.00% | OK |
| 100 | 41.41 | 1046.39 | 4564.82 | 4912.68 | 0.00% / 0.00% | OK |
| 200 | 33.79 | 3929.41 | 8066.56 | 8533.49 | 0.00% / 0.00% | OK |

**The p95 < 1 second target is not met by the 20-VU mixed workload.** Latency rises with contention/queueing. These short runs establish a reproducible baseline and successful invariants, not sustained high-concurrency capacity. Future work should separately measure hot-project writes, disjoint projects/batches, arrival-rate load, longer warmup/steady-state phases, lock waits and connection-pool behavior before choosing another optimization.

See [runner metadata](results/load-environment.json), `results/k6-*.json` and corresponding logs. VU-based closed-loop throughput includes response-time backpressure; it is not a fixed-arrival-rate capacity test. The random workload mix introduces normal run-to-run variation. CPU scheduling, Docker memory and other host activity are not controlled as in a dedicated benchmark environment.

## Reproduce

Use a separate PostgreSQL database and a role with CREATEDB. Never seed or run write load against application data.

```bash
export DATABASE_URL=postgresql://labops:labops-local@127.0.0.1:55433/labops_benchmark
.venv/bin/python manage.py migrate
.venv/bin/python manage.py seed_benchmark
.venv/bin/python manage.py benchmark_queries --output benchmarks/results/local.json --samples 30
.venv/bin/python benchmarks/run_load.py --template-url "$DATABASE_URL"
```

Create the empty benchmark database first (`createdb` or an equivalent PostgreSQL administrative command). The seeder refuses a database containing users/items. The runner creates uniquely named disposable clones, starts a local Gunicorn process, runs the checked-in k6 script, reconciles each clone, and removes it. Install k6 separately. Existing template records are retained.

## Fault evidence

[Failure drill results](results/failure-drills.json) record actual broker/Redis stop-and-restart tests, duplicate publication into a real broker after an injected acknowledgement/mark crash, observed DLQ messages, and pg_dump/pg_restore recovery. Tests also terminate an actual PostgreSQL command connection midway through posting and verify full rollback. These are local process/service drills, not replicated infrastructure failover.


Additional Phase 8 evidence: [real consumer SIGKILL results](results/consumer-crash.json) cover four transaction/offset crash windows across both consumers. The [reproducible harness](consumer_crash_drill.py) runs against isolated, generated databases/topics/groups and verifies replay of the same broker offset, one committed database effect, and final ledger/projection reconciliation. See [operations](../docs/operations.md#consumer-process-death-drill) for execution and cleanup behavior.

The [full PostgreSQL outage evidence](results/postgres-outage.json) records real authenticated HTTP requests while a dedicated database container is stopped, unchanged hashes for all 35 application tables, and successful same-key recovery without duplicate stock effects. Reproduce with [postgres_outage_drill.py](postgres_outage_drill.py) using the dedicated-server safeguards in the [operations guide](../docs/operations.md#full-postgresql-service-outage-drill).
