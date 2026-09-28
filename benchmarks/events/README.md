# Secure RF3 event acceptance

Run `acceptance.py` only against the fresh disposable cluster prepared by
`infra/events/validation/prepare.py`. It refuses non-loopback endpoints, a
nonempty business database, incorrect run/group identifiers, plaintext transport,
and a reused acceptance evidence directory. Infrastructure evidence may already
exist; `.acceptance-started` exclusively claims this execution. Cleanup belongs
to the workflow which created that exact Compose project.

The script uses real inventory services, PostgreSQL transactions, TLS/SCRAM
identities, broker records and independent worker processes. Receipt, issue,
transfer and reversal commands produce legitimate synthetic stock ledgers. HTTP
command throughput is a separate measurement.

After preparing the cluster, applying identities/ACLs, creating topics and
starting the restricted exporter, a short integration run is:

```bash
python benchmarks/events/acceptance.py \
  --run-id "$RUN_ID" --events 60 --rate 10 --duration 0 \
  --fault-repetitions 1 --fault-events 20 \
  --evidence-dir "evidence/$RUN_ID"
```

The initial staging capacity and fault-duration targets require a separate full
manual run, on a sufficiently provisioned disposable host:

```bash
python benchmarks/events/acceptance.py \
  --run-id "$RUN_ID" --events 90000 --rate 50 --duration 1800 \
  --fault-repetitions 20 --duplicate-events 10000 --poison-events 100 \
  --fault-events 30000 --single-broker-outage-seconds 300 \
  --all-broker-outage-seconds 600 --consumer-outage-seconds 600 \
  --drain-timeout-seconds 900 --evidence-dir "evidence/$RUN_ID"
```

The 90,000 steady-state events and each fault scenario's additional events have
separate denominators. The full manual command can take considerably longer than
30 minutes. Parameters are frozen before execution. Slower business commands,
publisher throughput, failures and unfinished effects remain visible; successful
short integration runs do not establish the full workload target.

## Cases and evidence

`events.jsonl` retains every generated event ID, kind, scenario, command timing,
insertion transaction ID and observed PostgreSQL commit timestamp. The insertion
transaction ID is captured inside an outer business transaction; later publisher
updates cannot replace it. `latency.csv` retains every steady event/consumer pair,
including missing commit timestamps. Effects use `ProcessedEvent.xmin` commit
timestamps. Full sample coverage, p95 ≤ 5 seconds and p99 ≤ 15 seconds are gates;
outbox creation timestamps are not substituted for transaction commits.

The harness records and verifies:

- Exact ledger/balance/projection quantities, counts and canonical hashes;
  expected active-recipient notifications and per-consumer dedupe counts.
- Two real duplicate publications per selected unique event, with observed
  consumer delivery denominators and zero additional database effects.
- Real publisher SIGKILL before send and after broker acknowledgement, lease
  expiry and stale-owner recovery with stable event IDs and rejected writeback.
- Real notification/analytics SIGKILL inside the effect transaction and after
  its outer commit but before broker offset commit; the same source coordinate
  must be redelivered and then committed.
- Real broker process stop/start, one broker, quorum loss and whole-cluster loss;
  committed business operations survive failed publication budgets.
- Independent notification progress while analytics is stopped, measured drain
  timing, actual consumer-group membership/assignments for 1→3→1 and shutdown
  outcomes including any forced kill escalation.
- PostgreSQL server stop during an uncommitted business command, consumer effect
  transaction and failed-delivery persistence. Partial effects remain absent and
  unpersisted outcomes cannot advance offsets.
- Durable retry and independent DLQ workers with append-only action audit;
  poisonous JSON/schema/UUID/time/decimal/size variants, valid JSON with NUL,
  near-1-MiB invalid raw data, normal messages continuing, and actual DLQ record
  inspection checking stable delivery IDs and original hashes.
- Six security negative classes, explicit authentication/authorization/TLS
  denial evidence and correlated broker log evidence where an anonymous client
  sees only a disconnect. Mere timeouts do not pass.
- Frozen-watermark `pg_dump` into a separate database, exact restored hashes for
  nine business/event/audit tables, legal-ledger projection rebuild and actual
  same-ID replay against restored database effects.
- Authenticated metrics from all five independent worker roles, denied anonymous
  scrapes, actual broker/replica/group-lag exporter metrics and durable RETRY/DEAD
  gauges distinct from group lag.

`harness-manifest.json` preserves the acceptance runtime metadata without secrets;
the workflow's infrastructure collector writes `manifest.json`. Every process log
has a unique generation filename. `markers/` contains precise fault boundaries;
`offsets-before.json`, `offsets-after.json`, `reconciliation.json`, `report.json`,
`summary.md` and `errors.jsonl` retain results and raw failure denominators. Keep
artifacts access controlled: the synthetic database backup contains users,
notifications and outbox payloads. Client credentials/private keys are never
copied into evidence.

## Required exercises beyond this harness

This same-host container run measures protocol/process failures. Independent
host/AZ failures, actual Prometheus alert trigger/recovery, PostgreSQL PITR,
older-snapshot business losses, retention exhaustion and same-name topic
replacement remain required staging exercises and are prominently marked
unexecuted in every summary. Frozen snapshot restoration reports a watermark,
snapshot RPO and measured restore time; it makes no claim about production RPO.
No result here establishes public deployment or exactly-once external email/SMS.
