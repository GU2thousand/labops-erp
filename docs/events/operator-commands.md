# Audited event recovery commands

Run from the repository root using the deployment's protected PostgreSQL and
Kafka environment. Examples below use `python manage.py`; with the development
stack prefix each with `docker compose exec web`. Restrict shell/output access:
outbox inspection can include recipient, trace and original payload fields.

## Inspect before changing state

```sh
python manage.py outbox_events inspect
python manage.py event_failures inspect
```

`event_failures inspect` hides envelope content by default. Authorized responders
may add `--include-payload` only into protected incident evidence. Record source
cluster, stream generation, delivery coordinates, original hash, status and age;
an offset with zero lag may still correspond to unresolved business work.

For poisoned input, inspect the evidence encoding as well as the checksum.
Kafka ingress preserves invalid UTF-8/JSON, nonfinite/deep JSON and NUL input as
base64 of original broker bytes. Direct delivery of an already parsed NUL value
preserves canonical JSON bytes and marks `canonical-json-with-nul`; its lexical
source bytes are unavailable. The semantic JSON checksum normalizes integral
numeric representations across PostgreSQL JSONB, so it is distinct from an exact
raw-byte digest. See [poison evidence](adr-002-immutable-contracts.md#poison-evidence-and-raw-bytes)
before interpreting a conflict or reconstructing restricted incident data.

## Retry and disposition

Stop the relevant retry/DLQ worker for manual disposition, wait for live leases to
expire or drain, fix the dependency/consumer defect, and retain the original
payload. Replace UUIDs and incident references in these examples with reviewed
targets. The reason describes the evidence for the decision, actor identifies
the operator, and authorization references the approved incident/change record.

```sh
python manage.py outbox_events retry --id EVENT_UUID \
  --reason 'Broker restored; original envelope verified' \
  --actor 'oncall@example.invalid' --authorization 'CHANGE-123'
python manage.py event_failures retry --id DELIVERY_UUID \
  --reason 'Consumer dependency restored; original hash verified' \
  --actor 'oncall@example.invalid' --authorization 'INCIDENT-123'
python manage.py event_failures resolve --id DELIVERY_UUID \
  --reason 'Documented disposition; no business effect requested' \
  --actor 'oncall@example.invalid' --authorization 'INCIDENT-123'
```

Outbox retry requeues only DEAD publisher rows. Failed-delivery retry resets the
durable schedule without changing identity/content. Resolve records a disposition
and does not apply the event. The latter rejects live retry/DLQ leases and a
changed retained hash. Append-only DeliveryAudit stores actor, time, reason,
authorization reference, before/after state and original hash; prior history is
not replaced by the most recent `resolution_note`.

These references are audit evidence, not an identity provider or an automatic
permission grant. OS/database access and the deployment's operator approval
process enforce who may invoke the command. Review the resulting audit and
reconcile effects before closing an incident. PostgreSQL failure during recovery
must leave a write uncommitted.

## Bounded retained-outbox replay

This tool reads the original retained database outbox and applies it to one
database consumer. It does not republish into Kafka, consume an arbitrary broker
range, or reset a live consumer group's offsets. Start/end use UTC, start
inclusive and end exclusive. Limit is 1-100,000; rate is finite and >0 up to
1,000 events/s. Narrow further with repeated `--event-id` options if needed.

Dry run is the default and makes no recovery writes:

```sh
python manage.py replay_events --consumer analytics \
  --start 2026-09-27T00:00:00Z --end 2026-09-28T00:00:00Z \
  --limit 100 --rate 10 --actor 'oncall@example.invalid' \
  --reason 'Verify restored original IDs and checksum boundary' \
  --authorization 'RESTORE-123' --dry-run
```

Preserve the matched/selected counts and every original ID/hash in the output.
Review `would_process`, `would_dedupe` and failures against the restore watermark.
The limit truncates selection; a successful limit of 100 does not mean the full
matched range was processed.

Only after that review, repeat the exact selection with `--execute`:

```sh
python manage.py replay_events --consumer analytics \
  --start 2026-09-27T00:00:00Z --end 2026-09-28T00:00:00Z \
  --limit 100 --rate 10 --actor 'oncall@example.invalid' \
  --reason 'Apply reviewed restore range; preserved original IDs' \
  --authorization 'RESTORE-123' --execute
```

Execute requires actor, reason and authorization. Each effect/duplicate/failure
is audited; summary retains selected, effects, duplicates and failed counts.
A failure exits nonzero. Stop and investigate conflicts or missing original
relations. Never clear processed markers or generate new IDs to force effects.
For notification replay, inspect restored users/notification state and the scope
of any real external delivery before applying a range.

## Conditional notification member canary

The acceptance harness's `notification-dual` preset is currently an unexecuted
candidate. Production scaling requires successful correctness/capacity evidence
and authorization for the actual deployment and bounded canary. First verify
the topic/source namespace, existing group and members, three partitions, role
credentials/ACLs, retained offsets, supervisor ownership and PostgreSQL connection
headroom. Budget the additional owning connection and actual transient metrics
connections; a long-running daemon's connection age setting does not establish
per-record connection closure.

Run the added member as a separately supervised instance of the same
`python manage.py consume_kafka notification` command, with the same intended
group prefix and notification credentials. Give it a distinct process/container
identity and metrics port or container endpoint. The static Prometheus target
`notification-consumer:9100` alone does not prove both instances were scraped;
verify each authenticated endpoint and its actual process identity. An
unqualified Compose scale command is insufficient for this admission.

Before canary input, observe exactly the two owned STABLE clients with nonempty,
disjoint assignments covering all three partitions. Preserve the existing
per-record durable transaction and synchronous ACK, group identity and offsets.
Measure logical event/effect/notification counts, deduplication, committed next
offsets, all errors, original latency gates and DB/CPU/RSS pressure. A second
logical group, offset reset or automatic commit/store would change the experiment.

If admission or canary fails, SIGTERM and reap only the newly added member.
Verify the retained member reacquires all three partitions and continues from
retained group offsets; original-ID redelivery must remain deduplicated. Record
the exit, assignment and effect/offset outcomes. A forced stop or missing proof
does not establish a successful rollback; retain the failure for investigation.

## Independent worker commands

```sh
python manage.py process_events --loop
python manage.py publish_events --loop --limit 100 --shard-index 0 --shard-count 1
python manage.py consume_kafka notification
python manage.py consume_kafka analytics
python manage.py retry_events --loop --limit 100
python manage.py publish_dlq --loop --limit 100
```

Use separately supervised processes and role credentials. Retry is PostgreSQL
only and needs no broker identity. Publisher, two consumers and DLQ each expose
their own authenticated worker metrics when enabled. Do not launch several
same-host workers on the same metrics port; assign a unique port or use separate
containers. SIGTERM finishes one bounded durable operation and a bounded producer
flush; forced death may legally cause original-ID redelivery.

`publish_events --loop` does not reacquire a lost dedicated PostgreSQL ownership
session: it purges client-pending sends and exits nonzero. After PostgreSQL is
healthy, its supervisor must start a **new process** that acquires a new shard
session. `consume_kafka` also exits if an effect or failure record cannot be
committed to PostgreSQL; restart the same consumer group to replay the offset
that was not committed. Existing outbox/failed records and leases remain intact,
and abandoned claims become eligible through natural lease expiry. Do not confuse
this process restart with a same-process resume after a broker pause.

Compose application workers use `restart: unless-stopped`; host deployments
and Kubernetes must provide an equivalent failed-process restart policy and
restart monitoring. For intentionally stopped development workers, explicitly
start them after database health is restored:

```sh
docker compose --profile events up -d publisher notification-consumer analytics-consumer retry-worker dlq-publisher
```

Record process exit and replacement start times, original-ID/offset replay and
eventual effects. Do not delete leases, markers or parked records as a restart
shortcut. See the [ownership and restart boundary](adr-003-ordering-and-ownership.md).

Continue `process_events` for imports, daily alerts and local routes. It does not
replace Kafka consumers, and Kafka consumers do not replace it. After command
changes, `python manage.py COMMAND --help` is the authoritative argument list.
