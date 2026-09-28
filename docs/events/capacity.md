# Capacity, recovery targets, and measurement contract

Status: production capacity is **unmeasured**. The following synthetic starting
targets come from the upgrade plan and must be frozen in the workload manifest
before a run. A reduced CI smoke workload exercises logic and records its own
denominators; it does not meet these duration or capacity targets.

| Dimension | Frozen starting assumption/target | Current measurement |
|---|---|---|
| Average/peak real inventory events/s | Collect from the deployment before sizing | Unmeasured |
| Steady synthetic workload | 50 unique inventory events/s for 1,800s, 90,000 events | Pending full execution |
| Event mix | Valid opening/receipt/issue/transfer/adjustment/reversal using committed ledger commands | Fixture mix must be recorded per run |
| Per-consumer complete effects | 90,000/90,000 effects or original-ID dedupe proofs; zero lost/extra effects | Pending full execution |
| Business-effect latency | p95 <= 5s, p99 <= 15s, measured to committed effect | Pending full execution |
| Broker/analytics downtime | 600s at 50/s, target input 30,000 unique events | Pending duration-qualified execution |
| Recovery catch-up | Drain within 900s after availability, with ledger/projection reconciliation | Pending full execution |
| Crash/rebalance repetitions | 20 per required scenario and consumer/boundary | Pending full execution |
| Duplicate workload | 10,000 IDs, two additional broker publishes each | Pending full execution |
| Poison workload | 100 illegal messages; application oversize and broker oversize separated | Pending full execution |
| Inventory retention | 30 days, `cleanup.policy=delete` | Configured candidate; disk suitability unmeasured |
| DLQ retention | 90 days | Configured candidate; privacy/capacity approval pending |
| Dedupe/outbox/audit retention | Retain all initially; no automatic deletion | Actual size/growth unmeasured |
| Restore RTO | <= 1,800s starting target | Isolated measured restore required |
| Database RPO | Set from actual backup/PITR interval and tested business watermark | Unknown; broker RF3 does not make it zero |
| Budget / broker disk / DB disk / fault domains | Deployment-specific decision | Not supplied |

## Size worksheet

Report payload p50/p95/max and line/recipient distribution for actual observed
events. If using synthetic fixtures, label them synthetic and include seed,
fixture/schema hash, committed command count, unique event count and exact encoded
byte samples. Do not infer HTTP commands/s from events/s: a command may produce a
different number of events.

For an explicitly hypothetical mean envelope size of 4 KiB at 50 events/s,
30-day inventory retention is approximately 531 GB of logical record bytes and
1.59 TB across RF3, before indexes, segment overhead, compression, headroom or
other topics. That is arithmetic from assumptions, not a measured capacity
recommendation. Replace size/rate with real samples and set an operational disk
headroom threshold before provisioning. Outbox, ProcessedEvent, FailedDelivery,
audit and notification table growth need a separate PostgreSQL estimate.

## Counting and latency

Always retain attempted input, committed inventory commands, unique original
event IDs, actual broker records, processed markers/effects per consumer,
duplicates, RETRY, DEAD, rejected publishes, errors/timeouts and unfinished events.
Failures and unfinished events stay in the denominator. With a sample of only a
few dozen events, a successful p99 is descriptive and cannot establish a long-run
tail-latency target.

Business-effect latency starts at the original outbox commit/occurrence and ends
at the successful database effect commit. Publish ack latency, broker lag and
scrape age are separate measurements. A parked retry with committed broker offset
is unfinished business work. Preserve raw timestamps and clock source/uncertainty
so latency can be recalculated; collect client overhead and metrics ledger-scan
cost using the same workload with observability enabled and disabled.

The final report must explain throttling, disk/CPU/RAM, poll/commit budgets,
single-row flush throughput, publisher shard count, successful catch-up rate and
all fault windows. Tune batching only if the measured workload misses capacity;
never weaken acknowledgement, transaction or dedupe boundaries to hit a target.
