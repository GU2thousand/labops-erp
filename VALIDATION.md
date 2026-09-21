# Backend upgrade validation — September 21, 2026

## Automated verification

- Original 29 acceptance tests first passed on PostgreSQL before refactoring (17.705 seconds).
- Final PostgreSQL suite: **60 tests passed**, including migration of existing ledger data, independent-connection concurrency, real PostgreSQL connection termination, consumer duplicate races, deferred foreign-key retry progression, numeric precision, query budgets and Redis failure policies.
- SQLite compatibility: **60 discovered, 44 passed, 16 PostgreSQL-only tests skipped**. A separate file-backed SQLite test database was used.
- `makemigrations --check --dry-run`, Python compilation, JavaScript syntax, and whitespace checks passed.
- The draft-edit race regression passes with the new lock selector and was independently observed failing when the previous selector was restored in memory.

## Actual service and recovery exercises

- PostgreSQL 17.11: created fresh and benchmark databases; migrated them; used independent connections for acceptance tests. A connection was terminated after ledger/balance writes but before event insertion; the complete command rolled back.
- Redpanda v25.1.9: stopped the broker, committed an inventory issue, and observed a pending outbox event plus a failed publish attempt. Restarted the broker and verified publication.
- Injected process death after real broker acknowledgement but before the database mark. Expired the lease to advance the test clock, republished the same event, consumed actual duplicates, and verified one effect per consumer plus projection-to-balance agreement.
- Sent an unsupported-schema message through the real broker. Both consumers parked it durably, bounded retries reached DEAD, and two DLQ records were read back from the real DLQ topic.
- Redis 7: confirmed cache hit/invalidation and an atomic 30-request bucket test (10 accepted, 20 rejected). Stopped Redis; catalog fell back to the database, protected login returned 503, and an inventory issue still committed and reconciled.
- PostgreSQL custom-format dump restored into a separate database; restored inventory passed ledger reconciliation. Broker-offset recovery after restoring an older database remains an operator procedure, not a tested automatic feature.

## Full-stack smoke test

- Built and started all Compose profiles together: PostgreSQL, two-worker Gunicorn, local worker, Redpanda publisher and both consumers, Redis, Prometheus, Grafana, OpenTelemetry Collector and Jaeger. Application/database/broker/cache health checks passed.
- Real browser sign-in and inventory rendering succeeded; the stock reconciliation dialog reported all balances match posted movements.
- Prometheus reported all three configured targets up (LabOps, Redpanda, Kafka exporter); Grafana health returned database `ok`.
- Posted a stock issue, observed its outbox status `PUBLISHED`, one processed record for each consumer, no unresolved consumer failures and complete projection-to-balance agreement. Jaeger returned one propagated trace containing `command.issue`, `inventory.post`, `outbox.publish`, `consumer.notification`, `consumer.analytics` and SQL spans.
- Initial startup under host resource pressure produced transient trace-export timeouts; the subsequent business trace was received successfully. This does not establish lossless telemetry delivery.

## Performance

See [benchmark report](benchmarks/README.md) for raw samples, plans and the exact execution environment. The fixed fixture contains 1,000 items, 10,000 batches, 100,000 ledger entries and 10,000 requests/orders. Real 20/50/100/200-VU mixed HTTP runs completed without HTTP/business errors and each reconciled its ledger. **The 20-VU mixed workload did not meet p95 < 1 second.** No sustained production capacity or high-availability claim is made.

## Verification boundaries

Local service shutdown, injected crash windows and individual connection loss were exercised. Production deployment, replicated failover, external email/API exactly-once effects, sustained stress/soak tests and an exhaustive browser regression were not performed. Historical records below describe the earlier SQLite application and do not supersede this upgrade record.

---

# LabOps Acceptance Record

## English localization verification — September 15, 2026

- All 29 existing business acceptance tests passed in 24.278 seconds using a separate file-backed SQLite test database.
- Django system checks, migration consistency, JavaScript syntax, and `git diff --check` passed. The Python syntax-tree comparison confirmed that Python edits changed text literals only.
- Scanned every tracked source/documentation file: no Chinese text or full-width Chinese punctuation remained.
- Created a fresh isolated database, applied migrations, and ran both demo seeds. All LabOps model records, including generated notifications and audit snapshots, contained English text. Checked 23 authenticated API endpoints and an English pagination error. Fresh and existing databases both passed stock reconciliation.
- Browser checks covered all navigation modules, translated demo labels, English dates, duplicate-code field errors with retained input, incorrect-password errors, and successful sign-in.
- Inspected desktop layout at 1440×1000 and mobile layout at 390×844. After adjusting wrapping and table scrolling, document widths were 1425px and 375px respectively, with no page-level horizontal overflow.
- Before translating the existing local demo database, created a verified SQLite backup. Updated 50 demo business records and 88 notifications with appended translation audit events. Compared original audit records, stock movements, movement lines, and balances before/after: all remained unchanged. Historical source text in immutable records remains verbatim; existing user-entered content is not automatically translated by the app.

## Original business acceptance — September 12, 2026

Environment: local macOS, Python 3.14, Django 5.2.17, and a file-backed SQLite database. These tests were not run against PostgreSQL or a production deployment.

All 29 tests passed; the final original run took 15.076 seconds. Test file: `labops/tests/test_acceptance.py`.

| PRD ID | Scenario | Result |
| --- | --- | --- |
| A01 | Trim and uppercase codes; reject duplicate items | Passed |
| A02 | Administrator attempts to approve their own request | Rejected |
| A03 | Independent connections concurrently allocate 70 and 50 against an approved quantity of 100 | One succeeds; one returns `OVER_ORDERED` |
| A04 | Order quantity 100, received 60, attempt to receive another 50 | Rejected; stock and successful audit records unchanged |
| A05 | Retry a posted receipt with the same and a new idempotency key | No duplicate stock receipt |
| A06 | Stock 10; two threads each issue 7 | One succeeds; one fails; balance is 3 |
| A07 | Inject a destination-warehouse write failure during transfer | Source, destination, and movement all roll back |
| A08 | Issue an expired batch or issue to a completed task | Rejected; stock unchanged |
| A09 | Reverse a receipt after some stock has been issued | Insufficient stock; rejected without negative balances |
| A10 | Reverse the same original movement twice | Second reversal rejected; cost and stock restored only once |
| A11 | Interrupt an import after one successful row, then resume | Successful row skipped; no duplicate record |
| A12 | Update a task using an old version | `VERSION_CONFLICT`; newer state preserved |
| A13 | Ordinary member accesses a known ID from another project | Reads and writes rejected |
| A14 | Consume a notification event twice | One notification per recipient |
| A15 | Receive 60, issue 10, transfer 5, reverse the issue | Balances 45+5, cost $120; after reversal total 60, cost $0; ledger reconciles |
| A16 | Skip sample receipt and attempt processing directly | Rejected; no successful transition event |

Additional coverage includes order closure and reopening after reversal, atomic multi-line issues, exact six-decimal arithmetic, issue on the expiry date, closing the opening-stock entry point, count-version conflicts, rejecting positive adjustments to expired batches, master-data deactivation constraints, unit locking after transactions, duplicate import codes and execution-time version conflicts, CSV formula escaping, CSRF, inactive-account sessions, auditor read-only permissions, list/detail APIs, pagination and sort allowlists, creation/issue-draft idempotency, sample barcode uniqueness and project scope, lab-order completion/cancellation prerequisites, asynchronous imports returning 202, and transaction lines in audit snapshots.

## Original browser validation

The real local Django service was exercised in the Codex browser using the administrator demo account:

- Sign-in succeeded. Dashboard database metrics showed 3 pending requests, 3 low-stock items, 4 expiring internal batches, and 2 overdue tasks.
- Created item `QA-UI-001`; it appeared immediately and persisted after reload.
- Opened an existing purchase order showing 60 received and 40 remaining.
- Entered receipt quantity 50; received `OVER_RECEIVED`, retained the form values, and saw a quantity-field error.
- Changed the received quantity to 40, saved a receipt draft, and posted it successfully.
- Reversed that receipt with a reason; the original demo balance was restored and movement history retained.
- Uploaded a fictional two-row UTF-8 CSV. Preflight passed, confirmation returned Running, and the worker updated it to Completed with 2 successful rows and 0 failures.
- Transitioned `SIM-2609-0001` from Registered to Received; its details showed an event with actor and timestamp.
- Inspected the desktop dashboard and a 390×844 mobile viewport. Mobile document width was 375px, below the 390px viewport, with working expandable navigation and actions. Wide tables used their own horizontal scrolling regions.

The browser checks did not cover every button combination for every role. Server-side tests cover cross-project access, auditor write restrictions, and concurrency conditions.

## Backup recovery exercise

Created a separate snapshot using the SQLite backup API. `PRAGMA integrity_check` returned `ok`. Reopened the snapshot through `LABOPS_DB` and ran `reconcile_stock`; all inventory balances matched posted movements. This exercise did not cover PostgreSQL backups, remote backup media, or long-term retention policies.

## Unverified targets

No claim is made that the PRD target of p95 < 1 second with 1,000 items, 10,000 batches, 100,000 movements, and 20 concurrent sessions has been met. No formal accessibility audit, production security audit, or public deployment was performed. PostgreSQL needs validation in the target environment. See README for implementation boundaries and unconfigured features.
