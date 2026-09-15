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
