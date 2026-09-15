# LabOps Laboratory Operations ERP

LabOps is a standalone web ERP learning project based on `LabOps_Requirements_ERD_v1.docx`. It provides a English-language interface, server-side authorization, persistent database storage, and fictional demo data. The application is a modular monolith built with Django 5.2.17. It uses SQLite locally and supports PostgreSQL configuration. It does not depend on ERPNext or connect to SpectraCell or any real laboratory system.

The interface, validation messages, generated notifications, documentation, and newly seeded demo data are in English. Dates and numbers use US English formatting; times remain in `America/New_York` and amounts remain in USD. Existing database content is not overwritten by seeding, and historical audit/ledger text is retained verbatim.

## Getting started

After cloning the repository, run the following commands. If you already have a local copy, open its project directory and run `./run.sh`.

```bash
cd labops-erp
./run.sh
```

On macOS, you can also double-click `start.command`. Open [LabOps](http://127.0.0.1:8765/). The script applies database migrations, seeds demo data only when the database is empty, and starts both the web server and background worker. Seeding does not overwrite existing data. Close the terminal or press Ctrl+C to stop the services.

A new machine needs Python 3.12 or later. On its first run, the script creates a virtual environment and installs the pinned dependencies. The original development environment used Python 3.14. The frontend uses plain JavaScript, HTML, and CSS; Node.js, npm, and a frontend build step are not required.

## Demo accounts

All accounts below use `LabOpsDemo!2026`. This password is intended only for local use with fictional data.

| Email | Display name | Role |
| --- | --- | --- |
| admin@labops.local | Lin Zhiyuan | Administrator |
| reviewer@labops.local | Chen Sining | Second administrator; can approve another user's requests |
| manager@labops.local | Zhou Yiran | Project manager |
| buyer@labops.local | Xu An | Purchasing officer |
| store@labops.local | Lu Chuan | Warehouse operator |
| tech@labops.local | Su He | Laboratory technician |
| auditor@labops.local | Shen Qing | Auditor; read-only access |

To switch accounts, sign out at the bottom of the sidebar. The server checks whether the account is active before every write. Administrators cannot approve their own purchase requests.

## Implemented workflows

- **Users and master data:** Django password hashing and sessions, CSRF protection, login rate limiting, role assignment, and account deactivation. Items, suppliers, and warehouses support creation, editing, deactivation, search, pagination, code normalization, and transaction-reference constraints.
- **Projects and tasks:** Project owners and members, a task board, status transitions, assignee validation, blocking reasons, comments, material issue history, optimistic version checks, and authorization scoped to project membership.
- **Purchasing:** Requests with multiple lines, withdrawal and resubmission, approval by another user, allocation of approved request lines across purchase orders, order confirmation and cancellation, partial receipts, a separate batch cost layer for each receipt, prevention of over-allocation and over-receipt, automatic order closure when fully received, and reopening after a receipt reversal.
- **Inventory:** Stock overview, unexpired quantities available for issue, batch and expiry tracking, draft material issue requests, atomic document posting, warehouse transfers, opening stock, count adjustments, line-by-line reversals, idempotency keys, and balance reconciliation. Posted ledger entries are not edited directly.
- **Imports:** UTF-8 CSV files up to 5 MB and 5,000 rows, downloadable templates, optional column mapping, explicit CREATE/UPDATE modes, row-level preflight validation, duplicate-code checks, execution-time version checks, background processing and status polling, skipping successful rows on retry, downloadable CSV results, and formula-injection escaping.
- **Notifications and audit:** A transactional outbox, daily checks for low stock, expiring batches, and overdue tasks, in-app notifications, idempotent delivery, processing leases, retries after 1/5/15/60/240 minutes, requeuing of DEAD events, append-only audit records, and request IDs.
- **Simulated samples:** A test catalog, laboratory orders, barcode registration, receipt, processing, completion, and rejection, with status events and project-level authorization. Samples do not affect reagent inventory.
- **Material costs:** Net material costs and remaining budgets derived from issue and reversal ledger entries, with date-range queries. Project and item filters are also available through the reporting API.

While `process_events --loop` is running, the worker consumes events, executes imports, checks daily alerts, and creates a daily consistent SQLite backup. If the worker is stopped, imports remain in the running state and notification events stay queued; the application does not report them as successfully completed.

## Walk through the demo

1. Open a pending request submitted by the purchasing officer from the administrator dashboard, enter an approval comment, and approve it.
2. As the purchasing officer, create an order from approved request lines, enter quantities and unit prices, and confirm the order.
3. As the warehouse operator, record a receipt with its warehouse, quantity, internal batch number, and expiry date. Saving creates a draft; stock increases only after selecting **Post receipt**.
4. As a technician or project manager, create a material issue request for an in-progress task. The warehouse operator posts it from the inventory document details.
5. An administrator can reverse the original issue. Stock is restored, the original ledger entries remain, and the project's net material cost decreases accordingly.

The fixed seed includes the PRD's numerical example: an order for 100 KIT first receives 60 into batch `B-001`; 10 are issued to a task and 5 are transferred to the laboratory supplies warehouse. The central warehouse then holds 45 and the destination holds 5, for a total of 50 and a project material cost of $120. Reversing the issue restores total stock to 60 and net material cost to $0. Later browser-validation records use `QA-` or `SIM-` codes so they can be distinguished from the workflow demo records.

## Project structure

```text
config/                         Django configuration and routing
labops/models.py                Relational models, foreign keys, unique/check constraints
labops/fields.py                 Fixed-point storage with six decimal places
labops/common.py                 Authorization, transactions, versions, errors, idempotency, audit
labops/catalog/services.py       User and master-data rules
labops/projects/services.py      Projects, tasks, members, and comments
labops/purchasing/services.py    Requests, approvals, orders, and receipt drafts
labops/inventory/services.py     Centralized inventory posting and reversals
labops/operations/services.py    CSV imports, outbox, notifications, and background jobs
labops/samples/services.py       Simulated laboratory orders and sample states
labops/queries.py                Page queries and derived material costs
labops/api.py                    JSON API and authentication pages
labops/migrations/               Database migrations
labops/templates/                Page templates and login screen
labops/static/                   Interface logic, forms, and styles
labops/tests/                    Repeatable acceptance tests
labops/management/commands/      Seeding, workers, backups, and ledger reconciliation
```

The application reuses Django's User, Group, and user-group relationships. Business records use UUID primary keys. Additional tables store idempotent command results, runtime state, and login rate-limit data. Business models are declared in one Django app, while business rules are separated into module services.

## API

All business endpoints are under `/api/v1/`. Authentication uses Django session cookies. Writes require `X-CSRFToken`; creation and inventory-posting commands use `Idempotency-Key`. Updates to editable records require `expected_version`.

Successful responses contain `data` and `request_id`; list responses also include `pagination`. Error responses contain `error.code`, `message`, `field_errors`, and `request_id`. The default page size is 20, with a maximum of 100.

- `/items`, `/suppliers`, `/warehouses`: GET/POST; `/{id}`: GET/PATCH.
- `/projects`, `/tasks`: GET/POST; `/{id}`: GET/PATCH; `/{id}/transition`: POST.
- `/projects/{id}/members`, `/tasks/{id}/comments`: POST.
- `/purchase-requests`: GET/POST; `/{id}`: GET/PATCH; `/{id}/submit|decision|withdraw|cancel`: POST.
- `/purchase-orders`: GET/POST; `/{id}/confirm|cancel`: POST.
- `/receipts`: GET/POST; `/{id}/post`: POST.
- `/stock/issues/drafts`, `/stock/issues`, `/stock/transfers`, `/stock/adjustments`, `/stock/opening`: POST.
- `/stock/movements/{id}/reverse`: POST.
- `/inventory`, `/balances`, `/movements`, `/stock/reconcile`: GET.
- `/import-jobs`: GET/POST multipart; `/{id}/execute`: POST, returning 202; poll `/{id}` for status; download results from `/{id}/errors`.
- `/notifications`, `/audit`, `/events`: GET; `/notifications/{id}/read`, `/events/{id}/retry`: POST.
- `/lab-orders`, `/samples`: GET/POST; `/{id}/transition`: POST; `/test-catalog`: GET.
- `/reports?project_id=UUID&item_id=UUID&from_date=YYYY-MM-DD&to_date=YYYY-MM-DD`: GET.

## Validation and data integrity

```bash
LABOPS_TEST_DB=/private/tmp/labops-acceptance.sqlite3 .venv/bin/python manage.py test labops.tests --noinput -v 2
.venv/bin/python manage.py check
.venv/bin/python manage.py reconcile_stock
```

Concurrency acceptance tests use a file-backed database with independent threads and connections. SQLite's default `:memory:` test database has different locking behavior, so use the separate test database path shown above. Do not point it at the application database: the tests create and destroy the specified test database.

Quantities and unit costs are stored as integer micro-units through `Fixed6Field` and calculated with Python `Decimal`, avoiding SQLite's conversion of DECIMAL values to floating point. The quantity range corresponds to NUMERIC(18,6). Project budgets use two decimal places, and monetary totals are displayed in USD to two decimal places.

Local write transactions use SQLite `BEGIN IMMEDIATE`. Under the PostgreSQL configuration, a lock on the runtime-state row serializes business commands. This prevents inventory operations with multiple lines, purchase allocations, and task closure from interleaving their commits. This learning implementation prioritizes consistency; it has not demonstrated the PRD's p95 < 1 second target at 1,000 items, 10,000 batches, 100,000 ledger entries, and 20 concurrent sessions. See the [Django database documentation](https://docs.djangoproject.com/en/5.2/ref/databases/#transactions-behavior) for SQLite transaction modes and limitations.

## Backup and recovery

The running background worker creates one consistent SQLite snapshot per day in `backups/`. To create a manual snapshot, choose a destination that does not already exist:

```bash
.venv/bin/python manage.py backup_database --output backups/manual.sqlite3
```

Stop the web server and worker, preserve the original database, copy a verified snapshot to another path, and point `LABOPS_DB` at it:

```bash
cp backups/manual.sqlite3 restored.sqlite3
LABOPS_DB="$PWD/restored.sqlite3" .venv/bin/python manage.py reconcile_stock
LABOPS_DB="$PWD/restored.sqlite3" ./run.sh
```

Snapshots do not include the application secret. Preserve `.local-secret` or the deployment's `LABOPS_SECRET_KEY`. A backup was created, reopened, and checked against the inventory ledger during validation; see [VALIDATION.md](VALIDATION.md) for the recorded evidence.

## Scope and limitations

This is a runnable local learning application. It has not been deployed publicly or passed production acceptance. It does not include real patient data, instrument integration, supplier settlement, a general ledger, taxation, multiple tenants, multiple currencies, unit conversions, or inventory reservations.

Local sessions, password hashing, server-side authorization, CSRF protection, input validation, and version checks are implemented. Password-recovery email and invitation email delivery are not configured; administrators can currently create accounts and reset passwords. The test catalog is initialized by the seed, with no catalog-management interface yet. Order and receipt drafts can be saved and processed, but interactive editing and deletion controls are not available for every draft line.

PostgreSQL connection configuration is provided, but validation was not run against PostgreSQL. To use it, set `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, and `POSTGRES_PORT`, then run migrations and tests against an isolated database first. Production deployment still requires an application server, HTTPS, a separate secret, real-account initialization, process supervision, PostgreSQL backups, and performance validation. `runserver` and the demo password are for local development only.
