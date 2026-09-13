# LabOps Laboratory Operations ERP

[English](#english) | [简体中文](#简体中文)

## English

LabOps is a standalone web ERP learning project based on `LabOps_Requirements_ERD_v1.docx`. It provides a Chinese-language interface, server-side authorization, persistent database storage, and fictional demo data. The application is a modular monolith built with Django 5.2.17. It uses SQLite locally and supports PostgreSQL configuration. It does not depend on ERPNext or connect to SpectraCell or any real laboratory system.

### Getting started

After cloning the repository, run the following commands. If you already have a local copy, open its project directory and run `./run.sh`.

```bash
cd labops-erp
./run.sh
```

On macOS, you can also double-click `start.command`. Open [LabOps](http://127.0.0.1:8765/). The script applies database migrations, seeds demo data only when the database is empty, and starts both the web server and background worker. Seeding does not overwrite existing data. Close the terminal or press Ctrl+C to stop the services.

A new machine needs Python 3.12 or later. On its first run, the script creates a virtual environment and installs the pinned dependencies. The original development environment used Python 3.14. The frontend uses plain JavaScript, HTML, and CSS; Node.js, npm, and a frontend build step are not required.

### Demo accounts

All accounts below use `LabOpsDemo!2026`. This password is intended only for local use with fictional data.

| Email | Display name | Role |
| --- | --- | --- |
| admin@labops.local | 林知远 | Administrator |
| reviewer@labops.local | 陈思宁 | Second administrator; can approve another user's requests |
| manager@labops.local | 周亦然 | Project manager |
| buyer@labops.local | 许安 | Purchasing officer |
| store@labops.local | 陆川 | Warehouse operator |
| tech@labops.local | 苏禾 | Laboratory technician |
| auditor@labops.local | 沈清 | Auditor; read-only access |

To switch accounts, sign out at the bottom of the sidebar. The server checks whether the account is active before every write. Administrators cannot approve their own purchase requests.

### Implemented workflows

- **Users and master data:** Django password hashing and sessions, CSRF protection, login rate limiting, role assignment, and account deactivation. Items, suppliers, and warehouses support creation, editing, deactivation, search, pagination, code normalization, and transaction-reference constraints.
- **Projects and tasks:** Project owners and members, a task board, status transitions, assignee validation, blocking reasons, comments, material issue history, optimistic version checks, and authorization scoped to project membership.
- **Purchasing:** Requests with multiple lines, withdrawal and resubmission, approval by another user, allocation of approved request lines across purchase orders, order confirmation and cancellation, partial receipts, a separate batch cost layer for each receipt, prevention of over-allocation and over-receipt, automatic order closure when fully received, and reopening after a receipt reversal.
- **Inventory:** Stock overview, unexpired quantities available for issue, batch and expiry tracking, draft material issue requests, atomic document posting, warehouse transfers, opening stock, count adjustments, line-by-line reversals, idempotency keys, and balance reconciliation. Posted ledger entries are not edited directly.
- **Imports:** UTF-8 CSV files up to 5 MB and 5,000 rows, downloadable templates, optional column mapping, explicit CREATE/UPDATE modes, row-level preflight validation, duplicate-code checks, execution-time version checks, background processing and status polling, skipping successful rows on retry, downloadable CSV results, and formula-injection escaping.
- **Notifications and audit:** A transactional outbox, daily checks for low stock, expiring batches, and overdue tasks, in-app notifications, idempotent delivery, processing leases, retries after 1/5/15/60/240 minutes, requeuing of DEAD events, append-only audit records, and request IDs.
- **Simulated samples:** A test catalog, laboratory orders, barcode registration, receipt, processing, completion, and rejection, with status events and project-level authorization. Samples do not affect reagent inventory.
- **Material costs:** Net material costs and remaining budgets derived from issue and reversal ledger entries, with date-range queries. Project and item filters are also available through the reporting API.

While `process_events --loop` is running, the worker consumes events, executes imports, checks daily alerts, and creates a daily consistent SQLite backup. If the worker is stopped, imports remain in the running state and notification events stay queued; the application does not report them as successfully completed.

### Walk through the demo

1. Open a pending request submitted by the purchasing officer from the administrator dashboard, enter an approval comment, and approve it.
2. As the purchasing officer, create an order from approved request lines, enter quantities and unit prices, and confirm the order.
3. As the warehouse operator, record a receipt with its warehouse, quantity, internal batch number, and expiry date. Saving creates a draft; stock increases only after selecting “确认过账入库” (confirm receipt posting).
4. As a technician or project manager, create a material issue request for an in-progress task. The warehouse operator posts it from the inventory document details.
5. An administrator can reverse the original issue. Stock is restored, the original ledger entries remain, and the project's net material cost decreases accordingly.

The fixed seed includes the PRD's numerical example: an order for 100 KIT first receives 60 into batch `B-001`; 10 are issued to a task and 5 are transferred to the laboratory supplies warehouse. The central warehouse then holds 45 and the destination holds 5, for a total of 50 and a project material cost of $120. Reversing the issue restores total stock to 60 and net material cost to $0. Later browser-validation records use `QA-` or `SIM-` codes so they can be distinguished from the workflow demo records.

### Project structure

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

### API

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

### Validation and data integrity

```bash
LABOPS_TEST_DB=/private/tmp/labops-acceptance.sqlite3 .venv/bin/python manage.py test labops.tests --noinput -v 2
.venv/bin/python manage.py check
.venv/bin/python manage.py reconcile_stock
```

Concurrency acceptance tests use a file-backed database with independent threads and connections. SQLite's default `:memory:` test database has different locking behavior, so use the separate test database path shown above. Do not point it at the application database: the tests create and destroy the specified test database.

Quantities and unit costs are stored as integer micro-units through `Fixed6Field` and calculated with Python `Decimal`, avoiding SQLite's conversion of DECIMAL values to floating point. The quantity range corresponds to NUMERIC(18,6). Project budgets use two decimal places, and monetary totals are displayed in USD to two decimal places.

Local write transactions use SQLite `BEGIN IMMEDIATE`. Under the PostgreSQL configuration, a lock on the runtime-state row serializes business commands. This prevents inventory operations with multiple lines, purchase allocations, and task closure from interleaving their commits. This learning implementation prioritizes consistency; it has not demonstrated the PRD's p95 < 1 second target at 1,000 items, 10,000 batches, 100,000 ledger entries, and 20 concurrent sessions. See the [Django database documentation](https://docs.djangoproject.com/en/5.2/ref/databases/#transactions-behavior) for SQLite transaction modes and limitations.

### Backup and recovery

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

### Scope and limitations

This is a runnable local learning application. It has not been deployed publicly or passed production acceptance. It does not include real patient data, instrument integration, supplier settlement, a general ledger, taxation, multiple tenants, multiple currencies, unit conversions, or inventory reservations.

Local sessions, password hashing, server-side authorization, CSRF protection, input validation, and version checks are implemented. Password-recovery email and invitation email delivery are not configured; administrators can currently create accounts and reset passwords. The test catalog is initialized by the seed, with no catalog-management interface yet. Order and receipt drafts can be saved and processed, but interactive editing and deletion controls are not available for every draft line.

PostgreSQL connection configuration is provided, but validation was not run against PostgreSQL. To use it, set `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, and `POSTGRES_PORT`, then run migrations and tests against an isolated database first. Production deployment still requires an application server, HTTPS, a separate secret, real-account initialization, process supervision, PostgreSQL backups, and performance validation. `runserver` and the demo password are for local development only.

---

## 简体中文

根据 `LabOps_Requirements_ERD_v1.docx` 实现的独立 Web ERP 学习项目。中文界面，真实服务端权限与数据库持久化，虚构演示数据。采用 Django 5.2.17 模块化单体；本地默认 SQLite，可配置 PostgreSQL。没有依赖 ERPNext，也不会连接 SpectraCell 或真实实验室系统。

### 运行

克隆仓库后执行（已有本地项目可直接进入项目目录运行 `./run.sh`）：

```bash
cd labops-erp
./run.sh
```

也可以双击 `start.command`。访问 http://127.0.0.1:8765/ 。脚本会迁移数据库、仅在空数据库中创建演示数据，同时启动 Web 服务与后台作业。已有数据不会被 seed 覆盖。关闭终端或按 Ctrl+C 停止服务。

新机器需要 Python 3.12 及以上。脚本首次运行会创建虚拟环境并安装锁定依赖。本工作区使用 Python 3.14。前端使用原生 JavaScript、HTML 和 CSS，无需 Node、npm 或前端构建。

### 演示账号

以下密码仅用于本地虚构数据：`LabOpsDemo!2026`。

| 邮箱 | 姓名 | 角色 |
| --- | --- | --- |
| admin@labops.local | 林知远 | 管理员 |
| reviewer@labops.local | 陈思宁 | 第二位管理员，可审批他人申请 |
| manager@labops.local | 周亦然 | 项目经理 |
| buyer@labops.local | 许安 | 采购员 |
| store@labops.local | 陆川 | 库管 |
| tech@labops.local | 苏禾 | 实验员 |
| auditor@labops.local | 沈清 | 审计员，只读 |

通过侧栏底部退出登录后切换账号。后台每次写入重新检查账号是否有效。管理员也不能审批自己的采购申请。

### 已实现业务

- 用户与主数据：Django 密码哈希和会话、CSRF、登录限速、用户角色与停用；物料、供应商、仓库的新增、编辑、停用、搜索、分页、编码归一化和交易引用约束。
- 项目与任务：项目负责人和成员、任务看板、状态流转、负责人约束、阻塞原因、任务评论、领料历史、乐观版本锁和项目范围权限。
- 采购：多行申请、撤回和重提、他人审批、从已批准行拆分采购订单、确认和取消、分批收货、每次收货独立成本批次、禁止超额分配和超收、订单自动收齐和冲销后重新打开。
- 库存：库存总览、未过期可领量、批次和有效期、领料需求草稿、整单过账、调拨、期初录入、盘点差额、逐行反向冲销、幂等键和库存余额核对。过账后不直接修改流水。
- 导入：UTF-8 CSV，5 MB/5000 行限制，模板下载，可选列映射，CREATE/UPDATE，逐行预检，重复编码检查，执行时版本检查，后台执行与状态轮询，成功行跳过，失败结果 CSV 下载及公式注入转义。
- 通知与审计：事务 outbox、每日低库存/临期/逾期检查、站内通知、幂等投递、租约、1/5/15/60/240 分钟重试、DEAD 事件重新排队、只追加审计及请求编号。
- 模拟样本：检测目录、检测单、条码登记、接收、处理、完成和拒收，状态事件追踪与项目权限，不参与试剂库存计算。
- 耗材成本：从领料与冲销流水计算净耗材成本、预算余额，可按日期范围查询。项目和物料筛选也可通过报表 API 查询参数使用。

后台作业在 `process_events --loop` 运行期间消费事件、执行导入、每天检查告警并为 SQLite 创建一次一致性备份。未启动后台作业时，导入会保留在“执行中”，通知事件会留在队列中；不会伪造执行成功。

### 从现有演示数据体验闭环

1. 管理员工作台有采购员提交的待审申请；打开详情，填写意见后批准。
2. 采购员从已批准申请创建订单，录入数量和单价，再确认订单。
3. 库管登记收货，填写仓库、数量、内部批次和有效期。保存的是草稿，点击“确认过账入库”才增加库存。
4. 实验员/项目经理创建进行中任务的领料需求；库管在库存单据详情中确认过账。
5. 管理员可以冲销原始领料，库存自动恢复，原流水保留，项目净耗材成本回退。

固定 seed 已包含 PRD 的数值例子：100 KIT 订单先收 60，批次 `B-001`；领用 10，向实验室耗材仓调拨 5。中央试剂仓为 45、目标仓为 5，总量 50，项目耗材成本 $120。冲销这笔领料后总量回到 60、净耗材成本为 $0。后续浏览器验证记录使用 `QA-` 或 `SIM-` 编码，和业务演示记录可区分。

### 代码结构

```text
config/                         Django 配置与路由
labops/models.py                关系模型、外键、唯一与检查约束
labops/fields.py                六位小数定点数存储
labops/common.py                权限、事务、版本、错误、幂等与审计
labops/catalog/services.py      用户和主数据规则
labops/projects/services.py     项目、任务、成员和评论
labops/purchasing/services.py   申请、审批、订单、收货草稿
labops/inventory/services.py    统一库存过账与冲销
labops/operations/services.py   CSV、outbox、通知和后台作业
labops/samples/services.py      模拟检测单与样本状态
labops/queries.py               页面查询与耗材成本派生
labops/api.py                   JSON API 和认证页面
labops/migrations/              物理数据库迁移
labops/templates/              页面骨架和登录页
labops/static/                 界面、表单和样式
labops/tests/                  可重复的验收测试
labops/management/commands/    seed、后台作业、备份、台账核对
```

Django 的 User、Group 和用户组关联复用框架权限实体。业务主键使用 UUID。实现额外维护命令幂等结果、运行状态与登录限速表。业务模型目前集中声明在一个 Django app，业务规则按模块服务划分。

### API

全部业务 API 位于 `/api/v1/`。使用 Django 会话 Cookie；写入必须携带 `X-CSRFToken`。创建和库存过账命令携带 `Idempotency-Key`。所有可编辑记录更新携带 `expected_version`。

成功返回 `data` 和 `request_id`；列表另返回 `pagination`。错误返回 `error.code`、`message`、`field_errors` 与 `request_id`。默认分页 20，最大 100。

- `/items`、`/suppliers`、`/warehouses`：GET/POST；`/{id}` GET/PATCH。
- `/projects`、`/tasks`：GET/POST；`/{id}` GET/PATCH；`/{id}/transition` POST。
- `/projects/{id}/members`、`/tasks/{id}/comments`：POST。
- `/purchase-requests`：GET/POST；`/{id}` GET/PATCH；`/{id}/submit|decision|withdraw|cancel` POST。
- `/purchase-orders`：GET/POST；`/{id}/confirm|cancel` POST。
- `/receipts`：GET/POST；`/{id}/post` POST。
- `/stock/issues/drafts`、`/stock/issues`、`/stock/transfers`、`/stock/adjustments`、`/stock/opening`：POST。
- `/stock/movements/{id}/reverse`：POST。
- `/inventory`、`/balances`、`/movements`、`/stock/reconcile`：GET。
- `/import-jobs`：GET/POST multipart；`/{id}/execute` POST 返回 202；`/{id}` 轮询；`/{id}/errors` 下载结果。
- `/notifications`、`/audit`、`/events`：GET；`/notifications/{id}/read`、`/events/{id}/retry`：POST。
- `/lab-orders`、`/samples`：GET/POST；`/{id}/transition`：POST；`/test-catalog`：GET。
- `/reports?project_id=UUID&item_id=UUID&from_date=YYYY-MM-DD&to_date=YYYY-MM-DD`：GET。

### 验证与数据库完整性

```bash
LABOPS_TEST_DB=/private/tmp/labops-acceptance.sqlite3 .venv/bin/python manage.py test labops.tests --noinput -v 2
.venv/bin/python manage.py check
.venv/bin/python manage.py reconcile_stock
```

并发验收使用文件数据库及独立线程/连接。默认 SQLite `:memory:` 测试数据库的锁语义不同，运行并发测试时请使用上方独立测试数据库路径，不要指定业务数据库路径。测试会创建并销毁该测试数据库。

数量和单位成本通过 `Fixed6Field` 用整数微单位存储，Python `Decimal` 计算；避免 SQLite 把 DECIMAL 变成浮点。数量范围对应 NUMERIC(18,6)。项目预算采用两位小数。金额展示按 USD 两位小数。

本地写事务使用 SQLite `BEGIN IMMEDIATE`；PostgreSQL 配置下使用运行状态单行锁串行化业务命令。多行库存、采购分配和任务关闭因此不能交错提交。该实现优先保证学习版本的一致性，尚未以 PRD 的 1000 物料/10000 批次/100000 流水及 20 会话规模证明 p95 < 1 秒。SQLite 的事务模式与限制参考 [Django 数据库文档](https://docs.djangoproject.com/en/5.2/ref/databases/#transactions-behavior)。

### 备份与恢复

运行中的后台作业每天在 `backups/` 创建一次 SQLite 一致性快照。可手动创建到一个尚不存在的位置：

```bash
.venv/bin/python manage.py backup_database --output backups/manual.sqlite3
```

先停止 Web 和后台进程，保留原数据库，再将验证过的快照复制到另一个路径，并通过 `LABOPS_DB` 指向它：

```bash
cp backups/manual.sqlite3 restored.sqlite3
LABOPS_DB="$PWD/restored.sqlite3" .venv/bin/python manage.py reconcile_stock
LABOPS_DB="$PWD/restored.sqlite3" ./run.sh
```

快照不包含应用密钥；保留 `.local-secret` 或部署环境中的 `LABOPS_SECRET_KEY`。已实际创建备份、重开备份数据库并核对库存流水，详细证据见 `VALIDATION.md`。

### 实施边界

这是可运行的本地学习版本，尚未部署公网或通过生产验收。没有真实患者数据、仪器接入、结算、总账、税务、多租户、多币种、单位换算或库存预留。

本地会话、密码哈希、后台权限、CSRF、输入与版本检查已实现；找回密码的邮件服务和正式邀请邮件尚未配置。当前提供管理员创建账号和重置密码。测试目录由 seed 初始化，尚无目录管理界面。订单/收货草稿可保存和继续执行，但尚未提供所有草稿行的交互式编辑或删除入口。

PostgreSQL 连接配置已提供，但本次未在 PostgreSQL 上运行测试。使用 PostgreSQL 时设置 `POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD`、`POSTGRES_HOST`、`POSTGRES_PORT`，并先在隔离数据库运行迁移和测试。正式部署需接入应用服务器、HTTPS、独立密钥、真实账号初始化、进程管理、PostgreSQL 备份与性能验证；`runserver` 和演示密码仅用于本地开发。
