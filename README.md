[English](#english) | [简体中文](#简体中文)

<a id="english"></a>

# English

# LabOps Laboratory Operations ERP

LabOps is a standalone web ERP learning project based on `LabOps_Requirements_ERD_v1.docx`. It provides an English-language interface, server-side authorization, persistent database storage, and fictional demo data. The application is a modular monolith built with Django 5.2.17. It uses SQLite locally and supports PostgreSQL configuration. It does not depend on ERPNext or connect to SpectraCell or any real laboratory system.

The interface, validation messages, generated notifications, documentation, and newly seeded demo data are in English. This README also provides a Chinese translation. Dates and numbers use US English formatting; times remain in `America/New_York` and amounts remain in USD. Existing database content is not overwritten by seeding, and historical audit/ledger text is retained verbatim.

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

---

<a id="简体中文"></a>

# 简体中文

# LabOps 实验室运营 ERP

LabOps 是基于 `LabOps_Requirements_ERD_v1.docx` 的独立 Web ERP 学习项目，提供英文界面、服务端授权、持久化数据库存储和虚构演示数据。应用采用 Django 5.2.17 模块化单体架构，本地使用 SQLite，并支持 PostgreSQL 配置。它不依赖 ERPNext，也不连接 SpectraCell 或任何真实实验室系统。

界面、校验消息、生成的通知、应用文档和新初始化的演示数据使用英文。日期和数字采用美式英文格式；时区保持 `America/New_York`，金额保持 USD。初始化不会覆盖已有数据库内容，历史审计和流水文本原样保留。本 README 另提供中英双语说明。

## 快速开始

克隆仓库后执行以下命令。如果已有本地副本，进入项目目录运行 `./run.sh` 即可。

```bash
cd labops-erp
./run.sh
```

在 macOS 上，也可双击 `start.command`。打开 [LabOps](http://127.0.0.1:8765/)。脚本会执行数据库迁移，仅在数据库为空时初始化演示数据，并同时启动 Web 服务器和后台工作进程。初始化不覆盖已有数据。关闭终端或按 Ctrl+C 停止服务。

新机器需要 Python 3.12 或更高版本。首次运行时，脚本创建虚拟环境并安装锁定版本的依赖。原开发环境使用 Python 3.14。前端使用原生 JavaScript、HTML 和 CSS，无需 Node.js、npm 或前端构建步骤。

## 演示账号

以下账号的密码均为 `LabOpsDemo!2026`，仅供本地虚构数据演示使用。

| 邮箱 | 显示名称 | 角色 |
| --- | --- | --- |
| admin@labops.local | Lin Zhiyuan | 管理员 |
| reviewer@labops.local | Chen Sining | 第二位管理员，可审批他人申请 |
| manager@labops.local | Zhou Yiran | 项目经理 |
| buyer@labops.local | Xu An | 采购员 |
| store@labops.local | Lu Chuan | 仓库操作员 |
| tech@labops.local | Su He | 实验室技术员 |
| auditor@labops.local | Shen Qing | 审计员，只读权限 |

可通过侧栏底部退出登录后切换账号。服务器在每次写操作前检查账号是否有效。管理员不能审批自己提交的采购申请。

## 已实现的业务流程

- **用户与主数据：** Django 密码哈希与会话、CSRF 防护、登录限流、角色分配和账号停用。物料、供应商和仓库支持创建、编辑、停用、搜索、分页、编码规范化及交易引用约束。
- **项目与任务：** 项目负责人和成员、任务看板、状态流转、负责人校验、阻塞原因、评论、领料历史、乐观版本检查，以及按项目成员身份限定的授权。
- **采购：** 多行采购申请、撤回与重新提交、他人审批、已批准申请行在采购订单间的分配、订单确认与取消、部分收货、每次收货独立的批次成本层、防止超额分配和超额收货、全部收货后自动关闭订单，以及收货冲销后重新打开订单。
- **库存：** 库存概览、未过期可领数量、批次与有效期跟踪、领料申请草稿、原子化单据过账、仓库调拨、期初库存、盘点调整、逐行冲销、幂等键和余额核对。已过账流水不可直接修改。
- **导入：** 最大 5 MB、5,000 行的 UTF-8 CSV 文件，可下载模板、可选列映射、明确的 CREATE/UPDATE 模式、逐行预检、重复编码检查、执行时版本检查、后台处理与状态轮询、重试时跳过成功行、可下载 CSV 结果和公式注入转义。
- **通知与审计：** 事务性 outbox、每日低库存/批次临期/任务逾期检查、站内通知、幂等投递、处理租约、1/5/15/60/240 分钟后的重试、DEAD 事件重新入队、只追加审计记录和请求 ID。
- **模拟样本：** 检测目录、实验室订单、条码登记、接收、处理、完成与拒收，并记录状态事件、实施项目级授权。样本不影响试剂库存。
- **物料成本：** 根据领料与冲销流水计算净物料成本和剩余预算，支持日期范围查询；报表 API 还支持项目和物料筛选。

运行 `process_events --loop` 时，后台工作进程消费事件、执行导入、检查每日提醒，并每日创建一致的 SQLite 备份。如果停止工作进程，导入会保持运行中状态，通知事件继续排队；应用不会将其报告为已成功完成。

## 演示操作流程

1. 在管理员仪表盘打开采购员提交的待审批申请，填写审批意见并批准。
2. 以采购员身份，根据已批准申请行创建订单，输入数量和单价，然后确认订单。
3. 以仓库操作员身份，填写收货仓库、数量、内部批次号和有效期。保存只创建草稿；选择 **Post receipt** 后库存才会增加。
4. 以技术员或项目经理身份，为进行中的任务创建领料申请。仓库操作员在库存单据详情中过账。
5. 管理员可以冲销原领料记录。库存恢复，原流水保留，项目净物料成本相应减少。

固定种子数据包含 PRD 中的数值示例：采购 100 KIT，先收货 60 到批次 `B-001`；其中 10 领用到任务，5 调拨到实验室耗材仓。此时中心仓剩 45、目标仓为 5，总库存 50，项目物料成本为 $120。冲销领料后，总库存恢复到 60，净物料成本为 $0。后续浏览器验证记录使用 `QA-` 或 `SIM-` 编码，以区别于业务演示记录。

## 项目结构

```text
config/                         Django 配置与路由
labops/models.py                关系模型、外键、唯一约束与检查约束
labops/fields.py                 六位小数的定点存储
labops/common.py                 授权、事务、版本、错误、幂等与审计
labops/catalog/services.py       用户与主数据规则
labops/projects/services.py      项目、任务、成员与评论
labops/purchasing/services.py    申请、审批、订单与收货草稿
labops/inventory/services.py     集中的库存过账与冲销
labops/operations/services.py    CSV 导入、outbox、通知与后台任务
labops/samples/services.py       模拟实验室订单与样本状态
labops/queries.py                页面查询与派生物料成本
labops/api.py                    JSON API 与认证页面
labops/migrations/               数据库迁移
labops/templates/                页面模板与登录页
labops/static/                   界面逻辑、表单与样式
labops/tests/                    可重复执行的验收测试
labops/management/commands/      初始化、工作进程、备份与流水核对
```

应用复用 Django 的 User、Group 和用户组关系。业务记录使用 UUID 主键。附加表存储幂等命令结果、运行状态和登录限流数据。业务模型定义在同一个 Django app 中，业务规则则拆分到各模块服务。

## API

所有业务接口位于 `/api/v1/` 下，使用 Django 会话 Cookie 认证。写操作需要 `X-CSRFToken`；创建与库存过账命令使用 `Idempotency-Key`。更新可编辑记录时需要 `expected_version`。

成功响应包含 `data` 和 `request_id`；列表响应还包含 `pagination`。错误响应包含 `error.code`、`message`、`field_errors` 和 `request_id`。默认每页 20 条，最多 100 条。

- `/items`、`/suppliers`、`/warehouses`：GET/POST；`/{id}`：GET/PATCH。
- `/projects`、`/tasks`：GET/POST；`/{id}`：GET/PATCH；`/{id}/transition`：POST。
- `/projects/{id}/members`、`/tasks/{id}/comments`：POST。
- `/purchase-requests`：GET/POST；`/{id}`：GET/PATCH；`/{id}/submit|decision|withdraw|cancel`：POST。
- `/purchase-orders`：GET/POST；`/{id}/confirm|cancel`：POST。
- `/receipts`：GET/POST；`/{id}/post`：POST。
- `/stock/issues/drafts`、`/stock/issues`、`/stock/transfers`、`/stock/adjustments`、`/stock/opening`：POST。
- `/stock/movements/{id}/reverse`：POST。
- `/inventory`、`/balances`、`/movements`、`/stock/reconcile`：GET。
- `/import-jobs`：GET/POST multipart；`/{id}/execute`：POST，返回 202；轮询 `/{id}` 查询状态，从 `/{id}/errors` 下载结果。
- `/notifications`、`/audit`、`/events`：GET；`/notifications/{id}/read`、`/events/{id}/retry`：POST。
- `/lab-orders`、`/samples`：GET/POST；`/{id}/transition`：POST；`/test-catalog`：GET。
- `/reports?project_id=UUID&item_id=UUID&from_date=YYYY-MM-DD&to_date=YYYY-MM-DD`：GET。

## 验证与数据完整性

```bash
LABOPS_TEST_DB=/private/tmp/labops-acceptance.sqlite3 .venv/bin/python manage.py test labops.tests --noinput -v 2
.venv/bin/python manage.py check
.venv/bin/python manage.py reconcile_stock
```

并发验收测试使用文件数据库、独立线程和连接。SQLite 默认的 `:memory:` 测试数据库具有不同的锁行为，因此请使用上述独立测试数据库路径。不要指向应用数据库：测试会创建并销毁指定的测试数据库。

数量与单价通过 `Fixed6Field` 以整数微单位存储，使用 Python `Decimal` 计算，避免 SQLite 将 DECIMAL 值转为浮点数。数量范围对应 NUMERIC(18,6)。项目预算保留两位小数，金额以 USD 显示并保留两位小数。

本地写事务使用 SQLite `BEGIN IMMEDIATE`。在 PostgreSQL 配置下，通过锁定运行状态行串行业务命令，防止多行库存操作、采购分配和任务关闭交错提交。该学习实现优先保证一致性；尚未证明在 1,000 个物料、10,000 个批次、100,000 条流水和 20 个并发会话下达到 PRD 的 p95 < 1 秒目标。SQLite 事务模式与限制参见 [Django 数据库文档](https://docs.djangoproject.com/en/5.2/ref/databases/#transactions-behavior)。

## 备份与恢复

运行中的后台工作进程每天在 `backups/` 中创建一个一致的 SQLite 快照。手动备份时，请选择尚不存在的目标路径：

```bash
.venv/bin/python manage.py backup_database --output backups/manual.sqlite3
```

停止 Web 服务器和工作进程，保留原数据库，将已验证快照复制到另一位置，再通过 `LABOPS_DB` 指向它：

```bash
cp backups/manual.sqlite3 restored.sqlite3
LABOPS_DB="$PWD/restored.sqlite3" .venv/bin/python manage.py reconcile_stock
LABOPS_DB="$PWD/restored.sqlite3" ./run.sh
```

快照不包含应用密钥。请保留 `.local-secret` 或部署环境的 `LABOPS_SECRET_KEY`。此前验证已创建、重新打开备份，并与库存流水核对；记录见 [VALIDATION.md](VALIDATION.md)。

## 范围与限制

这是可在本地运行的学习应用，尚未公开部署或通过生产验收。不包含真实患者数据、仪器集成、供应商结算、总账、税务、多租户、多币种、单位换算或库存预留。

已实现本地会话、密码哈希、服务端授权、CSRF 防护、输入校验和版本检查。未配置密码找回邮件和邀请邮件发送；管理员目前可以创建账号及重置密码。检测目录由种子数据初始化，尚无目录管理界面。订单与收货草稿可保存和处理，但并非所有草稿行都提供交互式编辑和删除控件。

项目提供 PostgreSQL 连接配置，但尚未在 PostgreSQL 上执行验证。使用时请设置 `POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD`、`POSTGRES_HOST` 和 `POSTGRES_PORT`，先在隔离数据库中执行迁移和测试。生产部署还需要应用服务器、HTTPS、独立密钥、真实账号初始化、进程管理、PostgreSQL 备份和性能验证。`runserver` 和演示密码仅用于本地开发。
