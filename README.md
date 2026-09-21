[English](#english) | [简体中文](#简体中文)

<a id="english"></a>
# LabOps: Transactional Laboratory Operations

LabOps is a Django modular monolith for laboratory purchasing, inventory, projects, CSV imports, and simulated samples. The backend upgrade makes PostgreSQL the reference development database and adds independently testable concurrency control, a Kafka-compatible transactional outbox, idempotent database consumers, performance experiments, and observability. The interface and generated business content are English. All supplied records are fictional.

## Run locally

Requires Docker with Compose. PostgreSQL 17 is the default; `run.sh` explicitly selects the lightweight SQLite demo.

```bash
cp .env.example .env
# Replace LABOPS_SECRET_KEY with a random value in .env.
docker compose up --build -d
docker compose exec web python manage.py seed_demo
docker compose exec web python manage.py seed_samples
```

Open [LabOps](http://127.0.0.1:8001). Example accounts: `admin@labops.local`, `reviewer@labops.local`, `buyer@labops.local`, `store@labops.local`, and `tech@labops.local`; local demo password: `LabOpsDemo!2026`. Seeds refuse to overwrite existing data. Migration runs once in a dedicated startup service.

For events, set `LABOPS_EVENT_TRANSPORT=kafka` in `.env`, then:

```bash
docker compose --profile events up --build -d
```

Optional Redis and observability profiles add rate limiting, catalog caching, Prometheus/Grafana and OTel/Jaeger. See [operations](docs/operations.md) for the required environment switches, startup, recovery, and ports. For an existing database, back up and follow the analytics [cutover procedure](docs/operations.md#existing-database-upgrade-and-projection-cutover) before starting consumers.

## Business invariants

- A posted inventory document commits its ledger, balances, audit, and inventory event together.
- Per batch/warehouse, the sum of posted movement deltas equals the balance; balances never go negative.
- Allocation cannot exceed approved request quantity; effective receipts cannot exceed ordered quantity.
- Reversal references an original posted movement; each original can be reversed once.
- Reusing a command key with the same request returns the original logical result. Conflicting content is rejected.
- Each `(consumer_name, event_id)` produces at most one committed database effect. Broker delivery remains **at least once**.

Quantities and unit prices use integer micro-units (`Fixed6Field`). PostgreSQL aggregation explicitly scales numeric products to preserve fractional precision and avoid bigint multiplication overflow. SQLite retains Decimal calculation for the demo.

## Architecture and transactions

```text
Browser → Django API → PostgreSQL (business rows + ledger + audit + outbox)
                         ↓
                 leased outbox publisher → Redpanda/Kafka
                                             ├─ notification consumer
                                             └─ analytics consumer
Redis (optional): catalog cache + rate limiting
Django/workers → OpenTelemetry Collector → Jaeger
Prometheus ← HTTP/DB metrics + durable queue state + Kafka exporter → Grafana
```

PostgreSQL uses READ COMMITTED. Commands acquire a shared catalog gate, ordered aggregate row locks, command-key locks, and sorted batch/warehouse balance locks. Master-data/account writes use the exclusive gate so deactivation cannot race business validation. The old exclusive singleton lock is removed from normal business writes. Project-scoped writes deliberately serialize within a project; separate inventory transfers can proceed independently. Deadlocks/serialization failures get bounded retries only at the outer transaction boundary.

Inventory events are inserted in the business transaction. Publishers use leases and `SKIP LOCKED`, publish outside the database transaction, and only mark records after broker acknowledgement. Consumers atomically commit deduplication and database effects before offset acknowledgement. Failed deliveries are durably parked with bounded retries and DLQ publication; manual replay retains event identity. Analytics uses commutative deltas and is never the authority for inventory posting. Existing data is supported by `rebuild_inventory_projection`.

Read the [architecture and consistency design](docs/architecture.md) for lock order, state transitions, ordering limits, and recovery semantics.

## API and workflows

Django session cookies authenticate `/api/v1/` routes. Writes require CSRF; creation/posting requires `Idempotency-Key`; editable records require `expected_version`. Role checks and project membership are enforced server-side. Success includes `data` and `request_id`; failures include a stable error code, message, field errors and request ID.

- Master data: users, items, suppliers, warehouses; inactive-account and transaction-reference constraints.
- Projects/tasks: membership, assignments, legal state transitions, comments, material cost and budgets.
- Purchasing: requests, separate-user approval, order allocation, confirmation/cancellation, partial receipts and reversal reopening.
- Inventory: opening, issue drafts/posting, transfer, count adjustment, expiry checks, reversals and reconciliation.
- Operations: validated resumable CSV imports, durable notifications, daily alerts, append-only application audit.
- Simulated samples: registration, receipt, processing, completion/rejection with scoped authorization; no patient records or instrument integration.

## Validation and benchmarks

```bash
uv venv --python 3.12
uv pip install --python .venv/bin/python -r requirements.txt
export DATABASE_URL=postgresql://labops:labops-local@127.0.0.1:55433/labops
.venv/bin/python manage.py migrate
.venv/bin/python manage.py test labops.tests --noinput
.venv/bin/python manage.py reconcile_stock
```

Tests create/drop a separate test database. Never set `LABOPS_TEST_DB` to application data. PostgreSQL tests exercise independent connections, duplicate commands/receipts, competing approvals, reversals, project/task races, independent progress, connection loss, consumer replay, deferred FK failures, and migration of existing data. SQLite runs the compatibility suite and explicitly skips PostgreSQL-only tests.

[Benchmark report](benchmarks/README.md) separates single-process handler timings from real Gunicorn/k6 load tests. It includes deterministic fixtures, query plans, counts, p50/p95/p99, throughput and error rate. [Validation evidence](VALIDATION.md) distinguishes live PostgreSQL/Redpanda/Redis drills from injected failures and historical SQLite checks. Phase 8 includes full PostgreSQL shutdown with real HTTP recovery and four real consumer SIGKILL/replay cases; scripts and raw results are checked in. CI runs both database modes.

## Observability and recovery

Metrics include HTTP/SQL/transaction latency, idempotency replay, cache outcomes, rate rejection, outbox backlog/age, failed deliveries, and reconciliation differences. Kafka exporter supplies actual consumer-group lag. Trace context is carried from the business command through outbox envelopes into publisher/consumer spans. Grafana provisions dashboards; Jaeger stores traces in memory for this local setup.

Cache failures fall back to PostgreSQL. If Redis is configured but unavailable, login/import/report rate limiting fails closed with 503; core inventory writes remain independent of Redis. Existing DB login protection remains when Redis is disabled.

Use `pg_dump`/`pg_restore` for PostgreSQL backups and validate restored data with reconciliation. `backup_database` remains SQLite-only. CLI tools `outbox_events` and `event_failures` inspect/requeue or resolve failures with a required reason. See [operations](docs/operations.md).

## Scope and limits

This is a locally validated engineering project, not a production-certified ERP. Compose uses one PostgreSQL instance and one broker (replication factor one). It does not demonstrate replicated failover, long-running capacity, production security or durable trace retention. No exactly-once claim applies to external email/API side effects. Read models are eventually consistent; key-scoped caches may be stale for their TTL. Ledger immutability is an application contract, not protection against privileged direct SQL.

No HR/payroll, accounting general ledger, tax, multi-tenancy, supplier settlement, Kubernetes or wholesale microservice split is included. No public deployment is performed by this repository's startup commands.

---
<a id="简体中文"></a>
# LabOps：事务型实验室运营系统

LabOps 保留 Django 模块化单体及现有采购、库存、项目、导入和模拟样本流程。本次升级以 PostgreSQL 为开发与并发验证主路径，加入可靠事件管道、消费者幂等、性能基准和可观测性。界面与生成的业务内容为英文，演示记录均为虚构数据。

## 本地启动

```bash
cp .env.example .env
# 在 .env 中将 LABOPS_SECRET_KEY 换成随机密钥。
docker compose up --build -d
docker compose exec web python manage.py seed_demo
docker compose exec web python manage.py seed_samples
```

打开 [LabOps](http://127.0.0.1:8001)。演示账号包括 `admin@labops.local`、`reviewer@labops.local`、`buyer@labops.local`、`store@labops.local` 和 `tech@labops.local`，密码为 `LabOpsDemo!2026`，仅用于本地演示。初始化不会覆盖已有数据。`run.sh` 明确选择 SQLite 演示模式。

在 `.env` 设置 `LABOPS_EVENT_TRANSPORT=kafka` 后，可使用 `docker compose --profile events up --build -d` 启动 Redpanda、发布器及两个独立消费者。Redis 和监控使用独立可选 profile；环境开关、端口及操作步骤见 [运行手册](docs/operations.md)。升级已有数据库应先备份，再执行库存分析投影重建。

## 核心保证

- 库存流水、余额、审计和事件在同一数据库事务中提交，按批次/仓库核对，余额不允许为负。
- 采购分配不超过批准数量，有效收货不超过订单数量，原流水最多冲销一次。
- 同一幂等键和内容重试返回原逻辑结果；冲突内容被拒绝。
- 消息采用至少一次投递，每个“消费者 + 事件”最多产生一次已提交的数据库副作用。
- PostgreSQL 使用 READ COMMITTED、按业务实体的行锁和有序余额锁；主数据停用通过共享/排他保护与业务校验协调。同项目写入仍会串行，无关调拨可独立推进。
- 库存分析投影最终一致，不参与库存写入授权。对历史库存提供 `rebuild_inventory_projection` 重建与事件检查点。

[设计说明](docs/architecture.md) 记录事务边界、锁顺序、事件顺序限制和恢复行为。[性能报告](benchmarks/README.md) 区分进程内请求测量与真实 HTTP 压测，提供原始结果及执行计划。[验证记录](VALIDATION.md) 区分真实服务故障演练、注入故障和历史测试。

## 使用边界

这是经过本地验证的工程项目。单节点 PostgreSQL/Redpanda 不代表高可用；短时压测不代表长期生产容量；外部邮件/API 不具备跨系统 exactly-once 保证。缓存允许 TTL 范围内的陈旧数据；Redis 不可用时目录查询回源，配置了 Redis 的登录/导入/报表限流返回 503，核心库存写入仍可运行。Jaeger 当前使用内存存储。

PostgreSQL 通过 `pg_dump`/`pg_restore` 备份恢复并进行流水核对；原 `backup_database` 命令仍只用于 SQLite。未新增 HR、工资、总账、多租户或 Kubernetes，也未执行公网部署。
