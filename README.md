# 电表归属数仓服务 (Meter Ownership Warehouse API)

纠正电表挂错产线的历史归属，让下游按**正确产线**取数。FastAPI + PostgreSQL 实现，
支持双实例并发部署，所有一致性保证由数据库的行锁、GiST 排他约束和乐观版本号强制。

## 1. 业务模型与核心规则

- **归属时段 (ownership)**：一块电表在某个左闭右开 `[valid_from, valid_to)` 时段归属一条产线。
  同一电表的时段**互不重叠**（相邻允许，边界为右开），由 PostgreSQL 的
  `EXCLUDE USING gist` 约束在数据库层强制，任何并发写入都无法产生重叠。
- **读数 (reading)**：带唯一编号 `reading_uid`、表号、UTC 时刻、整数 Wh。
  事实表通过**非空外键** `ownership_id` 硬绑定到覆盖其时刻的现行归属段；
  找不到覆盖段则整批拒收（HTTP 422）。
- **修正 (correction)**：客户端凭"所见归属版本" `base_version` 提交表号、左闭右开时段和
  新产线。服务端：
  1. 对该电表行加 `FOR UPDATE` 行锁并校验版本，过期版本返回 409；
  2. 校验修正窗口被现有时段**连续无缺口**覆盖，有缺口返回 422；
  3. **只替换窗口内**的归属，窗口两侧历史以独立段原样保留；
  4. 同一事务内把已入仓读数重新关联到新段，提交前再校验无孤儿事实；
  5. 任何一步失败整体回滚，原归属与事实表保持不变。
- **按产线取数**：只可能读到已完整提交的读数及其归属（READ COMMITTED + 单事务原子写）。

### 并发正确性如何保证

| 场景 | 机制 |
|------|------|
| 两个实例同时登记重叠时段 | 电表行 `FOR UPDATE` 串行化 + GiST 排他约束，败者事务回滚返回 409 |
| 两个实例同时修正同一电表 | 行锁 + 乐观 `version`：先提交者版本号 +1，后者 `base_version` 过期返回 409 |
| 入仓与修正并发 | 两者都先锁电表行，串行执行；读数关联覆盖其时刻的现行归属 |
| 修正中途失败 | 全程单事务 + 可延迟外键，回滚后归属段、读数关联、版本号全部恢复原状 |

`reading.ownership_id` 外键声明为 `DEFERRABLE INITIALLY DEFERRED`，使修正事务可以
"先删旧段、再建新段、最后重指读数"，外键检查推迟到提交时执行。

## 2. 数据模型（`app/schema.sql`）

- `meter`：表号主键 + 全局乐观版本 `version`，每次登记/修正成功 +1。
- `ownership`：现行归属段（仅存活段），含 GiST 排他约束防重叠。
- `ownership_history`：所有变更的不可变审计轨迹（INSERT / REPLACE / SPLIT / DELETE）。
- `reading`：事实表，`ownership_id` 非空、可延迟外键，`reading_uid` 全局唯一。

所有时间列均为 `TIMESTAMPTZ`，API 只接受带时区的时间并统一转 UTC。

## 3. API 一览（交互式文档见 `/docs`）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 健康检查 |
| POST | `/ownership/register` | 登记一个互不重叠的归属时段，返回当前版本 |
| GET  | `/ownership/{meter_no}` | 列出某电表的现行归属段 |
| GET  | `/meters/{meter_no}` | 查询电表及其当前版本号 |
| POST | `/readings/ingest` | 批量入仓读数；任一缺归属则整批拒收 |
| POST | `/ownership/correct` | 凭 `base_version` 修正窗口归属并重关联读数 |
| GET  | `/readings/by-line/{line}` | 按产线查询已提交读数（支持 start/end/limit） |

错误码：404 电表未登记；409 重叠/版本冲突/读数编号重复；422 无归属、窗口含缺口、时间区间非法。

## 4. 用 Docker Compose 启动（双实例 + 数据库）

```bash
docker compose up --build
# 数据库:   localhost:5432
# 实例一:   http://localhost:8001  （/docs）
# 实例二:   http://localhost:8002  （/docs）
```

两个 uvicorn 实例共享同一个 PostgreSQL，连接池各自独立。数据库带健康检查，
API 实例等数据库就绪后才启动。

## 5. /docs 演示流程

1. `POST /ownership/register` 给电表 `M1` 登记 `[2026-01-01T00:00Z, 12:00Z) -> LINE-A`。
2. 再登记 `[12:00Z, 24:00Z) -> LINE-B`（相邻不重叠，成功）；尝试重叠登记会得到 409。
3. `GET /meters/M1` 取得当前 `version`。
4. `POST /readings/ingest` 提交两条读数，分别落在两个时段；故意提交一条无归属读数看 422。
5. `POST /ownership/correct` 带上一步的 `version`，把 `[06:00Z, 18:00Z)` 改成 `LINE-C`。
6. `GET /readings/by-line/LINE-C` 看到被重新关联的读数；两侧历史仍在 LINE-A / LINE-B。
7. 用过期的 `base_version` 再修正一次，得到 409。

`scripts/e2e_smoke.sh` 与 `scripts/e2e_dual_instance.py` 把上述流程（含双实例并发竞争）
脚本化，可直接对运行中的实例执行。

## 6. 本地开发与测试（无 Docker 时）

仓库提供 `scripts/dev_pg.sh`，可用解压版 PostgreSQL 在用户目录起一个本地集群：

```bash
./scripts/dev_pg.sh                      # 默认 localhost:5439, 库 meterdb
export DATABASE_URL=postgresql://node:node@localhost:5439/meterdb
pip install -r requirements.txt httpx pytest
pytest tests/                            # 17 个功能 + 并发测试
```

## 7. 目录结构

```
app/
  config.py      环境变量配置（懒加载，便于测试切换库）
  db.py          psycopg 连接池与 schema 初始化
  schema.sql     表、约束、索引、审计表
  errors.py      领域异常 -> HTTP 状态码
  schemas.py     Pydantic 请求/响应模型（UTC 校验）
  services.py    登记 / 入仓 / 修正 / 查询的事务逻辑
  main.py        FastAPI 路由与生命周期
tests/           功能测试与双连接并发测试（每测试独立 schema）
scripts/         本地 PG、HTTP 冒烟与双实例并发脚本
docker-compose.yml, Dockerfile
```
