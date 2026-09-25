# Differential-Privacy Budget Ledger

基于 **Python · FastAPI · SQLite · asyncio** 的差分隐私预算账本服务，支持
Docker Compose 一键启动。

客户端为数据集登记 epsilon / delta 总预算（cap），然后提交带
`request_id`、`query_type`、`epsilon`、`delta` 的分析申请；服务在一组
Rényi DP（RDP）orders 上累加各次申请的组合消耗，在并发申请下**原子地**
决定预留成功或拒绝超预算，并对重复 `request_id` 做**幂等**处理。提供
预算明细与剩余额度查询。

此外支持两阶段的**预算预留（reservation）**：客户端以
`reservation_id`、`epsilon`、`delta`、`ttl_seconds` 原子地锁定一段预算；
同一 `reservation_id` + 同负载重放幂等、负载冲突返回 `409`。预留可在 TTL
内**确认（confirm）**为一次正式分析申请（沿用同一套 RDP 核算，预算不重复
扣减），也可随时幂等**释放（release）**；超过 TTL 未确认的预留自动过期、
在查询与新建预留/申请时不再占用额度。未到期且仍为 `granted` 的预留还可
**续期（renewal）**：以数据集内幂等键 `renewal_id` 与正整数
`extend_seconds` 把到期时刻向后延长（新到期时刻距创建时刻不得超过 90 天），
金额与原始 `ttl_seconds` 不变、不重复占额，并在同一个 SQLite 写事务内保存
**不可变续期记录**；同负载重放返回首次续期的前后到期时刻，异负载返回
`409`，已到期或终态预留的新续期返回 `409`。仍为 `granted` 且未到期的预留
还可以**调整预留额度（adjustment）**：以数据集内幂等键 `adjustment_id` 给出
新的合法 `epsilon` / `delta`，在**同一个 SQLite 写事务**内以新额度替换旧额度，
并与「未冲正已授权申请 + 其他全部有效预留」按既有 RDP 口径重新组合核算；超出
`epsilon_cap` 时整笔拒绝、预留额度保持原值（拒绝同样留痕）。同键同负载重放
返回**首次裁定与额度快照**，异负载（额度不同或指向本数据集另一预留）返回
`409`，终态或已过期预留不得再发起新调整。调整成功后，预留确认、预算查询与
释放预测都使用新额度；原预留创建请求按 `reservation_id` 的幂等重放仍以**创建时
负载**判定（额度列与创建负载分别留存）。还提供**只读**的预算释放预测接口：
给定一个未来 UTC 截止时间，按 `expires_at` 分组模拟预留到期并逐刻重算
剩余 epsilon 与活动预留数，全程不改写任何预留状态或数据库。

此外支持数据集内的**原子分析批次（request batch）**：
`POST /datasets/{dataset_id}/request-batches` 携带一个 `batch_id` 与
**2～20 个**各自带独立 `request_id`、`query_type`、`epsilon`、`delta` 的分析
项；服务在**同一个 SQLite 写事务**内把整批与当前已授权申请、有效预留一起
做 RDP 联合组合判定——整批可容纳才为每一项写入已授权请求，**任一超额则一项
也不占额**（不写任何 `requests` 记录），并把批次的拒绝结果持久化留痕。
`batch_id` 是幂等键：同负载重放返回**首次裁定与当时的组合值**（即使之后预算
已被其他申请占用或释放），负载（含项目顺序）不同返回 `409`；批内
`request_id` 重复、或某 `request_id` 已被其他请求占用（直接申请、预留确认、
先前已批准批次）都返回 `409` 且**不留任何部分记录**。可用
`GET /datasets/{dataset_id}/request-batches/{batch_id}` 按批次查询裁定；批次
内各项同时也是普通申请，现有单项提交 / 查询 / 冲正接口语义完全保留。

此外提供数据集之间的 **epsilon 上限转拨（budget transfer）**：
`POST /budget-transfers` 以全局幂等键 `transfer_id` 指定来源 / 目标数据集与
正数 `epsilon_amount`，在同一个 SQLite 写事务内按既有 RDP 口径核算两端的
已授权申请与未过期预留占用，原子地把 epsilon_cap 从来源划到目标；
delta_cap 与已入账申请、预留均不变。来源转出后上限必须仍为**正数**且足以
覆盖现有占用、目标转入后上限不得超过登记上限 **1000**，否则整笔拒绝、两个
上限均不变（拒绝也留痕）。相同 `transfer_id` + 相同负载重试返回原记录而不
再次转拨，异负载返回 `409`。

最后支持对**已授权分析申请的冲正（reversal）**：
`POST /datasets/{dataset_id}/requests/{request_id}/reversals` 携带全局幂等键
`reversal_id` 与原因，仅允许冲正**已授权且尚未冲正**的申请；冲正被拒绝的
申请或对同一申请再次冲正返回 `409`。原申请裁定与「预留 → 确认」的历史
记录**原样保留**，另写一条**不可变**冲正记录，并在同一个 SQLite 写事务内
完成；冲正后该申请被移出 RDP 组合——预算查询、后续申请、预留确认、上限
转拨与到期预测都按释放后的额度核算，而**仍然有效的预留继续计入**。相同
`reversal_id` + 相同目标与原因重试返回原冲正记录（不重复释放），异负载
`409`；原 `request_id` 同负载重试仍回放最初裁定、不重新占额。

另外提供**只读的候选组合试算（selection plan）**：
`POST /datasets/{dataset_id}/selection-plans` 一次提交 **1～12 个**候选分析项
（每项含请求内唯一的 `candidate_id`、`epsilon`、`delta` 与正整数 `value`），
服务按**当前账本**——全部未冲正已授权申请与全部未到期有效预留——用既有 RDP
口径**穷举所有可行子集**（至多 2¹² = 4096 个），选出总 `value` 最大的组合；
总 value 相同时取**组合投影 epsilon 更小**者，再相同则按 `candidate_id`
升序列表的字典序裁定。响应返回选中 ID、总 value、组合投影 epsilon、剩余额度
与未选中 ID；没有任何非空子集可行时返回空组合，占用字段即当前账本占用。
试算**严格只读**：不创建申请、预留或任何裁定记录，也不触发预留惰性清扫；
重复调用始终按最新账本重新计算。

最后提供**只读的隐私成本归因（cost attribution）**：
`POST /datasets/{dataset_id}/cost-attributions` 接收 **1～10 个互不重复**的
`request_id`，只允许选取本数据集内**已授权且未冲正**的申请（缺失 → `404`，
已拒绝或已冲正 → `409`，`request_id` 重复或数量越界 → `422`）。服务以
**其余**未冲正已授权申请与**当前未到期**的 granted 预留为基线，用同一套 RDP
口径穷举所选申请的全部 2ⁿ 子集并计算每个子集的投影 epsilon，按各申请加入
子集时的**边际增量**与**标准 Shapley 权重**
`|S|!(n−1−|S|)!/n!` 分摊整组造成的增量；响应给出基线占用、全组选入占用、
逐项分摊值与分摊总和（数值误差内恰等于两次占用之差）。归因**严格只读**：
不写任何记录、不触发预留惰性清扫，每次调用都按实时账本重算。

最后提供**只读的重复工作负载容量试算（repeat-run trial）**：
`POST /datasets/{dataset_id}/repeat-run-trials` 接收 **1～5 个**分析项（各含
`epsilon`、`delta`）与 `max_runs`（**1～100000**），把整组分析项视为**一次
运行（run）**叠加到当前账本（未冲正已授权申请 + 未到期 granted 预留）上，沿用
既有 RDP 组合口径求「不超 `epsilon_cap` 的最大**完整运行次数**」——即使连一次
都放不下也正常返回 `0`。响应给出最大次数、该次数下的投影占用，以及**再多运行
一次**的投影占用（当最大次数已达 `max_runs` 上界时不再多算、该字段为
`null`）；纯 ε 项在任意次数下仍走精确加法。试算**严格只读**：不创建申请、
不写任何裁定记录、不触发预留惰性清扫，每次调用都按最新账本重算。

此外提供**只读的冲正方案试算（reversal plan）**：
`POST /datasets/{dataset_id}/reversal-plans` 提交**一个待申请分析项**
（`epsilon`、`delta`）与 **1～12 个互异**的可冲正 `request_id`（仅限本数据集
**已授权且未冲正**的申请；缺失 → `404`，已拒绝或已冲正 → `409`）。服务以
**其余**未冲正已授权申请与**当前未到期**的 granted 预留为固定基线，用既有 RDP
口径**穷举候选冲正子集**并试算待申请项的投影，找出使其不超 `epsilon_cap` 的
方案：先取**冲正条数最少**，再取**冲正后账本投影占用最大**者，仍同分取
`request_id` 升序列表字典序最小者。响应返回建议冲正的 ID 与冲正前 / 冲正后 /
加入待申请项后的组合占用；没有任何可行子集时明确返回不可行。试算**严格只读**：
不执行冲正或申请、不写任何记录，也不触发预留惰性清扫；重复调用始终按最新账本
重新计算。

---

## 1. 快速启动（Docker Compose）

**单条命令启动**（首次会自动构建镜像，SQLite 数据保存在命名卷中，服务
监听容器内 **8000** 端口）：

```bash
docker compose up -d --build
```

启动后 API 地址为 **http://localhost:8000**（健康检查
http://localhost:8000/health，交互式文档 http://localhost:8000/docs）。
无需任何额外配置；必要的环境变量（数据库路径、监听地址 / **8000** 端口、
SQLite 超时、浮点容差）都已在 `docker-compose.yml` 中给出默认值，见下文
第 3 节。容器内固定监听 `8000`（Dockerfile `EXPOSE 8000`，compose
`"${LEDGER_PORT:-8000}:8000"` 端口映射），宿主机默认即用
**http://localhost:8000** 访问。

其他常用命令：

```bash
# 查看日志
docker compose logs -f

# 停止
docker compose down

# 停止并删除数据库卷（清空所有账本数据）
docker compose down -v

# 在线备份账本（事务一致快照，见第 8 节）
docker compose exec dp-ledger \
  python -m app.backup --output /data/backups/ledger.db
```

启动后：

| 项目 | 地址 |
| --- | --- |
| API Base URL | `http://localhost:8000` |
| 健康检查 | `GET http://localhost:8000/health` |
| Swagger 交互文档 | `http://localhost:8000/docs` |
| ReDoc 文档 | `http://localhost:8000/redoc` |
| OpenAPI Schema | `http://localhost:8000/openapi.json` |

宿主机端口可用环境变量覆盖：

```bash
LEDGER_PORT=18000 docker compose up -d --build
# 然后访问 http://localhost:18000
```

数据库文件持久化在命名卷 `dp-ledger-data` 内（容器内 `/data/ledger.db`，
SQLite WAL 模式）。

---

## 2. 本地（无 Docker）运行

需要 Python 3.11+：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 数据库文件路径（默认 /data/ledger.db，本地运行建议改成当前目录）
export DATABASE_PATH=./ledger.db
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 3. 配置

所有配置通过环境变量注入（见 `docker-compose.yml`）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_PATH` | `/data/ledger.db` | SQLite 数据库文件路径，所在目录需可写 |
| `HOST` | `0.0.0.0` | uvicorn 监听地址 |
| `PORT` | `8000` | 容器内监听端口 |
| `LEDGER_PORT` | `8000` | Compose 映射到宿主机的端口 |
| `SQLITE_BUSY_TIMEOUT_MS` | `10000` | SQLite 写锁等待超时（毫秒） |
| `EPSILON_TOLERANCE` | `1e-12` | 预算比较的浮点容差，避免累计舍入误拒 |

---

## 4. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/datasets/{dataset_id}` | 登记数据集及 epsilon/delta 上限 |
| `GET` | `/datasets/{dataset_id}` | 数据集信息 + 预算明细 |
| `GET` | `/datasets/{dataset_id}/budget` | 仅预算明细（消耗 / 预留 / 剩余 / RDP 曲线） |
| `POST` | `/datasets/{dataset_id}/requests` | 提交分析申请（幂等） |
| `GET` | `/datasets/{dataset_id}/requests` | 全部申请记录（含 granted / rejected，并标明是否已冲正） |
| `POST` | `/datasets/{dataset_id}/selection-plans` | **只读**试算：穷举 1～12 个候选的可行子集，返回总 value 最大的组合（不落库） |
| `POST` | `/datasets/{dataset_id}/cost-attributions` | **只读**：对 1～10 个已授权未冲正申请按 Shapley 边际增量分摊整组 epsilon 增量（不落库） |
| `POST` | `/datasets/{dataset_id}/repeat-run-trials` | **只读**：把 1～5 个分析项作为一组工作负载，求 1～100000 内不超 cap 的最大完整运行次数及再跑一次的投影（不落库） |
| `POST` | `/datasets/{dataset_id}/reversal-plans` | **只读**：为一个待申请分析项穷举 1～12 个可冲正申请的冲正子集，返回使投影不超 cap 的最少冲正方案（不落库） |
| `POST` | `/datasets/{dataset_id}/request-batches` | 原子提交 2～20 项分析批次（整批批准或整批拒绝，按 `batch_id` 幂等） |
| `GET` | `/datasets/{dataset_id}/request-batches/{batch_id}` | 按 `batch_id` 查询批次裁定与各项当时的组合值 |
| `POST` | `/datasets/{dataset_id}/requests/{request_id}/reversals` | 冲正一条已授权且未冲正的申请（按 `reversal_id` 幂等） |
| `GET` | `/datasets/{dataset_id}/reversals` | 数据集下全部冲正记录 |
| `GET` | `/datasets/{dataset_id}/requests/{request_id}/reversals/{reversal_id}` | 查询单条冲正记录 |
| `POST` | `/datasets/{dataset_id}/reservations` | 创建预算预留（幂等，带 TTL） |
| `GET` | `/datasets/{dataset_id}/reservations` | 全部预留记录（含各生命周期状态与时间） |
| `GET` | `/datasets/{dataset_id}/reservations/{reservation_id}` | 查询单个预留 |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/confirm` | 确认预留 → 分析申请（幂等） |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/release` | 释放预留（幂等） |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/renewals` | 续期预留：把未到期 granted 预留的到期时刻后移（按 `renewal_id` 幂等） |
| `POST` | `/datasets/{dataset_id}/reservations/{reservation_id}/adjustments` | 调整预留额度：在 cap 内以新的 epsilon/delta 原子替换旧额度（按 `adjustment_id` 幂等，超额拒绝并留痕） |
| `GET` | `/datasets/{dataset_id}/budget/release-forecast?deadline=...` | **只读**模拟截止时间前的预留到期释放 |
| `POST` | `/budget-transfers` | 在两个数据集间原子转拨 epsilon 上限（幂等） |
| `GET` | `/health` | 健康检查 |

`dataset_id` / `reservation_id` 允许字母、数字、`.`、`_`、`-`，长度 ≤ 200。

### 4.1 登记数据集

```bash
curl -X POST http://localhost:8000/datasets/sales-2026 \
  -H 'Content-Type: application/json' \
  -d '{
    "epsilon_cap": 1.0,
    "delta_cap": 1e-6,
    "description": "2026 销售数据集"
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `epsilon_cap` | float | `> 0`，总 epsilon 预算 |
| `delta_cap` | float | `>= 1e-20, < 1`，总 delta；同时作为 RDP 转回 `(ε,δ)` 时的目标 δ |
| `description` | string | 可选，≤ 2000 字符 |

重复登记同一 `dataset_id` 返回 `409 dataset_already_exists`。

### 4.2 提交分析申请

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/requests \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-20260923-0001",
    "query_type": "gaussian_mean",
    "epsilon": 0.2,
    "delta": 1e-8
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `request_id` | string | 必填，客户端生成的幂等键，**同一数据集内唯一** |
| `query_type` | string | 必填，查询类型标签（如 `gaussian_mean`、`histogram`） |
| `epsilon` | float | `> 0`，本次申请的 epsilon |
| `delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |

返回 `201` 与裁定结果：

```json
{
  "dataset_id": "sales-2026",
  "request_id": "req-20260923-0001",
  "query_type": "gaussian_mean",
  "epsilon": 0.2,
  "delta": 1e-8,
  "allowed": true,
  "status": "granted",
  "reason": null,
  "epsilon_before": 0.0,
  "epsilon_after": 0.203936,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00"
}
```

* 预算内 → `allowed=true, status="granted"`，预算被原子预留；
* 超预算 → 仍返回 `201`（请求被正常受理并留痕），但
  `allowed=false, status="rejected"`，`reason` 说明投影消耗，
  **不占用任何预算**；
* 数据集不存在 → `404`。

**极小正 delta**：`delta` 可取任意小的正数（直至最小正双精度约
`5e-324`，如 `1e-309`）。核算在对数域以 `-log(δ)` 求 `log(1/δ)`，
不会先取倒数 `1/δ`——后者在 `δ < ~5.6e-309` 时浮点溢出为 `inf`，
会把本有有限投影的申请误判为超额。例如数据集
`epsilon_cap=1`、`delta_cap=1e-6` 时，向 API（默认
**http://localhost:8000**，见第 1 节）提交 `epsilon=0.1, delta=1e-309`：

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/requests \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-20260925-tiny",
    "query_type": "gaussian_mean",
    "epsilon": 0.1,
    "delta": 1e-309
  }'
```

投影 epsilon 约为 `0.1886`（最优 order `8192.0`），远低于 cap，正常
`granted`；预算查询与后续准入同样按该有限占用核算。

### 4.3 幂等语义

* 相同 `(dataset_id, request_id)` + **相同负载**重复提交：返回 `200`、
  与第一次完全一致的裁定，`idempotent_replay=true`，预算**只扣一次**；
* 相同 `request_id` 但 `query_type` / `epsilon` / `delta` 不同：
  返回 `409 request_id_conflict`；
* 被拒绝的申请同样幂等：重复提交返回最初的拒绝结果。

### 4.4 查询预算明细与剩余额度

```bash
curl http://localhost:8000/datasets/sales-2026/budget
```

```json
{
  "epsilon_cap": 1.0,
  "delta_cap": 1e-6,
  "epsilon_consumed": 0.203936,
  "epsilon_reserved": 0.0,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "granted_requests": 1,
  "active_reservations": 0,
  "rdp_curve": {
    "2.0": 18.62, "4.0": 6.34, "8.0": 2.83, "16.0": 1.43,
    "32.0": 0.79, "64.0": 0.50, "128.0": 0.35, "512.0": 0.21,
    "4096.0": 0.204
  }
}
```

字段口径：

* `epsilon_consumed`：已授权（直接 granted 或由预留 confirmed）分析申请的
  RDP 投影 epsilon；
* `epsilon_reserved`：**未过期有效预留**在同一组合曲线上额外锁定的投影
  epsilon（相对 `epsilon_consumed` 的增量）；
* `epsilon_remaining`：`epsilon_cap −`（已授权申请与有效预留**一起组合**后
  的投影 epsilon），即此刻还能再授权的额度；
* `active_reservations`：处于 `granted`（已创建未确认/释放/过期）状态的
  预留数量；`rdp_curve` 同时包含已授权申请与有效预留。

### 4.5 查询申请明细

```bash
curl http://localhost:8000/datasets/sales-2026/requests
```

按受理顺序返回所有申请（granted 与 rejected），每条含申请参数、裁定、
理由、申请前后累计 epsilon 与时间戳。

### 4.6 原子分析批次（all-or-nothing batch）

一次提交 **2～20 个**分析项，由服务在同一个写事务内联合组合、整批判定：

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/request-batches \
  -H 'Content-Type: application/json' \
  -d '{
    "batch_id": "batch-20260924-0001",
    "items": [
      {"request_id": "req-20260924-0101", "query_type": "gaussian_mean",
       "epsilon": 0.1, "delta": 1e-8},
      {"request_id": "req-20260924-0102", "query_type": "histogram",
       "epsilon": 0.15, "delta": 1e-8},
      {"request_id": "req-20260924-0103", "query_type": "sql_count",
       "epsilon": 0.05, "delta": 0.0}
    ]
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `batch_id` | string | 必填，客户端生成的幂等键（字母/数字/`.`/`_`/`-`，1–200），**同一数据集内唯一** |
| `items` | array | 必填，**2～20 项**；每项含独立的 `request_id`、`query_type`、`epsilon`（`> 0`）、`delta`（`>= 0, < 1`，`0` 为纯 ε-DP） |

判定口径与单项申请完全一致（同一张 RDP orders 网格、同一浮点容差），只是把
「全部已授权（未冲正）申请 + 全部未过期有效预留」与**整批各项**一次性组合后
投影：投影 epsilon `≤ epsilon_cap` 才整批批准。批准时返回 `201`，批次内**每一
项**都成为一条普通 `granted` 申请（之后可在 `GET …/requests` 看到、可被冲正）；
各项的 `epsilon_before` / `epsilon_after` 是按提交顺序**累计**的组合 epsilon
快照：

```json
{
  "dataset_id": "sales-2026",
  "batch_id": "batch-20260924-0001",
  "allowed": true,
  "status": "granted",
  "reason": null,
  "item_count": 3,
  "epsilon_cap": 1.0,
  "epsilon_before": 0.0,
  "epsilon_after": 0.311,
  "epsilon_remaining": 0.689,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-24T09:00:00.000000+00:00",
  "items": [
    {"request_id": "req-20260924-0101", "query_type": "gaussian_mean",
     "epsilon": 0.1, "delta": 1e-8, "allowed": true, "status": "granted",
     "reason": null, "epsilon_before": 0.0, "epsilon_after": 0.103,
     "epsilon_remaining": 0.897, "rdp_best_order": 8192.0,
     "created_at": "2026-09-24T09:00:00.000000+00:00"},
    {"request_id": "req-20260924-0102", "query_type": "histogram",
     "epsilon": 0.15, "delta": 1e-8, "allowed": true, "status": "granted",
     "reason": null, "epsilon_before": 0.103, "epsilon_after": 0.259,
     "epsilon_remaining": 0.741, "rdp_best_order": 8192.0,
     "created_at": "2026-09-24T09:00:00.000000+00:00"},
    {"request_id": "req-20260924-0103", "query_type": "sql_count",
     "epsilon": 0.05, "delta": 0.0, "allowed": true, "status": "granted",
     "reason": null, "epsilon_before": 0.259, "epsilon_after": 0.311,
     "epsilon_remaining": 0.689, "rdp_best_order": 8192.0,
     "created_at": "2026-09-24T09:00:00.000000+00:00"}
  ]
}
```

整批超额时同样返回 `201`（已正常受理并留痕），但
`allowed=false, status="rejected"`，批次级 `reason` 给出投影消耗；
**一个 `requests` 行都不会写入**——`epsilon_before` / `epsilon_after` 保持在
批次前的组合值、占用为零，各 item 的 `epsilon_before` / `epsilon_after` /
`epsilon_remaining` / `rdp_best_order` 为 `null` 并带同样的拒绝原因。被拒批次
的 `request_id` **未被占用**，后续批次或单项申请可以照常使用。

幂等与冲突语义：

* 相同 `(dataset_id, batch_id)` + **完全相同的有序负载**（每项的
  `request_id` / `query_type` / `epsilon` / `delta` 与顺序都一致）重试：返回
  `200`、**首次裁定与首次的组合值**（批次级与各项 `epsilon_before/after`、
  `created_at` 原样回放），`idempotent_replay=true`，预算只占用一次；即使其间
  预算已被其他申请/预留/冲正改变，重放结果也不随之变化；被拒绝的批次同样
  幂等；
* 相同 `batch_id` 但负载任一不同（含项目增删、顺序对调）→
  `409 batch_id_conflict`，不产生任何变更；
* 同一批次内出现重复 `request_id` → `409 batch_item_request_id_duplicate`
  （响应给出重复的 `request_id`），不留批次记录、不留申请；
* 某个 `request_id` 已被其他请求占用（直接提交、预留确认、或先前**已批准**
  批次中的一项）→ `409 request_id_conflict`，整批不落库；
* 数据集不存在 → `404 dataset_not_found`；批次数 1 个或超过 20 个、
  `batch_id` 含非法字符、`epsilon`/`delta` 越界等 → `422`。

按批次查询裁定：

```bash
curl http://localhost:8000/datasets/sales-2026/request-batches/batch-20260924-0001
```

返回与首次提交一致的 `BatchVerdict`（`idempotent_replay` 恒为 `false`）；
批次不存在 → `404 batch_not_found`。批次内各项是普通申请：单项接口
`POST …/requests` 对其 `request_id` 同负载重放仍回放最初裁定（不重新占额）、
异负载 `409`；已批准批次中的申请也可以被冲正，冲正只改变此后的活预算口径，
**批次裁定作为历史记录保持首次的组合值不变**。

### 4.7 创建预算预留

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/reservations \
  -H 'Content-Type: application/json' \
  -d '{
    "reservation_id": "res-20260923-0001",
    "epsilon": 0.2,
    "delta": 1e-8,
    "ttl_seconds": 600
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `reservation_id` | string | 必填，客户端生成的幂等键，**同一数据集内唯一** |
| `epsilon` | float | `> 0`，预留的 epsilon |
| `delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |
| `ttl_seconds` | int | `1 … 7776000`（最长 90 天），预留有效期；过期未确认自动释放 |

服务在**同一个原子事务**内组合「全部已授权申请 + 全部未过期有效预留 +
本次预留」后按 RDP 投影判定，返回 `201`：

```json
{
  "dataset_id": "sales-2026",
  "reservation_id": "res-20260923-0001",
  "epsilon": 0.2,
  "delta": 1e-8,
  "ttl_seconds": 600,
  "allowed": true,
  "status": "granted",
  "reason": null,
  "epsilon_before": 0.0,
  "epsilon_after": 0.203936,
  "epsilon_remaining": 0.796064,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00",
  "expires_at": "2026-09-23T01:20:16.563790+00:00",
  "expired_at": null,
  "confirmed_at": null,
  "confirmed_request_id": null,
  "released_at": null
}
```

* 预算内 → `allowed=true, status="granted"`，额度立即锁定，
  `expires_at = created_at + ttl_seconds`；
* 超预算 → 仍返回 `201`（正常受理留痕），但 `allowed=false,
  status="rejected"`，`expires_at=null`，**不锁定任何额度**；
* 数据集不存在 → `404`。

预留状态机：`granted → confirmed | released | expired`，`rejected` 为终态。

### 4.8 预留幂等与冲突语义

* 相同 `(dataset_id, reservation_id)` + **相同负载**
  （`epsilon` / `delta` / `ttl_seconds` 全一致）重复提交：返回 `200`、
  与第一次一致的裁定和时间戳，`idempotent_replay=true`，额度**只锁一次**。
  注意 TTL **不会**因重放而延长（沿用首次的 `created_at`/`expires_at`）；
* 相同 `reservation_id` 但 `epsilon` / `delta` / `ttl_seconds` 任一不同：
  返回 `409 reservation_id_conflict`；
* 被拒绝的预留同样幂等。

### 4.9 确认预留（转为分析申请）

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/reservations/res-20260923-0001/confirm \
  -H 'Content-Type: application/json' \
  -d '{"request_id": "req-20260923-0001", "query_type": "gaussian_mean"}'
```

预留锁定的 `(epsilon, delta)` 变为一条 `granted` 分析申请，`query_type`
与正式 `request_id` 在确认时给出；响应体与 `POST /requests` 的
`Verdict` 完全一致（`201`）。

* 预算**不重复扣减**：确认前由预留锁定的组合消耗，确认后改由该申请承载，
  `epsilon_after` 保持不变；
* 确认必须在预留处于 `granted` 且未过期时进行；
  `released` / `expired` / `rejected` → `409`（错误码分别为
  `reservation_released` / `reservation_expired` /
  `reservation_rejected`）；预留不存在 → `404 reservation_not_found`；
* `request_id` 已被其他申请占用（或与同预留上次确认使用的不同）→
  `409 request_id_conflict` / `reservation_confirmed`，预留保持原状；
* 相同确认负载重放 → `200`、`idempotent_replay=true`，只生成一条申请。

### 4.10 释放预留

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/reservations/res-20260923-0001/release
```

* 对 `granted` 预留：置为 `released` 并记录 `released_at`，锁定额度
  **立即释放**，返回 `200`；
* 重复释放已 `released` / 已 `expired` 的预留：幂等返回最终状态
  （`idempotent_replay=true`）；
* `confirmed` / `rejected` 预留不可释放 → `409`；
  预留不存在 → `404`。

### 4.11 续期预留（renewal）

对**未到期且仍为 `granted`** 的预留，可以把到期时刻向后延长；预留的
`epsilon` / `delta` 与原始 `ttl_seconds` 不变，也**不会重复占额**——预留
只是把它已锁定的额度继续持有更久：

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/reservations/res-20260923-0001/renewals \
  -H 'Content-Type: application/json' \
  -d '{
    "renewal_id": "ren-20260925-0001",
    "extend_seconds": 600
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `renewal_id` | string | 必填，客户端生成的幂等键（字母/数字/`.`/`_`/`-`，1–200），**同一数据集内唯一** |
| `extend_seconds` | int | 必填，**正整数**（`1 … 7776000`），在当前 `expires_at` 上追加的秒数 |

服务在**同一个 SQLite 写事务**内完成「惰性过期清扫 → 状态与 90 天视野校验
→ 更新 `expires_at` → 写入不可变续期记录」，返回 `201`：

```json
{
  "renewal_id": "ren-20260925-0001",
  "dataset_id": "sales-2026",
  "reservation_id": "res-20260923-0001",
  "extend_seconds": 600,
  "expires_at_before": "2026-09-23T01:20:16.563790+00:00",
  "expires_at_after": "2026-09-23T01:30:16.563790+00:00",
  "idempotent_replay": false,
  "created_at": "2026-09-25T01:10:16.563790+00:00"
}
```

* 新到期时刻 = 当前 `expires_at` + `extend_seconds`；可用新的 `renewal_id`
  多次续期，后一次在前一次的基础上继续后移；
* **90 天视野上限**：新到期时刻距预留的 `created_at` 不得超过 `7776000`
  秒（90 天，恰好到界允许），否则返回 `409 renewal_horizon_exceeded`，
  到期时刻保持不变；
* 已到期（含被惰性清扫）或终态（`confirmed` / `released` / `rejected` /
  `expired`）的预留**不能续期** → `409`（错误码分别为
  `reservation_confirmed` / `reservation_released` / `reservation_rejected` /
  `reservation_expired`）；预留不存在 → `404 reservation_not_found`，
  数据集不存在 → `404 dataset_not_found`；
* 相同 `(dataset_id, renewal_id)` + **相同负载**（同一 `reservation_id` 与
  `extend_seconds`）重试：返回 `200` 与**首次续期记录**的前后到期时刻，
  `idempotent_replay=true`，到期时刻只后移一次——即使预留此后已过期也照样
  回放首次记录；相同 `renewal_id` 但负载不同（`extend_seconds` 不同或指向
  本数据集另一预留）→ `409 renewal_id_conflict`，不产生任何变更；
  `renewal_id` 只在本数据集内唯一，其他数据集可复用；
* 续期后，现有预留查询（`GET …/reservations[/…]`）、确认（`confirm`）与
  只读释放预测（`release-forecast`）都按**新的**到期时刻工作；
* `extend_seconds` 非正整数 / 超过 90 天、`renewal_id` 缺失或含非法字符等
  → `422`。

### 4.12 调整预留额度（adjustment）

对**仍为 `granted` 且未到期**的预留，可以把它锁定的额度直接改成新的合法
数值；预留的 `ttl_seconds` 与到期时刻不变，确认/释放/过期的生命周期语义也
完全不变，只是后续一律按**新额度**参与 RDP 组合：

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/reservations/res-20260923-0001/adjustments \
  -H 'Content-Type: application/json' \
  -d '{
    "adjustment_id": "adj-20260925-0001",
    "epsilon": 0.3,
    "delta": 1e-8
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `adjustment_id` | string | 必填，客户端生成的幂等键（字母/数字/`.`/`_`/`-`，1–200），**同一数据集内唯一** |
| `epsilon` | float | 必填，`> 0, <= 1000`，新的预留 epsilon |
| `delta` | float | 必填，`>= 0, < 1`；`0` 表示纯 ε-DP |

服务在**同一个 SQLite 写事务**内完成「惰性过期清扫 → 状态校验 → 以新额度
替换本预留条目并与未冲正已授权申请、其他全部有效预留联合 RDP 核算 → 更新
预留行 / 写入不可变调整记录」，返回 `201`：

```json
{
  "adjustment_id": "adj-20260925-0001",
  "dataset_id": "sales-2026",
  "reservation_id": "res-20260923-0001",
  "epsilon_before": 0.2,
  "delta_before": 1e-8,
  "requested_epsilon": 0.3,
  "requested_delta": 1e-8,
  "epsilon_after": 0.3,
  "delta_after": 1e-8,
  "allowed": true,
  "status": "adjusted",
  "reason": null,
  "epsilon_cap": 1.0,
  "epsilon_committed_before": 0.203936,
  "epsilon_committed_after": 0.304232,
  "epsilon_remaining": 0.695768,
  "rdp_best_order": 4096.0,
  "idempotent_replay": false,
  "created_at": "2026-09-25T02:10:16.563790+00:00"
}
```

* 投影组合 epsilon 在 `epsilon_cap`（含浮点容差）内 →
  `allowed=true, status="adjusted"`，预留行的 `epsilon` / `delta` 立即替换
  为新值；调小额度会释放预算、调大额度会多占预算，改 `delta`（纯 ε ↔ 近似
  DP）同样允许；
* 超出 cap → 仍返回 `201`（正常受理留痕），但 `allowed=false,
  status="rejected"`，`epsilon_after` / `delta_after` 与 `*_before` 相同、
  `epsilon_committed_after == epsilon_committed_before`，**预留额度与到期
  时刻保持原值**，被拒不阻塞之后用新的 `adjustment_id` 再试；
* 已到期（含被惰性清扫）或终态（`confirmed` / `released` / `rejected` /
  `expired`）的预留**不能调整** → `409`（错误码分别为
  `reservation_confirmed` / `reservation_released` / `reservation_rejected` /
  `reservation_expired`）；预留不存在 → `404 reservation_not_found`，
  数据集不存在 → `404 dataset_not_found`；
* 相同 `(dataset_id, adjustment_id)` + **相同负载**（同一 `reservation_id`
  与 `epsilon` / `delta`）重试：返回 `200` 与**首次裁定**（含拒绝）及其
  额度快照，`idempotent_replay=true`，额度只替换一次——即使预留此后已过期
  也照样回放首次记录；相同 `adjustment_id` 但负载不同（`epsilon` / `delta`
  不同或指向本数据集另一预留）→ `409 adjustment_id_conflict`，不产生任何
  变更；`adjustment_id` 只在本数据集内唯一，其他数据集可复用；
* 调整成功后，预留确认（`confirm`）以**新额度**生成申请且不重复扣减，预算
  查询（`GET …/budget`）、预留查询与只读释放预测（`release-forecast`）均按
  新额度核算；而原预留创建请求以相同 `reservation_id` 重放时，仍按**创建时
  负载**（`epsilon` / `delta` / `ttl_seconds`）判定幂等或
  `409 reservation_id_conflict`，不受后续调整影响；
* 可以用不同的 `adjustment_id` 连续调整：每次都基于预留当前额度做替换；
* `epsilon` / `delta` 越界、`adjustment_id` 缺失或含非法字符等 → `422`。

### 4.13 TTL 过期

* 预留不会被后台线程主动删除；任何接触预算的路径（查询预留/预算、新建
  申请或预留、确认、释放、续期、调整）都会在同一临界区内把 `expires_at ≤ now` 的
  `granted` 预留惰性标记为 `expired`（记录 `expired_at`），之后它们**不再
  占用额度**——因此过期后新的申请/预留可以使用这部分额度；
* 过期是终态：过期预留不能再确认（`409 reservation_expired`），对其调用
  release 返回幂等结果；
* `GET …/reservations` 返回全部预留及各自的 `status` 与
  `created_at` / `expires_at` / `expired_at` / `confirmed_at` /
  `released_at` 时间戳，便于审计。

### 4.14 预算释放预测（只读）

在不触碰任何预留状态的前提下，预测到某个未来 UTC 时刻为止，TTL 到期会按
什么节奏归还多少预算：

```bash
curl -G http://localhost:8000/datasets/sales-2026/budget/release-forecast \
  --data-urlencode 'deadline=2026-09-24T00:00:00Z'
```

查询参数：

| 参数 | 类型 | 约束 |
| --- | --- | --- |
| `deadline` | string (query) | 必填，**未来的**带时区 ISO-8601 UTC 时间，如 `2026-09-24T00:00:00Z`；也接受 `+02:00` 等其他 UTC 偏移，服务端归一化到 UTC |

模拟口径：

* 从**当前有效**（`status=granted` 且 `expires_at` 晚于现在）的预留出发；
  已拒绝 / 已确认 / 已释放 / 已过期的预留不参与，已授权申请始终计入；
* 按 `expires_at` 分组、按时间升序模拟：在每个到期时点**一次性移除同一
  时刻到期的全部预留**，并用与预算视图完全相同的 RDP 组合口径重新计算
  `epsilon_consumed` / `epsilon_reserved` / `epsilon_remaining` /
  `rdp_best_order` 与 `active_reservations`；
* 恰好在 `deadline` 时刻到期（`expires_at <= deadline`）的预留视为已移除；
  `expires_at` 严格晚于截止时间（或无到期时间）的预留**继续计入**
  `at_deadline`。

```json
{
  "dataset_id": "sales-2026",
  "generated_at": "2026-09-23T01:10:16.563790Z",
  "deadline": "2026-09-24T00:00:00Z",
  "initial": {
    "epsilon_consumed": 0.0,
    "epsilon_reserved": 0.203936,
    "epsilon_remaining": 0.796064,
    "rdp_best_order": 8192.0,
    "active_reservations": 1
  },
  "events": [
    {
      "expires_at": "2026-09-23T01:20:16.563790Z",
      "released_count": 1,
      "released_reservation_ids": ["res-20260923-0001"],
      "epsilon_consumed": 0.0,
      "epsilon_reserved": 0.0,
      "epsilon_remaining": 1.0,
      "rdp_best_order": 8192.0,
      "active_reservations": 0
    }
  ],
  "at_deadline": {
    "epsilon_consumed": 0.0,
    "epsilon_reserved": 0.0,
    "epsilon_remaining": 1.0,
    "rdp_best_order": 8192.0,
    "active_reservations": 0
  }
}
```

字段说明：`initial` 为「此刻」（任何模拟到期之前）的快照；`events` 每个
元素对应一个不同的到期时刻（同刻到期合并为一条，`released_count` 为该刻
释放数量），预算字段为该刻释放**之后**的状态；`at_deadline` 为截止时间
仍然有效（即尚未被模拟移除）的预留继续计入后的最终快照。

该接口**严格只读**：不会执行惰性过期清理、不会 UPDATE 预留行、不开启写
事务，因此即使数据库里存在「已到点但尚未被其他请求清扫」的预留，调用本
接口也不会改变它们的状态——重复调用（相同截止时间）结果稳定。错误情况：

* 数据集不存在 → `404 dataset_not_found`；
* 缺少 / 无法解析 `deadline` → `422`（FastAPI 校验错误）；
* 无时区信息的裸时间 → `422 invalid_forecast_deadline`；
* `deadline` 不晚于当前时间 → `422 invalid_forecast_deadline`。

### 4.15 epsilon 上限转拨（budget transfer）

在两个已登记数据集之间转拨 epsilon 上限，**delta_cap 与所有已入账申请、
预留都不改变**：

```bash
curl -X POST http://localhost:8000/budget-transfers \
  -H 'Content-Type: application/json' \
  -d '{
    "transfer_id": "xfer-20260923-0001",
    "source_dataset_id": "sales-2025",
    "target_dataset_id": "sales-2026",
    "epsilon_amount": 0.25
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `transfer_id` | string | 必填，客户端生成的**全局**幂等键（字母/数字/`.`/`_`/`-`，≤ 200） |
| `source_dataset_id` | string | 必填，被借记（转出）的数据集 |
| `target_dataset_id` | string | 必填，被贷记（转入）的数据集；必须与来源不同 |
| `epsilon_amount` | float | 必填，`> 0`（正数）且 `≤ 1000` |

转拨前，服务对两端**分别**按现有 RDP 口径计算「全部已授权申请 + 全部未
过期有效预留」的组合占用（即预算视图中的 committed epsilon；本调用内也会
惰性清扫到期预留）。成功条件（在同一个 `BEGIN IMMEDIATE` 事务内判定）：

1. 来源转出后 `epsilon_cap − epsilon_amount > 0`（必须保持**正数**）；
2. 来源转出后的新上限仍足以覆盖其当前占用
   （`新 cap ≥ committed epsilon`，允许 `EPSILON_TOLERANCE` 浮点容差）；
3. 目标转入后 `epsilon_cap + epsilon_amount ≤ 1000`（登记上限天花板）。

任一不满足 → 整笔**拒绝**（`allowed=false, status="rejected"`，`reason`
列出所有失败原因），**两个上限均保持不变**，拒绝尝试同样写入转拨记录留痕。
成功 → `allowed=true, status="transferred"`，返回 `201`：

```json
{
  "transfer_id": "xfer-20260923-0001",
  "source_dataset_id": "sales-2025",
  "target_dataset_id": "sales-2026",
  "epsilon_amount": 0.25,
  "allowed": true,
  "status": "transferred",
  "reason": null,
  "source": {
    "dataset_id": "sales-2025",
    "epsilon_cap": 0.75,
    "epsilon_committed": 0.203936,
    "epsilon_remaining": 0.546064
  },
  "target": {
    "dataset_id": "sales-2026",
    "epsilon_cap": 1.25,
    "epsilon_committed": 0.0,
    "epsilon_remaining": 1.25
  },
  "idempotent_replay": false,
  "created_at": "2026-09-23T01:10:16.563790+00:00"
}
```

`source` / `target` 中的 `epsilon_committed` 是转拨时点该端已授权申请与未
过期预留的 RDP 组合占用；`epsilon_remaining = epsilon_cap −
epsilon_committed`（成功时为转拨后的新 cap，拒绝时为原 cap）。随后用
`GET /datasets/{dataset_id}/budget` 即可看到两端的新 `epsilon_cap`，而
`delta_cap`、`granted_requests`、预留状态均与转拨前一致。

幂等与冲突语义：

* 相同 `transfer_id` + **完全相同负载**（来源、目标、金额一致）重试：
  返回 `200` 与**首次的原始记录**（含首次的裁定、两端 cap 快照与
  `created_at`），`idempotent_replay=true`，**不会再次转拨**；被拒绝的
  转拨同样幂等；
* 相同 `transfer_id` 但来源 / 目标 / 金额任一不同：
  返回 `409 transfer_id_conflict`，上限不变；
* 来源或目标数据集不存在 → `404 dataset_not_found`；
* `epsilon_amount ≤ 0`、两端相同、ID 含非法字符等 → `422`。

并发语义：转拨与普通申请 / 预留共用同一进程内写锁与 `BEGIN IMMEDIATE`
临界区，两端 cap 的「校验 + 更新 + 写转拨记录」在同一个 SQLite 写事务内
完成；并发转拨之间、转拨与申请 / 预留之间无论怎样交错，预算都不会透支
（来源新 cap 永远覆盖其占用），且两端 epsilon_cap 之和守恒。

### 4.16 冲正已授权申请（reversal）

当一次已授权（直接 granted，或由预留 confirmed）的分析申请需要撤销时，可
以对其发起**冲正**。冲正**不删除、不改写**原申请：`requests` 中的原始裁定
（`epsilon_before` / `epsilon_after` / 时间戳）与预留的 `confirmed` 历史都
原样保留；服务另写一条不可变冲正记录，并从冲正时刻起把该申请移出 RDP
组合，释放其占用的预算。

```bash
curl -X POST \
  http://localhost:8000/datasets/sales-2026/requests/req-20260923-0001/reversals \
  -H 'Content-Type: application/json' \
  -d '{
    "reversal_id": "rev-20260924-0001",
    "reason": "duplicate analysis, mistaken submission"
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `reversal_id` | string | 必填，客户端生成的**全局**幂等键（字母/数字/`.`/`_`/`-`，1–200） |
| `reason` | string | 必填，1–2000 字符，冲正原因，随记录不可变保存 |

成功返回 `201`：

```json
{
  "reversal_id": "rev-20260924-0001",
  "dataset_id": "sales-2026",
  "request_id": "req-20260923-0001",
  "reason": "duplicate analysis, mistaken submission",
  "epsilon": 0.2,
  "delta": 1e-8,
  "epsilon_cap": 1.0,
  "epsilon_before": 0.203936,
  "epsilon_after": 0.0,
  "epsilon_remaining": 1.0,
  "rdp_best_order": 8192.0,
  "idempotent_replay": false,
  "created_at": "2026-09-24T09:00:00.000000+00:00"
}
```

`epsilon_before` / `epsilon_after` 是冲正时点的组合 epsilon 快照（目标申请
移除前 / 移除后），`epsilon_remaining = epsilon_cap − epsilon_after`；
`epsilon` / `delta` 为被冲正申请自身的预算参数。冲正一条由预留确认而来的
申请时，释放的是该申请占用的额度，预留行仍保持 `confirmed` 不变。

状态与冲突语义：

* 仅 **granted 且未冲正** 的申请可冲正；
* 目标申请为 `rejected` → `409 request_not_granted`；
* 目标申请**已冲正** → `409 request_already_reversed`（detail 中给出已有
  冲正的 `reversal_id`）；
* 数据集不存在 → `404 dataset_not_found`；申请不存在 →
  `404 request_not_found`；
* 负载非法（空 id / 非法字符 / 空原因等）→ `422`。

幂等语义：

* 相同 `reversal_id` + 相同目标（dataset/request）+ 相同原因重试：返回
  `200` 与**首次的原始冲正记录**（含首次快照与 `created_at`），
  `idempotent_replay=true`，预算只释放一次；
* 相同 `reversal_id` 但目标申请或原因不同：返回
  `409 reversal_id_conflict`，不产生任何变更；
* 冲正后用原 `request_id` + 同负载重放 `POST /requests`：仍返回**最初的
  裁定**（`idempotent_replay=true`），**不会**重新占用预算——历史裁定与
  活预算口径分离。

冲正后的核算口径（与既有路径完全一致，只是 granted 集合排除被冲正申请）：

* `GET …/budget`、数据集信息：`epsilon_consumed` / `epsilon_remaining` /
  `granted_requests` / `rdp_curve` 按释放后重算；
* 后续 `POST …/requests` 与预留 `confirm`：在释放后的剩余额度上做准入；
* **仍然有效的预留继续计入**（`epsilon_reserved` /
  `active_reservations` 不受其他申请冲正影响）；
* `POST /budget-transfers`：两端占用重算时排除被冲正申请；
* `GET …/budget/release-forecast`：模拟基线的已授权申请集合同样排除被
  冲正申请，只对预留到期做模拟。

冲正记录查询：

* `GET /datasets/{dataset_id}/reversals` → 按冲正发生顺序返回该数据集全部
  冲正记录（字段同上，`idempotent_replay` 恒为 `false`）；
* `GET /datasets/{dataset_id}/requests/{request_id}/reversals/{reversal_id}`
  → 查询单条；`reversal_id` 不属于该申请 → `404 reversal_not_found`，申请
  本身不存在 → `404 request_not_found`；
* `GET …/requests` 列表中每条申请新增 `reversed`（bool）、`reversal_id`、
  `reversal_reason`、`reversed_at` 字段；未冲正的申请这些字段为
  `false` / `null`，原裁定字段保持为受理当时的历史值。

### 4.17 候选组合试算（selection plan，只读）

在**不创建任何申请 / 预留 / 裁定记录**的前提下，对 1～12 个候选分析项穷举
可行子集，返回当前账本下总 `value` 最大的组合。占用基线与正式准入完全一致：
全部 **granted 且未冲正**的申请+全部**未到期** granted 预留（已释放 / 已确认 /
已过期 / 被拒绝的预留不参与）。

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/selection-plans \
  -H 'Content-Type: application/json' \
  -d '{
    "candidates": [
      {"candidate_id": "cand-20260924-0001", "epsilon": 0.1, "delta": 1e-8, "value": 10},
      {"candidate_id": "cand-20260924-0002", "epsilon": 0.2, "delta": 1e-8, "value": 25},
      {"candidate_id": "cand-20260924-0003", "epsilon": 0.05, "delta": 0.0, "value": 7}
    ]
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `candidates` | array | 必填，**1～12 项** |
| `candidates[].candidate_id` | string | 必填，1–200 字符，**同一次请求内唯一** |
| `candidates[].epsilon` | float | `> 0, ≤ 1000`，候选分析的 epsilon |
| `candidates[].delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |
| `candidates[].value` | integer | 必填，**正整数**（`≥ 1`），组合要最大化的收益 |

服务对每个子集把其各项与当前占用一次性做 RDP 联合组合（与批次同一张 orders
网格、同一浮点容差），投影 epsilon `≤ epsilon_cap + EPSILON_TOLERANCE` 视为可行；
空集恒可行。在全部可行子集中按以下顺序裁定：

1. **总 `value` 最大**（各项 `value` 之和）；
2. 总 value 相同 → **组合投影 epsilon 更小**；
3. 仍相同 → 按 **`candidate_id` 升序列表的字典序**取更小者。

成功返回 `200`：

```json
{
  "dataset_id": "sales-2026",
  "generated_at": "2026-09-24T10:00:00.000000Z",
  "candidate_count": 3,
  "selected_candidate_ids": [
    "cand-20260924-0001",
    "cand-20260924-0002",
    "cand-20260924-0003"
  ],
  "unselected_candidate_ids": [],
  "total_value": 42,
  "epsilon_cap": 1.0,
  "epsilon_committed": 0.0,
  "projected_epsilon": 0.3612,
  "epsilon_remaining": 0.6388,
  "rdp_best_order": 8192.0
}
```

字段说明：

* `selected_candidate_ids` / `unselected_candidate_ids`：选中 / 未选中候选 ID，
  **均按 `candidate_id` 升序输出**；
* `total_value`：选中子集的 `value` 之和（空组合为 `0`）；
* `epsilon_committed`：试算时刻「未冲正已授权申请 + 未到期有效预留」的组合
  epsilon（与预算视图口径一致）；
* `projected_epsilon`：当前占用**再叠加选中子集各项**后的组合投影 epsilon
  （空组合即 `epsilon_committed`）；`rdp_best_order` 为取得该投影的最优 RDP order；
* `epsilon_remaining`：`epsilon_cap − projected_epsilon`（不为负）。

**没有任何非空子集可行**时，`selected_candidate_ids` 为 `[]`、
`total_value = 0`、`projected_epsilon = epsilon_committed`，全部候选出现在
`unselected_candidate_ids` 中。

语义与边界：

* **严格只读、无幂等键**：本接口不写任何表（不产生申请、预留、批次、转拨或
  冲正记录），也**不触发预留惰性清扫**（与 release-forecast 同为纯 SELECT）；
  因此没有 `idempotent_replay` 字段，**每次调用都按最新账本重算**——账本被
  其他申请 / 预留 / 冲正 / 转拨改变后，再次试算可能得到不同组合；
* 候选仅在试算内参与组合，不锁定任何额度，也不占用任何 `request_id` /
  `reservation_id` 命名空间；试算结束后客户端仍需通过普通申请 / 批次 / 预留
  接口正式提交（正式提交时按当时账本重新准入，试算结果不构成承诺）；
* 数据集不存在 → `404 dataset_not_found`；
* 候选数为 0 或超过 12、`candidate_id` 重复或为空、`epsilon` / `delta` 越界、
  `value` 非正整数（含小数）等 → `422`。

### 4.18 隐私成本归因（cost attribution，只读）

在**不创建任何记录、不改变预留状态**的前提下，把一组**已授权且未冲正**申请
「整组选入」造成的 epsilon 增量，按标准 **Shapley 值**分摊给组内每个申请。
可用于解释「这一批申请各自占用了多少边际隐私成本」（RDP 转换余量会因组合而
非可加，各申请的成本不能简单取自身投影）。

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/cost-attributions \
  -H 'Content-Type: application/json' \
  -d '{
    "request_ids": [
      "req-20260924-0001",
      "req-20260924-0002",
      "req-20260924-0003"
    ]
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `request_ids` | array | 必填，**1～10 个**、**互不重复**的 `request_id`（每个 1–200 字符）；顺序即响应逐项顺序，不影响数值 |

**基线 / 占用口径**（与正式准入、预算视图、试算完全一致的 RDP 口径）：

* **基线占用 `epsilon_baseline`**：本数据集内**除所选申请外**的全部未冲正
  granted 申请，加上**当前未到期**的 granted 预留，一起组合后的投影 epsilon；
  已拒绝 / 已冲正申请、已释放 / 已确认 / 已过期 / 被拒绝预留均不参与；
* **全组占用 `epsilon_full_group`**：在基线之上再把**全部所选申请**一次性
  组合进去后的投影 epsilon；
* **组增量 `epsilon_increment = epsilon_full_group − epsilon_baseline`**
  （不为负）。

**分摊方法**：记 `V(S)` 为基线叠加所选子集 `S` 后用同一套 RDP 网格算出的
投影 epsilon。对每个所选申请 `i`，穷举其余申请的全部子集
`S ⊆ N\\{i}`，按其加入各子集的边际增量与标准 Shapley 权重取平均：

```
phi_i = Σ_{S ⊆ N\\{i}} [ |S|! · (n−1−|S|)! / n! ] · ( V(S∪{i}) − V(S) )
```

由于 RDP 组合逐 order 可加、`V` 单调，每个边际增量与每份分摊都**非负**，且
Shapley 权重保证 **`Σ_i phi_i = V(N) − V(∅) = epsilon_increment`**（浮点舍入
误差内）。纯 ε-DP 申请在任何子集里的边际增量恒等于自身 epsilon，因此其分摊
精确等于自身 epsilon；近似 DP 申请之间则按 Shapley 权重公平分摊 RDP 转换的
保守余量。成功返回 `200`：

```json
{
  "dataset_id": "sales-2026",
  "generated_at": "2026-09-24T12:00:00.000000Z",
  "request_count": 3,
  "request_ids": ["req-20260924-0001", "req-20260924-0002", "req-20260924-0003"],
  "epsilon_cap": 1.0,
  "epsilon_baseline": 0.0,
  "epsilon_full_group": 0.3561844401133079,
  "epsilon_increment": 0.3561844401133079,
  "rdp_best_order_baseline": 8192.0,
  "rdp_best_order_full_group": 8192.0,
  "attributed_total_epsilon": 0.35618444011330785,
  "attributions": [
    {"request_id": "req-20260924-0001", "epsilon": 0.1, "delta": 1e-8,
     "attributed_epsilon": 0.10309222005665396},
    {"request_id": "req-20260924-0002", "epsilon": 0.2, "delta": 1e-8,
     "attributed_epsilon": 0.20309222005665392},
    {"request_id": "req-20260924-0003", "epsilon": 0.05, "delta": 0.0,
     "attributed_epsilon": 0.05}
  ]
}
```

字段说明：

* `epsilon_baseline` / `epsilon_full_group`：基线占用与全组选入占用，均附
  取得该投影的最优 RDP order（`rdp_best_order_baseline` /
  `rdp_best_order_full_group`）；
* `attributions[]`：按请求传入顺序逐项给出申请自身的 `epsilon` / `delta` 与
  Shapley 分摊值 `attributed_epsilon`；
* `attributed_total_epsilon`：逐项分摊之和，**在数值误差内等于
  `epsilon_full_group − epsilon_baseline`**（即 `epsilon_increment`）。

语义与边界：

* **严格只读、无幂等键**：本接口不写任何表（不产生申请、预留、批次、转拨或
  冲正记录），也**不触发预留惰性清扫**（与 release-forecast、selection plan
  同为纯 SELECT）；**每次调用都按实时账本重算**——账本被其他申请 / 预留 /
  冲正 / 转拨改变后，再次归因结果随之变化；
* 归因是对当前账本的一次性解释，不锁定额度、不改变任何申请状态，也不构成
  对未来准入的承诺；
* 数据集中不存在所选申请（含被其他数据集拥有的 `request_id`）→
  `404 request_not_found`；所选申请为 `rejected` →
  `409 request_not_granted`；所选申请**已冲正** →
  `409 request_already_reversed`（按请求顺序报告首个不合法 ID）；
* 数据集不存在 → `404 dataset_not_found`；
* `request_ids` 为空或超过 10 个、存在重复 ID、ID 为空或超长 → `422`。

### 4.19 重复工作负载容量试算（repeat-run trial，只读）

在**不创建任何申请 / 预留 / 裁定记录**的前提下，把一组固定分析项当作一次
「工作负载运行」，求当前账本下最多能完整重复运行多少次。占用基线与正式准入
完全一致：全部 **granted 且未冲正**的申请 + 全部**未到期** granted 预留（已
释放 / 已确认 / 已过期 / 被拒绝的预留不参与）。

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/repeat-run-trials \
  -H 'Content-Type: application/json' \
  -d '{
    "items": [
      {"epsilon": 0.02, "delta": 1e-8},
      {"epsilon": 0.01, "delta": 0.0}
    ],
    "max_runs": 1000
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `items` | array | 必填，**1～5 项**，整组构成一次运行 |
| `items[].epsilon` | float | `> 0, ≤ 1000`，分析项的 epsilon |
| `items[].delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |
| `max_runs` | integer | 必填，**1～100000**，考虑的运行次数上界 |

服务把整组各项与当前占用按既有 RDP 口径做**同一条**组合曲线（与批次、试算同
一张 orders 网格、同一浮点容差）。RDP 曲线逐 order 可加，因此把同一组重复
`runs` 次等价于把该组的纯 ε 偏移与近似 DP 曲线各乘以 `runs`——**纯 ε 项在任意
运行次数下都走精确加法**，不收取 RDP 转换余量。可行性（投影 epsilon
`≤ epsilon_cap + EPSILON_TOLERANCE`）随运行次数单调，因此用二分查找求
`[0, max_runs]` 内**最大的可行完整运行次数**；即使**零次**可行（连一次都放不
下）也正常返回。成功返回 `200`：

```json
{
  "dataset_id": "sales-2026",
  "generated_at": "2026-09-25T12:00:00.000000Z",
  "item_count": 2,
  "items": [
    {"epsilon": 0.02, "delta": 1e-8},
    {"epsilon": 0.01, "delta": 0.0}
  ],
  "max_runs": 1000,
  "epsilon_cap": 1.0,
  "epsilon_committed": 0.0,
  "max_complete_runs": 30,
  "at_max_runs": false,
  "projection": {
    "runs": 30,
    "epsilon_projected": 0.969153,
    "epsilon_remaining": 0.030847,
    "rdp_best_order": 8192.0,
    "feasible": true
  },
  "next_run_projection": {
    "runs": 31,
    "epsilon_projected": 1.001402,
    "epsilon_remaining": 0.0,
    "rdp_best_order": 8192.0,
    "feasible": false
  }
}
```

字段说明：

* `epsilon_committed`：试算时刻「未冲正已授权申请 + 未到期有效预留」的组合
  epsilon（与预算视图口径一致），即零次运行的占用；
* `max_complete_runs`：`[0, max_runs]` 内投影不超 cap 的**最大完整运行次数**，
  可能为 `0`（零次可行也会返回，此时 `projection` 即当前账本占用）；
* `at_max_runs`：最大次数是否已达到 `max_runs` 上界；
* `projection`：恰好运行 `max_complete_runs` 次后的投影占用（恒可行），含
  `epsilon_projected`、`epsilon_remaining`、取得该投影的最优 RDP order 与
  `feasible`；
* `next_run_projection`：再多运行一次（`max_complete_runs + 1` 次）后的投影；
  当 `at_max_runs = true`（上界内全部可行）时不再评估上界之外的运行，该字段为
  `null`；其余情形（含零次可行）都给出——零次可行时它就是运行一次的超额占用
  且 `feasible = false`。

语义与边界：

* **严格只读、无幂等键**：本接口不写任何表（不产生申请、预留、批次、转拨或
  冲正记录），也**不触发预留惰性清扫**（与 release-forecast、selection plan、
  cost attribution 同为纯 SELECT）；**每次调用都按最新账本重算**——账本被其他
  申请 / 预留 / 冲正 / 转拨改变后，再次试算可能得到不同次数；
* 分析项仅在试算内参与组合，不锁定任何额度，也不占用任何 `request_id` /
  `reservation_id` 命名空间；试算结果不构成对未来准入的承诺，正式提交仍按当时
  账本重新准入；
* 数据集不存在 → `404 dataset_not_found`；
* 分析项为 0 个或超过 5 个、`max_runs` 不在 `1..100000`（含小数）、
  `epsilon` / `delta` 越界等 → `422`。

### 4.20 冲正方案试算（reversal plan，只读）

在**不执行任何冲正或申请、不改变预留状态**的前提下，为一个**待申请分析项**
寻找「最少冲正哪几条已授权申请才能放得下」的方案。基线口径与正式准入完全一致：
除候选外的全部 **granted 且未冲正**申请 + 全部**未到期** granted 预留（已释放 /
已确认 / 已过期 / 被拒绝的预留不参与）。

```bash
curl -X POST http://localhost:8000/datasets/sales-2026/reversal-plans \
  -H 'Content-Type: application/json' \
  -d '{
    "item": {"epsilon": 0.45, "delta": 1e-8},
    "request_ids": ["req-20260924-0001", "req-20260924-0002"]
  }'
```

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `item` | object | 必填，待申请分析项（仅在试算内参与组合，不会真正提交） |
| `item.epsilon` | float | `> 0, ≤ 1000`，待申请项的 epsilon |
| `item.delta` | float | `>= 0, < 1`；`0` 表示纯 ε-DP |
| `request_ids` | array | 必填，**1～12 个**、**互不重复**的 `request_id`（每个 1–200 字符）；每个都必须是本数据集**已授权且未冲正**的申请 |

服务以「其余未冲正已授权申请 + 当前未到期 granted 预留」为**固定基线**，穷举
候选申请的**全部 2ⁿ 个冲正子集**（含空集，至多 2¹² = 4096 个），把每个子集的
「基线 + 剩余候选 + 待申请项」做**同一条** RDP 组合投影（与准入、批次及其他
试算同一张 orders 网格、同一浮点容差）；投影 `≤ epsilon_cap +
EPSILON_TOLERANCE` 的子集为可行。在全部可行子集中按以下顺序裁定：

1. **冲正条数最少**（待申请项本身就放得下时，空集最优，建议零条冲正）；
2. 条数相同 → **冲正后账本投影占用最大**（即释放预算最少、最保守的方案）；
3. 仍相同 → 按 **`request_id` 升序列表的字典序**取更小者。

成功返回 `200`：

```json
{
  "dataset_id": "sales-2026",
  "generated_at": "2026-09-25T10:00:00.000000Z",
  "item": {"epsilon": 0.45, "delta": 1e-8},
  "candidate_count": 2,
  "candidate_request_ids": ["req-20260924-0001", "req-20260924-0002"],
  "epsilon_cap": 1.0,
  "epsilon_committed": 0.6,
  "rdp_best_order_committed": 8192.0,
  "feasible": true,
  "reason": null,
  "suggested_request_ids": ["req-20260924-0001"],
  "reversal_count": 1,
  "epsilon_after_reversal": 0.4,
  "epsilon_with_pending": 0.85,
  "epsilon_remaining": 0.15,
  "rdp_best_order_after_reversal": 8192.0,
  "rdp_best_order_with_pending": 8192.0
}
```

字段说明：

* `epsilon_committed`：**冲正前**占用，即基线与全部候选（均仍在账）一起组合的
  投影 epsilon，与预算视图的实时占用一致；
* `suggested_request_ids`：建议冲正的 `request_id`，**按升序输出**；待申请项
  本身就放得下时为空列表（`reversal_count = 0`）；
* `epsilon_after_reversal`：**冲正后**（建议生效后、待申请项未计入）的账本投影
  占用；
* `epsilon_with_pending`：冲正后**再加入待申请项**的组合投影 epsilon，恒
  `≤ epsilon_cap + EPSILON_TOLERANCE`；`epsilon_remaining =
  epsilon_cap − epsilon_with_pending`（不为负）；
* `rdp_best_order_committed` / `rdp_best_order_after_reversal` /
  `rdp_best_order_with_pending`：取得各投影的最优 RDP order。

**无可行子集**（即使冲正全部候选也放不下）时仍返回 `200`，但
`feasible = false`、`reason` 说明原因，`suggested_request_ids` 为空，
`reversal_count` 与冲正后 / 加入待申请项后的占用及其 RDP order 字段均为
`null`（`epsilon_committed` 照常返回）。

语义与边界：

* **严格只读、无幂等键**：本接口**不执行冲正、不提交申请**、不写任何表，也
  **不触发预留惰性清扫**（与 release-forecast、selection plan、cost
  attribution、repeat-run trial 同为纯 SELECT）；**每次调用都按最新账本重算**——
  账本被其他申请 / 预留 / 冲正 / 转拨改变后，再次试算可能得到不同方案；
* 建议不构成承诺：客户端若采纳，仍需通过
  `POST …/requests/{request_id}/reversals` 逐条冲正、再正式提交申请（均按当时
  账本重新判定）；
* 候选申请不存在（含被其他数据集拥有的 `request_id`）→
  `404 request_not_found`；候选为 `rejected` → `409 request_not_granted`；候选
  **已冲正** → `409 request_already_reversed`（按请求顺序报告首个不合法 ID）；
* 数据集不存在 → `404 dataset_not_found`；
* `request_ids` 为空或超过 12 个、存在重复 ID、ID 为空或超长、`item` 缺失或
  `epsilon` / `delta` 越界等 → `422`。

---

## 5. RDP 组合会计（核算口径）

1. 每个申请被视为一个 `(ε, δ)`-DP 机制，转换为所有 Rényi order
   `α ∈ {1.1, 1.2, …, 10.9, 11, …, 63, 128, 256, 512, 1024, 2048, 4096, 8192}`
   上的 RDP 曲线：

   * 近似 DP（`δ > 0`）：`ε_rdp(α) = ε + log(1/δ) / (α − 1)`；
   * 纯 ε-DP（`δ = 0`）：`ε_rdp(α) = ε`（常数曲线）。

2. 组合（串行/并行同理）在每个 order 上逐点相加：
   `ε_rdp_total(α) = Σ_i ε_rdp_i(α)`。

3. 在数据集的目标 δ（`delta_cap`）处转换回 `(ε', δ)`-DP：

   ```
   ε'(δ) = min_α [ ε_rdp_total(α)
                   + (log((α−1)/α) + log((1−δ)/δ)) / (α−1) ]
   ```

4. 准入判定：把**当前累计曲线 + 本次申请/预留/整批分析项**投影后得到
   `ε_projected`，当且仅当 `ε_projected ≤ epsilon_cap + EPSILON_TOLERANCE`
   时授权/锁定。这里的「当前累计曲线」= 全部 `granted` 且**未冲正**的申请
   ∪ 全部**未过期** `granted` 预留；已释放/已过期预留与已冲正申请不参与组合
   （被冲正申请的原始行仍保留在 `requests` 中，只是存在对应
   `request_reversals` 行而被核算排除）。批次（batch）把本次的 **2～20 项**
   一次性追加到当前累计曲线上做**同一次**组合投影，然后整批判定：投影不超
   cap 才全部授权，否则一项也不授权。

5. 确认（confirm）只是把同一条 `(ε, δ)` 从「预留集合」移到「已授权申请
   集合」：确认前后的组合曲线完全相同，因此不发生二次扣减；释放与过期
   则从组合中移除该条预留，额度立即/自动归还。

6. 冲正（reversal）把一条已授权申请从「已授权申请集合」移除并另写不可变
   冲正记录：所有读 / 写路径的 granted 集合都统一排除带冲正记录的申请，
   原申请的历史 `epsilon_before` / `epsilon_after` 不被改写。批次裁定
   （含各项当时的累计快照）同样是**历史记录**：即使批次内某项事后被冲正，
   按 `batch_id` 重放 / 查询仍返回首次裁定与当时的组合值。

7. 试算（selection plan）**不改变上述任何集合**：它对 1～12 个候选穷举全部
   2ⁿ 个子集，每个子集与「未冲正已授权申请 ∪ 未到期预留」的当前组合曲线做
   **同一条** RDP 投影（纯 ε-DP 仍走精确可加分支），仅保留投影不超 cap 的
   可行子集并按「总 value → 投影 epsilon → 候选 ID 升序列表字典序」选优；
   全程纯 SELECT，不落任何记录、不做惰性清扫，重复调用按最新账本重算。

8. 成本归因（cost attribution）同样**不改变任何集合、纯 SELECT**：以「其余
   未冲正已授权申请 ∪ 当前未到期预留」为基线组合曲线，对所选 1～10 个申请
   穷举全部 2ⁿ 个子集的 RDP 投影 `V(S)`（与准入完全相同的口径与纯 ε 分支），
   再按标准 Shapley 权重对边际增量 `V(S∪{i}) − V(S)` 取平均，把全组增量
   `V(N) − V(∅)` 分摊到每个所选申请；组合曲线逐 order 可加保证各边际非负，
   Shapley 权重之和为 1，故逐项分摊之和恰等于全组与基线两次占用之差（舍入
   误差内）。

9. 重复工作负载试算（repeat-run trial）也**不改变任何集合、纯 SELECT**：把
   1～5 个分析项组成的一组在「未冲正已授权申请 ∪ 未到期预留」的当前组合曲线
   上重复叠加。由于 RDP 曲线逐 order 可加，重复 `runs` 次等于把该组的纯 ε
   偏移与近似 DP 曲线各乘以 `runs`（纯 ε 项始终走精确加法分支），可行性随
   `runs` 单调，故以二分查找求 `0..max_runs`（上界 100000）内投影不超 cap 的
   最大完整运行次数，并返回该次数与再运行一次（`max_complete_runs + 1`）两次
   投影；当最大次数已达 `max_runs` 时不评估上界之外的运行。零次可行也会返回。

10. 冲正方案试算（reversal plan）同样**不改变任何集合、纯 SELECT**：以「其余
    未冲正已授权申请 ∪ 当前未到期预留」为固定基线，穷举 1～12 个候选可冲正
    申请的全部 2ⁿ 个子集，把「基线 + 剩余候选 + 待申请项」用与准入完全相同
    的口径（含纯 ε 精确加法分支）投影，仅保留不超 cap 的可行子集，并按
    「冲正条数最少 → 冲正后账本投影占用最大 → `request_id` 升序列表字典序
    最小」裁定；不执行冲正或申请、不做惰性清扫，重复调用按最新账本重算。

特殊情形：

* 全部已授权申请与有效预留都是纯 ε-DP 时，直接采用精确的基本组合
  `ε_total = Σ ε_i`，不收取 RDP 转换的保守余量；
* 纯 DP 与近似 DP 混合时，纯 ε 之和作为常数偏移叠加到 RDP 曲线上再统一
  转换。
* 极小正 delta 的数值稳定：`log(1/δ)` 一律以 `-log(δ)` 在对数域求值，
  `δ` 小至最小正双精度（约 `5e-324`）时曲线仍为有限值；若先计算倒数
  `1/δ`，`δ < ~5.6e-309`（如 `1e-309`）会溢出为 `inf` 并污染整条曲线，
  使本可授权的申请被误判超额。单项申请、原子批次、预留（创建/确认/调整）
  与全部只读试算（selection plan、cost attribution、repeat-run trial、
  reversal plan、release forecast）共用这一条对数域曲线计算；已入账的
  裁定与同键重放仍按存储的历史值原样返回。

> 该口径使用的是教科书式、与机制无关的保守上界（Mironov 2017 的
> (ε,δ)-DP→RDP→(ε,δ) 界），不利用高斯机制等的更紧会计；优点是对任意
> `query_type` 都成立、实现简单可审计。`rdp_curve` 字段暴露各 order 的
> 累计 RDP 值，便于核对。

---

## 6. 并发与原子性

* 每个写操作（登记、提交申请、提交原子分析批次、创建/确认/释放/续期预留、
  epsilon 上限转拨、申请冲正）在进程内 `asyncio.Lock` 保护下打开一个 SQLite
  **`BEGIN IMMEDIATE`** 事务：「惰性过期清理 → 查重/查状态 → 重算（未冲正
  已授权申请 + 有效预留）组合消耗 → 判定 → 插入/更新记录」在同一个事务
  临界区内完成，并发操作被串行化，**申请与预留无论怎样并发交错，已授权
  （未冲正）+ 有效预留的组合消耗都不会超过 cap**；
* 批次事务在该临界区内一次完成「`batch_id` 查重与有序负载比对 → 批内 /
  已占用 `request_id` 校验 → 整批联合 RDP 投影 → 写批次记录（+ 批准时为
  每项写 granted 申请）」：批内任一 `request_id` 重复或已被占用、或整批投影
  超额时整笔回滚申请写入（超额时仅保留持久化的 rejected 批次记录），
  **绝不会出现部分项落库**；并发批次之间、批次与单项申请 / 预留 / 转拨 /
  冲正之间共用同一把写锁，组合消耗始终不超 cap；
* 冲正事务在该临界区内一次完成「幂等键查重 → 目标申请状态校验 → 移除前/
  移除后组合 epsilon 快照 → 写入不可变 request_reversals 记录」：冲正与
  申请 / 预留 / 转拨并发交错时，额度的释放与重新占用始终一致，同一条申请
  最多被冲正一次（`UNIQUE (dataset_id, request_id)` 兜底），同一
  `reversal_id` 全局唯一；
* 转拨事务在该临界区内一次完成「两端占用重算 → 正数/覆盖占用/不超 1000
  校验 → 两端 cap UPDATE → 写入 budget_transfers 记录」：校验失败时回滚
  （两个 cap 不变，仅留一条 rejected 记录），并发转拨与普通申请 / 预留
  交错也不会使来源预算透支；
* 预留的确认同样在该临界区内完成「插入 granted 申请 + 预留置
  confirmed」，两步原子，预算不可能被扣两次；
* 预留的续期在该临界区内一次完成「`renewal_id` 查重 → 惰性过期清扫与
  状态校验 → 90 天视野校验 → 更新 `expires_at` → 写入不可变
  reservation_renewals 记录」：续期与确认 / 释放 / 过期清扫并发交错时，
  到期时刻的移动与状态判定始终一致，同一 `(dataset_id, renewal_id)` 最多
  生效一次（`UNIQUE` 约束兜底），续期本身不重算组合、不重复占额；
* SQLite 启用 **WAL** 与 `busy_timeout`：读不阻塞写，写冲突排队等待
  而非立即报错；`(dataset_id, request_id)`、`(dataset_id, reservation_id)`、
  `(dataset_id, batch_id)` 与 `(dataset_id, renewal_id)` 上各有 `UNIQUE`
  约束作为最终防线；
* 拒绝不写入任何消耗，只留一条 `rejected` 记录，因此并发请求/预留中
  “先到先得、额满即拒”；
* TTL 过期为**惰性**处理：任何读写路径进入临界区时先把到期预留标记为
  `expired` 再计算额度，因此过期预留既不会继续占额，也不会被重复确认。
* 只读的候选组合试算（selection plan）与释放预测一样**不进入写临界区、不开启
  写事务**：仅用纯 SELECT 读取未冲正申请与未到期预留后在进程内穷举子集，
  与任何并发写互不阻塞，也绝不改变数据库状态。
* 只读的隐私成本归因（cost attribution）同样**不进入写临界区、不开启写事务、
  不做惰性清扫**：纯 SELECT 读取未冲正已授权申请与未到期预留后，在进程内
  穷举所选申请的全部子集并按 Shapley 权重分摊增量；与并发写互不阻塞，绝不
  改变数据库状态，每次调用都按实时账本重算。
* 只读的重复工作负载容量试算（repeat-run trial）同样**不进入写临界区、不开启
  写事务、不做惰性清扫**：纯 SELECT 读取未冲正申请与未到期预留后，在进程内以
  曲线倍乘 + 二分查找计算最大可行完整运行次数；与任何并发写互不阻塞，绝不改变
  数据库状态，每次调用都按实时账本重算。
* 只读的冲正方案试算（reversal plan）同样**不进入写临界区、不开启写事务、不做
  惰性清扫**：纯 SELECT 读取未冲正已授权申请与未到期预留后，在进程内穷举候选
  冲正子集并试算待申请项；与任何并发写互不阻塞，绝不改变数据库状态，每次调用
  都按实时账本重算。
* 备份命令（`python -m app.backup`，见第 8 节）通过 SQLite **在线备份 API**
  复制数据库：复制期间并发提交的写事务被安全地重拷，目标文件永远是事务一致
  的快照，绝不包含半笔事务；备份只读源库，不进入写临界区，也不阻塞服务的
  读写。

---

## 7. 数据库迁移

Schema 由 SQLite 的 `PRAGMA user_version` 管理，服务启动时自动顺序执行
待应用的迁移脚本（幂等 DDL，包在一个事务里），**无需人工干预**：

| 版本 | 内容 |
| --- | --- |
| 1 | `datasets`、`requests` 表（原始版本） |
| 2 | 新增 `reservations` 表（含状态、TTL、`expires_at` / `expired_at` / `confirmed_at` / `released_at` 等时间列与部分索引） |
| 3 | 新增 `budget_transfers` 表（全局唯一 `transfer_id`、来源/目标、金额、状态、两端 cap 转拨前后快照与占用、时间戳） |
| 4 | 新增 `request_reversals` 表（全局唯一 `reversal_id`、目标申请、原因、冲正前后组合 epsilon 快照与时间戳；`(dataset_id, request_id)` 唯一）。已有 granted 申请在显式冲正前核算口径不变 |
| 5 | 新增 `request_batches` / `request_batch_items` 表（`(dataset_id, batch_id)` 唯一、按提交顺序保存各项及累计组合快照；granted 批次的各项同时写入 `requests`，rejected 批次不写申请行）。已有申请 / 预留 / 冲正记录原样保留，核算口径不变 |
| 6 | 新增 `reservation_renewals` 表（`(dataset_id, renewal_id)` 唯一、目标预留、延长秒数、续期前后到期时刻与时间戳）。已有数据集、申请与预留记录原样保留，核算口径不变 |
| 7 | `reservations` 表新增 `original_epsilon` / `original_delta`（冻结创建时负载，旧行以当前额度回填），并新增 `reservation_adjustments` 表（`(dataset_id, adjustment_id)` 唯一、目标预留、调整前后额度、请求额度、裁定状态、调整前后组合 epsilon 快照与时间戳）。调整成功只改写预留行的实时额度列；已有申请、预留与续期记录原样保留 |

旧版本数据库（无 `user_version`，即 v0）启动时会先确保 v1 表存在再顺序升级
到最新版本，已有数据集、申请与预留记录原样保留。

---

## 8. 备份与恢复

### 8.1 在线备份（容器内执行）

服务运行期间即可在容器内执行备份命令，**无需停服**：

```bash
docker compose exec dp-ledger \
  python -m app.backup --output /data/backups/ledger-$(date +%Y%m%dT%H%M%S).db
```

* 数据源为当前 `DATABASE_PATH`（容器内默认 `/data/ledger.db`）；快照通过
  SQLite **在线备份 API** 流式复制——服务同时提交申请、预留或上限转拨也
  **不会复制出半笔事务**，目标文件始终是事务一致的快照；备份只读源库，
  不进入写临界区，也不阻塞服务的读写；
* 发布协议：先写入**同目录临时文件** → 对副本执行
  `PRAGMA integrity_check` 校验 → fsync 落盘 → 以**原子硬链接重命名**
  发布到 PATH；任一步失败都会清除临时文件，且 **PATH 上已有备份绝不
  被覆盖**（`os.link` 在目标已存在时原子失败，不存在检查与改名之间的
  竞态窗口）；
* PATH 必须尚不存在（防覆盖）；请把 PATH 放在持久卷 `/data` 下（如
  `/data/backups/`，目录不存在会自动创建），否则容器重建后备份随之
  丢失——命令检测到 PATH 不在数据库所在目录树下时会打印警告（不阻断）；
* 成功时退出码 `0`，stdout 打印备份路径与字节数；失败时退出码非 `0`，
  stderr 给出明确原因（源库缺失、目标已存在、完整性校验失败、目录不可
  写等）。

本地（无 Docker）运行同样可用：

```bash
export DATABASE_PATH=./ledger.db
python -m app.backup --output ./backups/ledger.db
```

### 8.2 恢复

备份文件是一个普通的事务一致 SQLite 数据库，恢复即「停服 → 放回
`DATABASE_PATH` → 重启」：

```bash
# 1. 停止服务（写操作停止，避免新旧数据混杂）
docker compose stop dp-ledger

# 2. 用备份覆盖数据库文件（同时清掉可能残留的 WAL/SHM 边车文件）
docker compose run --rm --no-deps dp-ledger sh -c \
  'cp /data/backups/ledger-20260925T120000.db /data/ledger.db && \
   rm -f /data/ledger.db-wal /data/ledger.db-shm'

# 3. 重启服务（启动时自动完成 WAL 恢复与 schema 迁移）
docker compose up -d dp-ledger
```

恢复后建议先访问 `GET /health` 与 `GET /datasets/{dataset_id}/budget`
核对账本内容。备份文件离线也可用
`sqlite3 /data/backups/xxx.db 'PRAGMA integrity_check;'` 校验。

---

## 9. 运行测试

```bash
pip install -r requirements-dev.txt
PYTHONPATH=. pytest -q
```

测试覆盖：登记/重复登记、授予/拒绝、拒绝不留痕、纯 ε-DP 精确可加、
RDP 数学性质、幂等回放（含拒绝回放）、`request_id` 冲突 409、缺失
数据集 404，以及 60 路并发申请不超预算、20 路重复 `request_id` 并发仅
一条被创建。极小正 delta（如 `1e-309`，`1/δ` 会浮点溢出）的回归测试
覆盖：单项申请正常授权且预算占用有限、同键重放原样返回，原子批次整批
授权，预留创建/确认按有限占用核算，只读 repeat-run 试算给出有限次数，
以及 `rdp.py` 层面曲线有限性、最小正双精度 delta、与纯 ε 项混合的精确
加法。

预留相关测试另覆盖：预留锁定/拒绝、预算视图、创建幂等与三种负载冲突
409、确认转申请且不双重扣减、确认幂等/冲突/终态 409、释放幂等与终态
409、TTL 过期在查询与新建时释放额度、60 路并发预留不超预算、15 路并发
确认仅一条申请、预留/确认/释放/直接申请混合并发下始终不超 cap，以及
从 v1 schema 启动时的自动迁移。

续期相关测试另覆盖：到期时刻按 `extend_seconds` 后移且金额 / 原始
`ttl_seconds` 不变、不重复占额，多次续期链式后移，同负载重放返回首次前后
到期时刻（含预留已过期后的重放）且只后移一次、异负载 / 跨预留同键 409、
`renewal_id` 按数据集隔离、缺失数据集 / 预留 404、confirmed / released /
rejected / expired 终态 409、90 天视野上限（恰好到界通过、超一秒 409 且
到期时刻不变）、负载非法 422、确认与只读释放预测按新到期时刻工作、续期后
的预留仍在新到期时刻才被惰性清扫、20 路重复 `renewal_id` 并发仅后移一次、
10 路不同 `renewal_id` 并发全部生效，以及从 v5 schema 启动时自动迁移出
`reservation_renewals` 表。

预留额度调整测试另覆盖：在 cap 内调大 / 调小 / 改 delta（含纯 ε ↔ 近似 DP）
后预算视图按新额度核算（并用 `rdp.py` 独立组合交叉校验）、调小释放的额度可被
新申请使用、其他有效预留与未冲正申请共同参与核算且超额时拒绝（拒绝留痕、额度
与到期时刻不变、不阻塞后续合法调整）、连续调整逐次替换、同负载重放返回首次
裁定与额度快照（含拒绝重放、预留已过期后的重放）且只替换一次、异额度 / 异
delta / 跨预留同键 409、`adjustment_id` 按数据集隔离、缺失数据集 / 预留
404、confirmed / released / rejected / expired 终态 409、负载非法 422、调整后
确认以新额度生成申请且不双重扣减、释放按新额度归还、释放预测按新额度工作、与
续期共存（额度变而到期时刻不变）、调整后的预留仍按原到期时刻被惰性清扫、原
预留创建请求仍按创建时负载幂等（按调整后负载重放为 409）、其他预留的入库快照
不被改写、20 路重复 `adjustment_id` 并发仅一条记录、10 路不同 `adjustment_id`
并发串行生效，以及从 v6 schema 启动时自动迁移出 `reservation_adjustments` 表
并回填 `original_epsilon` / `original_delta`。

只读释放预测测试覆盖：同刻到期分组移除与逐刻 RDP 重算（并用 `rdp.py`
独立核算交叉校验）、恰好在截止时刻到期的边界、早于任何到期的预测区间、
截止时仍有效预留继续计入、拒绝/已释放预留被忽略、非 UTC 带偏移时间归一
化、裸时间/过去时间/缺参数/坏格式返回 422、缺失数据集 404，以及预测
接口不改写预留状态（不触发惰性过期、重复调用结果一致）。

转拨测试另覆盖：两端 cap 原子增减且总量守恒、delta_cap/已入账申请不变、
已授权申请与有效预留均计入来源占用、来源转出后非正数/不足以覆盖占用/目标
超 1000 三类拒绝且两端 cap 不变（含恰好到边界的通过用例）、被拒不阻塞
后续合法转拨、来源/目标缺失 404、负载非法 422、相同负载重试返回原记录
（含被拒重试）、异负载 409、40 路并发转拨恰好不使来源变负、20 路转拨与
20 路普通申请混合并发不透支、20 路重复 `transfer_id` 并发仅一条记录，以及
从 v2 schema 启动时自动迁移出 `budget_transfers` 表。

冲正测试另覆盖：纯 ε / 近似 DP 冲正后预算按释放重算（并用 `rdp.py` 独立
核算交叉校验）、原申请历史字段不变而列表标明冲正、释放额度可被新申请使用
且原被拒申请重放仍回放原拒绝、冲正确认预留而来的申请释放额度且预留行保持
confirmed、其他有效预留继续计入并仍能阻止超额新申请、到期预测与上限转拨
的占用均反映释放、冲正 rejected 申请 / 二次冲正 / 申请缺失 / 数据集缺失 /
非法负载的 409 与 404 与 422、相同 `reversal_id` 同负载重放返回原记录且
只释放一次、异原因 / 异目标（含跨数据集全局键）409、原 `request_id` 同
负载重放不重新占额、冲正记录列表与单条查询、20 路重复 `reversal_id` 并发
仅一条记录、15 路不同 `reversal_id` 并发仅一条成功，以及从 v3 schema 启动
时自动迁移出 `request_reversals` 表。

原子批次测试另覆盖：2/20 项边界与 1/21 项 422、整批批准且各项累计快照与
`rdp.py` 独立组合核算交叉校验（含纯 ε-DP 可加）、整批超额时一项不落库
（无申请行、零占用、被拒 id 可复用）且 rejected 批次持久化可查、被拒不
阻塞后续合法批次、有效预留与批次联合计入、同负载重放（含在预算随后被占
用后）仍返回首次裁定与当时组合值、异负载 / 改顺序 / 增删项 409、批内重复
`request_id` 409 且无任何记录、`request_id` 被直接申请（含被拒申请）/
预留确认 / 先前已批准批次占用时 409 且无部分记录、单项接口对批次内申请的
重放 / 冲突语义保持不变、批次内申请可冲正且批次历史裁定不随之改变、缺失
数据集 / 批次 404、30 个批次并发不透支且被拒批次零残留、20 路重复
`batch_id` 并发仅一条记录、15 路共享 `request_id` 的批次并发仅一批成功，
以及从 v4 schema 启动时自动迁移出批次两张表。

候选组合试算测试另覆盖：空数据集全选、按总 value 而非项数 / epsilon 选优、
纯 ε-DP 背包组合、value 同分取更小投影 epsilon、value 与 epsilon 都同分按
候选 ID 升序列表字典序裁定、无可行非空子集时返回空组合与当前占用、当前已授权
申请与有效预留计入基线（并用 `rdp.py` 独立穷举交叉校验投影与选择结果）、已过期
预留与已冲正申请不参与、严格只读（不产生申请 / 预留 / 冲正记录、不触发惰性
清扫、重复调用结果稳定）、账本变化后再次试算按最新占用重算、12 项边界、缺失
数据集 404、0 / 13 项、重复 `candidate_id`、`value` 非正整数（含小数）与
epsilon/delta 越界 422。

隐私成本归因测试另覆盖：单申请分摊等于其相对基线的全部增量、逐项分摊之和等于
全组与基线两次占用之差（含混合纯 / 近似 DP 基线）、分摊结果与独立暴力 Shapley
穷举（直接调用 `rdp.py` 组合）逐项一致、纯 ε-DP 下分摊恰为自身 epsilon 且与
传入顺序无关、基线包含其他未冲正申请与未到期预留且全组占用等于预算视图的实时
占用、已冲正申请与已过期预留不参与且不被本接口清扫（状态保持 granted、重复调用
结果一致）、严格只读（不产生任何记录、占用不变）、账本变化后按最新占用重算、
10 项边界，以及缺失数据集 404、缺失申请 404、已拒绝 409 request_not_granted、
已冲正 409 request_already_reversed、空 / 11 项 / 重复 ID / 空 ID / 超长 ID
422。

重复工作负载容量试算测试另覆盖：纯 ε 单项 / 多项的精确可加与最大次数、近似
DP（含纯 / 近似混合组、含已存在占用）结果与直接调用 `rdp.py` 逐次组合的暴力
核算交叉校验（该次数、再多一次投影均一致）、零次可行（空数据集与已有占用两种）
照常返回零次投影与一次超额投影、达到 `max_runs` 上界时 `at_max_runs=true` 且
不再给出再跑一次投影、`max_runs=100000` 边界、当前已授权申请与未到期预留计入
基线、已过期预留与已冲正申请不参与、严格只读（不产生申请 / 预留 / 冲正记录、
不触发惰性清扫、重复调用结果稳定）、账本变化后按最新占用重算、恰好到 cap 的
第 N 次仍可行（含浮点容差）、1～5 项与混合组、5 项边界，以及缺失数据集 404、
0 / 6 项、`max_runs` 越界（0、100001、小数）与 epsilon/delta 越界 422。

冲正方案试算测试另覆盖：待申请项直接放得下时建议零条冲正、最少冲正条数优先、
同条数取冲正后账本投影占用最大者、完全同分按 `request_id` 升序列表字典序裁定、
需要多条冲正与冲正全部候选的情形、无可行子集明确返回不可行（方案字段为 null、
冲正前占用照常返回）、基线含其他未冲正申请与未到期预留且冲正前占用与预算视图
一致、混合纯 / 近似 DP 方案与 `rdp.py` 独立穷举交叉校验、纯 ε-DP 精确可加、
已过期预留不参与且不被本接口清扫（状态保持 granted、重复调用结果一致）、已冲正
申请不计入基线、严格只读（不产生任何记录、不执行冲正、占用不变）、账本变化后
按最新占用重算、12 个候选边界，以及缺失数据集 404、缺失申请 404、已拒绝 409
request_not_granted、已冲正 409 request_already_reversed、按请求顺序报告首个
不合法候选、空 / 13 个 / 重复 / 空 ID / 超长 ID 422、`item` 缺失或
epsilon/delta 越界 422。

备份命令测试另覆盖：快照内容与源库一致（表、行、`user_version`、完整性校验）、
写事务未提交的部分不进快照而提交后可见、持锁写事务期间备份照常成功、并发成对
提交（数据集 + 申请同一事务）下快照始终满足跨表计数一致、目标已存在时拒绝覆盖
且原文件字节不变（含发布阶段 `os.link` 原子防覆盖）、源库缺失与目标为目录的
明确报错、校验失败时临时文件清除且不产生输出、输出目录自动创建、退出码与
stderr 原因、`python -m app.backup` 子进程端到端调用（含重复运行拒绝覆盖）。

---

## 10. 项目结构

```
.
├── app/
│   ├── __init__.py
│   ├── config.py      # 环境变量配置
│   ├── backup.py      # python -m app.backup 在线备份命令
│   ├── rdp.py         # Rényi DP orders、组合与 (ε,δ) 转换
│   ├── schemas.py     # Pydantic 请求/响应模型
│   ├── db.py          # SQLite 连接、schema、数据访问
│   ├── ledger.py      # 原子准入控制 + 幂等业务逻辑
│   └── main.py        # FastAPI 路由与异常处理
├── tests/             # pytest 端到端 + RDP 单元测试
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```
