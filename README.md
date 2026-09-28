# 设备遥测接收 API（device-telemetry-ingest）

基于 **FastAPI + SQLite** 的本地设备遥测接收服务。设备按批次上报带递增序列号的遥测记录，批次可能乱序到达；服务为每台设备维护一个**滑动窗口**，实现乱序暂存、重复幂等丢弃、超时断档标记与断档重放，并在并发请求下保持状态一致。另支持**连续阈值告警**：按设备配置**版本化告警规则**（修改规则即追加递增版本、可指定生效序列号，历史版本与已生成告警永久冻结），对已提交记录按序检测连续越限并生成可查询告警；每条告警携带 `open`/`acknowledged` 状态，可通过**幂等确认接口**由确认人标记为已确认（记录确认人与确认时间，重复确认不改变首次确认信息）。

## 快速启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 启动后

| 项目 | 地址 |
|---|---|
| API 基础地址 | `http://localhost:8000/api/v1` |
| 交互式文档 (Swagger) | `http://localhost:8000/docs` |
| 健康检查 | `http://localhost:8000/health` |
| 服务信息/当前配置 | `http://localhost:8000/` |
| 端口 | `8000`（Compose 映射 `8000:8000`，可在 `docker-compose.yml` 修改） |

SQLite 数据库文件默认在容器内 `/data/telemetry.db`，Compose 通过命名卷 `telemetry-data` 持久化。升级到新镜像时，启动初始化会自动为旧库做幂等迁移，无需手动干预；但请仍按惯例在升级前备份卷内数据库：

- 旧库的 `alerts` 表自动补齐 `status`/`acknowledged_by`/`acknowledged_at` 列（历史告警视为 `open`）。
- **规则版本化迁移**：旧的宽表 `alert_rules`（参数直接存于规则行）会重建为“规则序列 + 不可变版本”结构——每条旧规则成为 **版本 1（自 seq 0 生效）**，原有的规则推进状态迁移到该版本，全部历史告警固定（pin）到版本 1；迁移结束做 `foreign_key_check` 校验，重复启动安全。

## 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TELEMETRY_DB_PATH` | `/data/telemetry.db` | SQLite 数据库文件路径 |
| `TELEMETRY_WINDOW_SIZE` | `1000` | 每设备滑动窗口大小：接受 `[next_expected, next_expected + window_size)` 内的序列号 |
| `TELEMETRY_GAP_TIMEOUT_SECONDS` | `30` | 缺口等待时间（秒），超时标记为断档 |
| `TELEMETRY_SWEEP_INTERVAL_SECONDS` | `5` | 后台扫描间隔（秒），无流量时也会判定断档 |
| `TELEMETRY_SWEEP_ENABLED` | `true` | 是否启用后台扫描（测试时可关闭，断档判定退化为访问时惰性触发） |
| `TELEMETRY_CURSOR_SECRET` | `telemetry-cursor-dev-secret` | 分页游标 HMAC 签名密钥；生产环境务必改为强随机值（`docker-compose.yml` 支持同名 shell 变量覆盖） |

## 核心语义

每台设备维护 `next_expected`（下一个期望的连续序列号），接收窗口为
`[next_expected, next_expected + window_size)`。设备首次出现时，以该批最小序列号锚定窗口起点。

| 到达序列号位置 | 处理 | 返回状态 |
|---|---|---|
| `seq == next_expected` | 落库为已提交，窗口前移，并连续冲刷已暂存的记录 | `committed` |
| 窗口内、`next_expected` 之后 | 暂存，等待前方缺口补齐 | `buffered` |
| 已存在相同序列号 | 幂等丢弃，不重复落库（批次整体重试安全） | `duplicate` |
| 窗口之下、属于已声明断档 | 作为补录数据填充断档 | `gap_filled` |
| 窗口之下、无记录、无断档、距窗口 ≤ window_size 且从未断档 | 窗口回锚（并发首批竞争导致锚点偏高时向下修正）后正常接收 | `committed`/`buffered` |
| 窗口之下、其余情况 | 过旧数据，忽略 | `stale` |
| 超出窗口上界 | 拒绝，客户端应在窗口前移后重试 | `out_of_window` |

**断档（gap）**：`next_expected` 与最小暂存序列号之间存在缺口，且持续超过
`TELEMETRY_GAP_TIMEOUT_SECONDS`，即标记断档：写入 `gaps` 表（状态 `open`），
`next_expected` 跳过缺口，暂存记录继续冲刷。断档由后台扫描器周期性判定，也会在每次请求时惰性判定。

**断档重放**：客户端查询断档 → 调用重放接口（状态转 `replaying`，返回缺失序列号区间）
→ 设备通过正常批次接口重发缺失序列号 → 逐条落为 `gap_filled` → 缺口全部补齐后断档自动转为 `recovered`。

**连续阈值告警**：每台设备可配置多条告警规则（payload 顶层数值字段 + `gt`/`lt` + 阈值 +
连续条数 N）。规则只在记录**首次转为 `committed`** 时按 seq 顺序推进——直接提交与一次提交
冲刷多条 `buffered` 记录都会逐条计算；`duplicate`/`stale`/`out_of_window`/`gap_filled`
不参与计数，因此重复批次不会重复计数，断档补录也不回溯重算。遇到**声明断档**或**字段缺失/
非数值**时连续计数中断（streak 清零并重新待命）。连续越限首次达到 N 条时生成一条告警，
同一连续区段不再重复生成；待计数中断（恢复正常）后，下一段连续越限才会再次触发。
规则创建前已提交的历史记录不回溯计算。

**告警规则版本化**：规则（rule series）创建后即拥有**版本 1（自 seq 0 生效）**。修改规则不是覆盖，而是**追加一个不可变新版本**：版本号在该规则内严格递增（1、2、3…），并可指定该版本的**生效序列号 `effective_seq`**（省略时取设备当前 `next_expected`，即在提交前沿切换）。某条记录首次转为 committed 时，引擎按该记录的 seq 原子选择唯一生效版本：

```sql
SELECT * FROM alert_rule_versions
WHERE rule_id = ? AND effective_seq <= :seq
ORDER BY effective_seq DESC LIMIT 1
```

数据库层的 `UNIQUE(rule_id, effective_seq)` 与规则内版本号唯一约束保证：同一 seq 下版本选择**唯一且无歧义**；`effective_seq` 必须**严格递增、且不得早于设备已提交前沿 `next_expected`**（允许指向仍在 buffered 区的 seq，但不允许回填已提交历史）。新版本从自己的生效 seq 开始、以**独立 streak（计数清零）**推进；被取代的旧版本的定义与状态（`last_seq`/`streak`/`alerted`）永久冻结。因此：

- **并发批次提交**、以及一次提交冲刷多条 buffered 记录时，每条记录都在同一 `BEGIN IMMEDIATE` 事务内按记录序号选择版本——修改与提交在 SQLite 写层串行化，要么某 seq 用旧版本、要么用新版本，绝不会半旧半新；两个并发修改也串行，后到者拿到下一个版本号且必须使用更大的生效序号（冲突返回 **409**）。
- **历史版本与已生成告警保持不变**：每条告警在生成时固定（pin）到产生它的版本（`rule_version_id` + `version_no`）并保留该版本的参数快照（field/op/threshold/consecutive），之后再如何修改规则都不会改写既有版本与告警行。
- **断档（断档）与版本**：声明断档会重置“仍可能推进记录”的版本（在跳转 seq 生效的版本及更晚版本）；已完全被取代的旧版本保持冻结。

**告警确认工作流**：每条告警生成时状态为 `open`，并带 `acknowledged_by`/`acknowledged_at`
两个空字段。运维方可按告警 ID 调用确认接口，将其置为 `acknowledged` 并记录**确认人标识**
与**服务端确认时间**。确认接口**幂等**：重复确认（无论是否换人）都返回当前记录，
首次确认人与确认时间**永不改变**。

**并发一致性**：每请求一个 SQLite 连接（WAL 模式 + `busy_timeout`），所有状态变更包裹在
`BEGIN IMMEDIATE` 写事务中，写操作在数据库层面串行化；同步端点运行在线程池中，
配合每序列号幂等，并发/重试/乱序下状态保持一致。告警状态与产生它的记录状态变更
在同一事务内提交。规则版本追加与记录评估使用同一写事务，按记录 seq 的版本选择因此
原子且唯一（直接提交与 buffered → committed 冲刷同理）。告警确认使用“仅当
`status = 'open'` 才更新”的条件 UPDATE，并发确认在数据库层串行化，恰好一个请求完成
首次确认，其余请求读到一致的已确认记录。

## API 一览

### 提交批次

```
POST /api/v1/batches
```

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}},
    {"seq": 2, "payload": {"temp": 21.6}}
  ]
}
```

响应（每条记录独立返回处理结果，HTTP 恒为 200，除非请求体校验失败）：

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "accepted": 2,
  "results": [{"seq": 1, "status": "committed"}, {"seq": 2, "status": "buffered"}],
  "window": {"next_expected": 2, "buffered_count": 1}
}
```

### 查询窗口状态

```
GET /api/v1/devices/{device_id}/window
```

返回 `next_expected`、窗口范围、暂存序列号列表、待判定缺口（含剩余等待时间）、
已声明断档列表与统计计数（committed/buffered/gap_filled/duplicates）。

### 一致性分页查询记录

```
GET /api/v1/devices/{device_id}/records?seq_from=1&seq_to=100&limit=50
GET /api/v1/devices/{device_id}/records?cursor=<next_cursor>
```

按 `seq` 升序做**键集分页**（keyset pagination），参数：

| 参数 | 说明 |
|---|---|
| `seq_from` / `seq_to` | 可选，序列号闭区间过滤（仅首页请求需要传入） |
| `limit` | 可选，每页条数，默认 100，最大 1000 |
| `cursor` | 可选，上一页响应返回的 `next_cursor`；传入后区间与页大小以游标为准 |

**一致性语义**：首次请求固定一个 SQLite rowid 高水位（snapshot 边界），并随不透明
`next_cursor` 一起返回；后续页沿用该高水位与查询区间。翻页期间到达的新批次、断档补录
（`gap_filled`）的 rowid 都大于高水位，因此**不会混入**本次记录集合；已存在的记录原地
更新（如 `buffered` → `committed`）不改变 rowid，**不会漏读**；键集翻页保证每条记录
**恰好返回一次**。集合成员固定，但记录字段（如 `status`）反映读取时的最新值。

**游标安全**：游标为 base64url(JSON) + HMAC-SHA256 签名，密钥取自
`TELEMETRY_CURSOR_SECRET`。以下情况均返回 **400**：签名不符或格式非法（篡改）、
跨设备复用、复用时更改 `seq_from`/`seq_to`/`limit`。与游标一致的重复参数允许传入。
未知设备返回 404；`seq_from > seq_to` 返回 400。

响应示例（无下一页时 `next_cursor` 为 `null`）：

```json
{
  "device_id": "dev-001",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}, "status": "committed", "received_at": 1758000000.12}
  ],
  "next_cursor": "eyJ2IjoxLC...9oZg.4f3b2a...9c"
}
```

### 查询断档

```
GET /api/v1/devices/{device_id}/gaps?status=open
```

`status` 可选过滤：`open` / `replaying` / `recovered`。

### 断档重放

```
POST /api/v1/devices/{device_id}/gaps/{gap_id}/replay
```

将断档标记为 `replaying` 并返回缺失区间，例如：

```json
{
  "gap": {"gap_id": 1, "start_seq": 4, "end_seq": 6, "status": "replaying", "...": "..."},
  "missing_ranges": [[4, 6]],
  "missing_count": 3
}
```

设备随后经 `POST /api/v1/batches` 重发缺失序列号即可补齐。

### 创建告警规则

```
POST /api/v1/devices/{device_id}/alert-rules
```

```json
{
  "field": "temp",
  "op": "gt",
  "threshold": 30.0,
  "consecutive": 3
}
```

`field` 为 payload 顶层数值字段名；`op` 取 `gt`（大于阈值越限）或 `lt`（小于阈值越限）；
`consecutive` 为触发告警所需的连续越限条数 N（≥ 1）。设备不存在也可先建规则
（设备由首个批次惰性创建），规则只评估创建之后新提交的记录。返回 `201`：

```json
{
  "rule_id": 1,
  "device_id": "dev-001",
  "created_at": 1758000000.12,
  "version_count": 1,
  "version_no": 1,
  "rule_version_id": 1,
  "field": "temp",
  "op": "gt",
  "threshold": 30.0,
  "consecutive": 3,
  "effective_seq": 0,
  "state": {"last_seq": null, "streak": 0, "alerted": false}
}
```

### 修改告警规则（追加新版本）

```
PUT /api/v1/devices/{device_id}/alert-rules/{rule_id}
```

```json
{
  "field": "temp",
  "op": "gt",
  "threshold": 35.0,
  "consecutive": 2,
  "effective_seq": 1000
}
```

请求体与创建相同，外加**可选** `effective_seq`（≥ 0）：新版本开始评估的第一条记录序号；
省略时取设备当前 `next_expected`（即“此刻起切换”）。服务端：

- 在该规则上追加**下一个递增版本号**（2、3、…），新版本以独立 streak 从 0 开始；
- 校验 `effective_seq` 在规则内**唯一且严格递增**，并且**不早于设备已提交前沿 `next_expected`**，
  否则返回 **409 Conflict**（历史/已暂存语义见上文“告警规则版本化”）；
- 规则不属于该设备（含不存在）返回 **404**。

返回 `200`，结构与创建响应一致（`version_count` 增加，字段为当前最新版本）。
**旧版本与已生成告警不受影响。**

### 告警规则列表

```
GET /api/v1/devices/{device_id}/alert-rules
```

返回该设备全部规则，每条规则展示其**当前（最新）版本**的参数与版本元数据
（`version_no`、`rule_version_id`、`effective_seq`、`version_count`）及该版本的实时状态：
`streak`（当前连续越限条数）、`alerted`（当前连续区段是否已触发告警）、
`last_seq`（最近参与计算的 committed 序列号）。

### 告警规则版本列表

```
GET /api/v1/devices/{device_id}/alert-rules/{rule_id}/versions
```

按版本号升序返回一条规则的**全部历史版本**（永久保留）。每个版本包含其不可变参数、
`effective_seq`、创建时间，以及**该版本各自的**冻结/实时状态（`last_seq`/`streak`/
`alerted`）。规则不属于该设备（含不存在）返回 **404**。

```json
{
  "device_id": "dev-001",
  "rule_id": 1,
  "versions": [
    {"version_no": 1, "rule_version_id": 1, "field": "temp", "op": "gt",
     "threshold": 30.0, "consecutive": 3, "effective_seq": 0,
     "created_at": 1758000000.12,
     "state": {"last_seq": 999, "streak": 0, "alerted": false}},
    {"version_no": 2, "rule_version_id": 2, "field": "temp", "op": "gt",
     "threshold": 35.0, "consecutive": 2, "effective_seq": 1000,
     "created_at": 1758000100.0,
     "state": {"last_seq": null, "streak": 0, "alerted": false}}
  ]
}
```

### 查询告警

```
GET /api/v1/devices/{device_id}/alerts
GET /api/v1/devices/{device_id}/alerts?rule_id=1
GET /api/v1/devices/{device_id}/alerts?status=open
GET /api/v1/devices/{device_id}/alerts?rule_id=1&version_no=2
GET /api/v1/devices/{device_id}/alerts?rule_version_id=2
GET /api/v1/devices/{device_id}/alerts?rule_id=1&status=acknowledged
```

`rule_id`、`status`、`version_no`、`rule_version_id` 均为可选过滤，可组合使用：

| 参数 | 说明 |
|---|---|
| `rule_id` | 仅返回该规则产生的告警 |
| `version_no` | 仅返回该规则**指定版本号**产生的告警；**必须与 `rule_id` 同时提供**（否则 400） |
| `rule_version_id` | 按全局唯一的规则版本 id 过滤（无需 `rule_id`） |
| `status` | `open`（未确认）/ `acknowledged`（已确认）；不传则保留**全部历史** |

每条告警记录越限区段的 `start_seq`/`end_seq`（end 为达到连续条数
的那条记录）、触发值 `trigger_value`、产生它的版本（`rule_version_id`/`version_no`）、
规则快照（field/op/threshold/consecutive）
以及确认工作流字段（`status`、`acknowledged_by`、`acknowledged_at`）。
过滤只筛选行，响应始终保留告警的全部字段：

```json
{
  "device_id": "dev-001",
  "alerts": [
    {
      "alert_id": 1,
      "rule_id": 1,
      "rule_version_id": 1,
      "version_no": 1,
      "device_id": "dev-001",
      "field": "temp",
      "op": "gt",
      "threshold": 30.0,
      "consecutive": 3,
      "start_seq": 1,
      "end_seq": 3,
      "trigger_value": 33.0,
      "created_at": 1758000000.34,
      "status": "open",
      "acknowledged_by": null,
      "acknowledged_at": null
    }
  ]
}
```

### 确认告警

```
POST /api/v1/devices/{device_id}/alerts/{alert_id}/acknowledge
```

请求体（`acknowledged_by` 为必填的确认人标识，1–128 字符）：

```json
{"acknowledged_by": "ops-alice"}
```

- 首次确认：告警 `open` → `acknowledged`，记录确认人与服务端确认时间，返回 `200` 与完整告警。
- **重复确认幂等**：对同一告警再次调用（即使换成另一个确认人）仍返回 `200` 与当前记录，
  首次确认的 `acknowledged_by`/`acknowledged_at` 不会被修改。
- **并发安全**：多个确认请求同时到达时，`BEGIN IMMEDIATE` 事务与条件更新保证恰好一次
  首次确认生效，所有响应的确认人/确认时间一致。
- 告警 ID 不属于该设备（含不存在）返回 `404`；请求体校验失败返回 `422`。

确认后响应示例：

```json
{
  "alert_id": 1,
  "rule_id": 1,
  "device_id": "dev-001",
  "field": "temp",
  "op": "gt",
  "threshold": 30.0,
  "consecutive": 3,
  "start_seq": 1,
  "end_seq": 3,
  "trigger_value": 33.0,
  "created_at": 1758000000.34,
  "status": "acknowledged",
  "acknowledged_by": "ops-alice",
  "acknowledged_at": 1758000012.80
}
```

### 其他

- `GET /api/v1/devices` — 设备列表
- `GET /health` — 健康检查
- `GET /` — 服务信息与当前配置

## 完整流程示例

```bash
# 1. 乱序提交：1,2,3,5,6（缺口 4）
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001",
  "records": [{"seq":1,"payload":{}},{"seq":2,"payload":{}},{"seq":3,"payload":{}},
              {"seq":5,"payload":{}},{"seq":6,"payload":{}}]
}'
# -> 1,2,3 committed；5,6 buffered

# 2. 查询窗口：pending_gap 显示缺口 4 及剩余等待时间
curl localhost:8000/api/v1/devices/dev-001/window

# 3. 超过 GAP_TIMEOUT 后，断档被标记（后台扫描或下次访问时）
curl localhost:8000/api/v1/devices/dev-001/gaps
# -> gap_id=1, start=4, end=4, status=open

# 4. 请求重放
curl -X POST localhost:8000/api/v1/devices/dev-001/gaps/1/replay
# -> status=replaying, missing_ranges=[[4,4]]

# 5. 设备重发 seq 4
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001", "records": [{"seq":4,"payload":{"temp":21.7}}]
}'
# -> status=gap_filled；断档自动 recovered

# 6. 一致性分页拉取记录（第一页，固定高水位快照）
curl 'localhost:8000/api/v1/devices/dev-001/records?seq_from=1&seq_to=100&limit=2'
# -> {"records": [...seq 1, 2...], "next_cursor": "eyJ2Ijox..."}

# 7. 后续页只带 cursor；期间新到的批次/补录不会混入本次结果
curl 'localhost:8000/api/v1/devices/dev-001/records?cursor=eyJ2Ijox...'
# -> 翻页直至 next_cursor 为 null，区间内每条记录恰好返回一次

# 8. 创建连续阈值告警规则：temp 连续 3 条 > 30 触发
curl -X POST localhost:8000/api/v1/devices/dev-001/alert-rules \
  -H 'Content-Type: application/json' \
  -d '{"field":"temp","op":"gt","threshold":30,"consecutive":3}'
# -> 201 {"rule_id": 1, ..., "state": {"streak": 0, "alerted": false}}

# 9. 上报连续越限数据；达到 3 条时生成一条告警
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001",
  "records": [{"seq":7,"payload":{"temp":31}},{"seq":8,"payload":{"temp":32}},
              {"seq":9,"payload":{"temp":33}}]
}'
curl localhost:8000/api/v1/devices/dev-001/alerts
# -> alerts[0]: start_seq=7, end_seq=9, trigger_value=33.0, status=open
#    同一连续区段不再重复告警；恢复正常（未越限/字段缺失/声明断档）后才可再次触发

# 10. 确认告警：记录确认人与确认时间；重复/并发确认幂等，首次确认信息不变
curl -X POST localhost:8000/api/v1/devices/dev-001/alerts/1/acknowledge \
  -H 'Content-Type: application/json' \
  -d '{"acknowledged_by":"ops-alice"}'
# -> 200 {"alert_id": 1, "status": "acknowledged",
#         "acknowledged_by": "ops-alice", "acknowledged_at": 1758000012.8, ...}
# 仅看待确认 / 已确认告警：
curl 'localhost:8000/api/v1/devices/dev-001/alerts?status=open'
curl 'localhost:8000/api/v1/devices/dev-001/alerts?status=acknowledged'

# 11. 查看规则实时状态（当前最新版本及其 streak/alerted）
curl localhost:8000/api/v1/devices/dev-001/alert-rules

# 12. 修改规则 -> 追加版本 2，自 seq 100 起生效（阈值 35、连续 2 条）；
#     版本 1 及上面的历史告警永久不变
curl -X PUT localhost:8000/api/v1/devices/dev-001/alert-rules/1 \
  -H 'Content-Type: application/json' \
  -d '{"field":"temp","op":"gt","threshold":35,"consecutive":2,"effective_seq":100}'
# -> 200 {"version_no": 2, "effective_seq": 100, "version_count": 2,
#         "state": {"streak": 0, "alerted": false}, ...}

# 13. 列出全部历史版本（旧版本定义与冻结状态仍可查询）
curl localhost:8000/api/v1/devices/dev-001/alert-rules/1/versions

# 14. 仅查看版本 1 / 版本 2 产生的告警
curl 'localhost:8000/api/v1/devices/dev-001/alerts?rule_id=1&version_no=1'
curl 'localhost:8000/api/v1/devices/dev-001/alerts?rule_id=1&version_no=2'
# 生效序号早于已提交前沿（或与既有版本重复）返回 409：
curl -X PUT localhost:8000/api/v1/devices/dev-001/alert-rules/1 \
  -H 'Content-Type: application/json' \
  -d '{"field":"temp","op":"gt","threshold":35,"consecutive":2,"effective_seq":50}'
# -> 409
```

## 项目结构

```
app/
  config.py   # 环境变量配置
  db.py       # SQLite 连接、建表、写事务（WAL + BEGIN IMMEDIATE）与启动迁移（规则版本化）
  window.py   # 滑动窗口核心引擎（提交/暂存/断档/重放/回锚）
  alerts.py   # 版本化连续阈值告警引擎（版本追加/按 seq 原子选版/规则推进/断档中断/告警生成）
  cursor.py   # HMAC 签名的不透明分页游标（高水位 + 键集位置）
  models.py   # 请求/响应模型
  main.py     # FastAPI 应用、路由、后台断档扫描器
tests/
  test_api.py          # 乱序/幂等/断档/重放/回锚/并发一致性测试
  test_records.py      # 一致性分页：快照隔离/游标篡改/跨设备/区间变更测试
  test_alerts.py       # 连续阈值告警：规则 CRUD/连续计数/断档中断/补录不重算/确认工作流/迁移测试
  test_rule_versions.py # 规则版本化：版本切换/按 seq 唯一选版/历史冻结/迁移/真实服务并发测试
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：顺序提交、乱序暂存与冲刷、重复幂等（含批内重复）、窗口外拒绝、
断档超时标记、重放恢复（含多序列号部分补齐）、窗口回锚、设备自举，
8 线程并发提交 200 条记录后的状态一致性校验（真实 uvicorn 服务），
以及一致性分页：多页不重不漏、翻页期间新批次/断档补录不混入、原地状态变更不漏读、
游标篡改/跨设备/更改区间复用均返回 400。
连续阈值告警：规则创建/校验/列表、gt/lt 越限判定、首次达 N 条生成告警且同区段不重复、
恢复正常后再次触发、字段缺失/非数值中断计数、buffered 冲刷逐条按序计算、
重复批次不重复计数、stale/out_of_window 不参与、声明断档中断连续计数、
断档补录不回溯重算、规则创建前历史不回溯、多规则独立评估。
告警确认工作流：新告警默认 open 且确认字段为空、首次确认记录确认人与时间且保留全部
历史字段、重复确认幂等且不改首次确认信息（换人也不可覆盖）、按状态筛选（可与 rule_id
组合，非法值 422）、未知/跨设备告警 404、请求体校验、旧版 SQLite 库启动自动迁移，
以及真实 uvicorn 服务下 16 并发确认单条告警时唯一确认人胜出、所有响应与落库状态一致。
告警规则版本化：创建即版本 1（自 seq 0 生效）、修改追加递增版本号并可指定生效序号、
省略生效序号默认取 `next_expected`、直接提交与 buffered→committed 冲刷均按记录 seq
选择唯一生效版本、新版本独立 streak、历史版本定义/状态与已生成告警冻结不变、
生效序号重复/非递增/早于提交前沿返回 409、未知或跨设备规则 404、版本列表与按
`version_no`/`rule_version_id` 筛选告警（`version_no` 缺 `rule_id` 返回 400）、
断档只重置仍在推进的版本而不改动已被取代版本、旧版宽表规则库（含缺失状态表、
孤儿告警）启动自动迁移到版本化结构且幂等；真实 uvicorn 服务下：12 线程并发修改
（含 16 并发同一生效序号恰好一个成功）版本号连续、生效序号唯一；8 线程分片并发
提交 1..200 时三个版本在各自 seq 区间产出确定且分区不重叠的告警；5 个切换线程与
提交线程持续竞争时每个 seq 恰好解析到一个版本、无告警跨越版本边界。
