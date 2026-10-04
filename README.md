# 冷链温区异常归因 API

基于 **FastAPI + SQLite** 的冷链运输温度异常检测与归因服务。客户端提交设备温度
采样序列及按时间排列的装卸/运输区段，服务逐区段按上下限检测连续越界，容忍短暂
回落、跨采样缺口自动拆段，并把每个异常归因到**异常区间内实际采样时刻数量最多**
的区段；历史回放始终依据保存的原始采样与区段重算，旧口径写入的归因会被自动修正。

- 🔍 连续越界检测：越界过程中的短暂回落不切断异常
- ✂️ 缺口拆段：采样间隔达到阈值即拆成独立异常
- 🧭 跨区段合并归因：异常可跨越区段边界，按**异常区间内实际采样时刻数量**归因
  （平局取峰值所在区段，再平取最早区段）
- 🔁 批次幂等：相同 `batch_id` 重复提交直接回放（含并发安全）；回放依据原始
  采样重算，不盲信历史异常行，旧口径误归因自动修正
- 🔒 处置绑定快照：分析响应携带 `analysis_digest`（重算后异常规范 JSON 的
  SHA-256）；处置复核必须原样回传，服务在同一 SQLite 事务内重算比对，
  快照过期返回 `409`
- 🧾 运输联单风险快照：按交接顺序提交批次清单（批次 + 摘要 + 案件 + 交接
  时间），服务在同一事务内逐批次重算核对快照、确认案件覆盖当前异常，
  全部通过才落库不可变联单，给出 `released`/`held` 结论；存在 open 案件
  即 held
- 🌡️ 设备温度校准档案：提交按时间排序、互不重叠的 `[start,end)` 生效区间
  （有限正 `scale`/`offset`），分析可选按档案以 `corrected=raw*scale+offset`
  校准后再检测；档案标识与内容进入请求哈希与 `analysis_digest`，历史回放、
  处置复核与联单核对始终基于同一份不可变档案
- 🗄️ SQLite 持久化（WAL 模式），无外部服务依赖
- 📦 本地依赖：所有 wheel 在 `vendor/` 内，镜像构建**不需要访问外网 PyPI**

## 单命令启动

```bash
docker compose up --build -d
```

启动后：

| 地址 | 说明 |
| --- | --- |
| http://localhost:8000/docs | Swagger 交互文档 |
| http://localhost:8000/health | 健康检查 |
| http://localhost:8000/api/v1/batches/analyze | 提交分析（POST） |
| http://localhost:8000/api/v1/batches/{batch_id} | 回放批次结果（GET） |
| http://localhost:8000/api/v1/calibration-profiles | 设备温度校准档案（POST） |
| http://localhost:8000/api/v1/cases/review | 处置复核提交（POST） |
| http://localhost:8000/api/v1/shipments/snapshot | 运输联单风险快照（POST） |

停止：`docker compose down`（数据保留在命名卷 `coldchain-data`）。

## 检测规则

1. 采样点按时间戳落入唯一区段，采用该区段的 `lower_limit / upper_limit`。
   相邻区段按 **`[start, end)` 半开区间**划分时间轴（最后一个区段右端闭合），
   交界时刻无歧义地属于后一个区段。
2. 温度超出 `[下限, 上限]`（边界值含）即为越界；连续越界构成一条异常。
3. **短暂回落不切断**：越界中出现正常采样，只要距最后一个越界点不超过
   `dip_tolerance_seconds`（默认 120s），随后再次越界则回落被收回，异常合并为一段。
4. **跨采样缺口拆段**：相邻采样间隔达到 `gap_seconds`（默认 600s）即视为缺口，
   缺口两侧拆成独立异常（缺口判定优先于回落容忍）。
5. 异常可跨越区段边界。归因规则：
   1. 异常时间区间 `[start, end]` 内**实际落入该区段的采样时刻数量**最多者
      （含被收回的短暂回落点；按采样点数而非区间重叠秒数，采样稀疏度不会
      改变归因）；
   2. 数量相同取**峰值所在区段**（峰值＝偏离自身温度带幅度最大的越界点）；
   3. 仍相同取开始时间最早的区段。
6. 不被任何区段覆盖的采样点不参与检测，并切断其两侧异常连续性。
7. 同一时间戳的重复采样保留最后一个温度值。

## API 示例

### 提交分析

```bash
curl -X POST http://localhost:8000/api/v1/batches/analyze \
  -H 'Content-Type: application/json' \
  -d '{
    "batch_id": "TRIP-001",
    "device_id": "reefer-A-17",
    "gap_seconds": 600,
    "dip_tolerance_seconds": 120,
    "samples": [
      {"timestamp": "2026-09-22T08:00:00Z", "temperature": 5.2},
      {"timestamp": "2026-09-22T08:01:00Z", "temperature": 9.4},
      {"timestamp": "2026-09-22T08:02:00Z", "temperature": 4.8},
      {"timestamp": "2026-09-22T08:03:00Z", "temperature": 10.1}
    ],
    "segments": [
      {"segment_id": "load-1", "segment_type": "loading",
       "start": "2026-09-22T08:00:00Z", "end": "2026-09-22T08:10:00Z",
       "lower_limit": 0.0, "upper_limit": 8.0}
    ]
  }'
```

请求字段：

| 字段 | 说明 |
| --- | --- |
| `batch_id` | 客户端批次号（必填），重复提交用于幂等回放 |
| `device_id` | 设备标识 |
| `profile_id` | 设备温度校准档案号（**可选**）；指定后先按档案校准再检测，不指定保持旧口径 |
| `samples` | `{timestamp, temperature}` 列表，时间戳为 ISO-8601；不带时区按 UTC。`temperature` 为设备原始读数，指定档案时按 `corrected=raw*scale+offset` 校准 |
| `segments` | 按时间排列的区段，`segment_type` 为 `loading`/`unloading`/`transport`；`segment_id` 可省略（自动生成 `seg-1`…）；区段不可重叠 |
| `gap_seconds` | 采样缺口阈值，默认 600 |
| `dip_tolerance_seconds` | 短暂回落容忍秒数，默认 120（不可大于 `gap_seconds`） |

响应（08:02 的回落 4.8℃ 未切断异常）：

```json
{
  "batch_id": "TRIP-001",
  "device_id": "reefer-A-17",
  "profile_id": null,
  "cached": false,
  "anomaly_count": 1,
  "anomalies": [
    {
      "seq": 0,
      "start": "2026-09-22T08:01:00.000Z",
      "end": "2026-09-22T08:03:00.000Z",
      "peak_temperature": 10.1,
      "peak_timestamp": "2026-09-22T08:03:00.000Z",
      "direction": "high",
      "peak_segment_id": "load-1",
      "attributed_segment_id": "load-1",
      "attributed_segment_type": "loading",
      "duration_seconds": 120.0,
      "segment_coverage_seconds": {"load-1": 120.0},
      "segment_sample_counts": {"load-1": 3}
    }
  ],
  "analysis_digest": "a1f9…（64 位十六进制 SHA-256，下文示例用 $DIGEST 代指）"
}
```

异常字段说明：

| 字段 | 说明 |
| --- | --- |
| `seq` | 批次内异常序号（从 0 起）；处置复核按该序号逐条给结论 |
| `start` / `end` | 合并后的异常区间（首个/末个越界采样时刻，UTC） |
| `peak_temperature` / `peak_timestamp` | 峰值温度及其时刻 |
| `direction` | `high` 超上限 / `low` 低下限 |
| `peak_segment_id` | 峰值发生时所属区段 |
| `attributed_segment_id` / `attributed_segment_type` | 归因区段 |
| `duration_seconds` | 持续秒数（`end - start`） |
| `segment_coverage_seconds` | 异常区间与每个区段的覆盖秒数（区间交集，仅展示用，按秒数降序） |
| `segment_sample_counts` | 异常区间内实际落入各区段的采样时刻数量（**跨段归因依据**，按数量降序） |

顶层 `analysis_digest` 是服务端对**重算后异常规范 JSON**（与历史回放同一口径，
键排序的紧凑 JSON）计算的 SHA-256。POST 与 GET 都会返回，且只要原始采样、
区段与校准档案不变，该值稳定一致。处置复核必须回传这个摘要，把处置结论
绑定到一份确定的分析快照。**使用校准档案时**，摘要输入为
`{"anomalies": 异常规范 JSON, "profile": {profile_id, 区间与系数}}`——
`profile_id` 与档案内容都参与摘要；未指定档案时保持旧口径（仅异常 JSON），
历史调用的摘要值不变。

### 跨区段归因示例（采样数 ≠ 重叠秒数）

装货区段 `[08:00, 08:10)`、运输区段 `[08:10, 08:20)`，同一连续越界
`08:09:50–08:11:40`：装货侧时间跨度只有 10 秒却有 3 个越界采样，运输侧跨度
100 秒但只有 1 个采样。旧口径按重叠秒数会归因运输；当前口径按实际采样时刻
数量（装货 3 > 运输 1）归因装货：

```json
{
  "start": "...08:09:50.000Z",
  "end": "...08:11:40.000Z",
  "attributed_segment_id": "load-1",
  "duration_seconds": 110.0,
  "segment_coverage_seconds": {"transport-1": 100.0, "load-1": 10.0},
  "segment_sample_counts": {"load-1": 3, "transport-1": 1}
}
```

### 幂等与历史回放语义

- 相同 `batch_id` + **相同请求体**重复 POST：返回 `cached: true`、响应头
  `X-Cache: HIT`，不会产生新记录。
- **历史回放始终重算**：命中批次时不直接读取历史异常行，而是用入库时保存的
  **原始采样与区段**按当前归因口径重新计算。按旧口径（重叠秒数）写入的错误
  归因会在回放时返回修正结果，同时原子覆盖库中旧异常行；原始请求（`request_json`、
  `request_hash`）不变，因此相同请求依旧幂等。
- 相同 `batch_id` + **不同请求体**：返回 `409 Conflict`（防止批次号被误用为不同数据）。
- 并发提交同一批次：SQLite 唯一约束保证只入库一次，其余请求自动重算回放。
- `GET /api/v1/batches/{batch_id}` 随时回放历史结果（同样重算修正）；不存在返回 `404`。

## 设备温度校准档案

设备探头可能存在系统偏差，需要按校准档案把原始读数换算为实际温度后再检测。
档案接口为 `POST /api/v1/calibration-profiles`：调用方提交 `device_id`、
`profile_id` 以及按时间排序、互不重叠的 `[start, end)` 生效区间，每段含
**有限的正数** `scale` 与 `offset`（允许首尾相接）。档案一旦写入即不可变：

- 相同 `profile_id` + **相同内容**重复提交：`cached: true`、`X-Cache: HIT`
  幂等回放，不产生新记录；
- 相同 `profile_id` + **不同内容**（换设备、改任一区间边界或系数）：`409`，
  原档案保持不变。

```bash
curl -X POST http://localhost:8000/api/v1/calibration-profiles \
  -H 'Content-Type: application/json' \
  -d '{
    "profile_id": "CAL-2026-Q3-007",
    "device_id": "reefer-A-17",
    "intervals": [
      {"start": "2026-09-01T00:00:00Z", "end": "2026-09-22T12:00:00Z",
       "scale": 1.00, "offset": 0.50},
      {"start": "2026-09-22T12:00:00Z", "end": "2026-10-01T00:00:00Z",
       "scale": 1.02, "offset": 0.10}
    ]
  }'
```

成功响应（区间按开始时间排序后原样返回）：

```json
{
  "profile_id": "CAL-2026-Q3-007",
  "device_id": "reefer-A-17",
  "cached": false,
  "intervals": [
    {"start": "2026-09-01T00:00:00.000Z", "end": "2026-09-22T12:00:00.000Z",
     "scale": 1.00, "offset": 0.50},
    {"start": "2026-09-22T12:00:00.000Z", "end": "2026-10-01T00:00:00.000Z",
     "scale": 1.02, "offset": 0.10}
  ]
}
```

### 带档案的分析

分析请求可选携带 `profile_id`。服务在**同一 SQLite 事务**内读取该设备的
不可变档案，按每个采样时刻选取 `[start, end)` 区间，以
`corrected = raw * scale + offset` 换算为校准温度后再执行现有越界检测；
响应中的异常峰值等温度一律使用**校准值**，并回显 `profile_id`：

```bash
curl -X POST http://localhost:8000/api/v1/batches/analyze \
  -H 'Content-Type: application/json' \
  -d '{
    "batch_id": "TRIP-CAL-001",
    "device_id": "reefer-A-17",
    "profile_id": "CAL-2026-Q3-007",
    "samples": [
      {"timestamp": "2026-09-22T08:00:00Z", "temperature": 8.0},
      {"timestamp": "2026-09-22T08:01:00Z", "temperature": 5.0},
      {"timestamp": "2026-09-22T08:02:00Z", "temperature": 9.0}
    ],
    "segments": [
      {"segment_id": "load-1", "segment_type": "loading",
       "start": "2026-09-22T08:00:00Z", "end": "2026-09-22T08:10:00Z",
       "lower_limit": 0.0, "upper_limit": 8.0}
    ]
  }'
```

08:00 的原始读数 8.0℃ 恰好在上限，但按第一段系数校准后为 8.5℃（越界）；
08:02 的 9.0℃ 校准为 9.5℃，成为峰值。若不带 `profile_id`，同一批采样
按原始口径只会有一个 08:02 的单点异常。

```json
{
  "batch_id": "TRIP-CAL-001",
  "device_id": "reefer-A-17",
  "profile_id": "CAL-2026-Q3-007",
  "cached": false,
  "anomaly_count": 1,
  "anomalies": [
    {
      "seq": 0,
      "start": "2026-09-22T08:00:00.000Z",
      "end": "2026-09-22T08:02:00.000Z",
      "peak_temperature": 9.5,
      "peak_timestamp": "2026-09-22T08:02:00.000Z",
      "direction": "high",
      "peak_segment_id": "load-1",
      "attributed_segment_id": "load-1",
      "attributed_segment_type": "loading",
      "duration_seconds": 120.0,
      "segment_coverage_seconds": {"load-1": 120.0},
      "segment_sample_counts": {"load-1": 3}
    }
  ],
  "analysis_digest": "…（输入含 profile 与档案内容的 SHA-256）"
}
```

档案语义与错误码：

- **任一采样无覆盖 `422`**：采样时刻不被档案的任何 `[start, end)` 区间覆盖
  （含落在区间右端点上的情形），整批拒绝、批次与异常均不落库，错误详情
  指明首个无覆盖的采样时刻。
- **档案缺失 / 不匹配 `409`**：`profile_id` 不存在，或档案属于其他
  `device_id`；同样整批拒绝、不落库。
- **档案进入请求哈希**：相同 `batch_id` 的两次提交，一个带档案、一个不带，
  或指向不同内容的档案，都按"同号不同内容"返回 `409`。
- **档案进入 `analysis_digest`**：摘要把 `profile_id` 与档案内容（区间边界、
  scale、offset）一并纳入，因此处置复核与运输联单核对能识别出档案口径
  是否变化——换档案内容后旧摘要立即过期（`409`），让历史回放、处置复核
  与联单核对始终重放同一结果。
- **历史回放**：GET/幂等 POST/复核/联单在事务内重算时，会重新读取批次
  保存的 `profile_id` 对应的不可变档案再校准；档案已删除（正常不会发生，
  档案不可变）或设备不匹配时返回 `409`。
- **旧调用保持不变**：不指定 `profile_id` 的请求与响应（`profile_id: null`）
  完全沿用旧口径，峰值为原始温度，`request_hash` 与 `analysis_digest`
  与升级前逐字节一致。

## 处置复核（绑定分析快照）

处置接口为 `POST /api/v1/cases/review`。客户端先做分析、拿到
`analysis_digest` 与每条异常的 `seq`，再对**全部当前异常**逐条给出结论。
服务在**同一 SQLite 事务**内重算异常规范 JSON 并比对摘要，保证处置依据
不会过期：

```bash
# 1) 分析，拿到 digest（示例用 jq 提取，也可手动复制）
DIGEST=$(curl -s -X POST http://localhost:8000/api/v1/batches/analyze \
  -H 'Content-Type: application/json' \
  -d '{ "batch_id": "TRIP-001", "device_id": "reefer-A-17",
        "samples": [
          {"timestamp": "2026-09-22T08:01:00Z", "temperature": 10.1},
          {"timestamp": "2026-09-22T08:03:00Z", "temperature": 10.1}
        ],
        "segments": [
          {"segment_id": "load-1", "segment_type": "loading",
           "start": "2026-09-22T08:00:00Z", "end": "2026-09-22T08:10:00Z",
           "lower_limit": 0.0, "upper_limit": 8.0}
        ] }' | jq -r .analysis_digest)

# 2) 提交处置复核（handled_at 不得早于对应异常 end；dismissed 必须带 note）
curl -X POST http://localhost:8000/api/v1/cases/review \
  -H 'Content-Type: application/json' \
  -d "{
    \"batch_id\": \"TRIP-001\",
    \"case_id\": \"CASE-2026-09-22-01\",
    \"analysis_digest\": \"$DIGEST\",
    \"decisions\": [
      {\"seq\": 0, \"decision\": \"resolved\",
       \"handled_at\": \"2026-09-22T08:10:00Z\",
       \"note\": \"现场复检温度恢复正常\"}
    ]
  }"
```

请求字段：

| 字段 | 说明 |
| --- | --- |
| `batch_id` | 分析批次号；不存在返回 `404` |
| `case_id` | 案件号；相同案件号**同内容幂等回放**，换内容返回 `409` |
| `analysis_digest` | 分析响应中的摘要；与服务端重算结果不一致返回 `409`（快照过期） |
| `decisions[]` | 逐条处置，顺序任意（按 `seq` 匹配），但必须**恰好覆盖全部当前异常** |
| `decisions[].seq` | 异常序号（对应 `anomalies[].seq`），不可重复 |
| `decisions[].decision` | `resolved`（已解决）/ `accepted`（接受风险）/ `dismissed`（驳回/误报） |
| `decisions[].handled_at` | 处置时间（ISO-8601），**不得早于该异常的 `end`**（相等允许） |
| `decisions[].note` | 说明；`decision=dismissed` 时**必填**（纯空白无效），其余可省略 |

成功响应（含案件状态与逐条处置记录）：

```json
{
  "batch_id": "TRIP-001",
  "case_id": "CASE-2026-09-22-01",
  "analysis_digest": "a1f9…",
  "status": "closed",
  "cached": false,
  "decisions": [
    {
      "seq": 0,
      "decision": "resolved",
      "handled_at": "2026-09-22T08:10:00.000Z",
      "note": "现场复检温度恢复正常"
    }
  ]
}
```

案件状态规则：

- 全部异常为 **`resolved` 或 `dismissed`** → `"closed"`；
- 只要存在一条 **`accepted`** → `"open"`；
- 批次没有异常时，`decisions` 传空数组即可，案件直接 `"closed"`。

复核语义与错误码：

- **快照过期 `409`**：`analysis_digest` 与同一事务内重算出的当前摘要不同，
  整个事务回滚，案件与处置行均不落库。请重新 GET/POST 分析后用新摘要提交。
- **换内容 `409`**：相同 `case_id` 已存在，但批次、摘要、任一结论/时间/说明
  与首次提交不同；案件处置不可变更，仅支持同内容幂等回放（回放响应
  `cached: true`、`X-Cache: HIT`，首次提交为 `X-Cache: MISS`）。
- **覆盖/序号/时间/说明校验 `422`**：序号缺失或越界、序号重复、
  `handled_at` 早于异常结束、`dismissed` 缺少说明。
- 批次不存在 → `404`。

## 运输联单风险快照

联单接口为 `POST /api/v1/shipments/snapshot`。客户端按**交接顺序**提交批次
清单，每项给出 `batch_id`、`analysis_digest`（该批次当前分析快照）、
`case_id`（覆盖该批次全部当前异常的处置案件）与 `handoff_at`（交接时间）。
服务在**同一 SQLite 事务**内逐批次校验：依据保存的原始采样重算异常并核对
摘要 → 确认案件存在、属于该批次、处置依据当前快照且覆盖全部当前异常。
任一项未通过则整个事务回滚、绝不部分写入，错误详情可定位到具体清单项；
全部通过后保存**不可变联单**，按交接顺序返回各批次异常数与案件状态，并
给出总体结果：存在 open 案件为 `held`，否则 `released`。

```bash
# 1) 两个批次分别分析，拿到各自 digest（此处用 shell 变量代指）
DIGEST_A=$(curl -s -X POST http://localhost:8000/api/v1/batches/analyze \
  -H 'Content-Type: application/json' -d @batch-a.json | jq -r .analysis_digest)
DIGEST_B=$(curl -s -X POST http://localhost:8000/api/v1/batches/analyze \
  -H 'Content-Type: application/json' -d @batch-b.json | jq -r .analysis_digest)

# 2) 两个批次分别完成处置复核（略，见上一节；假设案件 CASE-A 已 closed、
#    CASE-B 含 accepted 仍为 open）

# 3) 提交运输联单风险快照（handoff_at 必须按清单顺序严格递增）
curl -X POST http://localhost:8000/api/v1/shipments/snapshot \
  -H 'Content-Type: application/json' \
  -d "{
    \"shipment_id\": \"SHP-2026-09-22-01\",
    \"batches\": [
      {\"batch_id\": \"TRIP-001\", \"analysis_digest\": \"$DIGEST_A\",
       \"case_id\": \"CASE-A\", \"handoff_at\": \"2026-09-22T10:00:00Z\"},
      {\"batch_id\": \"TRIP-002\", \"analysis_digest\": \"$DIGEST_B\",
       \"case_id\": \"CASE-B\", \"handoff_at\": \"2026-09-22T14:00:00Z\"}
    ]
  }"
```

请求字段：

| 字段 | 说明 |
| --- | --- |
| `shipment_id` | 运输联单号；相同联单号**同内容幂等回放**，换内容返回 `409` |
| `batches[]` | 按交接顺序排列的批次清单（至少 1 项）；`batch_id` 不可重复 |
| `batches[].batch_id` | 已分析批次号；不存在返回 `404` |
| `batches[].analysis_digest` | 该批次当前分析摘要；与事务内重算结果不一致返回 `409`（批次过期） |
| `batches[].case_id` | 该批次处置案件号；案件缺失、属于其他批次、依据过期快照或未覆盖当前全部异常均返回 `409` |
| `batches[].handoff_at` | 交接时间（ISO-8601）；清单内必须**严格递增**，否则 `422` |

成功响应（CASE-B 为 open → 总体 `held`）：

```json
{
  "shipment_id": "SHP-2026-09-22-01",
  "cached": false,
  "result": "held",
  "batches": [
    {
      "seq": 0,
      "batch_id": "TRIP-001",
      "case_id": "CASE-A",
      "analysis_digest": "a1f9…",
      "handoff_at": "2026-09-22T10:00:00.000Z",
      "anomaly_count": 1,
      "case_status": "closed"
    },
    {
      "seq": 1,
      "batch_id": "TRIP-002",
      "case_id": "CASE-B",
      "analysis_digest": "b7e2…",
      "handoff_at": "2026-09-22T14:00:00.000Z",
      "anomaly_count": 2,
      "case_status": "open"
    }
  ]
}
```

联单语义与错误码：

- **总体结果**：全部案件 `closed` → `"released"`；存在 `open` 案件 → `"held"`。
- **批次过期 `409`**：某项 `analysis_digest` 与同一事务内重算出的当前摘要
  不一致；事务回滚，联单与批次行均不落库。请重新获取分析结果后再提交。
- **案件问题 `409`**：案件不存在（缺少处置）、案件属于其他批次、案件处置
  依据的摘要与当前快照不一致、或处置未覆盖当前全部异常。
- **换内容 `409`**：相同 `shipment_id` 已存在，但清单任一项（含顺序、案件、
  摘要、交接时间）与首次提交不同；联单不可变更，仅支持同内容幂等回放
  （回放响应 `cached: true`、`X-Cache: HIT`，首次提交为 `X-Cache: MISS`）。
- **结构校验 `422`**：清单为空、`batch_id` 重复、`handoff_at` 未严格递增。
- 批次不存在 → `404`（详情指明出问题的清单项）。

## 参数校验（422）

分析请求：

- 区段时间重叠、`end <= start`、下限大于上限
- `dip_tolerance_seconds > gap_seconds`
- 空采样/空区段、时间戳格式错误等
- 指定 `profile_id` 时任一采样时刻无校准区间覆盖（详见"设备温度校准档案"）

校准档案请求：

- 区间时间重叠、`end <= start`（首尾相接允许）、空区间列表
- `scale` / `offset` 不是有限正数（0、负数、Infinity、NaN 均拒绝）

档案不存在或档案与 `device_id` 不匹配返回 `409`。

## 本地开发（不使用 Docker）

所有依赖 wheel 已放在 `vendor/`，可完全离线安装：

```bash
python3 -m venv .venv
.venv/bin/pip install --no-index --find-links vendor -r requirements.txt
.venv/bin/uvicorn app.main:app --reload
```

运行测试（需额外的 `httpx` wheel，已包含在 `vendor/`）：

```bash
.venv/bin/pip install --no-index --find-links vendor httpx
COLDCHAIN_DB=/tmp/test.db .venv/bin/python -m unittest discover -s tests -v
```

> 重新生成本地依赖：
> `pip wheel --wheel-dir vendor -r requirements.txt`（在可联网机器上执行，
> 镜像基于 `python:3.11-slim`，请在 Linux x86_64 / CPython 3.11 环境下载）。

## 项目结构

```
.
├── app/
│   ├── main.py        # FastAPI 路由、序列化、幂等编排、校准档案、复核/联单事务
│   ├── detection.py   # 核心检测/合并/拆段/归因/校准算法（纯函数，无 IO）
│   ├── models.py      # Pydantic 请求/响应模型与时间转换
│   └── db.py          # SQLite 连接、建表、批次/案件/联单/档案读写与事务
├── tests/             # unittest 算法与 API 测试（86 个用例，含校准档案、快照绑定、复核与联单幂等）
├── vendor/            # 本地依赖 wheel（离线安装）
├── requirements.txt
├── Dockerfile         # pip --no-index 离线安装
├── docker-compose.yml # 单命令启动 + 数据卷 + 健康检查
└── README.md
```
