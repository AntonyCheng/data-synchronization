# Kafka 目标消息格式

适用范围：目标数据源为 Kafka 的单表任务与任务组（多表 / 整库）。输出格式决定平台桥接写入目标 topic 的**消息体（value）**，不影响引擎侧同步链路。单表任务配置在任务上（`ds_sync_task.kafka_output_format`），任务组为组级统一配置（`ds_sync_task_group.kafka_output_format`，整库自动发现的表项同样继承）。可选值：`ENVELOPE`（默认）、`CANAL_JSON`、`COMPATIBLE_DEBEZIUM_JSON`、`MAXWELL_JSON`、`OGG_JSON`。

## 1. 架构位置与不变量

```
SeaTunnel 引擎 ──写入──> raw topic（__ds_raw_{taskId}_v{configVersion}，私有）
                              │  FULL 模式 = 普通 JSON 行；FULL_CDC / INCREMENTAL = Debezium JSON（op 只有 c / d）
                              ▼
平台桥接（KafkaTaskBridgeService，单分区消费，UPDATE 的 d+c 对合并）
                              ▼
格式序列化器（KafkaEventSerializer，按 kafkaOutputFormat）
                              ▼
目标 topic（用户所有，消息体 = 本文各格式）
```

与格式无关的不变量：

- **消息 Key 恒为同步键 JSON 对象字符串**（如 `{"id":1}`），所有格式一致——按键稳定分区、单键保序。
- 生产端 `acks=all` + 幂等发送；raw topic 位点仅在目标 topic 收到 broker ack 后提交。
- 引擎 HOCON 与配置指纹（`engine_config_hash`）**不包含输出格式**——切换格式不触碰引擎配置。
- 修改格式与其它编辑一样提升 `configVersion`，下次启动使用新 raw topic（重新全量）。
- UPDATE 在 raw topic 中恒为相邻的两条：`op=d`（前像）+ `op=c`（后像）。SeaTunnel 的 `DEBEZIUM_JSON` sink 把 UPDATE_BEFORE / UPDATE_AFTER 分别写成 d / c，从不写 `u`（原生 MySQL 实测；GoldenDB 走同一个 MySQL-CDC 连接器）。桥接恒定合并为一条 `UPDATE`（含完整 before 前像）后再序列化——所有格式的 UPDATE 都是单条消息。修改同步键本身的 UPDATE 前后 key 不同，不合并，输出为旧 key 的 DELETE + 新 key 的 INSERT。

## 2. ENVELOPE —— 默认 JSON（平台事件信封）

| 字段 | 类型 | 说明 |
|---|---|---|
| `op` | string | `INSERT` / `UPDATE` / `DELETE` |
| `key` | object | 同步键列 → 值；永不为 null |
| `data` | object | 行后像；DELETE 为被删行前像；缺失时显式 `null` |
| `before` | object | 前像；仅合并 UPDATE 与 DELETE 携带，否则显式 `null` |
| `source` | object | `{"database": "...", "table": "..."}` |
| `sourceEventTime` | string | ISO-8601。`FULL_CDC` / `INCREMENTAL` 取 raw 事件的 `ts_ms`，即引擎**采集**该行的时刻（初始装载行是被快照读到的时刻，binlog 事件是被引擎处理的时刻），不是源库提交时间；`FULL` 为桥接接收时间 |
| `phase` | string | `SNAPSHOT` / `CDC`；MySQL 源 `FULL_CDC` 的初始装载行也是 `CDC`，见下文 |

```json
{"op":"UPDATE","key":{"id":1},"data":{"id":1,"name":"B","qty":5},"before":{"id":1,"name":"A","qty":5},
 "source":{"database":"source_db","table":"customers"},"sourceEventTime":"2026-09-10T00:00:01.500Z","phase":"CDC"}
```

```json
{"op":"DELETE","key":{"id":7},"data":{"id":7,"name":"gone"},"before":{"id":7,"name":"gone"},
 "source":{"database":"source_db","table":"customers"},"sourceEventTime":"2026-09-10T00:00:02Z","phase":"CDC"}
```

### phase 的真实含义（所有 MySQL 协议源）

| 任务同步模式 | 引擎写入 raw topic 的内容 | `phase` |
|---|---|---|
| `FULL` | 有界 JDBC 快照的普通 JSON 行（无 `op`） | 全部 `SNAPSHOT` |
| `FULL_CDC` | 初始装载行与 binlog 变更都是 Debezium JSON，插入一律 `op=c` | 全部 `CDC`，**包括初始装载行** |
| `INCREMENTAL` | binlog 变更 | 全部 `CDC` |

`FULL_CDC` 的初始装载行无法标成 `SNAPSHOT`，因为 raw topic 里没有能区分它的信号。下面是同一个 `FULL_CDC` 作业实测写出的两条 raw 事件，前一条是启动前已存在、被快照读出的行，后一条是启动后的 binlog INSERT：

```json
{"before":null,"after":{"id":1,"name":"snap-1","qty":10,"amount":1.5,"created_at":"2026-01-01T00:00:01.123"},"op":"c","source":{"schema":null,"database":"source_db","table":"kp_phase_small"},"ts_ms":1790258500371}
{"before":null,"after":{"id":4,"name":"cdc-4","qty":40,"amount":4.5,"created_at":"2026-01-01T00:00:04.5"},"op":"c","source":{"schema":null,"database":"source_db","table":"kp_phase_small"},"ts_ms":1790258542124}
```

两条逐字段同形，也都不带 Kafka header。SeaTunnel 2.3.13 的 MySQL-CDC 在反序列化时把 Debezium 的 READ（快照读）和 CREATE（binlog 插入）映射成同一个 `RowKind.INSERT`。`DEBEZIUM_JSON` sink 只写 `before` / `after` / `op` / `source{schema,database,table}` / `ts_ms` 这几个字段，`op` 只有 `c` / `d`。`ts_ms` 对两类事件都是 Debezium 信封的处理时间。原生 MySQL 与 GoldenDB 走同一个连接器，行为相同。平台不按消息间隔、顺序或“首条 UPDATE/DELETE”去猜边界：增量恰好只有 INSERT 时，这类启发式会把增量误标成快照。Zeta 也不暴露快照完成信号，见 `monitoring-and-consistency.md` 的“阶段”。

消费端可以依赖：

- **初始装载先于全部增量。** SeaTunnel 要等所有快照分片完成、且其后一次 checkpoint 完成，才开始读 binlog。checkpoint 会冲刷 Kafka sink，所以此时初始装载行已全部落进 raw topic。raw topic 单分区，桥接按 raw 顺序发布、按同步键分区，因此同一 key 的初始装载 INSERT 一定先于该 key 的增量事件。实测用 4 万行分块快照，快照期间持续写入 INSERT/UPDATE/DELETE：parallelism 1 和 4 下，raw topic 中都没有一条增量事件插在快照行之间。
- **按 key 幂等应用。** INSERT 按 upsert 处理，因为初始装载行、binlog 插入、作业故障恢复后的至少一次重放，都可能让同一 key 再次出现 INSERT。DELETE 一个不存在的 key 视为无操作。
- **初始装载是否完成，只能在带外确认。** 消息流里没有完成标记。不要用 `phase`、首条 UPDATE/DELETE 或消息间隔判断。需要时可对照源表行数与目标 topic 中去重后的 key 数。

## 3. CANAL_JSON

对应 SeaTunnel Kafka Source `format = canal_json`（Canal 生态消费方通用）。

| NormalizedEvent | 输出字段 |
|---|---|
| `op` | `type`：`INSERT` / `UPDATE` / `DELETE`（大写；快照 → `INSERT`，canal 无快照语义） |
| 后像（DELETE 为前像） | `data`：**单元素数组** `[行]` |
| UPDATE 前像 | `old`：单元素数组 `[变更列前值]`（仅变更列，canal 语义；消费端用 data 回填未变列）。前像缺失时为 `[{}]` |
| `sourceEventTime` | `es` 与 `ts`：epoch **毫秒** |
| 同步键 | `pkNames`：键列名数组 |
| — | `isDdl: false`、`sql: ""` 固定填充 |

```json
{"type":"UPDATE","database":"source_db","table":"customers","es":1757462401500,"ts":1757462401500,
 "data":[{"id":1,"name":"B","qty":5}],"old":[{"name":"A"}],"pkNames":["id"],"isDdl":false,"sql":""}
```

偏差说明：不输出 `mysqlType`/`sqlType`/`xid`/`position` 等 canal 扩展字段（平台不持有事务号与位点）；`old` 为变更列子集而非全量前像。

## 4. COMPATIBLE_DEBEZIUM_JSON

对应 Debezium JsonConverter 关闭 schema（`value.converter.schemas.enable=false`）的信封，SeaTunnel Kafka Source `format = debezium_json` 可直接消费。

| NormalizedEvent | 输出字段 |
|---|---|
| `op`（CDC 插入） | `op`：`c` |
| `op`（快照插入，即 `phase=SNAPSHOT`，只有 `FULL` 任务产生） | `op`：`r`（Debezium READ），并加 `source.snapshot: "true"`。`FULL_CDC` 的初始装载行是 `CDC` 插入，输出 `c` |
| `op`（更新 / 删除） | `op`：`u` / `d` |
| 前像 / 后像 | `before` / `after`：全量镜像；插入 `before=null`，删除 `after=null` |
| `sourceEventTime` | `ts_ms` 与 `source.ts_ms`：epoch 毫秒 |
| 库表 | `source.db` / `source.table` |
| — | `transaction: null`；不伪造 `version`/`connector`（平台不是 Debezium 连接器） |

```json
{"before":{"id":1,"name":"A"},"after":{"id":1,"name":"B"},"op":"u","ts_ms":1757462401500,
 "source":{"db":"source_db","table":"customers","ts_ms":1757462401500},"transaction":null}
```

## 5. MAXWELL_JSON

对应 Maxwell 生态，SeaTunnel Kafka Source `format = maxwell_json`。

| NormalizedEvent | 输出字段 |
|---|---|
| `op` | `type`：`insert` / `update` / `delete`（**小写**；快照 → `insert`） |
| 后像（DELETE 为前像） | `data`：**对象** |
| UPDATE 前像 | `old`：对象，仅变更列前值（Maxwell 语义；消费端回填）。前像缺失时为 `{}` |
| `sourceEventTime` | `ts`：epoch **秒**（消费端 ×1000） |
| 同步键 | `primary_key_columns`：键列名数组 |

```json
{"type":"update","database":"source_db","table":"customers","ts":1757462401,
 "data":{"id":1,"name":"B","qty":5},"old":{"name":"A"},"primary_key_columns":["id"]}
```

偏差说明：不输出 `xid`/`commit`/`xoffset`/`position`（平台不持有事务号与位点）。

## 6. OGG_JSON

对应 Oracle GoldenGate 生态，SeaTunnel Kafka Source `format = ogg_json`。

| NormalizedEvent | 输出字段 |
|---|---|
| `op` | `op_type`：`I` / `U` / `D`（快照 → `I`） |
| 库表 | `table`：`库.表` 拼接（消费端按 `.` 拆分） |
| `sourceEventTime` | `op_ts`：UTC 字符串 `yyyy-MM-dd HH:mm:ss.SSSSSS`（毫秒精度补零到微秒） |
| 前像 / 后像 | `before` / `after`：全量镜像；插入 `before=null`，删除 `after=null` |
| 同步键 | `primary_keys`：键列名数组 |

```json
{"table":"source_db.customers","op_type":"U","op_ts":"2026-09-10 00:00:01.500000",
 "before":{"id":1,"name":"A"},"after":{"id":1,"name":"B"},"primary_keys":["id"]}
```

偏差说明：不输出 `pos`/`current_ts`/`fields`（平台不持有复制位点）。

## 7. 已知限制与差异汇总

- **`FULL_CDC` 的初始装载行是 `phase=CDC`**：原因和消费端的替代依据见 §2“phase 的真实含义”。要可靠区分，只能改引擎侧 raw 格式。MySQL-CDC 源的 `format = compatible_debezium_json` 会原样输出 Debezium JSON，理论上保留 `op=r` 与源库 `source.ts_ms`，但尚未实测。它还会改变 raw 事件的值编码和引擎配置指纹，现有 Kafka CDC 任务都要重新初始化，因此目前未采用。
- **UPDATE 恒有前像**：MySQL 协议源的 UPDATE 在 raw topic 中恒为 d+c 对（见 §1），合并后 before 是完整前像，五种格式的 `before` / `old` 都有值。归一化器对 `op=u`（无前像）的处理只作防御：SeaTunnel 的 Kafka sink 不开启 UPDATE 合并，不会写出 `u`。
- **兼容格式不内嵌 key 对象**：record key（同步键 JSON）由生产端统一附加；ENVELOPE 额外在消息体内冗余 `key` 字段。
- SeaTunnel 自带的 canal/maxwell **sink** 会把 UPDATE 拆成 DELETE+INSERT 两条消息；平台的输出是合并后的单条 UPDATE，无此问题。
- **时间戳**：所有格式的时间戳都取自 `sourceEventTime`。`FULL_CDC` / `INCREMENTAL` 是引擎采集时间（raw `ts_ms`），`FULL` 是桥接接收时间。两者都不是源库提交时间，也不是 broker 写入时间。引擎落后于 binlog 时，事件时间会晚于提交时间，例如限速下的大快照之后，或暂停恢复后回放积压时。实测：限速快照期间 77 秒内陆续提交的变更，回放后 `ts_ms` 全部落在 43 毫秒之内，比提交时间晚 2～3 分钟。

## 8. 用 SeaTunnel Kafka Source 再消费

```
source {
  Kafka {
    bootstrap_servers = "broker:9092"
    topic = "customers"
    format = "canal_json"          # 或 debezium_json / maxwell_json / ogg_json
  }
}
```

| 平台格式 | SeaTunnel format |
|---|---|
| `CANAL_JSON` | `canal_json` |
| `COMPATIBLE_DEBEZIUM_JSON` | `debezium_json`（schema-less 信封） |
| `MAXWELL_JSON` | `maxwell_json` |
| `OGG_JSON` | `ogg_json` |
