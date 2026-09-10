# Kafka 目标消息格式

适用范围：目标数据源为 Kafka 的单表任务与任务组（多表 / 整库）。输出格式决定平台桥接写入目标 topic 的**消息体（value）**，不影响引擎侧同步链路。单表任务配置在任务上（`ds_sync_task.kafka_output_format`），任务组为组级统一配置（`ds_sync_task_group.kafka_output_format`，整库自动发现的表项同样继承）。可选值：`ENVELOPE`（默认）、`CANAL_JSON`、`COMPATIBLE_DEBEZIUM_JSON`、`MAXWELL_JSON`、`OGG_JSON`。

## 1. 架构位置与不变量

```
SeaTunnel 引擎 ──写入──> raw topic（__ds_raw_{taskId}_v{configVersion}，私有）
                              │  FULL 模式 = 普通 JSON 行；CDC 模式 = Debezium JSON
                              ▼
平台桥接（KafkaTaskBridgeService，单分区消费，GoldenDB UPDATE 的 DELETE+INSERT 对合并）
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
- GoldenDB 的 UPDATE 在 binlog 中是 DELETE+INSERT 两条，桥接恒定合并为一条 `UPDATE`（含 before 前像）后再序列化——所有格式的 UPDATE 都是单条消息。

## 2. ENVELOPE —— 默认 JSON（平台事件信封）

| 字段 | 类型 | 说明 |
|---|---|---|
| `op` | string | `INSERT` / `UPDATE` / `DELETE` |
| `key` | object | 同步键列 → 值；永不为 null |
| `data` | object | 行后像；DELETE 为被删行前像；缺失时显式 `null` |
| `before` | object | 前像；仅合并 UPDATE 与 DELETE 携带，否则显式 `null` |
| `source` | object | `{"database": "...", "table": "..."}` |
| `sourceEventTime` | string | ISO-8601；CDC 为 Debezium `ts_ms`，快照行为桥接接收时间 |
| `phase` | string | `SNAPSHOT` / `CDC` |

```json
{"op":"UPDATE","key":{"id":1},"data":{"id":1,"name":"B","qty":5},"before":{"id":1,"name":"A","qty":5},
 "source":{"database":"source_db","table":"customers"},"sourceEventTime":"2026-09-10T00:00:01.500Z","phase":"CDC"}
```

```json
{"op":"DELETE","key":{"id":7},"data":{"id":7,"name":"gone"},"before":{"id":7,"name":"gone"},
 "source":{"database":"source_db","table":"customers"},"sourceEventTime":"2026-09-10T00:00:02Z","phase":"CDC"}
```

> GoldenDB 注意：其 CDC 连接器把快照行发为 `op=c`（非 Debezium 标准的 `op=r`），因此 `FULL_CDC` 的快照事件也会带 `phase="CDC"`。消费端应按 key 应用事件，不要依赖 `phase` 判定初始装载是否完成。

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
| `op`（快照插入） | `op`：`r`（Debezium READ），并加 `source.snapshot: "true"` |
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

- **原生 MySQL 源的普通 UPDATE 无前像**：归一化器对普通 Debezium `u` 事件当前不保留 `before`（GoldenDB 的 DELETE+INSERT 合并路径恒有 before，不受影响）。后果：ENVELOPE/CANAL/MAXWELL/DEBEZIUM 四种格式的 `before`/`old` 为空均可正常消费；仅 OGG_JSON 的 `before=null` 不是 SeaTunnel ogg 反序列化器接受的形状（开启 `ignore-parse-error` 可跳过）。
- **兼容格式不内嵌 key 对象**：record key（同步键 JSON）由生产端统一附加；ENVELOPE 额外在消息体内冗余 `key` 字段。
- SeaTunnel 自带的 canal/maxwell **sink** 会把 UPDATE 拆成 DELETE+INSERT 两条消息；平台的输出是合并后的单条 UPDATE，无此问题。
- 所有格式的时间戳均源自事件时间（Debezium `ts_ms` / 快照行桥接接收时间），不是 broker 写入时间。

## 8. 用 SeaTunnel Kafka Source 再消费

```
source {
  Kafka {
    bootstrap_servers = "broker:9092"
    topic = "customers"
    format = "canal_json"          # 或 debezium_json / maxwell_json / ogg_json
    # ogg 再消费含 before=null 的 UPDATE 时建议开启：
    # ignore.parse.error = true
  }
}
```

| 平台格式 | SeaTunnel format |
|---|---|
| `CANAL_JSON` | `canal_json` |
| `COMPATIBLE_DEBEZIUM_JSON` | `debezium_json`（schema-less 信封） |
| `MAXWELL_JSON` | `maxwell_json` |
| `OGG_JSON` | `ogg_json` |
