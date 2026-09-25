# Kafka 目标消息格式

适用范围：目标数据源为 Kafka 的单表任务与任务组（多表 / 整库）。输出格式决定平台桥接写入目标 topic 的**消息体（value）**，不影响引擎侧同步链路。单表任务配置在任务上（`ds_sync_task.kafka_output_format`），任务组为组级统一配置（`ds_sync_task_group.kafka_output_format`，整库自动发现的表项同样继承）。可选值：`ENVELOPE`（默认）、`CANAL_JSON`、`COMPATIBLE_DEBEZIUM_JSON`、`MAXWELL_JSON`、`OGG_JSON`。

## 1. 架构位置与不变量

```
SeaTunnel 引擎 ──写入──> raw topic（__ds_raw_{taskId}_v{configVersion}，私有，单分区）
                              │  FULL 模式 = 普通 JSON 行
                              │  FULL_CDC / INCREMENTAL = Debezium 原生变更事件（初始装载 op=r，binlog 变更 op=c/u/d，见 §9）
                              ▼
平台桥接（KafkaTaskBridgeService + KafkaRawRecordReader，单分区消费，列值换算为既有表示）
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
- **只发布选中的字段。** `FULL` 模式的引擎 `SELECT` 只读选中字段；MySQL-CDC 没有列投影，`FULL_CDC` / `INCREMENTAL` 的每条变更记录都是整行，桥接在发布前把 before / after 裁剪到任务（或任务组表项）保存的字段列表（按列名忽略大小写匹配，保持表中列序）。源表之后新增的列因此也不会出现在目标 topic 里：单表任务要编辑字段选择；任务组表项重新初始化时，原先全选的表项会跟随表结构纳入新列（`multi-table.md`）。raw topic 本身仍是整行，见 §9。
- **UPDATE 恒为一条消息，带完整 before 前像。** Debezium 原生事件里 UPDATE 本就是一条 `op=u`（含前后像）。修改同步键本身的 UPDATE 前后 key 不同，输出为旧 key 的 DELETE + 新 key 的 INSERT：主键变化时 Debezium 本就写成 d + c；同步键是主键以外的唯一键时由桥接拆分。无主键、以非空唯一键为同步键的表同理，普通 UPDATE 是一条 UPDATE，只有改唯一键本身才是 DELETE + INSERT（实测）。升级前启动、尚未重新初始化的作业仍写 SeaTunnel `DEBEZIUM_JSON`，其中 UPDATE 是相邻的 `op=d` + `op=c`，桥接照旧合并成一条（§9）。

## 2. ENVELOPE —— 默认 JSON（平台事件信封）

| 字段 | 类型 | 说明 |
|---|---|---|
| `op` | string | `INSERT` / `UPDATE` / `DELETE` |
| `key` | object | 同步键列 → 值；永不为 null |
| `data` | object | 行后像；DELETE 为被删行前像；缺失时显式 `null` |
| `before` | object | 前像；仅 UPDATE 与 DELETE 携带，否则显式 `null` |
| `source` | object | `{"database": "...", "table": "..."}` |
| `sourceEventTime` | string | ISO-8601。`FULL_CDC` / `INCREMENTAL` 的变更取源库写 binlog 事件的时间，**精确到秒**；初始装载行取快照读到该行的时刻；`FULL` 为桥接接收时间。见下文“事件时间” |
| `phase` | string | `SNAPSHOT` / `CDC`，见下文 |

```json
{"op":"UPDATE","key":{"id":1},"data":{"id":1,"name":"B","qty":5},"before":{"id":1,"name":"A","qty":5},
 "source":{"database":"source_db","table":"customers"},"sourceEventTime":"2026-09-10T00:00:01Z","phase":"CDC"}
```

```json
{"op":"DELETE","key":{"id":7},"data":{"id":7,"name":"gone"},"before":{"id":7,"name":"gone"},
 "source":{"database":"source_db","table":"customers"},"sourceEventTime":"2026-09-10T00:00:02Z","phase":"CDC"}
```

### phase 的含义（所有 MySQL 协议源）

| 任务同步模式 | 引擎写入 raw topic 的内容 | `phase` |
|---|---|---|
| `FULL` | 有界 JDBC 快照的普通 JSON 行（无 `op`） | 全部 `SNAPSHOT` |
| `FULL_CDC` | Debezium 原生事件：初始装载行 `op=r`，其后的 binlog 变更 `op=c` / `u` / `d` | 初始装载 `SNAPSHOT`，其后 `CDC` |
| `INCREMENTAL` | binlog 变更 | 全部 `CDC` |

引擎的 MySQL-CDC 源配置为 `format = "compatible_debezium_json"`，把 Debezium 的变更事件原样交给 Kafka sink（§9）。下面是同一个 `FULL_CDC` 作业实测写出的 raw 事件（只列 `payload`，省略 value schema），依次是启动前已存在、被快照读出的行，启动后的 binlog INSERT，和一次 UPDATE：

```json
{"before":null,"after":{"code":"a-1","name":"甲","qty":1},"source":{"version":"1.9.8.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1790290921618,"snapshot":"false","db":"source_db","sequence":null,"table":"kd_uk","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1790290921618,"transaction":null}
{"before":null,"after":{"code":"d-4","name":"丁 🙂","qty":4},"source":{"version":"1.9.8.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1790290984000,"snapshot":"false","db":"source_db","sequence":null,"table":"kd_uk","server_id":223344,"gtid":"0be02b06-a868-11f1-8943-e643842cd70c:1550","file":"mysql-bin.000018","pos":3828700,"row":0,"thread":8497,"query":null},"op":"c","ts_ms":1790290984544,"transaction":null}
{"before":{"code":"a-1","name":"甲","qty":1},"after":{"code":"a-1","name":"甲-upd","qty":10},"source":{…,"ts_ms":1790290984000,…},"op":"u","ts_ms":1790290984578,"transaction":null}
```

区分初始装载的是 `op`：`r` 是 Debezium 的 READ（快照读），`c` 是 binlog 插入。`source.snapshot` 不能用，SeaTunnel 的分块快照对每一行都写 `"false"`。同一时刻用旧配置（SeaTunnel `DEBEZIUM_JSON` sink）跑的作业写出的前两条是：

```json
{"before":null,"after":{"code":"a-1","name":"甲","qty":1},"op":"c","source":{"schema":null,"database":"source_db","table":"kd_uk"},"ts_ms":1790290921305}
{"before":null,"after":{"code":"d-4","name":"丁 🙂","qty":4},"op":"c","source":{"schema":null,"database":"source_db","table":"kd_uk"},"ts_ms":1790290984544}
```

两条逐字段同形：SeaTunnel 2.3.13 把 READ 和 CREATE 映射成同一个 `RowKind.INSERT`，`DEBEZIUM_JSON` sink 只写 `op=c` / `d`，`ts_ms` 是引擎采集时间。所以升级前启动、尚未重新初始化的作业，初始装载仍是 `CDC`（§9）。

消费端可以依赖：

- **初始装载先于全部增量。** SeaTunnel 要等所有快照分片完成、且其后一次 checkpoint 完成，才开始读 binlog。checkpoint 会冲刷 Kafka sink，所以此时初始装载行已全部落进 raw topic。raw topic 单分区，桥接按 raw 顺序发布、按同步键分区，因此同一 key 的 `SNAPSHOT` 事件一定先于该 key 的 `CDC` 事件；目标 topic 只有一个分区时，全部 `SNAPSHOT` 先于全部 `CDC`。实测：4 万行分块快照、parallelism 4、快照期间持续 INSERT / UPDATE / DELETE，raw topic 前 40000 条全是 `op=r`，其后 182 条才是 `c` / `u` / `d`。例外是新的一轮初始装载：停止后重新启动、重新初始化都复用同一个 raw topic 和目标 topic，新一轮的 `SNAPSHOT` 事件排在上一轮的 `CDC` 事件之后。桥接按每条记录自己的 `op` 标注阶段，不因这种顺序拒绝发布。
- **按 key 幂等应用。** INSERT 按 upsert 处理：作业故障恢复后的至少一次重放可能让同一 key 再次出现 INSERT。DELETE 一个不存在的 key 视为无操作。
- **初始装载是否完成，仍只能在带外确认。** 消息流里没有完成标记。表在初始装载后没有变更时不会出现 `CDC` 事件；目标 topic 多分区时，一个分区上出现 `CDC` 不代表其他分区的 `SNAPSHOT` 已经到齐。需要时可对照源表行数与目标 topic 中去重后的 key 数。

### 事件时间

`FULL_CDC` / `INCREMENTAL` 的 binlog 变更，`sourceEventTime` 取 Debezium 的 `source.ts_ms`，即 binlog 事件头里的时间：源库写这条事件的时间，精确到秒（MySQL 记录的是语句开始执行的时刻，所以它不晚于提交时间，且与提交时间在同一秒或更早）。实测：23:03:04.490 起执行的一组语句，事件时间都是 `2026-09-24T23:03:04Z`。引擎落后于 binlog 时（限速下的大快照之后、暂停恢复后回放积压），事件时间仍是源库的时间，不会变成回放时刻，端到端延迟因此包含引擎的落后（见 `monitoring-and-consistency.md`）。实测：作业以保存点暂停期间 23:04:20 执行的变更，约一分钟后恢复回放，事件时间仍是 `2026-09-24T23:04:20Z`。

初始装载行没有源库变更时间，取快照读到该行的时刻（Debezium 对快照行的 `source.ts_ms`）。`FULL` 任务取桥接接收时间。

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
{"type":"UPDATE","database":"source_db","table":"customers","es":1757462401000,"ts":1757462401000,
 "data":[{"id":1,"name":"B","qty":5}],"old":[{"name":"A"}],"pkNames":["id"],"isDdl":false,"sql":""}
```

偏差说明：不输出 `mysqlType`/`sqlType`/`xid`/`position` 等 canal 扩展字段（平台不持有事务号与位点）；`old` 为变更列子集而非全量前像。

## 4. COMPATIBLE_DEBEZIUM_JSON

对应 Debezium JsonConverter 关闭 schema（`value.converter.schemas.enable=false`）的信封，SeaTunnel Kafka Source `format = debezium_json` 可直接消费。

| NormalizedEvent | 输出字段 |
|---|---|
| `op`（CDC 插入） | `op`：`c` |
| `op`（快照插入，即 `phase=SNAPSHOT`：`FULL` 任务的全部行、`FULL_CDC` 的初始装载行） | `op`：`r`（Debezium READ），并加 `source.snapshot: "true"` |
| `op`（更新 / 删除） | `op`：`u` / `d` |
| 前像 / 后像 | `before` / `after`：全量镜像；插入 `before=null`，删除 `after=null` |
| `sourceEventTime` | `ts_ms` 与 `source.ts_ms`：epoch 毫秒 |
| 库表 | `source.db` / `source.table` |
| — | `transaction: null`；不伪造 `version`/`connector`（平台不是 Debezium 连接器） |

```json
{"before":{"id":1,"name":"A"},"after":{"id":1,"name":"B"},"op":"u","ts_ms":1757462401000,
 "source":{"db":"source_db","table":"customers","ts_ms":1757462401000},"transaction":null}
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
{"table":"source_db.customers","op_type":"U","op_ts":"2026-09-10 00:00:01.000000",
 "before":{"id":1,"name":"A"},"after":{"id":1,"name":"B"},"primary_keys":["id"]}
```

偏差说明：不输出 `pos`/`current_ts`/`fields`（平台不持有复制位点）。

## 7. 已知限制与差异汇总

- **列值的表示沿用 SeaTunnel 行格式时期**，逐值不变（§9 的实测对比）。其中两点是原有行为，消费端需要知道：
  - `DECIMAL` 以 JSON 数值发布，桥接按 double 解析：有小数部分的值超过约 15～17 位有效数字即被舍入，并可能以科学计数法输出，如 `DECIMAL(38,10)` 的 `1234567890123456789012345678.0123456789` 发布为 `1.2345678901234569E27`，`12345678.90` 发布为 `1.23456789E7`；整数值（含 `BIGINT UNSIGNED`、小数部分为零的 `DECIMAL`）按整数发布，不丢精度。需要精确小数的消费方不应依赖该字段的全部位数。
  - `FLOAT` 发布的是引擎 Java 8 上 `Float.toString` 打印出的十进制值（如 `2285691904` 发布为 `2.2856919E9`），`TINYINT(1)` 发布为布尔值，`BIT(n>1)` / `BLOB` / `BINARY` 为 base64 字符串，`TIMESTAMP` 按数据源的服务器时区渲染成不带时区的本地时间。
  - `FLOAT` 是近似类型，同一个存储值在初始装载和 binlog 变更里可能末几位不同：初始装载经服务器的文本协议读取，只有约 6 位有效数字；binlog 带的是原始位模式。实测存储值 `2285691904`：初始装载发布 `2285690110.0`，binlog 变更发布 `2285691900.0`。需要精确值的列应使用 `DOUBLE` 或 `DECIMAL`。
- **GoldenDB（Oracle 兼容模式）建表时改写列类型**：`FLOAT` 建成 `double`，`DATE` 建成 `datetime`（以 `information_schema.columns` 为准），发布的也就是 `DOUBLE` / `DATETIME` 的表示，如 `DATE` 列发布 `2026-01-02T00:00:00`。列类型相同时 GoldenDB 与原生 MySQL 发布的事件逐字相同（§9）。
- **表结构变更之后的列**：Debezium 按表结构历史解析 binlog，`ADD COLUMN` 之后的记录带上新列，`DROP COLUMN` 之后不再带该列（实测）。桥接按任务保存的字段列表裁剪（§1），所以新列不会发布，与 SeaTunnel 行格式时期一致；已删除的列不再出现（SeaTunnel 行格式时期输出 `null`）。任务组的 DDL 检查会先暂停变更了结构的表（`DDL_BLOCKED`）。
- **SeaTunnel 行格式无法转换、作业直接失败的值，现在会发布**：1970 年以前、带亚毫秒部分的 `DATETIME(4..6)`（如 `1969-12-31T23:59:59.999999`），24 小时及以上的 `TIME`（按 MySQL 的写法，如 `838:59:59`），`GEOMETRY`（WKB 的 base64）。负的 `TIME` 不可靠：Debezium 1.9.8 快照读会丢符号（`-00:00:01` 读成 `00:00:01`），binlog 解码出错误的值（`-12:00:00` 解码成 `-12:04:03`），平台原样发布。
- **兼容格式不内嵌 key 对象**：record key（同步键 JSON）由生产端统一附加；ENVELOPE 额外在消息体内冗余 `key` 字段。
- SeaTunnel 自带的 canal/maxwell **sink** 会把 UPDATE 拆成 DELETE+INSERT 两条消息；平台的输出是合并后的单条 UPDATE，无此问题。
- **时间戳**：所有格式的时间戳都取自 `sourceEventTime`（§2“事件时间”）。binlog 变更是源库事件时间，精确到秒；初始装载行是快照读取时刻；`FULL` 是桥接接收时间。都不是 broker 写入时间。升级前启动、尚未重新初始化的 CDC 作业仍是引擎采集时间：引擎落后于 binlog 时，它会晚于源库时间（实测：限速快照期间 77 秒内陆续提交的变更，回放后采集时间全部落在 43 毫秒之内，比提交时间晚 2～3 分钟）。

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

## 9. raw topic 格式（引擎 → 桥接）

`FULL_CDC` / `INCREMENTAL` 的 Kafka 作业由 `SeaTunnelJobConfigGenerator` 生成如下源与 sink 配置：

```
MySQL-CDC {
  …
  format = "compatible_debezium_json"
  debezium = {
    key.converter.schemas.enable = false
    value.converter.schemas.enable = true
    datatype.propagate.source.type = ".+[.]TINYINT,.+[.]FLOAT( UNSIGNED)?( ZEROFILL)?"
  }
}
Kafka { … format = "COMPATIBLE_DEBEZIUM_JSON" … }
```

- 每条 raw 记录是 Kafka Connect 的 JSON 信封 `{"schema":…,"payload":<Debezium 变更事件>}`；心跳记录被引擎丢弃，墓碑已关闭（`tombstones.on.delete=false`），`schema-changes.enabled = false` 时不写 DDL 事件。桥接跳过任何没有 `op` 的记录。
- 列值是 Debezium 的编码：`DATETIME` 为 epoch 毫秒 / 微秒，`DATE` 为 epoch 天数，`TIME` 为微秒数，`TIMESTAMP` 为 UTC 时刻字符串，`DECIMAL` 为 JSON 数值。桥接（`KafkaRawRecordReader`）按 value schema 中每列的逻辑类型换算成 SeaTunnel 行格式时期发布的值，所以 value schema 必须开启。Debezium 把 `FLOAT` 扩成 double、把 `TINYINT` 报成 int16，单凭 schema 分不出 `FLOAT` / `DOUBLE` 和 `TINYINT(1)` / `TINYINT`，`datatype.propagate.source.type` 只为这两类列补上源类型。`FLOAT` 的值再按 Java 8 的 `Float.toString` 打印（`JavaEightFloats`）：Java 19 起的 `Float.toString` 对约 9% 的 float 位模式打印出不同的数字（绝大多数是 1e7～1e19 的整数值，如 `2285691904` Java 8 印 `2.2856919E9`、Java 19+ 印 `2.285692E9`），而桥接运行在 Java 21 上。已对全部 2^32 个位模式与 Java 8 逐一比对一致。`TIMESTAMP` 按数据源的服务器时区渲染，与生成配置时写入 `server-time-zone` 的是同一个值。
- **实测对比（2026-09-25，本地栈 MySQL 8.0.45 + SeaTunnel 2.3.13）**：同一张表同时跑旧配置（`DEBEZIUM_JSON`）和新配置两个作业，读同样的行和同一段 binlog，覆盖有 / 无符号整数、`DECIMAL` 至 (65,30)、`FLOAT` / `DOUBLE`、含中文与 emoji 的 `CHAR` / `VARCHAR` / `TEXT`、`DATE`、`DATETIME(0/3/6)`、`TIMESTAMP(0/3/6)`、`TIME(0/3/6)`、`YEAR`、`BIT(1/8/64)`、`BOOLEAN`、`BLOB` / `BINARY`、`JSON`、`ENUM` / `SET`、NULL，初始装载、INSERT / UPDATE（含改主键）/ DELETE、事务、保存点暂停后恢复；另有唯一键表和服务器时区为 `Asia/Shanghai` 的 `TIMESTAMP` 表。两边经桥接发布的五种格式逐条逐字相同（消息 key 也相同），差别只有初始装载行的 `phase` / `op`，以及事件时间字段。录制的 raw 记录和旧桥接的发布结果在 `ruoyi-sync/src/test/resources/kafka/raw-format-fidelity/`，`KafkaRawFormatFidelityTest` 以此回归。
- **体积**：value schema 随每条记录发送。实测平均每条 raw 记录：48 列的表 12.0 KB（旧格式 1.0 KB），6 列 3.2 KB（0.25 KB），3 列 2.4 KB（0.16 KB），约 10～15 倍。raw topic 的磁盘与网络占用相应增加；引擎的 `read_limit.bytes_per_second`（默认 10 MiB/s）按源端产出行的字节数限速，新格式下这一行就是整条 raw 记录，宽表会比以前更早触到字节限速：按每行 12 KB 估算，48 列的表约 870 行/秒，低于默认的 1000 行/秒（估算，未实测吞吐）。
- **raw topic 的内容与保留**：raw topic 与目标 topic 在同一个 Kafka 集群，保存的是整行（包括未选中的字段，§1）。未选字段中有不应出现在该集群上的数据时，需在 broker 上限制 `__ds_raw_*` 的访问权限。平台以 1 分区 / 1 副本创建 raw topic，不设置主题级参数，保留期跟随 broker 默认（`log.retention.hours`，默认 168 小时），磁盘占用约为“保留期内的变更行数 × 单条体积”；平台目前不删除 raw topic，任务删除或配置版本变更后旧的 `__ds_raw_{taskId}_v{n}` 仍在，其中数据按保留期过期。
- **GoldenDB 实测（2026-09-25，GoldenDB `V_ALL-DBV6.1.03.12SP1` 经平台）**：与本地 MySQL 同表同语句并跑 `FULL_CDC`，初始装载 3 行为 `SNAPSHOT`；GoldenDB 把 UPDATE 写成 binlog DELETE + INSERT，桥接合并为带前像的一条 `UPDATE`；改主键为旧 key 的 `DELETE` + 新 key 的 `INSERT`；未选字段不发布；以保存点暂停期间的变更在恢复后以 `CDC` 发布；列类型相同时两边 14 条事件逐字相同。
- **升级与兼容**：引擎配置变了，已有 Kafka CDC 任务（单表与任务组表项）的配置指纹都会变化。运行中的作业不受影响，继续写旧格式；桥接逐条识别格式，旧格式照旧读取（初始装载仍是 `CDC`，事件时间仍是采集时间）。暂停后恢复会因指纹不符被拒：单表任务进入 `REINITIALIZE_REQUIRED`，任务组表项进入 `FAILED` 并提示重新初始化该表。重新初始化或停止后重新启动时提交新格式的作业，写入同一个 raw topic（桥接从已提交的位点继续，新旧格式的记录可以前后相接），新一轮初始装载为 `SNAPSHOT`。修改数据源的服务器时区同样要求重新初始化；在此之前若桥接重启（例如后端重启），`TIMESTAMP` 会按新时区渲染。
