# MySQL 到 PostgreSQL 类型映射验证表

| MySQL 类型 | PostgreSQL 候选类型 | 风险 | POC 结果 |
|---|---|---|---|
| TINYINT(1) | boolean / smallint | 语义有歧义，需用户确认 | `active` 已验证为 boolean |
| INT | integer | 无 | `tenant_id` 已验证 |
| BIGINT UNSIGNED | numeric(20,0) | PostgreSQL bigint 范围不足 | 未验证 |
| DECIMAL(p,s) | numeric(p,s) | 精度上限需检查 | `credit`/`amount` 已验证 |
| VARCHAR(n) | varchar(n) | 字符长度语义需检查 | email、状态字段已验证 |
| TEXT | text | 大字段性能 | `notes` 已验证 |
| BLOB | bytea | 大字段性能 | 未验证 |
| JSON | text（MVP） | JDBC 以字符串绑定；json/jsonb 需要显式 cast，当前生成器不输出 cast | `profile` text 已验证；json/jsonb 由兼容性检查阻断 |
| DATE | date | MySQL 零日期不兼容 | 未验证 |
| DATETIME(p) | timestamp(p) without time zone | 无时区本地时间 | `registered_at`/`ordered_at` 已验证 |
| TIMESTAMP(p) | timestamp(p) with time zone | 明确源端时区 | `updated_at` 已验证，时区为 UTC |
| ENUM | varchar / text | 约束不会自动复制 | 未验证 |

## 时间类型与时区（PostgreSQL / MySQL 目标）

SeaTunnel 内部用不带时区的 `LocalDateTime` / `LocalTime` 表示 `DATETIME`、`TIMESTAMP` 和 `TIME`。由 SeaTunnel 自动建表时，`TIMESTAMP` 与 `DATETIME` 建成同一种类型：PostgreSQL 为 `timestamp without time zone`，MySQL 为 `datetime`。引擎侧每条 JDBC 连接的时区参数都由 `SeaTunnelJobConfigGenerator` 生成，并计入配置指纹：

| 连接 | 时区参数 | 行为 |
|---|---|---|
| MySQL-CDC 源端 | URL `serverTimezone=<数据源服务器时区>`；`server-time-zone = "<数据源服务器时区>"`；未设置时两处都是 `Asia/Shanghai` | 必须与源库实际时区一致，否则增量阶段 `TIMESTAMP` 偏移（见下文）；显式设置时区也避免了 Connector/J 去解释 GoldenDB 含义不唯一的 `CST`；DATETIME 不偏移 |
| FULL 源端（Jdbc `SELECT`） | `serverTimezone=UTC&preserveInstants=false` | DATETIME 原样读出，TIMESTAMP 按源库会话显示值读出，都与引擎 JVM 时区无关；不使用数据源时区 |
| MySQL 目标（Jdbc sink） | `preserveInstants=false`，不设时区 | 值原样写入，与引擎 JVM 时区无关 |
| PostgreSQL 目标 | 无 | PgJDBC 按 JVM 时区渲染，值原样写入 |

MySQL 目标以前和 CDC 源端共用同一个 URL，带 `serverTimezone=Asia/Shanghai`。Connector/J 8.0.23 起默认 `preserveInstants=true`，会把 sink 绑定的 `Timestamp`（`Timestamp.valueOf(LocalDateTime)`；MySQL 的 `TIME` 也按这种方式绑定）当作引擎 JVM 时区里的时刻，再换算成上海时间。结果是 DATETIME、TIMESTAMP 和 TIME 在 FULL 与 FULL_CDC 下都比源端晚 8 小时，只有 DATE 不受影响。修复后在本地栈上逐列核对了 FULL，以及 FULL_CDC 的快照、binlog 插入和 binlog 更新：DATETIME(3)、DATE 与源端一致，TIMESTAMP(3) 的快照值与源端一致，TIME(3) 在 FULL_CDC 下与源端一致（FULL 的 TIME 小数秒在源端就已丢失，见下文“TIME(p) 的小数秒”）。另外用 Connector/J 8.0.33 按 sink 的写法，在 UTC、Asia/Shanghai、America/New_York 三种 JVM 时区下分别写入，值都保持不变。

MySQL 目标的 `TIMESTAMP` 列按目标会话的 `time_zone` 解释写入值。源库与目标库的 `time_zone` 相同时，这类值表示的时刻也不变。

### 源端服务器时区（CDC 的 TIMESTAMP）

binlog 事件里的 `TIMESTAMP` 是 UTC 时刻，MySQL-CDC source 用 `server-time-zone` 把它换回不带时区的本地时间；快照阶段则直接读源库按自身时区渲染的值。两者只有在 `server-time-zone` 等于源库实际时区时才一致。以前这个值固定为 Asia/Shanghai，源库不在东八区时，增量阶段的 `TIMESTAMP` 会整体偏移，快照阶段不受影响，所有目标类型都一样。`DATETIME`、`DATE`、`TIME` 不带时区，不受影响。

现在时区按数据源配置（“服务器时区”，IANA ID，接口见 [api-contract.md](api-contract.md#源端服务器时区)）：

- 留空为兼容模式，按 Asia/Shanghai 生成，配置与以前逐字节相同，已有任务的配置指纹不变。写出 `Asia/Shanghai` 与留空等价。
- 设置或修改后，读取该数据源的 CDC 任务和 CDC 表项的指纹改变，恢复会被拒绝，需要重新初始化，因为原 binlog 位点是按旧时区读取的。运行中的任务需先暂停或停止才能修改。
- CDC 前置检查和连接测试实测源库当前偏移（`NOW()` 与 `UTC_TIMESTAMP()` 之差），不解释 `CST` 这类缩写，并与生效时区比较。不一致时，非硬性的 `timezone` 项不通过，写明偏移小时数和建议填写的时区。

本地栈实测（源库 `--default-time-zone=+00:00`，引擎 JVM 为 UTC；FULL_CDC 任务，先快照，再在 binlog 阶段插入一行、更新一行；PostgreSQL 与 MySQL 目标都由 SeaTunnel 自动建表）：

| 源库 TIMESTAMP(3) | 兼容模式（Asia/Shanghai） | 服务器时区 = UTC |
|---|---|---|
| 快照 `2026-01-15 10:20:30.456` | `2026-01-15 10:20:30.456` | `2026-01-15 10:20:30.456` |
| binlog 插入 `2026-03-10 01:02:03.005` | `2026-03-10 09:02:03.005`（+8 h） | `2026-03-10 01:02:03.005` |
| binlog 更新 `2026-01-15 22:00:00.111` | `2026-01-16 06:00:00.111`（+8 h） | `2026-01-15 22:00:00.111` |

两种目标结果相同。两种设置下，`DATETIME(3)`、`DATE` 在各阶段都与源端一致。

### FULL 源端与引擎 JVM 时区

SeaTunnel 的 Jdbc source 以 `rs.getTimestamp(i).toLocalDateTime()` 读取 `DATETIME` 和 `TIMESTAMP`。Connector/J 默认 `preserveInstants=true`，把服务端发来的值当作连接时区里的时刻，`toLocalDateTime()` 再按引擎 JVM 时区渲染。因此只带 `serverTimezone=UTC` 时，值只有在 UTC 引擎上才正确。用 Connector/J 8.0.33 复现 SeaTunnel 的读取调用，读取 `2026-01-15 10:20:30.123`，结果如下：

| JVM 时区 | `serverTimezone=UTC`（旧） | `serverTimezone=UTC&preserveInstants=false`（现） |
|---|---|---|
| UTC | `10:20:30.123` | `10:20:30.123` |
| Asia/Shanghai | `18:20:30.123`（+8 h） | `10:20:30.123` |
| America/New_York | `05:20:30.123`（-5 h；7 月的值 -4 h） | `10:20:30.123` |

`TIMESTAMP` 列的表现相同。加上 `preserveInstants=false` 后，`serverTimezone` 不再参与换算；把它换成 Asia/Shanghai 或去掉，结果也一样。在本地 UTC 引擎上，新旧两种 URL 的 FULL 作业写入 PostgreSQL 和 MySQL 的结果完全相同。MySQL 目标按平台的做法，从源表 DDL 克隆预建。

新 URL 改变了 FULL 配置的指纹。FULL 作业并非只跑一次：运行中的 FULL 任务可以带 savepoint 暂停，暂停或失败后恢复时同样要比对指纹。因此 `GeneratedConfig.matchesFingerprint` 仍然接受把这段 URL 还原为旧写法后算出的指纹。本次改动之前暂停的 FULL 任务或表项可以照常恢复，不需要重新初始化。在 UTC 引擎上，新旧 URL 读出的值相同。

### TIME(p) 的小数秒

`TIME(p)` 的小数秒在以下两处被 SeaTunnel 2.3.13 丢弃，URL 参数都修复不了。平台不做绕过。

- **FULL 源端，所有目标**：Connector/J 返回的 `java.sql.Time` 带有毫秒（实测 `getTime() % 1000 = 789`），但 SeaTunnel 的行转换器调用 `java.sql.Time#toLocalTime()`，只保留整秒。实测 `12:34:56.789` 写入 PostgreSQL 为 `12:34:56`，写入 MySQL `TIME(3)` 为 `12:34:56.000`。
- **PostgreSQL 目标，所有模式**：Jdbc sink 以 `setTime(Time.valueOf(LocalTime))` 绑定 `TIME`，`Time.valueOf` 只保留整秒。FULL_CDC 的快照和 binlog 行写入 `time(3)` 列后也只剩整秒。MySQL sink 改用 `Timestamp` 绑定，所以 CDC 模式写入 MySQL 能保留小数秒。
