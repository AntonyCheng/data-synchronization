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
| MySQL-CDC 源端 | URL `serverTimezone=Asia/Shanghai`；`server-time-zone = "Asia/Shanghai"` | 为 GoldenDB 的 `CST` 保留；Debezium 读出的 DATETIME 不偏移 |
| FULL 源端（Jdbc `SELECT`） | `serverTimezone=UTC` | 只有引擎 JVM 在 UTC 时，DATETIME 才能原样读出；本地栈和离线模板都设置了 `TZ=UTC` |
| MySQL 目标（Jdbc sink） | `preserveInstants=false`，不设时区 | 值原样写入，与引擎 JVM 时区无关 |
| PostgreSQL 目标 | 无 | PgJDBC 按 JVM 时区渲染，值原样写入 |

MySQL 目标以前和 CDC 源端共用同一个 URL，带 `serverTimezone=Asia/Shanghai`。Connector/J 8.0.23 起默认 `preserveInstants=true`，会把 sink 绑定的 `Timestamp`（`Timestamp.valueOf(LocalDateTime)`；MySQL 的 `TIME` 也按这种方式绑定）当作引擎 JVM 时区里的时刻，再换算成上海时间。结果是 DATETIME、TIMESTAMP 和 TIME 在 FULL 与 FULL_CDC 下都比源端晚 8 小时，只有 DATE 不受影响。修复后在本地栈上逐列核对了 FULL，以及 FULL_CDC 的快照、binlog 插入和 binlog 更新：DATETIME(3)、DATE、TIME(3) 与源端一致，TIMESTAMP(3) 的快照值与源端一致。另外用 Connector/J 8.0.33 按 sink 的写法，在 UTC、Asia/Shanghai、America/New_York 三种 JVM 时区下分别写入，值都保持不变。

MySQL 目标的 `TIMESTAMP` 列按目标会话的 `time_zone` 解释写入值。源库与目标库的 `time_zone` 相同时，这类值表示的时刻也不变。

以下问题尚未解决。它们发生在源端，PostgreSQL 目标同样受影响：

- CDC binlog 阶段的 `TIMESTAMP`：`server-time-zone` 固定为 Asia/Shanghai。源库 `time_zone` 不是东八区时（本地栈源库为 UTC），binlog 事件里的 TIMESTAMP 比源端显示值晚 8 小时，快照阶段则没有偏移。
- FULL 模式的 `TIME(p)`：Jdbc 源端通过 `java.sql.Time` 读取，小数秒会丢失。
- FULL 源端依赖引擎 JVM 在 UTC。给这个 URL 也加上 `preserveInstants=false` 就能去掉这个依赖，但所有 FULL 任务的配置指纹都会随之改变。
