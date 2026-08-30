# MySQL 到 Oracle 类型映射候选表

本表是 Phase 1.1 POC 前的候选契约，不代表已支持。只有标注“通过”的项目才能进入平台自动映射；未验证和风险类型必须在创建任务时阻断或要求人工确认。

| MySQL 类型 | Oracle 候选类型 | POC 状态 | 备注 |
|---|---|---|---|
| `BIGINT` | `NUMBER(19)` | 待验证 | 同步键与范围核对必须验证。 |
| `VARCHAR(n)` / `utf8mb4` | `VARCHAR2(n CHAR)` | 待验证 | 包含 emoji 与中文，按字符语义定义长度。 |
| `TINYINT(1)` | `NUMBER(1)` | 待验证 | 不默认映射为 Oracle `BOOLEAN`。 |
| `DECIMAL(p,s)` | `NUMBER(p,s)` | 待验证 | 目标精度或范围不足时阻断。 |
| `TEXT` | `CLOB` | 待验证 | 验证 JDBC 批量绑定与空值。 |
| `DATETIME(6)` | `TIMESTAMP(6)` | 待验证 | 保留无时区本地时间语义。 |
| `TIMESTAMP(6)` | `TIMESTAMP(6)` | 待验证 | 需补充 MySQL 时间点语义/会话时区验证。 |
| `JSON` | - | 阻断 | 首轮不映射，等待 Oracle JSON 和 JDBC 绑定专项 POC。 |
| `BLOB` | `BLOB` | 未开始 | 独立大字段性能与恢复用例后再开放。 |

Oracle 空字符串与 `NULL` 的语义不同，文本列的空字符串场景属于必测用例；在结果明确前不能宣称 MySQL 文本语义完全等价。
