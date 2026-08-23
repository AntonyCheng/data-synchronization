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
| JSON | jsonb / text | 顺序与格式语义 | `profile` 已验证为 text 兼容写入 |
| DATE | date | MySQL 零日期不兼容 | 未验证 |
| DATETIME(p) | timestamp(p) without time zone | 无时区本地时间 | `registered_at`/`ordered_at` 已验证 |
| TIMESTAMP(p) | timestamp(p) with time zone | 明确源端时区 | `updated_at` 已验证，时区为 UTC |
| ENUM | varchar / text | 约束不会自动复制 | 未验证 |
