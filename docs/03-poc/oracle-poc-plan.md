# MySQL 到 Oracle POC 计划（Phase 1.1）

## 目标与边界

本 POC 是 PRD v0.3 Phase 1.1 的第一个关系型目标端验证，只验证 MySQL 到 Oracle 的全量写入基线。它不改变现有 MySQL 到 PostgreSQL MVP，也不提前向平台开放 `ORACLE` 数据源、CDC 或任务组。

首轮只覆盖 `source_db.customers` 中的 `id`、`email`、`display_name`、`active`、`credit`、`notes`、`registered_at` 和 `updated_at`。MySQL JSON `profile` 明确排除，直到 Oracle JSON 类型与 SeaTunnel JDBC 绑定方式通过单独验证。

## 固定候选基线

| 组件 | 候选版本 | 状态 |
|---|---|---|
| SeaTunnel | 2.3.13 | 已在 MVP 固定 |
| MySQL 源端 | 8.0 | 复用 `ds-poc-mysql` |
| Oracle Free | `gvenzl/oracle-free:23-slim-faststart` | 待拉取与实测 |
| Oracle JDBC | `ojdbc11.jar`（Java 21 兼容版本） | 由部署方提供并校验 SHA-256 |

Oracle JDBC 驱动不在仓库中，也不在构建时下载。将其放入 `test/oracle/vendor/ojdbc11.jar`，并在同目录创建 `ojdbc11.jar.sha256`，内容为该文件的 64 位 SHA-256。该规则同时满足离网交付和供应链可追溯要求。

## 验收顺序

1. Oracle 容器和独立 SeaTunnel 引擎健康，且不影响 `ds-poc-*` 既有容器。
2. SeaTunnel JDBC 全量作业能创建或写入 `DS_POC_CUSTOMERS`。
3. 验证行数、主键、emoji、CLOB、`NUMBER(12,2)` 与 `TIMESTAMP(6)`。
4. 重复提交同一作业，确认主键行不重复、`MERGE` 语义可用。
5. 基线通过后，才验证 MySQL CDC 的 INSERT/UPDATE/DELETE，并补充 checkpoint/savepoint、Oracle 短暂不可用和性能基线。
6. 所有 POC 通过后，再扩展平台数据源注册、目标兼容检查、任务配置生成和前端类型选择。

## 预期风险

- Oracle JDBC sink 的自动建表与 `generate_sink_sql` 方言支持尚未确认，因此首轮由 Oracle 预建表并使用显式 `MERGE`。
- Oracle 标识符默认大写；平台层未来必须统一转义和大小写策略，不能复用 PostgreSQL 的 `schema.table` 规则。
- `CLOB`、JSON、时区、空字符串语义和批量 `MERGE` 的绑定类型需要分别验证，失败时默认阻断，不做字符串降级。
- Oracle 容器初始化较慢，验收脚本应使用健康检查而不是固定等待时间。
