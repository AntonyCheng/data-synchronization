# MySQL 到 PostgreSQL POC 计划

## 目标

使用完全隔离的 Docker 环境验证 SeaTunnel 2.3.13 是否能支撑 PRD v0.3 中的 MVP 核心承诺，并形成可复现的验证资产和结果报告。

## 隔离规则

- Compose 项目名固定为 `data-sync-poc`。
- 容器统一使用 `ds-poc-*` 前缀。
- 主机端口默认使用 `23306`、`25432`、`18080`，避开元数据库已占用的 `13306` 和现有 PostgreSQL 的 `5432`；容器内仍使用标准数据库端口。
- 所有数据库数据、checkpoint、日志和结果只挂载到 `test/runtime/` 与 `test/results/`。
- POC 脚本只操作带 `ds-poc-*` 前缀的容器，不调用未限定目标的 Docker 清理命令。

## 环境

| 组件 | POC 基线 | 用途 |
|---|---|---|
| MySQL | 8.0 | binlog ROW + GTID 源端 |
| PostgreSQL | 17.6 | JDBC 关系型目标端 |
| SeaTunnel | 2.3.13 | Zeta 执行引擎 |
| MySQL Connector/J | POC 镜像固定版本 | CDC/JDBC 驱动 |
| PostgreSQL JDBC | POC 镜像固定版本 | PostgreSQL sink 驱动 |

## 验证顺序

1. 环境与 CDC 前置检查。
2. MySQL 一致性快照和 PostgreSQL 自动建表。
3. 全量完成后连续进入 CDC。
4. INSERT、UPDATE、DELETE。
5. 联合主键与非空唯一键。
6. 进程重启后的 checkpoint 恢复。
7. savepoint/restore 暂停恢复。
8. PostgreSQL 暂时不可用后的重试与恢复。
9. 新增字段及高风险 DDL 行为。
10. 多表和新增表行为。
11. 大字段、emoji、时间和边界类型。
12. 性能基线。

## 通过标准

- 全量和 CDC 之间没有可观察的数据缺口。
- 增删改最终正确落入 PostgreSQL，主键不重复。
- 正常重启不重新执行全量。
- 无法恢复时任务明确失败，不从最新位点静默继续。
- 所有测试都能通过 `test/scripts/` 重复执行并在 `test/results/` 留下结果。

## 输出

- `poc-report.md`
- `compatibility-matrix.md`
- `type-mapping-mysql-postgresql.md`
- `failure-recovery-matrix.md`
- `engine-adapter-contract.md`
- `performance-baseline.md`
