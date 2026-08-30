# MVP 收口验收记录

## 覆盖刷新

在 `ds-poc-mysql` 与 `ds-poc-postgres` 隔离环境中执行了两条 `FULL + OVERWRITE` 演练：

- 成功路径：作业完成后平台状态刷新执行事务性表替换，正式表可读，临时表被移除，任务的 `overwrite_stage_table` 标记清空；重复刷新不会再次替换。
- 失败路径：故意指定不存在的临时表，平台返回 `FAILED` 和明确的替换错误；PostgreSQL 事务回滚，原正式表仍保留 `ORIGINAL` 数据。

演练使用的临时对象均为 `mvp_closeout_*`，完成后已删除测试任务。SeaTunnel 作业不会作为常驻任务保留。

## 已发现并修复

MyBatis-Plus 默认忽略 null 字段更新，导致替换成功后 `overwrite_stage_table` 旧值残留。现改为显式更新为 null，保证状态刷新幂等。替换失败时返回引擎终态 `FINISHED`、平台状态 `FAILED`，避免误报为引擎不可达。

## 收口前状态

平台进程重启对账、凭证加密迁移和本地扩大样本性能基线已在下文补齐。仍需保留的是最终全量回归和发布前人工浏览器验收；本地基线不形成生产容量承诺。

## 凭证迁移补充

在本地隔离环境启用 AES 加密后，`POST /sync/data-source/credential-migrate` 返回 `total=2, migrated=2`。元数据库密码字段已变为 `ENC_` 密文前缀，MySQL 数据源连接测试返回成功，Phase 6 配置预览回归仍通过且不包含明文密码。密钥仅通过当前进程环境变量注入，未写入仓库或验收结果。

## 元数据库结构迁移补充

2026-08-30 在 `dbs-mysql/ry-vue` 执行 `test/scripts/migrate-platform-schema.ps1`，按编号顺序应用 `ry_sync_migration_002.sql` 至 `ry_sync_migration_015.sql` 共 14 个脚本；随后重复执行一轮，结果均成功。迁移只增加同步模块字段、任务组/核对/配置版本表和菜单权限，没有删除或改写既有任务数据。

迁移后基线为：7 个单表任务、2 个任务组、7 个任务组表项、1 个 DDL 事件、17 个配置版本和 38 个同步权限；2 个数据源凭证均为 `ENC_` 密文。后端 `http://localhost:18081/` 返回 200，前端数据源页面可见，迁移凭证操作和两条数据源连接测试均通过。

## 平台重启对账补充

2026-08-30 在隔离 POC 环境创建临时 `FULL_CDC` 作业 `mvp-closeout-reconcile`，源为 `ds-poc-mysql/source_db.customers`，目标为 `ds-poc-postgres/public.mvp_closeout_reconcile_target`。作业启动后保持 `RUNNING`，并产生连续完成的 checkpoint。

随后停止并使用相同 Java 21/AES 环境重新启动平台后端。`ApplicationReadyEvent` 的 `recoverRunningTasks()` 自动查询该任务并刷新 SeaTunnel 状态，平台数据库仍为 `RUNNING`，`engine_job_id` 未变化，`last_checkpoint_status=COMPLETED`；重启后插入的 CDC marker 成功传播到目标端。该结果证明平台状态和引擎 jobId/checkpoint 可在进程重启后重新对账，不等同于独立的 checkpoint restore 演练；restore 边界见 `recovery-drill-report.md`。

临时任务、目标表和 marker 均已清理，SeaTunnel 作业已停止。

## 扩大样本性能补充

使用 `test/scripts/performance-baseline.ps1 -RowCount 50000` 完成隔离环境基线，证据目录为 `test/results/20260830-171539`：

| 指标 | 实测 |
|---|---:|
| 全量样本 | 50,000 行 |
| 全量完成时间（提交至目标可见） | 52.973 s |
| 全量吞吐 | 943.88 行/秒 |
| CDC marker 延迟（单事件） | 3.000 s |
| 结束时源/目标 benchmark 行数 | 50,001 / 50,001 |

该数据是当前开发机和单并行度、1,000 行/秒限速下的容量回归基线，不代表生产千万级容量或 CDC P95 承诺。脚本完成后已停止作业并清理样本数据。

## MVP 收口结论

POC、单表/多表同步、恢复边界、监控、数据核对、凭证迁移、调度与覆盖刷新均已有可重复证据；平台重启状态对账和 50,000 行性能基线本轮补齐。Oracle、SQL Server、Kafka、Redis 目标端、主动告警和高可用仍属于 PRD Phase 2，不纳入当前 MVP。

## 最终清洁回归补充

2026-08-30 使用 `test/scripts/run-poc.ps1` 在重置后的隔离 `test/runtime` 中重新执行初始快照和 CDC 回归，覆盖 `customers` 单主键、`orders` 联合主键以及 INSERT/UPDATE/DELETE 传播；结果为 PASS，证据目录为 `test/results/20260830-174627`。回归结束后已停止临时 SeaTunnel 作业，后续 `stability-check.ps1` 检查源/目标行数一致且无运行作业，证据目录为 `test/results/20260830-180545`。

前端 `web` 项目 TypeScript 检查和生产构建均通过；已使用登录态浏览器完成发布前人工验收：数据源迁移后 MySQL/PostgreSQL 连接测试成功，任务详情、目标兼容性、CDC 前置检查、KEY_RANGE 数据核对、配置预览和缓存监控均正常，控制台无错误。8003 preview 已重建并确认任务/任务组状态使用中文显示。

本次收口复验还执行了 `ResourceProtectionPolicyTest` 2/2、`SyncTaskSchedulerTest` 2/2、稳定性检查（源端/目标端各 2 行且一致）和离线交付前置校验（9 项必需资产齐全）。
