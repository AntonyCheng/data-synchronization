# 运行时表结构变更管理

## MVP 原则

- 平台只检测和记录 DDL，不自动修改 PostgreSQL 目标表。
- 每个表项在首次成功启动或发现时保存源表结构快照及 SHA-256 指纹。
- 周期检查和人工“结构检查”均以该快照为基线，比较字段名称、类型、长度、精度、可空性和主键列顺序。
- 新增可空字段归类为低风险；字段删除、类型/长度/精度/可空性变化以及同步键变化归类为高风险。

## 处置流程

1. 检测到差异后写入 `ds_sync_task_group_ddl_event`，保留变更详情、风险等级和修复建议。
2. 对仍有引擎作业的表项调用 savepoint 停止，并将表项状态置为 `DDL_BLOCKED`；同组其他表继续运行。
3. 目标表修复后重新执行结构检查。兼容性通过时事件变为 `READY_TO_RESUME`，否则维持 `PENDING_FIX`。
4. 仅允许对 `READY_TO_RESUME` 的表项执行“恢复该表”。恢复使用原 jobId/savepoint，成功后更新源表结构基线并将事件置为 `RESOLVED`。
5. 结构变化导致引擎配置指纹变化时禁止直接恢复，必须创建新配置版本并重新初始化该表。

## 接口

- `POST /sync/group/{groupId}/ddl-check`：检查组内表结构并返回事件列表。
- `POST /sync/group/{groupId}/item/{itemId}/resume-after-ddl`：在兼容性检查通过后恢复单个表项。

## 状态语义

`DDL_BLOCKED` 是表项状态，组级状态按现有聚合规则表现为 `DEGRADED`（仍有其他表运行）或 `FAILED`（没有可运行表）。待处理 DDL 事件不会被 SeaTunnel 的普通 `FAILED` 错误覆盖。
