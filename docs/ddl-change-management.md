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
5. 结构变化导致引擎配置指纹变化时禁止直接恢复（典型情况：表项启动时选择了全部字段，源表新增字段后生成器必须插入投影 transform，指纹随之变化）。此时使用“重新初始化该表”。

## 表级重新初始化

`POST /sync/group/{groupId}/item/{itemId}/reinitialize` 只重建一张表，其他表项不受影响：

1. 强制停止该表项的旧作业（可能已不存在，忽略），停止其 Kafka 桥接。
2. 按当前源表结构重新推导字段选择：**若表项启动时的选择覆盖了基线快照的全部字段，则视为“跟随整表”，自动纳入新增字段；否则保留用户的显式选择**（被删除的字段会导致校验失败，需要编辑任务组）。同步键不重新选择，同步键变化同样需要编辑任务组。
3. 目标兼容性必须通过（例如目标表尚未补齐新增字段时会被拒绝），整库 Kafka 组会确保 topic 存在。
4. 重拍源表结构基线，全新提交作业（无 savepoint，重新全量同步），清空该表项的 checkpoint 列，关闭该表项的待处理 DDL 事件（`RESOLVED`），重算组级状态。

允许的表项状态：`FAILED`、`DDL_BLOCKED`、`STOPPED`、`FINISHED`；任务组处于 `DRAFT` 或 `PAUSING` 时拒绝。

## 接口

- `POST /sync/group/{groupId}/ddl-check`：检查组内表结构并返回事件列表。
- `POST /sync/group/{groupId}/item/{itemId}/resume-after-ddl`：在兼容性检查通过后恢复单个表项。
- `POST /sync/group/{groupId}/item/{itemId}/reinitialize`：丢弃单个表项的作业与 savepoint 并重新全量同步。

## 状态语义

`DDL_BLOCKED` 是表项状态，组级状态按现有聚合规则表现为 `DEGRADED`（仍有其他表运行）或 `FAILED`（没有可运行表）。待处理 DDL 事件不会被 SeaTunnel 的普通 `FAILED` 错误覆盖。
