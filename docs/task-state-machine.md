# 任务状态机

MVP 状态集合：`DRAFT`（草稿）、`RUNNING`（运行中）、`PAUSING`（暂停中）、`PAUSED`（已暂停）、`STOPPED`（已停止）、`FAILED`（失败可恢复）、`REINITIALIZE_REQUIRED`（需重新初始化）、`FINISHED`（已完成）。

当前已交付配置阶段的规则：

- 新建任务进入 `DRAFT`。
- 只有 `DRAFT`、`STOPPED`、`REINITIALIZE_REQUIRED` 允许修改。
- `DRAFT`、`STOPPED`、`FAILED`、`FINISHED`、`REINITIALIZE_REQUIRED` 允许删除；运行中和暂停中任务不允许删除。删除仅清理任务配置和引擎恢复状态，不删除目标端已同步数据。
- `validate` 只校验任务引用的源端和目标端 JDBC 连接，不改变状态，也不提交 SeaTunnel 作业。
- `engine-config` 只生成脱敏配置预览，不改变状态，也不写入 `engine_job_id`。

引擎适配器阶段再补充以下转换：

```text
DRAFT -> RUNNING       提交并启动 SeaTunnel 作业
RUNNING -> STOPPED     主动停止成功
RUNNING -> FAILED      引擎异常但仍可能从 checkpoint 恢复
FAILED -> RUNNING      按相同配置版本从 checkpoint/savepoint 恢复
FAILED -> REINITIALIZE_REQUIRED  checkpoint 损坏、binlog 过期或恢复位点不可用
REINITIALIZE_REQUIRED -> RUNNING  丢弃旧恢复状态，执行新的全量初始化
STOPPED -> RUNNING     重新启动（不承诺复用旧恢复状态）
```

`REINITIALIZE_REQUIRED` 禁止普通“启动”和“恢复”直接绕过；纯增量任务不能在原任务内自动重新初始化，需新建全量或全量 + CDC 任务建立可信基线。

所有状态转换必须记录操作人、时间、引擎作业 ID 和错误摘要，避免仅依赖前端展示状态。
