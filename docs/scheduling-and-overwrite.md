# 调度与全量写入安全

本阶段对应 PRD 的“调度与编排”MVP 要求，范围限定为单任务调度，不引入任务依赖编排。

## 调度模式

`ds_sync_task.schedule_mode` 支持 `MANUAL`、`ONCE`、`CRON`、`REALTIME`。`MANUAL` 兼容迁移前任务；`ONCE` 保存后触发一次；`CRON` 使用 Spring 六位 Cron（秒、分、时、日、月、周）；`REALTIME` 表示常驻 CDC，需首次手动启动且不由 Cron 重复触发。

调度器每 15 秒扫描到期任务。每个任务使用 Redis 锁 `sync:task:schedule:{taskId}`，锁获取失败不重复提交。运行中或暂停中的任务不会并发启动，写入 `last_skip_reason` 后推进下一次 Cron 时间。触发时间写入 `last_trigger_time`，页面展示 `next_run_time` 和最近跳过原因。

## 配置版本

新任务从配置版本 1 开始；编辑任务生成新版本，并在 `ds_sync_task_config_version` 写入无凭证、不可变 JSON 快照。恢复逻辑继续校验引擎配置指纹，禁止使用新配置恢复旧 checkpoint。

## 覆盖刷新

`FULL`/`OVERWRITE` 任务启动时写入版本化临时表 `__ds_stage_{taskId}_v{configVersion}`，引擎仅对临时表执行 `DROP_DATA` 和写入。引擎状态只有在 `FINISHED` 后才收尾：平台在 PostgreSQL 事务内将正式表改名为备份、临时表改名为正式表并删除备份。任一 DDL 失败事务回滚，任务标记 `FAILED`，原正式表保持可用。

如果目标数据库不支持事务性表替换，应在兼容性矩阵中标记为维护窗口场景；当前实现不把半成品标记为成功。

## 运维参数

可通过 `sync.schedule.interval-ms` 和 `sync.schedule.initial-delay-ms` 调整扫描间隔和启动延迟。调度器依赖现有 Redisson/Redis，不触碰业务容器。
