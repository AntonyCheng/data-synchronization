# 调度与全量写入安全

本阶段对应 PRD 的“调度与编排”MVP 要求，范围限定为单任务调度，不引入任务依赖编排。

## 调度模式

`ds_sync_task.schedule_mode` 支持 `MANUAL`、`ONCE`、`CRON`、`REALTIME`。`MANUAL` 兼容迁移前任务；`ONCE` 保存后触发一次；`CRON` 使用 Spring 六位 Cron（秒、分、时、日、月、周）；`REALTIME` 表示常驻 CDC，需首次手动启动且不由 Cron 重复触发。

调度器每 15 秒扫描到期任务。每个任务复用启动锁 `sync:task:start:{taskId}`（与手动启动同一把锁），锁获取失败不重复提交。运行中（`RUNNING`/`PAUSING`）的任务不会并发启动，写入 `last_skip_reason` 后推进下一次 Cron 时间。触发时间写入 `last_trigger_time`，页面展示 `next_run_time` 和最近跳过原因。

`next_run_time` 为空表示调度已挂起，三种情况会挂起：手动停止；`ONCE` 已触发；到期时任务处于 `FAILED`、`REINITIALIZE_REQUIRED` 或 `PAUSED`——这三种状态不能由调度器直接启动，调度器把原因写入 `last_skip_reason` 并清空 `next_run_time`，而不是让过期的 `next_run_time` 一直挂在页面上。操作员通过启动、恢复或重新初始化把任务带回 `RUNNING` 时，`CRON` 任务的调度自动重新武装（`SyncSchedules.rearm`）。

## 配置版本

新任务从配置版本 1 开始；编辑任务生成新版本，并在 `ds_sync_task_config_version` 写入无凭证、不可变 JSON 快照。恢复逻辑继续校验引擎配置指纹，禁止使用新配置恢复旧 checkpoint。

## 覆盖刷新

`FULL`/`OVERWRITE` 任务启动时写入版本化临时表 `__ds_stage_{taskId}_v{configVersion}`，引擎仅对临时表执行 `DROP_DATA` 和写入。引擎状态只有在 `FINISHED` 后才收尾：平台在 PostgreSQL 事务内将正式表改名为备份、临时表改名为正式表并删除备份。任一 DDL 失败事务回滚，任务标记 `FAILED`，原正式表保持可用。

如果目标数据库不支持事务性表替换，应在兼容性矩阵中标记为维护窗口场景；当前实现不把半成品标记为成功。

## 运维参数

可通过 `sync.schedule.interval-ms` 和 `sync.schedule.initial-delay-ms` 调整扫描间隔和启动延迟。调度器依赖现有 Redisson/Redis，不触碰业务容器。

### 后台轮询的线程池

调度触发与 sync 模块其余全部后台轮询（任务 / 任务组状态对账、整库发现、DDL 检查、Kafka 桥接对账、孤儿作业清扫、告警、指标清理，共 9 个）都运行在模块自己的调度线程池上（`SyncSchedulingConfig`，线程名 `sync-sched-*`），不再与其他模块共用 RuoYi 的全局 `schedule-pool`（cores + 1 线程）。引擎不可达时，一轮状态对账可能被每次 10 秒的 REST 超时拖住数分钟；隔离之后它最多拖慢 sync 自己的轮询，不会饿死框架和其他模块的定时任务。

- `sync.scheduler.pool-size`（默认 9）：每个轮询都是 `fixedDelay`，不会与自身重叠，所以 9 = 每个轮询一条线程、互不排队，再大也用不上；调小则引擎不可达时卡住的轮询会推迟告警、桥接对账等其余轮询。新增 `@Scheduled` 轮询必须写 `scheduler = SyncSchedulingConfig.SCHEDULER`（`SyncSchedulingConfigTest` 会检查），并相应调大默认值。
- 停机：应用关闭时先停止触发新一轮，正在执行的一轮允许跑完（受 `spring.lifecycle.timeout-per-shutdown-phase` 约束）后再销毁数据源等 bean，不会在写库中途被中断。
- `dev-fast`（延迟初始化）下同样生效：`@Scheduled` bean 由 `DevFastLazyInitConfig` 保持 eager，注册定时任务时按 bean 名解析 `syncScheduler`，延迟的调度器 bean 随之被创建。
