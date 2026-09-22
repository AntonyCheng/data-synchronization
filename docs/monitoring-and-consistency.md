# 运行监控与数据核对

## 监控指标

单表状态接口 `POST /sync/task/{id}/status` 和任务组状态接口在刷新 SeaTunnel 作业状态时，同时读取引擎 `job-info.metrics` 投影到任务 / 表项详情：

- 阶段：`SNAPSHOT` / `CDC` / `MIXED`，由同步模式和引擎状态推导（`EngineJobStates.phaseOf`）：`FULL` 恒为 `SNAPSHOT`，`INCREMENTAL` 恒为 `CDC`；`FULL_CDC` 在引擎启动态（INITIALIZING/CREATED/PENDING/STARTING）为 `SNAPSHOT`，进入 RUNNING 后为 `MIXED`——Zeta 不暴露快照完成信号，平台不假装知道边界。
- 源端已读取、目标端已提交：行数和字节数；源端和目标端吞吐：引擎自己的 QPS。
- 积压：源端已读取 − 目标端已提交（不为负），是关系型目标唯一可得的"落后程度"指标。
- 端到端延迟：仅 Kafka 目标可得，由平台桥接按"源事件时间 → broker ack"计算；关系型目标显示为空，不以请求时间冒充延迟。

**Zeta 2.3.13 的两个事实**：`job-info.metrics` 把所有数值序列化成 JSON 字符串（`"SourceReceivedCount":"50"`），投影层同时接受字符串与数值；它不返回任何事件时间戳，因此关系型目标无法计算时间型 CDC 延迟。

### 指标历史

每次成功的引擎轮询（30 秒对账 + 手动刷新）为任务 / 表项追加一条采样到 `ds_sync_metrics_sample`；写入失败只告警，不影响对账本身。

- `GET /sync/task/{id}/metrics?minutes=60`、`GET /sync/group/{groupId}/item/{itemId}/metrics?minutes=60` 返回窗口内的采样序列（最早在前）和最新一条采样；窗口限制在 5～1440 分钟，超过 `sync.metrics.max-points`（1500）时等距抽稀。
- 任务列表 / 详情和任务组表项携带 `latestMetrics`（最新采样），列表页无需再点"刷新状态"即可看到吞吐与积压。
- 保留 `sync.metrics.retention-days`（默认 7 天）内的采样，每小时清理；删除任务 / 任务组时连带删除。
- 前端"运行指标历史"面板：吞吐（源端 / 目标端）与积压两张单轴折线图，Kafka 目标另有端到端延迟图，可选 15 分钟 / 1 小时 / 6 小时 / 24 小时窗口，30 秒自动刷新。

## Kafka 桥接的进程内对账

Kafka 目标的桥接 worker 运行在后端进程内。`KafkaBridgeReconciler` 在每个实例上每 `sync.kafka-bridge.reconcile-interval-ms`（30 秒）执行一次：为库中 `RUNNING` 的 Kafka 任务和存活任务组中 `RUNNING` 的表项确保本地 worker 存在，并停止其他一切 worker（在别的实例上被停止 / 暂停 / 重建的任务留下的 worker 因此不会变成僵尸消费者）。桥接需要与否的唯一规则是"表项 / 任务处于 RUNNING"；状态刷新里的即时修复遵循同一规则。

raw topic 只有一个分区，所以多个实例的 worker 加入同一 consumer group 时只有一个实际消费，其余待命；某实例崩溃后 Kafka 在下一次 rebalance 把分区交给幸存实例，平台不需要额外协调。启动失败（典型是输出 topic 不存在）每个 owner 只告警一次，恢复时再记一条。

## 状态告警（消息中心）

`SyncAlertNotifier` 每 `sync.alerts.interval-ms`（30 秒）扫描一次：单表任务进入 `FAILED` / `REINITIALIZE_REQUIRED`、任务组进入 `FAILED` / `REINITIALIZE_REQUIRED` / `DEGRADED`、存活任务组（`RUNNING` / `DEGRADED`）内的表项进入 `FAILED` / `DDL_BLOCKED` 时，通过 RuoYi 消息中心（`MessageService.publishAll`：写入 `sys_message` 系统分组 + SSE/WebSocket 推送给所有在线用户）发一条通知，正文为「对象 + 状态 + `last_error`」，`path` 指向任务 / 任务组页面，`data.title = 数据同步告警`，`data.level` 为 `error`（失败、需重新初始化）或 `warning`（降级、结构阻塞），前端据此弹红 / 黄色提示。

状态机不调用通知器。每行有 `alerted_status` 列记录"最近通知过的状态"：状态与之相同不再通知，离开告警集合时清空，再次进入才会再通知——所以一次失败无论经由引擎轮询、调度器、DDL 检查还是人工操作到达，都只通知一次；标记落库，重启和多实例都不会重复。任务组本身失败时不再为组内每张表单独通知。`sync.alerts.enabled=false` 关闭。

## 数据核对

`POST /sync/task/{id}/check` 默认执行源表和目标表 `COUNT(*)`，也支持通过请求体选择同步键范围分块核对：

```json
{"mode":"KEY_RANGE","blockSize":10000,"strictWatermark":true}
```

`KEY_RANGE` 当前 MVP 仅支持单列数值同步键，按 `[lowerBound, upperBound)` 逐块比较两端行数，并返回 `blocks`、`totalBlocks`、`matchedBlocks`、`mismatchedBlocks` 等汇总；联合键、非数值键或未配置同步键会明确提示改用 `COUNT`，不返回伪造的分块结论。`blockSize` 是同步键范围步长，服务端限制为 1 至 1000000。不读取或返回业务行内容。

对于 `FULL_CDC` 的 `RUNNING`/`PAUSING` 任务，`strictWatermark=true` 会拒绝核对并提示先暂停任务；这样不会把不同实时水位的行数误报为严格一致。静态任务或已暂停任务可执行分块核对。结果同时返回接口调用结果，并持久化到 `ds_sync_task` 的 `last_check_*` 字段，详情页可在刷新任务详情后看到最近一次核对时间、行数、差异和结论。

`POST /sync/group/{groupId}/check` 使用相同的只读口径逐表执行。每个表项的最近结果持久化在 `ds_sync_task_group_item.last_check_*`，任务组详情同时展示本次汇总和逐表最近结果。单项失败被记录为表级失败，不会中止其他表的核对；汇总仅说明行数检查结果，不掩盖失败表。

核对是只读操作。持续 CDC 运行期间源端和目标端可能处于不同水位，行数一致不等于严格同一时刻一致；需要严格结论时应在暂停作业或明确一致性水位后执行。核对失败仍记录时间和脱敏后的错误结论，匹配字段保持为空。

## MVP 边界

本阶段不引入自动 DDL 修复和邮件 / 短信等外部告警通道（告警只进消息中心）；指标历史只保留原始采样，不做聚合归档。
