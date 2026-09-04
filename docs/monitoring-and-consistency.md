# 运行监控与数据核对

## 监控指标

单表状态接口 `POST /sync/task/{id}/status` 在刷新 SeaTunnel 作业状态时，同时读取引擎 `job-info.metrics` 投影到任务详情：

- 阶段：`SNAPSHOT` 或 `CDC`。MVP 根据引擎作业状态映射，SeaTunnel 尚未提供独立快照完成事件时采用保守映射。
- 源端已读取、目标端已提交：行数和字节数（引擎返回时展示）。
- 源端和目标端吞吐：QPS（引擎返回时展示）。
- CDC 延迟：只有引擎同时返回源事件时间和目标提交时间，且时间顺序可信时计算；否则明确显示“暂不可计算”，不能以请求时间代替延迟。

指标是手动点击“刷新状态”时读取的 MVP 快照，不是服务端推送。引擎未返回某项指标时，接口返回空值和 `metricsMessage`，不阻断状态刷新。

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

本阶段不引入主动告警、历史时序指标、自动 DDL 修复或非 MySQL -> PostgreSQL 目标。
