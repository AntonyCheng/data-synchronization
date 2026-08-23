# SeaTunnel 作业管理适配器契约草案

## 目的

业务模块只依赖本适配器，不直接拼接 SeaTunnel REST 或命令行参数。

## 操作

| 操作 | 输入 | 输出 | SeaTunnel 候选实现 |
|---|---|---|---|
| validate | 配置版本 | 校验结果 | 配置检查 + 平台预检查 |
| submit | 配置版本、任务标识 | jobId | REST submit-job |
| status | jobId | 标准任务状态与指标 | REST job-info/checkpoint API |
| pause | jobId | savepoint 结果 | 官方 savepoint 控制能力 |
| resume | jobId、原配置版本 | 新运行状态 | 官方 restore 控制能力 |
| stop | jobId | 已停止 | REST stop-job/cancel |
| logs | jobId | 脱敏日志 | REST logs |
| reconcile | 平台任务集合 | 状态差异 | running/finished jobs API |

## 约束

- 所有操作必须幂等或带请求幂等键。
- jobId 与配置版本一同保存。
- 恢复时必须校验配置版本一致。
- 对新目标表建表，适配器必须把解析后的同步键列显式写入 `primary_keys`；不能把 `${primary_key}` 这类运行时模板变量直接传给 JDBC sink。
- 适配器错误转换为平台错误码，不把引擎堆栈直接返回前端。
- 密码不得出现在参数日志、错误日志和审计详情中。
