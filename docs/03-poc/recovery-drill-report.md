# 故障恢复演练报告

执行日期：2026-08-24

## 范围

本轮只操作 `ds-poc-*` 隔离容器，验证 PRD 中的恢复边界：

- savepoint 暂停与恢复；
- SeaTunnel 进程重启后使用同一 jobId 和 checkpoint 恢复；
- checkpoint 状态损坏时拒绝恢复；
- MySQL binlog 已过期时拒绝从旧位点继续；
- 修复恢复状态后不重复执行全量，源/目标行数保持一致。

## 结果

| 用例 | 结果 | 证据 |
|---|---|---|
| savepoint/restore | PASS | `test/results/20260824-212528` |
| SeaTunnel 重启后恢复 | PASS | `test/results/20260824-212707` |
| checkpoint 损坏、修复后恢复 | PASS | `test/results/20260824-213758` |
| binlog 过期错误注入 | PASS | `test/results/20260824-214420` |
| 故障演练后干净 POC 回归 | PASS | `test/results/20260824-214826` |

## 关键观察

1. SeaTunnel 进程重启后 `running-jobs` 为空，平台/适配器必须显式调用 restore；恢复日志显示已处理快照表为空，未重新执行全量。
2. 损坏 `.ser` checkpoint 时 restore 失败；恢复原文件后同一 jobId 可以继续运行。
3. 执行 `RESET MASTER` 后，SeaTunnel 作业最终进入 `FAILED`，错误包含 `mysql-bin.000003 ... no longer available`。
4. 平台状态刷新现在会检查 FAILED 作业的错误文本；涉及 checkpoint、savepoint、restore、binlog 或 offset 不可恢复时，统一落为 `REINITIALIZE_REQUIRED`。

## 执行方式

```powershell
pwsh -File test/scripts/pause-restore.ps1
docker restart ds-poc-seatunnel
pwsh -File test/scripts/restore-job.ps1 -JobId <jobId>
pwsh -File test/scripts/recovery-drill.ps1 -Scenario checkpoint
pwsh -File test/scripts/recovery-drill.ps1 -Scenario binlog
```

`binlog` 用例会破坏隔离 POC 的 binlog；执行后使用 `test/scripts/reset-poc.ps1 -Force` 和 `test/scripts/run-poc.ps1` 恢复干净环境。平台元数据库、现有 MySQL/PostgreSQL 和 `pptmaster-*` 容器不在脚本操作范围内。

## 未覆盖项

平台进程重启对账已在 2026-08-30 补充完成：后端启动时自动扫描 RUNNING/PAUSING 任务，重新获取同一 SeaTunnel jobId 的运行状态和 checkpoint，平台任务保持 RUNNING 且 CDC marker 可继续写入目标。该用例验证的是状态重新发现，不替代 SeaTunnel checkpoint restore；restore 结论仍以本报告前述演练为准。

本轮未使用真实登录会话调用平台的 `POST /sync/task/{taskId}/reinitialize` 完成 UI 端到端状态变更；接口实现、权限和前端按钮已完成，属于后续浏览器回归项。
