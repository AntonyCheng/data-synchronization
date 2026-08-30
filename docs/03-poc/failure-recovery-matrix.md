# 异常与恢复矩阵

| 故障 | 预期平台状态 | 恢复方式 | 是否重新全量 | POC 结果 |
|---|---|---|---|---|
| SeaTunnel 进程重启 | 恢复中 -> 增量运行中 | 最近成功 checkpoint + 平台调用 restore | 否 | 通过；重启后 running-jobs 为空，restore 后 RUNNING |
| 平台进程重启 | 状态对账 | 重新发现 jobId | 否 | 设计要求；本轮未单独停止平台 API 进程 |
| PostgreSQL 短暂不可用 | 重试中/失败可恢复 | 恢复连接后继续 | 否 | 通过；故障期间更新最终补齐 |
| MySQL 短暂不可用 | 恢复中/失败可恢复 | 等待源端健康后从 savepoint restore | 否 | 通过；源端恢复未就绪时 restore 会失败，健康后重试进入 RUNNING |
| checkpoint 损坏 | 需重新初始化 | 丢弃旧恢复状态后重新全量 | 是 | 通过；损坏 `.ser` 后 restore 明确失败，修复文件后同一 jobId 恢复成功；证据 `test/results/20260824-213758` |
| binlog 已过期 | 需重新初始化 | 丢弃旧恢复状态后重新全量 | 是 | 通过；`RESET MASTER` 后引擎报 `mysql-bin.000003 is no longer available`，作业进入 FAILED；证据 `test/results/20260824-214420` |
| 高风险 DDL | 表暂停/整组暂停 | 人工处理后恢复 | 视情况 | 新增字段实测会导致 sink 失败；补齐目标列后 restore 恢复 |
| 目标表类型不兼容 | 创建阻断 | 修复结构后重新校验 | 否 | 通过；`jsonb` 目标列被阻断，调整为 `text` 后重新校验通过，证据 `test/results/20260825-incremental-mode` |
| 整库发现到无同步键表 | 表项失败，组降级 | 修复表主键或非空唯一键后重新发现/启动该表 | 否 | 通过；两张失败表不阻断三张可同步表，组为 `DEGRADED`，证据 `test/results/20260826-database-discovery` |

## 2026-08-25 模式验收补充

| 模式 | 验证结果 | 证据/说明 |
|---|---|---|
| `FULL` + `OVERWRITE` | 通过 | 任务完成后为 `FINISHED`；目标端加入额外行再次执行后被清除，恢复为源端 2 行 |
| `FULL_CDC` | 已通过 | 任务组和单表此前均完成全量、增删改及生命周期验收 |
| `INCREMENTAL` | 通过（含失败记录回归） | `test/results/20260825-incremental-mode/acceptance.md`；`jsonb` 目标先被兼容性检查阻断，调整为 `text` 后完成 latest 位点、增删改和停止验收；失败堆栈按 UTF-8 安全截断后可正常落库 |

## 平台状态分类补充

SeaTunnel 作业最终状态为 `FAILED` 时，平台会继续检查引擎错误文本。包含 checkpoint、savepoint、restore、binlog、offset 或“日志已过期/不再可用”等恢复边界语义时，平台状态升级为 `REINITIALIZE_REQUIRED`，前端只展示重新初始化操作，不允许静默从最新位点继续。
