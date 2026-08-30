# 性能基线

## 测试条件

本轮基线使用 `test/scripts/performance-baseline.ps1`，默认向 `customers` 写入 5,000 行独立 ID 区间数据，并在测试结束后清理。脚本拒绝与现有 SeaTunnel 作业并行运行，不重置 POC 数据库。该基线用于比较本地环境的相对变化，不能替代生产容量压测。

## 指标

| 指标 | 目标/说明 | 实测 |
|---|---|---|
| 全量吞吐（行/秒） | 由样本量和目标可见时间计算 | `test/results/<timestamp>/metrics.json` |
| 全量吞吐（MB/秒） | 含大字段 | 待测 |
| CDC 峰值吞吐（事件/秒） | 本脚本只测单事件延迟；峰值压测另行执行 | 待测 |
| CDC P95 延迟 | 本脚本记录单个 marker 延迟；P95 需重复采样 | `test/results/<timestamp>/metrics.json` |
| checkpoint 时长与大小 | 稳态任务 | 已观察 3-10 ms、stateSize=3（小样本） |
| SeaTunnel 重启恢复时间 | 从启动到继续写入 | 已观察约 30-60 s（含容器启动与 restore 提交，小样本） |
| MySQL CPU/IO 增幅 | Compose 容器快照 | `test/results/<timestamp>/docker-stats.txt` |
| PostgreSQL 写入资源 | Compose 容器快照；WAL 需专项采集 | `test/results/<timestamp>/docker-stats.txt` |

## 2026-08-23 本地实测

执行命令：`test/scripts/performance-baseline.ps1 -RowCount 5000`。结果目录：`test/results/20260823-225803`。

| 指标 | 实测 |
|---|---:|
| 全量样本 | 5,000 行 |
| 全量完成时间（含提交和引擎启动） | 6.371 s |
| 全量吞吐 | 784.87 行/秒 |
| CDC marker 延迟（单事件） | 6.000 s |
| 结束时源/目标 benchmark 行数 | 5,001 / 5,001 |

该结果包含本机容器启动、checkpoint 和轮询开销，只能作为当前开发机的回归基线；它不是持续峰值吞吐或 P95 承诺。

## 2026-08-30 扩大样本实测

执行命令：`test/scripts/performance-baseline.ps1 -RowCount 50000`。结果目录：`test/results/20260830-171539`。

| 指标 | 实测 |
|---|---:|
| 全量样本 | 50,000 行 |
| 全量完成时间（含提交和引擎启动） | 52.973 s |
| 全量吞吐 | 943.88 行/秒 |
| CDC marker 延迟（单事件） | 3.000 s |
| 结束时源/目标 benchmark 行数 | 50,001 / 50,001 |

容器快照记录在 `docker-stats.txt`：MySQL 0.78% CPU/394.9 MiB，PostgreSQL 0.01% CPU/65.48 MiB，SeaTunnel 1.18% CPU/775.1 MiB。该结果仅用于本地相对回归，受单并行度和 1,000 行/秒限速影响，不能外推生产容量或 CDC P95。

## 执行

```powershell
Set-Location test
.\scripts\performance-baseline.ps1 -RowCount 5000
```

结果目录会保存 `metrics.json`、`job-info.json`、`checkpoints.json`、`docker-stats.txt` 和清理/失败信息。需要进行峰值吞吐或 P95 测试时，应扩展为固定时间窗口、多批次事件和 Prometheus/数据库指标采集，不应把单 marker 结果当作 P95。
