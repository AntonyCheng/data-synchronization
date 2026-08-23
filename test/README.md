# MySQL -> PostgreSQL SeaTunnel POC

本目录包含完全隔离、可重复执行的 POC 环境。它不会操作现有的 `mysql` 或 `pgvector` 容器。

## 端口与容器

| 服务 | 容器 | 主机端口 | 测试数据库 |
|---|---|---|---|
| MySQL | `ds-poc-mysql` | `13306` | `source_db` |
| PostgreSQL | `ds-poc-postgres` | `15432` | `sink_db` |
| SeaTunnel | `ds-poc-seatunnel` | `18080` | REST API |

Compose 项目名固定为 `data-sync-poc`，所有持久化内容位于 `runtime/`，测试证据位于 `results/`。

## 使用

```powershell
# 构建镜像并启动环境
.\test\scripts\up.ps1

# 提交全量+CDC 作业
.\test\scripts\submit-job.ps1

# 执行首轮数据正确性 POC
.\test\scripts\run-poc.ps1

# 暂停、保存点、恢复，并验证暂停期间写入的数据
.\test\scripts\pause-restore.ps1

# 验证无主键、非空联合唯一键的全量和 CDC
.\test\scripts\verify-unique-key.ps1

# 停止 POC 容器，保留所有数据
.\test\scripts\down.ps1

# 明确确认后重置 POC 数据（不会删除 results/）
.\test\scripts\reset-poc.ps1 -Force
```

`down.ps1` 不会删除数据库数据、checkpoint 或结果目录。`run-poc.ps1` 的初始快照断言要求干净基线；需要重跑时使用带 `-Force` 的 `reset-poc.ps1`，它只清理 `test/runtime/`，保留 `test/results/`。

## 目录

- `docker/`：数据库、SeaTunnel 镜像与运行配置。
- `jobs/`：SeaTunnel 作业定义。
- `cases/`：源端变更和目标端断言 SQL。
- `scripts/`：环境、提交和验证脚本。
- `runtime/`：数据库、checkpoint 和日志挂载目录。
- `results/`：每次执行的结果与证据。

## 已知运行语义

- `docker restart ds-poc-seatunnel` 只会重启 SeaTunnel 引擎，不会自动重新提交已运行作业；平台适配器必须发现失联作业并调用 `restore`，本 POC 可用 `restore-job.ps1` 完成恢复。
- 当前 MySQL CDC 配置关闭自动 DDL（`schema-changes.enabled=false`）。新增字段会被 CDC 捕获，但不会自动修改目标表；应由平台按 DDL 策略暂停受影响表并人工确认。
