# POC 验证报告

## 状态

已完成核心链路验证；DDL 自动变更和大规模性能仍不在本轮结论内。

> 证据目录中的时间戳使用本机执行时间；`manual-20260820-140020` 是首轮全量+CDC，`20260820-152430` 是脚本化 savepoint/restore。

## 环境信息

| 项目 | 实际值 |
|---|---|
| 执行日期 | 2026-08-20 |
| Docker Desktop | 本机 Docker Desktop，Compose 项目 `data-sync-poc` |
| SeaTunnel | 2.3.13 |
| MySQL | 8.0 |
| PostgreSQL | 17.6 |

## 结果摘要

| 场景 | 状态 | 证据 |
|---|---|---|
| 环境启动与健康检查 | 通过 | `test/results/manual-20260820-140020` |
| 全量+增量 | 通过 | `test/results/manual-20260820-140020` |
| INSERT/UPDATE/DELETE | 通过 | `test/results/manual-20260820-140020` |
| 联合主键 | 通过 | `test/results/manual-20260820-140020` |
| 非空唯一键 | 通过（独立作业） | `test/results/20260820-153313` |
| checkpoint 重启恢复 | 通过（重启后需适配器调用 restore） | `test/results/seatunnel-restart-20260820-141300` |
| savepoint/restore | 通过 | `test/results/20260820-152430` |
| 目标端故障恢复 | 通过 | `test/results/target-outage-20260820-141200` |
| 源端故障恢复 | 通过（需等待 MySQL 健康后 restore） | `test/results/mysql-outage-20260820-154000` |
| DDL 与新增表 | 新增字段被捕获但目标未自动变更，任务失败并需人工修复 | `test/results/ddl-add-column-20260820-141500` |
| 类型与字符集 | 通过（JSON、DECIMAL、DATETIME(6)、Unicode/emoji） | `test/results/manual-20260820-140020` |
| 性能基线 | 未完成（当前仅小数据正确性样本） | `docs/03-poc/performance-baseline.md` |

## 已发现并解决的问题

1. Windows bind mount 的 MySQL 配置文件会被容器识别为 world-writable，MySQL 因安全策略忽略该文件。POC 改为通过 Compose `command` 显式传入 binlog、GTID 和字符集参数。
2. SeaTunnel 官方 2.3.13 镜像同时包含 PostgreSQL JDBC 与 openGauss JDBC，后者也打包了 `org.postgresql.Driver` 并发生类路径抢占，连接 PostgreSQL 17 时出现 `Protocol error. Session setup failed`。定制镜像移除内置旧驱动和 `opengauss-jdbc`，只保留锁定的 PostgreSQL JDBC 42.7.5。

## 结论

SeaTunnel 2.3.13 在本隔离环境中可以作为 MySQL 8.0 ROW/GTID binlog 到 PostgreSQL 17.6 的 MVP 执行引擎：initial 快照后连续 CDC、增删改、联合主键、非空联合唯一键、幂等 upsert/delete、savepoint/restore、目标端短暂不可用重试均通过。平台必须持久化 checkpoint，并在引擎进程重启后重新发现作业并主动 restore；引擎不会自动重新提交作业。当前关闭自动 DDL，新增字段会让目标写入失败，生产实现必须先做 DDL 兼容性检查并按表隔离或人工处理。结果不能外推到千万级吞吐，性能基线需单独执行。
