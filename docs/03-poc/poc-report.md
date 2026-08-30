# POC 验证报告

## 状态

已完成核心链路验证；DDL 自动变更和大规模性能仍不在本轮结论内。

本轮复验日期：2026-08-25。由于元数据库 `dbs-mysql` 已占用主机 `13306`，POC 使用默认主机端口 `23306`（MySQL）、`25432`（PostgreSQL）和 `18080`（SeaTunnel REST），不触碰现有业务容器。

> 证据目录中的时间戳使用本机执行时间；`manual-20260820-140020` 是首轮全量+CDC，`20260820-152430` 是脚本化 savepoint/restore。

## 环境信息

| 项目 | 实际值 |
|---|---|
| 执行日期 | 2026-08-20 |
| Docker Desktop | 本机 Docker Desktop，Compose 项目 `data-sync-poc` |
| SeaTunnel | 2.3.13 |
| MySQL | 8.0 |
| PostgreSQL | 17.6 |
| 本轮主机端口 | MySQL `23306`、PostgreSQL `25432`、SeaTunnel REST `18080` |

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
| 性能基线 | 通过（5,000 行本地回归样本；非生产容量结论） | `test/results/20260823-225803` |

### 本轮复验证据

| 场景 | 状态 | 证据目录 |
|---|---|---|
| 全量 + INSERT/UPDATE/DELETE | 通过 | `test/results/20260823-183043` |
| savepoint 暂停、暂停期间隔离、恢复后 CDC | 通过 | `test/results/20260823-183115` |
| 后端同步模块 Java 21 编译 | 通过 | `server/ruoyi-modules/ruoyi-sync/target/` |
| 前端 TypeScript 检查与生产构建 | 通过 | `web/dist/` |

### 2026-08-25 任务组平台端到端验收

按 PRD 顺序对任务组 `TEST platform group fixed`（`2092223918949117953`）执行列表、详情、批量校验、配置预览、启动、状态刷新及生命周期操作。任务组包含 `customers` 单主键表和 `orders` 联合主键表，两个表项分别提交独立 SeaTunnel job（`1144608225996308481`、`1144608225996505089`）。

- 源/目标连接、MySQL binlog CDC 前置检查和两张目标表兼容性检查全部通过。
- 配置预览返回两段脱敏配置，密码均为 `******`，联合主键和逐表 server-id 正确生成。
- 源端对两张表各执行 UPDATE、DELETE、INSERT，目标端六项断言全部通过。
- 组级生命周期 `RUNNING -> PAUSING -> PAUSED(SAVEPOINT_DONE) -> RUNNING -> STOPPED` 通过，最终两个引擎作业均为 `CANCELED`。

详细 API 返回与数据断言见 `test/results/20260825-platform-group/acceptance.md`。

### 2026-08-25 单表模式与类型兼容性验收

完成 `FULL + OVERWRITE`、`FULL_CDC` 和 `INCREMENTAL` 契约复核。纯增量任务使用 `startup.mode=latest`，启动前历史数据不会补同步，启动后 INSERT/UPDATE/DELETE 均已验证。发现 PostgreSQL `jsonb` 目标列不能接受 SeaTunnel JDBC 的字符串绑定后，已将兼容性检查改为提交前阻断，并在目标列改为 `text` 后重新完成增量验收。此前 SeaTunnel 长堆栈写入 `last_error` 超出 MySQL 字节限制的问题也已修复，状态接口和错误记录回归通过。详细证据见 `test/results/20260825-incremental-mode/acceptance.md`。

### 2026-08-26 整库新增表发现与逐表隔离验收

隔离任务组 `TEST database discovery` 使用 `DATABASE` 同步范围发现五张源表。`customers`、`orders` 及新发现的 `poc_discovered_orders` 分别提交独立 SeaTunnel 作业并进入 `RUNNING`；无同步键的 `inventory_by_sku`、`no_key_platform` 仅标记为 `FAILED`，不阻断其余表启动，组级状态聚合为 `DEGRADED`。对新发现表完成首次全量及 INSERT、UPDATE、DELETE binlog CDC 验证后停止所有测试作业，`running-jobs` 为空。详细证据见 `test/results/20260826-database-discovery/acceptance.md`。

### 2026-08-26 运行时 DDL 变更隔离验收

对 `poc_discovered_orders` 增加可空字段 `note` 后，结构检查识别为低风险 `ADD_COLUMN`，仅该表进入 `DDL_BLOCKED` 并完成 savepoint，`customers` 与 `orders` 保持运行。目标端补齐字段后事件变为 `READY_TO_RESUME`，原 jobId 从 savepoint 恢复并回到 `RUNNING`。平台未自动修改目标表结构。详细证据见 `test/results/20260826-ddl-change/acceptance.md`。

### 2026-08-26 运行监控与核对结果验收

完成监控指标和核对结果持久化的接口回归。对已结束的单表 SeaTunnel 作业刷新状态，接口返回 200，任务状态为 `FAILED`，阶段为 `CDC`；SeaTunnel 未提供源事件时间时返回明确的“CDC 延迟暂不可计算”，不会因空引擎状态触发 500。对任务 `POC customers CDC` 执行数据核对，源表 `source_db.customers` 与目标表 `public.customers` 均为 4 行、差异为 0，结果为一致，并确认六个 `last_check_*` 字段已写入 `ds_sync_task`。

### 2026-08-27 任务组逐表数据核对验收

对隔离整库任务组 `TEST database discovery`（`2092300000000000001`）调用 `POST /sync/group/{groupId}/check`。5 张表均得到独立结果：`customers` 为 4/4、`orders` 为 3/3、`poc_discovered_orders` 为 1/1，三张表行数一致；`inventory_by_sku` 与 `no_key_platform` 的目标表不存在，分别记录为表级失败。组级汇总为“一致 3、不一致 0、失败 2”，其余表没有因失败项被中断。查询 `ds_sync_task_group_item` 已确认所有表项写入最近检查时间、行数或脱敏失败结论。

### 2026-08-23 前端端到端复验

通过前端页面创建并校验 `POC MySQL`、`POC PostgreSQL` 数据源及 `POC customers CDC` 任务后，点击“启动”提交 SeaTunnel 作业，作业进入 `RUNNING`，job ID 为 `1143894613568847873`。由于 SeaTunnel 容器不能访问宿主机 `127.0.0.1`，本次 UI 数据源地址改为宿主机可达地址 `192.168.1.9`，未修改任何容器端口或业务容器。

目标 PostgreSQL 首次全量得到 3 行；随后在源 MySQL 执行 UPDATE、INSERT、DELETE 各一次，目标端最终仍为 3 行且内容与源端一致，确认前端提交、SeaTunnel 作业、MySQL binlog CDC 和 PostgreSQL 写入完整贯通。

### 2026-08-23 前端生命周期验收

通过前端对同一任务完成 `RUNNING -> PAUSING -> PAUSED -> RUNNING -> STOPPED`：

- `PAUSED` 且 `SAVEPOINT_DONE` 后插入源端测试行，等待 10 秒，目标端行数保持为 0；恢复后该行成功同步。
- `STOPPED` 且引擎返回 `CANCELED` 后插入源端测试行，等待 10 秒，目标端仍为 0。
- 验收过程中发现并停止了一个早前脚本遗留的并行 POC 作业；清理后重新执行，避免并行作业干扰结论。

### 2026-08-23 MVP 稳定性验收

- 数据核对：前端“数据核对”弹窗返回源表 `source_db.customers` 3 行、目标表 `public.customers` 3 行、差异 0，结果为“源端和目标端行数一致”。
- 引擎不可达：停止 `ds-poc-seatunnel` 后刷新任务状态，页面提示 `UNREACHABLE`，任务持久化为 `FAILED`。
- 引擎恢复：重新启动 `ds-poc-seatunnel` 后从 `FAILED` 重新启动任务成功进入 `RUNNING`，随后停止并清理回 `STOPPED`。
- 应用启动恢复扫描已执行；启动日志中可见对 `RUNNING`/`PAUSING` 任务的查询，恢复逻辑不会因引擎不可达阻断应用启动。

### 2026-08-23 性能基线

使用独立 ID 区间运行 5,000 行 `customers` 全量 + 单 CDC marker，测试前后自动清理，未重置 POC 数据。全量达到目标行数耗时 6.371 秒，约 784.87 行/秒；CDC marker 端到端延迟约 6 秒；源/目标 benchmark 行数均为 5,001。容器 CPU/内存快照和 SeaTunnel checkpoint 见 `test/results/20260823-225803`。该数据仅作为开发机相对回归基线，不能外推到生产峰值或 P95。

### 2026-08-24 按 PRD 顺序回归

本轮先检查并启动隔离 POC 容器，再按“核心链路 → 同步键 → 恢复 → 稳定性 → 性能”顺序执行脚本。第一次核心脚本复用了已执行过 CDC 的旧数据，初始快照校验按预期阻断；仅重置 `test/runtime` 后重跑通过，平台元数据库、Redis 和 `pptmaster-*` 容器未被操作。

| 顺序 | 场景 | 状态 | 证据 |
|---|---|---|---|
| 1 | 全量 + CDC、INSERT/UPDATE/DELETE、多表 | 通过 | `test/results/20260824-221248` |
| 2 | 非空唯一键候选、CDC 更新/删除/插入 | 通过 | `test/results/20260824-221540` |
| 3 | checkpoint 损坏拒绝、修复后 restore、binlog 过期失败 | 通过 | `test/results/20260824-222120`、`test/results/20260824-222159` |
| 4 | 容器状态与源/目标行数稳定性核对 | 通过 | `test/results/20260824-222238` |
| 5 | 5,000 行全量 + CDC 性能回归 | 通过 | `test/results/20260824-222247` |

本轮性能样本全量耗时 6.452 秒（约 774.89 行/秒），CDC marker 延迟 5 秒，源端和目标端 benchmark 行数均为 5,001。测试结束后 benchmark 数据自动清理；隔离容器保持启动，便于继续进行平台端到端验收。

## 已发现并解决的问题

1. Windows bind mount 的 MySQL 配置文件会被容器识别为 world-writable，MySQL 因安全策略忽略该文件。POC 改为通过 Compose `command` 显式传入 binlog、GTID 和字符集参数。
2. SeaTunnel 官方 2.3.13 镜像同时包含 PostgreSQL JDBC 与 openGauss JDBC，后者也打包了 `org.postgresql.Driver` 并发生类路径抢占，连接 PostgreSQL 17 时出现 `Protocol error. Session setup failed`。定制镜像移除内置旧驱动和 `opengauss-jdbc`，只保留锁定的 PostgreSQL JDBC 42.7.5。

### 2026-08-27 源库保护参数验证

平台迁移 `011` 已将四个资源字段应用到 `ds_sync_task` 与 `ds_sync_task_group`。使用已登录平台会话检查任务组 `TEST platform group fixed` 的配置预览：两个表项均生成 `parallelism = 1`、`read_limit.rows_per_second = 1000`、`read_limit.bytes_per_second = 10485760` 和 `connection.pool.size = 2`，密码继续展示为 `******`。

隔离引擎 POC 使用 `resource-protection-poc.ps1 -RowCount 500 -RowsPerSecond 100 -BytesPerSecond 1048576` 验证 SeaTunnel 接受低速配置并清理测试数据。作业 `1145168185385811969` 在 16.867 秒内完成 500 行快照，实测约 29.64 行/秒，未超过 100 行/秒配置；结果、提交配置和引擎信息保存在 `test/results/20260827-092923/`。连接池参数采用 SeaTunnel 2.3.13 MySQL CDC connector 已核实的 `connection.pool.size`，不使用猜测的 HOCON 键名。任务组当前逐表提交独立 job，四个参数均为每表项生效值；组级累计连接预算不属于本阶段范围。

## 结论

SeaTunnel 2.3.13 在本隔离环境中可以作为 MySQL 8.0 ROW/GTID binlog 到 PostgreSQL 17.6 的 MVP 执行引擎：initial 快照后连续 CDC、增删改、联合主键、非空联合唯一键、幂等 upsert/delete、savepoint/restore、目标端短暂不可用重试均通过。平台必须持久化 checkpoint，并在引擎进程重启后重新发现作业并主动 restore；引擎不会自动重新提交作业。当前关闭自动 DDL，新增字段会让目标写入失败，生产实现必须先做 DDL 兼容性检查并按表隔离或人工处理。性能基线已完成一组 5,000 行开发机回归样本，但结果不能外推到千万级吞吐或生产 P95。
