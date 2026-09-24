# 端到端回归套件（e2e）

`ruoyi-sync` 的常规测试全部是 Mockito 单测，引擎交互此前只靠手工验证。e2e 套件以**黑盒**方式驱动正在运行的本地栈：只走后端 HTTP API（与浏览器同一条登录链路），用 JDBC 在真实源库/目标库里造数和断言，用 Kafka 客户端读真实 topic，并对照 SeaTunnel REST 核实引擎侧状态。它是任务 / 任务组生命周期重构（事务边界、两套生命周期实现合并）的安全网，所以断言的是生命周期语义，而不只是“接口返回 200”。

代码位于 `server/ruoyi-modules/ruoyi-sync/src/test/java/org/dromara/sync/e2e/`，全部标注 `@Tag("e2e")`。

## 前置条件

- 本地栈已启动：`.\dev.ps1 up -Poc`（后端 `:18081`、SeaTunnel REST `:18080`、源 MySQL `:23306`、PostgreSQL `:25432`、目标 MySQL `:23307`、Kafka `:29092`、平台 Redis `:16379`）。套件不启动、不停止、不重启任何进程或容器。
- 已登记数据源（按名称经 `GET /sync/data-source/options` 查找，不写死 ID）：`POC MySQL`、`POC PostgreSQL`、`POC MySQL Target`、`POC Kafka 3.8`。
- 登录验证码开启时，答案从平台 Redis 的 `global:captcha_codes:<uuid>` 读取（内置极简 RESP 客户端）。
- 源库建表/改表用 root（平台的 `seatunnel` 账号无 DDL 权限）。所有地址、账号可用 `-De2e.<key>=...` 或对应环境变量覆盖，见 `E2eConfig`。
- Maven 离线可用（依赖已由 `dev.ps1 up` 装入本地仓库）。

## 运行

在 `server/` 下：

```powershell
mvn -q -o -Pdev -pl ruoyi-modules/ruoyi-sync -Dmaven.test.skip=false -Dgroups=e2e -Dsurefire.failIfNoSpecifiedTests=false test
# 单个场景：追加 -Dtest=SingleTaskCdcLifecycleE2eTest
```

`-q` 会隐藏汇总，结果看 `ruoyi-modules/ruoyi-sync/target/surefire-reports/`。每一步都有 `[e2e <runId> +耗时] ...` 进度日志（在报告的 system-out 中）。一次完整运行约 4 分钟，其中场景 1–3 约 2.5 分钟。

分组接线：根 pom 的 surefire 把 `<groups>` 固定为 `${profiles.active}`，命令行 `-Dgroups` 原本会被静默忽略。`ruoyi-sync/pom.xml` 改为经同名属性 `groups`（默认值仍为 `${profiles.active}`）传入，因此 `-Dgroups=e2e` 只选中本套件，默认与 `-Dgroups=dev` 的常规回归不受影响、永远不会跑到 e2e。模块同时以 test 作用域引入 MySQL / PostgreSQL JDBC 驱动（与 `ruoyi-admin` 运行时同版本）。

## 场景与钉住的语义

| 测试类 | 场景 | 断言要点 |
|---|---|---|
| `SingleTaskFullE2eTest` | FULL，MySQL→PostgreSQL | 新建为 `DRAFT`、空字段选择展开为全列、同步键=主键；启动后自行进入 `FINISHED` 且引擎 `FINISHED`；目标逐行逐列与源一致（含 emoji/引号、NULL、DECIMAL、毫秒 DATETIME）；`COUNT` 与 `KEY_RANGE` 核对一致；`FINISHED` 可删除，删除不动目标数据 |
| 同上 | FULL，MySQL→MySQL | 目标表按源 DDL 预建（`varchar(64)` 不被放宽为 TEXT、保留主键）、除时间列外逐行一致；`fullToMysqlKeepsDatetimeWallClock` 单独断言 DATETIME 原样复制 |
| `SingleTaskCdcLifecycleE2eTest` | FULL_CDC 全生命周期，MySQL→PostgreSQL | 快照到达，INSERT/UPDATE/DELETE 生效；`RUNNING` 时拒绝再次启动、编辑、删除且状态不变；暂停 = savepoint：`PAUSING`→`PAUSED`、引擎 `SAVEPOINT_DONE`、记录 checkpoint；暂停期间写入不落目标；`PAUSED` 拒绝启动和删除；恢复复用原 jobId，引擎上只有这一个作业；暂停期写入在恢复后到达，**目标端手工打的标记行未被覆盖**（证明是 savepoint 续跑而非重新快照），无重复/无丢失；恢复后 CDC 继续；停止→`STOPPED`、调度挂起、引擎 `CANCELED` 且不在运行列表、之后的 binlog 不再消费、拒绝恢复；`STOPPED` 可删除 |
| `TaskGroupDdlE2eTest` | MULTI_TABLE 两表 FULL_CDC→PostgreSQL + DDL 漂移 | 组 `RUNNING`、两表项各自独立引擎作业且组记录恰为这两个 jobId；两表快照、一表 CDC 到达；对 B 表 `ADD COLUMN` 后 `ddl-check`：只产生 B 的一条未关闭事件，B `DDL_BLOCKED`（引擎 `SAVEPOINT_DONE`）、A 仍 `RUNNING`、组 `DEGRADED`；状态刷新不会把 `DDL_BLOCKED` 盖成 `PAUSED`；A 继续收 CDC、B 收不到；停止降级组→组与所有表项 `STOPPED`、引擎无残留作业；可删除 |
| `KafkaTaskE2eTest` | FULL_CDC→Kafka（ENVELOPE） | 经平台接口建输出 topic；消费真实输出 topic：初始装载 INSERT（`phase=CDC`，见下文）、CDC INSERT、合并后的单条 UPDATE（带 before 前像）、DELETE（data 为被删行），整个 topic 没有 `SNAPSHOT` 事件，消息 Key 为同步键 JSON；停止后引擎无作业 |
| `EngineStateMappingE2eTest` | 引擎启动过渡态的映射 | 在启动/恢复仍在进行时就排队刷新状态（同一把任务锁，锁一释放立即执行），逐条记录“引擎状态→平台状态”；任何过渡态（CREATED/INITIALIZING/PENDING/SCHEDULED）被报成 `FAILED` 即失败；本次一个过渡态都没采到时记为 skipped（无法判定），而不是通过 |

## 隔离与清理

- 每次运行生成 6 位 `runId`（可用 `-De2e.run-id=` 指定），所有表、任务、任务组、topic 都以 `e2e_<runId>_` / `e2e-<runId>-` 命名；不带本次 runId 的对象一律不碰，可与他人共用同一套栈并发运行。
- 清理是撤销日志：每个对象**创建之前**先登记清理步骤（任务/任务组按名称查找后清理，不依赖拿到 ID），场景中途失败也会在 `@AfterAll` 按后进先出执行：先停止并删除平台对象，再删目标表/topic，最后删源表。单步失败不影响其余步骤。
- 平台停止后，若引擎仍在运行本次的作业（按 jobId 或作业名 `ds-task-<任务或表项 ID>` 识别），给 20 秒让异步取消完成，仍在则直接调引擎 `/stop-job` 取消。
- 删除处于 `FAILED` / `REINITIALIZE_REQUIRED` 的任务（或含 `FAILED` 表项的任务组）前，先按作业名观察 20 秒：提交/恢复请求超时后，引擎可能在平台放弃后数秒才接受作业（“幽灵作业”，见下文缺陷）。出现即直接取消，并作为问题报告。
- 清理之后做泄漏检查：引擎上不得有本次启动的作业在运行，平台上不得残留本次创建的任务/任务组；否则该测试类失败并列出残留项。
- Kafka topic：输出 topic 与 raw topic 在清理时删除。平台共享的 `KafkaProducer` 会对最近 5 分钟内写过的 topic 继续请求元数据，而本地 broker 未关闭 `auto.create.topics.enable`，所以刚删掉的输出 topic 几十秒内会以空 topic 形式被重建。为此每次运行把自己建的 topic 名记到 `ruoyi-sync/target/e2e-pending-topics.txt`，下一次跑 Kafka 场景时先删掉其中又冒出来的。清单只含本检出目录自己建过的 topic。

## 确定性

- 不用固定 sleep 等结果，一律轮询（`Await`），超时信息包含最后观察到的平台状态、`lastError`、引擎作业状态和状态轨迹。
- 状态轮询用显式刷新接口（`POST .../status`），不依赖 30 秒的后台对账。
- 每次提交作业（启动、恢复）后，先等引擎自身离开启动过渡态再向平台要状态。原因是一个已知产品缺陷：刷新恰好落在引擎 `SCHEDULED` 时平台会落库 `FAILED`。生命周期场景因此不再与这个窗口赛跑，该缺陷由 `EngineStateMappingE2eTest` 单独钉住。
- “不得到达”类断言（暂停期、停止后、DDL 阻塞期）观察 15 秒，即 3 个 checkpoint 周期；JDBC sink 按 checkpoint 刷写，仍在消费的作业早已送达。
- 任务一律用 `MANUAL` 调度建，避免默认 `ONCE` 调度器与测试自己的启动抢跑。

## 已知失败（对应产品缺陷，断言未放宽）

- `SingleTaskFullE2eTest.fullToMysqlKeepsDatetimeWallClock`：MySQL 目标的 DATETIME 整体 +8 小时（FULL 与 FULL_CDC 均如此，PostgreSQL 目标正确）。
- `EngineStateMappingE2eTest`：引擎 `SCHEDULED` 被映射并持久化为 `FAILED`。采到过渡态时失败，没采到时为 skipped。

缺陷修复后这两项应转绿；在此之前，重构验收以“其余测试全绿、这两项结论不变”为准。

MySQL `FULL_CDC`→Kafka 的初始装载事件 `phase` 是 `CDC`，这是契约，`KafkaTaskE2eTest` 按此断言。SeaTunnel 把快照行写成 `op=c`，与 binlog INSERT 逐字段同形，平台拿不到可靠信号，因此不猜边界。实测证据与消费端的替代依据见 `kafka-event-formats.md` 的“phase 的真实含义”。

## 环境导致的失败

套件依赖本地栈的响应时间。Docker 虚拟机内存或 CPU 吃紧时，SeaTunnel 的 young GC 单次可达数秒（看 `GET :18080/system-monitoring-information` 的 `minor.gc.time` / `minor.gc.count`）。这会让平台自己的超时先触发：引擎 REST 10 秒、目标 JDBC socket 5 秒。表现为这类失败信息：`SeaTunnel 接口不可用：... submit-job ... Request cancelled`、`预建目标表失败：Communications link failure`，或引擎侧的 `CheckpointException`。这不是断言问题。先确认栈恢复正常，再重跑。这种情况下若清理报告了幽灵作业，说明产品缺陷被触发了：
- 恢复（`resume`）超时后，行仍带原 jobId 且处于 `FAILED`。孤儿清扫器只接管没有 jobId 的行，后台对账又只扫 `RUNNING`/`PAUSING`，所以平台再也不会管这个作业。
- 启动超时后，行是 `FAILED` 且没有 jobId，`EngineOrphanSweeper` 约 30 秒后会接管。但如果在这之前删除了任务，作业就同样成了孤儿。

## 副作用

- 场景 3 与状态映射缺陷会在消息中心产生告警（`DEGRADED`、`DDL_BLOCKED`、偶发 `FAILED`）。
- 删除任务组不会删除其 `ds_sync_task_group_ddl_event` 记录（产品缺陷，事件会以未关闭状态残留）。
- 引擎的已结束作业历史保留本次作业记录；DDL 阻塞表项停止后，其 `SAVEPOINT_DONE` 作业与 savepoint 仍留在引擎侧。