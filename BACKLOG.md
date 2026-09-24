# 优化清单（保持现有平台能力）

本清单是 2026-09-23 对 `ruoyi-sync` 后端与 `web` 前端做的一次通读审视结果。**收录标准**：不改变
平台对外能力和 API 语义、只提升正确性 / 资源占用 / 可维护性的改动。凡是"新增产品功能"的想法
放在最后的「非本清单范围」里单独列出，避免和优化混在一起。

每项给出：**问题 → 证据（文件:行）→ 方案 → 影响面 / 工作量 / 风险**。证据行号对应提交
`edf756c`。做完一项就地勾掉并补上提交号。

> 优先级判定：P0 = 会导致数据错误或客户库受损；P1 = 规模化后必然撞墙，或用户已能看到的错误数字；
> P2 = 结构与可维护性，收益长期但不紧急。

---

## P0 — 正确性 / 数据安全

### [x] P0-1 提交超时会留下"幽灵作业"，且可能造成目标表双写 — 已完成（见文末「已完成」）

**问题**：`submit` 受 `sync.engine.request-timeout`（10s）约束。引擎冷启动或繁忙时，平台侧超时抛错，
但**引擎其实已经接单并开始运行**。此时：

1. `markFailed(task, task.getEngineJobId(), ...)` 写入的是**旧的** jobId（首次启动时为 null），新作业的
   id 从未被捕获；
2. 状态对账只扫 `engine_job_id` 非空的行，这个作业永远不会被平台看见；
3. 操作员看到 FAILED 后重新初始化 → 再提交一个新作业 → **两个作业同时写同一张目标表**。

2026-09-22 的实测中真实复现过一次：引擎上留下 `ds-task-2102384833868902402` 持续运行并写检查点，
而平台库里该表项是 FAILED、`engine_job_id` 为 NULL。

**证据**：
- `server/ruoyi-modules/ruoyi-sync/src/main/java/org/dromara/sync/service/impl/SeaTunnelJobServiceImpl.java`
  submit 的 catch 分支（`markFailed(task, task.getEngineJobId(), ...)`）
- 任务组侧同构：`SyncTaskGroupServiceImpl.submitItem` / `submitWithBridge`
- `SeaTunnelRestClient` 没有任何按作业名反查的能力（`grep jobName` 只出现在提交参数里）

**方案**：作业名是确定性的（`ds-task-<taskId>` / 表项用 itemId），所以认领是可行的：

1. `SeaTunnelRestClient` 增加 `findByName(jobName)`（走 `GET /running-jobs`，Zeta 2.3.13 已验证可用）；
2. submit 抛错后立即按作业名反查一次：查到就认领 jobId 并置 RUNNING，查不到才落 FAILED；
3. 启动/重新初始化前也做一次同名检查，拒绝重复提交（双保险）。

**影响面**：单表任务 + 任务组表项两条提交路径。**工作量**：0.5 天。**风险**：低，纯增量逻辑。
**测试**：mock `findByName` 覆盖「超时后查到 / 查不到」两条分支。

### [x] P0-2 `TargetTableSwap` 是破坏性操作但零测试 — 已完成（见文末「已完成」）

**问题**：FULL/OVERWRITE 模式下做 `target → backup、stage → target、drop backup` 的重命名切换。
代码本身写得稳（标识符正确转义，PG 走事务、MySQL 走原子多表 RENAME），但**没有任何测试**，
而它一旦出错就是目标表丢失。

**证据**：`support/TargetTableSwap.java`（110 行，`find test -name TargetTableSwapTest.java` 无结果）

**方案**：至少覆盖 SQL 拼装（标识符转义、目标表存在 / 不存在两种分支）；有条件再用 Testcontainers
跑一次真实 PG + MySQL 的切换。**工作量**：0.5 天（纯拼装）/ 1.5 天（含容器）。**风险**：无。

---

## P1 — 规模化瓶颈 / 用户可见的错误数字

### [x] P1-1 首页仪表盘不统计任务组，且静默截断到 100 条 — 已完成（见文末）

**问题**：仪表盘在浏览器里拉 100 条任务 + 100 个数据源后自行聚合。两个后果：

1. **只统计单表任务**——只用 `多表同步` 的用户，首页"运行中任务"恒为 0；
2. 超过 100 条后所有数字（运行中任务、已核对行数、成功率、平均延迟）静默失真，页面不会提示。

**证据**：`web/src/pages/index.tsx:61-63`（`pageSize: 100`）、`:75-86`（客户端聚合，只遍历 `tasks`）

**方案**：后端加一个只读聚合端点（`GET /sync/overview`），用 COUNT/SUM 一次算出任务 + 任务组
+ 表项的口径，前端改读它。顺带补上自动刷新。**影响面**：新增一个端点 + 首页改造。
**工作量**：1 天。**风险**：低（新端点，不动既有接口）。

### [x] P1-2 元数据读取每次新建一条不池化的 JDBC 连接 —— 已做（见「已完成」）

**问题**：`JdbcUrls.open` 走 `DriverManager.getConnection`，每次调用一条新连接。DDL 检查每 60 秒
**逐表**调一次 `queryTableMetadata`——一个 20 张表的任务组 = 每分钟 20 次到客户源库的 TCP+认证握手，
长期常驻。这与产品主打的"源库保护"（限速、连接池上限）是矛盾的：限的是引擎侧，平台自己没限。

**证据**：`support/JdbcUrls.java:50`、`service/impl/DataSourceMetadataServiceImpl.java:538`、
`service/impl/SyncTaskGroupDdlServiceImpl.java:90-94`（每表一次调用）

**方案**（按性价比排序）：
1. **一次检查复用一条连接**——`doCheckDdl` 打开一条连接贯穿整组表项（改动最小，立竿见影，20 次 → 1 次）；
2. 给元数据读取加一个短 TTL（如 30s）的连接缓存或小连接池，向导里的连续探查也受益；
3. DDL 检查加粗筛：先用一条 `information_schema.columns` 查询把整库列指纹一次取回，变了再逐表细看。

**影响面**：`IDataSourceMetadataService` 需要一个"带连接"的内部重载。**工作量**：方案 1 约 0.5 天。
**风险**：中（要保证连接异常时逐表隔离的语义不变）。

### [x] P1-3 生命周期方法在数据库事务内做远程调用 — 已完成（见文末）

**问题**：`start / discover / pause / resume / stop / refreshStatus / reinitializeItem` 都标了
`@Transactional`，而方法体里串行发 N 次 SeaTunnel HTTP（每次上限 10s）+ 若干次客户库 JDBC 探查。
一个 20 表任务组的 `start`，最坏会**在一个数据库事务里挂住数分钟**，期间占着一条 MySQL 连接和行锁。

**证据**：`service/impl/SyncTaskGroupServiceImpl.java:247`（start）、`:574`（refreshStatus）等，
配合 `:870 submitItem` → `restClient.submit`

**方案**：事务只包住落库。把远程调用移出事务边界，用"先远程、后落库"的补偿式写法——`doStart`
已经有完整的补偿逻辑（失败时停掉已提交作业），把事务范围收窄到每次状态写入即可。
**影响面**：任务组 8 个方法 + 任务侧对照。**工作量**：1.5 天。
**风险**：**高**——这是本清单里最需要测试托底的一项，建议在 P2-3（生命周期抽取）之前或同时做，
且必须先把状态机单测跑绿。

### [x] P1-4 状态刷新串行，且 7 个后台轮询挤在共享线程池 — 已完成（两部分均见文末）

**问题**：
- `refreshStatus` 对每个表项串行发 2 次 REST（status + checkpoints），20 表 = 40 次往返，每 30 秒一轮；
- 模块内共有 7 个 `@Scheduled`（任务对账、任务组对账、DDL 检查、整库发现、调度触发、Kafka 桥接对账、
  告警、指标清理），全部跑在 RuoYi 共享的 `schedule-pool`（cores+1 线程），一个慢的会饿死其余。

**证据**：`grep -n "@Scheduled" service/impl/*.java` 共 7 处；`SyncTaskGroupServiceImpl:579 doRefreshStatus`
的串行 for 循环

**方案**：
1. 表项状态刷新改为有界并行（组锁不变，锁内并行发请求）；
2. 给 sync 模块配一个独立的 `ThreadPoolTaskScheduler`，与框架的通用调度隔离。

**影响面**：两处。**工作量**：1 天。**风险**：中（并发下的 `itemStatusFailureStreak` 计数要保持正确，
目前是 `ConcurrentHashMap`，可用）。

### [x] P1-5 后台热查询全是全表扫 — 已完成 f22e076（migration 022）

**问题**：`ds_sync_task` 只有 `task_name / source_id / target_id` 上的索引，没有 `status` 和
`next_run_time`。而调度器每 15 秒查一次 `schedule_mode in (…) and next_run_time <= now()`，
状态对账每 30 秒查 `status in ('RUNNING','PAUSING')`，告警每 30 秒查 `status in (…) or alerted_status <> ''`。
任务组和表项的 `status` 同样无索引。

**证据**：`server/script/sql/ry_sync.sql:76-79`（ds_sync_task 索引清单）、
`ry_sync_migration_004.sql`（组与表项建表）

**方案**：一次幂等迁移加 4 个索引：
`ds_sync_task(status)`、`ds_sync_task(schedule_mode, next_run_time)`、
`ds_sync_task_group(status)`、`ds_sync_task_group_item(status)`。
**影响面**：一个迁移文件。**工作量**：0.5 小时。**风险**：无。

### [x] P1-6 任务组分页有 N+1 查询 — 已完成（见文末）

**问题**：`queryPageList` 对分页结果每行各查一次表项、再查一次指标。一页 10 个组 = 21 次查询。

**证据**：`service/impl/SyncTaskGroupServiceImpl.java:124`（`forEach(this::attachItems)`）→
`attachItems` 内 `itemMapper.selectByGroupId` + `metricsService.latestForGroupItems`

**方案**：按整页 groupId 批量查表项，再按全部 itemId 批量查指标（`selectLatest` 已经支持批量入参），
在内存里分组回填。**影响面**：一个私有方法。**工作量**：0.5 天。**风险**：低。

### [x] P1-7 数据源下拉硬编码 100 条上限 — 已完成（见文末）

**问题**：任务页 / 任务组页 / 首页都用 `listDataSources({ pageNum: 1, pageSize: 100 })`。
超过 100 个数据源时，向导里直接选不到后面的。

**证据**：`web/src/pages/sync/task/index.tsx:28`、`web/src/pages/sync/group/index.tsx:27`、
`web/src/pages/index.tsx:62`

**方案**：后端加一个轻量 `GET /sync/data-source/options`（只返回 id/名称/类型/库名），前端改为
远程搜索式下拉。**工作量**：0.5 天。**风险**：低。

### [x] P1-8 Kafka 桥接线程池无上限 — 已完成（见文末）

**问题**：`Executors.newCachedThreadPool`，每个运行中的 Kafka 任务/表项独占一条线程 + 一个 consumer
+ 一个 producer。100 个 Kafka 任务就是 100 条常驻线程，没有任何上限或背压。

**证据**：`kafka/KafkaTaskBridgeService.java:56`

**方案**：换成有界线程池，超限时拒绝并把该表项标记为 FAILED + 落 `last_error`（让对账器下一轮重试），
而不是无声地把 JVM 撑爆。**工作量**：0.5 天。**风险**：中（要确认对账器不会因拒绝而陷入重试风暴）。

---

## P1-UX — 表单与向导体验

本节针对"填表不丝滑"。全部是纯交互改动，不动任何接口和校验规则。下面每条都在代码里能指到位置，
其中 UX-1 和 UX-2 是 2026-09-22 用浏览器实测两个向导时亲自踩到的。

### [x] UX-1 误按 Esc 或点遮罩会丢掉整份填了一半的表单 — 已完成 f22e076

**问题**：两个弹窗的 `modalProps` 都只有 `destroyOnHidden: true`，没有关闭防护。按 Esc、点遮罩
都会直接关闭，`destroyOnHidden` 随即销毁表单——单表向导填到第 5 步、任务组加了好几张表，
一次误操作全部清零，且没有任何确认。

**证据**：`web/src/pages/sync/task/components/TaskFormModal.tsx:308`、
`web/src/pages/sync/group/components/GroupFormModal.tsx:161`

**方案**：`maskClosable: false` + `keyboard: false`；点"取消"或右上角关闭时，若表单已被修改
（`form.isFieldsTouched()`）先弹二次确认。**工作量**：1 小时。**风险**：无。

### [x] UX-2 探查期间没有状态反馈，点"下一步"只会弹一条错误 — 已完成（见文末）

**问题**：选完源数据源后，后台串行跑连接测试 → CDC 预检 → 库列表 → 表列表（实测 1.5~2.5 秒），
这期间"下一步"按钮既不 loading 也不禁用。用户点下去得到的是一条 toast
`请等待源表元数据加载完成后继续`——**用错误提示代替了加载状态**，用户不知道该等多久、等什么。

**证据**：`TaskFormModal.tsx:320-321`（按钮无 `loading`/`disabled`）、`:242`（事后报错）、
`:85 metadataLoading` 这个状态只用在了表下拉的 loading 上（`:419`）

**方案**：`下一步` 绑定 `loading={metadataLoading}` 并在探查未完成时禁用；在步骤区显示当前进度
（"正在测试连接 / 正在读取 CDC 配置 / 正在读取表列表"），让等待可见。**工作量**：0.5 天。**风险**：无。

### [x] UX-3 校验失败靠 toast，不定位到出错的字段和步骤 — 已完成（见文末）

**问题**：向导里 6 处用 `message.error` 报校验失败。最伤的一条是第 5 步提交时的
`请返回前面步骤，补全源数据源、源表、目标表、字段和同步键`——**既不说是哪一步，也不跳转**，
用户只能自己一步步倒回去找。

**证据**：`TaskFormModal.tsx:157/238/242/266/270/313`

**方案**：把缺失项映射回它所属的步骤，自动 `setWizardStep` 跳过去 + `form.scrollToField` 滚动到该字段
并就地标红；toast 只留一句摘要。**工作量**：0.5 天。**风险**：低。

### [x] UX-4 字段选择在宽表上不可用 — 已完成（见文末）

**问题**：同步字段是一个平铺的 `Checkbox.Group`，没有全选 / 反选 / 搜索 / 已选计数。实测 9 列的
`customers` 就占掉大半屏；几十上百列的宽表会把弹窗撑爆，且用户没法"只排除两列"。

**证据**：`TaskFormModal.tsx:542-548`、任务组侧同构 `GroupFormModal.tsx:388`

**方案**：换成带搜索框 + 全选/反选 + "已选 N / 共 M"的选择器（列多时用虚拟滚动）；同步键字段保持
禁用且置顶显示，说明为什么不能取消。**工作量**：1 天。**风险**：低。

### [x] UX-5 关系型目标表名没有默认值，Kafka 有 — 已完成 f22e076

**问题**：选完源表后，Kafka 目标会自动填出 topic 名（`defaultKafkaTopic`），但 MySQL / PostgreSQL
目标的"目标表名"是空的，必须手输——而绝大多数场景就是同名。

**证据**：`TaskFormModal.tsx:224-227`（只有 Kafka 分支填默认值）

**方案**：关系型目标同样预填为源表名（可改），与 Kafka 分支对齐。**工作量**：15 分钟。**风险**：无。

### [x] UX-6 任务组表单单页过长，每张表都要重复填一遍 — 已完成（见文末）

**问题**：880px 的弹窗里依次塞了名称、粒度、同步方式、源/目标数据源、4 项限速、然后是表项列表——
每张表展开后有 源表 / 目标 Schema / 目标表 / 创建 topic / 删除 + 一整片字段复选框 + 同步键。
加到第三张表就要滚很久，而目标 Schema 这种组内通常一致的值要逐表填。

**证据**：`GroupFormModal.tsx:305-410`（每个表项 6 个 Form.Item）

**方案**：
1. 表项改用可折叠面板，标题行显示"源表 → 目标表 · 已选 N 字段"，默认折叠已配好的；
2. 支持**批量添加**：源表下拉改多选，一次生成多个表项并按默认规则填好目标表名；
3. 目标 Schema 提升为组级字段，表项里只在需要覆盖时展开。

**工作量**：1.5 天。**风险**：低（提交载荷结构不变，仍是 `items[]`）。

### [x] UX-7 两个入口的填写范式不一致 — 已完成（见文末）

单表是 5 步向导，任务组是单页长表单。同一个产品里两套范式，用户要学两次。建议在 UX-6 之后把
任务组也对齐成 3 步（数据源 → 表与字段 → 同步方式与限速）。**工作量**：1 天。**风险**：中
（改动面大，建议排在 UX-1~UX-6 全部落地并实测之后）。

---

## P2 — 结构与可维护性

### [x] P2-1 三个高风险类零测试 — `SyncColumnSelectionValidator`（f22e076）、`SeaTunnelRestClient`（P0-1）、`TargetTableSwap`（P0-2）、`SyncTaskGroupDdlServiceImpl`（P1-2）均已补

`SyncTaskGroupDdlServiceImpl`（漂移状态机，228 行，决定一张表是否被隔离）、
`SyncColumnSelectionValidator`（每个任务创建都要过的纯函数）、`TargetTableSwap`（见 P0-2）。
`SyncColumnSelectionValidator` 是纯函数，补测试几乎零成本，优先做。
**工作量**：合计 1.5 天。**风险**：无。

### [x] P2-2 全部是 mock 单测，没有一条集成测试 — 已完成（见文末）

97 个单测全部 mock 掉了引擎和数据库，引擎交互只靠人工 e2e 覆盖。建议用 Testcontainers 补一条
最小链路（MySQL → PG，FULL 模式，起停一次），挂在单独的 profile 上，不进默认构建。
**工作量**：2 天。**风险**：低（新增，不动现有）。

### [x] P2-3 任务与任务组的生命周期是两套平行实现 — 已完成（见文末）

**问题**：两个服务各自实现 start / pause / resume / stop / refresh / reinitialize，语义相同
（savepoint 暂停、指纹校验、桥接先起后提交、瞬时不可达容忍 3 次、失败落 FAILED）。
任何一侧的修复都必须手工同步到另一侧——本轮已经出现过多次（指纹、PAUSED 拒绝、目标预建），
每次都是改两遍。

**证据**：`SeaTunnelJobServiceImpl` 的 `doStart/doPause/doResume/doStop/doReinitialize/doRefreshStatus`
与 `SyncTaskGroupServiceImpl` 的同名方法一一对应

**方案**：抽出"单个引擎作业"的生命周期单元（输入是已投影的 `SyncTask` —— `SyncTaskGroupConfigGenerator.toTask`
这个接缝已经存在并在用），两个服务只保留编排与聚合。
**影响面**：模块核心。**工作量**：3 天。**风险**：**高**——必须在 P1-3 的事务收窄之后、状态机单测
全绿的前提下做，且建议单独一个提交、配一次完整 e2e。

### [x] P2-4 `SyncTaskGroupServiceImpl` 仍有 1046 行 — 已完成（见文末）

承担 CRUD + 校验 + 生命周期 + 整库发现 + 数据核对 + 两个后台轮询。DDL 和 Kafka topic 已经拆出去了，
"整库发现"和"数据核对"可以按同样的方式拆成独立服务。做完 P2-3 后会自然瘦身，建议合并考虑。
**工作量**：1 天。**风险**：中。

### [x] P2-5 `schemaHash` 存了但从不参与比较 — 已完成 f22e076

DDL 检查每次都把快照 JSON 解析成对象做完整 diff，而行上已经存了 `schema_hash`。先比 hash、
相等直接跳过 diff，能省掉绝大多数轮次的解析开销，语义不变。
**证据**：`SyncTaskGroupDdlServiceImpl.java:107`（算出 hash）、`:121`（仍做完整 diff）
**工作量**：1 小时。**风险**：无。

### [x] P2-6 `itemStatusFailureStreak` 不随表项删除清理 — 已完成 f22e076

内存里的 `ConcurrentMap<Long, Integer>`，表项被删除后其计数项永远留着。单条数据量极小，
但属于无界增长。删除表项/任务组时顺手清一下即可。
**证据**：`SyncTaskGroupServiceImpl.java:113`
**工作量**：15 分钟。**风险**：无。

### [x] P2-7 `MAX_TABLES_PER_GROUP = 20` 硬编码 — 已完成（见文末）

整库场景下 20 张表的上限可能不够，但放开之前必须先解决 P1-3 / P1-4（否则 50 张表的组会在一个
事务里串行发 100 次 REST）。**结论：先别动，等并发和事务收窄做完再评估**，这里只做记录。
**证据**：`SyncTaskGroupServiceImpl.java:86`

---

## 建议的推进顺序

1. **先做零风险的**：P1-5（索引）、P2-5（hash 短路）、P2-6（清理计数）、UX-1（误关保护）、
   UX-5（目标表名默认值）、P2-1 的纯函数测试 —— 合计约 1 天，收益立刻可感。
2. **再做 P0**：P0-1（幽灵作业）、P0-2（切换测试）—— 数据安全，且互不依赖。
3. **体验批次**：UX-2（加载状态）→ UX-3（错误定位）→ UX-4（字段选择器）→ UX-6（任务组表单），
   每做完一条用浏览器实测一遍再往下走。
4. **然后是用户可见的数字**：P1-1（仪表盘）、P1-7（数据源下拉）、P1-6（N+1）。
5. **接着是源库压力**：P1-2（连接复用）。
6. **最后动核心**：P1-3（事务收窄）→ P1-4（并行 + 独立调度池）→ P2-3（生命周期抽取），
   每一步都要跑完整单测 + 本地栈 e2e，且各自独立提交。
7. UX-7（任务组改向导）和 P2-7（放开 20 表上限）留到各自前置项做完后再评估。

---

## 非本清单范围（产品增量，不是优化）

这些会改变平台能力，需要单独立项，列在这里只是避免和上面的优化混淆：

- 告警外部通道（邮件 / Webhook / 钉钉），目前只进消息中心；
- 指标聚合归档（现在只有原始采样，窗口上限 1440 分钟）；
- 目标端额外唯一约束的非阻断告警（`docs/api-contract.md:94` 标注为"后续版本补充"）；
- 源端扩展到 MySQL 以外；
- 任务依赖编排（`docs/scheduling-and-overwrite.md` 明确划在 MVP 之外）。

---

## 已完成

### P0-1 幽灵作业（2026-09-23）

最终方案与清单里的设想不同，实测推翻了第一版：

1. **快路径**：`SeaTunnelRestClient.submit` 区分「引擎答了 HTTP 错误」（它拒绝了，什么都没创建，
   不认领）和「压根没收到应答」（超时/连接断，作业可能已被接受）。只有后者去 `/running-jobs`
   按作业名认领。用内部 `EngineUnreachable` 异常区分，不做错误文案匹配。
2. **兜底**：新增 `EngineOrphanSweeper`（每 30s）——列一次引擎运行中的作业，把
   `ds-task-<id>` 里平台没有 jobId 的重新接管。**这是主力**：实测发现冷引擎注册作业比任何
   合理的内联重试窗口都慢（第一版 1 秒重试完全没赶上），而让操作员的请求阻塞十几秒去等更糟。
   持锁的行会跳过，避免 reinitialize 换作业的过程中被误接管。

只认领 `engine_job_id` 为空的行——已有 jobId 的行归状态对账管，从这里改指向会掩盖真实分歧。

**验证**：把 `SEATUNNEL_REQUEST_TIMEOUT` 压到 500ms 真实复现了幽灵作业（引擎在跑
`ds-task-2102583902907822081`，平台 FAILED + jobId 为空）；部署修复后清扫器在 40 秒内接管，
随后刷新状态（引擎 RUNNING、已读取 3 行）、停止成功、引擎运行中作业归零——证明确实重获控制权。
单测 120/120，其中 `SeaTunnelRestClientTest` 用 JDK 自带 HttpServer 覆盖了
「应答丢失且能认领 / 丢失但查不到 / HTTP 500 不认领 / 无 jobId」四条分支。

### P0-2 目标表切换（2026-09-23）

把"生成哪些 SQL"从"在哪执行"里拆出来（`postgresPlan` / `mysqlPlan` 两个纯函数），风险最高的
部分从此可断言。7 条单测覆盖：目标表存在 / 不存在两条分支的完整语句序列、PostgreSQL 的
`RENAME TO` 必须用裸名（带 schema 是语法错误）、MySQL 两次重命名必须在**同一条** RENAME TABLE
里（DDL 非事务，拆开会露出目标表缺失的窗口）、残留 backup 一定先于任何重命名被清掉、
标识符转义（表名来自用户输入，引号必须成对翻倍而不是截断标识符）。

**真实验证**：在本地 PostgreSQL 上跑了两次 FULL/OVERWRITE——首次（目标表不存在）落 2 行、
无残留；源端加一行后再跑（目标表已存在，走 backup 分支）得到 3 行且数据被整体替换，
`\dt swap_check*` 确认没有遗留 stage/backup 表。

### UX-2 / UX-3 / UX-4 表单体验（2026-09-23）

- **UX-2**：源端探查现在分四个阶段实时播报（测试连接 → 检查 binlog/CDC → 读取数据库列表 →
  读取表列表/表结构），`下一步` 在探查期间变成禁用的「正在探查」。此前这段完全静默，唯一的
  反馈是点早了弹一条 `请等待源表元数据加载完成后继续`——**用错误提示代替了加载状态**。
- **UX-3**：提交时若有缺项，按字段反查它所属的步骤，自动跳回该步骤、滚动到该字段并在 toast 里
  点名「第 N 步「X」还缺少必填项：Y」，取代原先那句不说是哪一步也不跳转的
  `请返回前面步骤，补全…`。纯增量未过 CDC 预检时同样自动跳回第 1 步。
- **UX-4**：新增共用组件 `components/sync/ColumnSelector`，替换两个表单里平铺的 `Checkbox.Group`。
  带搜索、全选/反选/清空（只作用于搜索命中的字段，所以"筛出 created_ 再全选"是追加而不是丢弃
  其余）、「已选 N / 共 M」实时计数、超出高度滚动；同步键字段锁定不可取消并附上原因。

**验证**：用 Playwright 拦截并延迟数据源接口 900ms 模拟远端慢库（本地栈 40ms 根本观察不到瞬时
状态），实测 4 个探查阶段依次出现、按钮禁用；未选源表时是**就地标红**"请选择源表"而不是 toast，
且不会前进；字段选择器的搜索、计数、"清空只影响筛出项"、同步键锁定共 5 项断言全过。

顺带修正：我新加的 Alert 用 antd 6 的 `title` 而非已废弃的 `message`。仓库里其余 Alert 仍用
`message`（控制台有废弃告警），属于独立的一次性清理，未纳入本次改动。

### UX-6 任务组表单（2026-09-23）

- 表项改为可折叠面板，标题直接显示「源表 → 目标表 · 已选 N 字段 · 键 X」，配好的可以折起来；
  面板用 `forceRender` 保持挂载，所以折叠状态下校验照常生效，提交失败时自动展开全部面板。
- **批量添加**：源表选择器改为多选，一次加入多张，每张按规则预填目标表名（关系型同名，
  Kafka 走 `defaultKafkaTopic`）。已加入的表从候选中移除，避免重复选。
- 工具条上有「目标 Schema + 应用到全部表项」，解决组内 schema 一致却要逐表填的问题；
  右侧显示「已选 N / 上限 20 张」，把后端的 `MAX_TABLES_PER_GROUP` 摆到明面上。
- 顺带修掉一个因此暴露的问题：表单初始自带一个空表项（`emptyForm.items` 有一个空对象），
  批量加 2 张后变成 3 个面板、第一个是空的，直接导致保存校验失败。批量添加成为主路径后
  空起始项已无意义，改为空数组 + 「还没有表项」提示，并在提交前做客户端兜底校验。
- `defaultKafkaTopic` 从 `pages/sync/task/shared` 提到 `utils/syncNaming`，两个表单共用一份。

**验证**：浏览器实测 9 项断言全过——批量加 2 张生成 2 个面板、标题带字段数与同步键、
已加入的表从候选移除、批量设置 Schema、**折叠状态下保存成功**（字段没丢）、0 页面错误。

### P1-1 / P1-6 / P1-7 可见数字与查询（2026-09-23）

- **P1-1 仪表盘**：新增只读聚合端点 `GET /sync/overview`（`SyncOverviewVo` + `SyncOverviewServiceImpl`），
  在服务端一次性算出作业总数/运行中/失败、任务与任务组与表项的分项计数、状态分布、
  按目标类型的链路计数（含运行中）、核对行数与一致率、平均 CDC 延迟。前端改读它，
  并加了 30 秒自动刷新。**口径同时修正了两件事**：作业数现在 = 单表任务 + 任务组表项
  （此前只数单表任务，只用多表同步的用户首页恒为 0），且不再有 100 条静默截断。
- **P1-7 数据源下拉**：新增 `GET /sync/data-source/options`（`DataSourceOptionVo`，只含
  id/名称/类型/库名/状态，不含主机与凭证），任务页与任务组页改用它，去掉 `pageSize: 100`。
- **P1-6 N+1**：任务组分页此前每行各查一次表项 + 一次指标（一页 10 个组 = 21 次查询）。
  新增 `selectByGroupIds` 批量查，整页两次查询搞定，内存里按 groupId 归组回填。

**验证**：造了一个"只有任务组、没有任何单表任务"的场景（正是旧仪表盘显示 0 的那个 bug）——
`/sync/overview` 返回作业 2/2、单表任务 0、表项 2 运行中 2；浏览器实测首页 KPI 显示
「运行中作业 2 / 2」、链路卡片「2 个任务，2 个运行中」、状态分布饼图有数据，0 页面错误。

### P1-2 源库连接复用（2026-09-23）

DDL 检查过去**逐表**调 `queryTableMetadata`，而 `JdbcUrls.open` 走 `DriverManager` 不池化——
一个 20 张表的任务组每分钟就是 20 次到客户生产库的 TCP+认证握手，长期常驻。平台一边卖"源库保护"
（限速、连接数上限），一边自己是源库上最吵的那个客户端。

做法是给 `IDataSourceMetadataService` 加一个批量入口
`queryTablesMetadata(sourceId, database, tables)`：一条连接读完整组表，返回 `表名 -> TableMetadata`，
其中 `metadata` 与 `error` 恰有一个有值——**逐表失败隔离的语义必须原样保留**，一张表被删/被回收权限
不能连累其余表的检查。实现上把"开连接"与"读一张表"拆开（`readTable`），单表入口照旧；顺带把
charset/collation 提到批次级别读一次（它是库级属性，过去每张表都查一遍 `information_schema.schemata`）。
`doCheckDdl` 按 `sourceDatabase` 分组后每库一次批量读，整批连不上时退化成逐表 `TABLE_UNAVAILABLE`，
与改之前的行为一致。

**验证**：5 张表的任务组跑起来后开 MySQL general log 采样 70 秒（覆盖一整轮 DDL 检查），
平台侧 `seatunnel@172.19.0.1` 的 Connect 事件为 **1 次**（另外 14 次 `root@127.0.0.1` 是我自己的
探针命令）——改之前这里必然是 5 次。随后给 `ddl_probe_a` 加一列，`ddl-check` 只隔离了这一张
（ADD_COLUMN / 新增字段：email / READY_TO_RESUME，组转 DEGRADED），其余 4 张继续 RUNNING，
逐表隔离语义未变；单表元数据接口的 charset/collation 仍正确返回 `utf8mb4 / utf8mb4_0900_ai_ci`。

同时补齐 P2-1 最后一块：新增 `SyncTaskGroupDdlServiceImplTest`（8 条），覆盖"一轮一次批量读"、
"跨库分别读"、加列→隔离该表、目标不兼容→PENDING_FIX、回到基线→READY_TO_RESUME、
无基线→只建基线不报事件、单表不可读不连累其余、整库不可达逐表标记。后端单测 127 → 135 全绿。

**仍未做**：`checkTargetCompatibility` 内部仍是每次两条新连接（源+目标），只在表结构真变了或首次
建基线时触发，属低频路径；向导里的连续探查也还没有连接缓存（原清单方案 2），留待后续。

### P1-3 生命周期不再包在数据库事务里（2026-09-24）

任务组的 start / discover / pause / resume / stop / refresh / 逐表恢复 / 逐表重新初始化与 DDL 检查
全都标了 `@Transactional`，方法体里却串行调用引擎。比"长事务占连接"更严重的两点：组锁在方法体
返回时就释放、事务却在其后才提交，另一实例拿到锁会读到未提交的行；回滚撤销不了引擎侧动作——
resume 第二张表指纹不符时回滚，第一张表的作业已经在跑，库里却还是 PAUSED。

现在涉及引擎的生命周期方法一律不开事务：每次落库各自提交，**所有拒绝在第一次引擎调用之前判定**，
引擎中途失败留下诚实的逐表结果（暂停/恢复/停止部分失败时失败表置 FAILED 并记录原因，其余照常完成；
逐表重新初始化先校验、后销毁旧作业）。只动数据库的编辑/删除改为"先加锁、锁内 TransactionTemplate"，
杜绝与并发启动交错（否则启动可能为已删除的行提交出无主作业）。反射测试钉住"引擎侧方法不得带
`@Transactional`"，时序测试钉住"提交发生在锁内"。顺带修复：DEGRADED 组从不被后台对账；生命周期
`updateById` 把旧 `alerted_status` 写回导致同一告警发两次（该列改为 `updateStrategy=NEVER`）；
单表重新初始化同样先销毁后校验；部分失败的组操作在控制台显示为绿色成功提示（新增 `partial` 标记）。

### P1-4（第 2 部分）sync 模块独立调度线程池（2026-09-24，子代理）

模块 9 个 `@Scheduled` 轮询此前全部挤在 RuoYi 全局 `schedule-pool`（cores+1）上，引擎不可达时一轮
状态对账能被 10 秒超时拖住数分钟，连带饿死其他模块的定时任务。新增 `SyncSchedulingConfig`：
bean `syncScheduler`、线程 `sync-sched-*`、`sync.scheduler.pool-size` 默认 9（每个 fixedDelay 轮询一条
线程）。**刻意不用 `ThreadPoolTaskScheduler` bean**：应用里没有任何 `TaskScheduler`，Spring 会把
"唯一的 TaskScheduler"当成所有模块 `@Scheduled` 的默认调度器；改为 `autowireCandidate=false` 的
`ScheduledExecutorService`，按 bean 名路由。**验证**：`SyncSchedulingConfigTest` 用真实 Spring 容器
证明 sync 轮询跑在 `sync-sched-*`、其他模块仍在 `schedule-pool-*`、dev-fast 延迟初始化下照常触发；
真实后端线程转储确认 `sync-sched-1..9` 存在。

### P1-8 Kafka 桥接 worker 池加上限（2026-09-24，子代理）

`newCachedThreadPool` 换成有界线程池 + 准入控制，`sync.kafka-bridge.max-workers` 默认 64。原方案
"超限标 FAILED"被推翻：满额时引擎作业是健康的、raw topic 保留数据，判失败既停不掉作业又制造告警。
操作员路径（启动/恢复/重新初始化等）在提交引擎作业之前拒绝并点名配置项；作业已在运行的 owner
（对账器、刷新即时修复、重启恢复）被"挂起"：不改状态、不写库、只在进出时各记一条日志，空位出现后
优先于新启动、从已提交 offset 续传。挂起状态在读取时以 `lastError` 前缀展示。

### UX-7 任务组表单改为三步向导（2026-09-24，子代理）

任务组表单改为与单表同构的三步：数据源（源端探查 + CDC 预检 + 目标 + Kafka 输出格式）→ 表与字段 →
同步方式与限速（名称、模式、源库保护、提交确认）。两个向导共用新抽出的 `components/sync/SyncWizard`
与 `useSourceProbe`。顺带修掉 UX-1 留下的两个问题（保存后多弹一个「放弃本次填写？」、点 X 叠出两个
确认框）以及旧表单的元数据按位置错位、改选源表保留旧字段、DOM id 与列表搜索框冲突。
**验证**：Playwright 72 项断言全过、0 页面错误。

### P2-2 真实栈黑盒端到端回归套件（2026-09-24，子代理）

`org.dromara.sync.e2e`（`@Tag("e2e")`，`-Dgroups=e2e`，永不进入 dev 回归）只经后端 HTTP 驱动
`dev.ps1 up -Poc` 起来的真实栈，JDBC 在真实源/目标库造数与断言、Kafka 客户端读真实 topic、
SeaTunnel REST 交叉核对引擎状态。场景：FULL→PG/MySQL 逐行一致；FULL_CDC 全生命周期（暂停期写入不落
目标、恢复复用原 jobId 且目标端手工标记行未被覆盖——证明是 savepoint 续跑而非重新快照）；任务组 DDL
只隔离变更表；Kafka ENVELOPE 事件；引擎启动过渡态映射。撤销日志 + 泄漏检查保证不留残留。
根 pom 把 surefire `<groups>` 写死为 `${profiles.active}`，`-Dgroups` 原本被静默忽略，已修正接线。
**它首次运行就钉出三个产品缺陷**：引擎 `SCHEDULED` / `CANCELING` 过渡态落入 default 被判 FAILED
（已修）；删除任务组残留 DDL 事件（已修）；MySQL 目标 DATETIME 整体 +8 小时（排查中）。

### P2-3 任务与任务组共用一套引擎作业生命周期（2026-09-24）

新增 `engine.EngineJobRunner`，集中持有此前两边各写一遍、且已经分叉的不变量：桥接先于提交、提交
失败回收桥接（`preflight()` 区分"引擎未参与前被拒绝"与"提交本身失败"）；恢复守卫——配置指纹一致
**且确有 savepoint**（任务组此前从不检查，无 savepoint 时"从 savepoint 恢复"会静默重新全量，Kafka
目标整表事件重复）；状态轮询 3 次容忍 + 恢复边界即时判定；停止统一"先引擎后桥接"。两个服务只保留
各自的状态映射、校验、落库与聚合。随之修正：容量不足导致的恢复被拒不再落 FAILED；运行中作业的桥接
无法恢复改为记录 `lastError` 而非判失败（修复后从 offset 续传不丢数据）；任务组刷新不再把元数据库/
指标写入异常计为引擎失败（此前三次后会隔离健康的表）。两个服务的状态机测试改用真实 runner + mock 引擎。

### 本轮真实栈验收（2026-09-24）

以 P1-3 + P1-4(2) + P1-8 + P2-3 + 过渡态修复的完整代码重启后端，跑 P2-2 端到端套件：单表 FULL_CDC 全生命周期
（savepoint 暂停/恢复续跑/停止）、任务组 DDL 隔离、Kafka 事件信封、FULL→PG、FULL→MySQL 全部通过；唯一失败
是既有缺陷「MySQL 目标 DATETIME +8 小时」（非回归，排查中）。过渡态探针首轮因引擎 GC 停顿（11 次 young GC
共 7 秒）提交超时未能执行，重跑 3 次共 12 个采样均未落在过渡窗口（记为 skipped，映射由单测钉住），全程未出现
过渡态被判 FAILED，提交超时也未留下幽灵作业。真实后端线程转储确认 `sync-sched-1..9` 生效。

### MySQL 目标时间值 +8 小时（2026-09-24，子代理，由 P2-2 端到端套件发现）

根因在 MySQL Jdbc sink 这一跳：sink 与 MySQL-CDC 源端共用带 `serverTimezone=Asia/Shanghai` 的 URL，SeaTunnel
以 `setTimestamp(Timestamp.valueOf(LocalDateTime))` 绑定 DATETIME/TIMESTAMP/TIME，Connector/J 默认
`preserveInstants=true`，把它当作引擎 JVM 时区（UTC）的时刻再按上海时间渲染——FULL 与 FULL_CDC 都 +8h；PgJDBC
按 JVM 时区渲染，所以 PG 目标没问题。直接向引擎提交探针作业（同一源同时写 Console / 旧 URL / 新 URL / PG 四个
sink）逐跳定位。MySQL 目标改用独立 sink URL（`preserveInstants=false`、不设时区），值原样写入且与引擎时区无关；
源端与 PG sink URL 字节不变。**MySQL 目标任务的配置指纹随之改变**：已暂停/失败的需重新初始化（这也正是重写已偏移
数据所需的全量），运行中的需停止后重新初始化——刻意不把该项排除出指纹，否则旧作业会在错误数据上续写。
端到端用例 `fullToMysqlKeepsDatetimeWallClock` 已转绿。

### P2-4 拆分任务组服务（2026-09-24，子代理）

`SyncTaskGroupServiceImpl` 1207 → 958 行，只保留 CRUD、校验/预览、生命周期与状态刷新。整库新增表发现（含
autoDiscover 轮询）拆为 `SyncTaskGroupDiscoveryServiceImpl`（仍在组锁内、不包事务），数据核对拆为
`SyncTaskGroupDataCheckServiceImpl`；三者共用的表项级步骤收拢到包内可见的 `GroupItemOperations`，不暴露在任何
服务接口上。控制器直接调用新服务，路由、权限、日志与响应结构不变；新增 `constant.SyncScope` 与
`GroupStatuses.isLive` 收掉重复字面量。单测 178 → 193，合并后完整端到端回归全绿。

### P2-7 任务组表数上限可配置，整库发现也受约束（2026-09-24）

原先前后端各写死一份 20，且只在多表保存时校验——**整库自动发现完全不受约束**：500 张表的库会建出 500 个表项，
运行中且开启自动发现的组还会提交 500 个引擎作业，每个各占一条到客户源库的 binlog 连接。新增
`sync.group.max-tables`（默认 20、钳制 1..200；默认不上调，真正的瓶颈是源库连接数）；多表保存按它校验，整库发现
只补到上限、超出的表不纳入并在结果与组错误信息中说明（周期发现不重复写库）；`GET /sync/group/limits` 供向导显示
「已选 N / 上限 M」。真实后端验证接口返回 20、整库组照常发现全部 3 张表。

### P1-4（第 1 部分）任务组表项并行轮询（2026-09-24）

`EngineJobRunner.pollAll` 在自有的有界守护线程池（`sync.engine.poll-parallelism`，默认 4）上并行轮询各表项的
引擎作业；只有轮询并行，结果仍在调用线程上逐表应用，语义不变。引擎变慢时，20 张表 × 2 次、每次可达 10 秒超时的
串行轮询不再把组锁占住数分钟。单测用"三个轮询在桩里互相等待"证明确实并发；重启后完整端到端回归全绿。

### Kafka `phase` 契约按实测定案（2026-09-24，子代理）

实测 SeaTunnel 2.3.13 MySQL-CDC→Kafka(DEBEZIUM_JSON) 的 raw topic：初始装载行与 binlog INSERT 逐字段同形
（`op=c`、`source` 相同、`ts_ms` 均为引擎采集时间、无 header）；连接器把 Debezium READ/CREATE 都映射为
`RowKind.INSERT`，sink 只写 `c`/`d`。平台拿不到可靠的快照信号，因此**不猜边界**：FULL_CDC 的初始装载行恒为
`phase=CDC`，只有 FULL 任务出现 `SNAPSHOT`；原生 MySQL 与 GoldenDB 相同（原文档写成 GoldenDB 独有有误）。
4 万行分块快照期间持续写入，并行度 1 与 4 下均无增量穿插进快照行，现有的阶段顺序校验与引擎行为一致。文档写明
消费端的替代依据（快照整体先于增量、按 key 幂等、装载完成需带外判定），并更正 UPDATE 恒有前像、`sourceEventTime`
为采集时间而非提交时间。新增 `KafkaTaskBridgeRawEventsTest`（实测 raw 事件驱动真实桥接 worker）。

### 数据核对只写核对列（2026-09-24）

单表与任务组的数据核对都在比对开始前读出整行，比对（`COUNT(*)` / 分段 KEY_RANGE）可能持续数分钟，结束后用
`updateById` 整行写回——期间状态刷新写入的状态、错误与 checkpoint 被旧快照覆盖（例如已 FAILED 的表项被写回
RUNNING）；且核对失败时把 `lastCheckMatched` 置空，而 `updateById` 跳过 null，上一次的「一致」结论原样保留。
两个 mapper 新增 `recordCheck`，只显式写 `last_check_*` 列（含 null）。端到端 FULL 场景的 COUNT / KEY_RANGE 核对在
真实库上走通新语句。

### MySQL 源端时区按数据源配置（2026-09-24，子代理）

MySQL-CDC 的 `server-time-zone` 与源端连接时区原先写死为 Asia/Shanghai：本地栈源库为 UTC 时，binlog 阶段的 TIMESTAMP
整体 +8h（快照阶段不偏移），所有目标都受影响。数据源新增可选「服务器时区」（IANA ID，migration 023），CDC 配置按它生成；
**留空为兼容模式（Asia/Shanghai），配置与原来逐字节相同，存量 CDC 指纹不变**（测试钉住 9 个存量指纹）；设置或修改后读取
该数据源的 CDC 任务需重新初始化，且有运行中任务时拒绝修改（与改主机端口同一规则）。连接测试与 CDC 前置检查实测源库
偏移，不一致时给出「增量阶段 TIMESTAMP 将偏移 N 小时」的非阻断警告和建议时区，表单可一键填入。FULL 源端改为
`preserveInstants=false`，DATETIME/TIMESTAMP 不再依赖引擎 JVM 时区；FULL 任务可暂停恢复，旧 URL 的指纹仍被接受，
已暂停的 FULL 任务无需重新初始化。**验证**：真实引擎上兼容模式 binlog TIMESTAMP +8h、设为 UTC 后 PG 与 MySQL 目标
各阶段均与源端一致；真实后端的连接测试与预检正确识别出源库 +00:00 并建议 UTC；浏览器检查 12/12（字段、检测、一键填入、
非法缩写被拒、未保存）；合并后完整端到端回归全绿。单测 202 → 218。

### TIME(p) 小数秒截断改为建任务时明确提示（2026-09-25）

小数秒丢失发生在 SeaTunnel 内部（FULL 源端 `Time.toLocalTime`、PostgreSQL sink 所有模式 `Time.valueOf`），连接参数无法修复；
绕过方案（在 SELECT 里把 TIME 转成字符串）会让 PostgreSQL 目标的自动建表把列建成 varchar，代价大于收益，因此不绕过。改为
在两个向导的「提交确认」里，当选中的 `TIME(p>0)` 列会被截断（全量模式任意目标，或 PostgreSQL 目标任意模式）时点名提示；
MySQL 目标的 CDC 模式保留小数秒不提示，Kafka 目标未实测不下结论。元数据接口对 `TIME(3)` 报 `DECIMAL_DIGITS=0`，改用
`COLUMN_SIZE > 8` 识别。浏览器检查 6/6（PG 提示且只点名 `TIME(3)` 列、MySQL+CDC 不提示、切换全量后提示、未保存）。

### 整库任务组保存先规划后落库、编辑保留表项（2026-09-25，子代理 + 收尾）

两个待办同一个根因：整库发现把远程工作和写库混在一起，又在保存事务里执行。发现拆为只调外部系统、不写库的 `plan` /
`readmitRejected` 与落库两半：保存整库组时先规划（列源库表、逐表读结构、推导字段选择与基线、校验同步键与目标兼容性、
Kafka 目标建 topic），再用一个事务只写组与表项——保存事务内不再有对客户源库的 JDBC 读取和 Kafka AdminClient 调用；
源库不可达时保存被拒且不写任何数据。编辑时源数据源 / 源库 / 目标均未变，则保留已有表项（id、目标 schema、指标历史、
DDL 事件与状态）只补新表；运行前就被拒的 `FAILED` 表项按发现规则重新校验；端点变化则按新端点重建。一次保存只递增一次
`config_version`。收尾时顺带统一了启动路径：原先只有 Kafka 整库组在启动时重新接纳被拒的表，且只重打基线、不重新推导
字段选择（「选中全部字段」的表因此不再跟随源表新增字段）；现在所有整库组启动时都走同一条 `readmitRejected` 规则，运行过
后才失败的表仍交给「重新初始化该表」，无法同步的表（无可用键）不再为它建 topic。**验证**：单测 218 → 225；真实后端上新建
整库组为 v1、改限速后 v2 且 6 个表项 id 全部保留、改目标后按新端点重建；合并后完整端到端回归全绿。

### 仍开放的待办（2026-09-24 收尾）

- **Kafka 真实快照信号与延迟**：评估 MySQL-CDC `format = compatible_debezium_json`——可保留 `op=r` 与源端提交时间，
  从而正确标 `SNAPSHOT`、让 `kafka_lag_seconds` 包含引擎读 binlog 的落后（现状在限速快照或暂停恢复回放时偏小）。
  代价：raw topic 编码变化、桥接需适配、现有 Kafka CDC 任务指纹变化需重新初始化。

