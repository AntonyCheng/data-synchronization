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

### [ ] P0-1 提交超时会留下"幽灵作业"，且可能造成目标表双写

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

### [ ] P0-2 `TargetTableSwap` 是破坏性操作但零测试

**问题**：FULL/OVERWRITE 模式下做 `target → backup、stage → target、drop backup` 的重命名切换。
代码本身写得稳（标识符正确转义，PG 走事务、MySQL 走原子多表 RENAME），但**没有任何测试**，
而它一旦出错就是目标表丢失。

**证据**：`support/TargetTableSwap.java`（110 行，`find test -name TargetTableSwapTest.java` 无结果）

**方案**：至少覆盖 SQL 拼装（标识符转义、目标表存在 / 不存在两种分支）；有条件再用 Testcontainers
跑一次真实 PG + MySQL 的切换。**工作量**：0.5 天（纯拼装）/ 1.5 天（含容器）。**风险**：无。

---

## P1 — 规模化瓶颈 / 用户可见的错误数字

### [ ] P1-1 首页仪表盘不统计任务组，且静默截断到 100 条

**问题**：仪表盘在浏览器里拉 100 条任务 + 100 个数据源后自行聚合。两个后果：

1. **只统计单表任务**——只用 `多表同步` 的用户，首页"运行中任务"恒为 0；
2. 超过 100 条后所有数字（运行中任务、已核对行数、成功率、平均延迟）静默失真，页面不会提示。

**证据**：`web/src/pages/index.tsx:61-63`（`pageSize: 100`）、`:75-86`（客户端聚合，只遍历 `tasks`）

**方案**：后端加一个只读聚合端点（`GET /sync/overview`），用 COUNT/SUM 一次算出任务 + 任务组
+ 表项的口径，前端改读它。顺带补上自动刷新。**影响面**：新增一个端点 + 首页改造。
**工作量**：1 天。**风险**：低（新端点，不动既有接口）。

### [ ] P1-2 元数据读取每次新建一条不池化的 JDBC 连接

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

### [ ] P1-3 生命周期方法在数据库事务内做远程调用

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

### [ ] P1-4 状态刷新串行，且 7 个后台轮询挤在共享线程池

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

### [ ] P1-5 后台热查询全是全表扫

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

### [ ] P1-6 任务组分页有 N+1 查询

**问题**：`queryPageList` 对分页结果每行各查一次表项、再查一次指标。一页 10 个组 = 21 次查询。

**证据**：`service/impl/SyncTaskGroupServiceImpl.java:124`（`forEach(this::attachItems)`）→
`attachItems` 内 `itemMapper.selectByGroupId` + `metricsService.latestForGroupItems`

**方案**：按整页 groupId 批量查表项，再按全部 itemId 批量查指标（`selectLatest` 已经支持批量入参），
在内存里分组回填。**影响面**：一个私有方法。**工作量**：0.5 天。**风险**：低。

### [ ] P1-7 数据源下拉硬编码 100 条上限

**问题**：任务页 / 任务组页 / 首页都用 `listDataSources({ pageNum: 1, pageSize: 100 })`。
超过 100 个数据源时，向导里直接选不到后面的。

**证据**：`web/src/pages/sync/task/index.tsx:28`、`web/src/pages/sync/group/index.tsx:27`、
`web/src/pages/index.tsx:62`

**方案**：后端加一个轻量 `GET /sync/data-source/options`（只返回 id/名称/类型/库名），前端改为
远程搜索式下拉。**工作量**：0.5 天。**风险**：低。

### [ ] P1-8 Kafka 桥接线程池无上限

**问题**：`Executors.newCachedThreadPool`，每个运行中的 Kafka 任务/表项独占一条线程 + 一个 consumer
+ 一个 producer。100 个 Kafka 任务就是 100 条常驻线程，没有任何上限或背压。

**证据**：`kafka/KafkaTaskBridgeService.java:56`

**方案**：换成有界线程池，超限时拒绝并把该表项标记为 FAILED + 落 `last_error`（让对账器下一轮重试），
而不是无声地把 JVM 撑爆。**工作量**：0.5 天。**风险**：中（要确认对账器不会因拒绝而陷入重试风暴）。

---

## P1-UX — 表单与向导体验

本节针对"填表不丝滑"。全部是纯交互改动，不动任何接口和校验规则。下面每条都在代码里能指到位置，
其中 UX-1 和 UX-2 是 2026-09-22 用浏览器实测两个向导时亲自踩到的。

### [ ] UX-1 误按 Esc 或点遮罩会丢掉整份填了一半的表单

**问题**：两个弹窗的 `modalProps` 都只有 `destroyOnHidden: true`，没有关闭防护。按 Esc、点遮罩
都会直接关闭，`destroyOnHidden` 随即销毁表单——单表向导填到第 5 步、任务组加了好几张表，
一次误操作全部清零，且没有任何确认。

**证据**：`web/src/pages/sync/task/components/TaskFormModal.tsx:308`、
`web/src/pages/sync/group/components/GroupFormModal.tsx:161`

**方案**：`maskClosable: false` + `keyboard: false`；点"取消"或右上角关闭时，若表单已被修改
（`form.isFieldsTouched()`）先弹二次确认。**工作量**：1 小时。**风险**：无。

### [ ] UX-2 探查期间没有状态反馈，点"下一步"只会弹一条错误

**问题**：选完源数据源后，后台串行跑连接测试 → CDC 预检 → 库列表 → 表列表（实测 1.5~2.5 秒），
这期间"下一步"按钮既不 loading 也不禁用。用户点下去得到的是一条 toast
`请等待源表元数据加载完成后继续`——**用错误提示代替了加载状态**，用户不知道该等多久、等什么。

**证据**：`TaskFormModal.tsx:320-321`（按钮无 `loading`/`disabled`）、`:242`（事后报错）、
`:85 metadataLoading` 这个状态只用在了表下拉的 loading 上（`:419`）

**方案**：`下一步` 绑定 `loading={metadataLoading}` 并在探查未完成时禁用；在步骤区显示当前进度
（"正在测试连接 / 正在读取 CDC 配置 / 正在读取表列表"），让等待可见。**工作量**：0.5 天。**风险**：无。

### [ ] UX-3 校验失败靠 toast，不定位到出错的字段和步骤

**问题**：向导里 6 处用 `message.error` 报校验失败。最伤的一条是第 5 步提交时的
`请返回前面步骤，补全源数据源、源表、目标表、字段和同步键`——**既不说是哪一步，也不跳转**，
用户只能自己一步步倒回去找。

**证据**：`TaskFormModal.tsx:157/238/242/266/270/313`

**方案**：把缺失项映射回它所属的步骤，自动 `setWizardStep` 跳过去 + `form.scrollToField` 滚动到该字段
并就地标红；toast 只留一句摘要。**工作量**：0.5 天。**风险**：低。

### [ ] UX-4 字段选择在宽表上不可用

**问题**：同步字段是一个平铺的 `Checkbox.Group`，没有全选 / 反选 / 搜索 / 已选计数。实测 9 列的
`customers` 就占掉大半屏；几十上百列的宽表会把弹窗撑爆，且用户没法"只排除两列"。

**证据**：`TaskFormModal.tsx:542-548`、任务组侧同构 `GroupFormModal.tsx:388`

**方案**：换成带搜索框 + 全选/反选 + "已选 N / 共 M"的选择器（列多时用虚拟滚动）；同步键字段保持
禁用且置顶显示，说明为什么不能取消。**工作量**：1 天。**风险**：低。

### [ ] UX-5 关系型目标表名没有默认值，Kafka 有

**问题**：选完源表后，Kafka 目标会自动填出 topic 名（`defaultKafkaTopic`），但 MySQL / PostgreSQL
目标的"目标表名"是空的，必须手输——而绝大多数场景就是同名。

**证据**：`TaskFormModal.tsx:224-227`（只有 Kafka 分支填默认值）

**方案**：关系型目标同样预填为源表名（可改），与 Kafka 分支对齐。**工作量**：15 分钟。**风险**：无。

### [ ] UX-6 任务组表单单页过长，每张表都要重复填一遍

**问题**：880px 的弹窗里依次塞了名称、粒度、同步方式、源/目标数据源、4 项限速、然后是表项列表——
每张表展开后有 源表 / 目标 Schema / 目标表 / 创建 topic / 删除 + 一整片字段复选框 + 同步键。
加到第三张表就要滚很久，而目标 Schema 这种组内通常一致的值要逐表填。

**证据**：`GroupFormModal.tsx:305-410`（每个表项 6 个 Form.Item）

**方案**：
1. 表项改用可折叠面板，标题行显示"源表 → 目标表 · 已选 N 字段"，默认折叠已配好的；
2. 支持**批量添加**：源表下拉改多选，一次生成多个表项并按默认规则填好目标表名；
3. 目标 Schema 提升为组级字段，表项里只在需要覆盖时展开。

**工作量**：1.5 天。**风险**：低（提交载荷结构不变，仍是 `items[]`）。

### [ ] UX-7 两个入口的填写范式不一致

单表是 5 步向导，任务组是单页长表单。同一个产品里两套范式，用户要学两次。建议在 UX-6 之后把
任务组也对齐成 3 步（数据源 → 表与字段 → 同步方式与限速）。**工作量**：1 天。**风险**：中
（改动面大，建议排在 UX-1~UX-6 全部落地并实测之后）。

---

## P2 — 结构与可维护性

### [ ] P2-1 三个高风险类零测试

`SyncTaskGroupDdlServiceImpl`（漂移状态机，228 行，决定一张表是否被隔离）、
`SyncColumnSelectionValidator`（每个任务创建都要过的纯函数）、`TargetTableSwap`（见 P0-2）。
`SyncColumnSelectionValidator` 是纯函数，补测试几乎零成本，优先做。
**工作量**：合计 1.5 天。**风险**：无。

### [ ] P2-2 全部是 mock 单测，没有一条集成测试

97 个单测全部 mock 掉了引擎和数据库，引擎交互只靠人工 e2e 覆盖。建议用 Testcontainers 补一条
最小链路（MySQL → PG，FULL 模式，起停一次），挂在单独的 profile 上，不进默认构建。
**工作量**：2 天。**风险**：低（新增，不动现有）。

### [ ] P2-3 任务与任务组的生命周期是两套平行实现

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

### [ ] P2-4 `SyncTaskGroupServiceImpl` 仍有 1046 行

承担 CRUD + 校验 + 生命周期 + 整库发现 + 数据核对 + 两个后台轮询。DDL 和 Kafka topic 已经拆出去了，
"整库发现"和"数据核对"可以按同样的方式拆成独立服务。做完 P2-3 后会自然瘦身，建议合并考虑。
**工作量**：1 天。**风险**：中。

### [ ] P2-5 `schemaHash` 存了但从不参与比较

DDL 检查每次都把快照 JSON 解析成对象做完整 diff，而行上已经存了 `schema_hash`。先比 hash、
相等直接跳过 diff，能省掉绝大多数轮次的解析开销，语义不变。
**证据**：`SyncTaskGroupDdlServiceImpl.java:107`（算出 hash）、`:121`（仍做完整 diff）
**工作量**：1 小时。**风险**：无。

### [ ] P2-6 `itemStatusFailureStreak` 不随表项删除清理

内存里的 `ConcurrentMap<Long, Integer>`，表项被删除后其计数项永远留着。单条数据量极小，
但属于无界增长。删除表项/任务组时顺手清一下即可。
**证据**：`SyncTaskGroupServiceImpl.java:113`
**工作量**：15 分钟。**风险**：无。

### [ ] P2-7 `MAX_TABLES_PER_GROUP = 20` 硬编码

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
