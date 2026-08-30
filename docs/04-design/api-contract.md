# MVP API 契约

接口均位于 `/sync`，遵循 RuoYi 统一响应结构和 Sa-Token 权限校验。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/data-source/list` | 分页查询数据源 |
| GET | `/data-source/{id}` | 查询数据源详情（不返回密码） |
| POST | `/data-source` | 新增数据源 |
| PUT | `/data-source` | 修改数据源；密码为空时保留原值 |
| DELETE | `/data-source/{id}` | 删除数据源 |
| POST | `/data-source/{id}/test` | 测试 JDBC 连接 |
| GET | `/data-source/{id}/databases` | 探查可访问的数据库列表 |
| GET | `/data-source/{id}/tables?databaseName=...` | 探查指定数据库的表列表 |
| GET | `/data-source/{id}/metadata?databaseName=...&tableName=...` | 探查表字段、主键、唯一键和字符集 |
| POST | `/data-source/{id}/cdc-precheck` | 执行 MySQL binlog CDC 前置检查 |
| POST | `/data-source/credential-migrate` | 在启用加密密钥后，将存量明文凭证幂等迁移为密文 |
| GET | `/task/list` | 分页查询同步任务 |
| GET | `/task/{id}` | 查询任务详情 |
| POST | `/task` | 新建单表同步任务 |
| PUT | `/task` | 修改草稿或已停止任务 |
| DELETE | `/task/{id}` | 删除非运行中任务 |
| POST | `/task/{id}/validate` | 校验源端和目标端连接 |
| POST | `/task/{id}/target-compatibility` | 校验 PostgreSQL 目标表结构兼容性 |
| POST | `/task/{id}/engine-config` | 生成脱敏的 SeaTunnel HOCON 配置预览 |
| POST | `/task/{id}/start` | 提交并启动新的 SeaTunnel 作业 |
| POST | `/task/{id}/status` | 查询引擎状态并刷新任务状态、checkpoint 摘要和 SeaTunnel 运行指标投影 |
| POST | `/task/{id}/check` | 只读核对源表和目标表；默认整表行数，也可用 `{mode:"KEY_RANGE",blockSize:10000,strictWatermark:true}` 对单列数值同步键分块比较，并持久化最近一次核对结果 |
| POST | `/task/{id}/pause` | 使用 savepoint 暂停作业 |
| POST | `/task/{id}/resume` | 使用原 jobId 和 savepoint 恢复作业 |
| POST | `/task/{id}/stop` | 停止作业，不保留可恢复状态 |

任务请求新增 `scheduleMode`、`cronExpression`、`incrementalStartupMode`、`incrementalStartupTimestamp`、`incrementalStartupBinlogFile` 和 `incrementalStartupBinlogPosition`；响应新增 `nextRunTime`、`lastTriggerTime`、`lastSkipReason`、`configVersion` 和 `overwriteStageTable`。`CRON` 保存时必须通过 Spring 六位 Cron 校验。后台触发与手动 `start` 共用同一启动服务和任务级 Redis 锁语义。

## 多表任务组

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/group/list` | 分页查询任务组 |
| GET | `/group/{groupId}` | 查询任务组及表项 |
| POST | `/group` | 新建任务组及表映射 |
| PUT | `/group` | 修改草稿或已停止任务组并递增配置版本 |
| DELETE | `/group/{groupId}` | 删除非运行中任务组及表项 |
| POST | `/group/{groupId}/validate` | 批量连接、CDC 和目标兼容性校验 |
| POST | `/group/{groupId}/engine-config` | 生成逐表脱敏配置预览和配置指纹 |
| POST | `/group/{groupId}/start` | 按表提交 SeaTunnel 作业并聚合状态 |
| POST | `/group/{groupId}/discover` | 扫描整库任务组的源库并为新增表创建独立表项 |
| POST | `/group/{groupId}/ddl-check` | 对组内表项执行源表结构差异检查并隔离变更表 |
| POST | `/group/{groupId}/check` | 逐表只读比较源端和目标端行数，并持久化每个表项最近核对结果 |
| POST | `/group/{groupId}/item/{itemId}/resume-after-ddl` | 目标结构修复且兼容性通过后，使用原 savepoint 恢复单个表项 |
| POST | `/group/{groupId}/status` | 刷新所有表项作业状态 |
| POST | `/group/{groupId}/pause` | 对运行中的表项执行 savepoint 暂停 |
| POST | `/group/{groupId}/resume` | 使用表项 savepoint 恢复作业 |
| POST | `/group/{groupId}/stop` | 停止任务组全部表项作业 |

多表首版限制：仅支持 MySQL -> PostgreSQL、`FULL_CDC`，每组最多 20 张表；当前每张表独立 SeaTunnel job，组级接口返回聚合结果，表项状态和错误是排查依据。任务组 `syncScope` 可为 `MULTI_TABLE` 或 `DATABASE`；整库组以 `sourceDatabase` 作为扫描范围，可选择 `autoDiscover` 周期扫描。单表任务支持 `FULL`、`INCREMENTAL` 和 `FULL_CDC` 三种模式；任务组首版仍限定 `FULL_CDC`。

整库组发现表时会检查同步键及目标表兼容性。无主键且没有全列非空唯一键的表会写入失败表项但不创建引擎作业；整库启动会跳过这些已隔离表，成功表继续运行，组级状态返回 `DEGRADED`。源端、目标端或 CDC 前置检查失败仍会阻断整个整库组启动。

MVP 请求约束：`sourceType=MYSQL` 只能作为源端，`sourceType=POSTGRESQL` 只能作为目标端；单表 `syncMode` 为 `FULL`、`INCREMENTAL` 或 `FULL_CDC`，任务组首版为 `FULL_CDC`；`ddlPolicy` 默认 `FAIL`。`INCREMENTAL` 从提交作业后的最新 binlog 位点开始，不补齐此前历史，目标端必须已有可信基线；`FULL` 不执行 CDC，完成后进入 `FINISHED`。任务表单使用数据源配置的默认数据库，数据库/表探查用于创建前确认和元数据提示；整库任务仍需后续表实例模型。连接测试返回 `success`、`message`、`latencyMs`，任务校验返回 `source`、`target`、`cdcPrecheck`、`targetCompatibility` 和综合 `valid`。

## 字段映射与同步键

单表任务和多表表项支持 `selectedColumns` 与 `syncKeyColumns` 两个配置字段。HTTP 请求可使用逗号分隔的字段名；查询响应保持同一形式。`selectedColumns` 为空时服务端按源表全部字段补齐并持久化，MVP 只支持同名一对一映射和排除字段，不支持改名、计算字段、关联或行过滤。

`syncKeyColumns` 必须精确匹配源表主键，或一个所有组成字段均为 `NOT NULL` 的唯一索引。源表有多个可靠唯一键时，前端要求用户明确选择；同步键列必须存在于 `selectedColumns` 中。保存时服务端重新探查元数据并拒绝不存在的列、不可靠的键和排除同步键的请求。对已有目标表，兼容性检查只要求所选字段存在且类型兼容，并要求目标主键顺序与已选择同步键一致。

对于 `FULL_CDC` 和 `INCREMENTAL`，SeaTunnel 配置使用 `Sql` Transform 投影已选列，JDBC sink 只接收投影后的字段；`FULL` 任务使用显式列清单的 JDBC `SELECT`。因此字段排除同时作用于全量和 CDC 变更，不只是界面配置。`tinyint(1)`、TEXT/BLOB/JSON 和非 Unicode 源字符集会在创建表单显示风险提示，不会隐式改变字段类型。

## 源库保护参数

单表任务和任务组请求都可携带以下任务级字段；任务组字段按每个独立表项作业生效，不代表对整个组的共享连接预算：

| 字段 | 默认值 | 平台硬上限 | SeaTunnel 生效位置 |
|---|---:|---:|---|
| `readLimitRowsPerSecond` | 1000 行/秒 | 100000 | `env.read_limit.rows_per_second` |
| `readLimitBytesPerSecond` | 10485760 字节/秒 | 1073741824 | `env.read_limit.bytes_per_second` |
| `snapshotParallelism` | 1 | 4 | `env.parallelism` |
| `sourceConnectionLimit` | 2 | 8 | MySQL CDC `connection.pool.size` |

保存、启动和重新初始化均会执行范围校验；缺省值会按保守默认值持久化。`FULL` 模式的 JDBC source 是单连接读取，因此 `sourceConnectionLimit` 只对 MySQL CDC source 写入引擎配置。超过硬上限会被接口拒绝，前端限制仅用于提前反馈，不能替代服务端校验。

CDC 前置检查返回 `passed`、`message`、`serverId`、`gtidMode`、`binlogRetention` 和 `checks`。`checks` 中的硬性项目包括 `log_bin=ON`、`binlog_format=ROW`、`binlog_row_image=FULL`、正数 `server_id` 和复制权限；时区和保留策略作为风险提示返回。检查未通过时，前端不得继续执行任务校验或启动流程。

目标兼容性检查返回目标表是否存在和逐项 `checks`。目标表不存在属于可接受状态，由 SeaTunnel 按源表结构自动建表；目标表已存在时，源字段缺失、字段类型族不兼容、目标主键与源主键不一致、目标额外必填列无默认值均为硬性失败。额外唯一约束等潜在写入风险在后续版本补充为非阻断告警。

`POST /task/{id}/start` 在提交 SeaTunnel 前服务端强制执行同一套综合校验；校验失败只返回启动错误，不改变草稿/已停止任务状态。只有校验通过且引擎提交成功后，任务才会进入 `RUNNING`。

单表任务创建采用五步向导，具体页面规则和纯增量位点策略见 [`task-creation-wizard.md`](task-creation-wizard.md)。纯增量任务必须先通过 CDC 前置检查；`TIMESTAMP` 使用 Asia/Shanghai 本地时间转换为毫秒 epoch，`SPECIFIC` 的 binlog 位置不得小于 4。

## SeaTunnel 配置预览

`POST /task/{id}/engine-config` 只生成配置，不提交或启动作业。接口会重新读取源表 JDBC 元数据并解析主键列，支持联合主键；没有主键的表会被拒绝，以避免 CDC upsert 无法稳定定位目标行。返回的 `config` 已将 `password` 字段替换为 `******`，平台日志和前端不得尝试还原凭证。

启动接口在服务端生成同一份未脱敏配置并通过 SeaTunnel REST API 提交，平台只保存配置 SHA-256 指纹，不保存配置正文。暂停调用 `stop-job` 的 `isStopWithSavePoint=true`，恢复调用 `/submit-job` 的 `isStartWithSavePoint=true` 并复用原 `jobId`；配置指纹变化时禁止恢复。

SeaTunnel 2.3.13 connector 已通过运行镜像内的字节码核实 `connection.pool.size` 配置键，避免将平台参数写成引擎无法识别的 HOCON。任务组当前为“一表一作业”，所以组内多个表项同时启动时的累计源端连接数仍需后续调度/配额阶段治理。

引擎配置中的 `server-id` 根据任务 ID 稳定计算，同一任务重复预览得到相同值；后续提交阶段必须在任务配置版本变更时重新生成并校验冲突。

## 故障与数据核对

状态刷新访问 SeaTunnel 失败或返回不存在的 `jobId` 时，任务会持久化为 `FAILED`，并在 `lastError` 与接口 `errorMessage` 中返回脱敏后的原因；不会把不可达状态误报为运行中。应用启动完成后会扫描数据库中 `RUNNING`/`PAUSING` 任务并执行一次状态刷新，无法联系引擎的任务同样会被标记为失败。

数据核对接口只执行 `COUNT(*)`，不读取或返回业务行内容。表名必须是单段或两段字母、数字、`_`、`$` 标识符；连接失败时返回 `success=false` 和明确错误，行数一致时 `matched=true`。

`POST /group/{groupId}/check` 对任务组的每个表项独立执行同一检查。返回组级 `tableCount`、一致/不一致/失败数量、每个表项的行数和结论，并将最近一次结果写入 `ds_sync_task_group_item.last_check_*`。单个表连接或目标表失败不会中断其他表；组内任何失败或不一致都会使组级 `matched=false`。持续 CDC 运行时接口会返回“非同水位行数检查”说明，不能将结果作为严格同一时刻的一致性证明。
