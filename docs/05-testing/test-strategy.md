# POC 与 MVP 测试策略

## 分层

1. 配置静态检查：Compose、SQL、SeaTunnel 配置及必需文件。
2. 环境健康检查：容器、端口、数据库和 REST API。
3. 数据正确性：快照、增删改、联合主键、删除传播、类型与字符集。
4. 恢复测试：checkpoint、暂停恢复、数据库故障和 binlog 过期。
5. 性能测试：吞吐、延迟、源库影响和恢复时间。
6. 产品验收：任务状态与引擎状态一致，错误可理解且不会静默丢数。

## 测试数据原则

- 每个用例使用可预测的主键和业务值。
- 源端变更脚本与目标端期望结果分开保存。
- 校验不仅比较行数，还比较按同步键排序后的内容摘要。
- 失败用例必须验证错误状态，而不是只验证命令返回非零。
- 每次执行的环境信息、命令结果和数据库快照写入独立时间戳结果目录。

## POC 首轮范围

- `customers`：单主键、Unicode、时间和 JSON。
- `orders`：联合主键、decimal 和状态更新。
- `inventory_by_sku`：无主键、非空唯一键候选能力。
- 后续性能表：连续主键、可配置行数和可选大字段。

## 本阶段新增验收用例

- 单表五步向导：验证数据源连接失败不能进入下一步，前进/回退保留表单值，编辑任务能回填五步配置，末屏显示完整汇总后再提交。
- 纯增量启动位点：分别创建 `LATEST`、`TIMESTAMP` 和 `SPECIFIC` 草稿，配置预览必须包含对应 SeaTunnel 参数；未来时间、非法 binlog 文件名和位置小于 4 的请求必须被服务端拒绝。非增量任务的位点字段必须被清空。

- 凭证迁移：未启用加密时拒绝迁移；启用密钥后明文迁移为 `ENC_`，重复执行结果幂等，接口和日志不返回密码。
- 任务组正向：已执行并通过。创建两张表映射，批量校验通过，逐表配置预览脱敏，启动后组状态与表项状态可刷新；证据见 `test/results/20260825-platform-group/acceptance.md`。
- 任务组负向：连接失败、CDC 前置检查失败、目标字段缺失/类型不兼容、无主键表均阻断启动并返回表级原因。
- 任务组补偿：第 N 张表提交失败时，已提交表项被停止，组记录失败原因，不留下未记录的部分运行状态。
- 生命周期：已执行并通过。任务组暂停生成两个表项 savepoint，恢复复用同一配置版本，停止后引擎状态为 `CANCELED`；运行中修改和删除接口被拒绝的负向断言保留在 UI/API 回归用例中。
- 环境回归：迁移脚本重复执行不报错，既有单表 POC 任务和系统用户/部门菜单保持可用。
- 故障恢复演练：损坏 checkpoint 后 restore 必须失败并可在恢复文件后继续；清理 MySQL binlog 后 restore 必须进入 FAILED，错误必须明确指出 binlog/offset 不可用，平台随后归类为 `REINITIALIZE_REQUIRED`。
- 平台重启对账：保留隔离 `FULL_CDC` 作业，重启 Java 21 后端，验证 RUNNING 状态、engine jobId、checkpoint 重新发现且 CDC marker 继续传播；证据见 `docs/03-poc/mvp-closeout-report.md`。
- 运行监控：已验证已结束 SeaTunnel 作业状态为空时，状态接口仍返回 200，并返回任务状态、阶段和缺失指标说明，不因指标投影异常 500。
- 核对持久化：已验证 `POST /sync/task/{id}/check` 返回源端 4 行、目标端 4 行、差异 0，并将六个 `last_check_*` 字段写回任务表。
- 任务组逐表核对：已验证 `POST /sync/group/{groupId}/check` 对每张表独立执行行数检查并写回表项的六个 `last_check_*` 字段；目标表缺失时仅该表项失败，其余表仍完成核对，组级结果正确聚合一致/不一致/失败数量。
- 源库保护：已验证迁移后的默认值可被单表与任务组读取，任务组预览会为每个表项生成行数限速、字节限速、快照并行度以及 MySQL CDC `connection.pool.size`。隔离 POC 以 500 行、100 行/秒、1 MiB/秒验证 SeaTunnel 接受配置且实际吞吐不超过配置，证据见 `test/results/20260827-092923/`；超出平台硬上限的请求由服务端 `ResourceProtectionPolicy` 拒绝，前端数字输入仅作提前限制。
- 字段选择与同步键：新建任务默认包含源表全部字段。验证可排除 TEXT/JSON 等非同步键字段，并确认 SeaTunnel 全量快照和后续 INSERT/UPDATE/DELETE 都不会将排除字段写到目标表；尝试排除同步键、提交不存在字段、或把可空唯一键作为 CDC 同步键必须被服务端拒绝。多个非空唯一键候选时，验证只有用户选择的候选键会生成到 Jdbc `primary_keys`。
- 字段投影 POC：`test/jobs/mysql-to-postgres-selected-columns.conf` 使用 MySQL CDC `Sql` Transform 将 `customers` 投影为 `id`、`display_name`、`updated_at`。已验证 SeaTunnel 2.3.13 接受 `plugin_input/plugin_output` 配置，自动创建的 PostgreSQL 目标表仅包含三列；运行后应停止临时作业，避免影响常规 POC 容量。
- 调度与覆盖刷新：验证 `ONCE` 只触发一次、合法/非法 Cron 表达式校验、Cron 到期时同一任务 Redis 锁只允许一个运行实例，运行中触发写入跳过原因；`FULL + OVERWRITE` 先写入版本化临时表，成功后原子替换，失败时正式表和任务成功状态均保持不变。
- 扩大样本性能基线：`performance-baseline.ps1 -RowCount 50000` 已通过，记录全量吞吐、单事件 CDC 延迟、checkpoint 和容器资源快照；结果位于 `test/results/20260830-171539`，不作为生产容量承诺。

## Phase 5/6 可重复验收

核心边界先通过 `ruoyi-sync` 模块的 JUnit 回归执行。构建默认跳过测试，验收时必须显式关闭该开关并选择 `dev` 标签：

```powershell
cmd /c ".\mvnw.cmd -pl ruoyi-modules/ruoyi-sync -am -DskipTests=false -Dmaven.test.skip=false -Dgroups=dev -Dtest=ResourceProtectionPolicyTest,SyncTaskSchedulerTest -Dsurefire.failIfNoSpecifiedTests=false test"
```

`ResourceProtectionPolicyTest` 覆盖默认限速/并行度/连接池和硬上限拒绝；`SyncTaskSchedulerTest` 覆盖一次性调度只触发一次、Cron 到期后的跳过与下一次时间推进，并使用 Redis 锁和作业服务 mock，不提交 SeaTunnel 作业。

需要登录态时再运行真实 API 验收脚本。访问令牌只通过参数传入，不写入结果文件：

```powershell
pwsh -File test/scripts/phase5-acceptance.ps1 -Token $env:PHASE5_TOKEN
pwsh -File test/scripts/phase6-acceptance.ps1 -Token $env:PHASE6_TOKEN
```

Phase 5 脚本创建并清理临时 FULL 草稿，验证字段默认补齐、同步键、配置版本快照脱敏、编辑递增和非法位点/字段/模式拒绝。Phase 6 脚本验证 Cron 下次执行时间、非法 Cron、源库保护上限及覆盖刷新配置的版本化 stage 表；不会自动提交新 SeaTunnel 作业。真实覆盖替换成功和失败回滚应在隔离 POC 表上另行执行，避免影响现有任务。

覆盖刷新收口演练已在 `docs/03-poc/mvp-closeout-report.md` 记录：成功路径完成事务替换并清理 stage 标记，失败路径验证事务回滚和原表保留。平台重启对账、凭证加密迁移、50,000 行容量基线、最终清洁回归和登录态人工浏览器验收已完成；发布前须按 [发布检查清单](release-checklist.md) 完成离线交付包启动演练。离线包由 `test/scripts/build-offline-package.ps1` 装配，验证时在独立端口导入镜像、启动 Compose、启动 Java 21 后端，并访问前端与 `/prod-api/captchaImage`。

离线启动演练已完成：包内 14 份增量 SQL 在后端启动前执行，避免 JAR 与元数据库结构不匹配；MySQL/Redis 健康、SeaTunnel/Caddy 正常，后端、静态前端及 `/prod-api/captchaImage` 均返回 HTTP 200。交付构建拒绝包含 `.env` 或运行数据的输出目录，防止测试凭证和数据库文件被误打包。

## Phase 1.1 Oracle POC

Oracle 先以独立 POC 建立关系型目标端契约，不改动 PostgreSQL MVP 代码。运行 `test/oracle/scripts/up.ps1` 前，必须准备 Oracle Free 镜像和已校验的 `ojdbc11.jar`；脚本会在镜像、驱动、校验和、基础 MySQL POC 任一缺失时停止。首轮仅执行 `test/oracle/scripts/run-full.ps1`，验证预建 `DS_POC_CUSTOMERS` 的显式 `MERGE`、Unicode、CLOB、NUMBER 和 TIMESTAMP；MySQL JSON、CDC、恢复、性能、平台数据源注册及页面接入均需在全量基线通过后单独验收。
### Phase 4 数据质量核对完善

- `COUNT` 模式保持既有单表/任务组行数核对兼容性。
- 单列数值同步键使用 `KEY_RANGE` 模式时，验证块数、每块源/目标行数、差异和汇总统计。
- 人为删除或插入目标端一行，确认仅对应范围块标记不一致且总差异正确。
- 联合键、字符串键、无同步键表返回明确的不支持原因，并可切换回 `COUNT`。
- `FULL_CDC` 任务运行或暂停中使用严格水位核对时阻断并提示先暂停；静态任务和已暂停任务可继续。

## Phase 4 自动化验收脚本

`test/scripts/phase4-acceptance.ps1` 使用登录后的访问令牌调用真实接口，并只对 `ds-poc-mysql`、`ds-poc-postgres` 执行临时数据变更。脚本不会保存令牌；每次执行结果写入 `test/results/<timestamp>/phase4-acceptance.json`。

```powershell
pwsh -File test/scripts/phase4-acceptance.ps1 `
  -Token $env:PHASE4_TOKEN `
  -TaskId 2091505842154749953
```

脚本覆盖任务列表、`COUNT`、`KEY_RANGE`、范围内目标行缺失、非法分块步长和无同步键任务提示。严格水位用例通过 `-StrictTaskId` 指定一个状态为 `RUNNING` 或 `PAUSING` 的 `FULL_CDC` 任务；当前没有此类任务时会明确记录为 skipped，不会伪造通过。
