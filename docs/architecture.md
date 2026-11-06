# 系统架构

## 目标

平台负责把 SeaTunnel 的引擎能力包装成可配置、可观察、可恢复的数据同步产品。平台不实现 CDC 协议，也不自行保存或拼装引擎 checkpoint。

## 组件边界

```text
plus-ui-react
      |
      v
RuoYi-Vue-Plus 后端
      |-- 数据源与凭证
      |-- 任务/表实例/配置版本
      |-- 预检查与类型映射
      |-- 状态聚合、审计与数据核对
      |-- SeaTunnel 作业管理适配器
                 |-- REST: 提交、停止、状态、日志、指标
                 `-- savepoint/restore: 暂停与恢复
                              |
                              v
                      SeaTunnel Zeta
                       |           |
                       v           v
                 checkpoint     Connector
                 持久化存储     MySQL -> PostgreSQL
```

## 核心数据对象

- 逻辑任务：用户配置和操作的顶层对象。
- 表实例：逻辑任务下每张源表的独立执行与状态单元。
- 配置版本：已发布配置不可覆盖，checkpoint 与产生它的版本绑定。
- 引擎作业：SeaTunnel job。一个表实例（单表任务，或任务组中的一个表项）对应一个作业。
- 运行实例：一次启动或调度触发产生的执行记录。
- 引擎状态引用：jobId、checkpoint/savepoint 元数据及恢复状态位置，不包含实际引擎状态内容。

## 关键约束

1. SeaTunnel checkpoint/savepoint 是恢复的权威来源。
2. 平台状态必须定期与引擎状态对账。
3. 同一逻辑任务禁止并发运行多个实例。
4. 关系型目标默认按同步键执行幂等 upsert 和 delete。
5. 配置版本发生语义变化时不得直接复用旧 checkpoint。
6. 单机模式不承诺节点高可用，但所有状态必须落在持久化目录。

## POC 后已冻结的决策

- **一表一作业**：单表任务和任务组的每个表项各自是一个 SeaTunnel 作业，拥有独立的 checkpoint/savepoint；表级故障
  隔离与逐表暂停 / 恢复 / 重新初始化由此而来（[multi-table.md](multi-table.md)）。
- **接入方式**：Zeta REST API（`/submit-job`、`/job-info/{jobId}`、`/stop-job`、`/jobs/checkpoints/{jobId}`）；暂停 =
  `stop-job` 带 `isStopWithSavePoint=true`，恢复 = 以原 `jobId` 和同一配置 `submit-job` 带 `isStartWithSavePoint=true`
  （[job-lifecycle.md](job-lifecycle.md)、[engine-adapter-contract.md](engine-adapter-contract.md)）。
- **至少一次 + 幂等写入**，不启用 XA：关系型 sink 为 `exactly_once = false`、`enable_upsert = true`，按同步键 upsert / delete；
  Kafka 目标按同步键发布事件，由消费者按键应用。
- **checkpoint 存储**：`hdfs` 存储插件写本地文件系统（`file:///opt/seatunnel/checkpoint/`，本地引擎栈挂载到
  `deploy/local-stack/runtime/seatunnel/checkpoint`），间隔 5 秒、超时 60 秒、保留 10 个
  （`deploy/local-stack/seatunnel/config/seatunnel.yaml`）。
- **不做自动 DDL**：任务组检测到源表结构漂移时以 savepoint 暂停该表（`DDL_BLOCKED`），目标端修复并通过兼容性检查后恢复；
  或"重新初始化该表"按源表当前结构重建，整表选择的表项自动纳入新增列（[ddl-change-management.md](ddl-change-management.md)）。
