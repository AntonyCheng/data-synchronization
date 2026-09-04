# 系统架构初稿

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
- 引擎作业：SeaTunnel job，一张或多张表实例可映射到一个作业，具体分组策略由 POC 决定。
- 运行实例：一次启动或调度触发产生的执行记录。
- 引擎状态引用：jobId、checkpoint/savepoint 元数据及恢复状态位置，不包含实际引擎状态内容。

## 关键约束

1. SeaTunnel checkpoint/savepoint 是恢复的权威来源。
2. 平台状态必须定期与引擎状态对账。
3. 同一逻辑任务禁止并发运行多个实例。
4. 关系型目标默认按同步键执行幂等 upsert 和 delete。
5. 配置版本发生语义变化时不得直接复用旧 checkpoint。
6. 单机模式不承诺节点高可用，但所有状态必须落在持久化目录。

## POC 后需要冻结的决策

- 一表一作业还是多表合并作业，以及表级故障隔离方式。
- REST 与 savepoint/restore 的最终接入方式。
- PostgreSQL exactly-once 是否启用 XA，或采用至少一次加幂等写入。
- checkpoint 存储插件、目录和保留策略。
- 安全新增字段的自动 DDL 支持范围。
