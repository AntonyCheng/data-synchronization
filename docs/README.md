# 数据同步平台文档

产品已按 PRD 完成 MVP（MySQL 源端；PostgreSQL / MySQL / Kafka 目标端）。本目录只保留架构和已发布特性的设计参考，过程性文档（PRD、POC 计划/报告、测试策略、发布清单）已从仓库移除。

## 当前基线

- 源端：MySQL（binlog CDC）
- 目标端：PostgreSQL、MySQL、Kafka
- 同步引擎：Apache SeaTunnel 2.3.13 / Zeta
- 管理平台：RuoYi-Vue-Plus + plus-ui-react

## 架构与契约

| 文档 | 内容 |
|---|---|
| [architecture.md](architecture.md) | 系统架构、组件边界、核心数据对象、关键约束 |
| [api-contract.md](api-contract.md) | 接口与请求/响应的权威参考 |
| [engine-adapter-contract.md](engine-adapter-contract.md) | 平台与 SeaTunnel REST 的适配契约 |
| [metadata-schema.md](metadata-schema.md) | 元数据库表结构 |
| [task-state-machine.md](task-state-machine.md) | 任务状态机 |

## 设计文档

| 文档 | 内容 |
|---|---|
| [multi-table.md](multi-table.md) | 多表 / 整库同步设计 |
| [job-lifecycle.md](job-lifecycle.md) | SeaTunnel 作业生命周期 |
| [scheduling-and-overwrite.md](scheduling-and-overwrite.md) | 调度与覆盖策略 |
| [ddl-change-management.md](ddl-change-management.md) | 运行时表结构变更管理 |
| [monitoring-and-consistency.md](monitoring-and-consistency.md) | 运行监控与数据核对 |
| [credential-protection.md](credential-protection.md) | 凭证 AES 保护与迁移 |
| [task-creation-wizard.md](task-creation-wizard.md) | 任务创建向导页面规则 |
| [kafka-event-formats.md](kafka-event-formats.md) | Kafka 目标消息格式（默认 JSON 与 Canal/Debezium/Maxwell/OGG 兼容格式） |
| [type-mapping-mysql-postgresql.md](type-mapping-mysql-postgresql.md) | MySQL -> PostgreSQL 类型映射 |

## 运维

| 文档 | 内容 |
|---|---|
| [runbook-restart.md](runbook-restart.md) | 平台重启手册 |
| [runbook-metadata-migration.md](runbook-metadata-migration.md) | 元数据库迁移手册 |
| [git-strategy.md](git-strategy.md) | Git 管理策略 |

## 启动

| 文档 | 内容 |
|---|---|
| [agent-bootstrap.md](agent-bootstrap.md) | 在一台新机器上把项目跑起来（面向智能体，含仓库缺口清单） |

## 相关资源

- 本地引擎栈（SeaTunnel 镜像构建、Compose）、离线交付模板：仓库根目录 `deploy/`。
- 平台开发阶段的元数据库 MySQL 和 Redis：`platform/`。
- 启动方式见仓库根 `CLAUDE.md`（`dev.ps1`）。
