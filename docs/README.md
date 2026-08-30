# 数据同步平台文档中心

本目录是数据同步平台的唯一正式文档入口。产品需求、技术架构、POC 记录、详细设计和测试资料按阶段分类维护。

## 目录

| 目录 | 内容 | 状态 |
|---|---|---|
| `01-product/` | 产品需求与范围定义 | PRD v0.3 已完成 |
| `02-architecture/` | 系统架构、组件边界与关键技术决策 | 初版 |
| `03-poc/` | SeaTunnel 技术验证计划、结果与能力矩阵 | 核心 POC 与清洁回归完成，MVP 发布验收中 |
| `04-design/` | 领域模型、数据库、接口和任务状态机设计 | MVP 设计与联调完成 |
| `05-testing/` | 测试策略、验收场景和测试数据说明 | 回归与发布验收清单已形成 |
| `06-release/` | MVP 交付清单、离线包边界和启动顺序 | 收口中 |

## 当前基线

- 源端：MySQL
- MVP 目标端：PostgreSQL
- 同步引擎评估基线：Apache SeaTunnel 2.3.13 / Zeta
- 管理平台：RuoYi-Vue-Plus + plus-ui-react
- 部署形态：单机起步，离线环境可安装

## 阅读顺序

1. [产品需求](01-product/数据同步平台_PRD.md)
2. [系统架构](02-architecture/system-architecture.md)
3. [POC 计划](03-poc/poc-plan.md)
4. [POC 报告](03-poc/poc-report.md)
5. [测试策略](05-testing/test-strategy.md)

POC 的可执行资产位于仓库根目录的 `test/`，运行数据和测试结果也只写入该目录。

平台开发阶段的 MySQL 元数据库和 Redis 位于 `platform/`，与 POC 环境隔离。
