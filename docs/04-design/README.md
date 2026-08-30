# 开发设计资料

平台开发基线：

- 后端：RuoYi-Vue-Plus 6.0.0，Java 21，Spring Boot 4.1.0。
- 前端：plus-ui-react 6.0.0，Node.js >= 20.19.0，pnpm 10.34.5。
- 元数据库：独立 `dbs-mysql`，主机端口 `13306`，数据库 `ry-vue`。
- Redis：独立 `dbs-redis`，主机端口 `16379`。
- POC 资源仍位于 `test/`，与平台开发基础设施 `platform/` 分离。

## 当前阶段

脚手架数据库、Redis、后端和前端开发服务已完成基础联调。当前已交付数据源管理和单表同步任务 MVP，包括 CRUD、权限菜单、JDBC 连接测试、任务连接校验、SeaTunnel 配置生成与脱敏预览、作业提交、CDC checkpoint/savepoint、启动/暂停/恢复/停止、状态刷新、引擎故障落库和源/目标行数核对。前端端到端全量+CDC、生命周期和引擎故障恢复验收已通过。本阶段已补齐 5,000 行性能回归基线、数据源凭证 AES 保护设计、凭证迁移接口，以及多表任务组元数据、校验、配置预览和逐表 SeaTunnel 作业生命周期基础。业务模块的设计文档放在本目录，POC 证据继续放在 `docs/03-poc/` 与 `test/results/`。

## 设计索引

- [元数据模型](metadata-schema.md)
- [任务状态机](task-state-machine.md)
- [MVP API 契约](api-contract.md)
- [SeaTunnel 作业生命周期实施计划](job-lifecycle-plan.md)
- [多表同步设计](multi-table-plan.md)
- [运行时表结构变更管理](ddl-change-management.md)
- [凭证保护与迁移](credential-protection.md)
- [平台元数据库迁移手册](platform-migration-runbook.md)
- [运行监控与数据核对](monitoring-and-consistency.md)

## 基线验证记录

- `platform` Compose：`dbs-mysql`（`mysql:8.0`，`13306`）和 `dbs-redis`（`redis:7-alpine`，`16379`）均为 healthy。
- MySQL：`ry-vue` 数据库 24 张表，内置用户 3 条；中文种子数据通过 `utf8mb4` 初始化脚本导入。
- Redis：密码认证 `PONG`。
- 后端：JDK 21 下 `mvn -DskipTests compile` 与 `mvn -pl ruoyi-admin -am install -DskipTests` 均成功；应用监听 `8080`，可返回根欢迎页。
- 前端：Node 22.20.0，pnpm 10.34.5，`pnpm install --frozen-lockfile` 完成；Umi 开发服务监听 `8000`，浏览器 HTML 请求可正常返回，`/dev-api/` 代理到后端。指标详情在手动刷新状态后展示，缺失的引擎指标以空值和说明呈现。

前端脚手架的 `build:dev` 脚本内部会再次调用系统 `pnpm`，若系统全局版本不是 10.34.5，应使用 `corepack pnpm exec ...` 执行对应命令，避免被 pnpm 版本校验拦截。
