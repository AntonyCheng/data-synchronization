# 开发设计资料

平台开发基线：

- 后端：RuoYi-Vue-Plus 6.0.0，Java 21，Spring Boot 4.1.0。
- 前端：plus-ui-react 6.0.0，Node.js >= 20.19.0，pnpm 10.34.5。
- 元数据库：独立 `dbs-mysql`，主机端口 `13306`，数据库 `ry-vue`。
- Redis：独立 `dbs-redis`，主机端口 `16379`。
- POC 资源仍位于 `test/`，与平台开发基础设施 `platform/` 分离。

## 当前阶段

脚手架数据库、Redis、后端和前端开发服务已完成基础联调。下一步先确认登录/权限链路，再进入数据源管理、任务配置和 SeaTunnel 作业适配器的业务设计与实现。业务模块的设计文档放在本目录，POC 证据继续放在 `docs/03-poc/` 与 `test/results/`。

## 基线验证记录

- `platform` Compose：`dbs-mysql`（`mysql:8.0`，`13306`）和 `dbs-redis`（`redis:7-alpine`，`16379`）均为 healthy。
- MySQL：`ry-vue` 数据库 24 张表，内置用户 3 条；中文种子数据通过 `utf8mb4` 初始化脚本导入。
- Redis：密码认证 `PONG`。
- 后端：JDK 21 下 `mvn -DskipTests compile` 与 `mvn -pl ruoyi-admin -am install -DskipTests` 均成功；应用监听 `8080`，可返回根欢迎页。
- 前端：Node 22.20.0，pnpm 10.34.5，`pnpm install --frozen-lockfile` 完成；Umi 开发服务监听 `8000`，浏览器 HTML 请求可正常返回，`/dev-api/` 代理到后端。

前端脚手架的 `build:dev` 脚本内部会再次调用系统 `pnpm`，若系统全局版本不是 10.34.5，应使用 `corepack pnpm exec ...` 执行对应命令，避免被 pnpm 版本校验拦截。
