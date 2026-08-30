# MVP 交付

本目录描述 PRD v0.3 MVP 的交付边界。当前 MVP 只覆盖 MySQL 源端到 PostgreSQL 目标端，以及单表、多表、整库、全量、binlog CDC、全量+CDC、恢复、监控和数据核对。

## 交付前检查

在仓库根目录执行：

```powershell
.\test\scripts\verify-offline-package.ps1
```

脚本检查前端 `dist`、后端 JAR、平台 Compose、数据库迁移和文档是否齐全，并生成 `test/release/mvp/manifest.json`。它不会复制运行时数据库、checkpoint、测试结果、Node 依赖或凭证。

## 实际离线包

在可联网的构建机执行：

```powershell
.\test\scripts\build-offline-package.ps1
```

该命令装配前端、后端、SQL、SeaTunnel 配置以及已缓存的 MySQL、Redis、SeaTunnel、Caddy 镜像到 `test/release/mvp/`。离线环境的完整导入和启动步骤见 [offline/README.md](offline/README.md)。

## 启动顺序

1. 使用平台 Compose 启动 `dbs-mysql` 和 `dbs-redis`。
2. 使用 `test/scripts/migrate-platform-schema.ps1` 升级已有 `ry-vue` 元数据库。
3. 使用 `server/script/bin/start-backend-dev.ps1` 或交付环境的 Java 21 启动后端。
4. 启动前端静态服务，确认 `/dev-api` 或生产 API 地址指向后端。
5. 按 `docs/05-testing/release-checklist.md` 完成登录态验收。

## 不得打包

`platform/runtime`、`test/runtime`、`test/results`、`node_modules`、本地 `.env`、访问令牌、AES 主密钥和任何生产凭证不得进入交付包。生产环境必须通过受保护的部署变量注入密钥和数据库密码。
