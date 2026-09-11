# 离线交付包模板

此模板提供 PRD v0.4 MVP 的离网部署骨架：MySQL 元数据库、Redis、SeaTunnel、Kafka、前端静态站点和后端（容器化）。源端 MySQL 与目标 PostgreSQL/MySQL/Kafka 是被管理的数据源，不会作为业务数据源容器打入本包。部署机只需要 Docker + Docker Compose，不需要安装 Java。

## 启动

1. 在有网构建机（Windows/Linux 均可）执行 `deploy/build-offline-package.ps1`，会构建后端 Docker 镜像并生成完整目录和 `images/data-sync-mvp-images.tar`。
2. 将整个目录传入离网 Linux 部署机；安装 Docker Engine 与 Docker Compose 插件。
3. 执行 `docker load --input images/data-sync-mvp-images.tar`。
4. 复制 `.env.example` 为 `.env`，填入 MySQL、Redis、凭证主密钥。`.env` 只存放在受保护的部署机。
5. 运行 `docker compose --env-file .env --file compose.yml up --detach`。Compose 会按依赖顺序：等待 MySQL/Redis 健康 → 执行 `migrate` 一次性容器跑完 15 份幂等结构迁移（含 Kafka 指标字段）→ 启动后端并等其健康 → 启动前端。
6. 访问 `http://<部署机地址>:<WEB_PORT>`。

`compose.yml` 设置 `pull_policy: never`，缺少镜像将立即失败，确保运行过程不会访问公网。停止时使用 `docker compose --env-file .env --file compose.yml stop`；只有明确需要清空部署数据时才删除 `runtime/`。

后端日志落在 `runtime/backend/`（容器内 `/ruoyi/server/logs` 挂载）；查看启动状态用 `docker compose --env-file .env --file compose.yml logs -f backend`。
