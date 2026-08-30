# 离线交付包模板

此模板提供 PRD v0.3 MVP 的离网部署骨架：MySQL 元数据库、Redis、SeaTunnel、前端静态站点和本机 Java 21 后端。源端 MySQL 与目标 PostgreSQL 是被管理的数据源，不会作为平台容器打入本包。

## 启动

1. 在有网构建机执行 `test/scripts/build-offline-package.ps1`，获得完整目录和 `images/data-sync-mvp-images.tar`。
2. 将整个目录传入离网环境；安装 Docker Desktop/Engine 与 Java 21。
3. 执行 `docker load --input images/data-sync-mvp-images.tar`。
4. 复制 `.env.example` 为 `.env`，填入 MySQL、Redis、凭证主密钥。`.env` 只存放在受保护的部署机。
5. 运行 `docker compose --env-file .env --file compose.yml up --detach`，等待 MySQL、Redis 健康。
6. 运行 `./start-backend.ps1 -Background`。脚本会先执行包内 14 份幂等结构迁移，再启动 Java 21 后端。
7. 访问 `http://localhost:<WEB_PORT>`。

`compose.yml` 设置 `pull_policy: never`，缺少镜像将立即失败，确保运行过程不会访问公网。停止时使用 `docker compose --env-file .env --file compose.yml stop`；只有明确需要清空部署数据时才删除 `runtime/`。
