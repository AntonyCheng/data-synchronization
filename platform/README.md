# 平台开发基础设施

本目录只负责 RuoYi-Vue-Plus 6.0.0 开发阶段的基础服务，不包含 SeaTunnel POC 容器。

## 服务

| 服务 | 容器名 | 主机端口 | 用途 |
|---|---|---:|---|
| MySQL | `dbs-mysql` | `13306` | RuoYi 元数据库 `ry-vue` |
| Redis | `dbs-redis` | `16379` | 登录会话、缓存、分布式能力 |

Compose 默认使用脚手架版本 `mysql:8.4.9` 与 `redis:8.6.3`。当前本机 Docker Hub 拉取遇到网络 EOF，因此 `platform/.env` 临时回退到已有的 `mysql:8.0` 与 `redis:7-alpine`；网络恢复后，将 `.env` 中的两个镜像改回默认版本即可。

## 启动

在项目根目录执行：

```powershell
docker compose --project-name data-sync-platform --file platform/docker-compose.yml up --detach
```

查看健康状态：

```powershell
docker compose --project-name data-sync-platform --file platform/docker-compose.yml ps
```

停止服务但保留数据：

```powershell
docker compose --project-name data-sync-platform --file platform/docker-compose.yml stop
```

MySQL 第一次初始化时会通过 `platform/init/001-ry-vue.sh` 以 `utf8mb4` 客户端字符集执行 `server/script/sql/ry_vue.sql`，避免中文种子数据被按 `latin1` 解析。当前 Compose 使用 `platform/runtime/mysql-v2` 保存数据；已有数据时不会重复执行初始化脚本。

## 连接信息

- MySQL: `jdbc:mysql://localhost:13306/ry-vue`，账号 `root`，默认密码 `root`
- Redis: `localhost:16379`，默认密码 `ruoyi123`

密码可以通过 `platform/.env` 覆盖；不要把真实密码提交到仓库。
