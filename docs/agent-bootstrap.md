# 智能体启动说明书（在一台新机器上把项目跑起来）

面向对象：在一台**全新机器**上、只拿到本仓库 `git clone` 结果的 AI 智能体。
目标：把平台跑到「浏览器能登录、能建同步任务」的状态。

> 结论先行：装好 §1 的前置条件后，`.\dev.ps1 up` 会自动补齐仓库不携带的东西
> （`web/.env.development`、`web/node_modules`、`-Poc` 需要的 SeaTunnel 连接器 JAR），
> 不再需要手动步骤。**业务数据**（数据源、任务）在 gitignore 的数据库卷里，新机器是空的，
> 需要在 UI 里重建（§5）。

---

## 1. 前置条件（必须先装好）

| 组件 | 版本 | 用途 | 检查命令 |
|---|---|---|---|
| Windows + PowerShell | 5.1（无需 pwsh） | `dev.ps1` 只在 PS 5.1 上验证过 | `$PSVersionTable.PSVersion` |
| Docker Desktop | 任意近期版本 | 元数据库 MySQL / Redis、引擎栈 | `docker version` |
| JDK | **21**（严格） | 后端 | `java -version` |
| Maven | 3.9+（或用 `server/mvnw`） | 后端构建 | `mvn -v` |
| Node.js | ≥ 20.19.0 | 前端 | `node -v` |
| pnpm | 10.x（仓库锁定 `pnpm@10.34.5`） | 前端 | `pnpm -v` |
| 网络 | 能访问 Docker Hub、Maven Central、npm registry | 首次拉依赖 | — |

非 ASCII Windows 用户名的 JDK21/Redisson 套接字问题，启动脚本里已经处理（socket tmpdir 落到 `.dev-runtime\jdk-sockets`），无需额外操作。

---

## 2. 启动

在仓库根目录：

```powershell
.\dev.ps1 up
```

`up` 会依次做：

1. **自动补前端环境**（`Initialize-Frontend`）：
   - `web/.env.development` 不存在 → 从 `web/.env.example` 复制（模板里的值就是本地开发的正确值：端口 8003、API 打到 `18081`、默认 `sys_client` 客户端 ID）。
   - `web/node_modules` 不存在 → 跑 `pnpm install`。
2. `docker compose`（`platform/docker-compose.yml`，项目名 `data-sync-platform`）拉起 `dbs-mysql`(:13306) 和 `dbs-redis`(:16379)，健康门控。
   - 首次启动时 MySQL init 容器用 `utf8mb4` 客户端执行 `server/script/sql/ry_vue.sql`（RuoYi 上游全量 schema + 中文种子）和 `ry_sync.sql`（本项目基线 schema），都随仓库提交。
3. `deploy/migrate-platform-schema.ps1` 幂等地按文件名顺序套用 `server/script/sql/ry_sync_migration_*.sql`（002–017）。
4. 前端：`pnpm dev`（Umi/Vite dev server，端口取自 `.env.development` 的 `VITE_APP_PORT`=8003）。
5. 后端：reactor `install`（跳过 fat jar，约 35s 热态）→ 从 `server/ruoyi-admin/` 跑 `mvn spring-boot:run`，端口 `18081`。

其它常用子命令：

```powershell
.\dev.ps1 up -Fast        # 关 workflow/LiteFlow + 懒加载，启动快 ~15s（workflow 页面会 500）
.\dev.ps1 up -Poc         # 额外拉起本地引擎栈（跑真实同步作业才需要，见 §4）
.\dev.ps1 up -NoBuild     # 跳过 reactor 构建，直接起（代码没动时）
.\dev.ps1 status          # 容器 / 端口 / PID
.\dev.ps1 logs backend    # 或 frontend
.\dev.ps1 down [-All]     # 停后端+前端；-All 连带 docker compose stop
.\dev.ps1 restart-backend # 增量重装 ruoyi-sync + ruoyi-admin 后重启（约 1.5 min）
```

### 验证核心链路

1. 浏览器打开 `http://localhost:8003`
2. 用 `admin` / `admin123` 登录（有验证码）
3. 左侧菜单能看到「数据同步」相关页面；数据源列表是**空的**（正常，见 §5）

后端配置（`server/ruoyi-admin/src/main/resources/application-dev.yml`，随仓库提交，无需改）：
元数据库 `jdbc:mysql://localhost:13306/ry-vue` root/root，Redis `localhost:16379` 密码 `ruoyi123`，均与 `platform/docker-compose.yml` 的默认值一致。

### 镜像版本注意

`platform/docker-compose.yml` 默认 `mysql:8.4.9` / `redis:8.6.3`（compose 里是 `${VAR:-默认}`，所以 `platform/.env` 不提交也能跑）。若 Docker Hub 拉取失败，在 `platform/` 下建 `.env` 回退：

```dotenv
MYSQL_IMAGE=mysql:8.0
REDIS_IMAGE=redis:7-alpine
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=ry-vue
REDIS_PASSWORD=ruoyi123
```

---

## 3. 不用手动补，但要知道原理

| 仓库不携带 | 谁补的 | 从哪来 |
|---|---|---|
| `web/.env.development` | `dev.ps1` 的 `Initialize-Frontend` | `web/.env.example`（模板值已校正） |
| `web/node_modules` | `dev.ps1` 的 `Initialize-Frontend` | `pnpm install` + `pnpm-lock.yaml` |
| SeaTunnel 连接器 / JDBC 驱动 JAR | `dev.ps1 -Poc` 调 `deploy/local-stack/seatunnel/fetch-vendor.ps1` | Maven Central，按 `vendor/checksums.sha1` 校验 |

这三样都在 `.gitignore` 里（`**/.env.development`、`node_modules`、`*.jar`），是**可复现产物**，不进仓库。

---

## 4. 本地引擎栈 `-Poc`（跑真实同步作业才需要）

```powershell
.\dev.ps1 up -Poc
```

- 先跑 `deploy/local-stack/seatunnel/fetch-vendor.ps1`：下载并按 SHA-1 校验 4 个 JAR
  （`connector-cdc-mysql-2.3.13`、`connector-jdbc-2.3.13`、`mysql-connector-j-8.0.33`、`postgresql-42.7.5`）
  到 `deploy/local-stack/seatunnel/vendor/{connectors,drivers}/`。幂等，已存在且校验通过就跳过。
  - 离线 / Maven Central 拉不到时：从 Apache SeaTunnel 2.3.13 官方发行包的 `connectors/` 目录取对应 JAR，手动放进上述目录，以 `vendor/checksums.sha1` 为准校验。
- 然后 `deploy/local-stack/compose.yml`（项目名 `data-sync-poc`）构建 `ds-poc-seatunnel` 镜像并拉起：源 MySQL、PostgreSQL/MySQL/Kafka 目标、SeaTunnel、kafka-ui。

容器名 `ds-poc-*`、compose 项目名 `data-sync-poc`、网络 `data-sync-poc-network` 都是固定的，
`application-dev.yml` 里的 `sync.engine.connection-endpoint-overrides`（把 `localhost:23306` 之类重写成 `ds-poc-mysql:3306`）依赖这些名字，别改。

---

## 5. 业务数据不会跟着仓库走

- 用户建的数据源（含 GoldenDB 等）、同步任务、任务组、配置版本都存在元数据库 `ry-vue` 里，其数据卷 `platform/runtime/`（以及引擎栈的 `deploy/local-stack/runtime/`）都在 `.gitignore` 里。
- 新机器起来后这些表是空的（只有 schema 和 RuoYi 自带的系统种子数据）。需要在 UI 里重新登记数据源、重新建任务。
- 凭证加密（可选）：`SYNC_CREDENTIAL_ENCRYPTION_ENABLED=true` + `SYNC_CREDENTIAL_ENCRYPTION_PASSWORD=<16|24|32 位>`。不设则按明文存，POC 场景可用。详见 `docs/credential-protection.md`。

---

## 6. 从零到能登录 —— 最短路径

```powershell
# 0. 前置：Docker Desktop 已启动，java 21 / mvn / node / pnpm 就绪

# 1. 起核心链路（自动补 .env.development + pnpm install）
.\dev.ps1 up

# 2. 浏览器 http://localhost:8003 , admin / admin123

# 3.（可选，跑同步作业才需要）
.\dev.ps1 up -Poc
```

---

## 7. 排障速查

| 现象 | 原因 / 处理 |
|---|---|
| 前端能开但登录报 client 错误 / 请求 404 | 删掉 `web/.env.development` 重跑 `.\dev.ps1 up` 让它重新生成；或核对值是否被改过 |
| `pnpm install` 失败 | 网络 / registry 问题；`web/` 下手动 `pnpm install` 看详细报错 |
| 后端起不来，报 socket / Redisson 路径错 | 确认用的是 JDK 21；脚本已把 socket 目录挪到 `.dev-runtime\jdk-sockets` |
| `dbs-mysql` 拉不下来 | 网络问题，按 §2 建 `platform/.env` 回退镜像版本 |
| `.\dev.ps1 up -Poc` 报 "vendor JAR provisioning failed" | Maven Central 拉不到；按 §4 的离线兜底手动放 JAR |
| 元数据迁移报错 | 看是哪个 `ry_sync_migration_*.sql`；迁移是幂等的，可重跑 `deploy/migrate-platform-schema.ps1` |
| 数据源列表为空 | 正常，§5，需在 UI 重新登记 |

---

## 8. 仓库里已经齐全的部分

- 后端全部代码 + `application-dev.yml` / `application-dev-fast.yml`（业务模块只有 `server/ruoyi-modules/ruoyi-sync` 是本项目的）
- 前端全部 `web/src`、`web/config`、`web/vite.config.ts`、`web/package.json` + `pnpm-lock.yaml`
- 元数据库 schema：`ry_vue.sql` + `ry_sync.sql` + `ry_sync_migration_002..017.sql`
- 编排脚本：`dev.ps1`、`server/script/bin/*.ps1`、`deploy/migrate-platform-schema.ps1`、`deploy/local-stack/seatunnel/fetch-vendor.ps1`
- 引擎栈 compose、SeaTunnel `Dockerfile` / `entrypoint.sh` / `config/seatunnel.yaml` / `vendor/checksums.sha1`、源/目标库 init SQL
- 离线交付模板：`deploy/offline/`、`deploy/build-offline-package.ps1`
- Redis 配置：`server/script/docker/redis/conf/redis.conf`
