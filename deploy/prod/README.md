# 生产部署包（deploy/prod）

这里是生产部署包的源文件：单机 Docker Compose 部署，目标机为 Linux x86_64，部署时不访问任何镜像仓库。
打包脚本是 `deploy/build-prod-package.ps1`，在 Windows 构建机上执行，产物在 `deploy/release/data-sync-<版本>/`
（及同名 .tar，均不入库）。手册的源文件是 `docs/*.md`，打包时由 `docs/build-docx.cjs` 生成 Word 版。

| 路径 | 进入安装包后的位置 | 说明 |
|---|---|---|
| compose.yml、compose.https.yml | 同名 | 6 个服务：web、backend、seatunnel、mysql（profile bundled-db）、redis、migrate |
| env.example | .env.example | 全部可配置项；install.sh 校验必填项与取值 |
| caddy/ | caddy/ | Web 入口：HTTP 与 HTTPS 两份 Caddyfile |
| database/migrate.sh | database/migrate.sh | 空库导入 ry_vue.sql + ry_sync.sql，再执行全部 ry_sync_migration_*.sql（幂等），使用平台账号而非 root |
| seatunnel/seatunnel.yaml | seatunnel/seatunnel.yaml | 引擎配置（单节点，checkpoint 落在数据目录） |
| scripts/*.sh | scripts/ | install / verify / dc / backup / restore / upgrade / uninstall，公共函数在 lib.sh |
| images/*/ | 镜像 | backend、seatunnel、web 三个 Dockerfile（见下） |
| web.env.production | 不进安装包 | 前端生产构建的 .env.production（接口前缀 /prod-api） |
| README.txt | README.txt | 安装包内的快速开始 |
| docs/ | docs/*.docx | 部署手册、操作手册；截图在 docs/images/ |

镜像都由构建机上已有的镜像生成，构建时不拉取：

- `data-sync/backend`：取 `data-sync-mvp/backend:latest`（server/ruoyi-admin/Dockerfile 在 Liberica OpenJDK 21 上构建）的运行时，去掉旧 jar 压成单层，放入 `-Pprod` 打包的 jar 与生成 admin 口令哈希用的 Hutool + BCryptHash.java。
- `data-sync/seatunnel`：取 `data-sync-poc/seatunnel:2.3.13-before-kafka-patch`（deploy/local-stack/seatunnel/Dockerfile 的构建结果），按 `images/seatunnel/prune.sh` 只保留平台作业用到的连接器（MySQL-CDC、Jdbc、Kafka 及排障用的 console/fake/assert）后压成单层，再覆盖两个补丁 jar（vendor/patched）。打包脚本核对镜像中补丁 jar 的 SHA-1 与 vendor/patched 一致。
- `data-sync/web`：caddy:2.8.4-alpine + 前端构建产物。

构建（Windows，远程会话时前端用独立安装依赖的副本，见 docs/dev-workflow.md）：

```powershell
.\deploy\build-prod-package.ps1 -Version 1.0.0 -WebDir .dev-runtime\web-remote
```
