# Git 管理策略

## 仓库边界

整个 `data-synchronization` 目录作为一个 Git 仓库管理。`server/` 和 `web/` 是同一项目的后端、前端源码目录，不再分别维护嵌套 Git 仓库。

## 应提交内容

- `server/` 和 `web/` 的源码、配置模板、数据库脚本、锁文件及项目级编码规范。
- `platform/` 的 Compose、初始化脚本、`.env.example` 和说明文档。
- `test/` 的 POC Compose、镜像构建文件、作业配置、测试 SQL 和执行脚本。
- `docs/` 的 PRD、架构、POC 报告、设计和测试文档。
- 根目录 `.gitignore` 以及后续的项目级 README、CI 和开发脚本。

## 不应提交内容

- `server/**/target/`、`web/node_modules/`、`web/dist/`、Umi 缓存等构建产物。
- `platform/runtime/`、`test/runtime/` 中的 MySQL/Redis/SeaTunnel 数据卷、checkpoint、binlog 和证书。
- `test/results/` 中按执行时间生成的日志、快照和 REST 响应；结果目录只保留 `.gitkeep`，结论写入 `docs/03-poc/`。
- `server/logs/` 和其他运行日志。当前日志中包含 JWT 令牌，不能进入版本库。
- `platform/.env`、`test/.env`、`web/.env.development`、`web/.env.production` 等本地环境文件。它们包含密码、客户端配置或 RSA 私钥，只提交对应的 `.env.example` 模板（前端模板为 `web/.env.example`）。
- 上游脚手架配置中的默认 RSA 私钥和本地密码不能原样进入公共首提交。提交 `server/ruoyi-admin/src/main/resources/application.yml` 前，必须把密钥改为本地环境变量/未跟踪配置，并验证后端仍能启动。
- IDE 本地状态和机器相关配置；上游提供的 `.claude/`、`.codex/` 编码规范保留，`.run/` 和 `.gitee/` 等 IDE/托管平台专用目录排除。

## 提交顺序

当前工作区已经用 `chore(repo): establish project baseline` 建立首个基线提交，包含经过过滤的后端、前端、POC、平台配置和开发文档。后续变更按业务能力拆分提交，例如数据源管理、任务配置、SeaTunnel 适配器和前端页面，避免把生成物与功能代码混在一起。

## 首次提交前检查

```powershell
git status --short --ignored
git add --dry-run .
git diff --cached --check
```

确认没有 `.env`、私钥、日志、`target/`、`node_modules/`、Docker 数据卷或 POC 运行结果后，再创建首个提交。
