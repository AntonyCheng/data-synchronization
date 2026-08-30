# MySQL 到 Oracle POC

这是 PRD Phase 1.1 的独立技术验证环境，不会修改 `ds-poc-mysql`、`ds-poc-postgres`、`ds-poc-seatunnel` 或平台容器。它复用已运行的 `ds-poc-mysql` 作为源端，并创建 `ds-poc-oracle`、`ds-poc-oracle-seatunnel`。

## 准备

1. 确保基础 POC 已启动，`ds-poc-mysql` 处于运行状态。
2. 拉取 `gvenzl/oracle-free:23-slim-faststart`，该镜像受 Oracle Free Use Terms and Conditions 约束，应由部署方确认许可后获取。
3. 从受控制品库取得 Java 21 兼容的 `ojdbc11.jar`，放入 `vendor/ojdbc11.jar`；在同目录创建 `ojdbc11.jar.sha256`，填入其 SHA-256 值。驱动不进入仓库，也不会在构建时联网下载。
4. 将 `.env.example` 复制为 `.env`，只设置本地 POC 的 `ORACLE_PASSWORD`。首轮 HOCON 任务固定使用 `poc` / `poc_password` 作为应用账号。

## 运行

```powershell
.\test\oracle\scripts\up.ps1
.\test\oracle\scripts\run-full.ps1
```

首轮仅验证全量 `MERGE`。成功后才能继续实现 CDC、平台 `ORACLE` 数据源类型、Oracle 目标兼容性检查和配置生成器。
