# 平台重启对账运行手册

## 目标

平台进程重启后，元数据库中的 `RUNNING`/`PAUSING` 任务和任务组必须重新向 SeaTunnel 查询，不能仅依赖重启前的本地状态。

## 启动顺序

1. 启动 `dbs-mysql`、`dbs-redis`，确认健康检查通过。
2. 启动 SeaTunnel，确认 `/running-jobs` 可访问。
3. 在开发机使用本机 Java 21 启动后端。单表任务由 `SeaTunnelJobServiceImpl.recoverRunningTasks()` 对账；任务组由 `SyncTaskGroupServiceImpl.recoverRunningGroups()` 对账。
4. 对账结果写回元数据库：引擎仍运行则保持 `RUNNING`，已取消/完成则映射为 `STOPPED`/`FINISHED`，引擎不可达或作业不存在则标记 `FAILED`。

## 本机开发启动

开发期的后端不运行在 Docker 中；Docker 仅承载 MySQL、Redis 和隔离 POC 的 MySQL、PostgreSQL、SeaTunnel。后端使用本机 Java 21，通过 Maven 运行 `ruoyi-admin` 模块并监听 `18081`；前端使用本机 pnpm 开发服务，并将 `/dev-api` 代理到 `18081`。

当前 Windows 用户目录包含非 ASCII 字符，JDK 21 的 Unix-domain socket 在默认临时目录中无法创建，Redisson 会因此启动失败。项目已将兼容性参数固化在 `server/script/bin/start-backend-dev.ps1`，优先使用该脚本：

```powershell
Set-Location C:\projects\data-synchronization
.\server\script\bin\start-backend-dev.ps1
```

脚本会检查 Java 21、Maven、`dbs-mysql`、`dbs-redis` 与端口占用，自动创建 `test/runtime/jdk-sockets`。默认在前台启动；需要在后台运行并将日志写入 `test/runtime/backend` 时，使用：

```powershell
.\server\script\bin\start-backend-dev.ps1 -Background
```

后台模式会等待 `http://localhost:18081/auth/code` 返回 200 后才显示启动成功。首次启动通常约需 40 秒；默认超时为 90 秒，机器负载较高时可调整为 `-StartupTimeoutSeconds 180`。

如 PowerShell 的本机执行策略阻止脚本，可用不改变系统策略的单次调用：

```powershell
powershell -ExecutionPolicy Bypass -File .\server\script\bin\start-backend-dev.ps1
```

后端启动后用 `http://localhost:18081/auth/code` 验证服务、MySQL 和 Redis；响应码为 200 且包含验证码 UUID 即通过。若修改同步模块，先在 `server/` 执行 `mvn -pl ruoyi-modules/ruoyi-sync -am -DskipTests install`，再重启 Maven 开发进程，避免 `ruoyi-admin` 继续使用本地仓库中的旧模块包。

## 验收

- 页面刷新后以服务端状态为准，不使用浏览器本地缓存覆盖状态。
- 任务组表项逐项刷新，并保存最近 checkpoint 摘要和截断后的错误信息。
- 任一表项对账失败只影响该表项，聚合状态显示 `FAILED` 并保留错误原因。
