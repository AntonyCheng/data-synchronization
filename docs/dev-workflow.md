# 开发模式：Linux 开发机 + Windows 验证机

自 2026-11 起，代码在 **Linux 开发机**上编辑、提交、编译和跑单测；凡是需要"跑起来"的工作——`dev.ps1` 整套栈、
SeaTunnel 引擎栈、e2e 套件、浏览器验收、生产包构建与部署——都在 **Windows 验证机**上进行。Linux 经 SSH 驱动
Windows，仓库根目录的 `win.sh` 封装了全部操作（与 Windows 侧的 `dev.ps1` 对称）。

## 1. 分工

| | Linux 开发机 | Windows 验证机 |
|---|---|---|
| 位置 | `/home/tayyab/projects/data-synchronization`，Claude Code 在这里运行 | `10.9.0.6`（`DESKTOP-1BJC202`，SSH 别名 `lxz-win`），`C:\projects\data-synchronization` |
| 资源 | 2 vCPU / 3.8 GB 内存，资源紧张 | 8 核 / 16 GB；**C 盘仅剩约 16 GB** |
| 负责 | 改代码、git 提交、后端编译 + `@Tag("dev")` 单测、前端 `pnpm lint`（oxlint + `tsc`） | `dev.ps1`（含 `-Poc` 引擎栈）、e2e、浏览器验收、生产包构建与部署 |
| 不做 | 不起后端 / 前端 dev server / 容器，不跑 e2e | 不直接改代码——工作区只是 Linux 的镜像 |
| 工具链 | Ubuntu 24.04、OpenJDK 21（apt `openjdk-21-jdk-headless`）、`server/mvnw`（Maven 3.9.12）、Node 22 + pnpm 10.34.5（corepack）、git、ssh | Windows 11 + PowerShell 5.1、JDK 21（`JAVA_HOME=C:\dependence\jdk-21.0.10`）、Maven 3.8.6、nvm Node 22.20.0 + pnpm 10.34.5、Docker Desktop、Git for Windows |

**唯一的代码源是 Linux 仓库。** Windows 工作区固定在镜像分支 `sync/linux` 上，只通过 `./win.sh sync` 更新；
Windows 的 `main` 保持原样（跟踪 `origin/main`），不再使用。推送 GitHub（`origin`）只在用户同意后从 Linux 进行。

## 2. 连接

- `~/.ssh/config` 里的 `lxz-win`：密钥登录（`~/.ssh/id_ed25519`）、`BatchMode`、`ControlMaster` 复用主连接 10 分钟。
  VPN 往返约 440 ms，新建一次连接约 6 秒，复用后单条命令 1 秒内（PowerShell 启动另需 1～3 秒）。
- VPN 只放通 22 端口：Windows 上的 18081 / 8003 / 18080 / 13306 等开发端口从 Linux 无法直连，需要时用 `./win.sh tunnel`。
- 链路不对称：Windows → Linux 约 100 KB/s；**Linux → Windows 约 8 KB/s**（该方向丢包约 10%）。同步只传 git 增量，
  一次改动通常几 KB、几秒内完成；JAR、镜像、`node_modules` 一律在 Windows 本地构建，不从 Linux 传。
- Windows 上 SSH 的默认 shell 是 `cmd.exe`，输出为 GBK。`win.sh` 一律以 `powershell -EncodedCommand`（UTF-16LE
  base64）执行，避开 cmd 的引号和编码问题；直接 `ssh lxz-win '<cmd>'` 时中文要 `| iconv -f GBK -t UTF-8`。

## 3. `win.sh` 速查

| 命令 | 作用 |
|---|---|
| `./win.sh setup` | 一次性配置：Linux 添加 git 远程 `win`；Windows 设 `receive.denyCurrentBranch=updateInstead` 并切到 `sync/linux`。幂等 |
| `./win.sh sync` | 把 Linux 工作区（含未跟踪、未被忽略的文件）原样同步到 Windows |
| `./win.sh ps '<PowerShell>'` | 在 Windows 仓库根目录执行一段 PowerShell（UTF-8 输出；JDK 21 在 PATH 最前；pnpm 等 Node 工具远程不可用，见 §8） |
| `./win.sh bg <名字> '<PowerShell>'` | 同上，但脱离 SSH 会话运行，输出写入 `.dev-runtime\remote\<名字>-<时间>.log` |
| `./win.sh log <名字> [行数]` | 查看该名字最新一份日志的末尾 |
| `./win.sh wait <名字> [秒]` | 轮询日志直到后台运行结束：成功退出码 0，失败 1（日志末尾是抛出的错误） |
| `./win.sh dev <dev.ps1 参数>` | 执行 `dev.ps1`；`up` 与 `restart-backend` 自动走后台（日志名 `dev-up` / `dev-restart-backend`）；`up -Poc` 用已构建的镜像起引擎栈，不做 docker 构建（见 §8）；前端 dev server 远程起不来（见 §6） |
| `./win.sh devlog backend\|frontend [行数]` | 查看后端 / 前端日志末尾（`dev.ps1 logs` 会一直跟随，远程不可用） |
| `./win.sh e2e [mvn 参数]` | 在 Windows 上跑 e2e 套件（后台，日志名 `e2e`），如 `./win.sh e2e -Dtest=KafkaTaskE2eTest` |
| `./win.sh tunnel [stop]` | 把 Windows 的 18081 / 8003 / 18080 转发到本机 `127.0.0.1:28081` / `18003` / `28080` |

环境变量 `WIN_HOST`、`WIN_REPO`、`TUNNEL_PORTS`、`TUNNEL_OFFSET` 可覆盖默认值。隧道的本地端口故意比 Windows 端口
大 10000：用户的 VS Code 经 Remote-SSH 连着这台 Linux，会把这里新出现的监听端口以**同一端口号**自动转发到 Windows
桌面——本地若也用 8003，VS Code 就会占住 Windows 的 8003，前端 dev server 再也起不来（2026-11-06 实际发生过）。`ps` 的脚本受 cmd 命令行 8191 字符限制，约 2.5 KB 以内。

## 4. 代码同步

`./win.sh sync` 在临时索引里把当前工作区做成一个提交（Linux 的索引、HEAD 和分支都不动；工作区干净时直接用 HEAD），
再 `git push --force win <提交>:refs/heads/sync/linux`。Windows 仓库配置了 `receive.denyCurrentBranch=updateInstead`，
推送会就地更新它的工作区；**Windows 上有本地改动时推送被拒绝，不会覆盖任何东西**。

- 被 `.gitignore` 忽略的文件从不同步：`node_modules`、`target`、`.env*`、SeaTunnel 连接器 JAR、`.dev-runtime/` 都留在 Windows 本地。
- git 走 SSH 时远端执行 `git-upload-pack '<路径>'`，cmd 会把单引号当作路径的一部分，所以远程 `win` 配置成经
  PowerShell 执行（`remote.win.uploadpack` / `receivepack`），二进制 pack 原样通过（用 200 KB 随机数据验证过）。
- 同步之后：前端由用户在 Windows 终端里启动的 Vite dev server 自动热更新；后端改动要 `./win.sh dev restart-backend`（只重装 `ruoyi-sync` +
  `ruoyi-admin`）；新增 `ry_sync_migration_*.sql` 要 `./win.sh dev up -Poc`（`up` 会套用迁移，已在运行的后端会被跳过，
  需要再 `restart-backend`）。
- 极少数情况下要把 Windows 上的提交拿回来：`git fetch win`。

## 5. 编译与单测（Linux）

```bash
cd server
export MAVEN_OPTS="-Xmx768m -XX:+UseSerialGC"
sh mvnw -B -ntp -o -Pdev -pl ruoyi-modules/ruoyi-sync -am -Dmaven.test.skip=false -Dgroups=dev \
    -Dsurefire.failIfNoSpecifiedTests=false test        # 单个类：追加 -Dtest=EngineJobRunnerTest
sh mvnw -B -ntp -o -pl ruoyi-modules/ruoyi-sync -am compile     # 只编译
```

- `mvnw` 在 git 里没有可执行位，用 `sh mvnw`；它从华为云镜像取 Maven 3.9.12，依赖也走根 pom 配置的华为云仓库。
  `-o` 离线；新增依赖后去掉 `-o` 跑一次。
- 基线（2026-11-06，`eb26645`）：261 个单测全部通过。首次运行（下载依赖）约 5 分钟，之后离线约 35 秒，峰值内存约
  300～500 MB；`~/.m2` 约 150 MB。
- 不在 Linux 上打 fat jar、不跑 `spring-boot:run`、不跑 e2e。

前端 lint 与类型检查也在 Linux 上做（Windows 的远程会话跑不了 Node 工具，见 §8）：

```bash
cd web
pnpm install --frozen-lockfile   # 首次或依赖变化后：约 1 分钟，node_modules 约 1.3 GB
pnpm lint                        # oxlint + max setup + tsc --noEmit：约 30 秒，峰值内存约 650 MB
```

corepack 只在 `web/` 里按 `packageManager` 选用 pnpm 10.34.5，在仓库根执行 `pnpm` 会去拉别的版本。`pnpm dev` 不在 Linux 上跑。

## 6. 运行与验证（Windows）

```bash
./win.sh dev up -Poc && ./win.sh wait dev-up 1500   # 基础容器 + 引擎栈 + 迁移 + 后端（前端见下）
./win.sh dev status                                 # 容器 / 端口 / PID
./win.sh sync && ./win.sh dev restart-backend && ./win.sh wait dev-restart-backend 600
./win.sh devlog backend 200
./win.sh e2e && ./win.sh wait e2e 1200              # 报告在 Windows 的 server/ruoyi-modules/ruoyi-sync/target/surefire-reports/
./win.sh tunnel && curl -s http://127.0.0.1:28081/  # 从 Linux 访问后端接口（引擎 REST 是 28080）；用完 ./win.sh tunnel stop
./win.sh dev down                                   # 停后端 + 前端；down -All 连容器一起停（数据保留）
```

- **前端 dev server 不能从远程启动**（§8 高完整性）：`./win.sh dev up` 能起容器、迁移和后端，前端会报
  `frontend not ready`。需要浏览器验收时，请用户在 Windows 的终端里执行 `.\dev.ps1 up`（已在运行的容器和后端会被
  跳过，只补起前端），再打开 `http://localhost:8003`（`admin` / `admin123`，有验证码）。Vite 会热更新之后同步过去的前端改动。
- 元数据库（`dbs-mysql`）里已登记 e2e 依赖的数据源（`POC MySQL`、`POC PostgreSQL`、`POC MySQL Target`、
  `POC Kafka 3.8`），凭证用 Windows 用户级环境变量 `SYNC_CREDENTIAL_ENCRYPTION_PASSWORD` 加密——`dev.ps1` 会从
  用户环境读取。不要打印或复制该密码，也不要在别处起后端连这个库。
- 后台运行（`win.sh bg`、`dev up`、`restart-backend`、`e2e`）的脚本与日志都在 Windows 的 `.dev-runtime\remote\`。

## 7. 一次改动的完整流程

1. Linux：在本地分支上改代码，跑相关单测（必要时全量 `dev` 回归）；动了前端就在 `web/` 里 `pnpm lint`。
2. `./win.sh sync`。
3. Windows：后端改动 `restart-backend`；有新迁移先 `dev up -Poc`；前端由用户已启动的 dev server 自动热更新。
4. 验证：e2e、接口（`tunnel` + `curl`）、需要时请用户做浏览器验收。
5. 结果记入 `BACKLOG.md`；在 Linux 上提交。合入 `main`、推送 GitHub 需用户同意。

## 8. 远程运行的坑（`win.sh` 已处理，手写命令时要注意）

- **会话结束会杀进程**：Win32-OpenSSH 在 SSH 会话结束时结束会话内启动的所有进程，`Start-Process` 起的也不例外——
  在 SSH 里直接跑 `dev.ps1 up`，命令一返回后端和前端就没了。要活得比命令久的进程经 WMI（`Win32_Process.Create`）
  创建，父进程是 WmiPrvSE 而不是会话（`win.sh bg`）。
- **PS 5.1 的 stderr 陷阱**：对原生命令做 `2>&1`、`*>&1` 或管道时，stderr 的每一行都变成 ErrorRecord，在
  `$ErrorActionPreference = 'Stop'`（`dev.ps1` 就是）下直接终止脚本——`docker compose` 的进度输出就在 stderr。
  后台运行的输出因此由 `cmd.exe` 在进程级重定向，而不是用 PowerShell 的流重定向。
- **开关参数不能加引号**：`& .\dev.ps1 up '-Poc'` 会把 `'-Poc'` 当字符串绑定到位置参数。反过来，传给 `mvn` 的
  `-Dkey.with.dots=value` 必须加引号，否则 PS 5.1 会在第一个点处把它拆开。
- **远程会话是高完整性，跑不了 Node 工具**：管理员账号经 SSH 登录得到的是提升后的令牌（`High Mandatory Level`），WMI
  创建的进程沿用同一令牌。Windows 11 的重定向防护不让高完整性进程穿过普通权限进程创建的目录联接 / 符号链接
  （`The path cannot be traversed because it contains an untrusted mount point`）——pnpm 的 `node_modules` 全由
  联接组成，nvm 的 `C:\nvm\nvm4w\nodejs` 也是符号链接。所以远程会话里 `pnpm dev`（前端 dev server）、`pnpm lint`、
  `max` 都会失败；`win.sh` 只能把 nvm 链接的目标目录（当前 `C:\nvm\v22.20.0`）放到 PATH 最前，让 `node` / `pnpm`
  本身可用。`runas /trustlevel` 降不下完整性级别；在交互会话里以普通权限运行（例如计划任务）需要用户同意，未采用。
  Java / Maven / Docker / git 不受影响。
- **Java 版本**：PATH 上的 `java` 是 Oracle 的 25；`dev.ps1` 通过 `JAVA_HOME` 选 21，`win.sh` 也把 `%JAVA_HOME%\bin` 放在前面。
- **CLIXML 噪声**：非交互 PowerShell 会把进度记录以 `#< CLIXML` 写到 stderr，需设 `$ProgressPreference='SilentlyContinue'`；
  `Write-Host`（information 流，`dev.ps1` 的输出全靠它）在 `-EncodedCommand` 会话里也会被序列化成 CLIXML，`win.sh ps`
  因此用 `6>&1` 把它并入标准输出（只动 6 号流，不碰原生命令的 stderr）。
- **远程无法访问 Docker 凭据**：SSH 密钥登录和 WMI 创建的进程都没有交互登录会话，Docker 的凭据助手打不开 Windows
  凭据管理器，凡要访问镜像仓库的操作都会失败（`error getting credentials ... A specified logon session does not exist`）——
  `docker pull`，以及基础镜像不在本地的 `docker build`。`dev.ps1 up -Poc` 固定执行 `docker compose up --build`，而
  `apache/seatunnel:2.3.13` 的本地标签已不在，所以远程必然失败；`./win.sh dev up -Poc` 改为直接用已构建的
  `data-sync-poc/seatunnel:2.3.13`（2026-09-08 构建，此后 `deploy/local-stack/seatunnel/` 与 `compose.yml` 未变）起引擎栈，
  再执行 `dev.ps1 up`，并把 `dev-state.json` 标为 `poc`，让 `down -All` 照常停掉引擎栈。**改了
  `deploy/local-stack/seatunnel/` 或 `compose.yml` 需要重建镜像时，请用户在 Windows 的交互终端里执行 `.\dev.ps1 up -Poc`。**

## 9. 共用主机的约束

- 这台 Windows 还承载用户其他项目的容器、镜像和卷。只操作本项目的
  `dbs-*`、`ds-poc-*`（生产包验收另有测试实例 `data-sync-prodtest-*`）；**不要 `docker system prune`，不删别人的镜像或卷**。
- 磁盘紧张：C 盘约 16 GB 空闲，Docker 数据盘（`docker_data.vhdx`）约 74 GB。构建镜像（SeaTunnel 镜像 7.4 GB）或
  生产包前先 `./win.sh ps 'docker system df'`；需要清理时先问用户。
- Windows 上可能有用户自己的会话和工具在运行，不要结束不属于本项目的进程。

## 10. 排障

| 现象 | 原因 / 处理 |
|---|---|
| `sync` 推送被拒（`Working directory has unstaged changes` 之类） | Windows 工作区有本地改动。`./win.sh ps 'git status --short'` 查看；是用户的改动先拿回 Linux，确认后再清理 |
| `sync` 报 Windows 不在 `sync/linux` | 有人切换了分支；`./win.sh setup` 切回 |
| 命令返回后后端 / 前端就没了 | 用了会话内启动；改用 `./win.sh dev up` / `restart-backend` 或 `./win.sh bg` |
| `wait` 显示 `[win.sh] failed: ...` | 日志末尾就是抛出的错误；后端 / 前端自身的输出看 `./win.sh devlog` |
| `dev up` 卡在某一步 | `./win.sh log dev-up 80` 看进度；`./win.sh ps 'docker ps -a --filter name=ds-poc-'` 看容器 |
| `dev up` 报 `frontend not ready` | 预期行为：前端 dev server 远程起不来，请用户在 Windows 终端里 `.\dev.ps1 up`（§6） |
| Windows 的 8003 被 `Code.exe` 占用 | VS Code 的 Remote-SSH 自动端口转发（见 §3）；请用户在 VS Code 的「端口」面板里停止转发 8003。不要结束 `Code.exe` |
| `untrusted mount point` / 远程找不到 `node_modules` 里的模块 | 远程会话是高完整性，穿不过 pnpm 的目录联接（§8）；Node 工具在 Linux 或 Windows 交互终端里跑 |
| SSH 很慢或卡住 | 主连接失效：`ssh -O exit lxz-win` 后重试 |
