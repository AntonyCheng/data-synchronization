# 数据同步平台 部署手册

本手册说明如何用安装包在一台 Linux 服务器上以 Docker 方式部署数据同步平台，以及日常运维、备份恢复、升级与故障处理。平台的使用方法见《操作手册》。

> 提示：本手册中的命令都在安装目录（解压后的 data-sync-<版本> 目录）中执行，需要 root 权限（用 sudo 执行；数据目录里的文件属于容器内的用户，普通用户无法备份、恢复或删除）。

## 1 概述

### 1.1 系统组成

平台由 6 个容器组成，全部打包在安装包的镜像文件中，部署时不需要访问任何镜像仓库。

| 容器（服务名） | 镜像 | 作用 |
|---|---|---|
| web | data-sync/web | Web 入口（Caddy）：提供前端页面，把 /prod-api/ 请求转发给后端；唯一对外开放端口的容器 |
| backend | data-sync/backend | 平台后端（Java 21）：配置管理、前置检查、任务调度、状态对账、Kafka 发布桥接、告警 |
| seatunnel | data-sync/seatunnel | 同步引擎 Apache SeaTunnel 2.3.13（Zeta），带 GoldenDB 兼容与 Kafka 投递两个补丁 |
| mysql | mysql:8.0 | 元数据库：数据源、任务配置、运行状态（也可改用已有 MySQL，见 3.5） |
| redis | redis:7-alpine | 缓存、分布式锁、登录验证码 |
| migrate | mysql:8.0 | 一次性容器：初始化元数据库并执行结构迁移，完成后退出 |

### 1.2 数据流

以 GoldenDB → Kafka 的 CDC 任务为例：

1. 引擎以 binlog 复制方式连接源库（GoldenDB DN 节点或 MySQL），先读出全量数据，再持续读取增量变更；
2. 引擎把每条变更写入目标 Kafka 集群上的私有 raw topic（名称 `__ds_raw_<任务ID>_v<配置版本>`，由平台自动创建与清理）；
3. 后端的发布桥接按顺序读取 raw topic，转换成配置的输出格式后发布到目标 topic，消息 Key 为同步键；
4. 任一条变更发不出去时桥接停在原地并告警，不会跳过；问题排除后自动续传。

因此，部署平台的服务器需要能访问源库与 Kafka 集群，而源库与 Kafka 不需要访问平台。

### 1.3 安装包内容

| 路径 | 内容 |
|---|---|
| README.txt | 快速开始 |
| VERSION、manifest.json、SHA256SUMS | 版本号；构建提交号、镜像与文件清单；文件校验和 |
| compose.yml、compose.https.yml | 容器编排；HTTPS 叠加配置 |
| .env.example | 配置模板（复制为 .env 后填写） |
| images/ | 全部镜像（docker load 格式，约 1 GB） |
| scripts/ | install.sh、verify.sh、dc.sh、backup.sh、restore.sh、upgrade.sh、uninstall.sh |
| database/ | 元数据库初始数据、结构迁移脚本、migrate.sh |
| caddy/、seatunnel/、certs/ | Web 入口配置、引擎配置、HTTPS 证书存放位置 |
| docs/ | 本手册与《操作手册》 |

## 2 部署前准备

### 2.1 服务器要求

| 项目 | 最低 | 推荐 | 说明 |
|---|---|---|---|
| CPU | 4 核 | 8 核及以上 | x86_64（amd64）架构 |
| 内存 | 16 GB | 32 GB | 引擎堆默认 4 GB、后端 2 GB；每多一张同时运行的 CDC 表约增加 200~300 MB 引擎内存 |
| 磁盘 | 200 GB | 500 GB SSD | 数据目录存放元数据库、引擎 checkpoint、日志与备份 |
| 操作系统 | 内核 3.10+ 的 64 位 Linux | CentOS 7.9 / Rocky 8 / 麒麟 V10 / Ubuntu 22.04 等 | 需要 bash、tar、gzip、curl |
| Docker | Docker Engine 24+ | 最新稳定版 | 需要 Compose 插件 v2.20 及以上（docker compose version 查看） |
| 时间同步 | 已配置 NTP | | 源库、Kafka 与平台服务器都要校时；时钟偏差会直接计入同步延迟 |

检查命令：

```bash
uname -m                      # 应为 x86_64
docker version                # Server 版本 24 以上
docker compose version        # v2.20 以上
timedatectl                   # System clock synchronized: yes
```

### 2.2 网络与端口

| 方向 | 端口 | 说明 |
|---|---|---|
| 用户浏览器 → 平台服务器 | 80（HTTP）或 443（HTTPS），可在 .env 修改 | 唯一需要对外开放的端口 |
| 平台服务器 → 源库 | 源库端口（如 GoldenDB DN 3306/8880） | 元数据读取、全量读取与 binlog 复制 |
| 平台服务器 → Kafka | 每个 broker 的监听端口 | 必须能解析并访问 broker 的 advertised.listeners 地址（不只是引导地址） |
| 平台服务器 → 目标库 | 目标库端口 | 仅同步到 MySQL / PostgreSQL 时需要 |

MySQL、Redis、引擎与后端都不对外发布端口，只在本机的容器网络内通信。容器通过宿主机的路由访问外部地址。源库或 Kafka 用主机名配置时，容器要能解析这些主机名：宿主机使用 DNS 时容器自动沿用；只写在宿主机 /etc/hosts 中的主机名，需要在安装目录新建 compose.override.yml 为引擎和后端加上 extra_hosts：

```yaml
services:
  backend:
    extra_hosts:
      - "kafka-01.example.com:10.1.2.11"
      - "goldendb-dn1.example.com:10.1.3.21"
  seatunnel:
    extra_hosts:
      - "kafka-01.example.com:10.1.2.11"
      - "goldendb-dn1.example.com:10.1.3.21"
```

脚本会自动叠加 compose.override.yml，升级时也会随 .env 一起沿用。也可以在其中把引擎和后端接入宿主机上已有的 Docker 网络；平台内部服务之间用 datasync-mysql、datasync-redis、datasync-engine、datasync-backend 这几个别名互相访问，所以那个网络里有同名的 mysql、redis 等服务也不会连错，但不要在别处使用这几个别名。

### 2.3 源库要求（MySQL / GoldenDB）

CDC（全量 + CDC、纯增量）任务依赖源库的 binlog。平台在创建任务时自动执行「CDC 前置检查」，以下各项必须满足：

| 检查项 | 要求 | 查看方式 |
|---|---|---|
| binlog 已开启 | log_bin = ON | SHOW VARIABLES LIKE 'log_bin' |
| binlog 格式 | binlog_format = ROW | SHOW VARIABLES LIKE 'binlog_format' |
| binlog 行镜像 | binlog_row_image = FULL | SHOW VARIABLES LIKE 'binlog_row_image' |
| server-id | 大于 0 | SELECT @@server_id |
| 复制权限 | 同步账号具有 REPLICATION SLAVE、REPLICATION CLIENT | SHOW GRANTS |
| binlog 保留时间 | 建议 7 天以上，至少要长于可能的最长停机时间 | binlog_expire_logs_seconds |
| 时区 | 数据源上配置的时区与源库实际时区一致（不一致时只给警告） | 前置检查自动比对 |

同步账号示例（按实际库名调整）：

```sql
CREATE USER 'sync_reader'@'%' IDENTIFIED BY '********';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sync_reader'@'%';
```

GoldenDB 说明：

- 数据源连接 GoldenDB 的 DN（存储）节点或其对外代理地址，类型选 MySQL。GoldenDB 的 binlog 中 Table_map 事件格式与标准 MySQL 不同，安装包中的引擎已带兼容补丁。
- 每张 CDC 表各占用源库一条 binlog 复制连接，同时运行的表多时要评估源库的连接数与网络带宽。
- 务必为 GoldenDB 节点配置 NTP：曾实测到测试节点时钟慢约 16 分钟，这部分偏差会全部计入同步延迟指标。

### 2.4 Kafka 要求

| 项目 | 要求 |
|---|---|
| 版本 | Kafka 2.8 及以上（已验证 3.8） |
| 连接方式 | 当前版本只支持无认证的 PLAINTEXT 连接，数据源中填写一个 broker 的「主机:端口」作为引导地址（客户端会从它获取全部 broker）；引导 broker 不可用时无法连接，可填写指向多个 broker 的域名或负载均衡地址 |
| 自动建 topic | 必须设置 auto.create.topics.enable=false。开启时，刚删除的 topic 可能被平台的发布客户端在刷新元数据时按默认配置重新建出 |
| raw topic | 平台为每个 Kafka 任务（任务组的每张表）自动创建私有 raw topic：1 个分区，max.message.bytes 等于单条上限（默认 16 MiB）；任务删除后由平台自动清理。若开启了 ACL，平台使用的连接需要 topic 的 Create、Describe、DescribeConfigs、AlterConfigs、Read、Write 与 consumer group（ds-task-*）的 Read 权限 |
| 目标 topic | 生产环境建议由 Kafka 管理员按规范预先创建（副本数 ≥ 3，分区数按下游消费能力），在任务向导中选择「已有 topic」。向导里新建的 topic 固定为 1 分区 1 副本，仅适合测试 |
| 单条消息大小 | 目标 topic 的 max.message.bytes 要大于最大一行数据生成的事件；含大字段（LONGTEXT、BLOB、JSON）的表请按「目标兼容性」中「单条消息大小」的提示调大 |
| 保留时间 | raw topic 使用 broker 的默认保留时间（log.retention.hours，默认 7 天）。发布暂停超过保留时间时，未发布的变更会被 Kafka 删除，该任务只能重新初始化 |

### 2.5 准备配置值

安装前准备好以下值（.env 中填写，取值只能用字母、数字和 @#%+=_.,:~^-，不能有空格和引号）：

- 元数据库平台账号口令 DB_PASSWORD、MySQL root 口令 MYSQL_ROOT_PASSWORD、Redis 口令 REDIS_PASSWORD：各 12 位以上；
- 凭证主密钥 CREDENTIAL_KEY：恰好 32 个字符的随机串，可用 openssl rand -hex 16 生成。平台用它加密保存数据源口令，安装后不能修改，丢失后已保存的数据源口令无法解密，必须与备份分开、离线保管；
- 管理员 admin 的初始口令 ADMIN_INITIAL_PASSWORD：8~20 位；
- 数据目录 DATA_DIR：例如 /data/data-sync，建议放在独立的数据盘上。

## 3 安装

### 3.1 上传并校验安装包

把 data-sync-<版本>.tar 上传到服务器（例如 /opt），然后：

```bash
cd /opt
sha256sum -c data-sync-1.0.0.tar.sha256     # 输出 OK
tar -xf data-sync-1.0.0.tar
cd data-sync-1.0.0
sha256sum -c SHA256SUMS --quiet             # 没有输出表示全部文件完好
```

### 3.2 填写配置

```bash
cp .env.example .env
chmod 600 .env
vi .env
```

.env 中各项的含义见附录 B。至少要填写：DATA_DIR、DB_PASSWORD、MYSQL_ROOT_PASSWORD、REDIS_PASSWORD、CREDENTIAL_KEY、ADMIN_INITIAL_PASSWORD。DS_VERSION 随安装包给出，不要修改。

### 3.3 （可选）启用 HTTPS

1. 把 PEM 格式的证书（含中间证书链）和私钥分别放到 certs/server.crt、certs/server.key；
2. .env 中设置 ENABLE_HTTPS=true，按需修改 WEB_HTTPS_PORT（默认 443）；
3. 安装（或已安装时执行 bash scripts/dc.sh up -d web）后，HTTP 端口自动跳转到 HTTPS。

### 3.4 执行安装

```bash
sudo bash scripts/install.sh
```

安装脚本依次完成以下工作，全程约 3~5 分钟：

1. 检查 Docker 与 Compose 版本、CPU 架构、.env 的必填项与取值格式、数据目录剩余空间；
2. 导入 images/ 中的镜像（已存在则跳过）；
3. 启动 MySQL 与 Redis，执行 migrate：空库时导入初始数据，并执行全部结构迁移；
4. 把管理员 admin 的出厂口令替换为 ADMIN_INITIAL_PASSWORD（只在首次安装时生效）；
5. 启动引擎、后端与 Web，等待全部容器健康；
6. 运行自检 scripts/verify.sh，并打印访问地址。

安装脚本可以重复执行：已经完成的步骤会跳过，不会覆盖已有数据。

### 3.5 （可选）使用已有 MySQL 作为元数据库

1. 在已有 MySQL 8 上创建库和平台账号（库名与账号可自定）：

```sql
CREATE DATABASE `ry-vue` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE USER 'datasync'@'%' IDENTIFIED BY '********';
GRANT ALL PRIVILEGES ON `ry-vue`.* TO 'datasync'@'%';
```

2. .env 中清空 COMPOSE_PROFILES（COMPOSE_PROFILES=），把 DB_HOST、DB_PORT、DB_NAME、DB_USER、DB_PASSWORD 改为上面的值，MYSQL_ROOT_PASSWORD 留空；
3. 执行安装脚本。migrate 会在空库中导入初始数据。该 MySQL 建议设置 lower_case_table_names=1，字符集 utf8mb4。

### 3.6 安装后检查与首次登录

自检全部通过时输出「自检全部通过」：

```text
[ 通过 ] mysql：running healthy 重启0次
[ 通过 ] redis：running healthy 重启0次
[ 通过 ] seatunnel：running healthy 重启0次
[ 通过 ] backend：running healthy 重启0次
[ 通过 ] web：running healthy 重启0次
[ 通过 ] Web 首页 http://127.0.0.1:80/ 返回 200
[ 通过 ] 后端接口（经 Web 转发）正常
[ 通过 ] 引擎 SeaTunnel 2.3.13 正常
[ 通过 ] 后端可访问引擎
[ 通过 ] 元数据库结构为最新
[ 通过 ] 凭证加密已启用
自检全部通过
```

然后：

1. 浏览器打开 http://<服务器地址>/，用 admin 与 ADMIN_INITIAL_PASSWORD 登录（登录页需要输入算术验证码）；
2. 点击右上角头像 →「个人中心」修改口令；
3. 按《操作手册》第 3 章添加源库与 Kafka 数据源，执行「连接测试」与「CDC 前置检查」；
4. 按第 5 章配置定时备份。

## 4 日常运维

### 4.1 常用命令

scripts/dc.sh 是 docker compose 的快捷方式，自动带上 .env 与 HTTPS 配置，参数与 docker compose 相同。

| 操作 | 命令 |
|---|---|
| 运行状态自检 | bash scripts/verify.sh |
| 查看容器状态 | bash scripts/dc.sh ps |
| 查看日志 | bash scripts/dc.sh logs --tail 200 backend |
| 跟踪日志 | bash scripts/dc.sh logs -f backend |
| 重启后端 | bash scripts/dc.sh restart backend |
| 停止全部服务 | bash scripts/dc.sh stop |
| 启动全部服务 | bash scripts/dc.sh up -d |
| 查看资源占用 | docker stats |
| 进入引擎容器 | bash scripts/dc.sh exec seatunnel bash |

服务名：web、backend、seatunnel、mysql、redis。

### 4.2 重启的影响

- 重启 backend、web、redis：不影响正在运行的同步作业。后端启动后自动与引擎对账、恢复 Kafka 发布桥接，重启期间产生的变更随后按顺序补发。
- 重启 seatunnel（引擎）：服务器重启、修改引擎配置（如 ENGINE_HEAP）、升级和恢复备份都会重启引擎。引擎本身不保留作业，但每个作业的 checkpoint（同步进度，默认每 5 秒一份）保存在数据目录中。引擎恢复后的下一轮对账（约 30 秒）里，平台把运行中的任务和任务组的各张表从各自最近的 checkpoint 自动接着同步，不重新全量，期间源库的变更都会补上。Kafka 目标可能重复发布最后一个 checkpoint 周期内的少量变更，下游按同步键幂等处理即可。
- 引擎停机超过约 90 秒时，平台会先把任务标为「失败」并告警；引擎恢复后执行「恢复任务」（任务组「恢复」或「恢复该表」），同样从 checkpoint 继续。已暂停的任务照常「恢复」。
- 只有 checkpoint 已不在（数据目录中的 checkpoint 被删除或丢失）时，任务才会转为「需重新初始化」：平台不会从未知位点重启作业。
- 计划内的维护（服务器重启、调整引擎内存、升级）建议先在页面上「暂停」运行中的任务和任务组，完成后再「恢复」。暂停会保存 savepoint 并干净停止，恢复后不会重复发布。
- 服务器重启后，所有容器随 Docker 自动启动（restart: unless-stopped）。请确认 Docker 服务已设为开机自启：systemctl enable docker。

### 4.3 日志与数据位置

| 内容 | 位置 |
|---|---|
| 后端日志 | ${DATA_DIR}/backend/logs/（按天滚动） |
| 引擎日志 | ${DATA_DIR}/seatunnel/logs/ |
| 容器标准输出 | bash scripts/dc.sh logs <服务名>（每个容器最多保留 5 × 100 MB） |
| 元数据库 | ${DATA_DIR}/mysql/ |
| 引擎 checkpoint | ${DATA_DIR}/seatunnel/checkpoint/（每个作业保留最近 10 份；后端挂载它，引擎重启后据此恢复任务，并自动删除不再使用的作业目录；不要手工删除） |
| 备份 | ${DATA_DIR}/backups/（默认位置） |

### 4.4 资源调整

在 .env 中调整后执行 bash scripts/dc.sh up -d 生效（只重建被修改的容器）：

- BACKEND_HEAP：后端堆内存，默认 2g；
- ENGINE_HEAP：引擎堆内存，默认 4g。修改它会重建引擎容器，等同于重启引擎（见 4.2），建议先暂停运行中的任务，生效后再恢复。

### 4.5 定期检查

| 频率 | 内容 |
|---|---|
| 每天 | 页面右上角消息中心的「数据同步告警」；同步任务列表中非「运行中」的任务、积压与延迟；备份是否成功 |
| 每周 | bash scripts/verify.sh；磁盘使用率（df -h ${DATA_DIR}）；源库 binlog 保留与 Kafka 磁盘 |
| 每月 | 恢复演练（在测试机用备份恢复一次）；清理不再需要的任务 |

## 5 备份与恢复

### 5.1 备份内容

scripts/backup.sh 生成一个带时间戳的备份目录，包含：

| 文件 | 内容 |
|---|---|
| metadata.sql.gz | 元数据库逻辑导出（一致性快照，不锁表，平台运行中可执行） |
| seatunnel-checkpoint.tar.gz | 引擎 checkpoint：任务暂停、恢复以及引擎重启后接着同步所依赖的进度 |
| env.backup | .env 副本，含全部口令与凭证主密钥 |
| backup-info.txt | 版本、时间、主机、当时运行中的任务数 |

```bash
bash scripts/backup.sh                       # 备份到 ${DATA_DIR}/backups，保留最近 7 份
bash scripts/backup.sh /backup/data-sync 14  # 指定备份根目录，保留最近 14 份
```

建议每天凌晨执行一次，并把备份复制到另一台机器。crontab 示例（root 用户 crontab -e）：

```text
30 2 * * * cd /opt/data-sync-1.0.0 && bash scripts/backup.sh >> /var/log/data-sync-backup.log 2>&1
```

> 注意：备份目录含凭证主密钥，权限为 700。异地保存时请加密或放在受控位置。

### 5.2 恢复

```bash
bash scripts/restore.sh /data/data-sync/backups/data-sync-20261108-023000                    # 只恢复元数据库
bash scripts/restore.sh /data/data-sync/backups/data-sync-20261108-023000 --with-checkpoint  # 同时恢复引擎 checkpoint
```

恢复脚本会核对备份中的 CREDENTIAL_KEY 与当前 .env 是否一致（不一致时数据源口令无法解密，脚本会停下），停止 Web、后端与引擎，导入备份，执行结构迁移后重新启动并自检。

恢复会重启引擎：备份时处于运行中的任务，平台会从 checkpoint 自动接着同步（见 4.2）。带 --with-checkpoint 时进度回到备份时刻，之后的源库变更会重新同步一遍（Kafka 目标会重复发布这段时间的变更，按同步键幂等处理）；不带时沿用数据目录中现有的 checkpoint。恢复后登录平台，按《操作手册》7.6「恢复后检查」确认各任务状态。

整机迁移（换服务器）的步骤：在新服务器上解压同版本安装包 → 把备份中的 env.backup 复制为 .env（按新服务器修改 DATA_DIR）→ 执行 install.sh → 执行 restore.sh --with-checkpoint。

## 6 升级与回滚

### 6.1 升级步骤

1. 在页面上暂停全部运行中的任务与任务组，确认都变为「已暂停」（升级会重启引擎，暂停可以干净停止，见 4.2）；
2. 上传并解压新版本安装包到新目录（例如 /opt/data-sync-1.1.0），校验 SHA256SUMS；
3. 在新目录中执行：

```bash
cd /opt/data-sync-1.1.0
sudo bash scripts/upgrade.sh /opt/data-sync-1.0.0
```

升级脚本会：复制旧目录的 .env、证书与 compose.override.yml 并改为新版本号 → 自动备份 → 导入新镜像 → 迁移元数据库 → 用新镜像重建容器 → 自检。仍有运行中的任务时脚本会停下提示（--force 可跳过，运行中的任务在引擎重启后由平台从 checkpoint 自动接着同步）。

4. 登录平台，逐个「恢复」第 1 步暂停的任务与任务组，它们从 savepoint 接着同步。

> 注意：升级要求 .env 中的 DATA_DIR 为绝对路径，这样新旧安装目录共用同一份数据。

### 6.2 回滚

新版本运行异常需要回到旧版本时：

1. 在新目录执行 bash scripts/dc.sh down；
2. 用升级时自动生成的备份恢复元数据库：在旧目录执行 bash scripts/restore.sh <升级前的备份目录> --with-checkpoint（迁移后的结构对旧版本不一定兼容，必须恢复备份）；
3. 旧目录中 bash scripts/verify.sh 确认正常，然后按《操作手册》7.6「恢复后检查」确认各任务状态。

## 7 卸载

```bash
bash scripts/uninstall.sh                                  # 删除容器，保留数据与镜像
bash scripts/uninstall.sh --remove-images                  # 同时删除平台镜像（mysql、redis 公共镜像保留）
bash scripts/uninstall.sh --purge-data --remove-images     # 彻底删除，数据不可恢复
```

只删除容器时，重新执行 install.sh 即可用原数据恢复运行。--purge-data 只删除写有 .installed-version 的平台数据目录，DATA_DIR 指向其他目录时脚本拒绝执行。卸载前请先停止或删除平台上的任务；Kafka 上的 raw topic 不会随卸载删除，需要时由 Kafka 管理员删除 __ds_raw_ 开头的 topic。

## 8 故障处理

| 现象 | 可能原因 | 处理 |
|---|---|---|
| install.sh 报「.env 需要修改」 | 必填项为空、口令含不允许的字符、主密钥长度不是 16/24/32 | 按提示逐条修改 .env 后重新执行 |
| install.sh 报 Compose 版本过低 | 系统自带 docker-compose v1 或旧插件 | 安装 docker-compose-plugin 2.20 以上 |
| mysql 容器反复重启 | 数据目录无权限、磁盘满、旧数据目录与口令不匹配 | bash scripts/dc.sh logs mysql 查看；数据目录不能是 NFS 等不支持文件锁的存储 |
| backend 一直不健康 | 元数据库或 Redis 口令错误、迁移失败 | bash scripts/dc.sh logs --tail 300 backend，搜索 ERROR；确认 migrate 成功：bash scripts/dc.sh logs migrate |
| 页面打不开 | 端口未开放、端口被占用、web 容器未启动 | bash scripts/dc.sh ps；检查防火墙 firewall-cmd / iptables；ss -lntp 查看 80 端口占用 |
| 登录提示验证码错误或不显示 | Redis 不可用、浏览器缓存旧页面 | bash scripts/dc.sh ps redis；刷新验证码或清浏览器缓存 |
| 数据源「连接测试」失败 | 网络不通、源库白名单、主机名无法解析、账号口令错误 | 在服务器上用 nc -zv <主机> <端口> 测试；把平台服务器 IP 加入源库白名单 |
| CDC 前置检查不通过 | binlog 配置或账号权限不满足 2.3 | 按检查结果中的「处理建议」由 DBA 调整 |
| Kafka 连接失败或任务启动时报 broker 不可达 | 服务器解析不了 advertised.listeners 中的地址 | 在 /etc/hosts 中加入 broker 主机名，或让 Kafka 使用 IP 作为 advertised.listeners |
| 任务显示「运行中·发布暂停」 | 事件超过目标 topic 单条上限、无写权限、broker 故障 | 见《操作手册》7.1 |
| 任务变为「需重新初始化」 | binlog 已被清理、raw topic 中未发布的变更超过保留期被删除、配置改动 | 见《操作手册》7.2 |
| 服务器或引擎重启后任务「失败」 | 引擎停机超过约 90 秒 | 引擎恢复后执行「恢复任务」，从 checkpoint 继续（见 4.2） |
| 引擎重启后任务「需重新初始化」，提示引擎中已没有作业且没有可用的 checkpoint | 数据目录中该作业的 checkpoint 已不在 | 执行「重新初始化」（任务组逐表「重新初始化该表」） |
| 删除的 topic 又出现了 | Kafka 开启了 auto.create.topics.enable | 按 2.4 关闭自动建 topic 后再删除 |
| 磁盘使用率高 | 日志、备份、checkpoint 增长 | 清理旧备份（backup.sh 自动保留最近 7 份）；检查 ${DATA_DIR}/backend/logs |
| 同步延迟持续增大 | 源库写入量超过限速、引擎内存不足、目标端处理慢 | 查看任务详情的吞吐与积压；适当提高任务的「最大行数/秒」；调大 ENGINE_HEAP |

需要技术支持时，请提供：bash scripts/verify.sh 的输出、manifest.json、相关时段的 bash scripts/dc.sh logs --since 1h backend seatunnel 输出。

## 9 安全建议

- 只开放 Web 端口；生产环境启用 HTTPS（3.3）。
- .env 权限设为 600，只有运维人员可读；凭证主密钥离线另存。
- 首次登录后立即修改 admin 口令；为每位使用者在「系统管理 → 用户管理」中建立独立账号，按需分配角色。
- 源库同步账号只授予 2.3 所列权限；Kafka 按 2.4 配置 ACL。
- 定期执行备份与恢复演练。

## 附录 A 目录结构

| 路径 | 说明 |
|---|---|
| <安装目录>/ | 安装包解压目录，含 .env 与脚本 |
| ${DATA_DIR}/mysql | 随包元数据库数据文件 |
| ${DATA_DIR}/redis | Redis 持久化文件 |
| ${DATA_DIR}/seatunnel/checkpoint | 引擎 checkpoint |
| ${DATA_DIR}/seatunnel/logs | 引擎日志 |
| ${DATA_DIR}/backend/logs | 后端日志 |
| ${DATA_DIR}/caddy | Web 入口运行数据 |
| ${DATA_DIR}/backups | 默认备份位置 |

## 附录 B 环境变量（.env）

| 变量 | 默认值 | 说明 |
|---|---|---|
| DS_VERSION | 随包 | 镜像版本，不要修改 |
| COMPOSE_PROJECT_NAME | data-sync | 容器名前缀 |
| DATA_DIR | /data/data-sync | 数据目录，使用绝对路径 |
| TZ | Asia/Shanghai | 平台时区 |
| WEB_BIND | 0.0.0.0 | Web 绑定地址 |
| WEB_HTTP_PORT | 80 | HTTP 端口 |
| ENABLE_HTTPS | false | 是否启用 HTTPS |
| WEB_HTTPS_PORT | 443 | HTTPS 端口 |
| COMPOSE_PROFILES | bundled-db | 使用随包 MySQL；改用已有 MySQL 时清空 |
| DB_HOST / DB_PORT | datasync-mysql / 3306 | 元数据库地址（随包 MySQL 固定为 datasync-mysql） |
| DB_NAME | ry-vue | 元数据库库名 |
| DB_USER / DB_PASSWORD | datasync / 必填 | 平台连接元数据库的账号 |
| MYSQL_ROOT_PASSWORD | 必填（随包 MySQL） | 随包 MySQL 的 root 口令 |
| REDIS_PASSWORD | 必填 | Redis 口令 |
| CREDENTIAL_KEY | 必填 | 数据源口令加密主密钥，16/24/32 个字符，安装后不可修改 |
| ADMIN_INITIAL_PASSWORD | 必填 | admin 初始口令，8~20 位，仅首次安装生效 |
| BACKEND_HEAP | 2g | 后端堆内存 |
| ENGINE_HEAP | 4g | 引擎堆内存 |
| KAFKA_MAX_RECORD_BYTES | 16777216 | Kafka 单条记录上限（字节），引擎与发布桥接共用 |
| LOG_LEVEL | info | 平台日志级别 |

## 附录 C 端口一览

| 端口 | 容器 | 是否对外 | 说明 |
|---|---|---|---|
| 80 / 443 | web | 是 | Web 页面与接口入口 |
| 8080 | backend | 否 | 后端接口（经 Web 的 /prod-api/ 访问） |
| 8080 | seatunnel | 否 | 引擎 REST 接口（后端调用） |
| 3306 | mysql | 否 | 元数据库 |
| 6379 | redis | 否 | Redis |
