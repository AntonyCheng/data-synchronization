数据同步平台 生产部署包
========================

适用：Linux x86_64 服务器，Docker Engine 24+，Docker Compose v2.20+。部署过程不访问任何镜像仓库。

快速开始（详细说明见 docs/部署手册.docx）：

  1. 校验安装包：    sha256sum -c SHA256SUMS
  2. 准备配置：      cp .env.example .env && chmod 600 .env && vi .env
                     （填写各口令、32 位凭证主密钥 CREDENTIAL_KEY、admin 初始口令、DATA_DIR）
  3. 安装并启动：    sudo bash scripts/install.sh
  4. 浏览器访问：    http://<服务器地址>/   用户名 admin，口令为 .env 中的 ADMIN_INITIAL_PASSWORD

常用命令（在本目录执行）：

  bash scripts/verify.sh                 运行状态自检
  bash scripts/dc.sh ps                  查看容器
  bash scripts/dc.sh logs -f backend     跟踪后端日志（服务名：web backend seatunnel mysql redis）
  bash scripts/backup.sh                 备份元数据库、引擎 checkpoint 与配置
  bash scripts/upgrade.sh <旧安装目录>   在新版本安装包目录中执行，升级
  bash scripts/uninstall.sh              卸载（默认保留数据）

目录：
  compose.yml / compose.https.yml   容器编排
  .env.example                      配置模板
  scripts/                          安装、自检、备份、恢复、升级、卸载脚本
  images/                           镜像包（docker load 格式）
  database/                         元数据库初始数据与结构迁移
  caddy/  seatunnel/  certs/        Web 入口、引擎配置、HTTPS 证书位置
  docs/                             部署手册、操作手册（Word）
  manifest.json  SHA256SUMS         版本、提交号、镜像与文件校验信息
