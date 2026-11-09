#!/usr/bin/env bash
# 首次安装：检查环境与 .env → 导入镜像 → 初始化元数据库 → 设置 admin 初始口令 → 启动全部服务 → 自检。
# 用法：bash scripts/install.sh
# 可以重复执行：已安装时只会补齐缺失的部分并确保服务在运行。
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require_root "$@"

DEFAULT_ADMIN_HASH='$2a$10$7JB720yubVSZvUI0rEqK/.VqGOZTH.ulu33dHOiBE8ByOhJIrdAu2'

log "数据同步平台 $(package_version) 安装开始（目录 $PKG_DIR）"
require_docker
[[ $(uname -m) == x86_64 || $(uname -m) == amd64 ]] || warn "本安装包的镜像是 x86_64（amd64），当前机器为 $(uname -m)"
load_env
check_env
admin_pw=${ADMIN_INITIAL_PASSWORD:-}
[[ ${#admin_pw} -ge 8 && ${#admin_pw} -le 20 ]] || die "ADMIN_INITIAL_PASSWORD 需要 8~20 位"
ok ".env 检查通过"

prepare_data_dir
free_kb=$(df -Pk "$DATA_DIR" 2>/dev/null | awk 'NR==2 {print $4}' || true)
if [[ -n $free_kb ]] && (( free_kb < 20 * 1024 * 1024 )); then
    warn "数据目录 $DATA_DIR 剩余空间不足 20 GB（$((free_kb / 1024 / 1024)) GB）"
fi

load_images
ok "镜像就绪"

if bundled_db; then
    log "启动随包 MySQL 与 Redis"
    dc up -d mysql redis
    wait_healthy mysql 300 || die "MySQL 未能启动"
else
    log "使用已有 MySQL：$DB_HOST:$DB_PORT/$DB_NAME"
    dc up -d redis
fi
wait_healthy redis 120 || die "Redis 未能启动"

log "初始化 / 迁移元数据库"
dc run --rm migrate || die "元数据库初始化失败，见上方输出"
ok "元数据库就绪"

# admin 仍是出厂口令 admin123 时，换成 ADMIN_INITIAL_PASSWORD（只在首次安装时生效）。
current_hash=$(mysql_app -e "SELECT password FROM sys_user WHERE user_name = 'admin'" | tr -d '\r' | tail -1) || die "无法读取 admin 账号（元数据库查询失败）"
if [[ $current_hash == "$DEFAULT_ADMIN_HASH" ]]; then
    log "设置 admin 初始口令"
    new_hash=$(docker run --rm --entrypoint java "data-sync/backend:$(package_version)" \
        -cp '/opt/datasync/tools/*' /opt/datasync/tools/BCryptHash.java "$admin_pw" | tr -d '\r' | tail -1)
    [[ $new_hash == '$2a$'* ]] || die "生成口令哈希失败"
    mysql_app -e "UPDATE sys_user SET password = '$new_hash' WHERE user_name = 'admin' AND password = '$DEFAULT_ADMIN_HASH'"
    ok "admin 初始口令已设置"
else
    log "admin 口令已不是出厂值，保持不变"
fi

log "启动引擎、后端与 Web（首次启动约 1~2 分钟）"
dc up -d --remove-orphans
wait_healthy seatunnel 300 || die "引擎未能启动"
wait_healthy backend 420 || die "后端未能启动"
wait_healthy web 120 || die "Web 未能启动"

printf '%s\n' "$(package_version)" > "$DATA_DIR/.installed-version"
ok "全部服务已启动"
echo
bash "$PKG_DIR/scripts/verify.sh" || warn "自检有失败项，请按上面的提示处理"

scheme=http; port=${WEB_HTTP_PORT:-80}
if [[ ${ENABLE_HTTPS:-false} == true ]]; then scheme=https; port=${WEB_HTTPS_PORT:-443}; fi
host_ip=$(hostname -I 2>/dev/null | awk '{print $1}' || true)
echo
log "安装完成。访问 $scheme://${host_ip:-<服务器地址>}:$port ，用户名 admin，口令为 .env 中的 ADMIN_INITIAL_PASSWORD"
log "首次登录后请在「个人中心」修改口令，并按部署手册配置备份（scripts/backup.sh）"
