#!/usr/bin/env bash
# 升级：在新安装包目录里执行，沿用旧目录的 .env 与数据目录。
# 用法：bash scripts/upgrade.sh <旧安装目录> [--force]
#  1. 复制旧 .env 并改为新版本号；2. 自动备份；3. 导入新镜像；4. 迁移元数据库；5. 以新镜像重建容器；6. 自检。
# 升级会重启引擎。升级前请在页面上暂停全部运行中的任务和任务组（暂停会保存 savepoint、干净停止），
# 升级后再逐个恢复。仍有运行中的任务时脚本会停下，--force 可跳过此检查：引擎重启后平台会从各自
# 最近的 checkpoint 自动接着同步（Kafka 目标可能重复发布几秒内的变更）。
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require_root "$@"

old=${1:-}
force=0
[[ ${2:-} == --force ]] && force=1
[[ -n $old && -f $old/.env ]] || die "用法：bash scripts/upgrade.sh <旧安装目录> [--force]（旧目录里要有 .env）"
old=$(cd "$old" && pwd)
[[ $old != "$PKG_DIR" ]] || die "请在新安装包目录中执行，并把旧安装目录作为参数"
require_docker

new_version=$(package_version)
old_version=$(tr -d ' \r\n' < "$old/VERSION" 2>/dev/null || echo "?")
log "从 $old_version（$old）升级到 $new_version（$PKG_DIR）"

if [[ -f .env ]]; then
    warn "新目录已有 .env，保留不覆盖"
else
    sed "s/^DS_VERSION=.*/DS_VERSION=$new_version/" "$old/.env" > .env
    chmod 600 .env
fi
for f in server.crt server.key; do
    if [[ -f $old/certs/$f && ! -f certs/$f ]]; then mkdir -p certs && cp "$old/certs/$f" "certs/$f"; fi
done
if [[ -f $old/compose.override.yml && ! -f compose.override.yml ]]; then cp "$old/compose.override.yml" compose.override.yml; fi
load_env
case $DATA_DIR in
    /*|[A-Za-z]:/*) ;;
    *) die "旧 .env 的 DATA_DIR 是相对路径（$DATA_DIR）：先在旧目录把数据移到绝对路径并修改 DATA_DIR，再升级" ;;
esac
check_env

running=$(running_task_count || echo "")
if [[ $running =~ ^[0-9]+$ ]] && (( running > 0 )) && (( ! force )); then
    die "还有 $running 个任务/任务组在运行。请先在页面上暂停它们（升级后恢复），或加 --force 强制升级（引擎重启后平台从 checkpoint 自动接着同步）"
fi

log "升级前自动备份"
bash "$PKG_DIR/scripts/backup.sh" || die "自动备份失败，已停止升级"

load_images
log "迁移元数据库"
dc up -d redis $(bundled_db && echo mysql)
bundled_db && { wait_healthy mysql 300 || die "MySQL 未能启动"; }
dc run --rm migrate || die "元数据库迁移失败"

log "以新镜像重建容器"
dc up -d --remove-orphans
wait_healthy seatunnel 300 || die "引擎未能启动"
wait_healthy backend 420 || die "后端未能启动"
wait_healthy web 120 || die "Web 未能启动"
printf '%s\n' "$new_version" > "$DATA_DIR/.installed-version"
bash "$PKG_DIR/scripts/verify.sh" || warn "自检有失败项"
log "升级完成：$old_version → $new_version。旧目录 $old 可在确认运行正常后删除；回滚方法见部署手册"
