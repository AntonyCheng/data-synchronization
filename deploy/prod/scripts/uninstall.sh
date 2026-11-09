#!/usr/bin/env bash
# 卸载：停止并删除容器与网络。默认保留数据目录和镜像。
# 用法：bash scripts/uninstall.sh [--purge-data] [--remove-images]
#   --purge-data     同时删除数据目录（元数据库、checkpoint、日志），不可恢复，需要输入 yes 确认
#   --remove-images  同时删除平台镜像（data-sync/*；mysql、redis 公共镜像保留）
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require_root "$@"
load_env

purge=0; images=0
for arg in "$@"; do
    case $arg in
        --purge-data) purge=1 ;;
        --remove-images) images=1 ;;
        *) die "未知参数：$arg" ;;
    esac
done

# 防误删：--purge-data 只删除确实是本平台数据目录的路径（安装时写入了 .installed-version）。在删除容器之前检查。
if ((purge)); then
    case ${DATA_DIR%/} in
        ''|/|.|..|[A-Za-z]:) die "DATA_DIR=$DATA_DIR 不能整体删除" ;;
    esac
    [[ -f $DATA_DIR/.installed-version || -d $DATA_DIR/mysql || -d $DATA_DIR/seatunnel ]] \
        || die "$DATA_DIR 看起来不是本平台的数据目录（没有 .installed-version），未做任何改动"
fi

running=$(running_task_count || echo "")
if [[ $running =~ ^[0-9]+$ ]] && (( running > 0 )); then
    warn "还有 $running 个任务/任务组处于运行状态，卸载会中断它们"
fi
confirm "将停止并删除数据同步平台的全部容器" || die "已取消"

dc --profile bundled-db down --remove-orphans
ok "容器已删除"

if ((images)); then
    # 只删除平台自己的镜像；mysql / redis 是公共镜像，本机其他应用可能也在用，保留。
    while read -r image; do
        [[ $image == data-sync/* ]] || continue
        docker image rm "$image" >/dev/null 2>&1 && log "已删除镜像 $image" || true
    done < <(package_images)
    log "公共镜像 ${MYSQL_IMAGE:-mysql:8.0}、${REDIS_IMAGE:-redis:7-alpine} 已保留，不再需要时用 docker image rm 删除"
fi

if ((purge)); then
    confirm "将永久删除数据目录 $DATA_DIR（元数据库、checkpoint、日志、备份）" || die "已取消删除数据（容器已删除）"
    rm -rf "$DATA_DIR"
    ok "数据目录已删除"
else
    log "数据目录 $DATA_DIR 已保留；重新执行 scripts/install.sh 可用原数据恢复运行"
fi
