#!/usr/bin/env bash
# 从 backup.sh 生成的备份恢复元数据库与引擎 checkpoint。
# 用法：bash scripts/restore.sh <备份目录> [--with-checkpoint]
# 恢复会覆盖当前元数据库；执行前先停止后端和引擎，结束后重新启动。
# 恢复后：备份之后才创建的任务不存在了；恢复时处于运行中的任务按「操作手册 - 恢复后检查」逐个确认。
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require_root "$@"
load_env

src=${1:-}
[[ -n $src && -f $src/metadata.sql.gz ]] || die "用法：bash scripts/restore.sh <备份目录> [--with-checkpoint]（目录里要有 metadata.sql.gz）"
with_checkpoint=0
[[ ${2:-} == --with-checkpoint ]] && with_checkpoint=1

cat "$src/backup-info.txt" 2>/dev/null || true
if [[ -f $src/env.backup ]] && ! cmp -s <(grep '^CREDENTIAL_KEY=' "$src/env.backup") <(grep '^CREDENTIAL_KEY=' .env); then
    die "备份里的 CREDENTIAL_KEY 与当前 .env 不同：数据源口令将无法解密。请先把 .env 的 CREDENTIAL_KEY 改为备份中的值"
fi
confirm "将用 $src 覆盖当前元数据库$( ((with_checkpoint)) && echo '和引擎 checkpoint')" || die "已取消"

log "停止 Web、后端与引擎"
dc stop web backend seatunnel

if bundled_db; then
    dc up -d mysql
    wait_healthy mysql 300 || die "MySQL 未能启动"
    log "导入元数据库"
    gzip -dc "$src/metadata.sql.gz" | dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot --default-character-set=utf8mb4'
else
    log "导入元数据库（已有 MySQL）"
    gzip -dc "$src/metadata.sql.gz" | dc run --rm --no-deps -T --entrypoint sh migrate -c \
        'MYSQL_PWD="$DB_PASSWORD" mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" --default-character-set=utf8mb4'
fi
ok "元数据库已恢复"

if ((with_checkpoint)) && [[ -f $src/seatunnel-checkpoint.tar.gz ]]; then
    log "恢复引擎 checkpoint"
    rm -rf "$DATA_DIR/seatunnel/checkpoint"
    tar -xzf - -C "$DATA_DIR/seatunnel" < "$src/seatunnel-checkpoint.tar.gz"
    ok "引擎 checkpoint 已恢复"
fi

dc run --rm migrate
dc up -d --remove-orphans
wait_healthy backend 420 || die "后端未能启动"
wait_healthy web 120 || true
bash "$PKG_DIR/scripts/verify.sh" || true
log "恢复完成。请登录平台按操作手册「恢复后检查」确认各任务状态"
