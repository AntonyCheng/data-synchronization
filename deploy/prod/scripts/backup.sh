#!/usr/bin/env bash
# 备份：元数据库（逻辑导出）+ 引擎 checkpoint + .env（含口令与凭证主密钥）。
# 用法：bash scripts/backup.sh [备份根目录，默认 $DATA_DIR/backups] [保留份数，默认 7]
# 建议每天定时执行（crontab 示例见部署手册）。备份目录含密钥，权限设为 700，并复制到另一台机器保存。
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
require_root "$@"
load_env

root=${1:-$DATA_DIR/backups}
keep=${2:-7}
stamp=$(date +%Y%m%d-%H%M%S)
dest="$root/data-sync-$stamp"
mkdir -p "$dest"
chmod 700 "$root" "$dest" 2>/dev/null || true
log "备份到 $dest"

# 1. 元数据库：--single-transaction 得到一致快照，不锁表，平台运行中也可执行。
dump_ok=1
if bundled_db; then
    dc exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysqldump -uroot --single-transaction --routines --triggers --no-tablespaces --default-character-set=utf8mb4 --databases "$MYSQL_DATABASE"' \
        | gzip > "$dest/metadata.sql.gz" || dump_ok=0
else
    dc run --rm --no-deps -T --entrypoint sh migrate -c \
        'MYSQL_PWD="$DB_PASSWORD" mysqldump -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" --single-transaction --no-tablespaces --default-character-set=utf8mb4 --databases "$DB_NAME"' \
        | gzip > "$dest/metadata.sql.gz" || dump_ok=0
fi
(( dump_ok )) || die "元数据库导出失败（mysqldump 出错，见上方输出），本次备份不完整：$dest"
gzip -t "$dest/metadata.sql.gz" && [[ $(gzip -dc "$dest/metadata.sql.gz" | head -c 4096 | wc -c) -gt 100 ]] || die "元数据库导出失败"
ok "元数据库已导出（$(du -h "$dest/metadata.sql.gz" | cut -f1)）"

# 2. 引擎 checkpoint：任务的暂停 / 恢复依赖它。运行中的任务可能正写入最新一份，恢复时会用前一份完整的。
if [[ -d $DATA_DIR/seatunnel/checkpoint ]]; then
    # 归档经标准输入输出读写：tar 会把带冒号的文件名（如 host:path）当作远程归档。
    tar -czf - -C "$DATA_DIR/seatunnel" checkpoint > "$dest/seatunnel-checkpoint.tar.gz"
    ok "引擎 checkpoint 已打包（$(du -h "$dest/seatunnel-checkpoint.tar.gz" | cut -f1)）"
fi

# 3. 配置与密钥
cp .env "$dest/env.backup"
chmod 600 "$dest/env.backup"
cp VERSION "$dest/VERSION"
running=$(running_task_count || echo "?")
{
    echo "版本: $(package_version)"
    echo "时间: $(date '+%F %T %z')"
    echo "主机: $(hostname)"
    echo "运行中的任务/任务组: $running"
} > "$dest/backup-info.txt"
ok "配置已备份（env.backup 含全部口令和凭证主密钥）"

# 4. 只保留最近 $keep 份
mapfile -t old < <(ls -1d "$root"/data-sync-* 2>/dev/null | sort | head -n -"$keep")
if ((${#old[@]})); then
    rm -rf "${old[@]}"
    log "已清理 ${#old[@]} 份旧备份"
fi
log "备份完成：$dest"
