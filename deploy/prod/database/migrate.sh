#!/bin/sh
# 元数据库初始化与结构迁移（compose 的 migrate 服务执行，安装与每次升级都会运行）。
#  1. 库里还没有平台的表（sys_user 不存在）时导入初始数据：ry_vue.sql（基础框架）+ ry_sync.sql（同步模块）；
#     新装时再执行 prod-fresh-install.sql（隐藏上游示例菜单）；
#  2. 按文件名顺序执行 migrations/ 下全部结构迁移。迁移脚本都是幂等的，重复执行不会改动已迁移的库；
#  3. 执行 prod-hardening.sql：删除仍是出厂口令的演示账号等，同样可重复执行。
# 使用平台专用账号 DB_USER（只需对 DB_NAME 有全部权限），不需要 root。
set -eu

DIR=/opt/datasync/database
export MYSQL_PWD="$DB_PASSWORD"
sql() {
    mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" --default-character-set=utf8mb4 "$@"
}

echo "[migrate] 连接元数据库 $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
tries=0
until sql -e 'SELECT 1' "$DB_NAME" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
        echo "[migrate] 无法连接元数据库（已重试 60 次），最后一次的错误："
        sql -e 'SELECT 1' "$DB_NAME" || true
        exit 1
    fi
    sleep 2
done

seeded=$(sql -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'sys_user'" "$DB_NAME")
if [ "$seeded" = "0" ]; then
    echo "[migrate] 空库：导入初始数据"
    sql "$DB_NAME" < "$DIR/ry_vue.sql"
    sql "$DB_NAME" < "$DIR/ry_sync.sql"
    sql "$DB_NAME" < "$DIR/prod-fresh-install.sql"
fi

for f in "$DIR"/migrations/ry_sync_migration_*.sql; do
    echo "[migrate] $(basename "$f")"
    sql "$DB_NAME" < "$f" >/dev/null
done

echo "[migrate] 生产环境加固（演示账号等）"
sql "$DB_NAME" < "$DIR/prod-hardening.sql"

count=$(sql -N -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'ds_sync_task'" "$DB_NAME")
echo "[migrate] 完成：ds_sync_task 共 $count 列"
