#!/usr/bin/env bash
# 各运维脚本共用的函数。由 install.sh / verify.sh / upgrade.sh / backup.sh / restore.sh / uninstall.sh 引入。
set -euo pipefail

PKG_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PKG_DIR"

# Windows 上的 Git Bash 会把以 / 开头的参数改写成 Windows 路径；在 Linux 上这一行没有影响。
export MSYS_NO_PATHCONV=1

if [[ -t 1 ]]; then C_OK=$'\e[32m'; C_WARN=$'\e[33m'; C_ERR=$'\e[31m'; C_END=$'\e[0m'; else C_OK=; C_WARN=; C_ERR=; C_END=; fi
log()  { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }
ok()   { printf '%s[ 通过 ]%s %s\n' "$C_OK" "$C_END" "$*"; }
warn() { printf '%s[ 警告 ]%s %s\n' "$C_WARN" "$C_END" "$*" >&2; }
die()  { printf '%s[ 错误 ]%s %s\n' "$C_ERR" "$C_END" "$*" >&2; exit 1; }

package_version() { tr -d ' \r\n' < "$PKG_DIR/VERSION"; }

# 读取 .env（KEY=VALUE，不支持引号与空格，与 docker compose 的解析结果一致）。
load_env() {
    [[ -f .env ]] || die "缺少 .env：执行 cp .env.example .env，按注释填写后再运行"
    local line key value
    while IFS= read -r line || [[ -n $line ]]; do
        line=${line%$'\r'}
        [[ -z $line || $line == \#* ]] && continue
        [[ $line == *=* ]] || die ".env 中有无法识别的行：$line"
        key=${line%%=*}
        value=${line#*=}
        [[ $key =~ ^[A-Z_][A-Z0-9_]*$ ]] || die ".env 中的变量名不合法：$key"
        printf -v "$key" '%s' "$value"
        export "${key?}"
    done < .env
}

bundled_db() { [[ ",${COMPOSE_PROFILES:-}," == *",bundled-db,"* ]]; }

# compose.override.yml（可选）放现场的自定义项，例如 extra_hosts、额外的网络，升级时随 .env 一起沿用。
compose_args() {
    COMPOSE_ARGS=(--env-file .env -f compose.yml)
    if [[ ${ENABLE_HTTPS:-false} == true ]]; then COMPOSE_ARGS+=(-f compose.https.yml); fi
    if [[ -f compose.override.yml ]]; then COMPOSE_ARGS+=(-f compose.override.yml); fi
}

dc() {
    compose_args
    docker compose "${COMPOSE_ARGS[@]}" "$@"
}

# 数据目录里的文件属于容器内的用户（MySQL 为 999、引擎为 root），安装、备份、恢复、升级、卸载都要用 root 执行。
# Windows 上的 Git Bash（仅用于测试）跳过此检查。
require_root() {
    [[ $(uname -s) == Linux ]] || return 0
    [[ $(id -u) == 0 ]] || die "请用 root 执行：sudo bash scripts/$(basename "$0") $*"
}

require_docker() {
    command -v docker >/dev/null 2>&1 || die "未找到 docker，请先安装 Docker Engine 24+"
    docker info >/dev/null 2>&1 || die "无法连接 Docker 守护进程：请确认 docker 服务已启动，且当前用户有权限（root 或 docker 组）"
    local v
    v=$(docker compose version --short 2>/dev/null) || die "未找到 docker compose 插件（需要 Compose v2.20 以上）"
    v=${v#v}
    local major=${v%%.*} rest=${v#*.}
    local minor=${rest%%.*}
    if (( major < 2 || (major == 2 && minor < 20) )); then
        die "docker compose 版本 $v 过低，需要 2.20 以上"
    fi
}

# 校验 .env 的必填项与取值格式。
check_env() {
    local errors=() var value
    [[ ${DS_VERSION:-} == "$(package_version)" ]] || errors+=("DS_VERSION=${DS_VERSION:-} 与安装包版本 $(package_version) 不一致")
    for var in DATA_DIR DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD REDIS_PASSWORD CREDENTIAL_KEY; do
        [[ -n ${!var:-} ]] || errors+=("$var 未填写")
    done
    if bundled_db; then
        [[ -n ${MYSQL_ROOT_PASSWORD:-} ]] || errors+=("使用随包 MySQL 时 MYSQL_ROOT_PASSWORD 必须填写")
        [[ ${DB_HOST:-} == datasync-mysql ]] || errors+=("使用随包 MySQL 时 DB_HOST 必须为 datasync-mysql")
    fi
    local allowed='^[A-Za-z0-9@#%+=_.,:~^-]+$'
    for var in DB_PASSWORD REDIS_PASSWORD MYSQL_ROOT_PASSWORD ADMIN_INITIAL_PASSWORD CREDENTIAL_KEY; do
        value=${!var:-}
        [[ -z $value ]] && continue
        [[ $value =~ $allowed ]] || errors+=("$var 含不允许的字符：只能用字母、数字和 @#%+=_.,:~^-")
    done
    for var in DB_PASSWORD REDIS_PASSWORD MYSQL_ROOT_PASSWORD; do
        value=${!var:-}
        [[ -z $value || ${#value} -ge 8 ]] || errors+=("$var 至少 8 位")
    done
    local key=${CREDENTIAL_KEY:-}
    case ${#key} in
        16|24|32) ;;
        *) errors+=("CREDENTIAL_KEY 必须恰好 16、24 或 32 个字符（当前 ${#key} 个）") ;;
    esac
    [[ ${DATA_DIR:-} == /* || ${DATA_DIR:-} == ./* || ${DATA_DIR:-} =~ ^[A-Za-z]:/ ]] || errors+=("DATA_DIR 请写绝对路径（如 /data/data-sync）")
    [[ ${WEB_HTTP_PORT:-80} =~ ^[0-9]+$ ]] || errors+=("WEB_HTTP_PORT 不是数字")
    if [[ ${ENABLE_HTTPS:-false} == true ]]; then
        [[ -f certs/server.crt && -f certs/server.key ]] || errors+=("ENABLE_HTTPS=true 但缺少 certs/server.crt 或 certs/server.key")
    fi
    if ((${#errors[@]})); then
        printf '%s[ 错误 ]%s .env 需要修改：\n' "$C_ERR" "$C_END" >&2
        printf '  - %s\n' "${errors[@]}" >&2
        exit 1
    fi
}

# 安装包里的镜像（与 manifest.json 一致）。
package_images() {
    printf '%s\n' "data-sync/backend:$(package_version)" "data-sync/seatunnel:$(package_version)" \
        "data-sync/web:$(package_version)" "${MYSQL_IMAGE:-mysql:8.0}" "${REDIS_IMAGE:-redis:7-alpine}"
}

load_images() {
    local archive missing=0 image
    while read -r image; do
        docker image inspect "$image" >/dev/null 2>&1 || missing=1
    done < <(package_images)
    if (( missing == 0 )) && [[ ${FORCE_LOAD:-0} != 1 ]]; then
        log "镜像已全部存在，跳过导入（FORCE_LOAD=1 可强制重新导入）"
        return
    fi
    archive=$(ls images/data-sync-images-*.tar.gz 2>/dev/null | head -1 || true)
    [[ -n $archive ]] || die "images/ 下没有镜像包"
    log "导入镜像 $(basename "$archive")（约需 1~3 分钟）"
    docker load -i "$archive"
    while read -r image; do
        docker image inspect "$image" >/dev/null 2>&1 || die "导入后仍缺少镜像 $image"
    done < <(package_images)
}

prepare_data_dir() {
    mkdir -p "$DATA_DIR"/{mysql,redis,caddy,backend/logs,seatunnel/checkpoint,seatunnel/logs}
}

# 等待某个服务的容器进入 healthy（或一次性服务成功退出）。
wait_healthy() {
    local service=$1 timeout=${2:-300} waited=0 id status
    while (( waited < timeout )); do
        id=$(dc ps -q "$service" 2>/dev/null || true)
        if [[ -n $id ]]; then
            status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$id" 2>/dev/null || true)
            case $status in
                healthy) return 0 ;;
                unhealthy|exited|dead) break ;;
            esac
        fi
        sleep 5
        waited=$((waited + 5))
    done
    warn "$service 未在 ${timeout}s 内就绪（状态：${status:-无容器}），最近日志："
    dc logs --tail 40 "$service" >&2 || true
    return 1
}

# 通过 migrate 服务的镜像，以平台账号执行 SQL（随包 MySQL 与已有 MySQL 都适用）。
mysql_app() {
    dc run --rm --no-deps -T --entrypoint sh migrate -c \
        'MYSQL_PWD="$DB_PASSWORD" mysql -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" --default-character-set=utf8mb4 -N "$DB_NAME" "$@"' _ "$@"
}

# 交互确认；ASSUME_YES=1 时直接通过（无人值守执行）。
confirm() {
    local prompt=$1 answer
    if [[ ${ASSUME_YES:-0} == 1 ]]; then log "$prompt（ASSUME_YES=1，自动确认）"; return 0; fi
    read -r -p "$prompt，输入 yes 继续：" answer
    [[ $answer == yes ]]
}

running_task_count() {
    mysql_app -e "SELECT (SELECT COUNT(*) FROM ds_sync_task WHERE status IN ('RUNNING','PAUSING')) + (SELECT COUNT(*) FROM ds_sync_task_group WHERE status IN ('RUNNING','PAUSING','DEGRADED'))" 2>/dev/null | tr -d '\r' | tail -1
}
