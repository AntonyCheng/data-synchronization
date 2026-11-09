#!/usr/bin/env bash
# 运行状态自检：容器健康、Web 与接口、引擎、元数据库、后端与引擎的连通。任何一项失败时退出码为 1。
# 用法：bash scripts/verify.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

failed=0
fail() { printf '%s[ 失败 ]%s %s\n' "$C_ERR" "$C_END" "$*"; failed=1; }

services=(redis seatunnel backend web)
bundled_db && services=(mysql "${services[@]}")
for s in "${services[@]}"; do
    id=$(dc ps -q "$s" 2>/dev/null || true)
    if [[ -z $id ]]; then fail "$s：容器不存在"; continue; fi
    state=$(docker inspect -f '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}} 重启{{.RestartCount}}次' "$id")
    if [[ $state == "running healthy"* ]]; then ok "$s：$state"; else fail "$s：$state"; fi
done

base="http://127.0.0.1:${WEB_HTTP_PORT:-80}"
curl_opts=(-s --max-time 10)
if [[ ${ENABLE_HTTPS:-false} == true ]]; then
    base="https://127.0.0.1:${WEB_HTTPS_PORT:-443}"
    curl_opts+=(-k)
fi
if command -v curl >/dev/null 2>&1; then
    code=$(curl "${curl_opts[@]}" -o /dev/null -w '%{http_code}' "$base/" || true)
    [[ $code == 200 ]] && ok "Web 首页 $base/ 返回 200" || fail "Web 首页 $base/ 返回 ${code:-无响应}"
    body=$(curl "${curl_opts[@]}" -H 'Accept: application/json' "$base/prod-api/auth/code" || true)
    [[ $body == *'"code":200'* ]] && ok "后端接口（经 Web 转发）正常" || fail "后端接口异常：${body:0:200}"
else
    warn "未安装 curl，跳过 Web 检查"
fi

overview=$(dc exec -T seatunnel curl -s --max-time 10 http://127.0.0.1:8080/overview 2>/dev/null || true)
if [[ $overview == *projectVersion* ]]; then
    ver=$(printf '%s' "$overview" | sed -n 's/.*"projectVersion":"\([^"]*\)".*/\1/p')
    ok "引擎 SeaTunnel ${ver:-?} 正常"
else
    fail "引擎 REST 无响应"
fi

reach=$(dc exec -T backend curl -s --max-time 10 -o /dev/null -w '%{http_code}' http://datasync-engine:8080/overview 2>/dev/null || true)
[[ $reach == 200 ]] && ok "后端可访问引擎" || fail "后端访问引擎失败（HTTP ${reach:-无响应}）"

columns=$(mysql_app -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'ds_sync_task' AND column_name = 'kafka_bridge_backlog'" 2>/dev/null | tr -d '\r' | tail -1 || true)
[[ $columns == 1 ]] && ok "元数据库结构为最新" || fail "元数据库结构未迁移到最新（请执行 bash scripts/dc.sh run --rm migrate）"

key=${CREDENTIAL_KEY:-}
key_len=${#key}
[[ $key_len == 16 || $key_len == 24 || $key_len == 32 ]] && ok "凭证加密已启用" || fail "CREDENTIAL_KEY 长度不对"

echo
if (( failed )); then
    printf '%s自检未通过%s：排查方法见部署手册「故障处理」，查看日志：bash scripts/dc.sh logs --tail 200 <服务名>\n' "$C_ERR" "$C_END"
    exit 1
fi
printf '%s自检全部通过%s\n' "$C_OK" "$C_END"
