#!/usr/bin/env bash
# docker compose 的快捷方式：自动带上 .env 和（启用 HTTPS 时的）compose.https.yml。
# 例：bash scripts/dc.sh ps
#     bash scripts/dc.sh logs -f --tail 200 backend
#     bash scripts/dc.sh restart backend
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env
dc "$@"
