# 生产前端镜像：Caddy 提供前端静态文件（/srv）并把 /prod-api/ 反向代理到后端。
# 部署时 caddy/Caddyfile（或 Caddyfile.https）会覆盖这里内置的默认配置。
ARG CADDY_IMAGE=caddy:2.8.4-alpine
FROM ${CADDY_IMAGE}
COPY dist/ /srv/
COPY Caddyfile /etc/caddy/Caddyfile
EXPOSE 80 443
