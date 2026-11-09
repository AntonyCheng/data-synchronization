# 生产后端镜像：平台后端 jar（以 -Pprod 打包）运行在 Liberica OpenJDK 21 上。
#
# RUNTIME_IMAGE 是按 server/ruoyi-admin/Dockerfile 在 bellsoft/liberica-openjdk-rocky:21.0.12-cds
# 上构建过的后端镜像。这里取它的运行时（去掉旧 jar），压成单层后放入本次构建的 jar，
# 构建时不需要访问镜像仓库。@RUNTIME_ENV@ 由 deploy/build-prod-package.ps1 替换为源镜像的环境变量。
# tools/ 含 Hutool 加解密 jar 与 BCryptHash.java：安装脚本用它为 admin 初始口令生成 BCrypt 哈希。
ARG RUNTIME_IMAGE=data-sync-mvp/backend:latest
FROM ${RUNTIME_IMAGE} AS runtime
USER root
RUN rm -rf /ruoyi/server/app.jar /ruoyi/server/logs/* /ruoyi/server/temp/* /tmp/*

FROM scratch
COPY --from=runtime / /
# @RUNTIME_ENV@
WORKDIR /ruoyi/server
COPY app.jar /ruoyi/server/app.jar
COPY tools/ /opt/datasync/tools/
ENV SERVER_PORT=8080 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    JAVA_OPTS=""
EXPOSE 8080
# exec：让 java 成为 1 号进程，docker stop 时能正常收到 SIGTERM、优雅退出。
ENTRYPOINT ["/bin/sh", "-c", "exec java -Djava.security.egd=file:/dev/./urandom -Dserver.port=${SERVER_PORT} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/ruoyi/server/logs -XX:+UseZGC ${JAVA_OPTS} -jar /ruoyi/server/app.jar"]
