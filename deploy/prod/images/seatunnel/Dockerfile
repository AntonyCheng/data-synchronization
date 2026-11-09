# 生产引擎镜像：SeaTunnel 2.3.13（Zeta），两个补丁连接器，只保留平台作业用到的连接器。
#
# ENGINE_SOURCE_IMAGE 是按 deploy/local-stack/seatunnel/Dockerfile 从 apache/seatunnel:2.3.13
# 构建的完整镜像。这里删掉不用的连接器、驱动和 Flink/Spark starter 后，压成单层复制进 scratch，
# 镜像从约 3.8 GB 减到 1 GB 左右，构建时不需要访问镜像仓库。
# 构建上下文由 deploy/build-prod-package.ps1 准备：patched/（两个补丁 jar）、drivers/（MySQL、
# PostgreSQL 驱动）、prune.sh、entrypoint.sh；@RUNTIME_ENV@ 由脚本替换为源镜像的环境变量。
ARG ENGINE_SOURCE_IMAGE=data-sync-poc/seatunnel:2.3.13-before-kafka-patch
FROM ${ENGINE_SOURCE_IMAGE} AS full
USER root
COPY prune.sh /tmp/prune.sh
RUN sh /tmp/prune.sh && rm -f /tmp/prune.sh

FROM scratch
COPY --from=full / /
# @RUNTIME_ENV@
# 补丁连接器覆盖同名原版（2.3.13 从 connectors/ 本身加载插件）。
COPY patched/ /opt/seatunnel/connectors/
COPY drivers/ /opt/seatunnel/lib/
COPY entrypoint.sh /opt/seatunnel/entrypoint.sh
RUN chmod 0755 /opt/seatunnel/entrypoint.sh
WORKDIR /opt/seatunnel
EXPOSE 8080
ENTRYPOINT ["/opt/seatunnel/entrypoint.sh"]
