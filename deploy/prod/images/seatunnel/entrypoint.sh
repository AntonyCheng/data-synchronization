#!/bin/sh
# 生产引擎入口：按 ENGINE_HEAP 设置 JVM 堆，然后在前台启动单节点 Zeta 集群。
# seatunnel-cluster.sh 把 config/jvm_options 的各行追加在 JAVA_OPTS 之后，JVM 以最后一次出现的
# -Xms/-Xmx 为准，所以堆大小要写进 jvm_options（镜像内文件，每次启动重写），JAVA_OPTS 里的只作兜底。
set -eu
mkdir -p /opt/seatunnel/checkpoint /opt/seatunnel/logs
if [ -n "${ENGINE_HEAP:-}" ]; then
    opts=/opt/seatunnel/config/jvm_options
    if [ -f "$opts" ]; then
        sed -i -e '/^-Xm[sx]/d' -e '$a\' "$opts"
        printf '%s\n%s\n' "-Xms${ENGINE_HEAP}" "-Xmx${ENGINE_HEAP}" >> "$opts"
    fi
    JAVA_OPTS="${JAVA_OPTS:-} -Xms${ENGINE_HEAP} -Xmx${ENGINE_HEAP}"
    export JAVA_OPTS
fi
exec sh /opt/seatunnel/bin/seatunnel-cluster.sh
