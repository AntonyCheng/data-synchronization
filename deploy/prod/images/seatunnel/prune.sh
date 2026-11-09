#!/bin/sh
# 生产引擎镜像瘦身：在完整的 SeaTunnel 2.3.13 镜像里只留下平台作业用到的部分。
# 平台生成的作业只用 MySQL-CDC / Jdbc 两种 source、Sql 变换（lib/seatunnel-transforms-v2.jar）、
# Jdbc / Kafka 两种 sink；console / fake / assert 体积很小，保留供排障。
set -eu
cd /opt/seatunnel

keep="connector-cdc-base connector-cdc-mysql connector-jdbc connector-kafka connector-console connector-fake connector-assert"
for jar in connectors/connector-*.jar; do
    name=$(basename "$jar" | sed 's/-[0-9][0-9.]*\.jar$//')
    case " $keep " in
        *" $name "*) ;;
        *) rm -f "$jar" ;;
    esac
done
# 早期镜像把一份连接器放在 connectors/seatunnel/，2.3.13 只扫描 connectors/ 本身，这份不会被加载。
rm -rf connectors/seatunnel
for name in $keep; do
    ls connectors/"$name"-*.jar >/dev/null 2>&1 || { echo "prune: 缺少连接器 $name" >&2; exit 1; }
done
test -f connectors/plugin-mapping.properties

# lib/ 里与平台无关的驱动和 SDK（Oracle、SQL Server、DB2、达梦、Hive、AWS、阿里云、TiKV……）。
cd lib
rm -f aws-java-sdk-bundle-*.jar hive-exec-*.jar hive-service-*.jar libfb303-*.jar \
      ali-phoenix-shaded-thin-client-*.jar hadoop-aliyun-*.jar aliyun-sdk-oss-*.jar \
      tikv-client-java-*.jar trino-jdbc-*.jar presto-jdbc-*.jar ojdbc8-*.jar ngdbc-*.jar \
      db2jcc*.jar mssql-jdbc-*.jar redshift-jdbc*.jar terajdbc4-*.jar DmJdbcDriver*.jar \
      sqlite-jdbc-*.jar tablestore-jdbc-*.jar
test -f seatunnel-transforms-v2.jar
ls seatunnel-hadoop3-*-uber.jar >/dev/null
cd ..

# 只保留 Zeta 引擎的 starter（去掉 Flink / Spark 版）。
find starter -maxdepth 1 -name 'seatunnel-*-starter.jar' ! -name 'seatunnel-starter.jar' -exec rm -f {} +
test -f starter/seatunnel-starter.jar

rm -rf poc-entrypoint.sh poc-results logs/* checkpoint/* /tmp/* /var/lib/apt/lists/* 2>/dev/null || true
echo "prune: 完成"
du -sh /opt/seatunnel/connectors /opt/seatunnel/lib /opt/seatunnel/starter
