ARG SEATUNNEL_VERSION=2.3.13
FROM apache/seatunnel:${SEATUNNEL_VERSION}

USER root
RUN mkdir -p /opt/seatunnel/connectors/seatunnel /opt/seatunnel/checkpoint /opt/seatunnel/logs \
    && rm -f /opt/seatunnel/lib/mysql-connector-*.jar \
       /opt/seatunnel/lib/postgresql-*.jar \
       /opt/seatunnel/lib/opengauss-jdbc-*.jar
COPY vendor/connectors/ /opt/seatunnel/connectors/seatunnel/
# GoldenDB Table_map compatibility. Copied AFTER vendor/connectors/ so the patched
# connector-cdc-mysql jar overwrites the pristine one instead of sitting beside it
# (a duplicate class on the classpath). Built by build-patched-connector.ps1; see
# patch/README.md. Holds only .gitkeep when the patch has not been built.
COPY vendor/patched/ /opt/seatunnel/connectors/seatunnel/
COPY vendor/drivers/ /opt/seatunnel/lib/
COPY entrypoint.sh /opt/seatunnel/poc-entrypoint.sh
RUN chmod +x /opt/seatunnel/poc-entrypoint.sh

WORKDIR /opt/seatunnel
EXPOSE 8080
ENTRYPOINT ["/opt/seatunnel/poc-entrypoint.sh"]
