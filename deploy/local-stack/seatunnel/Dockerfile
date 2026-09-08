ARG SEATUNNEL_VERSION=2.3.13
FROM apache/seatunnel:${SEATUNNEL_VERSION}

USER root
RUN mkdir -p /opt/seatunnel/connectors/seatunnel /opt/seatunnel/checkpoint /opt/seatunnel/logs \
    && rm -f /opt/seatunnel/lib/mysql-connector-*.jar \
       /opt/seatunnel/lib/postgresql-*.jar \
       /opt/seatunnel/lib/opengauss-jdbc-*.jar
COPY vendor/connectors/ /opt/seatunnel/connectors/seatunnel/
# GoldenDB Table_map compatibility. SeaTunnel 2.3.13 scans /opt/seatunnel/connectors/
# (plugin-mapping.properties lives there), NOT connectors/seatunnel/, so the patched
# jar has to overwrite the base image's own connector build in that directory. It is
# derived from that same jar by build-patched-connector.ps1; see patch/README.md.
# Holds only .gitkeep when the patch has not been built.
COPY vendor/patched/ /opt/seatunnel/connectors/
COPY vendor/drivers/ /opt/seatunnel/lib/
COPY entrypoint.sh /opt/seatunnel/poc-entrypoint.sh
RUN chmod +x /opt/seatunnel/poc-entrypoint.sh

WORKDIR /opt/seatunnel
EXPOSE 8080
ENTRYPOINT ["/opt/seatunnel/poc-entrypoint.sh"]
