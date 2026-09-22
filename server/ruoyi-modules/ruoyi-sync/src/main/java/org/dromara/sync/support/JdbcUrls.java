package org.dromara.sync.support;

import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * JDBC URLs for connections the platform process opens itself (metadata reads, row
 * counts, connection tests, staging-table swaps). Engine-side URLs - the ones written
 * into a SeaTunnel job config - are built by the config generator instead, because they
 * go through {@code sync.engine.connection-endpoint-overrides} and feed the config
 * fingerprint.
 */
public final class JdbcUrls {

    /** Enough for a connection test, a COUNT(*) on an indexed range or a DDL statement. */
    public static final int DEFAULT_SOCKET_TIMEOUT_SECONDS = 5;
    /** Schema introspection through {@code DatabaseMetaData} can be slow on wide schemas. */
    public static final int METADATA_SOCKET_TIMEOUT_SECONDS = 10;

    private JdbcUrls() {
    }

    public static String of(DataSource dataSource, String database) {
        return of(dataSource, database, DEFAULT_SOCKET_TIMEOUT_SECONDS);
    }

    /** MySQL for {@code MYSQL} sources; every other relational type is treated as PostgreSQL. */
    public static String of(DataSource dataSource, String database, int socketTimeoutSeconds) {
        String db = StringUtils.isBlank(database) ? dataSource.getDatabaseName() : database;
        boolean ssl = "1".equals(dataSource.getSslEnabled());
        if (DataSourceType.isMysql(dataSource)) {
            return "jdbc:mysql://" + dataSource.getHost() + ':' + dataSource.getPort() + '/' + db
                + "?connectTimeout=5000&socketTimeout=" + socketTimeoutSeconds * 1000 + "&useSSL=" + ssl
                + "&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        }
        return "jdbc:postgresql://" + dataSource.getHost() + ':' + dataSource.getPort() + '/' + db
            + "?connectTimeout=5&socketTimeout=" + socketTimeoutSeconds + "&ssl=" + ssl;
    }

    public static Connection open(DataSource dataSource) throws SQLException {
        return open(dataSource, null, DEFAULT_SOCKET_TIMEOUT_SECONDS);
    }

    public static Connection open(DataSource dataSource, String database, int socketTimeoutSeconds) throws SQLException {
        return DriverManager.getConnection(of(dataSource, database, socketTimeoutSeconds),
            dataSource.getUsername(), dataSource.getPassword());
    }
}
