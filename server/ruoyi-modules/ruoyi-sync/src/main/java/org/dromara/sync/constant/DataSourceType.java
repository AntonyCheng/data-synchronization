package org.dromara.sync.constant;

import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;

import java.util.Locale;
import java.util.Set;

/**
 * Data source kinds the platform knows about. MySQL is the only supported source;
 * PostgreSQL, MySQL and Kafka are the supported targets.
 */
public final class DataSourceType {

    public static final String MYSQL = "MYSQL";
    public static final String POSTGRESQL = "POSTGRESQL";
    public static final String KAFKA = "KAFKA";

    public static final Set<String> ALL = Set.of(MYSQL, POSTGRESQL, KAFKA);
    public static final Set<String> TARGETS = ALL;

    private DataSourceType() {
    }

    /** Upper-cases and trims; blank stays blank so callers keep their own "missing" handling. */
    public static String normalize(String type) {
        return StringUtils.isBlank(type) ? type : type.trim().toUpperCase(Locale.ROOT);
    }

    public static boolean isSupportedTarget(String type) {
        return type != null && TARGETS.contains(type.toUpperCase(Locale.ROOT));
    }

    public static boolean isMysql(DataSource dataSource) {
        return dataSource != null && MYSQL.equalsIgnoreCase(dataSource.getSourceType());
    }

    public static boolean isPostgres(DataSource dataSource) {
        return dataSource != null && POSTGRESQL.equalsIgnoreCase(dataSource.getSourceType());
    }

    public static boolean isKafka(DataSource dataSource) {
        return dataSource != null && KAFKA.equalsIgnoreCase(dataSource.getSourceType());
    }
}
