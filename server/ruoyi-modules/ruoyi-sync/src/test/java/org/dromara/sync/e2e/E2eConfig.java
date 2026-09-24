package org.dromara.sync.e2e;

/**
 * Where the e2e suite finds the running local stack. Defaults match {@code dev.ps1 up -Poc}
 * ({@code deploy/local-stack/compose.yml}, {@code platform/docker-compose.yml},
 * {@code application-dev.yml}); every value can be overridden with {@code -De2e.<key>=...}
 * or the environment variable shown next to it.
 */
final class E2eConfig {

    static final String BACKEND = value("e2e.backend", "E2E_BACKEND", "http://localhost:18081");
    static final String ENGINE = value("e2e.engine", "E2E_ENGINE", "http://localhost:18080");

    static final String CLIENT_ID = value("e2e.client-id", "E2E_CLIENT_ID", "e5cd7e4891bf95d1d19206ce24a7b32e");
    static final String TENANT_ID = value("e2e.tenant-id", "E2E_TENANT_ID", "000000");
    static final String USERNAME = value("e2e.username", "E2E_USERNAME", "admin");
    static final String PASSWORD = value("e2e.password", "E2E_PASSWORD", "admin123");

    /** Captcha answers live in the platform Redis ({@code global:captcha_codes:<uuid>}). */
    static final String REDIS_HOST = value("e2e.redis.host", "E2E_REDIS_HOST", "localhost");
    static final int REDIS_PORT = Integer.parseInt(value("e2e.redis.port", "E2E_REDIS_PORT", "16379"));
    static final String REDIS_PASSWORD = value("e2e.redis.password", "E2E_REDIS_PASSWORD", "ruoyi123");

    /** Registered data sources, looked up by name through {@code /sync/data-source/options}. */
    static final String SOURCE_NAME = value("e2e.ds.source", "E2E_DS_SOURCE", "POC MySQL");
    static final String PG_TARGET_NAME = value("e2e.ds.pg", "E2E_DS_PG", "POC PostgreSQL");
    static final String MYSQL_TARGET_NAME = value("e2e.ds.mysql", "E2E_DS_MYSQL", "POC MySQL Target");
    static final String KAFKA_TARGET_NAME = value("e2e.ds.kafka", "E2E_DS_KAFKA", "POC Kafka 3.8");

    /** Source MySQL as root: the platform's {@code seatunnel} account cannot CREATE / ALTER / DROP. */
    static final String SOURCE_JDBC = value("e2e.source.jdbc", "E2E_SOURCE_JDBC",
        "jdbc:mysql://localhost:23306/source_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    static final String SOURCE_USER = value("e2e.source.user", "E2E_SOURCE_USER", "root");
    static final String SOURCE_PASSWORD = value("e2e.source.password", "MYSQL_ROOT_PASSWORD", "poc_root_pw");

    static final String PG_JDBC = value("e2e.pg.jdbc", "E2E_PG_JDBC", "jdbc:postgresql://localhost:25432/sink_db");
    static final String PG_USER = value("e2e.pg.user", "POSTGRES_USER", "poc");
    static final String PG_PASSWORD = value("e2e.pg.password", "POSTGRES_PASSWORD", "poc_password");
    static final String PG_SCHEMA = "public";

    static final String MYSQL_TARGET_JDBC = value("e2e.mysql-target.jdbc", "E2E_MYSQL_TARGET_JDBC",
        "jdbc:mysql://localhost:23307/sink_mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
    static final String MYSQL_TARGET_USER = value("e2e.mysql-target.user", "E2E_MYSQL_TARGET_USER", "root");
    static final String MYSQL_TARGET_PASSWORD = value("e2e.mysql-target.password", "TARGET_MYSQL_ROOT_PASSWORD", "poc_target_root_pw");

    static final String KAFKA_BOOTSTRAP = value("e2e.kafka.bootstrap", "E2E_KAFKA_BOOTSTRAP", "localhost:29092");

    private E2eConfig() {
    }

    private static String value(String property, String env, String fallback) {
        String fromProperty = System.getProperty(property);
        if (fromProperty != null && !fromProperty.isBlank()) return fromProperty.trim();
        String fromEnv = System.getenv(env);
        return fromEnv != null && !fromEnv.isBlank() ? fromEnv.trim() : fallback;
    }
}
