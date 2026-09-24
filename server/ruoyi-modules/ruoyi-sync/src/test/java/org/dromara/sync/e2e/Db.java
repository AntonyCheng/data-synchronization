package org.dromara.sync.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Plain JDBC against one of the stack's real databases. A connection per call keeps it immune
 * to container restarts between steps; the suite issues a few hundred statements at most.
 */
final class Db {

    static final Db SOURCE = new Db("source MySQL", E2eConfig.SOURCE_JDBC, E2eConfig.SOURCE_USER, E2eConfig.SOURCE_PASSWORD);
    static final Db PG = new Db("target PostgreSQL", E2eConfig.PG_JDBC, E2eConfig.PG_USER, E2eConfig.PG_PASSWORD);
    static final Db MYSQL_TARGET = new Db("target MySQL", E2eConfig.MYSQL_TARGET_JDBC,
        E2eConfig.MYSQL_TARGET_USER, E2eConfig.MYSQL_TARGET_PASSWORD);

    private final String label;
    private final String url;
    private final String user;
    private final String password;

    private Db(String label, String url, String user, String password) {
        this.label = label;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    Connection open() throws SQLException {
        DriverManager.setLoginTimeout(10);
        return DriverManager.getConnection(url, user, password);
    }

    void exec(String... statements) {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            for (String sql : statements) statement.execute(sql);
        } catch (SQLException ex) {
            throw new AssertionError(label + " statement failed: " + ex.getMessage() + " -- " + String.join("; ", statements), ex);
        }
    }

    long count(String table) {
        return queryLong("SELECT COUNT(*) FROM " + table);
    }

    long queryLong(String sql) {
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException ex) {
            throw new AssertionError(label + " query failed: " + ex.getMessage() + " -- " + sql, ex);
        }
    }

    /**
     * Rows as canonical strings ({@code id=1 | name=... | amount=12.5 | ...}) so two databases with
     * different type systems compare with a plain {@code assertEquals} and a readable diff.
     * DECIMAL scale and driver-specific temporal classes are normalized away.
     */
    List<String> rows(String sql) {
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData meta = resultSet.getMetaData();
            List<String> rows = new ArrayList<>();
            while (resultSet.next()) {
                StringBuilder row = new StringBuilder();
                for (int column = 1; column <= meta.getColumnCount(); column++) {
                    if (column > 1) row.append(" | ");
                    row.append(meta.getColumnLabel(column).toLowerCase(Locale.ROOT)).append('=')
                        .append(canonical(resultSet, column, meta.getColumnType(column)));
                }
                rows.add(row.toString());
            }
            return rows;
        } catch (SQLException ex) {
            throw new AssertionError(label + " query failed: " + ex.getMessage() + " -- " + sql, ex);
        }
    }

    private static String canonical(ResultSet resultSet, int column, int type) throws SQLException {
        Object value = switch (type) {
            case Types.TIMESTAMP, Types.DATE -> resultSet.getObject(column, LocalDateTime.class);
            default -> resultSet.getObject(column);
        };
        if (value == null) return "null";
        if (value instanceof BigDecimal decimal) return decimal.stripTrailingZeros().toPlainString();
        if (value instanceof Number number) return new BigDecimal(number.toString()).stripTrailingZeros().toPlainString();
        return value.toString();
    }

    @Override
    public String toString() {
        return label;
    }
}
