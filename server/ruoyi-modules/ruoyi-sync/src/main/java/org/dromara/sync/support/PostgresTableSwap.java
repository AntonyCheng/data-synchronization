package org.dromara.sync.support;

import org.dromara.sync.domain.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Atomically swaps a completed FULL/OVERWRITE staging table into place on a PostgreSQL
 * target: {@code target -> backup}, {@code stage -> target}, {@code drop backup}, all in
 * one transaction so readers never observe a missing table.
 */
public final class PostgresTableSwap {

    private PostgresTableSwap() {
    }

    public static void swap(DataSource target, String schema, String targetName, String stageName, String backupName)
        throws SQLException {
        try (Connection connection = JdbcUrls.open(target)) {
            connection.setAutoCommit(false);
            try {
                if (tableExists(connection, schema, targetName)) {
                    rename(connection, schema, targetName, backupName);
                }
                rename(connection, schema, stageName, targetName);
                if (tableExists(connection, schema, backupName)) {
                    execute(connection, "drop table " + quote(schema) + "." + quote(backupName));
                }
                connection.commit();
            } catch (SQLException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private static boolean tableExists(Connection connection, String schema, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "select 1 from information_schema.tables where table_schema=? and table_name=?")) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static void rename(Connection connection, String schema, String from, String to) throws SQLException {
        execute(connection, "alter table " + quote(schema) + "." + quote(from) + " rename to " + quote(to));
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String quote(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
