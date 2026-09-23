package org.dromara.sync.support;

import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Swaps a completed FULL/OVERWRITE staging table into place on a relational target so that
 * readers never observe a missing table: {@code target -> backup}, {@code stage -> target},
 * then drop the backup.
 *
 * <ul>
 *   <li>PostgreSQL: the renames run in one transaction (DDL is transactional).</li>
 *   <li>MySQL: DDL is not transactional, but a multi-table {@code RENAME TABLE} is atomic,
 *       so both renames go into a single statement.</li>
 * </ul>
 * A backup table left behind by an earlier failed swap of the same version is dropped first.
 */
public final class TargetTableSwap {

    private static final String[] TABLE_TYPES = {"TABLE"};

    private TargetTableSwap() {
    }

    public static void swap(DataSource target, String schema, String targetName, String stageName, String backupName)
        throws SQLException {
        if (DataSourceType.isMysql(target)) {
            swapMysql(target, targetName, stageName, backupName);
        } else {
            swapPostgres(target, schema, targetName, stageName, backupName);
        }
    }

    private static void swapPostgres(DataSource target, String schema, String targetName, String stageName, String backupName)
        throws SQLException {
        try (Connection connection = JdbcUrls.open(target)) {
            connection.setAutoCommit(false);
            try {
                for (String sql : postgresPlan(schema, targetName, stageName, backupName,
                    postgresTableExists(connection, schema, targetName))) {
                    execute(connection, sql);
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

    private static void swapMysql(DataSource target, String targetName, String stageName, String backupName) throws SQLException {
        String database = target.getDatabaseName();
        try (Connection connection = JdbcUrls.open(target)) {
            boolean targetExists;
            try (ResultSet tables = connection.getMetaData().getTables(database, null, targetName, TABLE_TYPES)) {
                targetExists = tables.next();
            }
            for (String sql : mysqlPlan(database, targetName, stageName, backupName, targetExists)) {
                execute(connection, sql);
            }
        }
    }

    /**
     * The statements a PostgreSQL swap runs, in order. Kept separate from execution because
     * this is the part that must be exactly right - it renames tables in a customer database.
     * The leading drop clears a backup left by an earlier failed swap, which would otherwise
     * block the rename; PostgreSQL's RENAME TO takes a bare name, never a qualified one.
     */
    static List<String> postgresPlan(String schema, String targetName, String stageName, String backupName, boolean targetExists) {
        List<String> plan = new ArrayList<>();
        plan.add("drop table if exists " + pgName(schema, backupName));
        if (targetExists) {
            plan.add("alter table " + pgName(schema, targetName) + " rename to " + pgQuote(backupName));
        }
        plan.add("alter table " + pgName(schema, stageName) + " rename to " + pgQuote(targetName));
        plan.add("drop table if exists " + pgName(schema, backupName));
        return plan;
    }

    /**
     * The statements a MySQL swap runs, in order. MySQL DDL is not transactional, so both
     * renames must live in ONE multi-table RENAME TABLE - that statement is atomic, which is
     * what keeps readers from ever seeing a missing table.
     */
    static List<String> mysqlPlan(String database, String targetName, String stageName, String backupName, boolean targetExists) {
        List<String> plan = new ArrayList<>();
        plan.add("DROP TABLE IF EXISTS " + myName(database, backupName));
        plan.add(targetExists
            ? "RENAME TABLE " + myName(database, targetName) + " TO " + myName(database, backupName)
            + ", " + myName(database, stageName) + " TO " + myName(database, targetName)
            : "RENAME TABLE " + myName(database, stageName) + " TO " + myName(database, targetName));
        if (targetExists) plan.add("DROP TABLE IF EXISTS " + myName(database, backupName));
        return plan;
    }

    private static boolean postgresTableExists(Connection connection, String schema, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "select 1 from information_schema.tables where table_schema=? and table_name=?")) {
            statement.setString(1, schema);
            statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String pgName(String schema, String table) {
        return pgQuote(schema) + "." + pgQuote(table);
    }

    private static String pgQuote(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static String myName(String database, String table) {
        return myQuote(database) + "." + myQuote(table);
    }

    private static String myQuote(String value) {
        return "`" + value.replace("`", "``") + "`";
    }
}
