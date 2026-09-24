package org.dromara.sync.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.dromara.sync.e2e.Json.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 1 (+5): a single-table FULL task is a bounded BATCH job - it must end FINISHED on
 * its own, leave the target identical to the source, and be deletable without touching the
 * synced data.
 */
@Tag("e2e")
class SingleTaskFullE2eTest extends E2eSupport {

    private static final int ROWS = 8;

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void fullToPostgresFinishesWithIdenticalRows() {
        String table = table("full_pg");
        createSourceTable(table, ROWS);
        dropPgTableLater(table);

        long taskId = createTask(taskBody(displayName("full-pg"), table, pgTargetId, E2eConfig.PG_SCHEMA, table, "FULL"));
        JsonNode draft = task(taskId);
        assertEquals("DRAFT", text(draft, "status"));
        assertEquals("1", text(draft, "configVersion"));
        assertEquals("id,name,amount,note,created_at", text(draft, "selectedColumns"), "blank selection is expanded to every source column");
        assertEquals("id", text(draft, "syncKeyColumns"), "sync key resolves to the source primary key");
        assertEquals("UPSERT", text(draft, "fullDataMode"));

        JsonNode started = startTask(taskId);
        assertEquals("RUNNING", text(started, "status"));
        String jobId = text(started, "engineJobId");
        assertNotNull(jobId, "start must record the engine job id");

        awaitTaskStatus(taskId, "FINISHED", JOB_START_TIMEOUT);
        assertEquals("FINISHED", engine.jobStatus(jobId), "platform FINISHED must mirror engine FINISHED");
        assertEquals(sourceRows(table), pgRows(table), "target rows after FULL sync. " + describeTask(taskId));

        JsonNode check = platform.post("/sync/task/" + taskId + "/check", Map.of("mode", "COUNT"));
        assertTrue(Json.bool(check, "success") && Json.bool(check, "matched"), "COUNT check: " + check);
        assertEquals(String.valueOf(ROWS), text(check, "sourceRows"));
        assertEquals(String.valueOf(ROWS), text(check, "targetRows"));

        JsonNode blocks = platform.post("/sync/task/" + taskId + "/check", Map.of("mode", "KEY_RANGE", "blockSize", 3));
        assertTrue(Json.bool(blocks, "matched"), "KEY_RANGE check on a finished task: " + blocks);
        assertEquals("KEY_RANGE", text(blocks, "checkMode"));
        assertEquals("0", text(blocks, "mismatchedBlocks"), "KEY_RANGE mismatched blocks: " + blocks);

        platform.delete("/sync/task/" + taskId);
        assertTrue(Json.isAbsent(task(taskId)), "FINISHED task must be deletable");
        assertEquals(ROWS, Db.PG.count(E2eConfig.PG_SCHEMA + "." + table), "deleting a task must not delete synced target data");
        assertNotRunningOnEngine(jobId, "ds-task-" + taskId);
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void fullToMysqlFinishesWithClonedKeyTypes() {
        String table = table("full_my");
        long taskId = runFullToMysql(table, "full-my");

        // Everything except the temporal column - see fullToMysqlKeepsDatetimeWallClock.
        String columns = "id, name, amount, note";
        List<String> source = Db.SOURCE.rows("SELECT " + columns + " FROM " + table + " ORDER BY id");
        List<String> target = Db.MYSQL_TARGET.rows("SELECT " + columns + " FROM " + table + " ORDER BY id");
        assertEquals(source, target, "MySQL target rows after FULL sync. " + describeTask(taskId));

        // MySQL -> MySQL FULL pre-creates the target from the source DDL instead of trusting
        // SeaTunnel's ResultSetMetaData inference (which widens bounded VARCHARs to TEXT).
        String nameType = Db.MYSQL_TARGET.rows("SELECT COLUMN_TYPE AS t FROM information_schema.COLUMNS "
            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '" + table + "' AND COLUMN_NAME = 'name'").get(0);
        assertEquals("t=varchar(64)", nameType, "target column type must be cloned from the source DDL");
        long primaryKeyColumns = Db.MYSQL_TARGET.queryLong("SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '" + table + "' AND CONSTRAINT_NAME = 'PRIMARY'");
        assertEquals(1, primaryKeyColumns, "target keeps the source primary key");

        String jobId = text(task(taskId), "engineJobId");
        platform.delete("/sync/task/" + taskId);
        assertTrue(Json.isAbsent(task(taskId)));
        assertNotRunningOnEngine(jobId, "ds-task-" + taskId);
    }

    /**
     * A DATETIME is a zone-less wall-clock value and must arrive unchanged - the PostgreSQL
     * scenario above pins the same for that target.
     */
    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void fullToMysqlKeepsDatetimeWallClock() {
        String table = table("full_my_dt");
        long taskId = runFullToMysql(table, "full-my-dt");
        String raw = "SELECT id, CAST(created_at AS CHAR) AS created_at FROM " + table + " ORDER BY id";
        assertEquals(Db.SOURCE.rows(raw), Db.MYSQL_TARGET.rows(raw),
            "DATETIME values must be copied verbatim (raw CAST(created_at AS CHAR) shown, source vs target). " + describeTask(taskId));
    }

    private long runFullToMysql(String table, String suffix) {
        createSourceTable(table, ROWS);
        cleanup.add("drop MySQL target table " + table, () -> Db.MYSQL_TARGET.exec("DROP TABLE IF EXISTS " + table));
        long taskId = createTask(taskBody(displayName(suffix), table, mysqlTargetId, null, table, "FULL"));
        String jobId = text(startTask(taskId), "engineJobId");
        awaitTaskStatus(taskId, "FINISHED", JOB_START_TIMEOUT);
        assertEquals("FINISHED", engine.jobStatus(jobId));
        return taskId;
    }
}
