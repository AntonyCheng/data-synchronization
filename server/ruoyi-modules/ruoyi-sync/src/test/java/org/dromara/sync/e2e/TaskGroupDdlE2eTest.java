package org.dromara.sync.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.dromara.sync.e2e.Json.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 3: a MULTI_TABLE FULL_CDC group of two tables into PostgreSQL - one SeaTunnel job per
 * table - and runtime DDL isolation (docs/multi-table.md, ddl-change-management.md):
 * <ol>
 *   <li>start -> group RUNNING with both items RUNNING on two distinct engine jobs; both
 *       snapshots arrive; CDC on one table arrives.</li>
 *   <li>{@code ALTER TABLE ... ADD COLUMN} on table B + ddl-check -> only B is paused with a
 *       savepoint (DDL_BLOCKED, open DDL event); A keeps RUNNING; group DEGRADED. A status
 *       refresh must not mask DDL_BLOCKED; A still delivers CDC, B does not.</li>
 *   <li>stop -> group and every item STOPPED, no engine job of the group left running; the
 *       group is deletable.</li>
 * </ol>
 */
@Tag("e2e")
class TaskGroupDdlE2eTest extends E2eSupport {

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void multiTableGroupIsolatesDdlDriftAndStops() {
        String tableA = table("ga");
        String tableB = table("gb");
        createSourceTable(tableA, 4);
        createSourceTable(tableB, 3);
        dropPgTableLater(tableA);
        dropPgTableLater(tableB);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("groupName", displayName("group"));
        body.put("sourceId", sourceId);
        body.put("targetId", pgTargetId);
        body.put("syncScope", "MULTI_TABLE");
        body.put("syncMode", "FULL_CDC");
        body.put("items", List.of(
            groupItem(tableA, E2eConfig.PG_SCHEMA, tableA),
            groupItem(tableB, E2eConfig.PG_SCHEMA, tableB)));
        long groupId = createGroup(body);
        JsonNode draft = group(groupId);
        assertEquals("DRAFT", text(draft, "status"));
        assertEquals("PENDING", text(item(draft, tableA), "status"));
        assertEquals("PENDING", text(item(draft, tableB), "status"));
        assertEquals("id,name,amount,note,created_at", text(item(draft, tableB), "selectedColumns"));
        String itemB = text(item(draft, tableB), "itemId");

        // ---- start: one engine job per table
        JsonNode started = platform.post("/sync/group/" + groupId + "/start", null);
        assertEquals("RUNNING", text(started, "status"), "group start: " + started);
        for (String jobId : String.valueOf(text(started, "engineJobIds")).split(",")) awaitEngineSettled(jobId);
        JsonNode running = awaitGroup(groupId, "RUNNING with both items RUNNING", status ->
            "RUNNING".equals(text(status, "status"))
                && "RUNNING".equals(text(item(status, tableA), "status"))
                && "RUNNING".equals(text(item(status, tableB), "status")));
        String jobA = text(item(running, tableA), "engineJobId");
        String jobB = text(item(running, tableB), "engineJobId");
        assertNotNull(jobA);
        assertNotNull(jobB);
        assertNotEquals(jobA, jobB, "each table item runs its own engine job");
        assertEquals(Set.of(jobA, jobB), Set.of(text(group(groupId), "engineJobId").split(",")),
            "the group records exactly its items' job ids");

        awaitTargetMatchesSource("snapshot of " + tableA, () -> sourceRows(tableA), () -> pgRows(tableA), () -> describeGroup(groupId));
        awaitTargetMatchesSource("snapshot of " + tableB, () -> sourceRows(tableB), () -> pgRows(tableB), () -> describeGroup(groupId));
        log("both snapshots arrived");

        Db.SOURCE.exec(
            "INSERT INTO " + tableA + " (" + COLUMNS + ") VALUES " + values(5, "group-cdc-insert"),
            "UPDATE " + tableA + " SET amount = 12.34 WHERE id = 1");
        awaitTargetMatchesSource("CDC on " + tableA, () -> sourceRows(tableA), () -> pgRows(tableA), () -> describeGroup(groupId));
        log("CDC on table A applied");

        // ---- DDL drift on table B
        Db.SOURCE.exec("ALTER TABLE " + tableB + " ADD COLUMN extra VARCHAR(32) NULL");
        log("ALTER TABLE applied on " + tableB);
        JsonNode ddl = platform.post("/sync/group/" + groupId + "/ddl-check", null);
        log("ddl-check -> " + ddl);
        assertEquals("DEGRADED", text(ddl, "status"), "one blocked table beside a running one degrades the group: " + ddl);
        List<JsonNode> events = StreamSupport.stream(ddl.path("events").spliterator(), false).toList();
        assertEquals(1, events.size(), "exactly one DDL event (table B only): " + ddl);
        JsonNode event = events.get(0);
        assertEquals(itemB, text(event, "itemId"));
        assertEquals(tableB, text(event, "sourceTable"));
        assertTrue(Set.of("PENDING_FIX", "READY_TO_RESUME").contains(text(event, "status")), "the DDL event is open: " + event);
        assertTrue(String.valueOf(text(event, "details")).contains("extra"), "event details name the added column: " + event);

        JsonNode degraded = group(groupId);
        assertEquals("DEGRADED", text(degraded, "status"), describeGroup(groupId));
        assertEquals("DDL_BLOCKED", text(item(degraded, tableB), "status"), describeGroup(groupId));
        assertEquals("RUNNING", text(item(degraded, tableA), "status"), describeGroup(groupId));
        awaitEngineStatus(jobB, Set.of("SAVEPOINT_DONE"), STATE_TIMEOUT);
        assertEquals("RUNNING", engine.jobStatus(jobA), "the healthy table's job keeps running");

        // A refresh sees B's engine job at SAVEPOINT_DONE (= PAUSED); the open DDL event must win.
        JsonNode refreshed = refreshGroup(groupId);
        assertEquals("DEGRADED", text(refreshed, "status"), describeGroupStatus(refreshed));
        assertEquals("DDL_BLOCKED", text(item(refreshed, tableB), "status"), describeGroupStatus(refreshed));
        assertEquals("RUNNING", text(item(refreshed, tableA), "status"), describeGroupStatus(refreshed));

        insertSource(tableA, 6, "after-ddl-on-other-table");
        Db.SOURCE.exec("INSERT INTO " + tableB + " (" + COLUMNS + ", extra) VALUES "
            + values(4, "written-while-ddl-blocked").replaceFirst("\\)$", ", 'x')"));
        Await.until(tableA + " to keep receiving CDC while " + tableB + " is blocked", DATA_TIMEOUT,
            () -> pgRows(tableA), rows -> containsId(rows, 6), null, rows -> "target " + rows + "; " + describeGroup(groupId));
        Await.holdsFor("the DDL-blocked table to receive nothing", QUIET_PERIOD,
            () -> pgRows(tableB), rows -> !containsId(rows, 4));
        log("table A still flowing, table B isolated");

        // ---- stop the degraded group
        JsonNode stopped = platform.call("POST", "/sync/group/" + groupId + "/stop", null).require();
        assertEquals("STOPPED", text(stopped, "status"), "group stop: " + stopped + "; " + describeGroup(groupId));
        JsonNode afterStop = group(groupId);
        assertEquals("STOPPED", text(afterStop, "status"), describeGroup(groupId));
        assertEquals("STOPPED", text(item(afterStop, tableA), "status"), describeGroup(groupId));
        assertEquals("STOPPED", text(item(afterStop, tableB), "status"), describeGroup(groupId));
        awaitEngineStatus(jobA, Set.of("CANCELED"), STATE_TIMEOUT);
        assertNotRunningOnEngine(jobA, "ds-task-" + text(item(afterStop, tableA), "itemId"));
        assertNotRunningOnEngine(jobB, "ds-task-" + itemB);
        assertFalse(engine.isRunning(jobA, null) || engine.isRunning(jobB, null), "no job of the stopped group may run");

        platform.delete("/sync/group/" + groupId);
        assertTrue(Json.isAbsent(group(groupId)), "STOPPED group must be deletable");
    }
}
