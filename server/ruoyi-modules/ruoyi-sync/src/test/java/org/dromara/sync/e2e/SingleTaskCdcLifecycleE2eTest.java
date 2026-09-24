package org.dromara.sync.e2e;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.dromara.sync.e2e.Json.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Scenario 2: the whole single-task FULL_CDC lifecycle against MySQL -> PostgreSQL, pinning
 * the semantics the lifecycle refactor must keep (docs/task-state-machine.md, job-lifecycle.md):
 * <ol>
 *   <li>DRAFT -> start -> RUNNING; snapshot rows arrive; INSERT/UPDATE/DELETE are applied.</li>
 *   <li>Guards while RUNNING: no second start, no edit, no delete.</li>
 *   <li>pause = stop-with-savepoint: PAUSING -> PAUSED, engine SAVEPOINT_DONE; writes made while
 *       paused do not reach the target; a PAUSED task can be neither started nor deleted.</li>
 *   <li>resume reuses the same engine job id from the savepoint: the paused-time write arrives, and
 *       a target-only marker survives - a fresh snapshot would have overwritten it.</li>
 *   <li>stop -> STOPPED, engine CANCELED and gone from running jobs, binlog no longer consumed,
 *       resume refused; STOPPED is deletable.</li>
 * </ol>
 */
@Tag("e2e")
class SingleTaskCdcLifecycleE2eTest extends E2eSupport {

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void fullCdcLifecyclePauseResumeStop() {
        String table = table("cdc");
        String pgTable = E2eConfig.PG_SCHEMA + "." + table;
        createSourceTable(table, 5);
        dropPgTableLater(table);
        long taskId = createTask(taskBody(displayName("cdc"), table, pgTargetId, E2eConfig.PG_SCHEMA, table, "FULL_CDC"));
        assertEquals("DRAFT", text(task(taskId), "status"));

        // ---- start: snapshot, then CDC
        JsonNode started = startTask(taskId);
        assertEquals("RUNNING", text(started, "status"));
        String jobId = text(started, "engineJobId");
        assertNotNull(jobId, "start must record the engine job id");
        awaitTaskStatus(taskId, "RUNNING", JOB_START_TIMEOUT);
        awaitTargetMatchesSource("initial snapshot of " + table, () -> sourceRows(table), () -> pgRows(table),
            () -> describeTask(taskId));
        log("snapshot arrived");

        Db.SOURCE.exec(
            "INSERT INTO " + table + " (" + COLUMNS + ") VALUES " + values(6, "inserted-by-cdc"),
            "UPDATE " + table + " SET name = 'updated-by-cdc', amount = 99.99, note = NULL WHERE id = 2",
            "DELETE FROM " + table + " WHERE id = 3");
        awaitTargetMatchesSource("CDC insert/update/delete", () -> sourceRows(table), () -> pgRows(table),
            () -> describeTask(taskId));
        log("CDC insert/update/delete applied");

        // ---- guards while RUNNING
        assertRejected(platform.call("POST", "/sync/task/" + taskId + "/start", null), "正在运行", "second start of a RUNNING task");
        assertRejected(platform.call("DELETE", "/sync/task/" + taskId, null), "不能删除", "delete of a RUNNING task");
        assertRejected(platform.call("PUT", "/sync/task", editBody(taskId, table)), "允许修改", "edit of a RUNNING task");
        assertEquals("RUNNING", text(task(taskId), "status"), "refused operations must not change the state");

        // ---- pause with savepoint
        JsonNode paused = platform.post("/sync/task/" + taskId + "/pause", null);
        assertEquals("PAUSING", text(paused, "status"), "pause answers PAUSING until the savepoint is confirmed");
        awaitTaskStatus(taskId, "PAUSED", STATE_TIMEOUT);
        assertEquals("SAVEPOINT_DONE", engine.jobStatus(jobId), "PAUSED must mean the engine completed a savepoint");
        assertFalse(engine.isRunning(jobId, "ds-task-" + taskId), "a paused job must not be running on the engine");
        assertNotNull(text(task(taskId), "lastCheckpointId"), "a paused task records the checkpoint/savepoint it will resume from");

        assertRejected(platform.call("POST", "/sync/task/" + taskId + "/start", null), "暂停", "start of a PAUSED task (would discard the savepoint)");
        assertRejected(platform.call("DELETE", "/sync/task/" + taskId, null), "不能删除", "delete of a PAUSED task");

        insertSource(table, 7, "written-while-paused");
        // Target-only marker: a savepoint resume replays only binlog after the savepoint and
        // leaves row 4 alone; a fresh snapshot would overwrite it with the source value.
        Db.PG.exec("UPDATE " + pgTable + " SET name = 'target-only-marker' WHERE id = 4");
        Await.holdsFor("the row written while paused to stay off the target", QUIET_PERIOD,
            () -> pgRows(table), rows -> !containsId(rows, 7));
        log("paused task consumed nothing");

        // ---- resume from savepoint
        JsonNode resumed = platform.post("/sync/task/" + taskId + "/resume", null);
        assertEquals("RUNNING", text(resumed, "status"));
        assertEquals(jobId, text(resumed, "engineJobId"), "resume must reuse the original engine job id (savepoint restore)");
        awaitEngineSettled(jobId);
        awaitTaskStatus(taskId, "RUNNING", JOB_START_TIMEOUT);
        assertEquals(List.of(jobId), jobIdsNamed("ds-task-" + taskId),
            "after resume the engine must run exactly the original job id again, not a newly submitted job");
        Await.until("the row written while paused to arrive after resume", DATA_TIMEOUT,
            () -> pgRows(table), rows -> containsId(rows, 7), null, rows -> "target " + rows + "; " + describeTask(taskId));
        List<String> afterResume = pgRows(table);
        assertTrue(afterResume.stream().anyMatch(row -> row.startsWith("id=4 | name=target-only-marker |")),
            "row 4 was re-written after resume, i.e. the job re-snapshotted instead of resuming from the savepoint: " + afterResume);
        assertEquals(Db.SOURCE.count(table), Db.PG.count(pgTable), "row count after resume (no duplicates, nothing lost)");
        assertEquals(0, Db.PG.queryLong("SELECT COUNT(*) FROM (SELECT id FROM " + pgTable + " GROUP BY id HAVING COUNT(*) > 1) d"),
            "duplicate ids on the target after resume");
        log("resumed from savepoint: paused-time row arrived, marker intact");

        // CDC keeps flowing after the resume, including onto the marked row.
        Db.SOURCE.exec(
            "INSERT INTO " + table + " (" + COLUMNS + ") VALUES " + values(8, "inserted-after-resume"),
            "UPDATE " + table + " SET name = 'updated-after-resume' WHERE id = 4");
        awaitTargetMatchesSource("CDC after resume", () -> sourceRows(table), () -> pgRows(table), () -> describeTask(taskId));

        // ---- stop
        JsonNode stopped = platform.post("/sync/task/" + taskId + "/stop", null);
        assertEquals("STOPPED", text(stopped, "status"));
        JsonNode afterStop = task(taskId);
        assertEquals("STOPPED", text(afterStop, "status"));
        assertTrue(text(afterStop, "nextRunTime") == null, "a manual stop parks the schedule");
        awaitEngineStatus(jobId, Set.of("CANCELED"), STATE_TIMEOUT);
        assertNotRunningOnEngine(jobId, "ds-task-" + taskId);

        insertSource(table, 9, "written-after-stop");
        Await.holdsFor("a stopped task to consume no more binlog", QUIET_PERIOD,
            () -> pgRows(table), rows -> !containsId(rows, 9));
        assertRejected(platform.call("POST", "/sync/task/" + taskId + "/resume", null), "恢复", "resume of a STOPPED task");
        assertEquals("STOPPED", text(task(taskId), "status"));

        // ---- delete
        platform.delete("/sync/task/" + taskId);
        assertTrue(Json.isAbsent(task(taskId)), "STOPPED task must be deletable");
        assertTrue(taskIdsNamed(displayName("cdc")).isEmpty(), "deleted task must not be listed");
        assertNotEquals(0, Db.PG.count(pgTable), "deleting a task must not delete synced target data");
    }

    private static List<String> jobIdsNamed(String jobName) {
        return engine.runningJobs().entrySet().stream()
            .filter(job -> jobName.equals(job.getValue()))
            .map(Map.Entry::getKey)
            .toList();
    }

    /** A harmless edit (same config): only the RUNNING-state guard can reject it. */
    private static Map<String, Object> editBody(long taskId, String table) {
        Map<String, Object> body = taskBody(displayName("cdc"), table, pgTargetId, E2eConfig.PG_SCHEMA, table, "FULL_CDC");
        body.put("taskId", taskId);
        return body;
    }
}
