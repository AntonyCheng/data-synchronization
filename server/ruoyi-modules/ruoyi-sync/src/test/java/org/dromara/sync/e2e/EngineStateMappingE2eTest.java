package org.dromara.sync.e2e;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.dromara.sync.e2e.Json.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins how the platform reads the engine while a job is coming up: every Zeta state a healthy
 * job passes through between a submit and RUNNING (CREATED, INITIALIZING, PENDING, SCHEDULED)
 * must never be reported - let alone persisted - as FAILED. A persisted FAILED is not cosmetic:
 * the 30 s background reconcile only revisits RUNNING / PAUSING rows, a Kafka bridge is torn
 * down on it, and it raises a failure alert, while the engine job keeps running.
 *
 * <p>The platform refresh is sampled right after a start and right after a savepoint resume;
 * each response carries the engine state it saw together with the platform state it derived,
 * so every sample is a self-contained piece of evidence. The engine's startup window is short
 * and cannot be forced, so a run in which no transitional state was sampled is reported as
 * skipped (inconclusive) rather than passed.
 */
@Tag("e2e")
class EngineStateMappingE2eTest extends E2eSupport {

    private static final Set<String> TRANSITIONAL = Set.of("CREATED", "INITIALIZING", "PENDING", "SCHEDULED");

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void transitionalEngineStatesAreNeverReportedAsFailed() {
        String table = table("states");
        createSourceTable(table, 3);
        dropPgTableLater(table);
        long taskId = createTask(taskBody(displayName("states"), table, pgTargetId, E2eConfig.PG_SCHEMA, table, "FULL_CDC"));

        List<String> samples = new ArrayList<>();
        JsonNode started = sampleAround(taskId, "start", samples);
        String jobId = text(started, "engineJobId");
        awaitTargetMatchesSource("snapshot", () -> sourceRows(table), () -> pgRows(table), () -> describeTask(taskId));

        platform.post("/sync/task/" + taskId + "/pause", null);
        awaitEngineStatus(jobId, Set.of("SAVEPOINT_DONE"), STATE_TIMEOUT);
        awaitTaskStatus(taskId, "PAUSED", STATE_TIMEOUT);
        sampleAround(taskId, "resume", samples);

        assertEquals("STOPPED", text(platform.post("/sync/task/" + taskId + "/stop", null), "status"));
        platform.delete("/sync/task/" + taskId);
        log("samples (engine -> platform): " + samples);

        List<String> violations = samples.stream().filter(sample -> sample.endsWith("->FAILED")
            && TRANSITIONAL.stream().anyMatch(state -> sample.contains(" " + state + "->"))).toList();
        assertTrue(violations.isEmpty(), "A healthy job's transitional engine state was reported as platform FAILED "
            + "(and persisted: lastError 'SeaTunnel 作业状态为 FAILED'). EngineJobStates.toPlatformStatus must map every "
            + "non-terminal Zeta JobStatus. Violations " + violations + " among samples " + samples);
        Assumptions.assumeTrue(samples.stream().anyMatch(sample -> TRANSITIONAL.stream().anyMatch(state -> sample.contains(" " + state + "->"))),
            "inconclusive: the engine was already RUNNING at every sample, no transitional state was observed " + samples);
    }

    /**
     * Fires the lifecycle call and, while it is still in flight, the first status refresh. Both
     * take the same per-task lock, so the refresh runs the moment start / resume releases it -
     * the earliest instant the platform itself could ever observe the new job.
     */
    private static JsonNode sampleAround(long taskId, String operation, List<String> samples) {
        CompletableFuture<JsonNode> call = CompletableFuture.supplyAsync(
            () -> platform.post("/sync/task/" + taskId + "/" + operation, null));
        PlatformClient.sleep(Duration.ofMillis(300));
        sampleUntilEngineRunning(taskId, operation, samples);
        JsonNode result = call.join();
        assertEquals("RUNNING", text(result, "status"), operation + " result: " + result);
        return result;
    }

    /**
     * Refreshes through the platform (each call also persists what it derived) until the engine
     * reports RUNNING twice in a row. Samples look like {@code start#3 SCHEDULED->FAILED}.
     */
    private static void sampleUntilEngineRunning(long taskId, String phase, List<String> samples) {
        long deadline = System.nanoTime() + JOB_START_TIMEOUT.toNanos();
        int running = 0;
        for (int index = 1; running < 2 && System.nanoTime() < deadline; index++) {
            PlatformClient.Result refresh = platform.call("POST", "/sync/task/" + taskId + "/status", null);
            if (refresh.ok()) {
                String engineStatus = text(refresh.data(), "engineStatus");
                samples.add(phase + "#" + index + " " + engineStatus + "->" + text(refresh.data(), "status"));
                running = "RUNNING".equals(engineStatus) ? running + 1 : 0;
            } else {
                samples.add(phase + "#" + index + " refresh failed: " + refresh.msg());
            }
            PlatformClient.sleep(Duration.ofMillis(150));
        }
    }
}
