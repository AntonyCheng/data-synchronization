package org.dromara.sync.e2e;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import tools.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.dromara.sync.e2e.Json.text;

/**
 * Black-box end-to-end base: drives the real backend over HTTP, checks real rows in the real
 * databases and cross-checks the engine. See docs/e2e-regression.md.
 *
 * <p>Isolation: every table / task / group / topic is named {@code e2e_<runId>_...}; nothing
 * that does not carry this run's id is ever modified. Cleanup is an undo log registered
 * <em>before</em> each object is created, run in {@link #cleanUpAndCheckForLeaks()} even when the
 * scenario failed, followed by a leak check: no engine job and no platform task/group this run
 * created may survive.
 */
@Tag("e2e")
abstract class E2eSupport {

    static final String RUN_ID = runId();

    static final Duration JOB_START_TIMEOUT = Duration.ofSeconds(120);
    static final Duration DATA_TIMEOUT = Duration.ofSeconds(90);
    static final Duration STATE_TIMEOUT = Duration.ofSeconds(90);
    /**
     * How long a negative check ("this row must NOT arrive") watches. Generated jobs checkpoint
     * every 5 s and the JDBC sink flushes on checkpoint, so three intervals is well past the
     * point where a still-consuming job would have delivered.
     */
    static final Duration QUIET_PERIOD = Duration.ofSeconds(15);

    /** Platform states a task / group cannot leave without an operator. */
    static final Set<String> FAILURE_STATES = Set.of("FAILED", "REINITIALIZE_REQUIRED");

    static final String COLUMNS = "id, name, amount, note, created_at";

    static final PlatformClient platform = new PlatformClient();
    static final EngineClient engine = new EngineClient();
    static final Cleanup cleanup = new Cleanup();

    static long sourceId;
    static long pgTargetId;
    static long mysqlTargetId;
    static long kafkaTargetId;

    /** Owners (task ids, group item ids) whose engine job {@code ds-task-<owner>} this run may have started. */
    private static final Set<Long> ownedJobOwners = ConcurrentHashMap.newKeySet();
    private static final Set<String> ownedJobIds = ConcurrentHashMap.newKeySet();
    private static final Set<Long> createdTasks = ConcurrentHashMap.newKeySet();
    private static final Set<Long> createdGroups = ConcurrentHashMap.newKeySet();
    /** Engine jobs that surfaced after the platform had marked their owner FAILED (see cancelGhostJob). */
    private static final List<String> ghostReports = new CopyOnWriteArrayList<>();
    /** Observed: the engine accepted a timed-out submit 5-10 s after the platform gave up on it. */
    private static final Duration GHOST_WATCH = Duration.ofSeconds(20);

    private static long classStartNanos = System.nanoTime();

    @BeforeAll
    static void connectToStack() {
        classStartNanos = System.nanoTime();
        if (sourceId != 0) return;
        JsonNode options = platform.get("/sync/data-source/options");
        sourceId = dataSourceId(options, E2eConfig.SOURCE_NAME);
        pgTargetId = dataSourceId(options, E2eConfig.PG_TARGET_NAME);
        mysqlTargetId = dataSourceId(options, E2eConfig.MYSQL_TARGET_NAME);
        kafkaTargetId = dataSourceId(options, E2eConfig.KAFKA_TARGET_NAME);
        log("run id " + RUN_ID + "; data sources: source=" + sourceId + ", pg=" + pgTargetId
            + ", mysql=" + mysqlTargetId + ", kafka=" + kafkaTargetId);
    }

    @AfterAll
    static void cleanUpAndCheckForLeaks() {
        log("cleanup");
        List<String> problems = new ArrayList<>(cleanup.runAll());
        problems.addAll(leaks());
        ownedJobOwners.clear();
        ownedJobIds.clear();
        createdTasks.clear();
        createdGroups.clear();
        if (!problems.isEmpty()) {
            throw new AssertionError("Cleanup / leak check failed:\n  - " + String.join("\n  - ", problems));
        }
        log("cleanup verified: no engine job and no platform task/group of this class left behind");
    }

    // ------------------------------------------------------------------ naming & logging

    static String table(String suffix) {
        return "e2e_" + RUN_ID + "_" + suffix;
    }

    static String displayName(String suffix) {
        return "e2e-" + RUN_ID + "-" + suffix;
    }

    static void log(String message) {
        double elapsed = (System.nanoTime() - classStartNanos) / 1_000_000_000.0;
        System.out.printf(Locale.ROOT, "[e2e %s +%6.1fs] %s%n", RUN_ID, elapsed, message);
    }

    // ------------------------------------------------------------------ source fixture

    /**
     * {@code id BIGINT PK, name VARCHAR(64), amount DECIMAL(12,2), note VARCHAR(255) NULL,
     * created_at DATETIME(3)} with rows {@code 1..rows}: unicode / emoji / quote in a name,
     * NULL notes, fractional decimals and millisecond timestamps.
     */
    static void createSourceTable(String table, int rows) {
        cleanup.add("drop source table " + table, () -> Db.SOURCE.exec("DROP TABLE IF EXISTS " + table));
        Db.SOURCE.exec("CREATE TABLE " + table + " ("
            + "id BIGINT NOT NULL, "
            + "name VARCHAR(64) NOT NULL, "
            + "amount DECIMAL(12,2) NOT NULL, "
            + "note VARCHAR(255) NULL, "
            + "created_at DATETIME(3) NOT NULL, "
            + "PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        List<String> values = new ArrayList<>();
        for (long id = 1; id <= rows; id++) values.add(values(id, defaultName(id)));
        Db.SOURCE.exec("INSERT INTO " + table + " (" + COLUMNS + ") VALUES " + String.join(", ", values));
    }

    static String defaultName(long id) {
        return id == 1 ? "Zoë 😀 O'Brien" : "row-" + id;
    }

    /** A VALUES tuple for {@link #COLUMNS}; every third row has a NULL note. */
    static String values(long id, String name) {
        String amount = id + "." + String.format(Locale.ROOT, "%02d", (id * 7) % 100);
        String note = id % 3 == 0 ? "NULL" : sql("note " + id);
        String createdAt = String.format(Locale.ROOT, "2026-09-01 08:%02d:%02d.%03d", id % 60, (id * 13) % 60, (id * 37) % 1000);
        return "(" + id + ", " + sql(name) + ", " + amount + ", " + note + ", '" + createdAt + "')";
    }

    static void insertSource(String table, long id, String name) {
        Db.SOURCE.exec("INSERT INTO " + table + " (" + COLUMNS + ") VALUES " + values(id, name));
    }

    static String sql(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    static List<String> sourceRows(String table) {
        return Db.SOURCE.rows("SELECT " + COLUMNS + " FROM " + table + " ORDER BY id");
    }

    static List<String> pgRows(String table) {
        return Db.PG.rows("SELECT " + COLUMNS + " FROM " + E2eConfig.PG_SCHEMA + "." + table + " ORDER BY id");
    }

    static void dropPgTableLater(String table) {
        cleanup.add("drop PostgreSQL table " + table,
            () -> Db.PG.exec("DROP TABLE IF EXISTS " + E2eConfig.PG_SCHEMA + "." + table));
    }

    static boolean containsId(List<String> rows, long id) {
        String prefix = "id=" + id + " |";
        return rows.stream().anyMatch(row -> row.startsWith(prefix));
    }

    /** Waits until the target rows equal the source rows exactly (content, count, no leftovers). */
    static void awaitTargetMatchesSource(String what, Supplier<List<String>> source, Supplier<List<String>> target,
                                         Supplier<String> context) {
        try {
            Await.until(what, DATA_TIMEOUT, () -> new RowsPair(source.get(), target.get()), RowsPair::matches,
                null, RowsPair::toString);
        } catch (AssertionError ex) {
            throw new AssertionError(ex.getMessage() + "\n  context: " + context.get(), ex);
        }
    }

    record RowsPair(List<String> source, List<String> target) {
        boolean matches() {
            return source.equals(target);
        }

        @Override
        public String toString() {
            if (matches()) return "in sync (" + source.size() + " rows)";
            List<String> missing = source.stream().filter(row -> !target.contains(row)).toList();
            List<String> unexpected = target.stream().filter(row -> !source.contains(row)).toList();
            return "source " + source.size() + " rows, target " + target.size() + " rows; missing on target "
                + missing + "; unexpected on target " + unexpected;
        }
    }

    // ------------------------------------------------------------------ single tasks

    /** A MANUAL-schedule task: the default ONCE schedule would let the scheduler race our own start. */
    static Map<String, Object> taskBody(String name, String sourceTable, long targetId, String targetSchema,
                                        String targetTable, String syncMode) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("taskName", name);
        body.put("sourceId", sourceId);
        body.put("targetId", targetId);
        body.put("sourceTable", sourceTable);
        if (targetSchema != null) body.put("targetSchema", targetSchema);
        body.put("targetTable", targetTable);
        body.put("syncMode", syncMode);
        body.put("scheduleMode", "MANUAL");
        return body;
    }

    /** Creates the task and returns its id; its cleanup is registered before the POST, by name. */
    static long createTask(Map<String, Object> body) {
        String name = (String) body.get("taskName");
        cleanup.add("discard task " + name, () -> discardTasksNamed(name));
        platform.post("/sync/task", body);
        List<Long> ids = taskIdsNamed(name);
        if (ids.size() != 1) throw new AssertionError("expected exactly one task named " + name + ", found " + ids);
        long taskId = ids.get(0);
        createdTasks.add(taskId);
        ownedJobOwners.add(taskId);
        log("created task " + name + " -> " + taskId);
        return taskId;
    }

    static List<Long> taskIdsNamed(String name) {
        JsonNode page = platform.get("/sync/task/list?pageNum=1&pageSize=50&taskName=" + encode(name));
        return StreamSupport.stream(page.path("rows").spliterator(), false)
            .filter(row -> name.equals(text(row, "taskName")))
            .map(row -> Json.longValue(row, "taskId"))
            .toList();
    }

    static JsonNode task(long taskId) {
        return platform.get("/sync/task/" + taskId);
    }

    /** Starts the task and waits for its engine job to settle (see {@link #awaitEngineSettled}). */
    static JsonNode startTask(long taskId) {
        JsonNode result = platform.post("/sync/task/" + taskId + "/start", null);
        String jobId = text(result, "engineJobId");
        if (jobId != null) {
            ownedJobIds.add(jobId);
            log("task " + taskId + " start -> " + text(result, "status") + ", engine job " + jobId + " " + awaitEngineSettled(jobId));
        }
        return result;
    }

    static JsonNode refreshTask(long taskId) {
        return platform.post("/sync/task/" + taskId + "/status", null);
    }

    /**
     * Polls the platform's explicit status refresh until {@code expected}. Aborts at once on a
     * failure state the task cannot leave by itself, with the engine's view in the message.
     */
    static JsonNode awaitTaskStatus(long taskId, String expected, Duration timeout) {
        JsonNode reached = Await.until("task " + taskId + " to reach " + expected, timeout,
            () -> refreshTask(taskId),
            status -> expected.equals(text(status, "status")),
            status -> !FAILURE_STATES.contains(expected) && FAILURE_STATES.contains(text(status, "status")),
            status -> "platform=" + text(status, "status") + ", engine=" + text(status, "engineStatus")
                + ", error=" + text(status, "errorMessage") + ", lastError=" + lastErrorOf(taskId));
        log("task " + taskId + " is " + expected + " (engine " + text(reached, "engineStatus") + ")");
        return reached;
    }

    private static String lastErrorOf(long taskId) {
        try {
            return text(task(taskId), "lastError");
        } catch (AssertionError ex) {
            return "<unavailable>";
        }
    }

    /** One line with everything needed to understand a task's situation. */
    static String describeTask(long taskId) {
        try {
            JsonNode task = task(taskId);
            if (Json.isAbsent(task)) return "task " + taskId + " does not exist";
            String jobId = text(task, "engineJobId");
            String engineError = engine.jobError(jobId);
            return "task " + taskId + " status=" + text(task, "status") + ", lastError=" + text(task, "lastError")
                + ", engineJobId=" + jobId + ", engine=" + engine.jobStatus(jobId)
                + (engineError == null ? "" : " (" + engineError + ")");
        } catch (RuntimeException | AssertionError ex) {
            return "task " + taskId + " (description failed: " + ex.getMessage() + ")";
        }
    }

    static void discardTasksNamed(String name) {
        List<String> failures = new ArrayList<>();
        for (Long taskId : taskIdsNamed(name)) {
            try {
                discardTask(taskId);
            } catch (RuntimeException | AssertionError ex) {
                failures.add(ex.getMessage());
            }
        }
        if (!failures.isEmpty()) throw new AssertionError(String.join("; ", failures));
    }

    /**
     * Stop (if the platform thinks it runs), make sure the engine really has nothing left, then
     * delete. A task stuck in a non-deletable state gets one status refresh to pick up the engine
     * truth (CANCELED -> STOPPED) before the second delete attempt.
     */
    static void discardTask(long taskId) {
        JsonNode task = task(taskId);
        if (Json.isAbsent(task)) return;
        String jobId = text(task, "engineJobId");
        String status = text(task, "status");
        if (jobId != null && !Set.of("DRAFT", "STOPPED", "FINISHED").contains(status)) {
            PlatformClient.Result stopped = platform.call("POST", "/sync/task/" + taskId + "/stop", null);
            if (!stopped.ok()) log("cleanup: stop task " + taskId + " (" + status + ") refused: " + stopped.msg());
        }
        // The engine safety net reports but never blocks the delete: a slow cancel must not leave the row behind.
        List<String> problems = new ArrayList<>();
        attempt(problems, () -> stopOnEngineIfRunning(jobId, "ds-task-" + taskId));
        if (FAILURE_STATES.contains(status)) {
            attempt(problems, () -> cancelGhostJob("ds-task-" + taskId, "task " + taskId + " (" + status + ")"));
        }
        PlatformClient.Result deleted = platform.call("DELETE", "/sync/task/" + taskId, null);
        if (!deleted.ok() && jobId != null) {
            platform.call("POST", "/sync/task/" + taskId + "/status", null);
            deleted = platform.call("DELETE", "/sync/task/" + taskId, null);
        }
        if (!Json.isAbsent(task(taskId))) {
            problems.add("task " + taskId + " could not be deleted (" + deleted.msg() + "): " + describeTask(taskId));
        }
        if (!problems.isEmpty()) throw new AssertionError(String.join("; ", problems));
    }

    /** Runs one cleanup action, recording instead of propagating its failure. */
    private static void attempt(List<String> problems, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | AssertionError ex) {
            problems.add(ex.getMessage());
        }
    }

    // ------------------------------------------------------------------ task groups

    static Map<String, Object> groupItem(String sourceTable, String targetSchema, String targetTable) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("sourceTable", sourceTable);
        if (targetSchema != null) item.put("targetSchema", targetSchema);
        item.put("targetTable", targetTable);
        return item;
    }

    static long createGroup(Map<String, Object> body) {
        String name = (String) body.get("groupName");
        cleanup.add("discard group " + name, () -> discardGroupsNamed(name));
        platform.post("/sync/group", body);
        List<Long> ids = groupIdsNamed(name);
        if (ids.size() != 1) throw new AssertionError("expected exactly one group named " + name + ", found " + ids);
        long groupId = ids.get(0);
        createdGroups.add(groupId);
        for (JsonNode item : group(groupId).path("items")) ownedJobOwners.add(Json.longValue(item, "itemId"));
        log("created group " + name + " -> " + groupId);
        return groupId;
    }

    static List<Long> groupIdsNamed(String name) {
        JsonNode page = platform.get("/sync/group/list?pageNum=1&pageSize=50&groupName=" + encode(name));
        return StreamSupport.stream(page.path("rows").spliterator(), false)
            .filter(row -> name.equals(text(row, "groupName")))
            .map(row -> Json.longValue(row, "groupId"))
            .toList();
    }

    static JsonNode group(long groupId) {
        return platform.get("/sync/group/" + groupId);
    }

    static JsonNode refreshGroup(long groupId) {
        return platform.post("/sync/group/" + groupId + "/status", null);
    }

    /** The group's item whose source table is {@code sourceTable}. */
    static JsonNode item(JsonNode groupOrStatus, String sourceTable) {
        for (JsonNode item : groupOrStatus.path("items")) {
            if (sourceTable.equals(text(item, "sourceTable"))) return item;
        }
        throw new AssertionError("no item for " + sourceTable + " in " + groupOrStatus);
    }

    /** Polls the group status refresh until {@code done}; aborts on a FAILED group unless that is awaited. */
    static JsonNode awaitGroup(long groupId, String what, Predicate<JsonNode> done) {
        JsonNode reached = Await.until("group " + groupId + ": " + what, STATE_TIMEOUT,
            () -> refreshGroup(groupId),
            done,
            status -> "FAILED".equals(text(status, "status")),
            E2eSupport::describeGroupStatus);
        log("group " + groupId + ": " + what + " -> " + describeGroupStatus(reached));
        return reached;
    }

    static String describeGroupStatus(JsonNode status) {
        String items = StreamSupport.stream(status.path("items").spliterator(), false)
            .map(item -> text(item, "sourceTable") + "=" + text(item, "status")
                + "(engine " + text(item, "engineStatus") + (text(item, "errorMessage") == null ? "" : ", " + text(item, "errorMessage")) + ")")
            .collect(Collectors.joining(", "));
        return "group=" + text(status, "status") + " [" + items + "]";
    }

    static String describeGroup(long groupId) {
        try {
            JsonNode group = group(groupId);
            if (Json.isAbsent(group)) return "group " + groupId + " does not exist";
            String items = StreamSupport.stream(group.path("items").spliterator(), false)
                .map(item -> text(item, "sourceTable") + "=" + text(item, "status") + "(job " + text(item, "engineJobId")
                    + " engine " + engine.jobStatus(text(item, "engineJobId")) + ", lastError=" + text(item, "lastError") + ")")
                .collect(Collectors.joining(", "));
            return "group " + groupId + " status=" + text(group, "status") + ", lastError=" + text(group, "lastError") + " [" + items + "]";
        } catch (RuntimeException | AssertionError ex) {
            return "group " + groupId + " (description failed: " + ex.getMessage() + ")";
        }
    }

    static void discardGroupsNamed(String name) {
        List<String> failures = new ArrayList<>();
        for (Long groupId : groupIdsNamed(name)) {
            try {
                discardGroup(groupId);
            } catch (RuntimeException | AssertionError ex) {
                failures.add(ex.getMessage());
            }
        }
        if (!failures.isEmpty()) throw new AssertionError(String.join("; ", failures));
    }

    static void discardGroup(long groupId) {
        JsonNode group = group(groupId);
        if (Json.isAbsent(group)) return;
        String status = text(group, "status");
        if (!Set.of("DRAFT", "STOPPED", "FAILED", "FINISHED").contains(status)) {
            PlatformClient.Result stopped = platform.call("POST", "/sync/group/" + groupId + "/stop", null);
            if (!stopped.ok()) log("cleanup: stop group " + groupId + " (" + status + ") refused: " + stopped.msg());
        }
        List<String> problems = new ArrayList<>();
        for (JsonNode item : group(groupId).path("items")) {
            String jobName = "ds-task-" + text(item, "itemId");
            attempt(problems, () -> stopOnEngineIfRunning(text(item, "engineJobId"), jobName));
            if ("FAILED".equals(text(item, "status"))) {
                attempt(problems, () -> cancelGhostJob(jobName, "group item " + text(item, "itemId") + " (FAILED)"));
            }
        }
        PlatformClient.Result deleted = platform.call("DELETE", "/sync/group/" + groupId, null);
        if (!deleted.ok()) {
            platform.call("POST", "/sync/group/" + groupId + "/status", null);
            deleted = platform.call("DELETE", "/sync/group/" + groupId, null);
        }
        if (!Json.isAbsent(group(groupId))) {
            problems.add("group " + groupId + " could not be deleted (" + deleted.msg() + "): " + describeGroup(groupId));
        }
        if (!problems.isEmpty()) throw new AssertionError(String.join("; ", problems));
    }

    // ------------------------------------------------------------------ engine

    /** Zeta job states between a submit and RUNNING. */
    static final Set<String> ENGINE_STARTUP_STATES = Set.of("CREATED", "INITIALIZING", "PENDING", "SCHEDULED", "STARTING");

    /**
     * Waits until the engine itself reports the job past its startup states. Lifecycle scenarios
     * call this after every submit (start / resume) before asking the platform for a status:
     * a platform refresh that lands while Zeta still reports SCHEDULED persists FAILED (product
     * bug, pinned separately by EngineStateMappingE2eTest), and racing that window would make
     * every scenario flaky instead of testing what it is about.
     */
    static String awaitEngineSettled(String jobId) {
        return Await.until("engine job " + jobId + " to leave its startup states", JOB_START_TIMEOUT,
            () -> engine.jobStatus(jobId), status -> !status.startsWith("<") && !ENGINE_STARTUP_STATES.contains(status));
    }

    static String awaitEngineStatus(String jobId, Set<String> expected, Duration timeout) {
        return Await.until("engine job " + jobId + " to reach " + expected, timeout,
            () -> engine.jobStatus(jobId), expected::contains);
    }

    static void assertNotRunningOnEngine(String jobId, String jobName) {
        Await.until("engine to drop " + jobName + " / " + jobId + " from its running jobs", STATE_TIMEOUT,
            engine::runningJobs,
            running -> !running.containsKey(jobId) && !running.containsValue(jobName),
            null, running -> "running jobs " + running);
    }

    /**
     * Cleanup safety net: hard-cancel a job of ours the platform failed to stop. A cancel the
     * platform just issued completes asynchronously, so the job gets a grace period to leave
     * the running list on its own first.
     */
    static void stopOnEngineIfRunning(String jobId, String jobName) {
        List<String> ours = List.of();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        do {
            ours = engine.runningJobs().entrySet().stream()
                .filter(job -> job.getKey().equals(jobId) || job.getValue() != null && job.getValue().equals(jobName))
                .map(Map.Entry::getKey)
                .toList();
            if (ours.isEmpty()) return;
            PlatformClient.sleep(Await.POLL_INTERVAL);
        } while (System.nanoTime() < deadline);
        log("cleanup: engine still runs " + ours + " (" + jobName + ") - cancelling it directly");
        ours.forEach(engine::stopJob);
        for (String id : ours) assertNotRunningOnEngine(id, jobName);
    }

    /**
     * A submit / resume whose answer timed out leaves the platform at FAILED while the engine may
     * still accept the job seconds later - a job nobody reconciles (docs/e2e-regression.md). Before
     * such a row is deleted, its job name is watched for a while; anything that shows up is
     * cancelled and reported, because it would otherwise keep writing to the target unattended.
     */
    static void cancelGhostJob(String jobName, String owner) {
        long deadline = System.nanoTime() + GHOST_WATCH.toNanos();
        while (System.nanoTime() < deadline) {
            List<String> ghosts = engine.runningJobs().entrySet().stream()
                .filter(job -> jobName.equals(job.getValue()))
                .map(Map.Entry::getKey)
                .toList();
            if (!ghosts.isEmpty()) {
                ghostReports.add("engine accepted " + ghosts + " (" + jobName + ") for " + owner
                    + " after the platform had reported the submit as failed; cancelled by cleanup");
                log("cleanup: ghost job " + ghosts + " for " + owner + " - cancelling it");
                ghosts.forEach(engine::stopJob);
                for (String id : ghosts) assertNotRunningOnEngine(id, jobName);
                return;
            }
            PlatformClient.sleep(Await.POLL_INTERVAL);
        }
    }

    private static List<String> leaks() {
        List<String> leaks = new ArrayList<>(ghostReports);
        ghostReports.clear();
        try {
            Set<String> ownedNames = ownedJobOwners.stream().map(owner -> "ds-task-" + owner).collect(Collectors.toSet());
            for (Map.Entry<String, String> job : engine.runningJobs().entrySet()) {
                if (ownedJobIds.contains(job.getKey()) || ownedNames.contains(job.getValue())) {
                    leaks.add("engine job " + job.getKey() + " (" + job.getValue() + ") still running after cleanup");
                    try {
                        engine.stopJob(job.getKey());
                    } catch (RuntimeException | AssertionError ignored) {
                        // Reported above either way.
                    }
                }
            }
        } catch (RuntimeException | AssertionError ex) {
            leaks.add("could not list engine jobs for the leak check: " + ex.getMessage());
        }
        for (Long taskId : createdTasks) {
            if (!Json.isAbsent(platform.call("GET", "/sync/task/" + taskId, null).data())) leaks.add("platform task " + taskId + " still exists");
        }
        for (Long groupId : createdGroups) {
            if (!Json.isAbsent(platform.call("GET", "/sync/group/" + groupId, null).data())) leaks.add("platform group " + groupId + " still exists");
        }
        return leaks;
    }

    // ------------------------------------------------------------------ misc

    /** A state-machine guard: the call must fail with a business error naming the reason. */
    static void assertRejected(PlatformClient.Result result, String messageFragment, String what) {
        if (result.ok()) throw new AssertionError(what + " must be refused, but succeeded: " + result.data());
        if (result.msg() == null || !result.msg().contains(messageFragment)) {
            throw new AssertionError(what + " must be refused with a message containing '" + messageFragment + "', got: " + result.msg());
        }
    }

    private static long dataSourceId(JsonNode options, String name) {
        for (JsonNode option : options) {
            if (name.equals(text(option, "sourceName"))) return Json.longValue(option, "sourceId");
        }
        throw new AssertionError("data source '" + name + "' is not registered; found "
            + StreamSupport.stream(options.spliterator(), false).map(option -> text(option, "sourceName")).toList());
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String runId() {
        String configured = System.getProperty("e2e.run-id");
        if (configured != null && configured.matches("[a-z0-9]{1,12}")) return configured;
        return String.format(Locale.ROOT, "%06x", ThreadLocalRandom.current().nextInt(0x1000000));
    }
}
