package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs one platform job - a single-table task, or one table item of a task group - on the engine
 * together with its Kafka bridge. The task lifecycle ({@code SeaTunnelJobServiceImpl}) and the
 * group lifecycle ({@code SyncTaskGroupServiceImpl}) used to implement every engine interaction
 * separately, so every fix had to be made twice and several were made only once: the group side
 * never checked that a savepoint existed before resuming, and stopped the bridge in the opposite
 * order. What the two share lives here; what differs - which status an outcome maps to,
 * validation, persistence, aggregation - stays with them.
 *
 * <p>The invariants owned here:
 * <ul>
 *   <li>a Kafka target's bridge is started <em>before</em> the job is submitted, so its topic
 *       precheck and its capacity check refuse while nothing runs on the engine yet, and it is
 *       torn down again when the submit fails;</li>
 *   <li>a job is resumed only with the configuration its savepoint was taken with, and only
 *       when a savepoint exists at all - resuming without one silently re-snapshots the table,
 *       which duplicates every event on a Kafka target;</li>
 *   <li>one failed status poll is not proof the job died: it is given up only after
 *       {@link #STATUS_FAILURE_TOLERANCE} consecutive failures, or at once when the engine
 *       says its recovery state is gone.</li>
 * </ul>
 * Owner ids are task ids or group item ids; both are snowflake ids, so they never collide in the
 * shared failure counter.
 */
@Component
public class EngineJobRunner implements DisposableBean {

    /**
     * Consecutive status-poll failures tolerated per job. One failed poll used to fail a job and
     * tear down its bridge; a single REST timeout or engine GC pause is not proof the job died -
     * the engine keeps running and the raw topic keeps buffering.
     */
    public static final int STATUS_FAILURE_TOLERANCE = 3;

    private final SeaTunnelRestClient restClient;
    private final KafkaTaskBridgeService bridge;
    /** Runs the polls of {@link #pollAll}; {@code ownedPool} when this runner created it. */
    private final Executor pollExecutor;
    private final ExecutorService ownedPool;

    /** Consecutive failed polls per owner; reset by any answered poll. */
    private final ConcurrentMap<Long, Integer> pollFailureStreak = new ConcurrentHashMap<>();

    /** The application's runner: {@link #pollAll} fans out on its own bounded, daemon pool. */
    @Autowired
    public EngineJobRunner(SeaTunnelRestClient restClient, KafkaTaskBridgeService bridge, SeaTunnelProperties properties) {
        this(restClient, bridge, boundedPool(properties.getPollParallelism()));
    }

    /** Polls on the calling thread, one after another - deterministic, for tests. */
    public EngineJobRunner(SeaTunnelRestClient restClient, KafkaTaskBridgeService bridge) {
        this(restClient, bridge, Runnable::run);
    }

    EngineJobRunner(SeaTunnelRestClient restClient, KafkaTaskBridgeService bridge, Executor pollExecutor) {
        this.restClient = restClient;
        this.bridge = bridge;
        this.pollExecutor = pollExecutor;
        this.ownedPool = pollExecutor instanceof ExecutorService pool ? pool : null;
    }

    private static ExecutorService boundedPool(int parallelism) {
        int threads = Math.max(1, parallelism);
        AtomicInteger counter = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(threads, threads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
            Thread thread = new Thread(runnable, "sync-engine-poll-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        // An instance without task groups keeps no threads.
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    @Override
    public void destroy() {
        if (ownedPool != null) ownedPool.shutdownNow();
    }

    /**
     * What submitting needs besides the config: the task (or a group item projected onto one with
     * {@code SyncTaskGroupConfigGenerator.toTask}) and its endpoints.
     */
    public record Job(SyncTask task, DataSource source, DataSource target, boolean groupItem) {

        public Long ownerId() {
            return task.getTaskId();
        }

        public boolean kafka() {
            return DataSourceType.isKafka(target);
        }
    }

    // ------------------------------------------------------------------ submit / resume

    /**
     * Everything that can refuse a submit while the engine is not involved yet - for a Kafka
     * target, the output-topic precheck and the bridge capacity - done by starting the bridge.
     * {@link #submit} / {@link #resume} do it themselves when it has not happened; a caller calls
     * it first when it must tell "refused, nothing ran" (leave the owner as it was) from "the
     * submit failed" (the engine may have accepted the job).
     */
    public void preflight(Job job) {
        if (!job.kafka()) return;
        if (job.groupItem()) bridge.startGroupItem(job.task(), job.target(), job.source());
        else bridge.start(job.task(), job.target(), job.source());
    }

    /**
     * The capacity half of {@link #preflight}, without starting anything: for a rebuild, which
     * has to be refusable before it destroys the old job but can only start the new bridge after
     * the old one is gone (a worker still running would be kept, with the old job's settings).
     */
    public void requireBridgeCapacity(Job job) {
        if (job.kafka()) bridge.requireCapacity(List.of(job.ownerId()));
    }

    /** A fresh job, no savepoint. Returns the engine job id. */
    public String submit(Job job, SeaTunnelJobConfigGenerator.GeneratedConfig generated) {
        return submitWithBridge(job, generated, null, false).jobId();
    }

    /** Resumes {@code jobId} from its savepoint (the job keeps its id); {@link #resumeRefusal} decides whether it may be. */
    public void resume(Job job, SeaTunnelJobConfigGenerator.GeneratedConfig generated, String jobId) {
        submitWithBridge(job, generated, jobId, true);
    }

    private SeaTunnelRestClient.SubmitResult submitWithBridge(Job job, SeaTunnelJobConfigGenerator.GeneratedConfig generated,
                                                              String jobId, boolean withSavepoint) {
        if (job.kafka() && !bridge.isRunning(job.ownerId())) preflight(job);
        try {
            return restClient.submit(generated.jobName(), generated.config(), jobId, withSavepoint);
        } catch (RuntimeException ex) {
            if (job.kafka()) bridge.stop(job.ownerId());
            throw ex;
        }
    }

    /**
     * Why {@code jobId} cannot be resumed with {@code generated}, or {@code null} when it can.
     * A refusal is final for this job - the caller parks it for a rebuild (REINITIALIZE_REQUIRED
     * for a task, FAILED + 重新初始化该表 for a table). A checkpoint read that fails for any other
     * reason (engine unreachable) is thrown: that is not an answer about the savepoint.
     */
    public String resumeRefusal(SeaTunnelJobConfigGenerator.GeneratedConfig generated, String storedFingerprint, String jobId) {
        if (StringUtils.isNotBlank(storedFingerprint) && !generated.matchesFingerprint(storedFingerprint)) {
            return "配置已变化，不能使用原 checkpoint 恢复，请重新初始化";
        }
        SeaTunnelRestClient.CheckpointSnapshot checkpoint;
        try {
            checkpoint = restClient.checkpoints(jobId);
        } catch (ServiceException ex) {
            if (EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) return ex.getMessage();
            throw ex;
        }
        if (checkpoint.id() == null) {
            return "没有可用的 checkpoint/savepoint，不能从未知位点恢复，请重新初始化";
        }
        return null;
    }

    // ------------------------------------------------------------------ halt / discard

    /**
     * Pause (with a savepoint) or stop. The engine goes first and the bridge only once the engine
     * accepted: a refused stop leaves a job that is still running, whose bridge should keep
     * normalizing - whether the owner then stays RUNNING or is parked is the caller's call, and
     * the bridge reconciler applies "bridge iff RUNNING" to whatever it decides.
     */
    public void halt(Long ownerId, boolean kafka, String jobId, boolean withSavepoint) {
        restClient.stop(jobId, withSavepoint, false);
        if (kafka) bridge.stop(ownerId);
    }

    /** Throws the job and its recovery state away before a rebuild. A job the engine no longer knows is fine. */
    public void discard(Long ownerId, boolean kafka, String jobId) {
        if (kafka) bridge.stop(ownerId);
        if (StringUtils.isBlank(jobId)) return;
        try {
            restClient.stop(jobId, false, true);
        } catch (ServiceException ignored) {
            // Nothing to stop or the engine no longer knows the job - the rebuild is still valid.
        }
    }

    // ------------------------------------------------------------------ status

    /** Outcome of one status poll. */
    public sealed interface Poll permits Poll.Observed, Poll.Unreachable, Poll.Lost, Poll.Boundary {

        /**
         * The engine answered. {@code checkpointError} is set when only the checkpoint read failed;
         * {@code checkpointBoundary} says that failure was the engine reporting the recovery state gone.
         */
        record Observed(SeaTunnelRestClient.JobSnapshot snapshot, SeaTunnelRestClient.CheckpointSnapshot checkpoint,
                        String checkpointError, boolean checkpointBoundary) implements Poll {

            public String platformStatus() {
                return EngineJobStates.toPlatformStatus(snapshot.status());
            }
        }

        /** No answer, fewer than {@link #STATUS_FAILURE_TOLERANCE} times in a row: hold the last-known state. */
        record Unreachable(String error, int streak) implements Poll {
        }

        /** No answer {@link #STATUS_FAILURE_TOLERANCE} times in a row: the job is given up. */
        record Lost(String error) implements Poll {
        }

        /** The engine says the job's recovery state (checkpoint / binlog position) is gone. */
        record Boundary(String error) implements Poll {
        }
    }

    /**
     * {@link #poll} for several jobs at once, on a bounded pool ({@code sync.engine.poll-parallelism}),
     * keyed and ordered as given. A group refresh is two REST calls per table; one after another,
     * an engine answering slowly (each call up to the request timeout) held the group lock for
     * minutes, and every operator action on that group waited behind it. The polls are
     * independent - each owner has its own failure count - so only they run in parallel; the
     * caller applies the outcomes on its own thread.
     */
    public Map<Long, Poll> pollAll(Map<Long, String> jobsByOwner) {
        Map<Long, CompletableFuture<Poll>> pending = new LinkedHashMap<>();
        jobsByOwner.forEach((ownerId, jobId) -> {
            try {
                pending.put(ownerId, CompletableFuture.supplyAsync(() -> poll(ownerId, jobId), pollExecutor));
            } catch (RejectedExecutionException shuttingDown) {
                // The pool is going away with the context; poll inline rather than fail the pass.
                pending.put(ownerId, CompletableFuture.completedFuture(poll(ownerId, jobId)));
            }
        });
        Map<Long, Poll> result = new LinkedHashMap<>();
        pending.forEach((ownerId, future) -> result.put(ownerId, future.join()));
        return result;
    }

    public Poll poll(Long ownerId, String jobId) {
        SeaTunnelRestClient.JobSnapshot snapshot;
        try {
            snapshot = restClient.status(jobId);
            pollFailureStreak.remove(ownerId);
        } catch (RuntimeException ex) {
            if (EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) {
                pollFailureStreak.remove(ownerId);
                return new Poll.Boundary(ex.getMessage());
            }
            int streak = pollFailureStreak.merge(ownerId, 1, Integer::sum);
            if (streak < STATUS_FAILURE_TOLERANCE) return new Poll.Unreachable(ex.getMessage(), streak);
            pollFailureStreak.remove(ownerId);
            return new Poll.Lost(ex.getMessage());
        }
        SeaTunnelRestClient.CheckpointSnapshot checkpoint = SeaTunnelRestClient.CheckpointSnapshot.empty();
        String checkpointError = null;
        boolean checkpointBoundary = false;
        try {
            checkpoint = restClient.checkpoints(jobId);
        } catch (RuntimeException ex) {
            checkpointError = ex.getMessage();
            checkpointBoundary = EngineJobStates.isRecoveryBoundaryError(ex.getMessage());
        }
        return new Poll.Observed(snapshot, checkpoint, checkpointError, checkpointBoundary);
    }

    /*
     * A bridge exactly while the owner is RUNNING - the rule KafkaBridgeReconciler applies
     * process-wide every 30 s; a status poll applies it at once for the owner it just saw.
     */

    /**
     * Brings the bridge of a RUNNING owner back if it is not up (died, or torn down by an earlier
     * failed poll). The owner's job already runs, so a full bridge pool only parks it.
     *
     * <p>Returns why the bridge could not be started (its output topic was deleted, say), or
     * {@code null}. That is reported, not escalated: the engine job is healthy and the raw topic
     * keeps every event, so once the cause is fixed the bridge resumes from its committed offset
     * and nothing is lost. Failing the owner instead would drop the bridge for good ("bridge iff
     * RUNNING") and leave a resume that the still-running job would refuse.
     */
    public String healBridge(Job job) {
        if (!job.kafka() || bridge.isRunning(job.ownerId())) return null;
        try {
            if (job.groupItem()) bridge.tryStartGroupItem(job.task(), job.target(), job.source());
            else bridge.tryStart(job.task(), job.target(), job.source());
            return null;
        } catch (RuntimeException ex) {
            return "Kafka 桥接未能启动（引擎作业仍在运行，事件在原始 topic 中保留）："
                + StringUtils.defaultIfBlank(ex.getMessage(), ex.getClass().getSimpleName());
        }
    }

    /** The owner is no longer RUNNING: its bridge goes (a no-op when there is none). */
    public void dropBridge(Long ownerId) {
        bridge.stop(ownerId);
    }

    /** Drops the poll-failure counter of an owner that is gone (deleted task / item). */
    public void forget(Long ownerId) {
        pollFailureStreak.remove(ownerId);
    }
}
