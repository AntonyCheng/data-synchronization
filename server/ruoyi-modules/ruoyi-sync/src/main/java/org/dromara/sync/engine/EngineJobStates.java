package org.dromara.sync.engine;

import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.vo.EngineJobMetrics;
import tools.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Set;

/**
 * Translates what the SeaTunnel Zeta REST API reports about a job into the platform's
 * state vocabulary. Shared by the single-task and task-group lifecycle services so both
 * interpret engine state identically.
 */
public final class EngineJobStates {

    /** Engine states before the job has read anything - whatever comes next starts with the snapshot. */
    private static final Set<String> STARTUP_STATES = Set.of("INITIALIZING", "CREATED", "PENDING", "SCHEDULED", "STARTING");

    public static final String PHASE_SNAPSHOT = "SNAPSHOT";
    public static final String PHASE_CDC = "CDC";
    /** A FULL_CDC job past startup: Zeta exposes no snapshot-complete signal, so the phase cannot be told apart. */
    public static final String PHASE_MIXED = "MIXED";

    private EngineJobStates() {
    }

    /**
     * Engine {@code jobStatus} -> {@link SyncStatus}; anything unknown (or missing) is a failure.
     * <p>Every transitional state of Zeta's {@code JobStatus} must be listed: the fallback is
     * FAILED, and a status refresh that lands in a transition persists it. {@code SCHEDULED}
     * (slots assigned, tasks not deployed yet) sits between PENDING and RUNNING on every start
     * and resume, so leaving it out turned a healthy start into FAILED whenever a refresh hit
     * that window; {@code CANCELING} is the way to CANCELED after a stop.
     */
    public static String toPlatformStatus(String engineStatus) {
        if (engineStatus == null) return SyncStatus.FAILED;
        return switch (engineStatus.toUpperCase(Locale.ROOT)) {
            case "RUNNING", "STARTING", "INITIALIZING", "CREATED", "PENDING", "SCHEDULED", "RESTARTING" -> SyncStatus.RUNNING;
            case "DOING_SAVEPOINT" -> SyncStatus.PAUSING;
            case "SAVEPOINT_DONE" -> SyncStatus.PAUSED;
            case "CANCELING", "CANCELLING", "CANCELED", "CANCELLED" -> SyncStatus.STOPPED;
            case "FINISHED" -> SyncStatus.FINISHED;
            default -> SyncStatus.FAILED;
        };
    }

    /**
     * A checkpoint / savepoint / binlog-position error means the job cannot be resumed
     * from where it was. It must fail fast into REINITIALIZE_REQUIRED rather than sit in
     * the transient-unreachability grace window.
     */
    public static boolean isRecoveryBoundaryError(String error) {
        if (StringUtils.isBlank(error)) return false;
        String normalized = error.toLowerCase(Locale.ROOT);
        return normalized.contains("checkpoint") || normalized.contains("savepoint")
            || normalized.contains("binlog") || normalized.contains("offset")
            || normalized.contains("restore") || normalized.contains("恢复")
            || normalized.contains("位点") || normalized.contains("日志已过期");
    }

    /**
     * What the job is doing, as far as it can honestly be known: a FULL job only ever
     * snapshots, an INCREMENTAL job only ever tails the binlog, and a FULL_CDC job is
     * SNAPSHOT until it leaves the startup states and MIXED afterwards.
     */
    public static String phaseOf(String syncMode, String engineStatus) {
        if (SyncMode.isFull(syncMode)) return PHASE_SNAPSHOT;
        if (SyncMode.INCREMENTAL.equalsIgnoreCase(syncMode)) return PHASE_CDC;
        return engineStatus != null && STARTUP_STATES.contains(engineStatus.toUpperCase(Locale.ROOT)) ? PHASE_SNAPSHOT : PHASE_MIXED;
    }

    /** Copies the engine's throughput counters, the derived backlog and (when available) the CDC lag onto a status VO. */
    public static void applyMetrics(EngineJobMetrics target, SeaTunnelRestClient.JobSnapshot snapshot, String syncMode) {
        JsonNode metrics = snapshot.metrics();
        target.setPhase(phaseOf(syncMode, snapshot.status()));
        target.setSourceReceivedCount(metricLong(metrics, "SourceReceivedCount"));
        target.setSinkCommittedCount(metricLong(metrics, "SinkCommittedCount"));
        target.setSourceReceivedBytes(metricLong(metrics, "SourceReceivedBytes"));
        target.setSinkCommittedBytes(metricLong(metrics, "SinkCommittedBytes"));
        target.setSourceQps(metricDouble(metrics, "SourceReceivedQPS"));
        target.setSinkQps(metricDouble(metrics, "SinkCommittedQPS"));
        Long received = target.getSourceReceivedCount();
        Long committed = target.getSinkCommittedCount();
        target.setBacklogRows(received != null && committed != null ? Math.max(0L, received - committed) : null);
        // Zeta 2.3.13 exposes no event timestamps over REST, so a time-based CDC lag can only
        // come from elsewhere (the Kafka bridge for Kafka targets); it stays null here.
        Long sourceEvent = metricLong(metrics, "SourceLatestEventTime");
        Long sinkCommit = metricLong(metrics, "SinkLatestCommitTime");
        if (sourceEvent != null && sinkCommit != null && sinkCommit >= sourceEvent) {
            target.setCdcLagSeconds((sinkCommit - sourceEvent) / 1000L);
        } else {
            target.setMetricsMessage("引擎未返回事件时间，以待提交行数衡量积压");
        }
    }

    /**
     * Zeta serializes every metric as a JSON string ("50", "0.37"), not a number - accept
     * both. Anything unparsable is reported as absent rather than as zero.
     */
    private static Long metricLong(JsonNode metrics, String key) {
        BigDecimal value = metricNumber(metrics, key);
        return value == null ? null : value.longValue();
    }

    private static Double metricDouble(JsonNode metrics, String key) {
        BigDecimal value = metricNumber(metrics, key);
        return value == null ? null : value.doubleValue();
    }

    private static BigDecimal metricNumber(JsonNode metrics, String key) {
        JsonNode node = metrics == null ? null : metrics.get(key);
        if (node == null || node.isNull()) return null;
        if (node.isNumber()) return node.decimalValue();
        if (!node.isTextual() || StringUtils.isBlank(node.asText())) return null;
        try {
            return new BigDecimal(node.asText().trim());
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
