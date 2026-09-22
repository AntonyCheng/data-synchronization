package org.dromara.sync.engine;

import org.dromara.common.core.utils.StringUtils;
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

    /** Engine states during which the job is still in its initial snapshot phase. */
    private static final Set<String> SNAPSHOT_PHASE_STATES = Set.of("INITIALIZING", "CREATED", "PENDING", "STARTING");

    private EngineJobStates() {
    }

    /** Engine {@code jobStatus} -> {@link SyncStatus}; anything unknown (or missing) is a failure. */
    public static String toPlatformStatus(String engineStatus) {
        if (engineStatus == null) return SyncStatus.FAILED;
        return switch (engineStatus.toUpperCase(Locale.ROOT)) {
            case "RUNNING", "STARTING", "INITIALIZING", "CREATED", "PENDING", "RESTARTING" -> SyncStatus.RUNNING;
            case "DOING_SAVEPOINT" -> SyncStatus.PAUSING;
            case "SAVEPOINT_DONE" -> SyncStatus.PAUSED;
            case "CANCELED", "CANCELLED" -> SyncStatus.STOPPED;
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

    /** Copies the engine's throughput counters, the derived backlog and (when available) the CDC lag onto a status VO. */
    public static void applyMetrics(EngineJobMetrics target, SeaTunnelRestClient.JobSnapshot snapshot) {
        JsonNode metrics = snapshot.metrics();
        String engineStatus = snapshot.status();
        target.setPhase(engineStatus != null && SNAPSHOT_PHASE_STATES.contains(engineStatus) ? "SNAPSHOT" : "CDC");
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
