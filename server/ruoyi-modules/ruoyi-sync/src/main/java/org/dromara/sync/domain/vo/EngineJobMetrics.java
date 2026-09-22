package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * Throughput counters SeaTunnel reports for a running job, plus the CDC lag derived from
 * them. Shared shape of the single-task and task-group-item status projections.
 */
@Data
public class EngineJobMetrics implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** SNAPSHOT / CDC / MIXED - see EngineJobStates.phaseOf; MIXED is a FULL_CDC job whose snapshot boundary the engine does not expose. */
    private String phase;
    private Long sourceReceivedCount;
    private Long sinkCommittedCount;
    private Long sourceReceivedBytes;
    private Long sinkCommittedBytes;
    private Double sourceQps;
    private Double sinkQps;
    /** Rows the source has emitted that the sink has not yet committed (received - committed, never negative). */
    private Long backlogRows;
    /** End-to-end lag in seconds: from the Kafka bridge for Kafka targets; null when nothing can measure it. */
    private Long cdcLagSeconds;
    /** Set when the lag cannot be computed from what the engine returned. */
    private String metricsMessage;
}
