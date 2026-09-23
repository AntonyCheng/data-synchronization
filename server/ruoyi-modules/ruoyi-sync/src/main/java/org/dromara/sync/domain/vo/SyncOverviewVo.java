package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only counters for the dashboard. Computed server-side over every row: the page used to
 * pull the first 100 tasks and aggregate them in the browser, which both truncated silently
 * past 100 and ignored task groups entirely (a tenant using only 多表同步 saw zeros).
 */
@Data
public class SyncOverviewVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** Single-table tasks plus the table items of every group - what actually runs on the engine. */
    private long jobTotal;
    private long jobRunning;
    private long jobFailed;

    private long taskTotal;
    private long taskRunning;
    private long groupTotal;
    private long groupRunning;
    private long groupItemTotal;
    private long groupItemRunning;

    private long dataSourceTotal;
    private long dataSourceEnabled;

    /** Rows the last consistency check compared, summed over tasks and table items. */
    private long checkedRows;
    private long checkedJobs;
    private long checkedMatched;

    /** Newest CDC lag across Kafka-target tasks, averaged; null when nothing reports one. */
    private Long avgCdcLagSeconds;

    /** Platform status -> count, over tasks and table items together. */
    private Map<String, Long> statusCounts = new LinkedHashMap<>();

    /** Target data-source type (POSTGRESQL / MYSQL / KAFKA) -> number of jobs writing to it. */
    private Map<String, Long> targetTypeCounts = new LinkedHashMap<>();

    /** Same keys, restricted to jobs currently RUNNING. */
    private Map<String, Long> targetTypeRunningCounts = new LinkedHashMap<>();
}
