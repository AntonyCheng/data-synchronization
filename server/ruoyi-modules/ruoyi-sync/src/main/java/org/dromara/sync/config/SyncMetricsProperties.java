package org.dromara.sync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Retention and query limits for the engine metrics history ({@code ds_sync_metrics_sample}). */
@Data
@Component
@ConfigurationProperties(prefix = "sync.metrics")
public class SyncMetricsProperties {

    /** Samples older than this are purged. At the 30 s reconcile cadence one owner writes ~2 880 rows/day. */
    private int retentionDays = 7;

    /** Largest trailing window a series query may ask for. */
    private int maxWindowMinutes = 1440;

    /** A series longer than this is thinned evenly before it is returned. */
    private int maxPoints = 1500;
}
