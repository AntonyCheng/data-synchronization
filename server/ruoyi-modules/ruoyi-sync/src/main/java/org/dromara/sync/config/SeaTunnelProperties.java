package org.dromara.sync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Runtime settings for the SeaTunnel job adapter. */
@Data
@Component
@ConfigurationProperties(prefix = "sync.engine")
public class SeaTunnelProperties {

    /** SeaTunnel REST API base URL. */
    private String endpoint = "http://localhost:18080";

    /** Timeout used by future submit/status operations. */
    private Duration requestTimeout = Duration.ofSeconds(10);

    /** Checkpoint interval written into generated jobs. */
    private int checkpointIntervalMs = 5000;

    /** Maximum source snapshot throughput for generated jobs. */
    private int readLimitRowsPerSecond = 1000;

    /** Default source snapshot byte throughput in bytes per second. */
    private long readLimitBytesPerSecond = 10 * 1024 * 1024L;

    /** Default SeaTunnel parallelism used by the initial snapshot. */
    private int snapshotParallelism = 1;

    /** Default MySQL CDC source connection pool size. */
    private int sourceConnectionLimit = 2;

    /** Platform hard limits. Requests above these values are rejected. */
    private int maxReadLimitRowsPerSecond = 100000;
    private long maxReadLimitBytesPerSecond = 1024 * 1024 * 1024L;
    private int maxSnapshotParallelism = 4;
    private int maxSourceConnectionLimit = 8;
}
