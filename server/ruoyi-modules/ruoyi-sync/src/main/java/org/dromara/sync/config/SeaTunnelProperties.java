package org.dromara.sync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Runtime settings for the SeaTunnel job adapter. */
@Data
@Component
@ConfigurationProperties(prefix = "sync.engine")
public class SeaTunnelProperties {

    /** SeaTunnel REST API base URL. */
    private String endpoint = "http://localhost:18080";

    /** Connect + read timeout for every call to the SeaTunnel REST API. */
    private Duration requestTimeout = Duration.ofSeconds(10);

    /**
     * How many jobs a group status refresh polls at once ({@code EngineJobRunner.pollAll}). Each
     * poll is two REST calls, so a 20-table group is 40 calls per pass; on a slow engine every
     * one of them can take the whole {@link #requestTimeout}, and polled one after another that
     * held the group lock for minutes.
     */
    private int pollParallelism = 4;

    /** Checkpoint interval written into generated jobs. */
    private int checkpointIntervalMs = 5000;

    /**
     * The engine's checkpoint storage directory as this process sees it - the engine's
     * {@code checkpoint.storage} namespace (one sub-directory per job id), mounted read-only into
     * the backend by the production deployment. The engine keeps no job state across a restart
     * while these files stay, so they are what a resume restores from once the engine no longer
     * remembers the job ({@code EngineCheckpointFiles}). Blank or not readable: such a resume is
     * refused as before - never resumed from an unknown position.
     */
    private String checkpointDir = "";

    /** Maximum source snapshot throughput for generated jobs. */
    private int readLimitRowsPerSecond = 1000;

    /** Default source snapshot byte throughput in bytes per second. */
    private long readLimitBytesPerSecond = 10 * 1024 * 1024L;

    /** Default SeaTunnel parallelism used by the initial snapshot. */
    private int snapshotParallelism = 1;

    /** Default MySQL CDC source connection pool size. */
    private int sourceConnectionLimit = 2;

    /**
     * Largest Kafka record, in bytes, the platform writes: the engine's raw-topic records (its Kafka
     * sink's {@code max.request.size}, and the {@code max.message.bytes} the bridge gives the raw topic)
     * and the bridge's published events (its producer's {@code max.request.size}; the output topic's own
     * {@code max.message.bytes} applies on top). Kafka's 1 MB default would refuse a row with a large
     * TEXT / BLOB / JSON value - a raw record also carries the value schema and, for an UPDATE, both
     * images. A larger record fails the engine job (or holds the bridge) instead of being dropped. Not
     * part of the config fingerprint: changing it never strands a savepoint.
     */
    private int kafkaMaxRecordBytes = 16 * 1024 * 1024;

    /** Addresses as seen from the SeaTunnel runtime rather than the platform process. */
    private Map<String, String> connectionEndpointOverrides = new HashMap<>();

    /** Platform hard limits. Requests above these values are rejected. */
    private int maxReadLimitRowsPerSecond = 100000;
    private long maxReadLimitBytesPerSecond = 1024 * 1024 * 1024L;
    private int maxSnapshotParallelism = 4;
    private int maxSourceConnectionLimit = 8;

    public String resolveEngineEndpoint(String host, Integer port) {
        String endpoint = host + ':' + port;
        String override = connectionEndpointOverrides.get(endpoint);
        return override == null || override.isBlank() ? endpoint : override.trim();
    }
}
