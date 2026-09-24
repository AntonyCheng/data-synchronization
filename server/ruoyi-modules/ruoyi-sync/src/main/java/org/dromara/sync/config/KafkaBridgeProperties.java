package org.dromara.sync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Limits of the in-process Kafka bridge ({@code KafkaTaskBridgeService}). */
@Data
@Component
@ConfigurationProperties(prefix = "sync.kafka-bridge")
public class KafkaBridgeProperties {

    /**
     * Most bridge workers one backend instance runs. Every RUNNING Kafka-target task / group
     * item holds one worker on every instance (all but one stand by in its consumer group), so
     * this is in effect the cluster-wide cap on concurrently running Kafka targets - keep it the
     * same on all instances. A worker is one thread plus one KafkaConsumer (its heartbeat thread
     * and up to ~1 MiB of fetch buffer for the single-partition raw topic); producers are shared
     * per Kafka cluster. 64 covers three whole-database groups at the 20-table cap plus a few
     * single tasks for roughly 130 threads and well under 100 MiB of buffers.
     */
    private int maxWorkers = 64;
}
