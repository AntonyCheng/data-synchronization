package org.dromara.sync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Limits of the in-process Kafka bridge ({@code KafkaTaskBridgeService}) and its raw-topic cleanup. */
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

    /**
     * How long a raw topic stays after the janitor first found no owner using it, before it is
     * deleted. Owners only move to a new raw topic while their job is stopped, but a bridge worker
     * on another instance may still be draining the old one until its reconciler retires it
     * (within one reconcile interval, 30 s); the grace covers that with a wide margin. A deleted
     * topic's registry row is kept for the same time, to catch a lingering client re-creating it.
     */
    private Duration rawTopicRetireGrace = Duration.ofMinutes(10);
}
