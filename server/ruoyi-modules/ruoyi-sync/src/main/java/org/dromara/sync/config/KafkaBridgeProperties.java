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

    /**
     * First wait before a bridge that stopped publishing (an event its output topic refused, a broker
     * error, an unreadable raw record) tries the same event again. Each further failure doubles the
     * wait up to {@link #retryMaxBackoff}. The bridge never moves past an event it could not publish:
     * everything after it stays in the raw topic until it goes through.
     */
    private Duration retryInitialBackoff = Duration.ofSeconds(10);

    /** Longest wait between two attempts of a stopped bridge; see {@link #retryInitialBackoff}. */
    private Duration retryMaxBackoff = Duration.ofMinutes(5);
}
