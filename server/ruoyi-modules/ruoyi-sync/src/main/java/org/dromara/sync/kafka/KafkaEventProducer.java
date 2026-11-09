package org.dromara.sync.kafka;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.SyncText;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Publishes normalized events to Kafka with stable key partitioning.
 * One {@link KafkaProducer} is kept alive per bootstrap-servers string and reused
 * across publish calls (KafkaProducer is thread-safe and is designed to be shared -
 * creating a new one per batch would pay the full connection/metadata-fetch cost on
 * every poll cycle of every running Kafka task, which is the dominant cost at any
 * real throughput).
 *
 * <p>A publish never skips an event: it stops at the first one that cannot be published and
 * reports where ({@link PublishOutcome}), so the bridge can hold its raw topic there.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

    /**
     * What a record adds to its key and value inside a single-record batch (batch header plus
     * record framing), rounded up. A record within {@code limit - RECORD_OVERHEAD_BYTES} passes both
     * the producer's {@code max.request.size} check and the topic's {@code max.message.bytes}.
     */
    static final int RECORD_OVERHEAD_BYTES = 128;
    private static final int SEND_TIMEOUT_SECONDS = 30;
    private static final int KAFKA_DEFAULT_MAX_BYTES = 1024 * 1024;

    private final SyncTaskMapper syncTaskMapper;
    private final KafkaEventSerializer serializer;
    private final SeaTunnelProperties properties;
    /** One producer per cluster connection: the same brokers with other credentials are another connection. */
    private final ConcurrentMap<KafkaConnection, Producer<String, String>> producers = new ConcurrentHashMap<>();

    /**
     * Publish normalized events for a platform task and persist delivery metrics of what got
     * through onto its row. {@code topicMaxMessageBytes} is the output topic's limit, {@code <= 0}
     * when unknown.
     */
    public PublishOutcome publishForTask(Long taskId, KafkaConnection connection, String topic,
                                         List<KafkaEventNormalizer.NormalizedEvent> events,
                                         KafkaOutputFormat format, long topicMaxMessageBytes) {
        if (syncTaskMapper.selectById(taskId) == null) throw new ServiceException("同步任务不存在");
        PublishOutcome outcome = publish(connection, topic, events, format, topicMaxMessageBytes);
        if (!outcome.published().isEmpty()) {
            try {
                persistMetrics(taskId, events.subList(0, outcome.published().size()), outcome.published());
            } catch (RuntimeException ex) {
                // The events are on the broker already; failing here would get them published twice.
                log.warn("Kafka publish metrics of task {} not saved: {}", taskId, ex.getMessage());
            }
        }
        return outcome;
    }

    void persistMetrics(Long taskId, List<KafkaEventNormalizer.NormalizedEvent> events, List<PublishResult> results) {
        if (results.isEmpty()) return;
        PublishResult last = results.get(results.size() - 1);
        KafkaEventNormalizer.NormalizedEvent lastEvent = events.get(events.size() - 1);
        LocalDateTime ack = LocalDateTime.now();
        LocalDateTime sourceTime = parseEventTime(lastEvent.sourceEventTime());
        Long lag = sourceTime == null ? null : Math.max(0L, Duration.between(sourceTime, ack).getSeconds());
        UpdateWrapper<SyncTask> update = new UpdateWrapper<SyncTask>()
            .eq("task_id", taskId)
            .setSql("kafka_published_count = coalesce(kafka_published_count, 0) + " + results.size())
            .set("kafka_last_partition", last.partition())
            .set("kafka_last_offset", last.offset())
            .set("kafka_last_source_event_time", sourceTime)
            .set("kafka_last_broker_ack_time", ack)
            .set("kafka_lag_seconds", lag);
        syncTaskMapper.update(null, update);
    }

    private static LocalDateTime parseEventTime(String value) {
        try {
            return value == null ? null : LocalDateTime.ofInstant(Instant.parse(value), ZoneId.systemDefault());
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    /**
     * Publishes {@code events} in order and stops at the first one that cannot be published. An event
     * larger than {@link #recordLimit} - or one that fails to serialize - is refused before it or anything
     * after it is sent. A send the broker fails ends the outcome at that event: everything before
     * {@link PublishOutcome#failedIndex()} has a broker ack, nothing from there on counts as published.
     * Sends issued after a failed one may still have landed (they are pipelined), so the caller
     * publishes them again - at least once, as everywhere on this path.
     */
    public PublishOutcome publish(KafkaConnection connection, String topic,
                                  List<KafkaEventNormalizer.NormalizedEvent> events,
                                  KafkaOutputFormat format, long topicMaxMessageBytes) {
        if (connection == null || connection.bootstrapServers() == null || connection.bootstrapServers().isBlank()) {
            throw new ServiceException("Kafka broker 地址不能为空");
        }
        if (topic == null || topic.isBlank()) throw new ServiceException("Kafka topic 不能为空");
        if (events == null || events.isEmpty()) return PublishOutcome.success(List.of());
        long limit = recordLimit(topicMaxMessageBytes);
        List<String> keys = new ArrayList<>(events.size());
        List<String> values = new ArrayList<>(events.size());
        String refusal = null;
        for (KafkaEventNormalizer.NormalizedEvent event : events) {
            String key = event.key().toString();
            String value;
            try {
                value = serializer.serialize(format, event);
            } catch (RuntimeException ex) {
                refusal = "同步键 " + key + " 的事件无法序列化：" + SyncText.safeMessage(ex, ex.getClass().getSimpleName());
                break;
            }
            long size = utf8Length(key) + utf8Length(value) + RECORD_OVERHEAD_BYTES;
            if (size > limit) {
                refusal = "同步键 " + key + " 的事件约 " + size + " 字节，超过单条上限 " + limit + " 字节（"
                    + describeLimit(topic, topicMaxMessageBytes) + "）";
                break;
            }
            keys.add(key);
            values.add(value);
        }
        Producer<String, String> producer = producerFor(connection);
        // Issue every send first (the producer pipelines them - idempotence keeps per-partition
        // ordering across the in-flight requests), then wait for acks in order. Blocking on get()
        // one send at a time would serialize on network round-trips.
        List<Future<RecordMetadata>> futures = new ArrayList<>(keys.size());
        for (int index = 0; index < keys.size(); index++) {
            try {
                futures.add(producer.send(new ProducerRecord<>(topic, keys.get(index), values.get(index))));
            } catch (RuntimeException ex) {
                evictIfFatal(connection, producer, ex);
                refusal = describe(ex);
                keys.subList(index, keys.size()).clear();
                break;
            }
        }
        List<PublishResult> results = new ArrayList<>(futures.size());
        for (int index = 0; index < futures.size(); index++) {
            try {
                RecordMetadata metadata = futures.get(index).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                results.add(new PublishResult(topic, metadata.partition(), metadata.offset(), keys.get(index)));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return new PublishOutcome(List.copyOf(results), index, "发布被中断");
            } catch (Exception ex) {
                evictIfFatal(connection, producer, ex);
                return new PublishOutcome(List.copyOf(results), index, describe(ex));
            }
        }
        return refusal == null ? PublishOutcome.success(List.copyOf(results)) : new PublishOutcome(List.copyOf(results), results.size(), refusal);
    }

    /**
     * Largest record (key + value + {@link #RECORD_OVERHEAD_BYTES}) a publish sends: the producer's own
     * {@code max.request.size}, or the output topic's {@code max.message.bytes} when that is smaller.
     */
    long recordLimit(long topicMaxMessageBytes) {
        long producerLimit = producerMaxRequestSize();
        return topicMaxMessageBytes > 0 ? Math.min(producerLimit, topicMaxMessageBytes) : producerLimit;
    }

    private String describeLimit(String topic, long topicMaxMessageBytes) {
        if (topicMaxMessageBytes > 0 && topicMaxMessageBytes <= producerMaxRequestSize()) {
            return "目标 topic " + topic + " 的 max.message.bytes";
        }
        return "平台的 sync.engine.kafka-max-record-bytes";
    }

    private int producerMaxRequestSize() {
        return Math.max(KAFKA_DEFAULT_MAX_BYTES, properties.getKafkaMaxRecordBytes());
    }

    private static long utf8Length(String text) {
        return text == null ? 0 : text.getBytes(StandardCharsets.UTF_8).length;
    }

    /** {@code RecordTooLargeException：The request included a message larger than…}, unwrapped from the future. */
    static String describe(Throwable ex) {
        Throwable cause = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
        return cause.getClass().getSimpleName() + "：" + SyncText.safeMessage(cause, "无详细信息");
    }

    /**
     * A shared producer that hit a fatal error refuses every later send - for every owner on that
     * cluster - until it is replaced, so it is dropped here and the next publish opens a new one.
     */
    private void evictIfFatal(KafkaConnection connection, Producer<String, String> producer, Throwable ex) {
        Throwable cause = ex instanceof ExecutionException && ex.getCause() != null ? ex.getCause() : ex;
        boolean fatal = cause instanceof AuthenticationException || cause instanceof ClusterAuthorizationException
            || cause instanceof UnsupportedVersionException || cause instanceof ProducerFencedException
            || cause instanceof InvalidProducerEpochException || cause instanceof IllegalStateException
            || cause instanceof KafkaException && String.valueOf(cause.getMessage()).contains("previous transactional or idempotent request has failed");
        if (!fatal || !producers.remove(connection, producer)) return;
        log.warn("Kafka producer for {} replaced after a fatal error: {}", connection, describe(cause));
        try {
            producer.close(Duration.ZERO);
        } catch (RuntimeException ignored) {
            // Closing a producer that is already broken is best effort.
        }
    }

    private Producer<String, String> producerFor(KafkaConnection connection) {
        return producers.computeIfAbsent(connection, this::openProducer);
    }

    /** Package-private for tests. */
    Producer<String, String> openProducer(KafkaConnection connection) {
        Properties config = connection.clientProperties();
        config.put("acks", "all");
        config.put("enable.idempotence", "true");
        config.put("retries", Integer.toString(Integer.MAX_VALUE));
        config.put("max.request.size", Integer.toString(producerMaxRequestSize()));
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(config);
    }

    @PreDestroy
    void close() {
        producers.values().forEach(producer -> producer.close(Duration.ofSeconds(5)));
        producers.clear();
    }

    public record PublishResult(String topic, int partition, long offset, String key) {
    }

    /**
     * {@code published} holds a broker ack for each event before {@code failedIndex} (all of them on
     * success, {@code failedIndex} = -1); {@code failure} says why the event at {@code failedIndex}
     * could not be published.
     */
    public record PublishOutcome(List<PublishResult> published, int failedIndex, String failure) {

        public static PublishOutcome success(List<PublishResult> published) {
            return new PublishOutcome(published, -1, null);
        }

        public boolean failed() {
            return failedIndex >= 0;
        }
    }
}
