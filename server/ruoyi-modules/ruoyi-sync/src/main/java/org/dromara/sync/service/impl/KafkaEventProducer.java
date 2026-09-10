package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Publishes only normalized PRD events to Kafka with stable key partitioning.
 * One {@link KafkaProducer} is kept alive per bootstrap-servers string and reused
 * across publish calls (KafkaProducer is thread-safe and is designed to be shared -
 * creating a new one per batch would pay the full connection/metadata-fetch cost on
 * every poll cycle of every running Kafka task, which is the dominant cost at any
 * real throughput).
 */
@Component
public class KafkaEventProducer {

    private final JsonMapper jsonMapper;
    private final SyncTaskMapper syncTaskMapper;
    private final KafkaEventSerializer serializer;
    private final ConcurrentMap<String, KafkaProducer<String, String>> producers = new ConcurrentHashMap<>();

    @Autowired
    public KafkaEventProducer(JsonMapper jsonMapper, SyncTaskMapper syncTaskMapper) {
        this.jsonMapper = jsonMapper;
        this.syncTaskMapper = syncTaskMapper;
        this.serializer = new KafkaEventSerializer(jsonMapper);
    }

    /** Constructor retained for envelope-only unit tests. */
    public KafkaEventProducer(JsonMapper jsonMapper) {
        this(jsonMapper, null);
    }

    /** Publish normalized events for a platform task and persist delivery metrics. */
    public List<PublishResult> publishForTask(Long taskId, String bootstrapServers, String topic,
                                               List<KafkaEventNormalizer.NormalizedEvent> events) {
        return publishForTask(taskId, bootstrapServers, topic, events, KafkaOutputFormat.ENVELOPE);
    }

    public List<PublishResult> publishForTask(Long taskId, String bootstrapServers, String topic,
                                               List<KafkaEventNormalizer.NormalizedEvent> events,
                                               KafkaOutputFormat format) {
        if (syncTaskMapper == null) throw new ServiceException("Kafka 任务发布器未配置任务存储");
        if (syncTaskMapper.selectById(taskId) == null) throw new ServiceException("同步任务不存在");
        List<PublishResult> results = publish(bootstrapServers, topic, events, format);
        persistMetrics(taskId, events, results);
        return results;
    }

    void persistMetrics(Long taskId, List<KafkaEventNormalizer.NormalizedEvent> events,
                                List<PublishResult> results) {
        if (results.isEmpty()) return;
        PublishResult last = results.get(results.size() - 1);
        KafkaEventNormalizer.NormalizedEvent lastEvent = events.get(events.size() - 1);
        LocalDateTime ack = LocalDateTime.now();
        LocalDateTime sourceTime = parseEventTime(lastEvent.sourceEventTime());
        Long lag = sourceTime == null ? null : Math.max(0L, java.time.Duration.between(sourceTime, ack).getSeconds());
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

    public List<PublishResult> publish(String bootstrapServers, String topic,
                                       List<KafkaEventNormalizer.NormalizedEvent> events) {
        return publish(bootstrapServers, topic, events, KafkaOutputFormat.ENVELOPE);
    }

    public List<PublishResult> publish(String bootstrapServers, String topic,
                                       List<KafkaEventNormalizer.NormalizedEvent> events,
                                       KafkaOutputFormat format) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) throw new ServiceException("Kafka broker 地址不能为空");
        if (topic == null || topic.isBlank()) throw new ServiceException("Kafka topic 不能为空");
        if (events == null || events.isEmpty()) return List.of();
        KafkaProducer<String, String> producer = producerFor(bootstrapServers);
        List<String> keys = new ArrayList<>(events.size());
        List<Future<RecordMetadata>> futures = new ArrayList<>(events.size());
        try {
            // Issue every send first (the producer pipelines them - idempotence keeps
            // per-partition ordering across the in-flight requests), then wait for acks.
            // Blocking on get() one send at a time here would serialize on network
            // round-trips and cap throughput far below what one producer can sustain.
            for (KafkaEventNormalizer.NormalizedEvent event : events) {
                String key = event.key().toString();
                keys.add(key);
                futures.add(producer.send(new ProducerRecord<>(topic, key, serializer.serialize(format, event))));
            }
            List<PublishResult> results = new ArrayList<>(events.size());
            for (int index = 0; index < futures.size(); index++) {
                RecordMetadata metadata = futures.get(index).get(30, TimeUnit.SECONDS);
                results.add(new PublishResult(topic, metadata.partition(), metadata.offset(), keys.get(index)));
            }
            return List.copyOf(results);
        } catch (Exception ex) {
            throw new ServiceException("Kafka 事件发布失败：" + safeMessage(ex));
        }
    }

    private KafkaProducer<String, String> producerFor(String bootstrapServers) {
        return producers.computeIfAbsent(bootstrapServers, KafkaEventProducer::createProducer);
    }

    private static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");
        properties.put("retries", Integer.toString(Integer.MAX_VALUE));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(properties);
    }

    @PreDestroy
    void close() {
        producers.values().forEach(producer -> producer.close(java.time.Duration.ofSeconds(5)));
        producers.clear();
    }

    ObjectNode toEnvelope(KafkaEventNormalizer.NormalizedEvent event) {
        return serializer.envelope(event);
    }

    private static String safeMessage(Exception ex) {
        String message = ex.getMessage();
        return message == null || message.isBlank() ? ex.getClass().getSimpleName() : message.replaceAll("(?i)(password\\s*[=:]\\s*)[^,;\\s}]+", "$1******");
    }

    public record PublishResult(String topic, int partition, long offset, String key) {
    }
}
