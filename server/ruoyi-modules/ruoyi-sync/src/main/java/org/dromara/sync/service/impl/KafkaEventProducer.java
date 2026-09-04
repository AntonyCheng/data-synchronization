package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
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
import java.util.concurrent.TimeUnit;

/** Publishes only normalized PRD events to Kafka with stable key partitioning. */
@Component
public class KafkaEventProducer {

    private final JsonMapper jsonMapper;
    private final SyncTaskMapper syncTaskMapper;

    @Autowired
    public KafkaEventProducer(JsonMapper jsonMapper, SyncTaskMapper syncTaskMapper) {
        this.jsonMapper = jsonMapper;
        this.syncTaskMapper = syncTaskMapper;
    }

    /** Constructor retained for envelope-only unit tests. */
    public KafkaEventProducer(JsonMapper jsonMapper) {
        this(jsonMapper, null);
    }

    /** Publish normalized events for a platform task and persist delivery metrics. */
    public List<PublishResult> publishForTask(Long taskId, String bootstrapServers, String topic,
                                               List<KafkaEventNormalizer.NormalizedEvent> events) {
        if (syncTaskMapper == null) throw new ServiceException("Kafka 任务发布器未配置任务存储");
        if (syncTaskMapper.selectById(taskId) == null) throw new ServiceException("同步任务不存在");
        List<PublishResult> results = publish(bootstrapServers, topic, events);
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
        if (bootstrapServers == null || bootstrapServers.isBlank()) throw new ServiceException("Kafka broker 地址不能为空");
        if (topic == null || topic.isBlank()) throw new ServiceException("Kafka topic 不能为空");
        if (events == null || events.isEmpty()) return List.of();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");
        properties.put("retries", Integer.toString(Integer.MAX_VALUE));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        List<PublishResult> results = new ArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (KafkaEventNormalizer.NormalizedEvent event : events) {
                String key = event.key().toString();
                RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, toEnvelope(event).toString()))
                    .get(30, TimeUnit.SECONDS);
                results.add(new PublishResult(topic, metadata.partition(), metadata.offset(), key));
            }
            producer.flush();
            return List.copyOf(results);
        } catch (Exception ex) {
            throw new ServiceException("Kafka 事件发布失败：" + safeMessage(ex));
        }
    }

    ObjectNode toEnvelope(KafkaEventNormalizer.NormalizedEvent event) {
        ObjectNode envelope = jsonMapper.createObjectNode();
        envelope.put("op", event.op());
        envelope.set("key", event.key());
        if (event.data() == null || event.data().isMissingNode()) envelope.putNull("data");
        else envelope.set("data", event.data());
        if (event.before() == null || event.before().isMissingNode()) envelope.putNull("before");
        else envelope.set("before", event.before());
        ObjectNode source = envelope.putObject("source");
        source.put("database", event.sourceDatabase());
        source.put("table", event.sourceTable());
        envelope.put("sourceEventTime", event.sourceEventTime());
        envelope.put("phase", event.phase());
        return envelope;
    }

    private static String safeMessage(Exception ex) {
        String message = ex.getMessage();
        return message == null || message.isBlank() ? ex.getClass().getSimpleName() : message.replaceAll("(?i)(password\\s*[=:]\\s*)[^,;\\s}]+", "$1******");
    }

    public record PublishResult(String topic, int partition, long offset, String key) {
    }
}
