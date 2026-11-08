package org.dromara.sync.kafka;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * A publish stops at the first event that cannot be published and says where, so the bridge can hold
 * its raw topic there instead of skipping it. The broker is a {@link MockProducer}.
 */
@Tag("dev")
class KafkaEventProducerTest {

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final List<MockProducer<String, String>> opened = new CopyOnWriteArrayList<>();

    @Test
    void anEventTheTopicWouldRefuseIsNotSentAndNeitherIsAnythingAfterIt() {
        KafkaEventProducer producer = producer(true);

        KafkaEventProducer.PublishOutcome outcome = producer.publish("kafka:9092", "t",
            List.of(event(1, "a"), event(2, "x".repeat(2000)), event(3, "c")), KafkaOutputFormat.ENVELOPE, 1500);

        assertTrue(outcome.failed());
        assertEquals(1, outcome.failedIndex());
        assertEquals(1, outcome.published().size());
        assertTrue(outcome.failure().contains("超过单条上限 1500 字节（目标 topic t 的 max.message.bytes）"), outcome.failure());
        assertTrue(outcome.failure().startsWith("同步键 {\"id\":2} 的事件约 "), outcome.failure());
        assertEquals(List.of("{\"id\":1}"), opened.getFirst().history().stream().map(record -> record.key()).toList());
    }

    @Test
    void thePlatformLimitAppliesWhenTheTopicsIsUnknownOrLarger() {
        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setKafkaMaxRecordBytes(2 * 1024 * 1024);
        KafkaEventProducer producer = producer(true, properties);

        assertEquals(2L * 1024 * 1024, producer.recordLimit(-1));
        assertEquals(2L * 1024 * 1024, producer.recordLimit(64L * 1024 * 1024));
        assertEquals(1000L, producer.recordLimit(1000));
        KafkaEventProducer.PublishOutcome outcome = producer.publish("kafka:9092", "t",
            List.of(event(1, "x".repeat(3 * 1024 * 1024))), KafkaOutputFormat.ENVELOPE, -1);
        assertTrue(outcome.failure().contains("sync.engine.kafka-max-record-bytes"), outcome.failure());
        assertTrue(opened.getFirst().history().isEmpty());
    }

    @Test
    void aSendTheBrokerFailsEndsTheOutcomeThere() throws Exception {
        KafkaEventProducer producer = producer(false);
        CompletableFuture<KafkaEventProducer.PublishOutcome> outcome = CompletableFuture.supplyAsync(() -> producer.publish(
            "kafka:9092", "t", List.of(event(1, "a"), event(2, "b"), event(3, "c")), KafkaOutputFormat.ENVELOPE, -1));
        MockProducer<String, String> broker = awaitProducer();
        awaitSends(broker, 3);

        assertTrue(broker.completeNext());
        assertTrue(broker.errorNext(new RecordTooLargeException("The request included a message larger than the max message size the server will accept.")));
        assertTrue(broker.completeNext());

        KafkaEventProducer.PublishOutcome result = outcome.get(5, TimeUnit.SECONDS);
        assertEquals(1, result.failedIndex());
        assertEquals(List.of("{\"id\":1}"), result.published().stream().map(KafkaEventProducer.PublishResult::key).toList());
        assertTrue(result.failure().startsWith("RecordTooLargeException：The request included"), result.failure());
        assertEquals(1, opened.size(), "an ordinary refusal keeps the shared producer");
    }

    @Test
    void aFatalErrorReplacesTheSharedProducer() throws Exception {
        KafkaEventProducer producer = producer(false);
        CompletableFuture<KafkaEventProducer.PublishOutcome> outcome = CompletableFuture.supplyAsync(() -> producer.publish(
            "kafka:9092", "t", List.of(event(1, "a")), KafkaOutputFormat.ENVELOPE, -1));
        MockProducer<String, String> broker = awaitProducer();
        awaitSends(broker, 1);
        assertTrue(broker.errorNext(new ClusterAuthorizationException("Cluster authorization failed.")));
        assertTrue(outcome.get(5, TimeUnit.SECONDS).failed());
        assertTrue(broker.closed(), "the broken producer is closed");

        CompletableFuture.runAsync(() -> producer.publish("kafka:9092", "t", List.of(event(2, "b")), KafkaOutputFormat.ENVELOPE, -1));
        for (int i = 0; i < 500 && opened.size() < 2; i++) Thread.sleep(10);
        assertEquals(2, opened.size(), "the next publish opens a new one");
    }

    @Test
    void anEmptyPublishSucceedsWithoutABroker() {
        KafkaEventProducer.PublishOutcome outcome = producer(true).publish("kafka:9092", "t", List.of(), KafkaOutputFormat.ENVELOPE, -1);
        assertFalse(outcome.failed());
        assertTrue(opened.isEmpty());
    }

    private KafkaEventProducer producer(boolean autoComplete) {
        return producer(autoComplete, new SeaTunnelProperties());
    }

    private KafkaEventProducer producer(boolean autoComplete, SeaTunnelProperties properties) {
        return new KafkaEventProducer(mock(SyncTaskMapper.class), new KafkaEventSerializer(mapper), properties) {
            @Override
            Producer<String, String> openProducer(String bootstrapServers) {
                MockProducer<String, String> broker = new MockProducer<>(autoComplete, new StringSerializer(), new StringSerializer());
                opened.add(broker);
                return broker;
            }
        };
    }

    private MockProducer<String, String> awaitProducer() throws InterruptedException {
        for (int i = 0; i < 500 && opened.isEmpty(); i++) Thread.sleep(10);
        return opened.getFirst();
    }

    private static void awaitSends(MockProducer<String, String> broker, int count) throws InterruptedException {
        for (int i = 0; i < 500 && broker.history().size() < count; i++) Thread.sleep(10);
        assertEquals(count, broker.history().size());
    }

    private KafkaEventNormalizer.NormalizedEvent event(long id, String name) {
        return new KafkaEventNormalizer.NormalizedEvent("INSERT", mapper.createObjectNode().put("id", id), null,
            mapper.createObjectNode().put("id", id).put("name", name), "CDC", "source_db", "customers", "2026-11-08T00:00:00Z");
    }
}
