package org.dromara.sync.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.domain.SyncTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * What a real bridge worker does when publishing stops and when its raw-topic position is gone. The
 * raw topic is scripted: its records, the first offset retention kept, and the group's committed
 * offset, which every consumer session of the worker (one per retry) sees. The producer and the
 * metadata database are mocks; the normalizer is the real one.
 */
@Tag("dev")
class KafkaTaskBridgeDeliveryTest {

    private static final long TASK_ID = 7L;
    private static final String RAW_TOPIC = "__ds_raw_" + TASK_ID + "_v1";
    private static final TopicPartition PARTITION = new TopicPartition(RAW_TOPIC, 0);

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final KafkaEventProducer producer = mock(KafkaEventProducer.class);
    private final KafkaBridgeStateStore stateStore = mock(KafkaBridgeStateStore.class);
    /** The keys ({@code id}) of the events of every publish call, in order. */
    private final List<List<Long>> publishes = new CopyOnWriteArrayList<>();
    private final List<Long> commits = new CopyOnWriteArrayList<>();
    private final List<String> values = new ArrayList<>();
    private volatile long retainedFrom;
    private volatile Long committed;
    private KafkaTaskBridgeService bridge;

    @AfterEach
    void closeBridge() {
        if (bridge != null) bridge.close();
    }

    @Test
    void aFailedEventHoldsTheRawTopicThereAndIsRetriedWithoutRepublishingWhatWentOut() {
        rawTopic(3, 0);
        AtomicInteger calls = new AtomicInteger();
        when(producer.publishForTask(anyLong(), anyString(), anyString(), anyList(), any(), anyLong())).thenAnswer(invocation -> {
            List<KafkaEventNormalizer.NormalizedEvent> events = invocation.getArgument(3);
            publishes.add(events.stream().map(event -> event.key().path("id").asLong()).toList());
            if (calls.incrementAndGet() == 1) {
                return new KafkaEventProducer.PublishOutcome(List.of(new KafkaEventProducer.PublishResult("events", 0, 0, "{\"id\":0}")),
                    1, "RecordTooLargeException：The request included a message larger than the max message size the server will accept.");
            }
            return KafkaEventProducer.PublishOutcome.success(List.of());
        });

        start(false);

        verify(stateStore, timeout(5000)).reportBlocked(eq(KafkaRawTopic.OWNER_TASK), eq(TASK_ID),
            argThat(reason -> reason.startsWith("Kafka 桥接已暂停发布：停在 raw offset 1，RecordTooLargeException")
                && reason.endsWith("（raw topic 积压 2 条）")), any(), eq(2L));
        // Once by the operator start, once when the retry got the event out.
        verify(stateStore, timeout(5000).times(2)).clearBlocked(KafkaRawTopic.OWNER_TASK, TASK_ID);
        // Event 0 went out once; the retry starts at the failed event 1, nothing is skipped.
        assertEquals(List.of(List.of(0L, 1L, 2L), List.of(1L, 2L)), publishes);
        assertEquals(List.of(1L, 3L), commits);
    }

    @Test
    void anUnreadableRawRecordStopsTheBridgeRightBeforeIt() {
        rawTopic(3, 0);
        values.set(1, "{not json");
        when(producer.publishForTask(anyLong(), anyString(), anyString(), anyList(), any(), anyLong())).thenAnswer(invocation -> {
            List<KafkaEventNormalizer.NormalizedEvent> events = invocation.getArgument(3);
            publishes.add(events.stream().map(event -> event.key().path("id").asLong()).toList());
            return KafkaEventProducer.PublishOutcome.success(List.of());
        });

        start(false);

        verify(stateStore, timeout(5000).atLeast(2)).reportBlocked(eq(KafkaRawTopic.OWNER_TASK), eq(TASK_ID),
            argThat(reason -> reason.startsWith("Kafka 桥接已暂停发布：停在 raw offset 1，raw 记录无法解析")), any(), eq(2L));
        // Every retry stops at the same record and publishes nothing behind it.
        assertEquals(List.of(List.of(0L)), publishes);
        assertEquals(List.of(1L), commits);
    }

    @Test
    void aPositionRetentionDeletedIsReportedNotSkipped() {
        rawTopic(7, 5);
        committed = 2L;
        when(stateStore.rawPosition(isNull(), eq(RAW_TOPIC))).thenReturn(new KafkaBridgeStateStore.RawPosition(2L, false));

        start(false);

        verify(stateStore, timeout(5000)).reportBlocked(eq(KafkaRawTopic.OWNER_TASK), eq(TASK_ID), argThat(reason ->
            KafkaTaskBridgeService.isRawTopicGap(reason) && reason.contains("offset 2") && reason.contains("offset 5")
                && reason.contains("其间 3 条变更已被 Kafka 保留策略删除")), any(), isNull());
        verify(producer, after(300).never()).publishForTask(anyLong(), anyString(), anyString(), anyList(), any(), anyLong());
        assertEquals(List.of(), commits);
    }

    @Test
    void aFreshRunStartsAtTheEarliestRetainedRecord() {
        rawTopic(7, 5);
        committed = 2L;
        when(stateStore.rawPosition(isNull(), eq(RAW_TOPIC))).thenReturn(new KafkaBridgeStateStore.RawPosition(2L, true));
        recordPublishes();

        start(true);

        verify(stateStore, timeout(5000)).consumeReset(null, RAW_TOPIC);
        awaitPublished(List.of(5L, 6L));
        assertEquals(List.of(5L, 7L), commits);
        verify(stateStore, never()).reportBlocked(any(), any(), any(), any(), any());
    }

    @Test
    void expiredGroupOffsetsResumeAtThePositionThePlatformRecorded() {
        rawTopic(5, 0);
        committed = null;
        when(stateStore.rawPosition(isNull(), eq(RAW_TOPIC))).thenReturn(new KafkaBridgeStateStore.RawPosition(3L, false));
        recordPublishes();

        start(false);

        awaitPublished(List.of(3L, 4L));
        assertEquals(List.of(3L, 5L), commits);
    }

    @Test
    void aTopicNeverConsumedStartsAtItsFirstRecord() {
        rawTopic(2, 0);
        committed = null;
        when(stateStore.rawPosition(isNull(), eq(RAW_TOPIC))).thenReturn(new KafkaBridgeStateStore.RawPosition(null, false));
        recordPublishes();

        start(false);

        awaitPublished(List.of(0L, 1L));
    }

    @Test
    void noPositionAnywhereWhileRetentionAlreadyDeletedRecordsIsAGap() {
        rawTopic(6, 3);
        committed = null;
        when(stateStore.rawPosition(isNull(), eq(RAW_TOPIC))).thenReturn(new KafkaBridgeStateStore.RawPosition(null, false));

        start(false);

        verify(stateStore, timeout(5000)).reportBlocked(eq(KafkaRawTopic.OWNER_TASK), eq(TASK_ID), argThat(reason ->
            KafkaTaskBridgeService.isRawTopicGap(reason) && reason.contains("前 3 条记录已被保留策略删除")), any(), isNull());
        verify(producer, after(300).never()).publishForTask(anyLong(), anyString(), anyString(), anyList(), any(), anyLong());
    }

    @Test
    void theRegistryCopyOfTheCommittedOffsetIsWritten() {
        rawTopic(2, 0);
        committed = 0L;
        recordPublishes();

        start(false);

        awaitPublished(List.of(0L, 1L));
        verify(stateStore, timeout(5000)).saveOffset(null, RAW_TOPIC, 2L);
    }

    // ------------------------------------------------------------------ fixture

    /** {@code count} INSERT records with ids 0.., retention having deleted those before {@code retainedFrom}. */
    private void rawTopic(int count, long retainedFrom) {
        for (int id = 0; id < count; id++) {
            values.add("{\"before\":null,\"after\":{\"id\":" + id + ",\"name\":\"row-" + id + "\"},\"op\":\"c\","
                + "\"source\":{\"schema\":null,\"database\":\"source_db\",\"table\":\"customers\"},\"ts_ms\":1790258542124}");
        }
        this.retainedFrom = retainedFrom;
        this.committed = retainedFrom;
    }

    private void recordPublishes() {
        when(producer.publishForTask(anyLong(), anyString(), anyString(), anyList(), any(), anyLong())).thenAnswer(invocation -> {
            List<KafkaEventNormalizer.NormalizedEvent> events = invocation.getArgument(3);
            publishes.add(events.stream().map(event -> event.key().path("id").asLong()).toList());
            return KafkaEventProducer.PublishOutcome.success(List.of());
        });
    }

    private void awaitPublished(List<Long> expected) {
        for (int i = 0; i < 500 && publishes.stream().mapToInt(List::size).sum() < expected.size(); i++) sleep(10);
        assertEquals(expected, publishes.stream().flatMap(List::stream).toList());
    }

    private void start(boolean freshRun) {
        KafkaBridgeProperties properties = new KafkaBridgeProperties();
        properties.setRetryInitialBackoff(Duration.ofMillis(100));
        properties.setRetryMaxBackoff(Duration.ofMillis(200));
        bridge = new KafkaTaskBridgeService(new KafkaEventNormalizer(mapper), producer, mapper, properties,
            new SeaTunnelProperties(), stateStore) {
            @Override
            void ensureTopics(SyncTask task, DataSource target, String ownerType) {
            }

            @Override
            long targetTopicMaxMessageBytes(String bootstrapServers, String topic) {
                return -1;
            }

            @Override
            Consumer<String, String> openConsumer(Properties consumerProperties) {
                assertEquals("none", consumerProperties.getProperty("auto.offset.reset"), "never a silent jump");
                return new Session();
            }
        };
        SyncTask task = new SyncTask();
        task.setTaskId(TASK_ID);
        task.setSourceTable("customers");
        task.setTargetTable("events");
        task.setSelectedColumns("id,name");
        task.setSyncKeyColumns("id");
        task.setConfigVersion(1);
        task.setKafkaOutputFormat(KafkaOutputFormat.ENVELOPE.name());
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setDatabaseName("source_db");
        DataSource kafka = new DataSource();
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        bridge.start(task, kafka, source, freshRun);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * One consumer session on the scripted raw topic. Like a real consumer with
     * {@code auto.offset.reset=none}: no committed offset fails the poll with NoOffsetForPartitionException,
     * a committed offset retention deleted fails it with OffsetOutOfRangeException.
     */
    private final class Session extends MockConsumer<String, String> {

        private Session() {
            super(OffsetResetStrategy.NONE);
            schedulePollTask(() -> {
                rebalance(List.of(PARTITION));
                updateBeginningOffsets(Map.of(PARTITION, retainedFrom));
                updateEndOffsets(Map.of(PARTITION, (long) values.size()));
                if (committed != null) super.commitSync(Map.of(PARTITION, new OffsetAndMetadata(committed)));
                for (long offset = retainedFrom; offset < values.size(); offset++) {
                    addRecord(new ConsumerRecord<>(RAW_TOPIC, 0, offset, null, values.get((int) offset)));
                }
            });
        }

        @Override
        public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            super.commitSync(offsets);
            committed = offsets.get(PARTITION).offset();
            commits.add(committed);
        }

        @Override
        public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
            ConsumerRecords<String, String> records = super.poll(timeout);
            if (records.isEmpty()) sleep(10);
            return records;
        }
    }
}
