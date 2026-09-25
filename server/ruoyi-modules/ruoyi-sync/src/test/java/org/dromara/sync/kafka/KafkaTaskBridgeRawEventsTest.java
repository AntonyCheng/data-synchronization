package org.dromara.sync.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.dromara.sync.config.KafkaBridgeProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.KafkaRawTopicMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Phase labelling of a real bridge worker fed raw records of the format CDC jobs were generated with
 * before {@code compatible_debezium_json}: SeaTunnel's {@code DEBEZIUM_JSON} rows. A job started
 * before an upgrade keeps writing them until it is reinitialized, so the bridge still reads them, and
 * their initial load stays CDC. The MySQL-CDC records below were copied from the raw topic of a
 * FULL_CDC MySQL-CDC -> Kafka ({@code format = "DEBEZIUM_JSON"}) job on the local stack (2026-09-24):
 * three rows present before the job started (initial load), then an INSERT, an UPDATE and a DELETE.
 * Only the broker is replaced - the normalizer and serializer are the real ones. The current format is
 * covered by {@link KafkaRawFormatFidelityTest}.
 */
@Tag("dev")
class KafkaTaskBridgeRawEventsTest {

    private static final long TASK_ID = 7L;
    private static final String RAW_TOPIC = "__ds_raw_" + TASK_ID + "_v1";
    private static final String SOURCE = "\"source\":{\"schema\":null,\"database\":\"source_db\",\"table\":\"kp_phase_small\"}";

    /** Initial-load rows: op=c (never r), ts_ms = the moment the snapshot read them. */
    private static final List<String> INITIAL_LOAD = List.of(
        "{\"before\":null,\"after\":{\"id\":1,\"name\":\"snap-1\",\"qty\":10,\"amount\":1.5,\"created_at\":\"2026-01-01T00:00:01.123\"},"
            + "\"op\":\"c\"," + SOURCE + ",\"ts_ms\":1790258500371}",
        "{\"before\":null,\"after\":{\"id\":2,\"name\":\"snap-2\",\"qty\":20,\"amount\":2.5,\"created_at\":\"2026-01-01T00:00:02\"},"
            + "\"op\":\"c\"," + SOURCE + ",\"ts_ms\":1790258500371}",
        "{\"before\":null,\"after\":{\"id\":3,\"name\":\"snap-3\",\"qty\":null,\"amount\":null,\"created_at\":null},"
            + "\"op\":\"c\"," + SOURCE + ",\"ts_ms\":1790258500371}");

    /** Binlog changes: the INSERT has the same shape as an initial-load row; UPDATE is a d + c pair. */
    private static final List<String> BINLOG = List.of(
        "{\"before\":null,\"after\":{\"id\":4,\"name\":\"cdc-4\",\"qty\":40,\"amount\":4.5,\"created_at\":\"2026-01-01T00:00:04.5\"},"
            + "\"op\":\"c\"," + SOURCE + ",\"ts_ms\":1790258542124}",
        "{\"before\":{\"id\":1,\"name\":\"snap-1\",\"qty\":10,\"amount\":1.5,\"created_at\":\"2026-01-01T00:00:01.123\"},\"after\":null,"
            + "\"op\":\"d\"," + SOURCE + ",\"ts_ms\":1790258542147}",
        "{\"before\":null,\"after\":{\"id\":1,\"name\":\"upd-1\",\"qty\":11,\"amount\":1.5,\"created_at\":\"2026-01-01T00:00:01.123\"},"
            + "\"op\":\"c\"," + SOURCE + ",\"ts_ms\":1790258542147}",
        "{\"before\":{\"id\":2,\"name\":\"snap-2\",\"qty\":20,\"amount\":2.5,\"created_at\":\"2026-01-01T00:00:02\"},\"after\":null,"
            + "\"op\":\"d\"," + SOURCE + ",\"ts_ms\":1790258542176}");

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final KafkaEventProducer producer = mock(KafkaEventProducer.class);
    private final KafkaEventSerializer serializer = new KafkaEventSerializer(mapper);
    private KafkaTaskBridgeService bridge;

    @AfterEach
    void closeBridge() {
        if (bridge != null) bridge.close();
    }

    @Test
    void debeziumJsonInitialLoadRowsArePublishedAsCdcBecauseSeaTunnelWritesThemAsBinlogInserts() {
        List<String> raw = new ArrayList<>(INITIAL_LOAD);
        raw.addAll(BINLOG);
        List<KafkaEventNormalizer.NormalizedEvent> events = bridgeAndCollect(raw, 2);

        assertEquals(List.of("INSERT#1", "INSERT#2", "INSERT#3", "INSERT#4", "UPDATE#1", "DELETE#2"),
            events.stream().map(e -> e.op() + "#" + e.key().path("id").asText()).toList());
        // Nothing in an initial-load row tells it apart from the binlog INSERT of id 4.
        assertTrue(events.stream().allMatch(e -> "CDC".equals(e.phase())), "every MySQL-CDC event is CDC: " + events);
        KafkaEventNormalizer.NormalizedEvent initialLoad = events.getFirst();
        KafkaEventNormalizer.NormalizedEvent binlogInsert = events.get(3);
        assertEquals(binlogInsert.op(), initialLoad.op());
        assertEquals(binlogInsert.phase(), initialLoad.phase());
        assertNull(initialLoad.before());
        assertEquals("kp_phase_small", initialLoad.sourceTable());

        // sourceEventTime is the raw ts_ms: when the engine captured the row, not a commit time.
        assertEquals("2026-09-24T14:01:40.371Z", initialLoad.sourceEventTime());
        assertEquals("2026-09-24T14:02:22.124Z", binlogInsert.sourceEventTime());

        KafkaEventNormalizer.NormalizedEvent update = events.get(4);
        assertEquals("snap-1", update.before().path("name").asText(), "the d + c pair carries a full before image");
        assertEquals("upd-1", update.data().path("name").asText());
        assertEquals("snap-2", events.get(5).data().path("name").asText());

        // No format claims a snapshot for an initial-load row.
        JsonNode debezium = mapper.readTree(serializer.serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON, initialLoad));
        assertEquals("c", debezium.path("op").asText());
        assertFalse(debezium.path("source").has("snapshot"));
        assertEquals("CDC", serializer.envelope(initialLoad).path("phase").asText());
        assertEquals("INSERT", mapper.readTree(serializer.serialize(KafkaOutputFormat.CANAL_JSON, initialLoad)).path("type").asText());
        assertEquals("I", mapper.readTree(serializer.serialize(KafkaOutputFormat.OGG_JSON, initialLoad)).path("op_type").asText());
    }

    @Test
    void fullModeRowsWithoutOpAreTheOnlySnapshotPhase() {
        // A FULL task's engine job is a Jdbc source with the plain JSON format: bare rows, no op.
        List<KafkaEventNormalizer.NormalizedEvent> events = bridgeAndCollect(List.of(
            "{\"id\":1,\"name\":\"snap-1\",\"qty\":10,\"amount\":1.5,\"created_at\":\"2026-01-01T00:00:01.123\"}",
            "{\"id\":2,\"name\":\"snap-2\",\"qty\":20,\"amount\":2.5,\"created_at\":\"2026-01-01T00:00:02\"}"), 1);

        assertEquals(2, events.size());
        assertTrue(events.stream().allMatch(e -> "INSERT".equals(e.op()) && "SNAPSHOT".equals(e.phase())), events.toString());
        JsonNode debezium = mapper.readTree(serializer.serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON, events.getFirst()));
        assertEquals("r", debezium.path("op").asText());
        assertEquals("true", debezium.path("source").path("snapshot").asText());
    }

    /** Runs a real worker over {@code rawValues} and returns everything it handed the producer. */
    @SuppressWarnings("unchecked")
    private List<KafkaEventNormalizer.NormalizedEvent> bridgeAndCollect(List<String> rawValues, int expectedPublishes) {
        bridge = new KafkaTaskBridgeService(new KafkaEventNormalizer(mapper), producer, mapper, new KafkaBridgeProperties(),
            mock(KafkaRawTopicMapper.class)) {
            @Override
            void ensureTopics(SyncTask task, DataSource target, String ownerType) {
            }

            @Override
            Consumer<String, String> openConsumer(Properties properties) {
                return new RawTopic(rawValues);
            }
        };
        bridge.start(task(), kafka(), mysql());

        ArgumentCaptor<List<KafkaEventNormalizer.NormalizedEvent>> published = ArgumentCaptor.forClass(List.class);
        verify(producer, timeout(5000).times(expectedPublishes)).publishForTask(
            eq(TASK_ID), anyString(), eq("customer-events"), published.capture(), eq(KafkaOutputFormat.ENVELOPE));
        return published.getAllValues().stream().flatMap(List::stream).toList();
    }

    private static SyncTask task() {
        SyncTask task = new SyncTask();
        task.setTaskId(TASK_ID);
        task.setSourceTable("kp_phase_small");
        task.setTargetTable("customer-events");
        task.setSelectedColumns("id,name,qty,amount,created_at");
        task.setSyncKeyColumns("id");
        task.setConfigVersion(1);
        return task;
    }

    private static DataSource mysql() {
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setDatabaseName("source_db");
        return source;
    }

    private static DataSource kafka() {
        DataSource kafka = new DataSource();
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        return kafka;
    }

    /** The single-partition raw topic holding {@code values}, then idle (without spinning). */
    private static final class RawTopic extends MockConsumer<String, String> {
        private final TopicPartition partition = new TopicPartition(RAW_TOPIC, 0);

        private RawTopic(List<String> values) {
            super(OffsetResetStrategy.EARLIEST);
            schedulePollTask(() -> {
                rebalance(List.of(partition));
                updateBeginningOffsets(Map.of(partition, 0L));
                for (int offset = 0; offset < values.size(); offset++) {
                    addRecord(new ConsumerRecord<>(RAW_TOPIC, 0, offset, null, values.get(offset)));
                }
            });
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            ConsumerRecords<String, String> records = super.poll(timeout);
            if (records.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            return records;
        }
    }
}
