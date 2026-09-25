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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * The published events do not change when a Kafka CDC task's engine job switches its raw topic from
 * SeaTunnel's {@code DEBEZIUM_JSON} to Debezium's own records ({@code compatible_debezium_json}),
 * except for what the switch is for: initial-load rows become {@code SNAPSHOT} and the event time
 * becomes the source's binlog time.
 *
 * <p>Fixtures ({@code src/test/resources/kafka/raw-format-fidelity}), recorded 2026-09-25 on the
 * local stack (MySQL 8.0.45, SeaTunnel 2.3.13 Zeta): for each source table two engine jobs ran side
 * by side on the same rows and the same binlog - one with the {@code DEBEZIUM_JSON} config the
 * platform generated until now, one with the config it generates now - and both raw topics were
 * recorded verbatim ({@code *-raw-*.jsonl.gz}). {@code *-published-before.jsonl.gz} is what the
 * bridge as it was before this change (commit dec7159) published from the {@code DEBEZIUM_JSON} raw
 * topic, in all five formats. Tables:
 * <ul>
 *   <li>{@code types}: every MySQL type family (signed/unsigned integers, DECIMAL up to (65,30),
 *       FLOAT/DOUBLE, CHAR/VARCHAR/TEXT with 中文 and emoji, DATE, DATETIME(0/3/6), TIMESTAMP(0/3/6),
 *       TIME(0/3/6), YEAR, BIT(1/8/64), BOOLEAN, BLOB/BINARY/VARBINARY, JSON, ENUM/SET, NULLs); 6 rows
 *       of initial load, then INSERTs, UPDATEs (one changing the primary key), DELETEs, a transaction,
 *       FLOAT values Java 8 prints differently from Java 19+, and changes made while the job was
 *       paused with a savepoint and resumed; server zone UTC;</li>
 *   <li>{@code uk}: no primary key, a NOT NULL unique key as sync key, one UPDATE changing it;</li>
 *   <li>{@code tz}: TIMESTAMP / DATETIME / DATE / TIME with {@code server-time-zone = Asia/Shanghai}
 *       on a UTC server (binlog TIMESTAMPs shift by +8 h in both pipelines).</li>
 * </ul>
 */
@Tag("dev")
class KafkaRawFormatFidelityTest {

    private static final String FIXTURES = "/kafka/raw-format-fidelity/";
    private static final long TASK_ID = 99L;

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final KafkaEventSerializer serializer = new KafkaEventSerializer(mapper);
    private KafkaTaskBridgeService bridge;

    @AfterEach
    void closeBridge() {
        if (bridge != null) bridge.close();
    }

    @Test
    void everyTypePublishesTheSameValuesInAllFormats() throws Exception {
        assertSameAsBefore("types", "source_db.kd_types", "id", "UTC", 6);
    }

    @Test
    void aUniqueKeyTableKeepsItsEventsAndKeys() throws Exception {
        assertSameAsBefore("uk", "source_db.kd_uk", "code", "UTC", 3);
    }

    @Test
    void timestampsAreRenderedInTheSourceZoneAsTheEngineDid() throws Exception {
        assertSameAsBefore("tz", "source_db.kd_tz", "id", "Asia/Shanghai", 2);
    }

    @Test
    void debeziumJsonRawRecordsOfAnOlderJobArePublishedExactlyAsBefore() throws Exception {
        for (String[] table : new String[][]{{"types", "source_db.kd_types", "id", "UTC"},
            {"uk", "source_db.kd_uk", "code", "UTC"}, {"tz", "source_db.kd_tz", "id", "Asia/Shanghai"}}) {
            List<String[]> before = expected(table[0]);
            List<String[]> published = publish(read(table[0] + "-raw-debezium-json.jsonl.gz"), table[1], table[2], table[3], before.size());
            assertEquals(before.size(), published.size(), table[0]);
            for (int i = 0; i < before.size(); i++) {
                assertEquals(before.get(i)[0], published.get(i)[0], table[0] + " #" + i + " key");
                assertEquals(before.get(i)[1], published.get(i)[1], table[0] + " #" + i + " value");
            }
        }
    }

    /**
     * Publishes the recorded compatible_debezium_json raw topic and compares every message with the
     * one published before, text for text, after removing the allowed differences.
     */
    private void assertSameAsBefore(String table, String sourceTable, String keys, String zone, int initialLoadRows) throws Exception {
        List<String[]> before = expected(table);
        List<String[]> published = publish(read(table + "-raw-compatible-debezium-json.jsonl.gz"), sourceTable, keys, zone, before.size());
        assertEquals(before.size(), published.size(), "same events");
        int formats = KafkaOutputFormat.values().length;
        for (int i = 0; i < before.size(); i++) {
            KafkaOutputFormat format = KafkaOutputFormat.values()[i % formats];
            boolean initialLoad = i / formats < initialLoadRows;
            String where = table + " event #" + i / formats + " " + format;
            assertEquals(before.get(i)[0], published.get(i)[0], where + ": the message key (partitioning) is unchanged");
            String was = before.get(i)[1];
            String now = published.get(i)[1];
            JsonNode wasTree = mapper.readTree(was);
            JsonNode nowTree = mapper.readTree(now);
            switch (format) {
                case ENVELOPE -> {
                    assertEquals("CDC", wasTree.path("phase").asString(), where);
                    assertEquals(initialLoad ? "SNAPSHOT" : "CDC", nowTree.path("phase").asString(), where);
                    was = mask(mask(was, wasTree, "phase"), wasTree, "sourceEventTime");
                    now = mask(mask(now, nowTree, "phase"), nowTree, "sourceEventTime");
                }
                case CANAL_JSON -> {
                    was = mask(mask(was, wasTree, "es"), wasTree, "ts");
                    now = mask(mask(now, nowTree, "es"), nowTree, "ts");
                }
                case COMPATIBLE_DEBEZIUM_JSON -> {
                    if (initialLoad) {
                        assertEquals("c", wasTree.path("op").asString(), where);
                        assertEquals("r", nowTree.path("op").asString(), where);
                        assertEquals("true", nowTree.path("source").path("snapshot").asString(), where);
                        was = mask(was, wasTree, "op");
                        now = mask(now, nowTree, "op").replace(",\"snapshot\":\"true\"", "");
                    }
                    // Top-level ts_ms and source.ts_ms carry the same time.
                    was = mask(was, wasTree, "ts_ms");
                    now = mask(now, nowTree, "ts_ms");
                }
                case MAXWELL_JSON -> {
                    was = mask(was, wasTree, "ts");
                    now = mask(now, nowTree, "ts");
                }
                case OGG_JSON -> {
                    was = mask(was, wasTree, "op_ts");
                    now = mask(now, nowTree, "op_ts");
                }
            }
            // The published text, not a re-parsed tree: 1 vs 1.0, 1.0E300 vs 1.0E+300 or a reordered
            // column would all compare equal as trees.
            assertEquals(was, now, where);
        }
    }

    /** Replaces every {@code "field":<value>} of the top-level field's value in {@code text} with {@code "field":*}. */
    private static String mask(String text, JsonNode tree, String field) {
        JsonNode value = tree.get(field);
        assertNotNull(value, field + " in " + text);
        String member = "\"" + field + "\":" + value;
        assertTrue(text.contains(member), member + " in " + text);
        return text.replace(member, "\"" + field + "\":*");
    }

    /**
     * {key, value} of every message the bridge publishes from {@code raw}, five formats per event;
     * waits for {@code expectedMessages} (a trailing DELETE is held back until the next empty poll).
     */
    @SuppressWarnings("unchecked")
    private List<String[]> publish(List<String> raw, String sourceTable, String keys, String zone, int expectedMessages)
        throws InterruptedException {
        KafkaEventProducer producer = mock(KafkaEventProducer.class);
        bridge = new KafkaTaskBridgeService(new KafkaEventNormalizer(mapper), producer, mapper, new KafkaBridgeProperties()) {
            @Override
            void ensureTopics(SyncTask task, DataSource target) {
            }

            @Override
            Consumer<String, String> openConsumer(Properties properties) {
                return new RawTopic(raw);
            }
        };
        SyncTask task = new SyncTask();
        task.setTaskId(TASK_ID);
        task.setSourceTable(sourceTable);
        task.setTargetTable("events");
        task.setSyncKeyColumns(keys);
        task.setConfigVersion(1);
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setDatabaseName("source_db");
        source.setServerTimeZone(zone);
        DataSource kafka = new DataSource();
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        bridge.start(task, kafka, source);

        int formats = KafkaOutputFormat.values().length;
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (publishedEvents(producer).size() * formats < expectedMessages && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        bridge.close();
        List<String[]> messages = new ArrayList<>();
        for (KafkaEventNormalizer.NormalizedEvent event : publishedEvents(producer)) {
            for (KafkaOutputFormat format : KafkaOutputFormat.values()) {
                messages.add(new String[]{event.key().toString(), serializer.serialize(format, event)});
            }
        }
        return messages;
    }

    @SuppressWarnings("unchecked")
    private static List<KafkaEventNormalizer.NormalizedEvent> publishedEvents(KafkaEventProducer producer) {
        List<KafkaEventNormalizer.NormalizedEvent> events = new ArrayList<>();
        for (Invocation invocation : mockingDetails(producer).getInvocations()) {
            if (!"publishForTask".equals(invocation.getMethod().getName())) continue;
            assertEquals(KafkaOutputFormat.ENVELOPE, invocation.getArgument(4));
            events.addAll((List<KafkaEventNormalizer.NormalizedEvent>) invocation.getArgument(3));
        }
        return events;
    }

    private List<String[]> expected(String table) throws IOException {
        List<String[]> messages = new ArrayList<>();
        int index = 0;
        for (String line : lines(table + "-published-before.jsonl.gz")) {
            JsonNode message = mapper.readTree(line);
            assertEquals(KafkaOutputFormat.values()[index++ % KafkaOutputFormat.values().length].name(), message.path("format").asString());
            messages.add(new String[]{message.path("key").asString(), message.path("value").asString()});
        }
        return messages;
    }

    private List<String> read(String resource) throws IOException {
        List<String> values = new ArrayList<>();
        for (String line : lines(resource)) values.add(mapper.readTree(line).path("value").asString());
        return values;
    }

    private static List<String> lines(String resource) throws IOException {
        try (InputStream in = KafkaRawFormatFidelityTest.class.getResourceAsStream(FIXTURES + resource)) {
            assertNotNull(in, resource);
            BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(in), StandardCharsets.UTF_8));
            return reader.lines().filter(line -> !line.isBlank()).toList();
        }
    }

    /** The single-partition raw topic holding {@code values}, then idle (without spinning). */
    private static final class RawTopic extends MockConsumer<String, String> {
        private final TopicPartition partition = new TopicPartition("__ds_raw_" + TASK_ID + "_v1", 0);

        private RawTopic(List<String> values) {
            super(OffsetResetStrategy.EARLIEST);
            schedulePollTask(() -> {
                rebalance(List.of(partition));
                updateBeginningOffsets(Map.of(partition, 0L));
                for (int offset = 0; offset < values.size(); offset++) {
                    addRecord(new ConsumerRecord<>(partition.topic(), 0, offset, null, values.get(offset)));
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
