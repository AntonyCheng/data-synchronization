package org.dromara.sync.kafka;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shapes and values {@link KafkaRawRecordReader} handles beyond what {@link KafkaRawFormatFidelityTest}
 * compares. The {@code edge} records were recorded with the others (2026-09-25): values SeaTunnel's
 * row format could not convert at all - the job failed - so there is nothing to compare them with.
 */
@Tag("dev")
class KafkaRawRecordReaderTest {

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final KafkaRawRecordReader reader = new KafkaRawRecordReader(mapper, ZoneId.of("UTC"));

    @Test
    void valuesSeaTunnelCouldNotConvertArePublishedAsMySqlHasThem() throws IOException {
        List<String> raw = lines("edge-raw-compatible-debezium-json.jsonl.gz");
        JsonNode first = reader.read(raw.getFirst());

        assertEquals("r", first.path("op").asString());
        JsonNode row = first.path("after");
        // DATETIME(6) before 1970 with a sub-millisecond part failed SeaTunnel's conversion.
        assertEquals("1969-12-31T23:59:59.999999", row.path("dt6").asString());
        // TIME of 24 hours or more (SeaTunnel: LocalTime only).
        assertEquals("838:59:59", row.path("t6").asString());
        // GEOMETRY: the WKB, base64 like every other binary value.
        assertEquals("AQEAAAAAAAAAAADwPwAAAAAAAABA", row.path("g").asString());
        assertEquals(1.5, row.path("f").doubleValue());
        assertEquals("100:00:00", reader.read(raw.get(2)).path("after").path("t0").asString());
    }

    @Test
    void debeziumRecordsBecomeTheEventShapeTheNormalizerReads() throws IOException {
        JsonNode update = reader.read(lines("edge-raw-compatible-debezium-json.jsonl.gz").get(2));

        assertEquals("u", update.path("op").asString());
        assertEquals("source_db", update.path("source").path("database").asString());
        assertEquals("kd_edge", update.path("source").path("table").asString());
        // source.ts_ms: the binlog event time, in whole seconds.
        assertEquals(0, update.path("ts_ms").asLong() % 1000);
        assertTrue(update.path("before").isObject() && update.path("after").isObject());
    }

    @Test
    void recordsWithoutARowChangeAreSkipped() {
        assertNull(reader.read(null), "a tombstone");
        assertNull(reader.read("{\"schema\":{\"type\":\"struct\",\"fields\":[]},\"payload\":{\"source\":{\"db\":\"source_db\"},"
            + "\"databaseName\":\"source_db\",\"ddl\":\"ALTER TABLE t ADD COLUMN c INT\"}}"), "a schema change record");
    }

    @Test
    void recordsOfTheFormerFormatsAreReadAsTheyAre() {
        JsonNode legacy = reader.read("{\"before\":null,\"after\":{\"id\":1,\"amount\":1.50},\"op\":\"c\","
            + "\"source\":{\"schema\":null,\"database\":\"source_db\",\"table\":\"t\"},\"ts_ms\":1790258500371}");
        assertEquals("c", legacy.path("op").asString());
        assertEquals("{\"id\":1,\"amount\":1.5}", legacy.path("after").toString());
        assertEquals(1790258500371L, legacy.path("ts_ms").asLong());

        JsonNode fullRow = reader.read("{\"id\":1,\"name\":\"snap-1\"}");
        assertNotNull(fullRow);
        assertTrue(!fullRow.has("op"));
    }

    private static List<String> lines(String resource) throws IOException {
        try (InputStream in = KafkaRawRecordReaderTest.class.getResourceAsStream("/kafka/raw-format-fidelity/" + resource)) {
            assertNotNull(in, resource);
            BufferedReader text = new BufferedReader(new InputStreamReader(new GZIPInputStream(in), StandardCharsets.UTF_8));
            JsonMapper mapper = JsonMapper.builder().build();
            return text.lines().filter(line -> !line.isBlank()).map(line -> mapper.readTree(line).path("value").asString()).toList();
        }
    }
}
