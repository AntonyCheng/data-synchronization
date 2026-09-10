package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exact wire shapes for every Kafka output format. The four compatible formats are
 * checked against the message structures SeaTunnel 2.3.13's own format deserializers
 * accept, so a Kafka source with the matching format can re-consume the topic.
 */
@Tag("dev")
class KafkaEventSerializerTest {

    private static final String EVENT_TIME = "2026-09-10T00:00:01.500Z";

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final KafkaEventSerializer serializer = new KafkaEventSerializer(mapper);

    @Test
    void envelopeKeepsLegacyPrdShape() {
        JsonNode envelope = serialize(KafkaOutputFormat.ENVELOPE,
            event("UPDATE", "{\"id\":1,\"name\":\"A\",\"qty\":5}", "{\"id\":1,\"name\":\"B\",\"qty\":5}", "CDC"));

        assertEquals("UPDATE", envelope.path("op").asText());
        assertEquals("1", envelope.path("key").path("id").asText());
        assertEquals("B", envelope.path("data").path("name").asText());
        assertEquals("A", envelope.path("before").path("name").asText());
        assertEquals("source_db", envelope.path("source").path("database").asText());
        assertEquals("customers", envelope.path("source").path("table").asText());
        assertEquals(EVENT_TIME, envelope.path("sourceEventTime").asText());
        assertEquals("CDC", envelope.path("phase").asText());
    }

    @Test
    void canalJsonCarriesRowArrayAndChangedColumnsBefore() {
        JsonNode update = serialize(KafkaOutputFormat.CANAL_JSON,
            event("UPDATE", "{\"id\":1,\"name\":\"A\",\"qty\":5}", "{\"id\":1,\"name\":\"B\",\"qty\":5}", "CDC"));
        long epochMs = Instant.parse(EVENT_TIME).toEpochMilli();

        assertEquals("UPDATE", update.path("type").asText());
        assertEquals("source_db", update.path("database").asText());
        assertEquals("customers", update.path("table").asText());
        assertEquals(epochMs, update.path("es").asLong());
        assertEquals(epochMs, update.path("ts").asLong());
        assertEquals(1, update.path("data").size());
        assertEquals("B", update.path("data").get(0).path("name").asText());
        assertEquals(1, update.path("old").size());
        assertEquals("A", update.path("old").get(0).path("name").asText());
        assertFalse(update.path("old").get(0).has("qty"), "unchanged columns must not appear in old");
        assertFalse(update.path("old").get(0).has("id"), "key columns never change");
        assertEquals("id", update.path("pkNames").get(0).asText());
        assertFalse(update.path("isDdl").asBoolean());
        assertTrue(update.path("sql").asText().isEmpty());

        JsonNode insert = serialize(KafkaOutputFormat.CANAL_JSON,
            event("INSERT", null, "{\"id\":1,\"name\":\"B\"}", "SNAPSHOT"));
        assertEquals("INSERT", insert.path("type").asText(), "canal has no snapshot op");
        assertFalse(insert.has("old"));

        JsonNode delete = serialize(KafkaOutputFormat.CANAL_JSON,
            event("DELETE", "{\"id\":1,\"name\":\"gone\"}", "{\"id\":1,\"name\":\"gone\"}", "CDC"));
        assertEquals("DELETE", delete.path("type").asText());
        assertEquals("gone", delete.path("data").get(0).path("name").asText(), "canal DELETE carries the row in data");
        assertFalse(delete.has("old"));
    }

    @Test
    void compatibleDebeziumJsonMapsSnapshotToReadOp() {
        long epochMs = Instant.parse(EVENT_TIME).toEpochMilli();
        JsonNode insert = serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON,
            event("INSERT", null, "{\"id\":1,\"name\":\"A\"}", "CDC"));
        assertEquals("c", insert.path("op").asText());
        assertTrue(insert.path("before").isNull());
        assertEquals("A", insert.path("after").path("name").asText());

        JsonNode snapshot = serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON,
            event("INSERT", null, "{\"id\":1,\"name\":\"A\"}", "SNAPSHOT"));
        assertEquals("r", snapshot.path("op").asText(), "Debezium READ/snapshot op");
        assertEquals("true", snapshot.path("source").path("snapshot").asText());
        assertFalse(insert.path("source").has("snapshot"));

        JsonNode update = serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON,
            event("UPDATE", "{\"id\":1,\"name\":\"A\"}", "{\"id\":1,\"name\":\"B\"}", "CDC"));
        assertEquals("u", update.path("op").asText());
        assertEquals("A", update.path("before").path("name").asText());
        assertEquals("B", update.path("after").path("name").asText());
        assertEquals(epochMs, update.path("ts_ms").asLong());
        assertEquals(epochMs, update.path("source").path("ts_ms").asLong());
        assertEquals("source_db", update.path("source").path("db").asText());
        assertEquals("customers", update.path("source").path("table").asText());
        assertTrue(update.path("transaction").isNull());

        JsonNode delete = serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON,
            event("DELETE", "{\"id\":1,\"name\":\"gone\"}", "{\"id\":1,\"name\":\"gone\"}", "CDC"));
        assertEquals("d", delete.path("op").asText());
        assertTrue(delete.path("after").isNull());
        assertEquals("gone", delete.path("before").path("name").asText());
    }

    @Test
    void maxwellJsonUsesLowercaseTypeAndEpochSeconds() {
        JsonNode update = serialize(KafkaOutputFormat.MAXWELL_JSON,
            event("UPDATE", "{\"id\":1,\"name\":\"A\",\"qty\":5}", "{\"id\":1,\"name\":\"B\",\"qty\":5}", "CDC"));

        assertEquals("update", update.path("type").asText());
        assertEquals(Instant.parse(EVENT_TIME).toEpochMilli() / 1000, update.path("ts").asLong(),
            "maxwell ts is epoch seconds");
        assertEquals("B", update.path("data").path("name").asText());
        assertEquals(1, update.path("old").size());
        assertEquals("A", update.path("old").path("name").asText());
        assertFalse(update.path("old").has("qty"));
        assertEquals("id", update.path("primary_key_columns").get(0).asText());

        JsonNode delete = serialize(KafkaOutputFormat.MAXWELL_JSON,
            event("DELETE", "{\"id\":1,\"name\":\"gone\"}", "{\"id\":1,\"name\":\"gone\"}", "CDC"));
        assertEquals("delete", delete.path("type").asText());
        assertEquals("gone", delete.path("data").path("name").asText(), "maxwell DELETE carries the row in data");
        assertFalse(delete.has("old"));
    }

    @Test
    void oggJsonEmitsQualifiedTableAndMicrosecondUtcTimestamp() {
        JsonNode update = serialize(KafkaOutputFormat.OGG_JSON,
            event("UPDATE", "{\"id\":1,\"name\":\"A\"}", "{\"id\":1,\"name\":\"B\"}", "CDC"));

        assertEquals("U", update.path("op_type").asText());
        assertEquals("source_db.customers", update.path("table").asText());
        assertEquals("2026-09-10 00:00:01.500000", update.path("op_ts").asText());
        assertEquals("A", update.path("before").path("name").asText());
        assertEquals("B", update.path("after").path("name").asText());
        assertEquals("id", update.path("primary_keys").get(0).asText());

        JsonNode insert = serialize(KafkaOutputFormat.OGG_JSON,
            event("INSERT", null, "{\"id\":1,\"name\":\"A\"}", "SNAPSHOT"));
        assertEquals("I", insert.path("op_type").asText());
        assertTrue(insert.path("before").isNull());

        JsonNode delete = serialize(KafkaOutputFormat.OGG_JSON,
            event("DELETE", "{\"id\":1,\"name\":\"gone\"}", "{\"id\":1,\"name\":\"gone\"}", "CDC"));
        assertEquals("D", delete.path("op_type").asText());
        assertEquals("gone", delete.path("before").path("name").asText());
        assertTrue(delete.path("after").isNull());
    }

    /**
     * A native-MySQL Debezium "u" event reaches the serializer without a before image
     * (the normalizer only carries before for DELETE and the merged GoldenDB path).
     * Each format degrades to the shape its consumers can still read.
     */
    @Test
    void updateWithoutBeforeImageDegradesPerFormat() {
        JsonNode canal = serialize(KafkaOutputFormat.CANAL_JSON,
            event("UPDATE", null, "{\"id\":1,\"name\":\"B\"}", "CDC"));
        assertEquals(1, canal.path("old").size());
        assertEquals(0, canal.path("old").get(0).size(), "canal old backfills from data");

        JsonNode maxwell = serialize(KafkaOutputFormat.MAXWELL_JSON,
            event("UPDATE", null, "{\"id\":1,\"name\":\"B\"}", "CDC"));
        assertEquals(0, maxwell.path("old").size());

        JsonNode debezium = serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON,
            event("UPDATE", null, "{\"id\":1,\"name\":\"B\"}", "CDC"));
        assertTrue(debezium.path("before").isNull());

        JsonNode ogg = serialize(KafkaOutputFormat.OGG_JSON,
            event("UPDATE", null, "{\"id\":1,\"name\":\"B\"}", "CDC"));
        assertTrue(ogg.path("before").isNull());
    }

    /** The record key is attached by the producer for every format; compatible bodies must not duplicate it. */
    @Test
    void compatibleFormatsDoNotEmbedTheKeyObject() {
        KafkaEventNormalizer.NormalizedEvent update =
            event("UPDATE", "{\"id\":1,\"name\":\"A\"}", "{\"id\":1,\"name\":\"B\"}", "CDC");
        assertTrue(serialize(KafkaOutputFormat.ENVELOPE, update).has("key"));
        assertFalse(serialize(KafkaOutputFormat.CANAL_JSON, update).has("key"));
        assertFalse(serialize(KafkaOutputFormat.COMPATIBLE_DEBEZIUM_JSON, update).has("key"));
        assertFalse(serialize(KafkaOutputFormat.MAXWELL_JSON, update).has("key"));
        assertFalse(serialize(KafkaOutputFormat.OGG_JSON, update).has("key"));
    }

    @Test
    void parseDefaultsBlankToEnvelopeAndRejectsUnknownValues() {
        assertEquals(KafkaOutputFormat.ENVELOPE, KafkaOutputFormat.parse(null));
        assertEquals(KafkaOutputFormat.ENVELOPE, KafkaOutputFormat.parse("  "));
        assertEquals(KafkaOutputFormat.CANAL_JSON, KafkaOutputFormat.parse(" CANAL_JSON "));
        assertThrows(ServiceException.class, () -> KafkaOutputFormat.parse("AVRO"));
    }

    private JsonNode serialize(KafkaOutputFormat format, KafkaEventNormalizer.NormalizedEvent event) {
        try {
            return mapper.readTree(serializer.serialize(format, event));
        } catch (Exception ex) {
            throw new IllegalStateException("serialized message is not valid JSON", ex);
        }
    }

    private KafkaEventNormalizer.NormalizedEvent event(String op, String beforeJson, String dataJson, String phase) {
        ObjectNode key = mapper.createObjectNode();
        key.put("id", 1);
        return new KafkaEventNormalizer.NormalizedEvent(op, key,
            beforeJson == null ? null : readTree(beforeJson),
            readTree(dataJson), phase, "source_db", "customers", EVENT_TIME);
    }

    private JsonNode readTree(String json) {
        try {
            return mapper.readTree(json);
        } catch (Exception ex) {
            throw new IllegalArgumentException("bad fixture JSON: " + json, ex);
        }
    }
}
