package org.dromara.sync.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.dromara.common.core.exception.ServiceException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class KafkaEventNormalizerTest {

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final KafkaEventNormalizer normalizer = new KafkaEventNormalizer(mapper);

    @Test
    void coalescesDebeziumDeleteCreateAndAddsPrdEnvelope() throws Exception {
        List<JsonNode> raw = List.of(
            event("c", null, "{\"id\":1,\"name\":\"A\"}", 1000),
            event("d", "{\"id\":1,\"name\":\"A\"}", null, 2000),
            event("c", null, "{\"id\":1,\"name\":\"B\"}", 2001),
            event("d", "{\"id\":2,\"name\":\"X\"}", null, 3000));

        List<KafkaEventNormalizer.NormalizedEvent> normalized = normalizer.normalize(raw, 1, List.of("id"));

        assertEquals(3, normalized.size());
        assertEquals("INSERT", normalized.get(0).op());
        assertEquals("SNAPSHOT", normalized.get(0).phase());
        assertEquals("UPDATE", normalized.get(1).op());
        assertEquals("B", normalized.get(1).data().path("name").asText());
        assertEquals("CDC", normalized.get(1).phase());
        assertEquals("DELETE", normalized.get(2).op());
        assertEquals("2", normalized.get(2).key().path("id").asText());
        assertEquals("1970-", normalized.get(2).sourceEventTime().substring(0, 5));
    }

    @Test
    void rejectsMissingKey() {
        JsonNode raw = event("c", null, "{\"name\":\"A\"}", 1000);
        assertThrows(ServiceException.class, () -> normalizer.normalize(List.of(raw), 1, List.of("id")));
    }

    @Test
    void rejectsInvalidPhaseOrder() {
        JsonNode first = event("c", null, "{\"id\":1}", 1000);
        JsonNode second = event("r", null, "{\"id\":2}", 2000);
        assertThrows(ServiceException.class, () -> normalizer.normalize(List.of(first, second), 0, List.of("id")));
    }

    @Test
    void preservesCompositeKeyInConfiguredOrder() {
        JsonNode raw = event("c", null, "{\"tenant_id\":10,\"order_no\":1001,\"amount\":88.8}", 1000);
        KafkaEventNormalizer.NormalizedEvent normalized = normalizer.normalize(List.of(raw), 1, List.of("tenant_id", "order_no")).getFirst();

        assertEquals("10", normalized.key().path("tenant_id").asText());
        assertEquals("1001", normalized.key().path("order_no").asText());
        assertEquals("SNAPSHOT", normalized.phase());
    }

    @Test
    void producerEnvelopeContainsPrdFields() {
        JsonNode raw = event("d", "{\"id\":7,\"name\":\"gone\"}", null, 1000);
        KafkaEventNormalizer.NormalizedEvent normalized = normalizer.normalize(List.of(raw), 0, List.of("id")).getFirst();
        KafkaEventProducer producer = new KafkaEventProducer(mapper);

        JsonNode envelope = producer.toEnvelope(normalized);

        assertEquals("DELETE", envelope.path("op").asText());
        assertEquals("7", envelope.path("key").path("id").asText());
        assertEquals("CDC", envelope.path("phase").asText());
        assertEquals("source_db", envelope.path("source").path("database").asText());
        assertEquals("customers", envelope.path("source").path("table").asText());
        assertEquals("gone", envelope.path("data").path("name").asText());
        assertEquals("gone", envelope.path("before").path("name").asText());
    }

    @Test
    void convertsJdbcSnapshotRowsToPrdSnapshotEnvelope() throws Exception {
        List<JsonNode> rows = List.of(
            mapper.readTree("{\"id\":1,\"name\":\"A\"}"),
            mapper.readTree("{\"id\":2,\"name\":\"B\"}"));
        List<KafkaEventNormalizer.NormalizedEvent> normalized =
            normalizer.normalizeSnapshotRows(rows, "source_db", "customers", List.of("id"), List.of("id", "name"));
        assertEquals(2, normalized.size());
        assertEquals("INSERT", normalized.getFirst().op());
        assertEquals("SNAPSHOT", normalized.getFirst().phase());
        assertEquals("source_db", normalized.getFirst().sourceDatabase());
        assertEquals("customers", normalized.getFirst().sourceTable());
        assertEquals("1", normalized.getFirst().key().path("id").asText());
        assertEquals("A", normalized.getFirst().data().path("name").asText());
    }

    @Test
    void canonicalisesUpperCaseSnapshotColumnsFromOracleCompatSource() throws Exception {
        // GoldenDB in ORA_COMPATIBLE_MODE folds JDBC result-set labels to UPPER CASE.
        List<JsonNode> rows = List.of(mapper.readTree("{\"ID\":1,\"NAME\":\"A\"}"));
        List<KafkaEventNormalizer.NormalizedEvent> normalized =
            normalizer.normalizeSnapshotRows(rows, "test", "t_test", List.of("id"), List.of("id", "name"));
        assertEquals("1", normalized.getFirst().key().path("id").asText());
        assertEquals("A", normalized.getFirst().data().path("name").asText());
        assertTrue(normalized.getFirst().data().has("id"));
    }

    private JsonNode event(String op, String before, String after, long ts) {
        String source = "{\"database\":\"source_db\",\"table\":\"customers\"}";
        String json = "{\"op\":\"" + op + "\",\"before\":" + (before == null ? "null" : before)
            + ",\"after\":" + (after == null ? "null" : after) + ",\"source\":" + source + ",\"ts_ms\":" + ts + "}";
        return mapper.readTree(json);
    }
}
