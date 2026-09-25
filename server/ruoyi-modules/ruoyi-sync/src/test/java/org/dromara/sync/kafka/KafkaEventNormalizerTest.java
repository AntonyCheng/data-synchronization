package org.dromara.sync.kafka;

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
    void debeziumUpdateCarriesItsBeforeImage() {
        // compatible_debezium_json: an UPDATE is one op=u with both images.
        List<KafkaEventNormalizer.NormalizedEvent> normalized = normalizer.normalize(List.of(
            event("u", "{\"code\":\"a-1\",\"name\":\"甲\",\"qty\":1}", "{\"code\":\"a-1\",\"name\":\"甲-upd\",\"qty\":10}", 1790290984000L)),
            0, List.of("code"));

        assertEquals(1, normalized.size());
        KafkaEventNormalizer.NormalizedEvent update = normalized.getFirst();
        assertEquals("UPDATE", update.op());
        assertEquals("{\"code\":\"a-1\"}", update.key().toString());
        assertEquals("甲", update.before().path("name").asString());
        assertEquals("甲-upd", update.data().path("name").asString());
        assertEquals("CDC", update.phase());
        assertEquals("2026-09-24T23:03:04Z", update.sourceEventTime(), "the binlog event time, whole seconds");
    }

    @Test
    void debeziumUpdateOfTheSyncKeyRetiresTheOldKey() {
        // Only when the sync key is a unique key other than the primary key: Debezium sends op=u
        // (the primary key did not change) while the key every event is published under did.
        List<KafkaEventNormalizer.NormalizedEvent> normalized = normalizer.normalize(List.of(
            event("u", "{\"id\":7,\"code\":\"b-2\"}", "{\"id\":7,\"code\":\"b-2x\"}", 1000)), 0, List.of("code"));

        assertEquals(List.of("DELETE{\"code\":\"b-2\"}", "INSERT{\"code\":\"b-2x\"}"),
            normalized.stream().map(e -> e.op() + e.key()).toList());
        assertEquals("b-2", normalized.get(0).data().path("code").asString());
        assertEquals("b-2x", normalized.get(1).data().path("code").asString());
        assertTrue(normalized.stream().allMatch(e -> "CDC".equals(e.phase())));
    }

    @Test
    void debeziumReadIsTheInitialLoad() {
        List<KafkaEventNormalizer.NormalizedEvent> normalized = normalizer.normalize(List.of(
            event("r", null, "{\"id\":1}", 1000), event("r", null, "{\"id\":2}", 1001), event("c", null, "{\"id\":3}", 2000)),
            0, List.of("id"));

        assertEquals(List.of("SNAPSHOT", "SNAPSHOT", "CDC"), normalized.stream().map(KafkaEventNormalizer.NormalizedEvent::phase).toList());
        assertTrue(normalized.stream().allMatch(e -> "INSERT".equals(e.op())));
    }

    @Test
    void rejectsMissingKey() {
        JsonNode raw = event("c", null, "{\"name\":\"A\"}", 1000);
        assertThrows(ServiceException.class, () -> normalizer.normalize(List.of(raw), 1, List.of("id")));
    }

    @Test
    void aNewInitialLoadAfterChangesIsSnapshotAgain() {
        // Restarting a stopped task, or reinitializing it, reuses the raw topic: the new run's initial
        // load follows the previous run's changes, possibly in the same poll batch.
        List<KafkaEventNormalizer.NormalizedEvent> normalized = normalizer.normalize(List.of(
            event("c", null, "{\"id\":1}", 1000), event("r", null, "{\"id\":1}", 2000), event("r", null, "{\"id\":2}", 2000)),
            0, List.of("id"));

        assertEquals(List.of("CDC", "SNAPSHOT", "SNAPSHOT"), normalized.stream().map(KafkaEventNormalizer.NormalizedEvent::phase).toList());
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

        JsonNode envelope = new KafkaEventSerializer(mapper).envelope(normalized);

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
