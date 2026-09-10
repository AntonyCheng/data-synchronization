package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.KafkaOutputFormat;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Serializes normalized events into the selected Kafka wire format. ENVELOPE is the
 * platform PRD event envelope; the four compatible formats follow the message shapes
 * SeaTunnel 2.3.13's own format deserializers accept (see docs/kafka-event-formats.md),
 * so a SeaTunnel Kafka source with the matching format can re-consume the topic.
 * The Kafka record key is format-independent: the producer always attaches the
 * sync-key JSON, so partitioning and per-key ordering stay identical across formats.
 */
class KafkaEventSerializer {

    /** OGG op_ts is a UTC datetime string with fixed microsecond digits (ms padded to 6). */
    private static final DateTimeFormatter OGG_OP_TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private final JsonMapper jsonMapper;

    KafkaEventSerializer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    String serialize(KafkaOutputFormat format, KafkaEventNormalizer.NormalizedEvent event) {
        return switch (format) {
            case ENVELOPE -> envelope(event).toString();
            case CANAL_JSON -> canalJson(event).toString();
            case COMPATIBLE_DEBEZIUM_JSON -> compatibleDebeziumJson(event).toString();
            case MAXWELL_JSON -> maxwellJson(event).toString();
            case OGG_JSON -> oggJson(event).toString();
        };
    }

    /** The platform PRD event envelope. */
    ObjectNode envelope(KafkaEventNormalizer.NormalizedEvent event) {
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

    /**
     * Canal JSON. UPDATE carries the changed columns' before values in {@code old}
     * (canal semantics; consumers backfill unchanged columns from {@code data});
     * DELETE carries the deleted row in {@code data}.
     */
    private ObjectNode canalJson(KafkaEventNormalizer.NormalizedEvent event) {
        ObjectNode message = jsonMapper.createObjectNode();
        message.put("type", event.op());
        message.put("database", event.sourceDatabase());
        message.put("table", event.sourceTable());
        long epochMs = epochMilli(event);
        message.put("es", epochMs);
        message.put("ts", epochMs);
        JsonNode image = rowImage(event);
        message.putArray("data").add(image == null ? jsonMapper.createObjectNode() : image);
        if ("UPDATE".equals(event.op())) {
            JsonNode before = image(event.before());
            // An absent before image (native MySQL "u" without one) degrades to an
            // empty old - the deserializer then backfills every column from data.
            message.putArray("old").add(before == null ? jsonMapper.createObjectNode() : changedBefore(before, image));
        }
        message.set("pkNames", keyNames(event.key()));
        message.put("isDdl", false);
        message.put("sql", "");
        return message;
    }

    /** Schema-less Debezium envelope ({@code value.converter.schemas.enable=false}). */
    private ObjectNode compatibleDebeziumJson(KafkaEventNormalizer.NormalizedEvent event) {
        ObjectNode message = jsonMapper.createObjectNode();
        JsonNode before = image(event.before());
        JsonNode after = afterImage(event);
        if (before == null) message.putNull("before"); else message.set("before", before);
        if (after == null) message.putNull("after"); else message.set("after", after);
        message.put("op", debeziumOp(event));
        long epochMs = epochMilli(event);
        message.put("ts_ms", epochMs);
        ObjectNode source = message.putObject("source");
        source.put("db", event.sourceDatabase());
        source.put("table", event.sourceTable());
        source.put("ts_ms", epochMs);
        if ("SNAPSHOT".equals(event.phase())) source.put("snapshot", "true");
        message.putNull("transaction");
        return message;
    }

    /**
     * Maxwell JSON. {@code ts} is epoch SECONDS (the deserializer multiplies by 1000);
     * UPDATE carries changed columns' before values in {@code old}, DELETE the deleted
     * row in {@code data}.
     */
    private ObjectNode maxwellJson(KafkaEventNormalizer.NormalizedEvent event) {
        ObjectNode message = jsonMapper.createObjectNode();
        message.put("type", event.op().toLowerCase(Locale.ROOT));
        message.put("database", event.sourceDatabase());
        message.put("table", event.sourceTable());
        message.put("ts", epochMilli(event) / 1000);
        JsonNode image = rowImage(event);
        if (image == null) message.putNull("data"); else message.set("data", image);
        if ("UPDATE".equals(event.op())) {
            JsonNode before = image(event.before());
            message.set("old", before == null ? jsonMapper.createObjectNode() : changedBefore(before, image));
        }
        message.set("primary_key_columns", keyNames(event.key()));
        return message;
    }

    /**
     * Oracle GoldenGate JSON. {@code table} is the qualified db.table (the deserializer
     * splits on "."). A null before image on UPDATE is not consumable by SeaTunnel's
     * ogg_json source - see the documented limitation in docs/kafka-event-formats.md.
     */
    private ObjectNode oggJson(KafkaEventNormalizer.NormalizedEvent event) {
        ObjectNode message = jsonMapper.createObjectNode();
        message.put("table", event.sourceDatabase() + "." + event.sourceTable());
        message.put("op_type", String.valueOf(event.op().charAt(0)));
        message.put("op_ts", OGG_OP_TS.format(instant(event)));
        JsonNode before = image(event.before());
        JsonNode after = afterImage(event);
        if (before == null) message.putNull("before"); else message.set("before", before);
        if (after == null) message.putNull("after"); else message.set("after", after);
        message.set("primary_keys", keyNames(event.key()));
        return message;
    }

    private static String debeziumOp(KafkaEventNormalizer.NormalizedEvent event) {
        if ("INSERT".equals(event.op())) return "SNAPSHOT".equals(event.phase()) ? "r" : "c";
        if ("UPDATE".equals(event.op())) return "u";
        return "d";
    }

    /** DELETE carries its row image in {@code before} rather than {@code data}. */
    private static JsonNode rowImage(KafkaEventNormalizer.NormalizedEvent event) {
        JsonNode image = image("DELETE".equals(event.op()) ? event.before() : event.data());
        return image;
    }

    /**
     * The after image is absent for DELETE - the normalizer carries the deleted row in
     * both {@code data} and {@code before}, but Debezium/OGG consumers expect
     * {@code after: null} there.
     */
    private static JsonNode afterImage(KafkaEventNormalizer.NormalizedEvent event) {
        return "DELETE".equals(event.op()) ? null : image(event.data());
    }

    private static JsonNode image(JsonNode node) {
        return node == null || node.isMissingNode() || node.isNull() ? null : node;
    }

    /** Only columns whose before value differs from (or is absent in) the after image. */
    private ObjectNode changedBefore(JsonNode before, JsonNode after) {
        ObjectNode changed = jsonMapper.createObjectNode();
        for (var entry : before.properties()) {
            JsonNode afterValue = after == null ? null : after.get(entry.getKey());
            if (afterValue == null || afterValue.isNull() || !entry.getValue().equals(afterValue)) {
                changed.set(entry.getKey(), entry.getValue());
            }
        }
        return changed;
    }

    private ArrayNode keyNames(ObjectNode key) {
        ArrayNode names = jsonMapper.createArrayNode();
        for (var entry : key.properties()) names.add(entry.getKey());
        return names;
    }

    private static Instant instant(KafkaEventNormalizer.NormalizedEvent event) {
        try {
            return Instant.parse(event.sourceEventTime());
        } catch (RuntimeException ex) {
            throw new ServiceException("Kafka 事件时间无效");
        }
    }

    private static long epochMilli(KafkaEventNormalizer.NormalizedEvent event) {
        return instant(event).toEpochMilli();
    }
}
