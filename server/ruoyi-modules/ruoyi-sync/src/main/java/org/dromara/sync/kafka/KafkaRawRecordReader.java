package org.dromara.sync.kafka;

import org.dromara.common.core.exception.ServiceException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectReader;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Reads one record of a task's raw topic into the event shape {@link KafkaEventNormalizer} takes.
 * The raw topic holds one of three shapes, told apart record by record:
 * <ul>
 *   <li><b>MySQL-CDC with {@code format = compatible_debezium_json}</b> (FULL_CDC / INCREMENTAL tasks
 *       started by this version): Kafka Connect's JSON envelope {@code {"schema":…,"payload":…}}
 *       around Debezium's own change event. It keeps what SeaTunnel's row format drops - {@code op=r}
 *       for an initial-load row and {@code source.ts_ms}, the time the binlog event was written - and
 *       carries every value in Debezium's encoding, which this class converts (see below).</li>
 *   <li><b>SeaTunnel {@code DEBEZIUM_JSON}</b> (CDC tasks whose engine job was started before, still
 *       running after an upgrade): read unchanged. Their initial load stays {@code op=c}.</li>
 *   <li><b>Plain JSON rows</b> (FULL tasks, a bounded JDBC snapshot): read unchanged.</li>
 * </ul>
 *
 * <p>A column value in a Connect envelope is converted to exactly the JSON value the
 * {@code DEBEZIUM_JSON} raw format produced for it: SeaTunnel's MySQL type mapping, its JSON row
 * writer, and this bridge's own parse of that JSON. So every output format keeps its values when a
 * task moves to the new raw format. That includes the artefacts of that path: a {@code DECIMAL}
 * arrives as a JSON number and is parsed as a double; {@code TINYINT(1)} is a boolean; a
 * {@code FLOAT} goes through {@code Float.toString} on the engine's Java 8
 * ({@link JavaEightFloats}). {@code FLOAT} and {@code TINYINT(1)} cannot be recognised from the
 * Connect schema alone (Debezium widens {@code FLOAT} to a double and gives {@code TINYINT} an int16),
 * so the engine job propagates their source type ({@code datatype.propagate.source.type}, see
 * {@code SeaTunnelJobConfigGenerator}). A {@code TIMESTAMP} arrives as a UTC instant and is rendered
 * in the source's zone, the zone the engine job was given as {@code server-time-zone}, which is what
 * SeaTunnel did.
 */
final class KafkaRawRecordReader {

    /** Kafka Connect's JsonConverter always writes the schema first. */
    private static final String CONNECT_ENVELOPE_PREFIX = "{\"schema\":";
    private static final String SOURCE_TYPE = "__debezium.source.column.type";
    private static final String SOURCE_LENGTH = "__debezium.source.column.length";
    /** SeaTunnel's JSON row writer formats TIME as {@code HH:mm:ss} plus the fraction it has. */
    private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
        .appendPattern("HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
        .toFormatter();
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    private final JsonMapper jsonMapper;
    /** Decimals must reach the conversion exactly; the default parse would already round them to doubles. */
    private final ObjectReader exactReader;
    private final JsonNodeFactory nodes = JsonNodeFactory.instance;
    private final ZoneId sourceZone;

    KafkaRawRecordReader(JsonMapper jsonMapper, ZoneId sourceZone) {
        this.jsonMapper = jsonMapper;
        this.exactReader = jsonMapper.reader().with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        this.sourceZone = sourceZone;
    }

    /**
     * The event to normalize, or {@code null} for a record that carries no row change (a tombstone,
     * or anything in a Connect envelope without an {@code op}).
     */
    JsonNode read(String value) {
        if (value == null) return null;
        if (!value.startsWith(CONNECT_ENVELOPE_PREFIX)) return jsonMapper.readTree(value);
        return fromConnectEnvelope(exactReader.readTree(value));
    }

    /**
     * {@code {"before","after","op","source":{"database","table"},"ts_ms"}} - the shape of the
     * {@code DEBEZIUM_JSON} records - with {@code ts_ms} = {@code source.ts_ms}: when the source wrote
     * the binlog event (whole seconds), or when the snapshot read the row.
     */
    private JsonNode fromConnectEnvelope(JsonNode envelope) {
        JsonNode payload = envelope.path("payload");
        JsonNode op = payload.get("op");
        if (op == null || op.isNull()) return null;
        Map<String, JsonNode> columns = columnSchemas(envelope.path("schema"));
        ObjectNode event = nodes.objectNode();
        event.set("before", row(payload.get("before"), columns));
        event.set("after", row(payload.get("after"), columns));
        event.put("op", op.asString());
        ObjectNode source = event.putObject("source");
        JsonNode origin = payload.path("source");
        source.set("database", origin.path("db"));
        source.set("table", origin.path("table"));
        long sourceTime = origin.path("ts_ms").asLong(0);
        event.put("ts_ms", sourceTime > 0 ? sourceTime : payload.path("ts_ms").asLong(0));
        return event;
    }

    /** Column name to field schema, from the {@code after} (else {@code before}) struct of the envelope schema. */
    private static Map<String, JsonNode> columnSchemas(JsonNode schema) {
        Map<String, JsonNode> columns = new LinkedHashMap<>();
        for (String image : new String[]{"after", "before"}) {
            for (JsonNode field : schema.path("fields")) {
                if (!image.equals(field.path("field").asString())) continue;
                for (JsonNode column : field.path("fields")) columns.putIfAbsent(column.path("field").asString(), column);
            }
            if (!columns.isEmpty()) break;
        }
        if (columns.isEmpty()) throw new ServiceException("Kafka 原始事件缺少行结构（schema）");
        return columns;
    }

    private JsonNode row(JsonNode image, Map<String, JsonNode> columns) {
        if (image == null || image.isNull()) return nodes.nullNode();
        ObjectNode row = nodes.objectNode();
        for (Map.Entry<String, JsonNode> column : columns.entrySet()) {
            row.set(column.getKey(), value(image.get(column.getKey()), column.getValue()));
        }
        return row;
    }

    private JsonNode value(JsonNode value, JsonNode schema) {
        if (value == null || value.isNull()) return nodes.nullNode();
        String logicalType = schema.path("name").asString("");
        switch (logicalType) {
            case "org.apache.kafka.connect.data.Decimal":
                return decimal(value.decimalValue());
            case "io.debezium.time.Date":
                return nodes.stringNode(LocalDate.ofEpochDay(value.longValue()).toString());
            case "io.debezium.time.Timestamp":
                return dateTime(Math.floorDiv(value.longValue(), 1_000L), Math.floorMod(value.longValue(), 1_000L) * 1_000_000L);
            case "io.debezium.time.MicroTimestamp":
                return dateTime(Math.floorDiv(value.longValue(), 1_000_000L), Math.floorMod(value.longValue(), 1_000_000L) * 1_000L);
            case "io.debezium.time.NanoTimestamp":
                return dateTime(Math.floorDiv(value.longValue(), 1_000_000_000L), Math.floorMod(value.longValue(), 1_000_000_000L));
            case "io.debezium.time.ZonedTimestamp":
                return nodes.stringNode(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                    LocalDateTime.ofInstant(Instant.parse(value.asString()), sourceZone)));
            case "io.debezium.time.MicroTime":
                return time(value.longValue());
            case "io.debezium.time.Time":
                return time(value.longValue() * 1_000L);
            case "io.debezium.time.NanoTime":
                return time(Math.floorDiv(value.longValue(), 1_000L));
            default:
                break;
        }
        return switch (schema.path("type").asString()) {
            case "boolean" -> nodes.booleanNode(value.asBoolean());
            case "int8", "int16", "int32", "int64" -> isTinyIntOne(schema)
                ? nodes.booleanNode(value.longValue() != 0)
                : integral(value.bigIntegerValue());
            case "float", "double" -> isFloat(schema)
                ? nodes.numberNode(JavaEightFloats.parsedValue((float) value.doubleValue()))
                : nodes.numberNode(value.doubleValue());
            // bytes (BLOB, BINARY, BIT(n>1)) are base64 text in both formats; strings pass through.
            case "bytes", "string" -> nodes.stringNode(value.asString());
            // A geometry is a struct around its WKB; SeaTunnel could not map it at all.
            case "struct" -> value.has("wkb") ? nodes.stringNode(value.path("wkb").asString()) : value;
            default -> value;
        };
    }

    /**
     * SeaTunnel wrote a DECIMAL through a JSON node factory that strips trailing zeros (zero stays
     * {@code 0}) and printed it plain, and the bridge parsed that text: a fraction became a double,
     * an integral value an int / long / big integer node.
     */
    private JsonNode decimal(BigDecimal value) {
        if (value.signum() == 0) return nodes.numberNode(0);
        String text = value.stripTrailingZeros().toPlainString();
        return text.indexOf('.') >= 0 ? nodes.numberNode(Double.parseDouble(text)) : integral(new BigInteger(text));
    }

    /** The node the bridge's JSON parse creates for an integer: int, long or big integer by magnitude. */
    private JsonNode integral(BigInteger value) {
        if (value.bitLength() < Integer.SIZE) return nodes.numberNode(value.intValue());
        if (value.bitLength() < Long.SIZE) return nodes.numberNode(value.longValue());
        return nodes.numberNode(value);
    }

    private JsonNode dateTime(long epochSecond, long nanoOfSecond) {
        return nodes.stringNode(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
            LocalDateTime.ofEpochSecond(epochSecond, (int) nanoOfSecond, ZoneOffset.UTC)));
    }

    /**
     * A MySQL TIME within a day is a time of day, formatted as SeaTunnel did. MySQL also allows a
     * negative TIME or one of 24 hours and more, which SeaTunnel could not convert at all (the job
     * failed); it is rendered here as MySQL shows it, e.g. {@code -01:30:00} or {@code 838:59:59}.
     */
    private JsonNode time(long micros) {
        if (micros >= 0 && micros < MICROS_PER_DAY) {
            return nodes.stringNode(TIME_FORMAT.format(LocalTime.ofNanoOfDay(micros * 1_000L)));
        }
        long magnitude = Math.abs(micros);
        long seconds = magnitude / 1_000_000L;
        long fraction = magnitude % 1_000_000L;
        StringBuilder text = new StringBuilder(micros < 0 ? "-" : "")
            .append(String.format("%02d:%02d:%02d", seconds / 3600, seconds / 60 % 60, seconds % 60));
        if (fraction != 0) text.append('.').append(String.format("%06d", fraction).replaceFirst("0+$", ""));
        return nodes.stringNode(text.toString());
    }

    /** SeaTunnel maps a signed {@code tinyint(1)} to BOOLEAN; every other TINYINT stays a number. */
    private static boolean isTinyIntOne(JsonNode schema) {
        JsonNode parameters = schema.path("parameters");
        return "TINYINT".equalsIgnoreCase(parameters.path(SOURCE_TYPE).asString())
            && "1".equals(parameters.path(SOURCE_LENGTH).asString());
    }

    /** {@code FLOAT}, {@code FLOAT UNSIGNED}, {@code FLOAT UNSIGNED ZEROFILL}: SeaTunnel's FLOAT type. */
    private static boolean isFloat(JsonNode schema) {
        return schema.path("parameters").path(SOURCE_TYPE).asString().toUpperCase(Locale.ROOT).startsWith("FLOAT");
    }
}
