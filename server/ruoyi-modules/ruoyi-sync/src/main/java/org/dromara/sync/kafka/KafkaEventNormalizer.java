package org.dromara.sync.kafka;

import org.dromara.common.core.exception.ServiceException;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Converts raw change events into the PRD Kafka event envelope. The input is what
 * {@link KafkaRawRecordReader} makes of a raw record: {@code op} {@code r} (initial load) /
 * {@code c} / {@code u} / {@code d}, the {@code before} / {@code after} images and {@code ts_ms}.
 * Debezium's own records carry an UPDATE as one {@code u}; SeaTunnel's {@code DEBEZIUM_JSON} records
 * split it into an adjacent {@code d} + {@code c}, which is merged back here.
 */
@Component
public class KafkaEventNormalizer {

    private final JsonMapper jsonMapper;

    public KafkaEventNormalizer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    /** {@link #normalize(List, int, List, List)} publishing every column of the source rows. */
    public List<NormalizedEvent> normalize(List<JsonNode> rawEvents, int snapshotCount, List<String> keyFields) {
        return normalize(rawEvents, snapshotCount, keyFields, List.of());
    }

    /**
     * {@code selectedColumns} is the task's column selection (empty = every column). MySQL-CDC
     * has no column projection of its own, so a change record always carries the whole row;
     * the before / after images are cut down to the selection here, before anything is published.
     */
    public List<NormalizedEvent> normalize(List<JsonNode> changeEvents, int snapshotCount, List<String> keyFields,
                                           List<String> selectedColumns) {
        if (changeEvents == null || changeEvents.isEmpty()) throw new ServiceException("Kafka 原始事件不能为空");
        if (snapshotCount < 0 || snapshotCount > changeEvents.size()) throw new ServiceException("Kafka 快照事件数量无效");
        if (keyFields == null || keyFields.isEmpty()) throw new ServiceException("Kafka 必须配置可靠同步键");
        List<JsonNode> rawEvents = selectedColumns == null || selectedColumns.isEmpty()
            ? changeEvents : project(changeEvents, selectedColumns);
        List<NormalizedEvent> result = new ArrayList<>();
        for (int index = 0; index < rawEvents.size(); index++) {
            JsonNode current = rawEvents.get(index);
            String operation = requiredText(current, "op");
            JsonNode source = current.path("source");
            String database = requiredText(source, "database");
            String table = requiredText(source, "table");
            JsonNode currentRow = "d".equals(operation) ? current.path("before") : current.path("after");
            ObjectNode key = key(currentRow, keyFields);

            if ("d".equals(operation) && index + 1 < rawEvents.size()) {
                JsonNode next = rawEvents.get(index + 1);
                if ("c".equals(next.path("op").asText())
                    && database.equals(next.path("source").path("database").asText())
                    && table.equals(next.path("source").path("table").asText())) {
                    ObjectNode nextKey = key(next.path("after"), keyFields);
                    if (key.toString().equals(nextKey.toString())) {
                        result.add(new NormalizedEvent(
                            "UPDATE", key, current.path("before"), next.path("after"),
                            phase(index, snapshotCount), database, table,
                            eventTime(Math.max(timestamp(current), timestamp(next)))));
                        index++;
                        continue;
                    }
                }
            }

            String phase = "r".equals(operation) || index < snapshotCount ? "SNAPSHOT" : "CDC";
            String eventTime = eventTime(timestamp(current));
            if ("u".equals(operation)) {
                // Debezium's own UPDATE (compatible_debezium_json raw records) carries both images.
                JsonNode before = current.path("before");
                ObjectNode beforeKey = before.isObject() ? key(before, keyFields) : key;
                if (beforeKey.toString().equals(key.toString())) {
                    result.add(new NormalizedEvent("UPDATE", key, before.isObject() ? before : null,
                        current.path("after"), phase, database, table, eventTime));
                } else {
                    // The sync key itself changed: the old key is gone, as for a d + c pair with two keys.
                    result.add(new NormalizedEvent("DELETE", beforeKey, before, before, phase, database, table, eventTime));
                    result.add(new NormalizedEvent("INSERT", key, null, current.path("after"), phase, database, table, eventTime));
                }
                continue;
            }
            String normalizedOperation = switch (operation) {
                case "c", "r" -> "INSERT";
                case "d" -> "DELETE";
                default -> throw new ServiceException("不支持的 Debezium 操作类型：" + operation);
            };
            JsonNode data = "DELETE".equals(normalizedOperation) ? current.path("before") : current.path("after");
            result.add(new NormalizedEvent(
                normalizedOperation, key,
                "DELETE".equals(normalizedOperation) ? current.path("before") : null,
                data, phase, database, table, eventTime));
        }
        // No ordering check across phases: a restarted or reinitialized job writes a new initial load
        // (op=r) into the same raw topic after the previous run's changes, and each record's own op
        // says which phase it belongs to. Within one run the engine reads the binlog only after the
        // whole initial load (docs/kafka-event-formats.md).
        return List.copyOf(result);
    }

    /** Converts bounded JDBC JSON rows into the same PRD snapshot event contract. */
    public List<NormalizedEvent> normalizeSnapshotRows(List<JsonNode> rows, String database, String table,
                                                        List<String> keyFields, List<String> canonicalColumns) {
        if (rows == null || rows.isEmpty()) return List.of();
        if (database == null || database.isBlank() || table == null || table.isBlank()) {
            throw new ServiceException("Kafka 快照缺少源库或源表");
        }
        if (keyFields == null || keyFields.isEmpty()) throw new ServiceException("Kafka 必须配置可靠同步键");
        String eventTime = Instant.now().toString();
        List<NormalizedEvent> result = new ArrayList<>(rows.size());
        for (JsonNode raw : rows) {
            if (raw == null || !raw.isObject()) throw new ServiceException("Kafka 快照行不是 JSON 对象");
            JsonNode row = canonicalizeRow(raw, canonicalColumns);
            result.add(new NormalizedEvent("INSERT", key(row, keyFields), null, row,
                "SNAPSHOT", database, table, eventTime));
        }
        return List.copyOf(result);
    }

    /**
     * GoldenDB in Oracle-compatible mode (ORA_COMPATIBLE_MODE) returns JDBC result-set
     * column labels folded to UPPER CASE, so a bounded snapshot row arrives as {"ID":1}
     * while the platform's introspected schema and the CDC (Debezium) path both use the
     * table's real casing. Rebuild the row with the canonical column names. Native MySQL
     * already reports the real casing, so every lookup hits on the first try and the
     * original node is returned unchanged.
     */
    private JsonNode canonicalizeRow(JsonNode row, List<String> canonicalColumns) {
        if (canonicalColumns == null || canonicalColumns.isEmpty()) return row;
        ObjectNode rebuilt = jsonMapper.createObjectNode();
        boolean remapped = false;
        for (String column : canonicalColumns) {
            JsonNode value = row.get(column);
            if (value == null) {
                value = row.get(column.toUpperCase(Locale.ROOT));
                if (value == null) value = row.get(column.toLowerCase(Locale.ROOT));
                if (value != null) remapped = true;
            }
            if (value != null) rebuilt.set(column, value);
        }
        // Only substitute when every field was accounted for; otherwise keep the
        // original row so an unexpected shape never silently drops columns.
        return remapped && rebuilt.size() == row.size() ? rebuilt : row;
    }

    /** Copies of the events with {@code before} / {@code after} limited to the selected columns, in row order. */
    private List<JsonNode> project(List<JsonNode> events, List<String> selectedColumns) {
        Set<String> selected = new HashSet<>();
        for (String column : selectedColumns) selected.add(column.toLowerCase(Locale.ROOT));
        List<JsonNode> projected = new ArrayList<>(events.size());
        for (JsonNode event : events) {
            if (!(event instanceof ObjectNode source)) {
                projected.add(event);
                continue;
            }
            ObjectNode copy = jsonMapper.createObjectNode();
            copy.setAll(source);
            for (String image : List.of("before", "after")) {
                JsonNode row = source.get(image);
                if (row == null || !row.isObject()) continue;
                ObjectNode kept = jsonMapper.createObjectNode();
                for (var entry : row.properties()) {
                    if (selected.contains(entry.getKey().toLowerCase(Locale.ROOT))) kept.set(entry.getKey(), entry.getValue());
                }
                copy.set(image, kept);
            }
            projected.add(copy);
        }
        return projected;
    }

    private ObjectNode key(JsonNode row, List<String> keyFields) {
        if (row == null || row.isMissingNode() || row.isNull()) throw new ServiceException("Kafka 事件缺少可靠同步键数据");
        ObjectNode key = jsonMapper.createObjectNode();
        for (String field : keyFields) {
            JsonNode value = fieldValue(row, field);
            if (value == null || value.isNull()) throw new ServiceException("Kafka 同步键字段缺失或为空：" + field);
            key.set(field, value);
        }
        return key;
    }

    /** Resolves a field case-insensitively so an Oracle-compat UPPER CASE label still matches. */
    private static JsonNode fieldValue(JsonNode row, String field) {
        JsonNode value = row.get(field);
        if (value == null) value = row.get(field.toUpperCase(Locale.ROOT));
        if (value == null) value = row.get(field.toLowerCase(Locale.ROOT));
        return value;
    }

    private static String phase(int index, int snapshotCount) {
        return index < snapshotCount ? "SNAPSHOT" : "CDC";
    }

    private static long timestamp(JsonNode event) {
        JsonNode value = event.get("ts_ms");
        if (value == null || !value.canConvertToLong()) throw new ServiceException("Kafka 事件缺少有效 ts_ms");
        return value.asLong();
    }

    private static String eventTime(long timestamp) {
        try {
            return Instant.ofEpochMilli(timestamp).toString();
        } catch (RuntimeException ex) {
            throw new ServiceException("Kafka 事件时间无效");
        }
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new ServiceException("Kafka 事件缺少字段：" + field);
        }
        return value.asText();
    }

    public record NormalizedEvent(String op, ObjectNode key, JsonNode before, JsonNode data,
                                  String phase, String sourceDatabase, String sourceTable,
                                  String sourceEventTime) {
    }
}
