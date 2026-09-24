package org.dromara.sync.e2e;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Null-tolerant accessors over Jackson trees; the platform serializes Long ids as strings. */
final class Json {

    static final JsonMapper MAPPER = JsonMapper.builder().build();

    private Json() {
    }

    static JsonNode parse(String body) {
        return MAPPER.readTree(body == null || body.isBlank() ? "null" : body);
    }

    static String write(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    static boolean isAbsent(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode();
    }

    /** Field as text; null when missing, JSON null or blank. */
    static String text(JsonNode node, String field) {
        if (isAbsent(node)) return null;
        JsonNode value = node.get(field);
        if (isAbsent(value)) return null;
        String text = value.isValueNode() ? value.asString() : value.toString();
        return text == null || text.isBlank() ? null : text;
    }

    static Long longValue(JsonNode node, String field) {
        String text = text(node, field);
        return text == null ? null : Long.valueOf(text);
    }

    static boolean bool(JsonNode node, String field) {
        return "true".equalsIgnoreCase(text(node, field));
    }
}
