package org.dromara.sync.domain;

import org.dromara.common.core.exception.ServiceException;

/**
 * Wire format the Kafka bridge serializes normalized events into on the target topic.
 * The engine-side raw topic is unaffected - this only chooses the message body the
 * platform bridge publishes, so engine config fingerprints stay stable across formats.
 */
public enum KafkaOutputFormat {

    /** Platform PRD event envelope (historical default). */
    ENVELOPE,
    CANAL_JSON,
    COMPATIBLE_DEBEZIUM_JSON,
    MAXWELL_JSON,
    OGG_JSON;

    private static final String SUPPORTED = "ENVELOPE/CANAL_JSON/COMPATIBLE_DEBEZIUM_JSON/MAXWELL_JSON/OGG_JSON";

    /** Blank resolves to the default {@link #ENVELOPE}; an unknown value is rejected. */
    public static KafkaOutputFormat parse(String value) {
        if (value == null || value.isBlank()) return ENVELOPE;
        try {
            return KafkaOutputFormat.valueOf(value.trim());
        } catch (IllegalArgumentException ex) {
            throw new ServiceException("不支持的 Kafka 输出格式：" + value + "，可选 " + SUPPORTED);
        }
    }
}
