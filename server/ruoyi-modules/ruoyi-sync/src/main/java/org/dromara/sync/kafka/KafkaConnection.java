package org.dromara.sync.kafka;

import org.dromara.sync.domain.DataSource;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Where and how to reach one Kafka cluster: its bootstrap servers and the client security properties
 * ({@link KafkaSecurity}). Every platform client of a Kafka data source is built from one, so the
 * AdminClient, the bridge's consumer and its producer always authenticate the same way.
 *
 * <p>Two connections are equal when address and security are - a changed password is another
 * connection, so the bridge's cached producer for the old one is not reused. {@link #toString()}
 * never shows the security properties: they carry credentials.
 */
public record KafkaConnection(String bootstrapServers, Map<String, String> security) {

    public KafkaConnection {
        security = security == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(security));
    }

    /** The data source's brokers and security. */
    public static KafkaConnection of(DataSource target) {
        return new KafkaConnection(KafkaAdminClients.bootstrapServers(target), KafkaSecurity.clientProperties(target));
    }

    /** A cluster reached without authentication. */
    public static KafkaConnection plaintext(String bootstrapServers) {
        return new KafkaConnection(bootstrapServers, Map.of());
    }

    /** {@code bootstrap.servers} plus the security properties, for any Kafka client; the caller adds its own. */
    public Properties clientProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        security.forEach(properties::put);
        return properties;
    }

    @Override
    public String toString() {
        return security.isEmpty() ? bootstrapServers : bootstrapServers + " (" + security.get("security.protocol") + ")";
    }
}
