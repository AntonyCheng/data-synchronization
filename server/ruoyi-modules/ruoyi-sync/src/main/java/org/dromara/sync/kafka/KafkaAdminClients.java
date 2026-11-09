package org.dromara.sync.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

/** Builds short-lived {@link AdminClient}s against a Kafka data source with the platform's timeouts. */
public final class KafkaAdminClients {

    private KafkaAdminClients() {
    }

    /**
     * The data source's {@code bootstrap.servers}: its host field may list several brokers separated by
     * commas, each {@code host} or {@code host:port}; one without a port takes the data source's port.
     * A single host gives {@code host:port} exactly as before.
     */
    public static String bootstrapServers(DataSource target) {
        return String.join(",", brokers(target, (host, port) -> host + ':' + port));
    }

    /** Each broker of the data source mapped by {@code endpoint} (host, port) - e.g. to the address the engine reaches it at. */
    public static List<String> brokers(DataSource target, BiFunction<String, Integer, String> endpoint) {
        if (target == null || StringUtils.isBlank(target.getHost()) || target.getPort() == null) {
            throw new ServiceException("Kafka broker 地址不能为空");
        }
        List<String> brokers = new ArrayList<>();
        for (String entry : target.getHost().split(",")) {
            String broker = entry.trim();
            if (broker.isEmpty()) continue;
            int colon = broker.lastIndexOf(':');
            // host:port; a bracketed IPv6 address keeps its own colons ([::1]:9092).
            boolean hasPort = colon >= 0 && broker.indexOf(':') == colon || broker.startsWith("[") && broker.lastIndexOf(']') == colon - 1;
            if (!hasPort) {
                brokers.add(endpoint.apply(broker, target.getPort()));
                continue;
            }
            String host = broker.substring(0, colon).trim();
            int port;
            try {
                port = Integer.parseInt(broker.substring(colon + 1).trim());
            } catch (NumberFormatException ex) {
                throw new ServiceException("Kafka broker 地址格式不正确：" + broker + "（应为 主机 或 主机:端口，多个用逗号分隔）");
            }
            if (host.isEmpty() || port < 1 || port > 65535) {
                throw new ServiceException("Kafka broker 地址格式不正确：" + broker + "（应为 主机 或 主机:端口，多个用逗号分隔）");
            }
            brokers.add(endpoint.apply(host, port));
        }
        if (brokers.isEmpty()) throw new ServiceException("Kafka broker 地址不能为空");
        return brokers;
    }

    public static Properties adminProperties(KafkaConnection connection) {
        Properties properties = connection.clientProperties();
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        return properties;
    }

    /** Caller closes it; every admin call must also carry its own {@code get(timeout)}. */
    public static AdminClient open(DataSource target) {
        return AdminClient.create(adminProperties(KafkaConnection.of(target)));
    }
}
