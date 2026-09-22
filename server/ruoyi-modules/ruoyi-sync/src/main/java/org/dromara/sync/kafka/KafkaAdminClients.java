package org.dromara.sync.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;

import java.util.Properties;

/** Builds short-lived {@link AdminClient}s against a Kafka data source with the platform's timeouts. */
public final class KafkaAdminClients {

    private KafkaAdminClients() {
    }

    public static String bootstrapServers(DataSource target) {
        if (target == null || StringUtils.isBlank(target.getHost()) || target.getPort() == null) {
            throw new ServiceException("Kafka broker 地址不能为空");
        }
        return target.getHost() + ':' + target.getPort();
    }

    public static Properties adminProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        return properties;
    }

    /** Caller closes it; every admin call must also carry its own {@code get(timeout)}. */
    public static AdminClient open(DataSource target) {
        return AdminClient.create(adminProperties(bootstrapServers(target)));
    }
}
