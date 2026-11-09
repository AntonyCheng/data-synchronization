package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.mapper.DataSourceMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("dev")
class DataSourceServiceImplTest {

    @Test
    void onlyEndpointAndIdentityFieldsCountAsAConnectionChange() {
        DataSource current = dataSource();

        DataSource renamed = dataSource();
        renamed.setSourceName("renamed");
        renamed.setRemark("note");
        renamed.setStatus("1");
        renamed.setPassword("rotated");
        assertFalse(DataSourceServiceImpl.connectionChanged(current, renamed), "name / remark / status / password are free to change");

        DataSource blankSchema = dataSource();
        blankSchema.setSchemaName("");
        assertFalse(DataSourceServiceImpl.connectionChanged(current, blankSchema), "null and blank optional fields are the same endpoint");

        for (String field : new String[]{"type", "host", "port", "database", "schema", "username", "ssl", "timezone"}) {
            DataSource changed = dataSource();
            switch (field) {
                case "type" -> changed.setSourceType("POSTGRESQL");
                case "host" -> changed.setHost("other.example");
                case "port" -> changed.setPort(3307);
                case "database" -> changed.setDatabaseName("other_db");
                case "schema" -> changed.setSchemaName("public");
                case "username" -> changed.setUsername("someone");
                case "timezone" -> changed.setServerTimeZone("UTC");
                default -> changed.setSslEnabled("1");
            }
            assertTrue(DataSourceServiceImpl.connectionChanged(current, changed), field);
        }
    }

    /** Blank and an explicit Asia/Shanghai generate the same CDC config, so switching between them is no change. */
    @Test
    void writingOutTheCompatibleDefaultIsNotAConnectionChange() {
        DataSource explicit = dataSource();
        explicit.setServerTimeZone("Asia/Shanghai");
        assertFalse(DataSourceServiceImpl.connectionChanged(dataSource(), explicit));
        DataSource cleared = dataSource();
        cleared.setServerTimeZone("");
        assertFalse(DataSourceServiceImpl.connectionChanged(explicit, cleared));
    }

    /** The zone is validated with the other connection fields, before anything connects. */
    @Test
    void anAmbiguousZoneIsRefusedBeforeConnecting() {
        DataSourceMapper mapper = mock(DataSourceMapper.class);
        DataSource stored = dataSource();
        stored.setServerTimeZone("CST");
        when(mapper.selectById(1L)).thenReturn(stored);
        ConnectionTestResult result = new DataSourceServiceImpl(mapper, null, null, null).testConnection(1L, null);
        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("缩写"), result.getMessage());
        assertNull(result.getTimeZone());
    }

    /** ds_data_source keeps database_name / username / password NOT NULL; a Kafka cluster may have none of them. */
    @Test
    void aKafkaClusterWithoutDatabaseOrCredentialsIsStoredWithEmptyStrings() {
        DataSource kafka = new DataSource();
        kafka.setSourceName("kafka");
        kafka.setSourceType("KAFKA");
        kafka.setHost("kafka.example");
        kafka.setPort(9092);
        DataSourceServiceImpl.normalizeAndValidate(kafka, true);
        assertEquals("", kafka.getDatabaseName());
        assertEquals("", kafka.getUsername());
        assertEquals("", kafka.getPassword());

        DataSource named = dataSource();
        named.setSourceType("KAFKA");
        DataSourceServiceImpl.normalizeAndValidate(named, true);
        assertEquals("root", named.getUsername(), "given values are kept");
        assertEquals("secret", named.getPassword());

        DataSource mysql = dataSource();
        mysql.setDatabaseName(null);
        assertThrows(ServiceException.class, () -> DataSourceServiceImpl.normalizeAndValidate(mysql, true), "a relational source still needs its database");
    }

    /** A Kafka cluster's security setup is a connection parameter; its password and client key are credentials. */
    @Test
    void kafkaSecurityCountsAsAConnectionChangeButNotItsCredentials() {
        DataSource current = kafkaCluster();
        DataSource rotated = kafkaCluster();
        rotated.setPassword("rotated");
        rotated.setKafkaSslClientKey("-----BEGIN PRIVATE KEY-----\nother\n-----END PRIVATE KEY-----");
        assertFalse(DataSourceServiceImpl.connectionChanged(current, rotated));

        DataSource plaintext = kafkaCluster();
        plaintext.setKafkaSecurityProtocol(null);
        assertTrue(DataSourceServiceImpl.connectionChanged(current, plaintext));
        DataSource scram = kafkaCluster();
        scram.setKafkaSaslMechanism("SCRAM-SHA-512");
        assertTrue(DataSourceServiceImpl.connectionChanged(current, scram));
        DataSource otherCa = kafkaCluster();
        otherCa.setKafkaSslCaCert("-----BEGIN CERTIFICATE-----\nother\n-----END CERTIFICATE-----");
        assertTrue(DataSourceServiceImpl.connectionChanged(current, otherCa));
        DataSource noHostCheck = kafkaCluster();
        noHostCheck.setKafkaSslVerifyHostname("0");
        assertTrue(DataSourceServiceImpl.connectionChanged(current, noHostCheck));
    }

    /** Saving validates the broker list and the security setup; a relational source keeps no Kafka fields. */
    @Test
    void kafkaFieldsAreValidatedForKafkaAndDroppedForOtherTypes() {
        DataSource cluster = kafkaCluster();
        DataSourceServiceImpl.normalizeAndValidate(cluster, true);
        assertEquals("SASL_SSL", cluster.getKafkaSecurityProtocol());

        DataSource badBroker = kafkaCluster();
        badBroker.setHost("k1:abc");
        assertThrows(ServiceException.class, () -> DataSourceServiceImpl.normalizeAndValidate(badBroker, true));

        DataSource mysql = dataSource();
        mysql.setKafkaSecurityProtocol("SASL_SSL");
        mysql.setKafkaSslCaCert("-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----");
        DataSourceServiceImpl.normalizeAndValidate(mysql, true);
        assertNull(mysql.getKafkaSecurityProtocol());
        assertNull(mysql.getKafkaSslCaCert());
    }

    private static DataSource kafkaCluster() {
        DataSource cluster = new DataSource();
        cluster.setSourceId(2L);
        cluster.setSourceName("kafka");
        cluster.setSourceType("KAFKA");
        cluster.setHost("k1, k2:9093");
        cluster.setPort(9092);
        cluster.setUsername("sync");
        cluster.setPassword("secret");
        cluster.setKafkaSecurityProtocol("SASL_SSL");
        cluster.setKafkaSaslMechanism("SCRAM-SHA-256");
        cluster.setKafkaSslCaCert("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----");
        cluster.setKafkaSslVerifyHostname("1");
        return cluster;
    }

    private static DataSource dataSource() {
        DataSource dataSource = new DataSource();
        dataSource.setSourceId(1L);
        dataSource.setSourceName("mysql");
        dataSource.setSourceType("MYSQL");
        dataSource.setHost("mysql.example");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("source_db");
        dataSource.setSchemaName(null);
        dataSource.setUsername("root");
        dataSource.setPassword("secret");
        dataSource.setSslEnabled("0");
        dataSource.setStatus("0");
        return dataSource;
    }
}
