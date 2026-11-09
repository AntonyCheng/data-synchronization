package org.dromara.sync.engine;

import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.kafka.KafkaAdminClients;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class KafkaTaskRuntimeConfigTest {

    private static final SourceColumns COLUMNS = SourceColumns.fixed(List.of("id", "display_name", "email"));

    @Test
    void generatesVersionedRawTopicJobForKafkaTarget() {
        SyncTask task = task("FULL_CDC");
        SeaTunnelJobConfigGenerator.GeneratedConfig config = SeaTunnelJobConfigGenerator.generate(task, mysql(), kafka(), new SeaTunnelProperties(), COLUMNS);

        assertEquals("customer-events", config.targetTable());
        assertEquals("__ds_raw_42_v3", KafkaTaskBridgeService.rawTopic(task));
        assertTrue(config.config().contains("topic = \"__ds_raw_42_v3\""));
        assertTrue(config.config().contains("bootstrap.servers = \"broker.example:9092\""));
        // Debezium's own records, value schema on, FLOAT / TINYINT source types propagated - what the
        // bridge needs to publish the initial load as SNAPSHOT with unchanged values.
        assertTrue(config.config().contains("    format = \"compatible_debezium_json\"\n"
            + "    debezium = {\n"
            + "      key.converter.schemas.enable = false\n"
            + "      value.converter.schemas.enable = true\n"
            + "      datatype.propagate.source.type = \".+[.]TINYINT,.+[.]FLOAT( UNSIGNED)?( ZEROFILL)?\"\n"
            + "    }\n"), config.config());
        assertTrue(config.config().contains("format = \"COMPATIBLE_DEBEZIUM_JSON\""));
        assertFalse(config.config().contains("\"DEBEZIUM_JSON\""), "SeaTunnel's row format is no longer written");
        assertFalse(config.config().contains("partition_key_fields"), "the rows are Connect JSON strings, not columns");
    }

    @Test
    void generatesBoundedKafkaFullSnapshotJob() {
        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL"), mysql(), kafka(), new SeaTunnelProperties(), COLUMNS);
        assertTrue(config.config().contains("job.mode = \"BATCH\""));
        assertTrue(config.config().contains("format = \"JSON\""));
        assertTrue(config.config().contains("SELECT `id`, `display_name` FROM"));
        assertTrue(config.config().contains("topic = \"__ds_raw_42_v3\""));
    }

    @Test
    void createsBrokerAddressFromKafkaDataSource() {
        assertEquals("broker.example:9092", KafkaAdminClients.bootstrapServers(kafka()));
    }

    @Test
    void generatesMysqlTargetJdbcConfig() {
        DataSource target = mysqlTarget();
        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), target, new SeaTunnelProperties(), COLUMNS);

        assertEquals("customer-events", config.targetTable());
        assertTrue(config.config().contains("url = \"jdbc:mysql://target.example:3306/sink_db?"));
        assertTrue(config.config().contains("driver = \"com.mysql.cj.jdbc.Driver\""));
        assertTrue(config.config().contains("table = \"customer-events\""));
    }

    @Test
    void usesEngineNetworkOverridesOnlyForSubmittedConfig() {
        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setConnectionEndpointOverrides(Map.of(
            "mysql.example:3306", "mysql-in-engine:3306",
            "broker.example:9092", "kafka-in-engine:9092"));

        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), kafka(), properties, COLUMNS);

        assertTrue(config.config().contains("jdbc:mysql://mysql-in-engine:3306/source_db"));
        assertTrue(config.config().contains("bootstrap.servers = \"kafka-in-engine:9092\""));
    }

    /** A cluster without authentication: the engine producer config every existing Kafka task was fingerprinted with. */
    @Test
    void aPlaintextClusterKeepsTheProducerConfigItAlwaysHad() {
        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), kafka(), new SeaTunnelProperties(), COLUMNS);
        assertTrue(config.config().contains(
            "    kafka.config = { acks = \"all\", enable.idempotence = \"true\", max.request.size = \"16777216\" }\n"), config.config());
    }

    /**
     * SASL / SSL go into the engine's producer config on the same line, the secrets redacted from the
     * API-facing copy and the whole security setup kept out of the fingerprint: turning authentication
     * on for a cluster, or rotating its password, never strands a savepoint.
     */
    @Test
    void securityReachesTheEngineProducerRedactedAndOutsideTheFingerprint() {
        SeaTunnelProperties properties = new SeaTunnelProperties();
        SeaTunnelJobConfigGenerator.GeneratedConfig plain =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), kafka(), properties, COLUMNS);
        DataSource secured = kafka();
        secured.setKafkaSecurityProtocol("SASL_SSL");
        secured.setKafkaSaslMechanism("SCRAM-SHA-256");
        secured.setUsername("sync");
        secured.setPassword("s3cr\"t");
        secured.setKafkaSslCaCert("-----BEGIN CERTIFICATE-----\nAAA\n-----END CERTIFICATE-----");
        secured.setKafkaSslVerifyHostname("0");

        SeaTunnelJobConfigGenerator.GeneratedConfig secure =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), secured, properties, COLUMNS);

        String producerLine = secure.config().lines().filter(line -> line.contains("kafka.config")).findFirst().orElseThrow();
        assertTrue(producerLine.endsWith(" }"), producerLine);
        assertTrue(producerLine.contains(", security.protocol = \"SASL_SSL\", sasl.mechanism = \"SCRAM-SHA-256\""), producerLine);
        assertTrue(producerLine.contains("ScramLoginModule required username=\\\"sync\\\" password=\\\"s3cr\\\\\\\"t\\\";\""), producerLine);
        assertTrue(producerLine.contains("ssl.truststore.certificates = \"-----BEGIN CERTIFICATE-----\\nAAA\\n-----END CERTIFICATE-----\""), producerLine);
        assertTrue(producerLine.contains("ssl.endpoint.identification.algorithm = \"\""), producerLine);

        assertFalse(secure.redactedConfig().contains("s3cr"), secure.redactedConfig());
        assertTrue(secure.redactedConfig().contains("sasl.jaas.config = \"******\""));
        assertEquals(plain.fingerprint(), secure.fingerprint());
        secured.setPassword("rotated");
        assertEquals(plain.fingerprint(),
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), secured, properties, COLUMNS).fingerprint());
    }

    @Test
    void everyBrokerOfAListIsMappedToTheEngineNetwork() {
        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setConnectionEndpointOverrides(Map.of("k1:9092", "kafka-a:9092"));
        DataSource cluster = kafka();
        cluster.setHost("k1, k2:9093");

        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), cluster, properties, COLUMNS);

        assertTrue(config.config().contains("bootstrap.servers = \"kafka-a:9092,k2:9093\""), config.config());
    }

    private static SyncTask task(String syncMode) {
        SyncTask task = new SyncTask();
        task.setTaskId(42L);
        task.setConfigVersion(3);
        task.setSourceTable("source_db.customers");
        task.setTargetTable("customer-events");
        task.setSyncMode(syncMode);
        task.setSelectedColumns("id,display_name");
        task.setSyncKeyColumns("id");
        task.setSnapshotParallelism(1);
        task.setReadLimitRowsPerSecond(100);
        task.setReadLimitBytesPerSecond(1024L);
        task.setSourceConnectionLimit(2);
        return task;
    }

    private static DataSource mysql() {
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setHost("mysql.example");
        source.setPort(3306);
        source.setDatabaseName("source_db");
        source.setUsername("reader");
        source.setPassword("secret");
        source.setSslEnabled("0");
        return source;
    }

    private static DataSource kafka() {
        DataSource target = new DataSource();
        target.setSourceType("KAFKA");
        target.setHost("broker.example");
        target.setPort(9092);
        return target;
    }

    private static DataSource mysqlTarget() {
        DataSource target = new DataSource();
        target.setSourceType("MYSQL");
        target.setHost("target.example");
        target.setPort(3306);
        target.setDatabaseName("sink_db");
        target.setUsername("writer");
        target.setPassword("secret");
        target.setSslEnabled("0");
        return target;
    }
}
