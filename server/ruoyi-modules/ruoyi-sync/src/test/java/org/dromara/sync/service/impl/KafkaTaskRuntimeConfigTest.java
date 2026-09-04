package org.dromara.sync.service.impl;

import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class KafkaTaskRuntimeConfigTest {

    @Test
    void generatesVersionedRawTopicJobForKafkaTarget() {
        SyncTask task = task("FULL_CDC");
        SeaTunnelJobConfigGenerator.GeneratedConfig config = SeaTunnelJobConfigGenerator.generate(task, mysql(), kafka(), new SeaTunnelProperties());

        assertEquals("customer-events", config.targetTable());
        assertEquals("__ds_raw_42_v3", KafkaTaskBridgeService.rawTopic(task));
        assertTrue(config.config().contains("topic = \"__ds_raw_42_v3\""));
        assertTrue(config.config().contains("bootstrap.servers = \"broker.example:9092\""));
        assertTrue(config.config().contains("partition_key_fields = [\"id\"]"));
        assertTrue(config.config().contains("format = \"DEBEZIUM_JSON\""));
    }

    @Test
    void generatesBoundedKafkaFullSnapshotJob() {
        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL"), mysql(), kafka(), new SeaTunnelProperties());
        assertTrue(config.config().contains("job.mode = \"BATCH\""));
        assertTrue(config.config().contains("format = \"JSON\""));
        assertTrue(config.config().contains("SELECT `id`, `display_name` FROM"));
        assertTrue(config.config().contains("topic = \"__ds_raw_42_v3\""));
    }

    @Test
    void createsBrokerAddressFromKafkaDataSource() {
        assertEquals("broker.example:9092", KafkaTaskBridgeService.bootstrapServers(kafka()));
    }

    @Test
    void generatesMysqlTargetJdbcConfig() {
        DataSource target = mysqlTarget();
        SeaTunnelJobConfigGenerator.GeneratedConfig config =
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), target, new SeaTunnelProperties());

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
            SeaTunnelJobConfigGenerator.generate(task("FULL_CDC"), mysql(), kafka(), properties);

        assertTrue(config.config().contains("jdbc:mysql://mysql-in-engine:3306/source_db"));
        assertTrue(config.config().contains("bootstrap.servers = \"kafka-in-engine:9092\""));
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
