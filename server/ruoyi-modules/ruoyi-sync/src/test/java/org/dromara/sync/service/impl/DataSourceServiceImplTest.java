package org.dromara.sync.service.impl;

import org.dromara.sync.domain.DataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        for (String field : new String[]{"type", "host", "port", "database", "schema", "username", "ssl"}) {
            DataSource changed = dataSource();
            switch (field) {
                case "type" -> changed.setSourceType("POSTGRESQL");
                case "host" -> changed.setHost("other.example");
                case "port" -> changed.setPort(3307);
                case "database" -> changed.setDatabaseName("other_db");
                case "schema" -> changed.setSchemaName("public");
                case "username" -> changed.setUsername("someone");
                default -> changed.setSslEnabled("1");
            }
            assertTrue(DataSourceServiceImpl.connectionChanged(current, changed), field);
        }
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
