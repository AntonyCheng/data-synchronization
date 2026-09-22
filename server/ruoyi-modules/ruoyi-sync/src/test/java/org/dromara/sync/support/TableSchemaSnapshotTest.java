package org.dromara.sync.support;

import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class TableSchemaSnapshotTest {

    private static final TableSchemaSnapshot.Snapshot BASELINE = TableSchemaSnapshot.of(metadata("id", "name", "email"));

    /** A selection that covered the whole table at start must keep following the table on reinitialize. */
    @Test
    void fullSelectionCoversBaseline() {
        assertTrue(TableSchemaSnapshot.coversAllColumns(BASELINE, List.of("id", "name", "email")));
        // Column names are compared case-insensitively, as everywhere else in the module.
        assertTrue(TableSchemaSnapshot.coversAllColumns(BASELINE, List.of("ID", "Name", "EMAIL")));
    }

    @Test
    void partialSelectionIsKept() {
        assertFalse(TableSchemaSnapshot.coversAllColumns(BASELINE, List.of("id", "name")));
    }

    @Test
    void unknownBaselineNeverWidens() {
        assertFalse(TableSchemaSnapshot.coversAllColumns(null, List.of("id", "name", "email")));
        assertFalse(TableSchemaSnapshot.coversAllColumns(new TableSchemaSnapshot.Snapshot(), List.of("id")));
        assertFalse(TableSchemaSnapshot.coversAllColumns(BASELINE, null));
    }

    @Test
    void addedNullableColumnIsLowRiskAndDroppedColumnIsHigh() {
        TableSchemaSnapshot.Snapshot baseline = TableSchemaSnapshot.of(metadata("id", "name"));
        TableSchemaSnapshot.Diff added = TableSchemaSnapshot.diff(baseline, TableSchemaSnapshot.of(metadata("id", "name", "note")));
        assertTrue(added.changed());
        assertEquals("ADD_COLUMN", added.changeType());
        assertEquals("LOW", added.riskLevel());

        TableSchemaSnapshot.Diff dropped = TableSchemaSnapshot.diff(baseline, TableSchemaSnapshot.of(metadata("id")));
        assertEquals("DROP_COLUMN", dropped.changeType());
        assertEquals("HIGH", dropped.riskLevel());

        assertFalse(TableSchemaSnapshot.diff(baseline, TableSchemaSnapshot.of(metadata("id", "name"))).changed());
    }

    /** The JSON is persisted in ds_sync_task_group_item.schema_snapshot; its shape is a contract. */
    @Test
    void snapshotJsonShapeIsStableAndRoundTrips() {
        TableSchemaSnapshot.Snapshot snapshot = TableSchemaSnapshot.of(metadata("Id", "name"));
        String json = TableSchemaSnapshot.toJson(snapshot);
        assertEquals("{\"columns\":[{\"name\":\"id\",\"typeName\":\"varchar\",\"jdbcType\":null,\"size\":null,\"scale\":null,\"nullable\":true},"
            + "{\"name\":\"name\",\"typeName\":\"varchar\",\"jdbcType\":null,\"size\":null,\"scale\":null,\"nullable\":true}],\"primaryKeys\":[\"id\"]}", json);
        assertEquals(snapshot, TableSchemaSnapshot.fromJson(json));
        // A field added to the format later must not break reading rows written before it.
        assertTrue(TableSchemaSnapshot.fromJson("{\"columns\":[],\"primaryKeys\":[],\"unknownLater\":1}").getColumns().isEmpty());
    }

    private static DataSourceMetadataVo metadata(String... columns) {
        DataSourceMetadataVo metadata = new DataSourceMetadataVo();
        for (String name : columns) {
            DataSourceColumnVo column = new DataSourceColumnVo();
            column.setName(name);
            column.setTypeName("varchar");
            column.setNullable(true);
            metadata.getColumns().add(column);
        }
        metadata.getPrimaryKeys().add("id");
        return metadata;
    }
}
