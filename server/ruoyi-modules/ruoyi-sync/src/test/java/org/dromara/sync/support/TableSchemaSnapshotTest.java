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
