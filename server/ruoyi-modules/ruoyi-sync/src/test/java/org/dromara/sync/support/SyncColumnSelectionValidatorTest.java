package org.dromara.sync.support;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceIndexVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The projection + sync-key rules every task and table item passes through on save, start and
 * reinitialize. A wrong answer here either drops columns silently or lets a task run on a key
 * that cannot support idempotent upsert/delete.
 */
@Tag("dev")
class SyncColumnSelectionValidatorTest {

    @Test
    void anEmptySelectionMeansTheWholeTableAndTheKeyDefaultsToThePrimaryKey() {
        SyncColumnSelectionValidator.Selection selection =
            SyncColumnSelectionValidator.validate(metadata(), null, null);

        assertEquals(List.of("id", "email", "display_name"), selection.selectedColumns());
        assertEquals(List.of("id"), selection.syncKeyColumns());
    }

    @Test
    void columnNamesAreMatchedCaseInsensitivelyButKeepTheSourcesOwnSpelling() {
        SyncColumnSelectionValidator.Selection selection =
            SyncColumnSelectionValidator.validate(metadata(), " ID , Display_Name ", "id");

        assertEquals(List.of("id", "display_name"), selection.selectedColumns());
        assertEquals(List.of("id"), selection.syncKeyColumns());
    }

    @Test
    void aDuplicatedColumnIsKeptOnlyOnce() {
        assertEquals(List.of("id", "email"),
            SyncColumnSelectionValidator.validate(metadata(), "id,email,ID,email", "id").selectedColumns());
    }

    @Test
    void aColumnThatIsNotInTheSourceIsRejectedByName() {
        ServiceException ex = assertThrows(ServiceException.class,
            () -> SyncColumnSelectionValidator.validate(metadata(), "id,nope", "id"));
        assertTrue(ex.getMessage().contains("同步字段不存在于源表：nope"), ex.getMessage());
    }

    @Test
    void theSyncKeyMustBeThePrimaryKeyOrAnAllNotNullUniqueIndex() {
        // The all-NOT NULL unique index is accepted...
        assertEquals(List.of("email"),
            SyncColumnSelectionValidator.validate(metadata(), null, "email").syncKeyColumns());
        // ...a nullable unique index is not, and neither is an arbitrary column.
        assertTrue(assertThrows(ServiceException.class,
            () -> SyncColumnSelectionValidator.validate(metadata(), null, "display_name"))
            .getMessage().contains("同步键必须选择源表主键或所有字段均为非空的唯一索引"));
    }

    @Test
    void theSyncKeyCannotBeExcludedFromTheProjection() {
        ServiceException ex = assertThrows(ServiceException.class,
            () -> SyncColumnSelectionValidator.validate(metadata(), "email,display_name", "id"));
        assertTrue(ex.getMessage().contains("同步键字段不能排除：id"), ex.getMessage());
    }

    @Test
    void aTableWithoutAnyReliableKeyYieldsAnEmptyKeyRatherThanGuessing() {
        DataSourceMetadataVo keyless = new DataSourceMetadataVo();
        keyless.getColumns().add(column("a"));
        keyless.getColumns().add(column("b"));
        // A unique index that allows NULLs is not a usable sync key.
        keyless.getUniqueKeys().add(uniqueKey("uk_a", false, "a"));

        SyncColumnSelectionValidator.Selection selection =
            SyncColumnSelectionValidator.validate(keyless, null, null);

        assertEquals(List.of("a", "b"), selection.selectedColumns());
        assertTrue(selection.syncKeyColumns().isEmpty());
    }

    @Test
    void aCompositeKeyMustMatchTheIndexColumnOrder() {
        DataSourceMetadataVo composite = new DataSourceMetadataVo();
        composite.getColumns().add(column("tenant_id"));
        composite.getColumns().add(column("order_no"));
        composite.getPrimaryKeys().addAll(List.of("tenant_id", "order_no"));

        assertEquals(List.of("tenant_id", "order_no"),
            SyncColumnSelectionValidator.validate(composite, null, "tenant_id,order_no").syncKeyColumns());
        assertThrows(ServiceException.class,
            () -> SyncColumnSelectionValidator.validate(composite, null, "order_no,tenant_id"));
    }

    @Test
    void aSourceWithoutColumnsIsRejected() {
        assertTrue(assertThrows(ServiceException.class,
            () -> SyncColumnSelectionValidator.validate(new DataSourceMetadataVo(), null, null))
            .getMessage().contains("源表没有可同步字段"));
        assertTrue(assertThrows(ServiceException.class,
            () -> SyncColumnSelectionValidator.validate(null, null, null))
            .getMessage().contains("源表没有可同步字段"));
    }

    @Test
    void parseAndSerializeRoundTripAndTreatBlanksAsAbsent() {
        assertEquals(List.of("a", "b"), SyncColumnSelectionValidator.parseColumns(" a , ,b "));
        assertTrue(SyncColumnSelectionValidator.parseColumns("  ").isEmpty());
        assertTrue(SyncColumnSelectionValidator.parseColumns(null).isEmpty());
        assertEquals("a,b", SyncColumnSelectionValidator.serialize(List.of("a", "b")));
        // Null (not "") so the not_null update strategy leaves the column untouched.
        assertNull(SyncColumnSelectionValidator.serialize(List.of()));
        assertNull(SyncColumnSelectionValidator.serialize(null));
    }

    /** customers-shaped: id PK, email unique NOT NULL, display_name unique but nullable. */
    private static DataSourceMetadataVo metadata() {
        DataSourceMetadataVo metadata = new DataSourceMetadataVo();
        metadata.getColumns().add(column("id"));
        metadata.getColumns().add(column("email"));
        metadata.getColumns().add(column("display_name"));
        metadata.getPrimaryKeys().add("id");
        metadata.getUniqueKeys().add(uniqueKey("uk_email", true, "email"));
        metadata.getUniqueKeys().add(uniqueKey("uk_display_name", false, "display_name"));
        return metadata;
    }

    private static DataSourceColumnVo column(String name) {
        DataSourceColumnVo column = new DataSourceColumnVo();
        column.setName(name);
        column.setTypeName("varchar");
        return column;
    }

    private static DataSourceIndexVo uniqueKey(String name, boolean allNotNull, String... columns) {
        DataSourceIndexVo index = new DataSourceIndexVo();
        index.setName(name);
        index.setAllNotNull(allNotNull);
        index.setColumns(List.of(columns));
        return index;
    }
}
