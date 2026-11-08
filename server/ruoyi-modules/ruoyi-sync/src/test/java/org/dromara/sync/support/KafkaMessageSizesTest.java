package org.dromara.sync.support;

import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class KafkaMessageSizesTest {

    @Test
    void anOrdinaryRowStaysFarBelowKafkasDefaultLimit() {
        KafkaMessageSizes.Estimate estimate = KafkaMessageSizes.estimate(
            List.of(column("id", "BIGINT", 20), column("name", "VARCHAR", 64), column("amount", "DECIMAL", 12), column("created_at", "DATETIME", 19)),
            List.of(), true);

        assertTrue(estimate.eventBytes() < 4 * 1024, "event " + estimate.eventBytes());
        assertTrue(estimate.rawRecordBytes() < 8 * 1024, "raw " + estimate.rawRecordBytes());
        assertEquals(List.of(), estimate.largeColumns());
    }

    @Test
    void aLongTextOrJsonColumnCanExceedAnyLimitAndIsNamed() {
        KafkaMessageSizes.Estimate estimate = KafkaMessageSizes.estimate(
            List.of(column("id", "INT UNSIGNED", 10), column("note", "LONGTEXT", 0), column("attrs", "JSON", 0), column("body", "MEDIUMBLOB", 0)),
            List.of("id", "note"), true);

        assertTrue(estimate.eventBytes() > 1024L * 1024 * 1024, "event " + estimate.eventBytes());
        assertTrue(estimate.rawRecordBytes() >= estimate.eventBytes());
        // A CDC raw record carries the whole row, so unselected large columns count there too.
        assertEquals(List.of("note(LONGTEXT)", "attrs(JSON)", "body(MEDIUMBLOB)"), estimate.largeColumns());
    }

    @Test
    void aFullSnapshotRecordHoldsOnlyTheSelectedColumns() {
        List<DataSourceColumnVo> columns = List.of(column("id", "INT", 10), column("note", "LONGTEXT", 0));

        KafkaMessageSizes.Estimate full = KafkaMessageSizes.estimate(columns, List.of("ID"), false);
        KafkaMessageSizes.Estimate cdc = KafkaMessageSizes.estimate(columns, List.of("ID"), true);

        assertTrue(full.rawRecordBytes() < 2048, "raw " + full.rawRecordBytes());
        assertEquals(List.of(), full.largeColumns());
        assertTrue(cdc.rawRecordBytes() > 1024L * 1024 * 1024, "a CDC raw record still carries note");
        assertEquals(List.of("note(LONGTEXT)"), cdc.largeColumns());
    }

    @Test
    void manyTextColumnsAddUp() {
        List<DataSourceColumnVo> columns = new java.util.ArrayList<>();
        columns.add(column("id", "INT", 10));
        for (int i = 0; i < 10; i++) columns.add(column("t" + i, "TEXT", 65535));

        KafkaMessageSizes.Estimate estimate = KafkaMessageSizes.estimate(columns, List.of(), true);

        // Ten full TEXT values, in both images of an UPDATE: past Kafka's 1 MB default.
        assertTrue(estimate.eventBytes() > 1_048_588L, "event " + estimate.eventBytes());
        assertEquals(List.of(), estimate.largeColumns(), "no single column is large");
    }

    @Test
    void sizesReadLikeAnOperatorWritesThem() {
        assertEquals("800 B", KafkaMessageSizes.human(800));
        assertEquals("1.5 KB", KafkaMessageSizes.human(1536));
        assertEquals("16.0 MB", KafkaMessageSizes.human(16L * 1024 * 1024));
        assertEquals("4.0 GB", KafkaMessageSizes.human(4L * 1024 * 1024 * 1024));
    }

    private static DataSourceColumnVo column(String name, String type, int size) {
        DataSourceColumnVo column = new DataSourceColumnVo();
        column.setName(name);
        column.setTypeName(type);
        column.setSize(size);
        return column;
    }
}
