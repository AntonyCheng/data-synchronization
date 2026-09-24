package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckItemResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.IDataSourceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Row-count check of a task group: one read-only comparison per table, each outcome persisted on
 * its item, a failing table counted apart instead of aborting the others or passing for a match.
 */
@Tag("dev")
class SyncTaskGroupDataCheckServiceImplTest {

    private static final long GROUP_ID = 7L;
    private static final long MYSQL_ID = 1L;
    private static final long POSTGRES_ID = 2L;
    private static final long KAFKA_ID = 3L;

    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final IDataSourceService dataSourceService = mock(IDataSourceService.class);
    private final IDataConsistencyService consistencyService = mock(IDataConsistencyService.class);

    private final SyncTaskGroupDataCheckServiceImpl service =
        new SyncTaskGroupDataCheckServiceImpl(groupMapper, itemMapper, dataSourceService, consistencyService);

    private final DataSource mysql = dataSource(MYSQL_ID, "MYSQL", "source_db");
    private final DataSource postgres = dataSource(POSTGRES_ID, "POSTGRESQL", "sink_db");
    private final DataSource kafka = dataSource(KAFKA_ID, "KAFKA", null);
    private final List<SyncTaskGroupItem> items = new ArrayList<>();

    @BeforeEach
    void wireCollaborators() {
        when(dataSourceService.requireById(eq(MYSQL_ID), anyString())).thenReturn(mysql);
        when(dataSourceService.requireById(eq(POSTGRES_ID), anyString())).thenReturn(postgres);
        when(dataSourceService.requireById(eq(KAFKA_ID), anyString())).thenReturn(kafka);
        when(itemMapper.selectByGroupId(GROUP_ID)).thenAnswer(invocation -> new ArrayList<>(items));
    }

    @Test
    void everyTableIsCheckedAndPersistedAndAFailingOneNeitherStopsNorPassesForTheOthers() {
        persisted(group("STOPPED", POSTGRES_ID));
        SyncTaskGroupItem matched = item(11L, "customers");
        SyncTaskGroupItem mismatched = item(12L, "orders");
        SyncTaskGroupItem unreachable = item(13L, "invoices");
        // No schema / database of its own: the target's default schema and the source's database apply.
        unreachable.setTargetSchema(null);
        unreachable.setSourceDatabase(null);
        items.addAll(List.of(matched, mismatched, unreachable));
        when(consistencyService.check(mysql, postgres, "source_db", "customers", "public", "customers")).thenReturn(counted(10, 10));
        when(consistencyService.check(mysql, postgres, "source_db", "orders", "public", "orders")).thenReturn(counted(10, 7));
        when(consistencyService.check(mysql, postgres, "source_db", "invoices", "public", "invoices"))
            .thenThrow(new ServiceException("目标库连接超时"));

        SyncTaskGroupDataCheckResult result = service.checkData(GROUP_ID);

        assertEquals(3, result.getTableCount());
        assertEquals(1, result.getMatchedTableCount());
        assertEquals(1, result.getMismatchedTableCount());
        assertEquals(1, result.getFailedTableCount());
        assertFalse(result.isSuccess());
        assertFalse(result.isMatched());
        assertEquals("已核对 3 张表：一致 1，不一致 1，失败 1", result.getMessage());
        assertTrue(result.getConsistencyNote().contains("只读行数检查"), result.getConsistencyNote());

        SyncTaskGroupDataCheckItemResult failed = result.getItems().get(2);
        assertEquals("source_db.invoices", failed.getSourceTable());
        assertEquals("public.invoices", failed.getTargetTable());
        assertEquals("数据核对失败：目标库连接超时", failed.getMessage());

        // Only the check columns are written - never the whole (by now stale) row.
        verify(itemMapper).recordCheck(eq(matched.getItemId()), eq(10L), eq(10L), eq(0L), eq("1"), notNull(), anyString());
        verify(itemMapper).recordCheck(eq(mismatched.getItemId()), eq(10L), eq(7L), eq(3L), eq("0"), notNull(), anyString());
        // A failed check writes an explicit null verdict, clearing whatever the previous check concluded.
        verify(itemMapper).recordCheck(eq(unreachable.getItemId()), any(), any(), any(), isNull(), notNull(),
            eq("数据核对失败：目标库连接超时"));
        verify(itemMapper, never()).updateById(any(SyncTaskGroupItem.class));
    }

    @Test
    void aCheckOfALiveGroupSaysTheCountsAreNotAtOneWatermark() {
        persisted(group("DEGRADED", POSTGRES_ID));
        items.add(item(11L, "customers"));
        items.add(item(12L, "orders"));
        when(consistencyService.check(any(), any(), anyString(), anyString(), anyString(), anyString())).thenReturn(counted(5, 5));

        SyncTaskGroupDataCheckResult result = service.checkData(GROUP_ID);

        assertTrue(result.isSuccess());
        assertTrue(result.isMatched());
        assertEquals("全部 2 张表行数一致", result.getMessage());
        assertTrue(result.getConsistencyNote().contains("非同水位"), result.getConsistencyNote());
    }

    @Test
    void aKafkaGroupIsNotReconciledByRowCount() {
        persisted(group("RUNNING", KAFKA_ID));
        items.add(item(11L, "customers"));

        SyncTaskGroupDataCheckResult result = service.checkData(GROUP_ID);

        assertFalse(result.isSuccess());
        assertEquals(1, result.getTableCount());
        assertTrue(result.getMessage().contains("topic"), result.getMessage());
        assertTrue(result.getItems().isEmpty());
        verify(consistencyService, never()).check(any(), any(), any(), any(), any(), any());
        verify(itemMapper, never()).updateById(any(SyncTaskGroupItem.class));
        verify(itemMapper, never()).recordCheck(any(), any(), any(), any(), any(), any(), any());
    }

    // ------------------------------------------------------------------ fixtures

    private void persisted(SyncTaskGroup group) {
        when(groupMapper.selectById(GROUP_ID)).thenReturn(group);
    }

    private static SyncTaskDataCheckResult counted(long sourceRows, long targetRows) {
        SyncTaskDataCheckResult result = new SyncTaskDataCheckResult();
        result.setSourceRows(sourceRows);
        result.setTargetRows(targetRows);
        result.setDifference(sourceRows - targetRows);
        result.setMatched(sourceRows == targetRows);
        result.setSuccess(true);
        result.setMessage(sourceRows == targetRows ? "源端与目标端行数一致" : "源端与目标端行数不一致");
        return result;
    }

    private static SyncTaskGroup group(String status, long targetId) {
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(GROUP_ID);
        group.setGroupName("g");
        group.setStatus(status);
        group.setSyncScope("MULTI_TABLE");
        group.setSourceId(MYSQL_ID);
        group.setTargetId(targetId);
        return group;
    }

    private static SyncTaskGroupItem item(long id, String table) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(GROUP_ID);
        item.setSourceDatabase("source_db");
        item.setSourceTable(table);
        item.setTargetSchema("public");
        item.setTargetTable(table);
        item.setStatus("RUNNING");
        return item;
    }

    private static DataSource dataSource(long id, String type, String database) {
        DataSource dataSource = new DataSource();
        dataSource.setSourceId(id);
        dataSource.setSourceType(type);
        dataSource.setDatabaseName(database);
        return dataSource;
    }
}
