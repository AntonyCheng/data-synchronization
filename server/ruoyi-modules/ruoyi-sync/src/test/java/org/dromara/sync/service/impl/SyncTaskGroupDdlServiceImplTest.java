package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupDdlEvent;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlEventVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.mapper.SyncTaskGroupDdlEventMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceMetadataService.TableMetadata;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncTaskGroupService;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableSchemaSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DDL drift checks for task groups: the pass reads every table of the group over one batch call
 * (the source database is the customer's, and this runs every minute), isolates only what changed,
 * and keeps one unreadable table from costing the others their check.
 */
@Tag("dev")
class SyncTaskGroupDdlServiceImplTest {

    private static final long GROUP_ID = 7L;
    private static final long MYSQL_ID = 1L;
    private static final long POSTGRES_ID = 2L;

    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final SyncTaskGroupDdlEventMapper ddlEventMapper = mock(SyncTaskGroupDdlEventMapper.class);
    private final IDataSourceService dataSourceService = mock(IDataSourceService.class);
    private final IDataSourceMetadataService metadataService = mock(IDataSourceMetadataService.class);
    private final ISyncTaskGroupService groupService = mock(ISyncTaskGroupService.class);
    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final SyncLocks locks = mock(SyncLocks.class);

    private final SyncTaskGroupDdlServiceImpl service = new SyncTaskGroupDdlServiceImpl(groupMapper, itemMapper,
        ddlEventMapper, dataSourceService, metadataService, groupService, restClient, locks);

    private final DataSource mysql = dataSource(MYSQL_ID, "MYSQL", "source_db");
    private final DataSource postgres = dataSource(POSTGRES_ID, "POSTGRESQL", "sink_db");
    private final SyncTaskGroup group = group();
    private final List<SyncTaskGroupItem> items = new ArrayList<>();

    /** Source structure the batch read reports, per table; anything absent is "unreadable". */
    private final Map<String, TableMetadata> sourceSchemas = new LinkedHashMap<>();

    @BeforeEach
    void wireCollaborators() {
        when(locks.withGroupLock(any(), any())).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(1)).get());
        when(groupMapper.selectById(GROUP_ID)).thenReturn(group);
        when(dataSourceService.requireById(eq(MYSQL_ID), anyString())).thenReturn(mysql);
        when(dataSourceService.requireById(eq(POSTGRES_ID), anyString())).thenReturn(postgres);
        when(itemMapper.selectByGroupId(GROUP_ID)).thenAnswer(invocation -> new ArrayList<>(items));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString())).thenReturn(compatibility(true));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString(), any(), any()))
            .thenReturn(compatibility(true));
        when(metadataService.queryTablesMetadata(eq(MYSQL_ID), anyString(), any())).thenAnswer(invocation -> {
            Map<String, TableMetadata> answer = new LinkedHashMap<>();
            for (String table : invocation.<Collection<String>>getArgument(2)) {
                answer.put(table, sourceSchemas.getOrDefault(table, new TableMetadata(null, "表不存在")));
            }
            return answer;
        });
    }

    // --------------------------------------------------------------- one read per pass

    @Test
    void aPassReadsEveryTableOfTheGroupInOneBatchCall() {
        items.add(baselined(11L, "customers", metadata("id", "name")));
        items.add(baselined(12L, "orders", metadata("id", "name")));
        items.add(baselined(13L, "invoices", metadata("id", "name")));

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        ArgumentCaptor<Collection<String>> tables = ArgumentCaptor.captor();
        verify(metadataService).queryTablesMetadata(eq(MYSQL_ID), eq("source_db"), tables.capture());
        assertEquals(List.of("customers", "orders", "invoices"), List.copyOf(tables.getValue()));
        // The per-table entry point is what used to open one connection per table.
        verify(metadataService, never()).queryTableMetadata(anyLong(), anyString(), anyString());
        assertTrue(result.getEvents().isEmpty(), "an unchanged group raises nothing");
        assertEquals("未发现运行中表结构变更", result.getMessage());
    }

    @Test
    void itemsOfDifferentSourceDatabasesAreReadOncePerDatabase() {
        SyncTaskGroupItem other = baselined(12L, "orders", metadata("id", "name"));
        other.setSourceDatabase("other_db");
        items.add(baselined(11L, "customers", metadata("id", "name")));
        items.add(other);

        service.checkDdl(GROUP_ID);

        verify(metadataService).queryTablesMetadata(MYSQL_ID, "source_db", List.of("customers"));
        verify(metadataService).queryTablesMetadata(MYSQL_ID, "other_db", List.of("orders"));
    }

    // --------------------------------------------------------------- drift detection

    @Test
    void anAddedColumnPausesOnlyThatTableWithASavepoint() {
        SyncTaskGroupItem changed = baselined(11L, "customers", metadata("id", "name"));
        SyncTaskGroupItem stable = baselined(12L, "orders", metadata("id", "name"));
        items.add(changed);
        items.add(stable);
        sourceSchemas.put("customers", readable(metadata("id", "name", "email")));

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        assertEquals("DDL_BLOCKED", changed.getStatus());
        assertEquals("RUNNING", stable.getStatus());
        verify(restClient).stop("job-11", true, false);
        verify(restClient, never()).stop(eq("job-12"), any(Boolean.class), any(Boolean.class));
        assertEquals(1, result.getEvents().size());
        SyncTaskGroupDdlEventVo event = result.getEvents().get(0);
        assertEquals("customers", event.getSourceTable());
        assertTrue(event.getDetails().contains("email"), "the event names the new column: " + event.getDetails());
        // The target still accepts the new shape, so the table only waits for an operator's go-ahead.
        assertEquals(SyncTaskGroupDdlEvent.STATUS_READY_TO_RESUME, event.getStatus());
        assertEquals("DEGRADED", group.getStatus());
        assertEquals("", group.getLastError(), "nothing needs fixing, so the group carries no error");
    }

    @Test
    void aChangeTheTargetCannotTakeIsReportedAsPendingFix() {
        SyncTaskGroupItem changed = baselined(11L, "customers", metadata("id", "name"));
        items.add(changed);
        sourceSchemas.put("customers", readable(metadata("id", "name", "email")));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString(), any(), any()))
            .thenReturn(compatibility(false));

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        assertEquals(SyncTaskGroupDdlEvent.STATUS_PENDING_FIX, result.getEvents().get(0).getStatus());
        assertEquals("DDL_BLOCKED", changed.getStatus());
        assertTrue(group.getLastError().contains("1 张表"), group.getLastError());
    }

    @Test
    void aTableBackAtItsBaselineBecomesReadyToResume() {
        SyncTaskGroupItem item = baselined(11L, "customers", metadata("id", "name"));
        item.setStatus("DDL_BLOCKED");
        items.add(item);
        SyncTaskGroupDdlEvent open = new SyncTaskGroupDdlEvent();
        open.setEventId(5L);
        open.setItemId(11L);
        open.setStatus(SyncTaskGroupDdlEvent.STATUS_PENDING_FIX);
        when(ddlEventMapper.selectLatestOpen(11L)).thenReturn(open);

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        assertEquals(SyncTaskGroupDdlEvent.STATUS_READY_TO_RESUME, open.getStatus());
        assertEquals("DDL_BLOCKED", item.getStatus(), "the operator still confirms the target before resuming");
        assertEquals(1, result.getEvents().size());
        assertTrue(result.getMessage().contains("1 条已可恢复"));
    }

    @Test
    void aTableWithoutABaselineGetsOneInsteadOfAnEvent() {
        SyncTaskGroupItem item = item(11L, "customers");
        items.add(item);
        sourceSchemas.put("customers", readable(metadata("id", "name")));

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        assertNotNull(item.getSchemaSnapshot(), "first successful check captures the baseline");
        assertEquals(64, item.getSchemaHash().length());
        assertTrue(result.getEvents().isEmpty());
        verify(ddlEventMapper, never()).insert(any(SyncTaskGroupDdlEvent.class));
    }

    // --------------------------------------------------------------- per-table failure isolation

    @Test
    void oneUnreadableTableDoesNotCostTheOthersTheirCheck() {
        SyncTaskGroupItem dropped = baselined(11L, "customers", metadata("id", "name"));
        SyncTaskGroupItem stable = baselined(12L, "orders", metadata("id", "name"));
        items.add(dropped);
        items.add(stable);
        sourceSchemas.put("customers", new TableMetadata(null, "表 customers 不存在"));

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        assertEquals("DDL_BLOCKED", dropped.getStatus());
        assertEquals("RUNNING", stable.getStatus());
        assertEquals(1, result.getEvents().size());
        assertEquals("TABLE_UNAVAILABLE", result.getEvents().get(0).getChangeType());
        assertTrue(result.getEvents().get(0).getDetails().contains("表 customers 不存在"));
    }

    @Test
    void anUnreachableSourceMarksEveryTableUnavailableInsteadOfAbortingThePass() {
        items.add(baselined(11L, "customers", metadata("id", "name")));
        items.add(baselined(12L, "orders", metadata("id", "name")));
        when(metadataService.queryTablesMetadata(anyLong(), anyString(), any()))
            .thenThrow(new ServiceException("数据库连接失败：connection refused"));

        SyncTaskGroupDdlCheckResult result = service.checkDdl(GROUP_ID);

        assertEquals(2, result.getEvents().size());
        for (SyncTaskGroupDdlEventVo event : result.getEvents()) {
            assertEquals("TABLE_UNAVAILABLE", event.getChangeType());
            assertTrue(event.getDetails().contains("connection refused"), event.getDetails());
        }
        assertEquals("FAILED", group.getStatus(), "every table isolated aggregates to a failed group");
    }

    // --------------------------------------------------------------- helpers

    private static TableMetadata readable(DataSourceMetadataVo metadata) {
        return new TableMetadata(metadata, null);
    }

    private SyncTaskGroupItem baselined(long id, String table, DataSourceMetadataVo baseline) {
        SyncTaskGroupItem item = item(id, table);
        String json = TableSchemaSnapshot.toJson(TableSchemaSnapshot.of(baseline));
        item.setSchemaSnapshot(json);
        item.setSchemaHash(SyncText.sha256Hex(json));
        // Unless a test overrides it, the source still looks exactly like the baseline.
        sourceSchemas.putIfAbsent(table, readable(baseline));
        return item;
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
        item.setEngineJobId("job-" + id);
        item.setSelectedColumns("id,name");
        item.setSyncKeyColumns("id");
        item.setDdlPolicy("FAIL");
        return item;
    }

    private static SyncTaskGroup group() {
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(GROUP_ID);
        group.setGroupName("g7");
        group.setSourceId(MYSQL_ID);
        group.setTargetId(POSTGRES_ID);
        group.setSyncScope("MULTI_TABLE");
        group.setSyncMode("FULL_CDC");
        group.setStatus("RUNNING");
        return group;
    }

    private static DataSource dataSource(long id, String type, String database) {
        DataSource dataSource = new DataSource();
        dataSource.setSourceId(id);
        dataSource.setSourceType(type);
        dataSource.setHost(type.toLowerCase() + ".example");
        dataSource.setPort(9);
        dataSource.setDatabaseName(database);
        dataSource.setUsername("u");
        dataSource.setPassword("p");
        return dataSource;
    }

    private static DataSourceMetadataVo metadata(String... columns) {
        DataSourceMetadataVo metadata = new DataSourceMetadataVo();
        for (String name : columns) {
            DataSourceColumnVo column = new DataSourceColumnVo();
            column.setName(name);
            column.setTypeName("varchar");
            column.setNullable(!"id".equals(name));
            metadata.getColumns().add(column);
        }
        metadata.getPrimaryKeys().add("id");
        return metadata;
    }

    private static TargetCompatibilityVo compatibility(boolean passed) {
        TargetCompatibilityVo result = new TargetCompatibilityVo();
        result.setPassed(passed);
        result.setMessage(passed ? "目标表结构兼容" : "目标表存在必须修复的兼容性问题");
        return result;
    }
}
