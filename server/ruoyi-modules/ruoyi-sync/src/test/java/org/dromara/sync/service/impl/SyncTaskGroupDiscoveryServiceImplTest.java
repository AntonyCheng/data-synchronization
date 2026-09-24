package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.EngineJobRunner;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.support.SyncLocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * New-table discovery for whole-database groups: every table without an item gets one (selection,
 * baseline, topic), a table that cannot be synced is kept visible as FAILED instead of skipped, and on
 * a live group each new table's job starts at once without one refusal stopping the others.
 */
@Tag("dev")
class SyncTaskGroupDiscoveryServiceImplTest {

    private static final long GROUP_ID = 7L;
    private static final long MYSQL_ID = 1L;
    private static final long POSTGRES_ID = 2L;
    private static final long KAFKA_ID = 3L;

    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final IDataSourceService dataSourceService = mock(IDataSourceService.class);
    private final IDataSourceMetadataService metadataService = mock(IDataSourceMetadataService.class);
    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final KafkaTaskBridgeService bridge = mock(KafkaTaskBridgeService.class);
    private final SyncLocks locks = mock(SyncLocks.class);
    private final SeaTunnelProperties properties = new SeaTunnelProperties();

    // Real item operations and runner over the mocked engine: submitting a new table is pinned end to end.
    private final SyncTaskGroupDiscoveryServiceImpl service = new SyncTaskGroupDiscoveryServiceImpl(groupMapper, itemMapper,
        dataSourceService, metadataService, bridge,
        new GroupItemOperations(itemMapper, metadataService, properties, new EngineJobRunner(restClient, bridge)), locks);

    private final AtomicBoolean lockHeld = new AtomicBoolean();
    private final DataSource mysql = dataSource(MYSQL_ID, "MYSQL", "source_db");
    private final DataSource postgres = dataSource(POSTGRES_ID, "POSTGRESQL", "sink_db");
    private final DataSource kafka = dataSource(KAFKA_ID, "KAFKA", null);
    private final List<SyncTaskGroupItem> items = new ArrayList<>();
    /** Every item discovery inserted, in order. */
    private final List<SyncTaskGroupItem> inserted = new ArrayList<>();
    private long nextItemId = 100;
    private int nextJobId = 100;

    @BeforeEach
    void wireCollaborators() {
        when(locks.withGroupLock(any(), any())).thenAnswer(invocation -> {
            lockHeld.set(true);
            try {
                return ((Supplier<?>) invocation.getArgument(1)).get();
            } finally {
                lockHeld.set(false);
            }
        });
        when(dataSourceService.requireById(eq(MYSQL_ID), anyString())).thenReturn(mysql);
        when(dataSourceService.requireById(eq(POSTGRES_ID), anyString())).thenReturn(postgres);
        when(dataSourceService.requireById(eq(KAFKA_ID), anyString())).thenReturn(kafka);
        when(itemMapper.selectByGroupId(GROUP_ID)).thenAnswer(invocation -> new ArrayList<>(items));
        when(itemMapper.insert(any(SyncTaskGroupItem.class))).thenAnswer(invocation -> {
            SyncTaskGroupItem item = invocation.getArgument(0);
            assertTrue(lockHeld.get(), "items are inserted under the group lock");
            item.setItemId(nextItemId++);
            inserted.add(item);
            return 1;
        });
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), anyString())).thenAnswer(invocation -> metadata("id", "name"));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString())).thenReturn(compatibility(true));
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenAnswer(invocation ->
            new SeaTunnelRestClient.SubmitResult("job-" + (nextJobId++), invocation.getArgument(0)));
    }

    @Test
    void eachNewTableGetsAnItemAndAGroupThatIsNotLiveSubmitsNothing() {
        SyncTaskGroup group = persisted(group("STOPPED", POSTGRES_ID));
        SyncTaskGroupItem existing = item(11L, "Customers");
        existing.setTargetSchema("sales");
        items.add(existing);
        sourceTables("customers", "orders", "invoices");

        SyncTaskGroupOperationResult result = service.discover(GROUP_ID);

        assertEquals(List.of("orders", "invoices"), inserted.stream().map(SyncTaskGroupItem::getSourceTable).toList(),
            "an existing table is matched case-insensitively");
        for (SyncTaskGroupItem item : inserted) {
            assertEquals("PENDING", item.getStatus());
            assertEquals("source_db", item.getSourceDatabase());
            assertEquals("sales", item.getTargetSchema(), "a new table follows the schema of the group's tables");
            assertEquals(item.getSourceTable(), item.getTargetTable());
            assertEquals("FAIL", item.getDdlPolicy());
            assertEquals("id,name", item.getSelectedColumns());
            assertEquals("id", item.getSyncKeyColumns());
            assertTrue(item.getSchemaSnapshot().contains("\"primaryKeys\":[\"id\"]"), "baseline captured on discovery");
            assertNull(item.getEngineJobId());
        }
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
        assertEquals(2, group.getConfigVersion());
        assertEquals("", group.getLastError());
        verify(groupMapper).updateById(group);
        assertEquals("发现 2 张新表，已启动 0 张，失败 0 张", result.getMessage());
        assertFalse(result.isPartial());
    }

    @Test
    void onALiveGroupEachNewTableStartsAtOnceAndARefusedOneIsIsolated() {
        SyncTaskGroup group = persisted(group("RUNNING", POSTGRES_ID));
        group.setEngineJobId("job-a");
        sourceTables("orders", "invoices");
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false)))
            .thenReturn(new SeaTunnelRestClient.SubmitResult("job-100", "x"))
            .thenThrow(new ServiceException("SeaTunnel 接口不可用：refused"));

        SyncTaskGroupOperationResult result = service.discover(GROUP_ID);

        SyncTaskGroupItem orders = inserted.get(0);
        SyncTaskGroupItem invoices = inserted.get(1);
        assertEquals("RUNNING", orders.getStatus());
        assertEquals("job-100", orders.getEngineJobId());
        assertEquals(64, orders.getEngineConfigHash().length());
        assertEquals("FAILED", invoices.getStatus());
        assertTrue(invoices.getLastError().contains("refused"), invoices.getLastError());
        assertEquals("job-a,job-100", group.getEngineJobId());
        assertTrue(group.getLastError().contains("1 张表校验或提交失败"), group.getLastError());
        assertEquals("发现 2 张新表，已启动 1 张，失败 1 张", result.getMessage());
        assertTrue(result.isPartial());
    }

    @Test
    void aNewTableWithoutAUsableKeyIsKeptAsFailedAndNeverSubmitted() {
        persisted(group("DEGRADED", POSTGRES_ID));
        sourceTables("audit_log", "orders");
        DataSourceMetadataVo noKey = metadata("id", "name");
        noKey.getPrimaryKeys().clear();
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), eq("audit_log"))).thenReturn(noKey);

        SyncTaskGroupOperationResult result = service.discover(GROUP_ID);

        SyncTaskGroupItem auditLog = inserted.get(0);
        assertEquals("FAILED", auditLog.getStatus());
        assertEquals("源表没有可用同步键", auditLog.getLastError());
        assertNull(auditLog.getEngineJobId());
        assertEquals("RUNNING", inserted.get(1).getStatus());
        verify(restClient).submit(eq(SeaTunnelJobConfigGenerator.JOB_NAME_PREFIX + inserted.get(1).getItemId()), anyString(), isNull(), eq(false));
        assertEquals("发现 2 张新表，已启动 1 张，失败 1 张", result.getMessage());
    }

    @Test
    void aWholeDatabaseKafkaGroupCreatesOneTopicPerDiscoveredTable() {
        persisted(group("STOPPED", KAFKA_ID));
        sourceTables("orders");

        service.discover(GROUP_ID);

        verify(bridge).ensureTopicExists(kafka, "orders");
        assertEquals("PENDING", inserted.get(0).getStatus());
    }

    @Test
    void nothingNewLeavesTheGroupUntouched() {
        SyncTaskGroup group = persisted(group("RUNNING", POSTGRES_ID));
        items.add(item(11L, "orders"));
        sourceTables("orders");

        SyncTaskGroupOperationResult result = service.discover(GROUP_ID);

        assertEquals("未发现新增表", result.getMessage());
        assertEquals(1, group.getConfigVersion());
        verify(itemMapper, never()).insert(any(SyncTaskGroupItem.class));
        verify(groupMapper, never()).updateById(any(SyncTaskGroup.class));
    }

    @Test
    void onlyWholeDatabaseGroupsDiscoverAndAlwaysUnderTheGroupLock() {
        SyncTaskGroup multiTable = group("RUNNING", POSTGRES_ID);
        multiTable.setSyncScope("MULTI_TABLE");
        persisted(multiTable);

        String refused = assertThrows(ServiceException.class, () -> service.discover(GROUP_ID)).getMessage();

        assertEquals("仅整库同步任务组支持发现新增表", refused);
        verify(locks).withGroupLock(eq(GROUP_ID), any());
        verify(metadataService, never()).queryTables(anyLong(), any());
    }

    @Test
    void theBackgroundPassSkipsBusyGroupsAndOneFailingGroupDoesNotStopTheOthers() {
        SyncTaskGroup busy = group("RUNNING", POSTGRES_ID);
        busy.setGroupId(5L);
        SyncTaskGroup vanished = group("RUNNING", POSTGRES_ID);
        vanished.setGroupId(6L);
        SyncTaskGroup healthy = persisted(group("RUNNING", POSTGRES_ID));
        when(groupMapper.selectLiveAutoDiscoverDatabaseGroups()).thenReturn(List.of(busy, vanished, healthy));
        when(locks.isGroupBusy(5L)).thenReturn(true);
        sourceTables("orders");

        service.discoverDatabaseGroups();

        verify(locks, never()).withGroupLock(eq(5L), any());
        verify(locks).withGroupLock(eq(6L), any());
        assertEquals(List.of("orders"), inserted.stream().map(SyncTaskGroupItem::getSourceTable).toList());
        assertEquals(GROUP_ID, inserted.get(0).getGroupId());
    }

    // ------------------------------------------------------------------ fixtures

    private SyncTaskGroup persisted(SyncTaskGroup group) {
        when(groupMapper.selectById(group.getGroupId())).thenReturn(group);
        return group;
    }

    private void sourceTables(String... tables) {
        when(metadataService.queryTables(MYSQL_ID, "source_db")).thenReturn(List.of(tables));
    }

    private static SyncTaskGroup group(String status, long targetId) {
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(GROUP_ID);
        group.setGroupName("g");
        group.setStatus(status);
        group.setSyncScope("DATABASE");
        group.setSourceId(MYSQL_ID);
        group.setTargetId(targetId);
        group.setSyncMode("FULL_CDC");
        group.setConfigVersion(1);
        group.setDdlPolicy("FAIL");
        group.setReadLimitRowsPerSecond(100);
        group.setReadLimitBytesPerSecond(1024L);
        group.setSnapshotParallelism(1);
        group.setSourceConnectionLimit(2);
        return group;
    }

    private static SyncTaskGroupItem item(long id, String table) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(GROUP_ID);
        item.setSourceDatabase("source_db");
        item.setSourceTable(table);
        item.setTargetTable(table);
        item.setStatus("RUNNING");
        return item;
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
        dataSource.setSslEnabled("0");
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
