package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupDdlEvent;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.engine.SourceColumns;
import org.dromara.sync.engine.SyncTaskGroupConfigGenerator;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.SyncTaskGroupDdlEventMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.TableSchemaSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * State-machine tests for task groups: one engine job per table item, per-item isolation,
 * compensation on a failed start, and the aggregate group status.
 */
@Tag("dev")
class SyncTaskGroupServiceImplTest {

    private static final long GROUP_ID = 7L;
    private static final long MYSQL_ID = 1L;
    private static final long POSTGRES_ID = 2L;
    private static final long KAFKA_ID = 3L;

    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final SyncTaskGroupDdlEventMapper ddlEventMapper = mock(SyncTaskGroupDdlEventMapper.class);
    private final IDataSourceService dataSourceService = mock(IDataSourceService.class);
    private final IDataSourceMetadataService metadataService = mock(IDataSourceMetadataService.class);
    private final IDataConsistencyService consistencyService = mock(IDataConsistencyService.class);
    private final SeaTunnelProperties properties = new SeaTunnelProperties();
    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final KafkaTaskBridgeService bridge = mock(KafkaTaskBridgeService.class);
    private final ISyncMetricsService metrics = mock(ISyncMetricsService.class);
    private final SyncLocks locks = mock(SyncLocks.class);
    private final PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);

    private final SyncTaskGroupServiceImpl service = new SyncTaskGroupServiceImpl(groupMapper, itemMapper, ddlEventMapper,
        dataSourceService, metadataService, consistencyService, properties, restClient, new ResourceProtectionPolicy(properties),
        bridge, metrics, locks, new TransactionTemplate(transactionManager));

    /** True only while the fake group lock is held. */
    private final AtomicBoolean lockHeld = new AtomicBoolean();

    private final DataSource mysql = dataSource(MYSQL_ID, "MYSQL", "source_db");
    private final DataSource postgres = dataSource(POSTGRES_ID, "POSTGRESQL", "sink_db");
    private final DataSource kafka = dataSource(KAFKA_ID, "KAFKA", null);
    private final List<SyncTaskGroupItem> items = new ArrayList<>();
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
        when(dataSourceService.testConnection(anyLong(), isNull())).thenReturn(ConnectionTestResult.success(1));
        DataSourceCdcPrecheckVo cdc = new DataSourceCdcPrecheckVo();
        cdc.setPassed(true);
        when(metadataService.checkMysqlCdc(MYSQL_ID)).thenReturn(cdc);
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), anyString())).thenAnswer(invocation -> metadata("id", "name"));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString(), any(), any())).thenReturn(compatibility(true));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString())).thenReturn(compatibility(true));
        when(itemMapper.selectByGroupId(GROUP_ID)).thenAnswer(invocation -> new ArrayList<>(items));
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenAnswer(invocation ->
            new SeaTunnelRestClient.SubmitResult("job-" + (nextJobId++), invocation.getArgument(0)));
    }

    // ------------------------------------------------------------------ start

    @Test
    void startSubmitsOneJobPerTableAndAggregatesToRunning() {
        SyncTaskGroup group = persisted(group("STOPPED", "MULTI_TABLE", POSTGRES_ID));
        items.add(item(11L, "customers", "PENDING"));
        items.add(item(12L, "orders", "PENDING"));

        SyncTaskGroupOperationResult result = service.start(GROUP_ID);

        assertEquals("RUNNING", group.getStatus());
        assertEquals("job-100,job-101", group.getEngineJobId());
        assertEquals("任务组已提交 2 个作业", result.getMessage());
        for (SyncTaskGroupItem item : items) {
            assertEquals("RUNNING", item.getStatus());
            assertTrue(item.getEngineJobId().startsWith("job-"));
            assertEquals(64, item.getEngineConfigHash().length());
            assertTrue(item.getSchemaSnapshot().contains("\"primaryKeys\":[\"id\"]"), "schema baseline captured at start");
        }
        verify(restClient, times(2)).submit(anyString(), anyString(), isNull(), eq(false));
    }

    @Test
    void startFollowsTheSourceForFullySelectedTablesAndKeepsExplicitSubsets() {
        persisted(group("STOPPED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem whole = item(11L, "customers", "STOPPED");
        whole.setSchemaSnapshot(TableSchemaSnapshot.toJson(TableSchemaSnapshot.of(metadata("id", "name"))));
        SyncTaskGroupItem subset = item(12L, "orders", "STOPPED");
        subset.setSelectedColumns("id");
        subset.setSchemaSnapshot(TableSchemaSnapshot.toJson(TableSchemaSnapshot.of(metadata("id", "name"))));
        items.add(whole);
        items.add(subset);
        // Both source tables grew a column since the last baseline.
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), anyString())).thenReturn(metadata("id", "name", "email"));

        service.start(GROUP_ID);

        assertEquals("id,name,email", whole.getSelectedColumns());
        assertEquals("id", subset.getSelectedColumns());
        assertTrue(whole.getSchemaSnapshot().contains("email"), "baseline moves to the live schema");
        // The widened projection is what the pre-start target check saw.
        verify(metadataService).checkTargetCompatibility(eq(MYSQL_ID), eq(POSTGRES_ID), eq("customers"), any(), eq("customers"), eq("id,name,email"), any());
        assertEquals("RUNNING", whole.getStatus());
        assertEquals("RUNNING", subset.getStatus());
    }

    @Test
    void aMultiTableStartCompensatesAlreadySubmittedJobsWhenALaterTableFails() {
        SyncTaskGroup group = persisted(group("STOPPED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem first = item(11L, "customers", "PENDING");
        SyncTaskGroupItem second = item(12L, "orders", "PENDING");
        items.add(first);
        items.add(second);
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false)))
            .thenReturn(new SeaTunnelRestClient.SubmitResult("job-a", "x"))
            .thenThrow(new ServiceException("SeaTunnel 接口不可用：refused"));

        SyncTaskGroupOperationResult result = service.start(GROUP_ID);

        verify(restClient).stop("job-a", false, false);
        assertEquals("STOPPED", first.getStatus());
        assertEquals("FAILED", second.getStatus());
        assertEquals("FAILED", group.getStatus());
        assertTrue(group.getLastError().contains("refused"));
        assertTrue(result.getMessage().startsWith("任务组启动失败，已补偿停止已提交作业"));
    }

    @Test
    void aWholeDatabaseStartIsolatesFailingTablesAndSkipsAlreadyIsolatedOnes() {
        SyncTaskGroup group = persisted(group("STOPPED", "DATABASE", POSTGRES_ID));
        SyncTaskGroupItem good = item(11L, "customers", "PENDING");
        SyncTaskGroupItem bad = item(12L, "orders", "PENDING");
        SyncTaskGroupItem isolated = item(13L, "broken", "FAILED");
        items.add(good);
        items.add(bad);
        items.add(isolated);
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false)))
            .thenReturn(new SeaTunnelRestClient.SubmitResult("job-a", "x"))
            .thenThrow(new ServiceException("目标不可写"));

        SyncTaskGroupOperationResult result = service.start(GROUP_ID);

        assertEquals("RUNNING", good.getStatus());
        assertEquals("FAILED", bad.getStatus());
        assertEquals("FAILED", isolated.getStatus());
        assertEquals("DEGRADED", group.getStatus());
        assertEquals("job-a", group.getEngineJobId());
        assertEquals("整库任务已提交 1 个作业，隔离失败 2 张", result.getMessage());
        verify(restClient, never()).stop(anyString(), eq(false), eq(false));
    }

    @Test
    void startNeverResubmitsATableThatAlreadyHasALiveJob() {
        SyncTaskGroup group = persisted(group("DEGRADED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem live = item(11L, "customers", "RUNNING");
        live.setEngineJobId("job-live");
        SyncTaskGroupItem blocked = item(12L, "orders", "DDL_BLOCKED");
        items.add(live);
        items.add(blocked);

        service.start(GROUP_ID);

        verify(restClient, times(1)).submit(anyString(), anyString(), isNull(), eq(false));
        assertEquals("job-live", live.getEngineJobId());
        assertEquals("RUNNING", blocked.getStatus());
        assertEquals("job-live,job-100", group.getEngineJobId());
        verify(ddlEventMapper).resolveOpen(eq(12L), anyString());
    }

    @Test
    void startIsRefusedWhileRunningAndWhenValidationFails() {
        persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        assertTrue(assertThrows(ServiceException.class, () -> service.start(GROUP_ID)).getMessage().contains("正在运行"));

        persisted(group("PAUSED", "MULTI_TABLE", POSTGRES_ID));
        assertTrue(assertThrows(ServiceException.class, () -> service.start(GROUP_ID)).getMessage().contains("请使用恢复任务组"));

        persisted(group("STOPPED", "MULTI_TABLE", POSTGRES_ID));
        items.add(item(11L, "customers", "PENDING"));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString(), any(), any())).thenReturn(compatibility(false));
        String refused = assertThrows(ServiceException.class, () -> service.start(GROUP_ID)).getMessage();
        assertTrue(refused.contains("启动前校验未通过") && refused.contains("customers：目标表存在必须修复的兼容性问题"), refused);
        verify(restClient, never()).submit(anyString(), anyString(), any(), eq(false));
    }

    // ------------------------------------------------------------------ refreshStatus

    @Test
    void refreshMapsEngineStateToItemsAndAggregatesTheGroup() {
        SyncTaskGroup group = persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem running = runningItem(11L, "customers", "job-a");
        SyncTaskGroupItem finished = runningItem(12L, "orders", "job-b");
        items.add(running);
        items.add(finished);
        when(restClient.status("job-a")).thenReturn(new SeaTunnelRestClient.JobSnapshot("job-a", "x", "RUNNING", null, null));
        when(restClient.status("job-b")).thenReturn(new SeaTunnelRestClient.JobSnapshot("job-b", "x", "FAILED", "boom", null));
        when(restClient.checkpoints(anyString())).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());

        SyncTaskGroupStatus status = service.refreshStatus(GROUP_ID);

        assertEquals("RUNNING", running.getStatus());
        assertEquals("FAILED", finished.getStatus());
        assertEquals("boom", finished.getLastError());
        assertEquals("DEGRADED", status.getStatus());
        assertEquals("DEGRADED", group.getStatus());
        verify(metrics, times(2)).recordGroupItem(any(), anyString(), any());
    }

    @Test
    void anOpenDdlEventKeepsAnItemBlockedAndTheGroupDegradedWhateverTheEngineSays() {
        SyncTaskGroup group = persisted(group("DEGRADED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem blocked = runningItem(11L, "customers", "job-a");
        blocked.setStatus("DDL_BLOCKED");
        SyncTaskGroupItem healthy = runningItem(12L, "orders", "job-b");
        items.add(blocked);
        items.add(healthy);
        // The DDL service parked job-a with a savepoint, so the engine reports it finished.
        when(restClient.status("job-a")).thenReturn(new SeaTunnelRestClient.JobSnapshot("job-a", "x", "SAVEPOINT_DONE", null, null));
        when(restClient.status("job-b")).thenReturn(new SeaTunnelRestClient.JobSnapshot("job-b", "x", "RUNNING", null, null));
        when(restClient.checkpoints(anyString())).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());
        when(ddlEventMapper.selectLatestOpen(11L)).thenReturn(new SyncTaskGroupDdlEvent());

        SyncTaskGroupStatus status = service.refreshStatus(GROUP_ID);

        assertEquals("DDL_BLOCKED", blocked.getStatus());
        assertEquals("RUNNING", healthy.getStatus());
        assertEquals("DEGRADED", status.getStatus());
        assertEquals("DEGRADED", group.getStatus());
    }

    @Test
    void transientEngineFailuresHoldTheItemForTwoPassesThenIsolateIt() {
        persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem item = runningItem(11L, "customers", "job-a");
        items.add(item);
        when(restClient.status("job-a")).thenThrow(new ServiceException("SeaTunnel 接口不可用：timeout"));

        assertEquals("RUNNING", service.refreshStatus(GROUP_ID).getItems().get(0).getStatus());
        assertEquals("RUNNING", service.refreshStatus(GROUP_ID).getItems().get(0).getStatus());
        assertEquals("RUNNING", item.getStatus());

        SyncTaskGroupStatus third = service.refreshStatus(GROUP_ID);
        assertEquals("FAILED", item.getStatus());
        assertEquals("FAILED", third.getStatus());
    }

    // ------------------------------------------------------------------ pause / resume / stop

    @Test
    void pauseAndStopFanOutToEveryTableAndTheKafkaBridge() {
        SyncTaskGroup group = persisted(group("RUNNING", "MULTI_TABLE", KAFKA_ID));
        SyncTaskGroupItem item = runningItem(11L, "customers", "job-a");
        items.add(item);

        service.pause(GROUP_ID);
        verify(bridge).stop(11L);
        verify(restClient).stop("job-a", true, false);
        assertEquals("PAUSING", item.getStatus());
        assertEquals("PAUSING", group.getStatus());

        service.stop(GROUP_ID);
        verify(restClient).stop("job-a", false, false);
        assertEquals("STOPPED", item.getStatus());
        assertEquals("STOPPED", group.getStatus());
    }

    @Test
    void resumeIsRefusedWhileADdlEventIsOpenOrTheFingerprintChanged() {
        SyncTaskGroup group = persisted(group("PAUSED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem item = runningItem(11L, "customers", "job-a");
        item.setStatus("PAUSED");
        items.add(item);

        when(ddlEventMapper.selectLatestOpen(11L)).thenReturn(new SyncTaskGroupDdlEvent());
        assertTrue(assertThrows(ServiceException.class, () -> service.resume(GROUP_ID)).getMessage().contains("表结构变更"));

        when(ddlEventMapper.selectLatestOpen(11L)).thenReturn(null);
        item.setEngineConfigHash("stale");
        assertTrue(assertThrows(ServiceException.class, () -> service.resume(GROUP_ID)).getMessage().contains("配置已变化"));

        item.setEngineConfigHash(fingerprint(group, item, postgres));
        when(restClient.submit(anyString(), anyString(), eq("job-a"), eq(true))).thenReturn(new SeaTunnelRestClient.SubmitResult("job-a", "x"));
        service.resume(GROUP_ID);
        verify(restClient).submit(anyString(), anyString(), eq("job-a"), eq(true));
        assertEquals("RUNNING", item.getStatus());
        assertEquals("RUNNING", group.getStatus());
    }

    @Test
    void resumeDecidesEveryRefusalBeforeTheFirstSubmit() {
        SyncTaskGroup group = persisted(group("PAUSED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem first = runningItem(11L, "customers", "job-a");
        first.setStatus("PAUSED");
        first.setEngineConfigHash(fingerprint(group, first, postgres));
        SyncTaskGroupItem second = runningItem(12L, "orders", "job-b");
        second.setStatus("PAUSED");
        second.setEngineConfigHash("stale");
        items.add(first);
        items.add(second);

        assertTrue(assertThrows(ServiceException.class, () -> service.resume(GROUP_ID)).getMessage().contains("orders"));

        // Nothing was resubmitted, so nothing is running behind rows that still say PAUSED.
        verify(restClient, never()).submit(anyString(), anyString(), anyString(), anyBoolean());
        assertEquals("PAUSED", first.getStatus());
        assertEquals("PAUSED", group.getStatus());
    }

    @Test
    void aTableTheEngineRefusesToResumeIsParkedWhileTheOthersKeepRunning() {
        SyncTaskGroup group = persisted(group("PAUSED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem first = runningItem(11L, "customers", "job-a");
        SyncTaskGroupItem second = runningItem(12L, "orders", "job-b");
        for (SyncTaskGroupItem item : List.of(first, second)) {
            item.setStatus("PAUSED");
            item.setEngineConfigHash(fingerprint(group, item, postgres));
            items.add(item);
        }
        when(restClient.submit(anyString(), anyString(), eq("job-a"), eq(true))).thenReturn(new SeaTunnelRestClient.SubmitResult("job-a", "x"));
        when(restClient.submit(anyString(), anyString(), eq("job-b"), eq(true))).thenThrow(new ServiceException("savepoint 不存在"));

        SyncTaskGroupOperationResult result = service.resume(GROUP_ID);

        assertEquals("RUNNING", first.getStatus());
        assertEquals("FAILED", second.getStatus());
        assertEquals("job-b", second.getEngineJobId(), "the savepoint stays addressable for 恢复该表");
        assertTrue(second.getLastError().contains("savepoint 不存在"));
        assertEquals("DEGRADED", group.getStatus());
        assertTrue(result.getMessage().contains("1 张恢复失败"), result.getMessage());
    }

    @Test
    void aTableWhoseSavepointRequestFailsIsParkedAndTheGroupKeepsPausing() {
        SyncTaskGroup group = persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem first = runningItem(11L, "customers", "job-a");
        SyncTaskGroupItem second = runningItem(12L, "orders", "job-b");
        items.add(first);
        items.add(second);
        doThrow(new ServiceException("SeaTunnel 接口不可用：timeout")).when(restClient).stop("job-b", true, false);

        SyncTaskGroupOperationResult result = service.pause(GROUP_ID);

        assertEquals("PAUSING", first.getStatus());
        assertEquals("FAILED", second.getStatus());
        // Still PAUSING so the status refresh keeps polling it and settles the aggregate.
        assertEquals("PAUSING", group.getStatus());
        assertTrue(group.getLastError().contains("orders"));
        assertTrue(result.getMessage().contains("orders"));
    }

    @Test
    void stopKeepsGoingPastATableTheEngineCannotStop() {
        SyncTaskGroup group = persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem stuck = runningItem(11L, "customers", "job-a");
        SyncTaskGroupItem healthy = runningItem(12L, "orders", "job-b");
        items.add(stuck);
        items.add(healthy);
        doThrow(new ServiceException("SeaTunnel 接口不可用：timeout")).when(restClient).stop("job-a", false, false);

        SyncTaskGroupOperationResult result = service.stop(GROUP_ID);

        verify(restClient).stop("job-b", false, false);
        assertEquals("STOPPED", healthy.getStatus());
        assertEquals("FAILED", stuck.getStatus());
        assertEquals("job-a", stuck.getEngineJobId(), "kept, so a retried stop still reaches the job");
        assertEquals("FAILED", group.getStatus());
        assertTrue(result.getMessage().contains("customers"), result.getMessage());
    }

    @Test
    void stopSkipsTablesWhoseJobIsAlreadyOver() {
        SyncTaskGroup group = persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem stopped = runningItem(11L, "customers", "job-a");
        stopped.setStatus("STOPPED");
        SyncTaskGroupItem finished = runningItem(12L, "orders", "job-b");
        finished.setStatus("FINISHED");
        SyncTaskGroupItem running = runningItem(13L, "invoices", "job-c");
        items.add(stopped);
        items.add(finished);
        items.add(running);

        service.stop(GROUP_ID);

        verify(restClient).stop("job-c", false, false);
        verify(restClient, never()).stop(eq("job-a"), anyBoolean(), anyBoolean());
        verify(restClient, never()).stop(eq("job-b"), anyBoolean(), anyBoolean());
        assertEquals("STOPPED", group.getStatus());
    }

    @Test
    void resumeItemReadsTheNewBaselineBeforeSubmitting() {
        SyncTaskGroup group = persisted(group("DEGRADED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem blocked = runningItem(11L, "customers", "job-a");
        blocked.setStatus("DDL_BLOCKED");
        blocked.setEngineConfigHash(fingerprint(group, blocked, postgres));
        items.add(blocked);
        when(itemMapper.selectOneOfGroup(GROUP_ID, 11L)).thenReturn(blocked);
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), eq("customers"))).thenThrow(new ServiceException("连接超时"));

        assertThrows(ServiceException.class, () -> service.resumeItem(GROUP_ID, 11L));

        // Failing after the submit would have left a running job behind a row saying DDL_BLOCKED.
        verify(restClient, never()).submit(anyString(), anyString(), anyString(), anyBoolean());
        assertEquals("DDL_BLOCKED", blocked.getStatus());
    }

    // ------------------------------------------------------------------ transaction boundaries

    /**
     * The lifecycle talks to the engine, and a rollback cannot un-submit a job; an @Transactional
     * on any of these would also commit only after the group lock is released. See the class
     * comment of SyncTaskGroupServiceImpl - this pins the decision so it is not quietly re-added.
     */
    @Test
    void theEngineFacingLifecycleRunsWithoutADatabaseTransaction() throws NoSuchMethodException {
        for (String name : List.of("start", "discover", "pause", "resume", "stop", "refreshStatus")) {
            assertFalse(SyncTaskGroupServiceImpl.class.getMethod(name, Long.class).isAnnotationPresent(Transactional.class), name);
        }
        for (String name : List.of("resumeItem", "reinitializeItem")) {
            assertFalse(SyncTaskGroupServiceImpl.class.getMethod(name, Long.class, Long.class).isAnnotationPresent(Transactional.class), name);
        }
        assertFalse(SyncTaskGroupDdlServiceImpl.class.getMethod("checkDdl", Long.class).isAnnotationPresent(Transactional.class));
        assertFalse(SyncTaskGroupDdlServiceImpl.class.getMethod("resumeDdlItem", Long.class, Long.class).isAnnotationPresent(Transactional.class));
    }

    @Test
    void aGroupDeleteCommitsWhileItStillHoldsTheGroupLock() {
        persisted(group("STOPPED", "MULTI_TABLE", POSTGRES_ID));
        when(groupMapper.deleteById(GROUP_ID)).thenReturn(1);
        AtomicBoolean committedUnderLock = new AtomicBoolean();
        doAnswer(invocation -> {
            committedUnderLock.set(lockHeld.get());
            return null;
        }).when(transactionManager).commit(any());

        assertTrue(service.deleteById(GROUP_ID));

        verify(locks).withGroupLock(eq(GROUP_ID), any());
        verify(transactionManager).commit(any());
        assertTrue(committedUnderLock.get(), "the transaction must commit before the group lock is released");
    }

    // ------------------------------------------------------------------ Kafka bridge capacity

    @Test
    void aFullBridgePoolRefusesAKafkaGroupResumeBeforeAnyTableIsResubmitted() {
        persisted(group("PAUSED", "MULTI_TABLE", KAFKA_ID));
        SyncTaskGroupItem first = runningItem(11L, "customers", "job-a");
        first.setStatus("PAUSED");
        SyncTaskGroupItem second = runningItem(12L, "orders", "job-b");
        second.setStatus("PAUSED");
        items.add(first);
        items.add(second);
        doThrow(new ServiceException("Kafka 桥接容量不足（已用 63/64，本次需要 2 个）：请调大 sync.kafka-bridge.max-workers"))
            .when(bridge).requireCapacity(any());

        String refused = assertThrows(ServiceException.class, () -> service.resume(GROUP_ID)).getMessage();

        assertTrue(refused.contains("本次需要 2 个"), refused);
        verify(bridge).requireCapacity(List.of(11L, 12L));
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
        verify(bridge, never()).startGroupItem(any(), any(), any());
        assertEquals("PAUSED", first.getStatus());
        assertEquals("PAUSED", second.getStatus());
    }

    @Test
    void aTableRefusedABridgeIsNeverSubmittedAndItsSiblingsAreCompensated() {
        SyncTaskGroup group = persisted(group("STOPPED", "MULTI_TABLE", KAFKA_ID));
        SyncTaskGroupItem first = item(11L, "customers", "PENDING");
        SyncTaskGroupItem second = item(12L, "orders", "PENDING");
        items.add(first);
        items.add(second);
        doThrow(new ServiceException("Kafka 桥接容量已满（64/64）：请调大 sync.kafka-bridge.max-workers"))
            .when(bridge).startGroupItem(argThat(task -> task.getTaskId() == 12L), any(), any());

        SyncTaskGroupOperationResult result = service.start(GROUP_ID);

        // Only the first table reached the engine, and it was stopped again with its bridge.
        verify(restClient, times(1)).submit(anyString(), anyString(), isNull(), eq(false));
        verify(restClient).stop("job-100", false, false);
        verify(bridge).stop(11L);
        assertEquals("STOPPED", first.getStatus());
        assertEquals("FAILED", second.getStatus());
        assertEquals("FAILED", group.getStatus());
        assertTrue(result.getMessage().contains("sync.kafka-bridge.max-workers"), result.getMessage());
    }

    @Test
    void aFullBridgePoolNeverIsolatesAKafkaTableThatAlreadyRunsOnTheEngine() {
        SyncTaskGroup group = persisted(group("RUNNING", "MULTI_TABLE", KAFKA_ID));
        SyncTaskGroupItem item = runningItem(11L, "customers", "job-a");
        items.add(item);
        when(restClient.status("job-a")).thenReturn(new SeaTunnelRestClient.JobSnapshot("job-a", "x", "RUNNING", null, null));
        when(restClient.checkpoints(anyString())).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());
        when(bridge.isRunning(11L)).thenReturn(false);
        when(bridge.tryStartGroupItem(any(), any(), any())).thenReturn(false); // parked: pool full

        // More passes than the transient-failure tolerance: a throwing heal would isolate the table by now.
        for (int pass = 0; pass < 4; pass++) service.refreshStatus(GROUP_ID);

        assertEquals("RUNNING", item.getStatus());
        assertEquals("RUNNING", group.getStatus());
        verify(bridge, times(4)).tryStartGroupItem(any(), any(), eq("source_db"));
        verify(bridge, never()).startGroupItem(any(), any(), any());
    }

    // ------------------------------------------------------------------ table-level reinitialize

    @Test
    void reinitializeRebuildsOneTableAndWidensAFullSelectionToTheNewColumn() {
        SyncTaskGroup group = persisted(group("DEGRADED", "MULTI_TABLE", POSTGRES_ID));
        group.setEngineJobId("job-old,job-other");
        SyncTaskGroupItem blocked = runningItem(11L, "customers", "job-old");
        blocked.setStatus("DDL_BLOCKED");
        blocked.setSelectedColumns("id,name");
        blocked.setSchemaSnapshot(TableSchemaSnapshot.toJson(TableSchemaSnapshot.of(metadata("id", "name"))));
        items.add(blocked);
        when(itemMapper.selectOneOfGroup(GROUP_ID, 11L)).thenReturn(blocked);
        // The source table has grown a column since the baseline.
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), eq("customers"))).thenReturn(metadata("id", "name", "email"));

        SyncTaskGroupOperationResult result = service.reinitializeItem(GROUP_ID, 11L);

        verify(restClient).stop("job-old", false, true);
        assertEquals("id,name,email", blocked.getSelectedColumns());
        assertEquals("RUNNING", blocked.getStatus());
        assertEquals("job-100", blocked.getEngineJobId());
        assertEquals("job-other,job-100", group.getEngineJobId());
        verify(itemMapper).clearCheckpoint(11L);
        verify(ddlEventMapper).resolveOpen(eq(11L), anyString());
        assertTrue(result.getMessage().contains("已纳入源表新增字段"));
    }

    @Test
    void reinitializeKeepsAnExplicitPartialSelectionAndRefusesAnIncompatibleTarget() {
        persisted(group("DEGRADED", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem failed = runningItem(11L, "customers", "job-old");
        failed.setStatus("FAILED");
        failed.setSelectedColumns("id");
        failed.setSchemaSnapshot(TableSchemaSnapshot.toJson(TableSchemaSnapshot.of(metadata("id", "name"))));
        items.add(failed);
        when(itemMapper.selectOneOfGroup(GROUP_ID, 11L)).thenReturn(failed);
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), eq("customers"))).thenReturn(metadata("id", "name", "email"));
        when(metadataService.checkTargetCompatibility(anyLong(), anyLong(), anyString(), any(), anyString(), eq("id"), any())).thenReturn(compatibility(false));

        assertTrue(assertThrows(ServiceException.class, () -> service.reinitializeItem(GROUP_ID, 11L)).getMessage().contains("目标表兼容性未通过"));
        assertEquals("id", failed.getSelectedColumns());
        verify(restClient, never()).submit(anyString(), anyString(), any(), eq(false));
        // Refused before anything destructive: the old job and its savepoint are still there.
        verify(restClient, never()).stop(anyString(), anyBoolean(), anyBoolean());
        assertEquals("job-old", failed.getEngineJobId());
    }

    @Test
    void aReinitializeWhoseSubmitFailsParksTheTableWithoutItsDestroyedJob() {
        SyncTaskGroup group = persisted(group("DEGRADED", "MULTI_TABLE", POSTGRES_ID));
        group.setEngineJobId("job-old,job-other");
        SyncTaskGroupItem blocked = runningItem(11L, "customers", "job-old");
        blocked.setStatus("DDL_BLOCKED");
        items.add(blocked);
        items.add(runningItem(12L, "orders", "job-other"));
        when(itemMapper.selectOneOfGroup(GROUP_ID, 11L)).thenReturn(blocked);
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenThrow(new ServiceException("SeaTunnel 接口不可用：refused"));

        assertThrows(ServiceException.class, () -> service.reinitializeItem(GROUP_ID, 11L));

        verify(restClient).stop("job-old", false, true);
        verify(itemMapper).detachEngineJob(11L);
        assertEquals("FAILED", blocked.getStatus());
        assertTrue(blocked.getLastError().startsWith("重新初始化提交失败"), blocked.getLastError());
        assertEquals("job-other", group.getEngineJobId(), "the destroyed job leaves the group's job list");
        assertEquals("DEGRADED", group.getStatus());
    }

    @Test
    void reinitializeIsOnlyForParkedTablesOfAStartedGroup() {
        persisted(group("RUNNING", "MULTI_TABLE", POSTGRES_ID));
        SyncTaskGroupItem running = runningItem(11L, "customers", "job-a");
        when(itemMapper.selectOneOfGroup(GROUP_ID, 11L)).thenReturn(running);
        assertTrue(assertThrows(ServiceException.class, () -> service.reinitializeItem(GROUP_ID, 11L)).getMessage().contains("只有失败、结构阻塞"));

        persisted(group("DRAFT", "MULTI_TABLE", POSTGRES_ID));
        running.setStatus("FAILED");
        assertTrue(assertThrows(ServiceException.class, () -> service.reinitializeItem(GROUP_ID, 11L)).getMessage().contains("尚未启动"));
    }

    // ------------------------------------------------------------------ fixtures

    private SyncTaskGroup persisted(SyncTaskGroup group) {
        when(groupMapper.selectById(GROUP_ID)).thenReturn(group);
        return group;
    }

    private static SyncTaskGroup group(String status, String scope, long targetId) {
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(GROUP_ID);
        group.setGroupName("g");
        group.setStatus(status);
        group.setSyncScope(scope);
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

    private static SyncTaskGroupItem item(long id, String table, String status) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(GROUP_ID);
        item.setSourceDatabase("source_db");
        item.setSourceTable(table);
        item.setTargetSchema("public");
        item.setTargetTable(table);
        item.setStatus(status);
        item.setSelectedColumns("id,name");
        item.setSyncKeyColumns("id");
        item.setDdlPolicy("FAIL");
        return item;
    }

    private static SyncTaskGroupItem runningItem(long id, String table, String jobId) {
        SyncTaskGroupItem item = item(id, table, "RUNNING");
        item.setEngineJobId(jobId);
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

    private String fingerprint(SyncTaskGroup group, SyncTaskGroupItem item, DataSource target) {
        return SyncTaskGroupConfigGenerator.generateItem(group, item, mysql, target, properties, SourceColumns.fromMetadata(metadataService, mysql)).fingerprint();
    }
}
