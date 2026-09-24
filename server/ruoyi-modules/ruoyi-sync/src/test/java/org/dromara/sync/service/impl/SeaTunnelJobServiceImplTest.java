package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SeaTunnelJobStatus;
import org.dromara.sync.domain.vo.SyncTaskValidationResult;
import org.dromara.sync.engine.EngineJobRunner;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.engine.SourceColumns;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.service.ISyncTaskService;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncText;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * State-machine tests for the single-table lifecycle: every transition is driven through
 * a mocked engine and checked against what gets persisted on the task row.
 */
@Tag("dev")
class SeaTunnelJobServiceImplTest {

    private static final long TASK_ID = 42L;
    private static final long MYSQL_ID = 1L;
    private static final long POSTGRES_ID = 2L;
    private static final long KAFKA_ID = 3L;

    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final DataSourceMapper dataSourceMapper = mock(DataSourceMapper.class);
    private final IDataSourceService dataSourceService = mock(IDataSourceService.class);
    private final IDataSourceMetadataService metadataService = mock(IDataSourceMetadataService.class);
    private final SeaTunnelProperties properties = new SeaTunnelProperties();
    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final ISyncTaskService syncTaskService = mock(ISyncTaskService.class);
    private final SyncLocks locks = mock(SyncLocks.class);
    private final KafkaTaskBridgeService bridge = mock(KafkaTaskBridgeService.class);
    private final ISyncMetricsService metrics = mock(ISyncMetricsService.class);
    private final JsonMapper json = JsonMapper.builder().build();

    // A real runner over the mocked engine and bridge: these tests pin the lifecycle end to end.
    private final SeaTunnelJobServiceImpl service = new SeaTunnelJobServiceImpl(taskMapper, dataSourceMapper, dataSourceService,
        metadataService, properties, syncTaskService, new ResourceProtectionPolicy(properties), locks, metrics,
        new EngineJobRunner(restClient, bridge));

    private final DataSource mysql = dataSource(MYSQL_ID, "MYSQL", "source_db");
    private final DataSource postgres = dataSource(POSTGRES_ID, "POSTGRESQL", "sink_db");
    private final DataSource kafka = dataSource(KAFKA_ID, "KAFKA", null);

    @BeforeEach
    void wireCollaborators() {
        when(locks.withTaskLock(any(), any())).thenAnswer(invocation -> ((Supplier<?>) invocation.getArgument(1)).get());
        when(dataSourceMapper.selectById(POSTGRES_ID)).thenReturn(postgres);
        when(dataSourceMapper.selectById(KAFKA_ID)).thenReturn(kafka);
        when(dataSourceService.requireUsable(eq(MYSQL_ID), anyString())).thenReturn(mysql);
        when(dataSourceService.requireUsable(eq(POSTGRES_ID), anyString())).thenReturn(postgres);
        when(dataSourceService.requireUsable(eq(KAFKA_ID), anyString())).thenReturn(kafka);
        when(syncTaskService.validate(TASK_ID)).thenReturn(validation(true));
        when(metadataService.queryTableMetadata(eq(MYSQL_ID), anyString(), anyString())).thenAnswer(invocation -> sourceSchema());
    }

    private static DataSourceMetadataVo sourceSchema() {
        DataSourceMetadataVo metadata = new DataSourceMetadataVo();
        for (String name : new String[]{"id", "name", "email"}) {
            DataSourceColumnVo column = new DataSourceColumnVo();
            column.setName(name);
            column.setTypeName("varchar");
            metadata.getColumns().add(column);
        }
        metadata.getPrimaryKeys().add("id");
        return metadata;
    }

    // ------------------------------------------------------------------ start

    @Test
    void startSubmitsTheJobAndMarksTheTaskRunning() {
        SyncTask task = persisted(task("STOPPED", POSTGRES_ID));
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenReturn(new SeaTunnelRestClient.SubmitResult("job-9", "ds-task-42"));

        var result = service.start(TASK_ID);

        assertEquals("RUNNING", task.getStatus());
        assertEquals("job-9", task.getEngineJobId());
        assertEquals(64, task.getEngineConfigHash().length());
        assertEquals("", task.getLastError());
        assertEquals("RUNNING", result.getStatus());
        verify(taskMapper, times(2)).updateById(task); // resource-protection defaults, then RUNNING
        verify(bridge, never()).start(any(), any(), any());
    }

    @Test
    void startIsRefusedForRunningAndForRecoverableTasks() {
        persisted(task("RUNNING", POSTGRES_ID));
        assertTrue(assertThrows(ServiceException.class, () -> service.start(TASK_ID)).getMessage().contains("正在运行"));

        persisted(task("FAILED", POSTGRES_ID));
        assertTrue(assertThrows(ServiceException.class, () -> service.start(TASK_ID)).getMessage().contains("恢复风险"));
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
    }

    @Test
    void aPausedTaskCannotBeStartedFreshOverItsSavepoint() {
        persisted(task("PAUSED", POSTGRES_ID));
        assertTrue(assertThrows(ServiceException.class, () -> service.start(TASK_ID)).getMessage().contains("请使用恢复任务"));
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
    }

    @Test
    void startStopsAtFailedValidationWithoutTouchingTheEngine() {
        SyncTask task = persisted(task("STOPPED", POSTGRES_ID));
        when(syncTaskService.validate(TASK_ID)).thenReturn(validation(false));

        assertTrue(assertThrows(ServiceException.class, () -> service.start(TASK_ID)).getMessage().contains("启动前校验未通过"));
        assertEquals("STOPPED", task.getStatus());
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
    }

    @Test
    void kafkaStartBringsTheBridgeUpBeforeSubmitAndTearsItDownWhenSubmitFails() {
        SyncTask task = persisted(task("STOPPED", KAFKA_ID));
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenThrow(new ServiceException("SeaTunnel 接口不可用：refused"));

        assertThrows(ServiceException.class, () -> service.start(TASK_ID));

        InOrder order = inOrder(bridge, restClient);
        order.verify(bridge).start(eq(task), eq(kafka), eq("source_db"));
        order.verify(restClient).submit(anyString(), anyString(), isNull(), eq(false));
        order.verify(bridge).stop(TASK_ID);
        assertEquals("FAILED", task.getStatus());
        assertTrue(task.getLastError().contains("refused"));
    }

    @Test
    void aFullBridgePoolRefusesAKafkaStartBeforeAnythingIsSubmitted() {
        SyncTask task = persisted(task("STOPPED", KAFKA_ID));
        doThrow(new ServiceException("Kafka 桥接容量已满（2/2）：请调大 sync.kafka-bridge.max-workers"))
            .when(bridge).start(any(), any(), any());

        String refused = assertThrows(ServiceException.class, () -> service.start(TASK_ID)).getMessage();

        assertTrue(refused.contains("sync.kafka-bridge.max-workers"), refused);
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
        assertEquals("STOPPED", task.getStatus());
        assertNull(task.getLastError());
    }

    @Test
    void aFullBridgePoolRefusesAKafkaResumeBeforeTheSavepointIsResubmitted() {
        SyncTask task = persisted(task("PAUSED", KAFKA_ID));
        task.setEngineConfigHash(fingerprint(task, kafka));
        when(restClient.checkpoints("job-1")).thenReturn(new SeaTunnelRestClient.CheckpointSnapshot("7", LocalDateTime.now(), "COMPLETED"));
        doThrow(new ServiceException("Kafka 桥接容量已满（2/2）：请调大 sync.kafka-bridge.max-workers"))
            .when(bridge).start(any(), any(), any());

        String refused = assertThrows(ServiceException.class, () -> service.resume(TASK_ID)).getMessage();

        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
        assertTrue(refused.contains("max-workers"), refused);
        // Nothing reached the engine, so nothing changed: still PAUSED on its savepoint, no
        // "运行失败" alert, and resume simply works once a slot frees.
        assertEquals("PAUSED", task.getStatus());
    }

    @Test
    void aFullBridgePoolNeverFailsAKafkaTaskThatAlreadyRunsOnTheEngine() {
        SyncTask task = persisted(running(KAFKA_ID));
        when(restClient.status("job-1")).thenReturn(snapshot("RUNNING", null, null));
        when(restClient.checkpoints("job-1")).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());
        when(bridge.isRunning(TASK_ID)).thenReturn(false);
        when(bridge.tryStart(any(), any(), any())).thenReturn(false); // parked: pool full
        when(taskMapper.selectActive()).thenReturn(List.of(task));

        SeaTunnelJobStatus status = service.refreshStatus(TASK_ID);
        service.refreshRunningTaskStatus(); // the background pass fails a task on any exception

        assertEquals("RUNNING", status.getStatus());
        assertEquals("RUNNING", task.getStatus());
        assertEquals("", task.getLastError());
        verify(bridge, never()).start(any(), any(), any());
        verify(bridge, never()).stop(TASK_ID);
    }

    // ------------------------------------------------------------------ refreshStatus

    @Test
    void refreshCopiesEngineStateCheckpointAndMetricsOntoTheTask() {
        SyncTask task = persisted(running(POSTGRES_ID));
        LocalDateTime checkpointTime = LocalDateTime.of(2026, 9, 22, 10, 0);
        when(restClient.status("job-1")).thenReturn(snapshot("RUNNING", null, "{\"SourceReceivedCount\":\"5\",\"SinkCommittedCount\":\"3\"}"));
        when(restClient.checkpoints("job-1")).thenReturn(new SeaTunnelRestClient.CheckpointSnapshot("7", checkpointTime, "COMPLETED"));

        SeaTunnelJobStatus status = service.refreshStatus(TASK_ID);

        assertEquals("RUNNING", task.getStatus());
        assertEquals("7", task.getLastCheckpointId());
        assertEquals(checkpointTime, task.getLastCheckpointTime());
        assertEquals("", task.getLastError());
        assertEquals(5L, status.getSourceReceivedCount());
        assertEquals(2L, status.getBacklogRows());
        verify(metrics).recordTask(eq(task), eq("RUNNING"), any());
    }

    @Test
    void refreshMovesACompletedBatchJobToFinished() {
        SyncTask task = persisted(running(POSTGRES_ID));
        when(restClient.status("job-1")).thenReturn(snapshot("FINISHED", null, null));
        when(restClient.checkpoints("job-1")).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());

        assertEquals("FINISHED", service.refreshStatus(TASK_ID).getStatus());
        assertEquals("FINISHED", task.getStatus());
    }

    @Test
    void transientEngineFailuresAreToleratedTwiceThenFailTheTask() {
        SyncTask task = persisted(running(POSTGRES_ID));
        when(restClient.status("job-1")).thenThrow(new ServiceException("SeaTunnel 接口不可用：I/O error"));

        for (int attempt = 1; attempt <= 2; attempt++) {
            SeaTunnelJobStatus status = service.refreshStatus(TASK_ID);
            assertEquals("UNREACHABLE_TRANSIENT", status.getEngineStatus());
            assertEquals("RUNNING", status.getStatus());
            assertTrue(status.getErrorMessage().contains("第 " + attempt + "/3 次"));
        }
        verify(taskMapper, never()).updateById(any(SyncTask.class));

        SeaTunnelJobStatus failed = service.refreshStatus(TASK_ID);
        assertEquals("FAILED", failed.getStatus());
        assertEquals("FAILED", task.getStatus());
        assertTrue(task.getLastError().contains("I/O error"));
        verify(taskMapper).updateById(task);
    }

    @Test
    void aRecoveryBoundaryErrorSkipsTheGraceWindowAndRequiresReinitialize() {
        SyncTask task = persisted(running(KAFKA_ID));
        when(restClient.status("job-1")).thenThrow(new ServiceException("checkpoint 位点已过期，无法恢复"));

        SeaTunnelJobStatus status = service.refreshStatus(TASK_ID);

        assertEquals("REINITIALIZE_REQUIRED", status.getStatus());
        assertEquals("RECOVERY_REQUIRED", status.getEngineStatus());
        assertEquals("REINITIALIZE_REQUIRED", task.getStatus());
        verify(bridge).stop(TASK_ID);
    }

    @Test
    void aBoundaryErrorFromTheCheckpointEndpointAlsoRequiresReinitialize() {
        SyncTask task = persisted(running(POSTGRES_ID));
        when(restClient.status("job-1")).thenReturn(snapshot("RUNNING", null, null));
        when(restClient.checkpoints("job-1")).thenThrow(new ServiceException("savepoint 不可用"));

        service.refreshStatus(TASK_ID);
        assertEquals("REINITIALIZE_REQUIRED", task.getStatus());
    }

    @Test
    void refreshAppliesTheBridgeRuleForKafkaTargets() {
        SyncTask task = persisted(running(KAFKA_ID));
        when(restClient.checkpoints("job-1")).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());

        when(restClient.status("job-1")).thenReturn(snapshot("RUNNING", null, null));
        when(bridge.isRunning(TASK_ID)).thenReturn(false);
        when(bridge.tryStart(any(), any(), any())).thenReturn(true);
        service.refreshStatus(TASK_ID);
        // The heal of a job that already runs never takes the operator path that refuses when full.
        verify(bridge).tryStart(eq(task), eq(kafka), eq("source_db"));
        verify(bridge, never()).start(any(), any(), any());

        when(restClient.status("job-1")).thenReturn(snapshot("DOING_SAVEPOINT", null, null));
        service.refreshStatus(TASK_ID);
        assertEquals("PAUSING", task.getStatus());
        verify(bridge).stop(TASK_ID);
    }

    @Test
    void aFailedOverwriteSwapKeepsTheEngineOutcomeVisible() {
        SyncTask task = persisted(running(POSTGRES_ID));
        task.setSyncMode("FULL");
        task.setOverwriteStageTable("__ds_stage_42_v1");
        when(restClient.status("job-1")).thenReturn(snapshot("FINISHED", null, null));
        when(restClient.checkpoints("job-1")).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());

        // No JDBC driver / target reachable here, so the swap fails - the task must say so, not pretend it finished.
        SeaTunnelJobStatus status = service.refreshStatus(TASK_ID);

        assertEquals("FAILED", status.getStatus());
        assertEquals("FINISHED", status.getEngineStatus());
        assertTrue(task.getLastError().startsWith("覆盖刷新替换正式表失败"));
    }

    // ------------------------------------------------------------------ pause / resume / stop / reinitialize

    @Test
    void pauseRequestsASavepointStopOnlyWhileRunning() {
        SyncTask task = persisted(running(POSTGRES_ID));
        service.pause(TASK_ID);
        verify(restClient).stop("job-1", true, false);
        assertEquals("PAUSING", task.getStatus());

        persisted(task("STOPPED", POSTGRES_ID));
        assertThrows(ServiceException.class, () -> service.pause(TASK_ID));
    }

    @Test
    void resumeReusesTheSavepointOnlyWhenTheConfigFingerprintStillMatches() {
        SyncTask task = persisted(task("PAUSED", POSTGRES_ID));
        task.setEngineConfigHash(fingerprint(task, postgres));
        when(restClient.checkpoints("job-1")).thenReturn(new SeaTunnelRestClient.CheckpointSnapshot("7", LocalDateTime.now(), "COMPLETED"));

        service.resume(TASK_ID);

        verify(restClient).submit(eq("ds-task-42"), anyString(), eq("job-1"), eq(true));
        assertEquals("RUNNING", task.getStatus());
    }

    @Test
    void aPasswordRotationOrALegacyRawConfigHashStillResumesFromTheSavepoint() {
        SyncTask task = persisted(task("PAUSED", POSTGRES_ID));
        when(restClient.checkpoints("job-1")).thenReturn(new SeaTunnelRestClient.CheckpointSnapshot("7", LocalDateTime.now(), "COMPLETED"));
        // Rows written before the fingerprint moved to the redacted config carry the raw-config hash.
        task.setEngineConfigHash(SyncText.sha256Hex(SeaTunnelJobConfigGenerator.generate(task, mysql, postgres, properties, columns()).config()));
        service.resume(TASK_ID);
        assertEquals("RUNNING", task.getStatus());

        // Credentials are not part of the fingerprint, so rotating a password keeps the checkpoint usable.
        task.setStatus("PAUSED");
        task.setEngineConfigHash(fingerprint(task, postgres));
        postgres.setPassword("rotated-secret");
        service.resume(TASK_ID);
        assertEquals("RUNNING", task.getStatus());
        verify(restClient, times(2)).submit(eq("ds-task-42"), anyString(), eq("job-1"), eq(true));
    }

    @Test
    void resumeWithAChangedConfigLandsInReinitializeRequiredNotFailed() {
        SyncTask task = persisted(task("PAUSED", POSTGRES_ID));
        task.setEngineConfigHash("stale-fingerprint");

        assertTrue(assertThrows(ServiceException.class, () -> service.resume(TASK_ID)).getMessage().contains("配置已变化"));
        // FAILED would invite another resume; the savepoint is unusable for good.
        assertEquals("REINITIALIZE_REQUIRED", task.getStatus());
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
    }

    @Test
    void resumeIsRefusedWithoutACheckpoint() {
        SyncTask task = persisted(task("PAUSED", POSTGRES_ID));
        task.setEngineConfigHash(fingerprint(task, postgres));
        when(restClient.checkpoints("job-1")).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());

        assertTrue(assertThrows(ServiceException.class, () -> service.resume(TASK_ID)).getMessage().contains("checkpoint/savepoint"));
        assertEquals("REINITIALIZE_REQUIRED", task.getStatus());
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
    }

    @Test
    void aManualStartRearmsAParkedCronSchedule() {
        SyncTask task = persisted(task("STOPPED", POSTGRES_ID));
        task.setScheduleMode("CRON");
        task.setCronExpression("0 0 * * * *");
        task.setNextRunTime(null); // parked by an earlier stop or a failed run
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenReturn(new SeaTunnelRestClient.SubmitResult("job-9", "ds-task-42"));

        service.start(TASK_ID);

        assertEquals("RUNNING", task.getStatus());
        assertNotNull(task.getNextRunTime());
        assertTrue(task.getNextRunTime().isAfter(LocalDateTime.now()));
    }

    @Test
    void stopCancelsTheJobAndDisarmsTheSchedule() {
        SyncTask task = persisted(running(POSTGRES_ID));
        task.setNextRunTime(LocalDateTime.now().minusMinutes(1));

        service.stop(TASK_ID);

        verify(restClient).stop("job-1", false, false);
        assertEquals("STOPPED", task.getStatus());
        assertNull(task.getNextRunTime());
    }

    @Test
    void reinitializeDiscardsTheOldJobEvenIfItIsGoneAndResubmitsFresh() {
        SyncTask task = persisted(task("STOPPED", POSTGRES_ID));
        task.setLastCheckpointId("7");
        doThrow(new ServiceException("SeaTunnel 中不存在作业 job-1")).when(restClient).stop("job-1", false, true);
        when(restClient.submit(anyString(), anyString(), isNull(), eq(false))).thenReturn(new SeaTunnelRestClient.SubmitResult("job-2", "ds-task-42"));

        service.reinitialize(TASK_ID);

        assertEquals("RUNNING", task.getStatus());
        assertEquals("job-2", task.getEngineJobId());
        assertNull(task.getLastCheckpointId());
        // The null checkpoint columns go through the explicit mapper UPDATE, since updateById drops nulls.
        verify(taskMapper).clearCheckpoint(TASK_ID);
    }

    @Test
    void aRefusedReinitializeLeavesTheOldJobAndTheTaskAsTheyWere() {
        SyncTask task = persisted(task("FAILED", POSTGRES_ID));
        task.setLastCheckpointId("7");
        when(syncTaskService.validate(TASK_ID)).thenReturn(validation(false));

        assertTrue(assertThrows(ServiceException.class, () -> service.reinitialize(TASK_ID)).getMessage().contains("校验未通过"));

        // A FAILED task can still resume from job-1's savepoint; a refusal must not destroy it.
        verify(restClient, never()).stop(anyString(), anyBoolean(), anyBoolean());
        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
        assertEquals("FAILED", task.getStatus());
        assertEquals("job-1", task.getEngineJobId());
        assertEquals("7", task.getLastCheckpointId());
    }

    @Test
    void reinitializeHasNoMeaningForIncrementalTasks() {
        SyncTask task = persisted(task("STOPPED", POSTGRES_ID));
        task.setSyncMode("INCREMENTAL");
        assertTrue(assertThrows(ServiceException.class, () -> service.reinitialize(TASK_ID)).getMessage().contains("纯增量"));
    }

    // ------------------------------------------------------------------ fixtures

    private SyncTask persisted(SyncTask task) {
        when(taskMapper.selectById(TASK_ID)).thenReturn(task);
        return task;
    }

    private static SyncTask running(long targetId) {
        return task("RUNNING", targetId);
    }

    private static SyncTask task(String status, long targetId) {
        SyncTask task = new SyncTask();
        task.setTaskId(TASK_ID);
        task.setTaskName("t");
        task.setStatus(status);
        task.setSourceId(MYSQL_ID);
        task.setTargetId(targetId);
        task.setSourceTable("customers");
        task.setTargetSchema("public");
        task.setTargetTable(targetId == KAFKA_ID ? "customer-events" : "customers");
        task.setSyncMode("FULL_CDC");
        task.setConfigVersion(1);
        task.setSelectedColumns("id,name");
        task.setSyncKeyColumns("id");
        task.setEngineJobId("job-1");
        task.setReadLimitRowsPerSecond(100);
        task.setReadLimitBytesPerSecond(1024L);
        task.setSnapshotParallelism(1);
        task.setSourceConnectionLimit(2);
        return task;
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

    private String fingerprint(SyncTask task, DataSource target) {
        return SeaTunnelJobConfigGenerator.generate(task, mysql, target, properties, columns()).fingerprint();
    }

    private SourceColumns columns() {
        return SourceColumns.fromMetadata(metadataService, mysql);
    }

    private SeaTunnelRestClient.JobSnapshot snapshot(String status, String error, String metricsJson) {
        return new SeaTunnelRestClient.JobSnapshot("job-1", "ds-task-42", status, error, metricsJson == null ? null : json.readTree(metricsJson));
    }

    private static SyncTaskValidationResult validation(boolean valid) {
        SyncTaskValidationResult result = new SyncTaskValidationResult();
        result.setValid(valid);
        result.setMessage(valid ? "ok" : "连接校验未通过");
        return result;
    }
}
