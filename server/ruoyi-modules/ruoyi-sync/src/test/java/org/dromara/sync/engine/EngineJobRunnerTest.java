package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * The engine-side invariants both lifecycles (single-table task and task-group item) now share:
 * bridge before submit with compensation, the resume guard, and the status-poll tolerance.
 */
@Tag("dev")
class EngineJobRunnerTest {

    private static final long OWNER = 42L;

    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final KafkaTaskBridgeService bridge = mock(KafkaTaskBridgeService.class);
    private final EngineJobRunner runner = new EngineJobRunner(restClient, bridge);

    private final SeaTunnelJobConfigGenerator.GeneratedConfig generated = new SeaTunnelJobConfigGenerator.GeneratedConfig(
        "ds-task-42", "customers", "customers", List.of("id"), List.of("id", "name"), "env {}", "env {}");

    // ------------------------------------------------------------------ submit

    @Test
    void aKafkaJobGetsItsBridgeBeforeTheSubmitAndLosesItWhenTheSubmitFails() {
        EngineJobRunner.Job job = job("KAFKA", false);
        when(restClient.submit(anyString(), anyString(), any(), anyBoolean())).thenThrow(new ServiceException("SeaTunnel 接口不可用：refused"));

        assertThrows(ServiceException.class, () -> runner.submit(job, generated));

        InOrder order = inOrder(bridge, restClient);
        order.verify(bridge).start(job.task(), job.target(), "source_db");
        order.verify(restClient).submit("ds-task-42", "env {}", null, false);
        order.verify(bridge).stop(OWNER);
    }

    @Test
    void aRefusedBridgeMeansNothingReachesTheEngine() {
        EngineJobRunner.Job job = job("KAFKA", true);
        doThrow(new ServiceException("Kafka 桥接容量已满（64/64）")).when(bridge).startGroupItem(any(), any(), any());

        assertThrows(ServiceException.class, () -> runner.submit(job, generated));

        verify(restClient, never()).submit(anyString(), anyString(), any(), anyBoolean());
    }

    @Test
    void aPreflightedBridgeIsNotStartedTwice() {
        EngineJobRunner.Job job = job("KAFKA", false);
        when(bridge.isRunning(OWNER)).thenReturn(true);
        when(restClient.submit(anyString(), anyString(), any(), anyBoolean())).thenReturn(new SeaTunnelRestClient.SubmitResult("job-1", "ds-task-42"));

        assertEquals("job-1", runner.submit(job, generated));

        verify(bridge, never()).start(any(), any(), any());
    }

    @Test
    void aRelationalJobNeverTouchesTheBridge() {
        EngineJobRunner.Job job = job("POSTGRESQL", false);
        when(restClient.submit(anyString(), anyString(), any(), anyBoolean())).thenReturn(new SeaTunnelRestClient.SubmitResult("job-1", "ds-task-42"));

        runner.submit(job, generated);
        runner.halt(OWNER, false, "job-1", true);

        verifyNoInteractions(bridge);
    }

    // ------------------------------------------------------------------ resume guard

    @Test
    void resumeIsRefusedWhenTheConfigChangedOrThereIsNoSavepoint() {
        assertTrue(runner.resumeRefusal(generated, "stale", "job-1").contains("配置已变化"));

        when(restClient.checkpoints("job-1")).thenReturn(SeaTunnelRestClient.CheckpointSnapshot.empty());
        assertTrue(runner.resumeRefusal(generated, generated.fingerprint(), "job-1").contains("checkpoint/savepoint"));

        when(restClient.checkpoints("job-1")).thenReturn(new SeaTunnelRestClient.CheckpointSnapshot("7", LocalDateTime.now(), "COMPLETED"));
        assertNull(runner.resumeRefusal(generated, generated.fingerprint(), "job-1"));
        // A row saved before fingerprints existed has nothing to compare, only the savepoint check applies.
        assertNull(runner.resumeRefusal(generated, null, "job-1"));
    }

    @Test
    void anUnreachableCheckpointEndpointIsNotAnAnswerAboutTheSavepoint() {
        when(restClient.checkpoints("job-1")).thenThrow(new ServiceException("SeaTunnel 接口不可用：timeout"));

        assertThrows(ServiceException.class, () -> runner.resumeRefusal(generated, generated.fingerprint(), "job-1"));
    }

    @Test
    void aRecoveryBoundaryFromTheCheckpointEndpointIsARefusal() {
        when(restClient.checkpoints("job-1")).thenThrow(new ServiceException("checkpoint 不存在"));

        String refusal = runner.resumeRefusal(generated, generated.fingerprint(), "job-1");

        assertNotNull(refusal);
        assertTrue(EngineJobStates.isRecoveryBoundaryError(refusal), refusal);
    }

    // ------------------------------------------------------------------ poll

    @Test
    void aJobIsGivenUpOnlyAfterTheToleranceAndAnAnswerResetsTheCount() {
        when(restClient.status("job-1")).thenThrow(new ServiceException("SeaTunnel 接口不可用：timeout"));

        assertEquals(1, assertInstanceOf(EngineJobRunner.Poll.Unreachable.class, runner.poll(OWNER, "job-1")).streak());
        assertEquals(2, assertInstanceOf(EngineJobRunner.Poll.Unreachable.class, runner.poll(OWNER, "job-1")).streak());
        assertInstanceOf(EngineJobRunner.Poll.Lost.class, runner.poll(OWNER, "job-1"));
        // Given up once; a fresh streak starts from one again.
        assertEquals(1, assertInstanceOf(EngineJobRunner.Poll.Unreachable.class, runner.poll(OWNER, "job-1")).streak());

        mockRunning();
        assertInstanceOf(EngineJobRunner.Poll.Observed.class, runner.poll(OWNER, "job-1"));
        doThrow(new ServiceException("SeaTunnel 接口不可用：timeout")).when(restClient).status("job-1");
        assertEquals(1, assertInstanceOf(EngineJobRunner.Poll.Unreachable.class, runner.poll(OWNER, "job-1")).streak());
    }

    @Test
    void aRecoveryBoundaryIsFinalAtOnce() {
        when(restClient.status("job-1")).thenThrow(new ServiceException("binlog 已过期"));

        assertInstanceOf(EngineJobRunner.Poll.Boundary.class, runner.poll(OWNER, "job-1"));
    }

    @Test
    void aFailedCheckpointReadStillReportsTheJobAndSaysWhetherItWasABoundary() {
        mockRunning();
        doThrow(new ServiceException("checkpoint 不存在")).when(restClient).checkpoints("job-1");

        var observed = assertInstanceOf(EngineJobRunner.Poll.Observed.class, runner.poll(OWNER, "job-1"));

        assertEquals("RUNNING", observed.platformStatus());
        assertTrue(observed.checkpointBoundary());
        assertEquals("checkpoint 不存在", observed.checkpointError());
    }

    // ------------------------------------------------------------------ halt / discard / bridge

    @Test
    void haltStopsTheEngineFirstAndKeepsTheBridgeWhenTheEngineRefuses() {
        doThrow(new ServiceException("SeaTunnel 接口不可用：timeout")).when(restClient).stop("job-1", true, false);

        assertThrows(ServiceException.class, () -> runner.halt(OWNER, true, "job-1", true));

        // The job may still be running; its bridge keeps normalizing until someone decides otherwise.
        verify(bridge, never()).stop(OWNER);
    }

    @Test
    void discardToleratesAJobTheEngineNoLongerKnows() {
        doThrow(new ServiceException("SeaTunnel 中不存在作业 job-1")).when(restClient).stop("job-1", false, true);

        runner.discard(OWNER, true, "job-1");
        runner.discard(OWNER, true, null);

        verify(bridge, org.mockito.Mockito.times(2)).stop(OWNER);
        verify(restClient).stop("job-1", false, true);
    }

    @Test
    void aBridgeThatCannotBeHealedIsReportedNotThrown() {
        EngineJobRunner.Job job = job("KAFKA", false);
        when(bridge.tryStart(any(), any(), any())).thenThrow(new ServiceException("Kafka 目标 topic 不存在"));

        String error = runner.healBridge(job);

        assertTrue(error.contains("topic 不存在"), error);
        assertTrue(error.contains("引擎作业仍在运行"), error);
        when(bridge.isRunning(OWNER)).thenReturn(true);
        assertNull(runner.healBridge(job), "a running bridge is left alone");
    }

    // ------------------------------------------------------------------ fixtures

    /** doReturn, not when(...): the tolerance test re-stubs a call that is currently stubbed to throw. */
    private void mockRunning() {
        doReturn(new SeaTunnelRestClient.JobSnapshot("job-1", "ds-task-42", "RUNNING", null, null)).when(restClient).status("job-1");
        doReturn(SeaTunnelRestClient.CheckpointSnapshot.empty()).when(restClient).checkpoints("job-1");
    }

    private static EngineJobRunner.Job job(String targetType, boolean groupItem) {
        SyncTask task = new SyncTask();
        task.setTaskId(OWNER);
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setDatabaseName("source_db");
        DataSource target = new DataSource();
        target.setSourceType(targetType);
        return new EngineJobRunner.Job(task, source, target, groupItem);
    }
}
