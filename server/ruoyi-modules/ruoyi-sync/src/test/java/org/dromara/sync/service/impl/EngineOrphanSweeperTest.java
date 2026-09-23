package org.dromara.sync.service.impl;

import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.SyncLocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The safety net for a submit whose answer was lost: the engine is running the job, the
 * platform holds no job id for it, and nothing else would ever look at it again.
 */
@Tag("dev")
class EngineOrphanSweeperTest {

    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final SyncLocks locks = mock(SyncLocks.class);
    private final EngineOrphanSweeper sweeper =
        new EngineOrphanSweeper(taskMapper, groupMapper, itemMapper, restClient, locks);

    @BeforeEach
    void nothingIsLockedByDefault() {
        when(locks.isTaskBusy(any())).thenReturn(false);
        when(locks.isGroupBusy(any())).thenReturn(false);
    }

    @Test
    void aTaskThatLostItsJobIdIsReattachedToTheRunningJob() {
        SyncTask lost = task(42L, "FAILED", null);
        when(restClient.runningJobIdsByName()).thenReturn(Map.of("ds-task-42", "job-9"));
        when(taskMapper.selectById(42L)).thenReturn(lost);

        assertEquals(1, sweeper.sweepOnce());

        assertEquals("job-9", lost.getEngineJobId());
        assertEquals("RUNNING", lost.getStatus());
        verify(taskMapper).updateById(lost);
    }

    @Test
    void aTaskThatAlreadyHoldsAJobIdIsLeftAlone() {
        // The status reconcile owns this row; repointing it here could hide a real divergence.
        SyncTask healthy = task(42L, "RUNNING", "job-1");
        when(restClient.runningJobIdsByName()).thenReturn(Map.of("ds-task-42", "job-9"));
        when(taskMapper.selectById(42L)).thenReturn(healthy);

        assertEquals(0, sweeper.sweepOnce());

        assertEquals("job-1", healthy.getEngineJobId());
        verify(taskMapper, never()).updateById(any(SyncTask.class));
    }

    @Test
    void aLockedTaskIsSkippedSoAnInFlightRebuildIsNotAdoptedMidFlight() {
        // reinitialize stops the old job and submits a new one under the same name.
        SyncTask rebuilding = task(42L, "FAILED", null);
        when(restClient.runningJobIdsByName()).thenReturn(Map.of("ds-task-42", "job-old"));
        when(taskMapper.selectById(42L)).thenReturn(rebuilding);
        when(locks.isTaskBusy(42L)).thenReturn(true);

        assertEquals(0, sweeper.sweepOnce());
        verify(taskMapper, never()).updateById(any(SyncTask.class));
    }

    @Test
    void aGroupItemIsReattachedAndItsGroupStatusRecomputed() {
        SyncTaskGroupItem lost = item(77L, 7L, "FAILED", null);
        SyncTaskGroupItem sibling = item(78L, 7L, "RUNNING", "job-2");
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(7L);
        group.setStatus("FAILED");
        when(restClient.runningJobIdsByName()).thenReturn(Map.of("ds-task-77", "job-9"));
        when(taskMapper.selectById(77L)).thenReturn(null);
        when(itemMapper.selectById(77L)).thenReturn(lost);
        when(groupMapper.selectById(7L)).thenReturn(group);
        when(itemMapper.selectByGroupId(7L)).thenReturn(List.of(lost, sibling));

        assertEquals(1, sweeper.sweepOnce());

        assertEquals("job-9", lost.getEngineJobId());
        assertEquals("RUNNING", lost.getStatus());
        assertEquals("RUNNING", group.getStatus(), "both items run, so the group is no longer failed");
        verify(groupMapper).updateById(group);
    }

    @Test
    void jobsThatAreNotOursOrHaveNoOwnerRowAreIgnored() {
        when(restClient.runningJobIdsByName()).thenReturn(Map.of(
            "some-other-teams-job", "job-1",
            "ds-task-notanumber", "job-2",
            "ds-task-999", "job-3"));
        when(taskMapper.selectById(999L)).thenReturn(null);
        when(itemMapper.selectById(999L)).thenReturn(null);

        assertEquals(0, sweeper.sweepOnce());
        verify(taskMapper, never()).updateById(any(SyncTask.class));
        verify(itemMapper, never()).updateById(any(SyncTaskGroupItem.class));
    }

    @Test
    void anUnreachableEngineIsNotTreatedAsAnEmptyEngine() {
        when(restClient.runningJobIdsByName()).thenReturn(Map.of());

        assertEquals(0, sweeper.sweepOnce());
        verify(taskMapper, never()).selectById(any());
    }

    private static SyncTask task(long id, String status, String engineJobId) {
        SyncTask task = new SyncTask();
        task.setTaskId(id);
        task.setTaskName("t" + id);
        task.setStatus(status);
        task.setEngineJobId(engineJobId);
        return task;
    }

    private static SyncTaskGroupItem item(long id, long groupId, String status, String engineJobId) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(groupId);
        item.setSourceTable("t" + id);
        item.setStatus(status);
        item.setEngineJobId(engineJobId);
        return item;
    }
}
