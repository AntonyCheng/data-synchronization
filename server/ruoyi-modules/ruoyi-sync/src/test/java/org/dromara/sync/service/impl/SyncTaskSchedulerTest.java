package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.dromara.sync.support.SyncSchedules;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Tag("dev")
class SyncTaskSchedulerTest {

    private final SyncTaskMapper mapper = mock(SyncTaskMapper.class);
    private final ISeaTunnelJobService jobs = mock(ISeaTunnelJobService.class);
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock lock = mock(RLock.class);
    private final SyncTaskScheduler scheduler = new SyncTaskScheduler(mapper, jobs, redisson);
    private final LocalDateTime now = LocalDateTime.now();

    @BeforeEach
    void lockIsFree() throws Exception {
        when(redisson.getLock("sync:task:start:42")).thenReturn(lock);
        when(lock.tryLock(0, 30, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
    }

    @Test
    void onceScheduleAdvancesBeforeStartingAndRunsOnlyOnce() {
        SyncTask task = due("ONCE", null, "DRAFT");

        scheduler.trigger(task, now);

        assertNull(task.getNextRunTime());
        assertEquals(now, task.getLastTriggerTime());
        verify(jobs).start(42L);
        verify(mapper, atLeastOnce()).updateById(task);
    }

    @Test
    void runningTaskIsSkippedAndCronMovesToNextOccurrence() {
        SyncTask task = due("CRON", "*/5 * * * * *", "RUNNING");

        scheduler.trigger(task, now);

        assertNotNull(task.getNextRunTime());
        assertTrue(task.getNextRunTime().isAfter(now));
        assertTrue(task.getLastSkipReason().contains("仍未结束"));
        verifyNoInteractions(jobs);
    }

    @Test
    void aFailedCronTaskParksItsScheduleWithTheReasonInsteadOfGoingStale() {
        for (String status : new String[]{"FAILED", "REINITIALIZE_REQUIRED", "PAUSED"}) {
            SyncTask task = due("CRON", "*/5 * * * * *", status);

            scheduler.trigger(task, now);

            assertNull(task.getNextRunTime(), status);
            assertTrue(task.getLastSkipReason().contains("调度已挂起"), status);
            verify(mapper).updateById(task);
        }
        verifyNoInteractions(jobs);
    }

    @Test
    void aStartThatIsRefusedKeepsTheNextOccurrenceAndRecordsWhy() {
        SyncTask task = due("CRON", "*/5 * * * * *", "STOPPED");
        doThrow(new ServiceException("启动前校验未通过：目标不可达")).when(jobs).start(42L);

        scheduler.trigger(task, now);

        assertTrue(task.getNextRunTime().isAfter(now));
        assertTrue(task.getLastSkipReason().contains("目标不可达"));
    }

    @Test
    void rearmOnlyRevivesAParkedCronSchedule() {
        SyncTask parked = due("CRON", "0 0 * * * *", "STOPPED");
        parked.setNextRunTime(null);
        parked.setLastSkipReason("任务处于失败状态，调度已挂起");
        SyncSchedules.rearm(parked, now);
        assertNotNull(parked.getNextRunTime());
        assertEquals("", parked.getLastSkipReason());

        SyncTask armed = due("CRON", "0 0 * * * *", "STOPPED");
        LocalDateTime already = armed.getNextRunTime();
        SyncSchedules.rearm(armed, now);
        assertEquals(already, armed.getNextRunTime());

        SyncTask once = due("ONCE", null, "STOPPED");
        once.setNextRunTime(null);
        SyncSchedules.rearm(once, now);
        assertNull(once.getNextRunTime());
    }

    private SyncTask due(String mode, String cron, String status) {
        SyncTask task = new SyncTask();
        task.setTaskId(42L);
        task.setScheduleMode(mode);
        task.setCronExpression(cron);
        task.setStatus(status);
        task.setNextRunTime(now.minusSeconds(1));
        when(mapper.selectById(42L)).thenReturn(task);
        return task;
    }
}
