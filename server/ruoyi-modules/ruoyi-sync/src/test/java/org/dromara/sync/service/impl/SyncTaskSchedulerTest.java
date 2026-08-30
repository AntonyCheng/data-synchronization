package org.dromara.sync.service.impl;

import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Tag("dev")
class SyncTaskSchedulerTest {

    @Test
    void onceScheduleAdvancesBeforeStartingAndRunsOnlyOnce() throws Exception {
        SyncTaskMapper mapper = mock(SyncTaskMapper.class);
        ISeaTunnelJobService jobs = mock(ISeaTunnelJobService.class);
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        SyncTask task = task("ONCE", null, "DRAFT", LocalDateTime.now().minusSeconds(1));
        when(mapper.selectById(task.getTaskId())).thenReturn(task);
        when(redisson.getLock("sync:task:start:" + task.getTaskId())).thenReturn(lock);
        when(lock.tryLock(0, 30, java.util.concurrent.TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        trigger(new SyncTaskScheduler(mapper, jobs, redisson), task);

        assertEquals(null, task.getNextRunTime());
        verify(jobs).start(task.getTaskId());
        verify(mapper, atLeastOnce()).updateById(task);
    }

    @Test
    void runningTaskIsSkippedAndCronMovesToNextOccurrence() throws Exception {
        SyncTaskMapper mapper = mock(SyncTaskMapper.class);
        ISeaTunnelJobService jobs = mock(ISeaTunnelJobService.class);
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        SyncTask task = task("CRON", "*/5 * * * * *", "RUNNING", LocalDateTime.now().minusSeconds(1));
        when(mapper.selectById(task.getTaskId())).thenReturn(task);
        when(redisson.getLock("sync:task:start:" + task.getTaskId())).thenReturn(lock);
        when(lock.tryLock(0, 30, java.util.concurrent.TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);

        trigger(new SyncTaskScheduler(mapper, jobs, redisson), task);

        org.junit.jupiter.api.Assertions.assertNotNull(task.getNextRunTime());
        org.junit.jupiter.api.Assertions.assertTrue(task.getLastSkipReason().contains("仍未结束"));
        verifyNoInteractions(jobs);
    }

    private static SyncTask task(String mode, String cron, String status, LocalDateTime due) {
        SyncTask task = new SyncTask();
        task.setTaskId(42L);
        task.setScheduleMode(mode);
        task.setCronExpression(cron);
        task.setStatus(status);
        task.setNextRunTime(due);
        return task;
    }

    private static void trigger(SyncTaskScheduler scheduler, SyncTask task) throws Exception {
        var method = SyncTaskScheduler.class.getDeclaredMethod("trigger", SyncTask.class, LocalDateTime.class);
        method.setAccessible(true);
        method.invoke(scheduler, task, LocalDateTime.now());
    }
}
