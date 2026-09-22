package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncSchedules;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Triggers persisted task schedules with a distributed per-task lock. */
@Component
@RequiredArgsConstructor
public class SyncTaskScheduler {

    /**
     * A due trigger cannot start these; the schedule is parked (nextRunTime = null) with the
     * reason on the task, and SyncSchedules.rearm() revives it once an operator brings the
     * task back to RUNNING. Silently leaving nextRunTime in the past looked like a stuck cron.
     */
    private static final Map<String, String> PARKED_REASONS = Map.of(
        SyncStatus.FAILED, "任务处于失败状态，调度已挂起：请先处理失败原因并恢复或重新初始化任务，成功后调度自动继续",
        SyncStatus.REINITIALIZE_REQUIRED, "任务需要重新初始化，调度已挂起：重新初始化成功后调度自动继续",
        SyncStatus.PAUSED, "任务处于暂停状态，调度已挂起：恢复任务后调度自动继续");

    private final SyncTaskMapper taskMapper;
    private final ISeaTunnelJobService jobService;
    private final RedissonClient redissonClient;

    @Scheduled(fixedDelayString = "${sync.schedule.interval-ms:15000}", initialDelayString = "${sync.schedule.initial-delay-ms:10000}")
    public void triggerDueTasks() {
        LocalDateTime now = LocalDateTime.now();
        List<SyncTask> tasks = taskMapper.selectList(new LambdaQueryWrapper<SyncTask>()
            .in(SyncTask::getScheduleMode, SyncSchedules.MODE_ONCE, SyncSchedules.MODE_CRON)
            .le(SyncTask::getNextRunTime, now));
        for (SyncTask task : tasks) trigger(task, now);
    }

    void trigger(SyncTask task, LocalDateTime now) {
        RLock lock = redissonClient.getLock(SyncLocks.TASK_LOCK_PREFIX + task.getTaskId());
        boolean acquired = false;
        try {
            acquired = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!acquired) return;
            SyncTask current = taskMapper.selectById(task.getTaskId());
            if (current == null || current.getNextRunTime() == null || current.getNextRunTime().isAfter(now)) return;
            if (SyncStatus.isActive(current.getStatus())) {
                current.setLastSkipReason("上一次运行实例仍未结束，本次调度已跳过");
                SyncSchedules.advance(current, now);
                taskMapper.updateById(current);
                return;
            }
            String parkedReason = PARKED_REASONS.get(current.getStatus());
            if (parkedReason != null) {
                current.setLastSkipReason(parkedReason);
                current.setNextRunTime(null);
                taskMapper.updateById(current);
                return;
            }
            current.setLastTriggerTime(now);
            current.setLastSkipReason("");
            SyncSchedules.advance(current, now);
            taskMapper.updateById(current);
            jobService.start(current.getTaskId());
        } catch (ServiceException ex) {
            SyncTask failed = taskMapper.selectById(task.getTaskId());
            if (failed != null) {
                failed.setLastSkipReason("调度触发失败：" + ex.getMessage());
                taskMapper.updateById(failed);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) lock.unlock();
        }
    }
}
