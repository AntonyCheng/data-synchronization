package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Triggers persisted task schedules with a distributed per-task lock. */
@Component
@RequiredArgsConstructor
public class SyncTaskScheduler {

    private final SyncTaskMapper taskMapper;
    private final ISeaTunnelJobService jobService;
    private final RedissonClient redissonClient;

    @Scheduled(fixedDelayString = "${sync.schedule.interval-ms:15000}", initialDelayString = "${sync.schedule.initial-delay-ms:10000}")
    public void triggerDueTasks() {
        LocalDateTime now = LocalDateTime.now();
        List<SyncTask> tasks = taskMapper.selectList(new LambdaQueryWrapper<SyncTask>()
            .in(SyncTask::getScheduleMode, "ONCE", "CRON")
            .le(SyncTask::getNextRunTime, now)
            .in(SyncTask::getStatus, "DRAFT", "STOPPED", "FINISHED", "RUNNING", "PAUSING"));
        for (SyncTask task : tasks) trigger(task, now);
    }

    private void trigger(SyncTask task, LocalDateTime now) {
        RLock lock = redissonClient.getLock("sync:task:start:" + task.getTaskId());
        boolean acquired = false;
        try {
            acquired = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!acquired) return;
            SyncTask current = taskMapper.selectById(task.getTaskId());
            if (current == null || current.getNextRunTime() == null || current.getNextRunTime().isAfter(now)) return;
            if (List.of("RUNNING", "PAUSING").contains(current.getStatus())) {
                current.setLastSkipReason("上一次运行实例仍未结束，本次调度已跳过");
                advance(current, now);
                taskMapper.updateById(current);
                return;
            }
            current.setLastTriggerTime(now);
            current.setLastSkipReason("");
            advance(current, now);
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

    private static void advance(SyncTask task, LocalDateTime now) {
        if ("CRON".equalsIgnoreCase(task.getScheduleMode())) {
            try {
                task.setNextRunTime(CronExpression.parse(task.getCronExpression()).next(now));
            } catch (IllegalArgumentException ex) {
                task.setNextRunTime(null);
                task.setLastSkipReason("Cron 表达式无效，已暂停调度：" + ex.getMessage());
            }
        } else {
            task.setNextRunTime(null);
        }
    }
}
