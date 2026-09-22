package org.dromara.sync.support;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Per-task and per-group mutation locks (Redisson). Every operation that reads a task or
 * group row, talks to the engine and writes the row back runs under the matching lock, so
 * a user action and a background reconcile pass (RuoYi's {@code schedule-pool} is
 * multi-threaded) never interleave their full-row updates.
 *
 * <p>Interactive callers wait up to {@link #WAIT_SECONDS}; background passes should test
 * {@link #isTaskBusy} / {@link #isGroupBusy} first and skip a busy row - the next cycle
 * picks it up. Leases are watchdog-renewed for as long as the holding thread works.
 */
@Component
@RequiredArgsConstructor
public class SyncLocks {

    /** Shared with {@code SyncTaskScheduler}, which takes it around a scheduled start. */
    public static final String TASK_LOCK_PREFIX = "sync:task:start:";
    public static final String GROUP_LOCK_PREFIX = "sync:group:lock:";
    private static final long WAIT_SECONDS = 10;

    private final RedissonClient redissonClient;

    public <T> T withTaskLock(Long taskId, Supplier<T> action) {
        return locked(TASK_LOCK_PREFIX + taskId, "任务正在执行其他操作，请稍后重试", action);
    }

    public <T> T withGroupLock(Long groupId, Supplier<T> action) {
        return locked(GROUP_LOCK_PREFIX + groupId, "任务组正在执行其他操作，请稍后重试", action);
    }

    public boolean isTaskBusy(Long taskId) {
        return redissonClient.getLock(TASK_LOCK_PREFIX + taskId).isLocked();
    }

    public boolean isGroupBusy(Long groupId) {
        return redissonClient.getLock(GROUP_LOCK_PREFIX + groupId).isLocked();
    }

    private <T> T locked(String key, String busyMessage, Supplier<T> action) {
        RLock lock = redissonClient.getLock(key);
        boolean acquired = false;
        try {
            acquired = lock.tryLock(WAIT_SECONDS, -1, TimeUnit.SECONDS);
            if (!acquired) throw new ServiceException(busyMessage);
            return action.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ServiceException("等待锁被中断");
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) lock.unlock();
        }
    }
}
