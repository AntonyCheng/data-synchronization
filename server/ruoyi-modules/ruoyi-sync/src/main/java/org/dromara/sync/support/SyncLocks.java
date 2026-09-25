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
 * a user action and a background reconcile pass (the module's scheduler,
 * {@code SyncSchedulingConfig}, runs passes concurrently) never interleave their full-row updates.
 *
 * <p>Interactive callers wait up to {@link #WAIT_SECONDS}; background passes should test
 * {@link #isTaskBusy} / {@link #isGroupBusy} first and skip a busy row - the next cycle
 * picks it up. Leases are watchdog-renewed for as long as the holding thread works.
 *
 * <p>{@link #runIfFree} is for a pass that must not run on two instances at once (the Kafka raw-topic
 * cleanup): it never waits.
 */
@Component
@RequiredArgsConstructor
public class SyncLocks {

    /** Shared with {@code SyncTaskScheduler}, which takes it around a scheduled start. */
    public static final String TASK_LOCK_PREFIX = "sync:task:start:";
    public static final String GROUP_LOCK_PREFIX = "sync:group:lock:";
    /** Held by the one instance running the Kafka raw-topic cleanup pass. */
    public static final String RAW_TOPIC_CLEANUP_LOCK = "sync:kafka:raw-topic-cleanup";
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

    /**
     * For a background pass that one instance at a time should run: runs {@code action} under the
     * lock, or returns {@code false} at once when another instance holds it - no waiting, the
     * next cycle tries again.
     */
    public boolean runIfFree(String key, Runnable action) {
        RLock lock = redissonClient.getLock(key);
        boolean acquired = false;
        try {
            acquired = lock.tryLock(0, -1, TimeUnit.SECONDS);
            if (!acquired) return false;
            action.run();
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) lock.unlock();
        }
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
