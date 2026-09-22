package org.dromara.sync.support;

import org.dromara.sync.domain.SyncTask;
import org.springframework.scheduling.support.CronExpression;

import java.time.LocalDateTime;

/**
 * Bookkeeping for a task's ONCE / CRON schedule ({@code next_run_time}, {@code last_skip_reason}).
 * A null {@code nextRunTime} means the schedule is parked: a manual stop, a parked failure
 * or a fired ONCE trigger all land there, and a successful start / resume / reinitialize
 * of a CRON task re-arms it.
 */
public final class SyncSchedules {

    public static final String MODE_ONCE = "ONCE";
    public static final String MODE_CRON = "CRON";

    private SyncSchedules() {
    }

    public static boolean isCron(SyncTask task) {
        return MODE_CRON.equalsIgnoreCase(task.getScheduleMode());
    }

    /** Moves the schedule past {@code now}: the next cron occurrence, or parked for ONCE / invalid cron. */
    public static void advance(SyncTask task, LocalDateTime now) {
        if (!isCron(task)) {
            task.setNextRunTime(null);
            return;
        }
        try {
            task.setNextRunTime(CronExpression.parse(task.getCronExpression()).next(now));
        } catch (IllegalArgumentException ex) {
            task.setNextRunTime(null);
            task.setLastSkipReason("Cron 表达式无效，已暂停调度：" + ex.getMessage());
        }
    }

    /** Re-arms a parked CRON schedule after the task was brought back to RUNNING by an operator; the parked reason goes with it. */
    public static void rearm(SyncTask task, LocalDateTime now) {
        if (!isCron(task) || task.getNextRunTime() != null) return;
        task.setLastSkipReason("");
        advance(task, now);
    }
}
