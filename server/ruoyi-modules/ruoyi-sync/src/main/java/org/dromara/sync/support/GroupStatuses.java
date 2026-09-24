package org.dromara.sync.support;

import org.dromara.sync.constant.SyncStatus;

import java.util.List;

/** Derives a task group's status from the statuses of its table items. */
public final class GroupStatuses {

    private GroupStatuses() {
    }

    /** FAILED or DDL_BLOCKED: the table is parked and excluded from a whole-database start. */
    public static boolean isIsolated(String itemStatus) {
        return SyncStatus.FAILED.equals(itemStatus) || SyncStatus.DDL_BLOCKED.equals(itemStatus);
    }

    /** RUNNING or DEGRADED: the group's table jobs are live on the engine, so a new table joins them at once. */
    public static boolean isLive(String groupStatus) {
        return SyncStatus.RUNNING.equals(groupStatus) || SyncStatus.DEGRADED.equals(groupStatus);
    }

    /**
     * Any isolated table alongside a running one is DEGRADED; only isolated tables is FAILED;
     * otherwise the most "active" item state wins, and an empty group is DRAFT.
     */
    public static String aggregate(List<String> itemStatuses) {
        boolean hasFailure = itemStatuses.stream().anyMatch(GroupStatuses::isIsolated);
        boolean hasRunning = itemStatuses.stream().anyMatch(SyncStatus.RUNNING::equals);
        if (hasFailure && hasRunning) return SyncStatus.DEGRADED;
        if (hasFailure) return SyncStatus.FAILED;
        if (hasRunning) return SyncStatus.RUNNING;
        if (itemStatuses.stream().anyMatch(SyncStatus.PAUSING::equals)) return SyncStatus.PAUSING;
        if (!itemStatuses.isEmpty() && itemStatuses.stream().allMatch(SyncStatus.PAUSED::equals)) return SyncStatus.PAUSED;
        if (!itemStatuses.isEmpty() && itemStatuses.stream()
            .allMatch(status -> SyncStatus.STOPPED.equals(status) || SyncStatus.FINISHED.equals(status))) {
            return SyncStatus.STOPPED;
        }
        return SyncStatus.DRAFT;
    }
}
