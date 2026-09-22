package org.dromara.sync.constant;

/**
 * Platform-side lifecycle states persisted in {@code ds_sync_task.status},
 * {@code ds_sync_task_group.status} and {@code ds_sync_task_group_item.status}.
 * See docs/task-state-machine.md. Engine (SeaTunnel) states are a different
 * vocabulary and are mapped onto these by {@code EngineJobStates}.
 */
public final class SyncStatus {

    /** Configured, never submitted. Editable and startable. */
    public static final String DRAFT = "DRAFT";
    public static final String RUNNING = "RUNNING";
    /** stop-job with savepoint requested; waiting for the engine to confirm. */
    public static final String PAUSING = "PAUSING";
    public static final String PAUSED = "PAUSED";
    /** Stopped without savepoint. Editable and startable. */
    public static final String STOPPED = "STOPPED";
    public static final String FAILED = "FAILED";
    /** Checkpoint/binlog position is unrecoverable; plain start/resume are refused. */
    public static final String REINITIALIZE_REQUIRED = "REINITIALIZE_REQUIRED";
    /** A BATCH (FULL) job completed on the engine. */
    public static final String FINISHED = "FINISHED";

    /** Group only: some table items run while others are isolated (FAILED / DDL_BLOCKED). */
    public static final String DEGRADED = "DEGRADED";
    /** Group item only: saved but not yet submitted. */
    public static final String PENDING = "PENDING";
    /** Group item only: paused with savepoint because its source schema drifted. */
    public static final String DDL_BLOCKED = "DDL_BLOCKED";

    private SyncStatus() {
    }

    /** States in which the platform must keep reconciling against the engine. */
    public static boolean isActive(String status) {
        return RUNNING.equals(status) || PAUSING.equals(status);
    }
}
