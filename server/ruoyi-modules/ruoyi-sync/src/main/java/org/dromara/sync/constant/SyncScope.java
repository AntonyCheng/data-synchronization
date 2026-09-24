package org.dromara.sync.constant;

/** Task group scopes persisted in {@code ds_sync_task_group.sync_scope}. See docs/multi-table.md. */
public final class SyncScope {

    /** Tables the operator picked. Starts all-or-nothing: a table that fails compensates the others. */
    public static final String MULTI_TABLE = "MULTI_TABLE";
    /**
     * Every table of one source database. The platform picks the tables (new ones are discovered),
     * owns their Kafka topics, and isolates a failing table instead of failing the group.
     */
    public static final String DATABASE = "DATABASE";

    private SyncScope() {
    }

    public static boolean isDatabase(String scope) {
        return DATABASE.equals(scope);
    }
}
