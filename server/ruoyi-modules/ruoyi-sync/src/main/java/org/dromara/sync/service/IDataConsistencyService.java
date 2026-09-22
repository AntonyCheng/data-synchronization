package org.dromara.sync.service;

import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.bo.SyncTaskDataCheckRequest;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;

/** Read-only source/target row-count reconciliation. */
public interface IDataConsistencyService {

    /** Checks one task and persists the outcome into its {@code last_check_*} columns. */
    SyncTaskDataCheckResult check(Long taskId, SyncTaskDataCheckRequest request);

    /**
     * Runs the same bounded read-only row-count comparison for an explicitly supplied table mapping.
     * The caller owns persistence of the returned result.
     */
    SyncTaskDataCheckResult check(DataSource source, DataSource target, String sourceDatabase, String sourceTable,
                                  String targetSchema, String targetTable);
}
