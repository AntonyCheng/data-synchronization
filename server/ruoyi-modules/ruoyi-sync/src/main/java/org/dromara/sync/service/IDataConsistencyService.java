package org.dromara.sync.service;

import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.bo.SyncTaskDataCheckRequest;

/** Compares source and target row counts for a synchronization task. */
public interface IDataConsistencyService {

    SyncTaskDataCheckResult check(Long taskId);

    SyncTaskDataCheckResult check(Long taskId, SyncTaskDataCheckRequest request);

    /**
     * Runs the same bounded read-only row-count comparison for an explicitly supplied table mapping.
     * The caller owns persistence of the returned result.
     */
    SyncTaskDataCheckResult check(DataSource source, DataSource target, String sourceDatabase, String sourceTable,
                                  String targetSchema, String targetTable);
}
