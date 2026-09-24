package org.dromara.sync.service;

import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;

/** Read-only source/target row-count reconciliation of a task group, table by table. */
public interface ISyncTaskGroupDataCheckService {

    /** Compares every table item and keeps each outcome in the item's {@code last_check_*} columns. */
    SyncTaskGroupDataCheckResult checkData(Long groupId);
}
