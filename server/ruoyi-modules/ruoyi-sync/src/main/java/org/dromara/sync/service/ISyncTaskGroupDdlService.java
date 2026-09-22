package org.dromara.sync.service;

import org.dromara.sync.domain.vo.SyncTaskGroupDdlCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;

/**
 * Source-schema drift detection for task groups. The platform never alters the target
 * schema itself: a changed table is paused with a savepoint, a DDL event records what
 * changed and how to fix it, and an operator resumes the table once the target matches.
 */
public interface ISyncTaskGroupDdlService {

    /** Compare every table with the schema captured at its last successful start. */
    SyncTaskGroupDdlCheckResult checkDdl(Long groupId);

    /** Re-check, require an open READY_TO_RESUME event, then resubmit the table from its savepoint. */
    SyncTaskGroupOperationResult resumeDdlItem(Long groupId, Long itemId);
}
