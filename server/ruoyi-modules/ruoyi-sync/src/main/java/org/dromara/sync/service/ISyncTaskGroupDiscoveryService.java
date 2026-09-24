package org.dromara.sync.service;

import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;

/**
 * New-table discovery for whole-database task groups: the platform, not the operator, decides
 * which tables such a group syncs, so every table of its source database that has no table item
 * yet gets one - and, on a live group, its own engine job right away.
 */
public interface ISyncTaskGroupDiscoveryService {

    /** Discover tables of a database-scope group and isolate new table failures. */
    SyncTaskGroupOperationResult discover(Long groupId);
}
