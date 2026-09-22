package org.dromara.sync.service;

import org.dromara.common.core.domain.PageResult;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.domain.bo.SyncTaskGroupBo;
import org.dromara.sync.domain.vo.SyncTaskGroupConfigPreview;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupValidationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;

/** Multi-table / whole-database task group service. */
public interface ISyncTaskGroupService {

    PageResult<SyncTaskGroupVo> queryPageList(SyncTaskGroupBo bo, PageQuery pageQuery);

    SyncTaskGroupVo queryById(Long groupId);

    Boolean insertByBo(SyncTaskGroupBo bo);

    Boolean updateByBo(SyncTaskGroupBo bo);

    Boolean deleteById(Long groupId);

    SyncTaskGroupValidationResult validate(Long groupId);

    SyncTaskGroupConfigPreview previewConfig(Long groupId);

    SyncTaskGroupOperationResult start(Long groupId);

    /** Discover tables of a database-scope group and isolate new table failures. */
    SyncTaskGroupOperationResult discover(Long groupId);

    SyncTaskGroupDataCheckResult checkData(Long groupId);

    /**
     * Resubmit one table item from its own savepoint (e.g. after a DDL isolation has been
     * resolved). Refuses when the regenerated engine config no longer matches the fingerprint
     * the savepoint was taken under.
     */
    SyncTaskGroupOperationResult resumeItem(Long groupId, Long itemId);

    /**
     * Discard one table item's engine job and recovery state and rebuild it from a fresh
     * snapshot, leaving the other tables untouched. The way out of an isolated (FAILED /
     * DDL_BLOCKED) table whose savepoint can no longer be reused - e.g. after a source
     * column was added. A selection that covered every column at start follows the
     * table and picks the new column up; an explicit partial selection is kept.
     */
    SyncTaskGroupOperationResult reinitializeItem(Long groupId, Long itemId);

    SyncTaskGroupStatus refreshStatus(Long groupId);

    SyncTaskGroupOperationResult pause(Long groupId);

    SyncTaskGroupOperationResult resume(Long groupId);

    SyncTaskGroupOperationResult stop(Long groupId);
}
