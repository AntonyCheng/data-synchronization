package org.dromara.sync.service;

import org.dromara.common.core.domain.PageResult;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.domain.bo.SyncTaskGroupBo;
import org.dromara.sync.domain.vo.SyncTaskGroupConfigPreview;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupValidationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;

public interface ISyncTaskGroupService {

    PageResult<SyncTaskGroupVo> queryPageList(String groupName, String status, PageQuery pageQuery);

    SyncTaskGroupVo queryById(Long groupId);

    Boolean insertByBo(SyncTaskGroupBo bo);

    Boolean updateByBo(SyncTaskGroupBo bo);

    Boolean deleteById(Long groupId);

    SyncTaskGroupValidationResult validate(Long groupId);

    SyncTaskGroupConfigPreview previewConfig(Long groupId);

    SyncTaskGroupOperationResult start(Long groupId);

    /** Discover tables of a database-scope group and isolate new table failures. */
    SyncTaskGroupOperationResult discover(Long groupId);

    SyncTaskGroupDdlCheckResult checkDdl(Long groupId);

    SyncTaskGroupDataCheckResult checkData(Long groupId);

    SyncTaskGroupOperationResult resumeDdlItem(Long groupId, Long itemId);

    SyncTaskGroupStatus refreshStatus(Long groupId);

    SyncTaskGroupOperationResult pause(Long groupId);

    SyncTaskGroupOperationResult resume(Long groupId);

    SyncTaskGroupOperationResult stop(Long groupId);
}
