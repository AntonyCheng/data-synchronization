package org.dromara.sync.service;

import org.dromara.common.core.domain.PageResult;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.domain.bo.SyncTaskBo;
import org.dromara.sync.domain.vo.SyncTaskValidationResult;
import org.dromara.sync.domain.vo.SyncTaskVo;

/**
 * Synchronization task service.
 */
public interface ISyncTaskService {

    PageResult<SyncTaskVo> queryPageList(SyncTaskBo bo, PageQuery pageQuery);

    SyncTaskVo queryById(Long taskId);

    Boolean insertByBo(SyncTaskBo bo);

    Boolean updateByBo(SyncTaskBo bo);

    Boolean deleteById(Long taskId);

    SyncTaskValidationResult validate(Long taskId);
}
