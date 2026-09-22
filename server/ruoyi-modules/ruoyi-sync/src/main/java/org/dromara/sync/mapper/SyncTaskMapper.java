package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.vo.SyncTaskVo;

import java.util.List;

/**
 * Synchronization task mapper.
 */
public interface SyncTaskMapper extends BaseMapperPlus<SyncTask, SyncTaskVo> {

    /** Tasks whose engine state the platform must keep reconciling (RUNNING / PAUSING). */
    default List<SyncTask> selectActive() {
        return selectList(new LambdaQueryWrapper<SyncTask>()
            .in(SyncTask::getStatus, SyncStatus.RUNNING, SyncStatus.PAUSING));
    }

    /** Tasks that use the data source on either side. */
    default long countByDataSource(Long sourceId) {
        return selectCount(new LambdaQueryWrapper<SyncTask>()
            .and(w -> w.eq(SyncTask::getSourceId, sourceId).or().eq(SyncTask::getTargetId, sourceId)));
    }
}
