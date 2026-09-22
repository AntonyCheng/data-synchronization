package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
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

    /** Nulls the checkpoint columns; a plain updateById would drop the nulls under the global not_null strategy. */
    default int clearCheckpoint(Long taskId) {
        return update(null, new LambdaUpdateWrapper<SyncTask>()
            .eq(SyncTask::getTaskId, taskId)
            .set(SyncTask::getLastCheckpointId, null)
            .set(SyncTask::getLastCheckpointTime, null)
            .set(SyncTask::getLastCheckpointStatus, null));
    }

    /** Clears the FULL/OVERWRITE staging marker once the stage table has been swapped in. */
    default int clearOverwriteStage(Long taskId) {
        return update(null, new LambdaUpdateWrapper<SyncTask>()
            .eq(SyncTask::getTaskId, taskId)
            .set(SyncTask::getOverwriteStageTable, null));
    }
}
