package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.vo.SyncTaskVo;

import java.time.LocalDateTime;
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

    /** Tasks with a live engine job (RUNNING / PAUSING) that use the data source on either side. */
    default long countActiveByDataSource(Long sourceId) {
        return selectCount(new LambdaQueryWrapper<SyncTask>()
            .in(SyncTask::getStatus, SyncStatus.RUNNING, SyncStatus.PAUSING)
            .and(w -> w.eq(SyncTask::getSourceId, sourceId).or().eq(SyncTask::getTargetId, sourceId)));
    }

    /** Rows the alert notifier must look at: in an alertable state, or still carrying an open alert marker. */
    default List<SyncTask> selectAlertCandidates() {
        return selectList(new LambdaQueryWrapper<SyncTask>()
            .in(SyncTask::getStatus, SyncStatus.FAILED, SyncStatus.REINITIALIZE_REQUIRED)
            .or(w -> w.isNotNull(SyncTask::getAlertedStatus).ne(SyncTask::getAlertedStatus, "")));
    }

    default int updateAlertedStatus(Long taskId, String alertedStatus) {
        return update(null, new LambdaUpdateWrapper<SyncTask>()
            .eq(SyncTask::getTaskId, taskId)
            .set(SyncTask::getAlertedStatus, alertedStatus));
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

    /**
     * Writes the result of a data consistency check - only the {@code last_check_*} columns, and
     * explicitly (a failed check clears the previous verdict: {@code updateById} would skip the
     * null and keep showing the old "matched"). A check can run for minutes, and the row it read
     * at the start is stale by then: a full-row {@code updateById} wrote back the status, error and
     * checkpoint the status refresh had replaced in the meantime (a FAILED table reverting to RUNNING).
     */
    default int recordCheck(Long id, Long sourceRows, Long targetRows, Long difference, String matched,
                            LocalDateTime checkedAt, String message) {
        return update(null, new LambdaUpdateWrapper<SyncTask>()
            .eq(SyncTask::getTaskId, id)
            .set(SyncTask::getLastCheckSourceRows, sourceRows)
            .set(SyncTask::getLastCheckTargetRows, targetRows)
            .set(SyncTask::getLastCheckDifference, difference)
            .set(SyncTask::getLastCheckMatched, matched)
            .set(SyncTask::getLastCheckTime, checkedAt)
            .set(SyncTask::getLastCheckMessage, message));
    }
}
