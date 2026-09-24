package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.constant.SyncScope;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;

import java.util.List;

public interface SyncTaskGroupMapper extends BaseMapperPlus<SyncTaskGroup, SyncTaskGroupVo> {

    /**
     * Groups whose engine state the platform must keep reconciling (RUNNING / PAUSING / DEGRADED).
     * DEGRADED belongs here: its healthy tables are live jobs, and leaving it out meant a table
     * that failed or finished inside a degraded group was never noticed.
     */
    default List<SyncTaskGroup> selectActive() {
        return selectList(new LambdaQueryWrapper<SyncTaskGroup>()
            .in(SyncTaskGroup::getStatus, SyncStatus.RUNNING, SyncStatus.PAUSING, SyncStatus.DEGRADED));
    }

    /** Groups that use the data source on either side. */
    default long countByDataSource(Long sourceId) {
        return selectCount(new LambdaQueryWrapper<SyncTaskGroup>()
            .and(w -> w.eq(SyncTaskGroup::getSourceId, sourceId).or().eq(SyncTaskGroup::getTargetId, sourceId)));
    }

    /** Groups with live table jobs (RUNNING / PAUSING / DEGRADED) that use the data source on either side. */
    default long countActiveByDataSource(Long sourceId) {
        return selectCount(new LambdaQueryWrapper<SyncTaskGroup>()
            .in(SyncTaskGroup::getStatus, SyncStatus.RUNNING, SyncStatus.PAUSING, SyncStatus.DEGRADED)
            .and(w -> w.eq(SyncTaskGroup::getSourceId, sourceId).or().eq(SyncTaskGroup::getTargetId, sourceId)));
    }

    /** Groups the alert notifier must look at: in an alertable state, or still carrying an open alert marker. */
    default List<SyncTaskGroup> selectAlertCandidates() {
        return selectList(new LambdaQueryWrapper<SyncTaskGroup>()
            .in(SyncTaskGroup::getStatus, SyncStatus.FAILED, SyncStatus.REINITIALIZE_REQUIRED, SyncStatus.DEGRADED)
            .or(w -> w.isNotNull(SyncTaskGroup::getAlertedStatus).ne(SyncTaskGroup::getAlertedStatus, "")));
    }

    default int updateAlertedStatus(Long groupId, String alertedStatus) {
        return update(null, new LambdaUpdateWrapper<SyncTaskGroup>()
            .eq(SyncTaskGroup::getGroupId, groupId)
            .set(SyncTaskGroup::getAlertedStatus, alertedStatus));
    }

    /** Groups that still have live table jobs (RUNNING / DEGRADED) and so need DDL checks. */
    default List<SyncTaskGroup> selectLive() {
        return selectList(new LambdaQueryWrapper<SyncTaskGroup>()
            .in(SyncTaskGroup::getStatus, SyncStatus.RUNNING, SyncStatus.DEGRADED));
    }

    /** Live whole-database groups that opted into automatic new-table discovery. */
    default List<SyncTaskGroup> selectLiveAutoDiscoverDatabaseGroups() {
        return selectList(new LambdaQueryWrapper<SyncTaskGroup>()
            .eq(SyncTaskGroup::getSyncScope, SyncScope.DATABASE)
            .eq(SyncTaskGroup::getAutoDiscover, "1")
            .in(SyncTaskGroup::getStatus, SyncStatus.RUNNING, SyncStatus.DEGRADED));
    }
}
