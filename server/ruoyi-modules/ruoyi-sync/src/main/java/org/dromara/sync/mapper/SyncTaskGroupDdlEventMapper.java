package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.domain.SyncTaskGroupDdlEvent;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlEventVo;

import java.time.LocalDateTime;

public interface SyncTaskGroupDdlEventMapper extends BaseMapperPlus<SyncTaskGroupDdlEvent, SyncTaskGroupDdlEventVo> {

    /** The newest unresolved (PENDING_FIX / READY_TO_RESUME) event of a table item, or null. */
    default SyncTaskGroupDdlEvent selectLatestOpen(Long itemId) {
        return selectOne(new LambdaQueryWrapper<SyncTaskGroupDdlEvent>()
            .eq(SyncTaskGroupDdlEvent::getItemId, itemId)
            .in(SyncTaskGroupDdlEvent::getStatus, SyncTaskGroupDdlEvent.STATUS_PENDING_FIX, SyncTaskGroupDdlEvent.STATUS_READY_TO_RESUME)
            .orderByDesc(SyncTaskGroupDdlEvent::getDetectedAt)
            .last("limit 1"));
    }

    /** Closes the open event of a table item (if any), appending how it was resolved. Returns whether one existed. */
    default boolean resolveOpen(Long itemId, String resolutionNote) {
        SyncTaskGroupDdlEvent event = selectLatestOpen(itemId);
        if (event == null) return false;
        event.setStatus(SyncTaskGroupDdlEvent.STATUS_RESOLVED);
        event.setResolvedAt(LocalDateTime.now());
        if (resolutionNote != null) event.setRemediation(event.getRemediation() + " " + resolutionNote);
        updateById(event);
        return true;
    }

    /**
     * Every event of a group. Events hang off item ids, and items are re-created on each save and
     * deleted with the group - without this an unresolved event outlived its table for good.
     */
    default int deleteByGroupId(Long groupId) {
        return delete(new LambdaQueryWrapper<SyncTaskGroupDdlEvent>().eq(SyncTaskGroupDdlEvent::getGroupId, groupId));
    }
}
