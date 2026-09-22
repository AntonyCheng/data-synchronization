package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.domain.SyncTaskGroupDdlEvent;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlEventVo;

public interface SyncTaskGroupDdlEventMapper extends BaseMapperPlus<SyncTaskGroupDdlEvent, SyncTaskGroupDdlEventVo> {

    /** The newest unresolved (PENDING_FIX / READY_TO_RESUME) event of a table item, or null. */
    default SyncTaskGroupDdlEvent selectLatestOpen(Long itemId) {
        return selectOne(new LambdaQueryWrapper<SyncTaskGroupDdlEvent>()
            .eq(SyncTaskGroupDdlEvent::getItemId, itemId)
            .in(SyncTaskGroupDdlEvent::getStatus, "PENDING_FIX", "READY_TO_RESUME")
            .orderByDesc(SyncTaskGroupDdlEvent::getDetectedAt)
            .last("limit 1"));
    }
}
