package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.SyncTaskGroupItemVo;

import java.util.Collection;
import java.util.List;

public interface SyncTaskGroupItemMapper extends BaseMapperPlus<SyncTaskGroupItem, SyncTaskGroupItemVo> {

    /** All table items of a group in creation order. */
    default List<SyncTaskGroupItem> selectByGroupId(Long groupId) {
        return selectList(new LambdaQueryWrapper<SyncTaskGroupItem>()
            .eq(SyncTaskGroupItem::getGroupId, groupId)
            .orderByAsc(SyncTaskGroupItem::getItemId));
    }

    /** The item only if it belongs to the group; null otherwise. */
    default SyncTaskGroupItem selectOneOfGroup(Long groupId, Long itemId) {
        SyncTaskGroupItem item = selectById(itemId);
        return item != null && groupId.equals(item.getGroupId()) ? item : null;
    }

    /** All items of the given groups, ordered so each group's tables keep a stable order. */
    default List<SyncTaskGroupItem> selectByGroupIds(Collection<Long> groupIds) {
        if (groupIds == null || groupIds.isEmpty()) return List.of();
        return selectList(new LambdaQueryWrapper<SyncTaskGroupItem>()
            .in(SyncTaskGroupItem::getGroupId, groupIds)
            .orderByAsc(SyncTaskGroupItem::getItemId));
    }

    /** Items the alert notifier must look at: isolated / DDL-blocked, or still carrying an open alert marker. */
    default List<SyncTaskGroupItem> selectAlertCandidates() {
        return selectList(new LambdaQueryWrapper<SyncTaskGroupItem>()
            .in(SyncTaskGroupItem::getStatus, SyncStatus.FAILED, SyncStatus.DDL_BLOCKED)
            .or(w -> w.isNotNull(SyncTaskGroupItem::getAlertedStatus).ne(SyncTaskGroupItem::getAlertedStatus, "")));
    }

    default int updateAlertedStatus(Long itemId, String alertedStatus) {
        return update(null, new LambdaUpdateWrapper<SyncTaskGroupItem>()
            .eq(SyncTaskGroupItem::getItemId, itemId)
            .set(SyncTaskGroupItem::getAlertedStatus, alertedStatus));
    }

    /** Nulls the checkpoint columns of one item (see SyncTaskMapper#clearCheckpoint). */
    default int clearCheckpoint(Long itemId) {
        return update(null, new LambdaUpdateWrapper<SyncTaskGroupItem>()
            .eq(SyncTaskGroupItem::getItemId, itemId)
            .set(SyncTaskGroupItem::getLastCheckpointId, null)
            .set(SyncTaskGroupItem::getLastCheckpointTime, null)
            .set(SyncTaskGroupItem::getLastCheckpointStatus, null));
    }

    default int deleteByGroupId(Long groupId) {
        return delete(new LambdaQueryWrapper<SyncTaskGroupItem>().eq(SyncTaskGroupItem::getGroupId, groupId));
    }
}
