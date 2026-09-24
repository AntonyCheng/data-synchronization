package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.SyncTaskGroupItemVo;

import java.time.LocalDateTime;
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

    /**
     * Forgets the item's engine job (and its checkpoint) after that job was deliberately
     * destroyed. {@code updateById} skips null fields, so this needs an explicit update.
     */
    default int detachEngineJob(Long itemId) {
        return update(null, new LambdaUpdateWrapper<SyncTaskGroupItem>()
            .eq(SyncTaskGroupItem::getItemId, itemId)
            .set(SyncTaskGroupItem::getEngineJobId, null)
            .set(SyncTaskGroupItem::getEngineConfigHash, null)
            .set(SyncTaskGroupItem::getLastCheckpointId, null)
            .set(SyncTaskGroupItem::getLastCheckpointTime, null)
            .set(SyncTaskGroupItem::getLastCheckpointStatus, null));
    }

    default int deleteByGroupId(Long groupId) {
        return delete(new LambdaQueryWrapper<SyncTaskGroupItem>().eq(SyncTaskGroupItem::getGroupId, groupId));
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
        return update(null, new LambdaUpdateWrapper<SyncTaskGroupItem>()
            .eq(SyncTaskGroupItem::getItemId, id)
            .set(SyncTaskGroupItem::getLastCheckSourceRows, sourceRows)
            .set(SyncTaskGroupItem::getLastCheckTargetRows, targetRows)
            .set(SyncTaskGroupItem::getLastCheckDifference, difference)
            .set(SyncTaskGroupItem::getLastCheckMatched, matched)
            .set(SyncTaskGroupItem::getLastCheckTime, checkedAt)
            .set(SyncTaskGroupItem::getLastCheckMessage, message));
    }
}
