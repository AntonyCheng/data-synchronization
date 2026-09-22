package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.domain.SyncMetricsSample;
import org.dromara.sync.domain.vo.SyncMetricsSampleVo;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public interface SyncMetricsSampleMapper extends BaseMapperPlus<SyncMetricsSample, SyncMetricsSampleVo> {

    /** Samples of one owner since {@code since}, oldest first, capped at {@code limit} rows. */
    default List<SyncMetricsSample> selectSeries(String ownerType, Long ownerId, LocalDateTime since, int limit) {
        return selectList(new LambdaQueryWrapper<SyncMetricsSample>()
            .eq(SyncMetricsSample::getOwnerType, ownerType)
            .eq(SyncMetricsSample::getOwnerId, ownerId)
            .ge(SyncMetricsSample::getSampledAt, since)
            .orderByAsc(SyncMetricsSample::getSampledAt)
            .last("limit " + limit));
    }

    /** The newest sample of each owner in {@code ownerIds} (owners never sampled are absent). */
    default List<SyncMetricsSample> selectLatest(String ownerType, Collection<Long> ownerIds) {
        if (ownerIds == null || ownerIds.isEmpty()) return List.of();
        // Owner ids are Longs, so joining them into the sub-select is injection-safe.
        String idList = ownerIds.stream().map(String::valueOf).collect(Collectors.joining(","));
        return selectList(new LambdaQueryWrapper<SyncMetricsSample>()
            .eq(SyncMetricsSample::getOwnerType, ownerType)
            .in(SyncMetricsSample::getOwnerId, ownerIds)
            .apply("(owner_id, sampled_at) in (select owner_id, max(sampled_at) from ds_sync_metrics_sample"
                + " where owner_type = {0} and owner_id in (" + idList + ") group by owner_id)", ownerType));
    }

    default int deleteBefore(LocalDateTime cutoff) {
        return delete(new LambdaQueryWrapper<SyncMetricsSample>().lt(SyncMetricsSample::getSampledAt, cutoff));
    }

    default int deleteByOwner(String ownerType, Long ownerId) {
        return delete(new LambdaQueryWrapper<SyncMetricsSample>()
            .eq(SyncMetricsSample::getOwnerType, ownerType)
            .eq(SyncMetricsSample::getOwnerId, ownerId));
    }

    default int deleteByGroupId(Long groupId) {
        return delete(new LambdaQueryWrapper<SyncMetricsSample>().eq(SyncMetricsSample::getGroupId, groupId));
    }
}
