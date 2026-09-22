package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.MapstructUtils;
import org.dromara.sync.config.SyncMetricsProperties;
import org.dromara.sync.domain.SyncMetricsSample;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.EngineJobMetrics;
import org.dromara.sync.domain.vo.SyncMetricsSampleVo;
import org.dromara.sync.domain.vo.SyncMetricsSeriesVo;
import org.dromara.sync.mapper.SyncMetricsSampleMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISyncMetricsService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Appends one row per successful engine poll and serves trailing windows of them. */
@Slf4j
@RequiredArgsConstructor
@Service
public class SyncMetricsServiceImpl implements ISyncMetricsService {

    private static final int DEFAULT_WINDOW_MINUTES = 60;
    private static final int MIN_WINDOW_MINUTES = 5;

    private final SyncMetricsSampleMapper sampleMapper;
    private final SyncTaskMapper syncTaskMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SyncMetricsProperties properties;

    @Override
    public void recordTask(SyncTask task, String engineStatus, EngineJobMetrics metrics) {
        record(SyncMetricsSample.OWNER_TASK, task.getTaskId(), null, task.getEngineJobId(), engineStatus, metrics);
    }

    @Override
    public void recordGroupItem(SyncTaskGroupItem item, String engineStatus, EngineJobMetrics metrics) {
        record(SyncMetricsSample.OWNER_GROUP_ITEM, item.getItemId(), item.getGroupId(), item.getEngineJobId(), engineStatus, metrics);
    }

    private void record(String ownerType, Long ownerId, Long groupId, String engineJobId, String engineStatus, EngineJobMetrics metrics) {
        if (ownerId == null || metrics == null) return;
        try {
            SyncMetricsSample sample = new SyncMetricsSample();
            sample.setOwnerType(ownerType);
            sample.setOwnerId(ownerId);
            sample.setGroupId(groupId);
            sample.setEngineJobId(engineJobId);
            sample.setEngineStatus(engineStatus);
            sample.setPhase(metrics.getPhase());
            sample.setSourceReceivedCount(metrics.getSourceReceivedCount());
            sample.setSinkCommittedCount(metrics.getSinkCommittedCount());
            sample.setSourceReceivedBytes(metrics.getSourceReceivedBytes());
            sample.setSinkCommittedBytes(metrics.getSinkCommittedBytes());
            sample.setSourceQps(metrics.getSourceQps());
            sample.setSinkQps(metrics.getSinkQps());
            sample.setBacklogRows(metrics.getBacklogRows());
            sample.setCdcLagSeconds(metrics.getCdcLagSeconds());
            sample.setSampledAt(LocalDateTime.now());
            sampleMapper.insert(sample);
        } catch (RuntimeException ex) {
            // History is best-effort; the reconcile that produced these numbers already succeeded.
            log.warn("metrics sample not recorded: ownerType={} ownerId={} - {}", ownerType, ownerId, ex.getMessage());
        }
    }

    @Override
    public SyncMetricsSeriesVo queryTaskSeries(Long taskId, Integer minutes) {
        if (syncTaskMapper.selectById(taskId) == null) throw new ServiceException("同步任务不存在");
        return series(SyncMetricsSample.OWNER_TASK, taskId, minutes);
    }

    @Override
    public SyncMetricsSeriesVo queryGroupItemSeries(Long groupId, Long itemId, Integer minutes) {
        if (itemMapper.selectOneOfGroup(groupId, itemId) == null) throw new ServiceException("表项不存在或不属于当前任务组");
        return series(SyncMetricsSample.OWNER_GROUP_ITEM, itemId, minutes);
    }

    private SyncMetricsSeriesVo series(String ownerType, Long ownerId, Integer minutes) {
        int window = minutes == null ? DEFAULT_WINDOW_MINUTES
            : Math.max(MIN_WINDOW_MINUTES, Math.min(minutes, properties.getMaxWindowMinutes()));
        SyncMetricsSeriesVo result = new SyncMetricsSeriesVo();
        result.setOwnerType(ownerType);
        result.setOwnerId(ownerId);
        result.setWindowMinutes(window);
        // Fetch a little beyond the cap so thinning can still represent the whole window evenly.
        List<SyncMetricsSample> samples = sampleMapper.selectSeries(ownerType, ownerId,
            LocalDateTime.now().minusMinutes(window), properties.getMaxPoints() * 4);
        result.setSamples(MapstructUtils.convert(thin(samples, properties.getMaxPoints()), SyncMetricsSampleVo.class));
        List<SyncMetricsSample> latest = sampleMapper.selectLatest(ownerType, List.of(ownerId));
        result.setLatest(latest.isEmpty() ? null : MapstructUtils.convert(latest.get(0), SyncMetricsSampleVo.class));
        return result;
    }

    /** Keeps every k-th point (plus the last one) so a long window still fits the chart. */
    static <T> List<T> thin(List<T> points, int maxPoints) {
        if (maxPoints < 2 || points.size() <= maxPoints) return points;
        int stride = (int) Math.ceil(points.size() / (double) maxPoints);
        List<T> kept = new ArrayList<>(maxPoints + 1);
        for (int index = 0; index < points.size(); index += stride) kept.add(points.get(index));
        T last = points.get(points.size() - 1);
        if (kept.get(kept.size() - 1) != last) kept.add(last);
        return kept;
    }

    @Override
    public Map<Long, SyncMetricsSampleVo> latestForTasks(Collection<Long> taskIds) {
        return latest(SyncMetricsSample.OWNER_TASK, taskIds);
    }

    @Override
    public Map<Long, SyncMetricsSampleVo> latestForGroupItems(Collection<Long> itemIds) {
        return latest(SyncMetricsSample.OWNER_GROUP_ITEM, itemIds);
    }

    private Map<Long, SyncMetricsSampleVo> latest(String ownerType, Collection<Long> ownerIds) {
        Map<Long, SyncMetricsSampleVo> result = new HashMap<>();
        if (ownerIds == null || ownerIds.isEmpty()) return result;
        for (SyncMetricsSample sample : sampleMapper.selectLatest(ownerType, ownerIds)) {
            result.put(sample.getOwnerId(), MapstructUtils.convert(sample, SyncMetricsSampleVo.class));
        }
        return result;
    }

    @Override
    public void deleteForTask(Long taskId) {
        sampleMapper.deleteByOwner(SyncMetricsSample.OWNER_TASK, taskId);
    }

    @Override
    public void deleteForGroup(Long groupId) {
        sampleMapper.deleteByGroupId(groupId);
    }

    @Scheduled(fixedDelayString = "${sync.metrics.purge-interval-ms:3600000}", initialDelayString = "${sync.metrics.purge-initial-delay-ms:120000}")
    public void purgeExpired() {
        int days = Math.max(1, properties.getRetentionDays());
        try {
            int removed = sampleMapper.deleteBefore(LocalDateTime.now().minusDays(days));
            if (removed > 0) log.info("purged {} metrics samples older than {} days", removed, days);
        } catch (RuntimeException ex) {
            log.warn("metrics purge failed: {}", ex.getMessage());
        }
    }
}
