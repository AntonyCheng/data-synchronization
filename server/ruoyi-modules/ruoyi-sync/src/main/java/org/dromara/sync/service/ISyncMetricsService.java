package org.dromara.sync.service;

import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.EngineJobMetrics;
import org.dromara.sync.domain.vo.SyncMetricsSampleVo;
import org.dromara.sync.domain.vo.SyncMetricsSeriesVo;

import java.util.Collection;
import java.util.Map;

/**
 * Engine run-metrics history. Recording never throws: a metrics write failure must not
 * turn a successful status reconcile into a task failure.
 */
public interface ISyncMetricsService {

    void recordTask(SyncTask task, String engineStatus, EngineJobMetrics metrics);

    void recordGroupItem(SyncTaskGroupItem item, String engineStatus, EngineJobMetrics metrics);

    /** Trailing series for a task; {@code minutes} null or out of range is clamped. */
    SyncMetricsSeriesVo queryTaskSeries(Long taskId, Integer minutes);

    SyncMetricsSeriesVo queryGroupItemSeries(Long groupId, Long itemId, Integer minutes);

    /** Newest sample per task id; tasks never polled are absent. */
    Map<Long, SyncMetricsSampleVo> latestForTasks(Collection<Long> taskIds);

    Map<Long, SyncMetricsSampleVo> latestForGroupItems(Collection<Long> itemIds);

    void deleteForTask(Long taskId);

    void deleteForGroup(Long groupId);
}
