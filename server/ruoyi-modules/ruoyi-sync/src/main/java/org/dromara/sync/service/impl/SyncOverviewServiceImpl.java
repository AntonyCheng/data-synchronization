package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.SyncOverviewVo;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISyncOverviewService;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Dashboard counters. Deliberately reads the four tables in full rather than issuing a dozen
 * COUNT queries: the platform's own scale is tasks/groups in the hundreds, and one pass keeps
 * every number consistent with the same snapshot.
 */
@RequiredArgsConstructor
@Service
public class SyncOverviewServiceImpl implements ISyncOverviewService {

    private static final String ENABLED = "0";
    private static final String MATCHED = "1";

    private final SyncTaskMapper taskMapper;
    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final DataSourceMapper dataSourceMapper;

    @Override
    public SyncOverviewVo overview() {
        List<SyncTask> tasks = taskMapper.selectList(null);
        List<SyncTaskGroup> groups = groupMapper.selectList(null);
        List<SyncTaskGroupItem> items = itemMapper.selectList(null);
        List<DataSource> dataSources = dataSourceMapper.selectList(null);

        SyncOverviewVo result = new SyncOverviewVo();
        result.setTaskTotal(tasks.size());
        result.setTaskRunning(tasks.stream().filter(task -> SyncStatus.RUNNING.equals(task.getStatus())).count());
        result.setGroupTotal(groups.size());
        result.setGroupRunning(groups.stream()
            .filter(group -> SyncStatus.RUNNING.equals(group.getStatus()) || SyncStatus.DEGRADED.equals(group.getStatus()))
            .count());
        result.setGroupItemTotal(items.size());
        result.setGroupItemRunning(items.stream().filter(item -> SyncStatus.RUNNING.equals(item.getStatus())).count());

        // One engine job per single-table task and per table item, so that is the honest "job" count.
        result.setJobTotal(tasks.size() + items.size());
        result.setJobRunning(result.getTaskRunning() + result.getGroupItemRunning());
        result.setJobFailed(tasks.stream().filter(task -> isFailed(task.getStatus())).count()
            + items.stream().filter(item -> isFailed(item.getStatus())).count());

        result.setDataSourceTotal(dataSources.size());
        result.setDataSourceEnabled(dataSources.stream().filter(source -> ENABLED.equals(source.getStatus())).count());

        long checkedRows = tasks.stream().mapToLong(task -> orZero(task.getLastCheckTargetRows())).sum()
            + items.stream().mapToLong(item -> orZero(item.getLastCheckTargetRows())).sum();
        result.setCheckedRows(checkedRows);
        result.setCheckedJobs(tasks.stream().filter(task -> StringUtils.isNotBlank(task.getLastCheckMatched())).count()
            + items.stream().filter(item -> StringUtils.isNotBlank(item.getLastCheckMatched())).count());
        result.setCheckedMatched(tasks.stream().filter(task -> MATCHED.equals(task.getLastCheckMatched())).count()
            + items.stream().filter(item -> MATCHED.equals(item.getLastCheckMatched())).count());

        List<Long> lags = tasks.stream().map(SyncTask::getKafkaLagSeconds).filter(java.util.Objects::nonNull).toList();
        result.setAvgCdcLagSeconds(lags.isEmpty() ? null
            : Math.round(lags.stream().mapToLong(Long::longValue).average().orElse(0)));

        Map<String, Long> statusCounts = new HashMap<>();
        tasks.forEach(task -> statusCounts.merge(statusOf(task.getStatus()), 1L, Long::sum));
        items.forEach(item -> statusCounts.merge(statusOf(item.getStatus()), 1L, Long::sum));
        result.setStatusCounts(statusCounts);

        // A job's target type comes from its data source; a group's items all share the group's.
        Map<Long, String> typeBySourceId = dataSources.stream()
            .collect(Collectors.toMap(DataSource::getSourceId, DataSource::getSourceType, (left, right) -> left));
        Map<Long, Long> targetByGroupId = groups.stream()
            .filter(group -> group.getTargetId() != null)
            .collect(Collectors.toMap(SyncTaskGroup::getGroupId, SyncTaskGroup::getTargetId, (left, right) -> left));
        Map<String, Long> targetTypeCounts = new HashMap<>();
        Map<String, Long> targetTypeRunning = new HashMap<>();
        tasks.forEach(task -> {
            countTarget(targetTypeCounts, typeBySourceId, task.getTargetId());
            if (SyncStatus.RUNNING.equals(task.getStatus())) {
                countTarget(targetTypeRunning, typeBySourceId, task.getTargetId());
            }
        });
        items.forEach(item -> {
            Long targetId = targetByGroupId.get(item.getGroupId());
            countTarget(targetTypeCounts, typeBySourceId, targetId);
            if (SyncStatus.RUNNING.equals(item.getStatus())) countTarget(targetTypeRunning, typeBySourceId, targetId);
        });
        result.setTargetTypeCounts(targetTypeCounts);
        result.setTargetTypeRunningCounts(targetTypeRunning);
        return result;
    }

    private static void countTarget(Map<String, Long> counts, Map<Long, String> typeBySourceId, Long targetId) {
        String type = targetId == null ? null : typeBySourceId.get(targetId);
        if (StringUtils.isNotBlank(type)) counts.merge(type, 1L, Long::sum);
    }

    private static boolean isFailed(String status) {
        return SyncStatus.FAILED.equals(status) || SyncStatus.REINITIALIZE_REQUIRED.equals(status)
            || SyncStatus.DDL_BLOCKED.equals(status);
    }

    private static String statusOf(String status) {
        return StringUtils.isBlank(status) ? SyncStatus.DRAFT : status;
    }

    private static long orZero(Long value) {
        return value == null ? 0L : value;
    }
}
