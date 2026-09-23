package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.GroupStatuses;
import org.dromara.sync.support.SyncLocks;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Re-attaches jobs the engine is running that the platform lost track of.
 *
 * <p>A submit is bounded by {@code sync.engine.request-timeout}. When that fires the answer is
 * lost but the engine usually went on to accept the job: the platform then records FAILED with
 * no {@code engine_job_id}, the status reconcile (which only looks at rows that have one) never
 * sees it, and the job keeps writing to the target unattended. Worse, a retry would submit a
 * second job onto the same table.
 *
 * <p>Job names are deterministic ({@code ds-task-<taskId>} for a single task, and the same shape
 * on a group item's id), so one listing of the engine's running jobs is enough to find them.
 * {@code SeaTunnelRestClient.submit} already tries to adopt inline, but a cold engine can take
 * longer to register the job than a caller should ever wait - this pass is the safety net that
 * does not depend on that timing.
 */
@Slf4j
@RequiredArgsConstructor
@Component
public class EngineOrphanSweeper {

    private final SyncTaskMapper taskMapper;
    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SeaTunnelRestClient restClient;
    private final SyncLocks locks;

    @Scheduled(fixedDelayString = "${sync.orphan-sweep.interval-ms:30000}", initialDelayString = "${sync.orphan-sweep.initial-delay-ms:50000}")
    public void sweep() {
        try {
            sweepOnce();
        } catch (RuntimeException ex) {
            log.warn("orphan sweep failed: {}", ex.getMessage());
        }
    }

    /** One pass; returns how many jobs were re-attached. */
    int sweepOnce() {
        Map<String, String> running = restClient.runningJobIdsByName();
        if (running.isEmpty()) return 0;
        int adopted = 0;
        for (Map.Entry<String, String> job : running.entrySet()) {
            Long ownerId = ownerIdOf(job.getKey());
            if (ownerId == null) continue;
            adopted += adoptTask(ownerId, job.getValue()) || adoptGroupItem(ownerId, job.getValue()) ? 1 : 0;
        }
        return adopted;
    }

    /** {@code ds-task-42} -> 42; anything else on the engine is not ours. */
    private static Long ownerIdOf(String jobName) {
        if (jobName == null || !jobName.startsWith(SeaTunnelJobConfigGenerator.JOB_NAME_PREFIX)) return null;
        try {
            return Long.valueOf(jobName.substring(SeaTunnelJobConfigGenerator.JOB_NAME_PREFIX.length()));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private boolean adoptTask(Long taskId, String jobId) {
        SyncTask task = taskMapper.selectById(taskId);
        if (task == null || !needsAdoption(task.getEngineJobId(), task.getStatus())) return false;
        // A lock is held while someone starts / stops / rebuilds this task; the job they are
        // about to replace must not be adopted mid-flight. The next pass picks it up.
        if (locks.isTaskBusy(taskId)) return false;
        log.warn("adopting engine job {} for task {} ({}): the platform had lost its job id",
            jobId, taskId, task.getTaskName());
        task.setEngineJobId(jobId);
        task.setStatus(SyncStatus.RUNNING);
        task.setLastError("已接管引擎上仍在运行的作业（提交时未收到应答）");
        taskMapper.updateById(task);
        return true;
    }

    private boolean adoptGroupItem(Long itemId, String jobId) {
        SyncTaskGroupItem item = itemMapper.selectById(itemId);
        if (item == null || !needsAdoption(item.getEngineJobId(), item.getStatus())) return false;
        if (locks.isGroupBusy(item.getGroupId())) return false;
        log.warn("adopting engine job {} for group item {} (table {}): the platform had lost its job id",
            jobId, itemId, item.getSourceTable());
        item.setEngineJobId(jobId);
        item.setStatus(SyncStatus.RUNNING);
        item.setLastError("已接管引擎上仍在运行的作业（提交时未收到应答）");
        itemMapper.updateById(item);
        SyncTaskGroup group = groupMapper.selectById(item.getGroupId());
        if (group != null) {
            group.setStatus(GroupStatuses.aggregate(
                itemMapper.selectByGroupId(item.getGroupId()).stream().map(SyncTaskGroupItem::getStatus).toList()));
            groupMapper.updateById(group);
        }
        return true;
    }

    /**
     * Only rows that hold no job id are candidates. A row that already points at some job is
     * either correct or handled by the status reconcile, and must not be repointed from here.
     */
    private static boolean needsAdoption(String engineJobId, String status) {
        return StringUtils.isBlank(engineJobId) && !SyncStatus.RUNNING.equals(status);
    }
}
