package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.config.SyncSchedulingConfig;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.engine.EngineCheckpointFiles;
import org.dromara.sync.engine.EngineJobStates;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.SyncLocks;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Deletes the engine checkpoint directories of jobs the platform has given up.
 *
 * <p>Zeta deletes a job's checkpoints only when that job itself ends FINISHED or CANCELED. A stop,
 * reinitialize or delete of a job that does not run on the engine - paused, failed, or forgotten by
 * a restarted engine - is a no-op there, so every such job left its checkpoint directory in the
 * engine's storage for good. Nothing restores from them any more: a task or group item resumes only
 * the job id it still points at.
 *
 * <p>A directory is deleted when all of these hold:
 * <ul>
 *   <li>no task and no group item references its job id ({@code engine_job_id});</li>
 *   <li>nothing was written into it for {@code sync.engine.checkpoint-retire-grace} - a running job
 *       writes a checkpoint every interval, and a job just submitted may not be saved on its owner yet;</li>
 *   <li>the engine does not report the job as running or pausing (an orphan {@code EngineOrphanSweeper}
 *       is about to adopt still runs). A job the engine has no record of, or one it reports finished,
 *       failed, cancelled or paused, is given up.</li>
 * </ul>
 * One instance at a time: the pass runs under a Redisson lock taken without waiting, a busy pass is
 * skipped. An engine that does not answer ends the pass - nothing is deleted on a guess. Nothing
 * happens while the storage is not visible to this process ({@code sync.engine.checkpoint-dir}).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EngineCheckpointJanitor {

    private final EngineCheckpointFiles checkpointFiles;
    private final SeaTunnelRestClient restClient;
    private final SyncTaskMapper taskMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SeaTunnelProperties properties;
    private final SyncLocks locks;

    @Scheduled(fixedDelayString = "${sync.engine.checkpoint-cleanup-interval-ms:3600000}", initialDelayString = "${sync.engine.checkpoint-cleanup-initial-delay-ms:300000}", scheduler = SyncSchedulingConfig.SCHEDULER)
    public void clean() {
        try {
            if (!cleanExclusively(Instant.now())) log.debug("engine checkpoint cleanup skipped: another instance runs it");
        } catch (RuntimeException ex) {
            log.warn("engine checkpoint cleanup pass failed, retried next pass: {}", ex.getMessage());
        }
    }

    /** One pass unless another instance holds the lock ({@code false}); package-private for tests. */
    boolean cleanExclusively(Instant now) {
        return locks.runIfFree(SyncLocks.CHECKPOINT_CLEANUP_LOCK, () -> cleanOnce(now));
    }

    /** One pass; package-private for tests. Returns the job ids whose checkpoint directory was deleted. */
    List<String> cleanOnce(Instant now) {
        if (!checkpointFiles.visible()) return List.of();
        Instant graceStart = now.minus(grace());
        // Directories before owners: a job saved on its owner after this read is still referenced below,
        // and one whose directory appears after it is simply seen next pass.
        List<String> idle = new ArrayList<>();
        for (String jobId : checkpointFiles.jobIds()) {
            Instant written = checkpointFiles.lastWritten(jobId);
            if (written != null && written.isBefore(graceStart)) idle.add(jobId);
        }
        if (idle.isEmpty()) return List.of();
        Set<String> referenced = new HashSet<>(taskMapper.selectEngineJobIds());
        referenced.addAll(itemMapper.selectEngineJobIds());
        List<String> deleted = new ArrayList<>();
        for (String jobId : idle) {
            if (referenced.contains(jobId) || runsOnEngine(jobId)) continue;
            if (checkpointFiles.delete(jobId)) {
                deleted.add(jobId);
                log.info("Deleted the engine checkpoints of job {}: no task or table uses it any more", jobId);
            }
        }
        return deleted;
    }

    /** Throws when the engine does not answer: the pass ends without deleting on a guess. */
    private boolean runsOnEngine(String jobId) {
        String engineStatus = restClient.status(jobId).status();
        if (StringUtils.isBlank(engineStatus)) return false;
        String status = EngineJobStates.toPlatformStatus(engineStatus);
        return SyncStatus.RUNNING.equals(status) || SyncStatus.PAUSING.equals(status);
    }

    private Duration grace() {
        Duration grace = properties.getCheckpointRetireGrace();
        return grace == null || grace.isNegative() ? Duration.ZERO : grace;
    }
}
