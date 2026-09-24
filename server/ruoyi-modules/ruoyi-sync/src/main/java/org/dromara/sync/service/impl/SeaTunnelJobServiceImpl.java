package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.config.SyncSchedulingConfig;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.vo.SeaTunnelJobConfigPreview;
import org.dromara.sync.domain.vo.SeaTunnelJobOperationResult;
import org.dromara.sync.domain.vo.SeaTunnelJobStatus;
import org.dromara.sync.engine.EngineJobStates;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.engine.SourceColumns;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.service.ISyncTaskService;
import org.dromara.sync.support.TargetTableSwap;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncSchedules;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Single-table task lifecycle against SeaTunnel: preview, start, status reconciliation,
 * pause/resume with savepoint, stop and reinitialize. Credentials never reach API callers.
 */
@RequiredArgsConstructor
@Service
public class SeaTunnelJobServiceImpl implements ISeaTunnelJobService {

    /**
     * Consecutive {@link SeaTunnelRestClient#status} failures tolerated per task. One failed
     * poll used to flip a task straight to FAILED and tear down its Kafka bridge; a single
     * REST timeout or engine GC pause is not proof the job died. The engine keeps running
     * and Debezium keeps buffering into the raw topic, so give up only after this many.
     */
    private static final int STATUS_FAILURE_TOLERANCE = 3;
    private static final Set<String> REINITIALIZABLE_STATUSES = Set.of(SyncStatus.REINITIALIZE_REQUIRED, SyncStatus.FAILED, SyncStatus.STOPPED);

    private final SyncTaskMapper syncTaskMapper;
    private final DataSourceMapper dataSourceMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final SeaTunnelProperties properties;
    private final SeaTunnelRestClient restClient;
    private final ISyncTaskService syncTaskService;
    private final ResourceProtectionPolicy resourceProtectionPolicy;
    private final SyncLocks locks;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;
    private final ISyncMetricsService metricsService;

    /** Consecutive status-poll failures per task; reset on any successful poll. */
    private final ConcurrentMap<Long, Integer> statusFailureStreak = new ConcurrentHashMap<>();

    @Override
    public SeaTunnelJobConfigPreview previewConfig(Long taskId) {
        SyncTask task = requireTask(taskId);
        SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
        SeaTunnelJobConfigPreview preview = new SeaTunnelJobConfigPreview();
        preview.setTaskId(taskId);
        preview.setJobName(generated.jobName());
        preview.setSourceTable(generated.sourceTable());
        preview.setTargetTable(generated.targetTable());
        preview.setPrimaryKeys(generated.primaryKeys());
        preview.setConfig(generated.redactedConfig());
        return preview;
    }

    @Override
    public SeaTunnelJobOperationResult start(Long taskId) {
        return locks.withTaskLock(taskId, () -> doStart(taskId));
    }

    private SeaTunnelJobOperationResult doStart(Long taskId) {
        boolean submissionStarted = false;
        boolean kafkaBridgeStarted = false;
        try {
            SyncTask task = requireTask(taskId);
            ensureStartable(task);
            prepareResourceProtection(task);
            prepareOverwriteStage(task);
            var validation = syncTaskService.validate(taskId);
            if (!validation.isValid()) throw new ServiceException("启动前校验未通过：" + validation.getMessage());
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
            prepareTarget(task, generated);
            if (isKafkaTask(task)) {
                startBridge(task);
                kafkaBridgeStarted = true;
            }
            submissionStarted = true;
            SeaTunnelRestClient.SubmitResult submitted = restClient.submit(generated.jobName(), generated.config(), null, false);
            markRunning(task, submitted.jobId(), generated);
            return operation(task, "作业已提交");
        } catch (ServiceException ex) {
            if (kafkaBridgeStarted) kafkaTaskBridgeService.stop(taskId);
            SyncTask task = syncTaskMapper.selectById(taskId);
            if (submissionStarted && task != null) markFailed(task, task.getEngineJobId(), ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            if (kafkaBridgeStarted) kafkaTaskBridgeService.stop(taskId);
            SyncTask task = syncTaskMapper.selectById(taskId);
            if (submissionStarted && task != null) markFailed(task, task.getEngineJobId(), ex.getMessage());
            throw new ServiceException("启动同步任务失败：" + SyncText.safeMessage(ex, "未知错误"));
        }
    }

    @Override
    public SeaTunnelJobStatus refreshStatus(Long taskId) {
        return locks.withTaskLock(taskId, () -> doRefreshStatus(taskId));
    }

    private SeaTunnelJobStatus doRefreshStatus(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        boolean kafka = isKafkaTask(task);
        SeaTunnelRestClient.JobSnapshot snapshot;
        try {
            snapshot = restClient.status(jobId);
            statusFailureStreak.remove(taskId);
        } catch (ServiceException ex) {
            if (EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) {
                statusFailureStreak.remove(taskId);
                if (kafka) kafkaTaskBridgeService.stop(taskId);
                return markReinitializeRequired(task, jobId, ex.getMessage());
            }
            int streak = statusFailureStreak.merge(taskId, 1, Integer::sum);
            if (streak < STATUS_FAILURE_TOLERANCE) {
                // Transient engine unreachability - leave the task RUNNING and the Kafka
                // bridge alone; the raw topic keeps buffering until the REST endpoint
                // recovers. Persist nothing so a recovered poll reports real state.
                return transientStatus(task, jobId, ex.getMessage(), streak);
            }
            statusFailureStreak.remove(taskId);
            if (kafka) kafkaTaskBridgeService.stop(taskId);
            return markFailed(task, jobId, ex.getMessage());
        }
        SeaTunnelRestClient.CheckpointSnapshot checkpoint = SeaTunnelRestClient.CheckpointSnapshot.empty();
        String checkpointError = null;
        try {
            checkpoint = restClient.checkpoints(jobId);
        } catch (ServiceException ex) {
            if (EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) {
                return markReinitializeRequired(task, jobId, ex.getMessage());
            }
            checkpointError = ex.getMessage();
        }
        String platformStatus = EngineJobStates.toPlatformStatus(snapshot.status());
        if (kafka) {
            // A bridge is wanted exactly while the task is RUNNING (KafkaBridgeReconciler applies
            // the same rule process-wide). Raw events buffered during a pause are consumed after
            // resume - offsets are only committed after the broker acked the normalized event.
            if (!SyncStatus.RUNNING.equals(platformStatus)) {
                kafkaTaskBridgeService.stop(taskId);
            } else if (!kafkaTaskBridgeService.isRunning(taskId)) {
                // Engine job alive, bridge dead (torn down by an earlier status failure or died
                // on its own) - heal it right here rather than waiting for the next reconcile pass.
                startBridge(task);
            }
        }
        if (SyncStatus.FAILED.equals(platformStatus) && EngineJobStates.isRecoveryBoundaryError(snapshot.errorMessage())) {
            return markReinitializeRequired(task, jobId, snapshot.errorMessage());
        }
        if (SyncStatus.FINISHED.equals(platformStatus) && StringUtils.isNotBlank(task.getOverwriteStageTable())) {
            try {
                finalizeOverwrite(task);
            } catch (Exception ex) {
                return markFailedAfterEngineCompletion(task, jobId, "覆盖刷新替换正式表失败：" + SyncText.safeMessage(ex, "未知错误"));
            }
        }
        task.setStatus(platformStatus);
        String statusError = SyncStatus.FAILED.equals(platformStatus) && StringUtils.isBlank(snapshot.errorMessage())
            ? "SeaTunnel 作业状态为 FAILED" : "";
        String lastError = StringUtils.defaultIfBlank(snapshot.errorMessage(),
            StringUtils.defaultIfBlank(checkpointError, StringUtils.defaultIfBlank(statusError, "")));
        task.setLastError(SyncText.truncateForColumn(lastError));
        if (checkpoint.id() != null) {
            task.setLastCheckpointId(checkpoint.id());
            task.setLastCheckpointTime(checkpoint.time());
            task.setLastCheckpointStatus(checkpoint.status());
        }
        syncTaskMapper.updateById(task);
        SeaTunnelJobStatus result = statusOf(task, jobId, snapshot.status(), platformStatus,
            StringUtils.isBlank(snapshot.errorMessage()) ? null : SyncText.truncateForColumn(snapshot.errorMessage()));
        copyKafkaMetrics(result, task);
        EngineJobStates.applyMetrics(result, snapshot, task.getSyncMode());
        if (kafka && result.getCdcLagSeconds() == null && task.getKafkaLagSeconds() != null) {
            // The bridge measures source event time -> broker ack: the end-to-end lag of a Kafka target.
            result.setCdcLagSeconds(task.getKafkaLagSeconds());
            result.setMetricsMessage(null);
        }
        metricsService.recordTask(task, snapshot.status(), result);
        return result;
    }

    @Override
    @EventListener(ApplicationReadyEvent.class)
    public void recoverRunningTasks() {
        syncTaskMapper.selectActive().forEach(task -> {
            try {
                if (SyncStatus.RUNNING.equals(task.getStatus()) && isKafkaTask(task)) startBridge(task);
                refreshStatus(task.getTaskId());
            } catch (Exception ex) {
                markFailed(task, task.getEngineJobId(), ex.getMessage());
            }
        });
    }

    /**
     * Beyond the startup reconciliation above, nothing else polled engine state for a task
     * left RUNNING between user actions - a FULL-mode task (or any job that finishes or dies
     * on the engine side without the user ever clicking "刷新状态") stayed stuck showing
     * RUNNING in the list indefinitely. Poll periodically so platform status doesn't silently
     * drift from engine truth while the process keeps running.
     */
    @Scheduled(fixedDelayString = "${sync.status-refresh.interval-ms:30000}", initialDelayString = "${sync.status-refresh.initial-delay-ms:20000}", scheduler = SyncSchedulingConfig.SCHEDULER)
    public void refreshRunningTaskStatus() {
        syncTaskMapper.selectActive().forEach(task -> {
            // Someone (a user action, the scheduler, or another pass) already holds this task;
            // skipping is cheaper than queueing behind it - the next cycle picks it up.
            if (locks.isTaskBusy(task.getTaskId())) return;
            try {
                refreshStatus(task.getTaskId());
            } catch (Exception ex) {
                markFailed(task, task.getEngineJobId(), ex.getMessage());
            }
        });
    }

    @Override
    public SeaTunnelJobOperationResult pause(Long taskId) {
        return locks.withTaskLock(taskId, () -> doPause(taskId));
    }

    private SeaTunnelJobOperationResult doPause(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        if (!SyncStatus.isActive(task.getStatus())) {
            throw new ServiceException("只有运行中的任务可以暂停");
        }
        restClient.stop(jobId, true, false);
        if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
        task.setStatus(SyncStatus.PAUSING);
        task.setLastError("");
        syncTaskMapper.updateById(task);
        return operation(task, "暂停请求已提交，请刷新状态确认 savepoint 完成");
    }

    @Override
    public SeaTunnelJobOperationResult resume(Long taskId) {
        return locks.withTaskLock(taskId, () -> doResume(taskId));
    }

    private SeaTunnelJobOperationResult doResume(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        if (!SyncStatus.PAUSED.equals(task.getStatus()) && !SyncStatus.FAILED.equals(task.getStatus())) {
            throw new ServiceException("只有已暂停或可恢复失败任务可以恢复");
        }
        boolean kafka = isKafkaTask(task);
        // Both preconditions mean the savepoint can never be reused, so the task lands in
        // REINITIALIZE_REQUIRED on purpose - not FAILED, which would invite another resume.
        SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
        if (StringUtils.isNotBlank(task.getEngineConfigHash()) && !generated.matchesFingerprint(task.getEngineConfigHash())) {
            throw refuseResume(task, jobId, "任务配置已变化，不能使用原 checkpoint 恢复，请重新初始化");
        }
        SeaTunnelRestClient.CheckpointSnapshot checkpoint;
        try {
            checkpoint = restClient.checkpoints(jobId);
        } catch (ServiceException ex) {
            if (EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) throw refuseResume(task, jobId, ex.getMessage());
            markFailed(task, jobId, ex.getMessage());
            throw ex;
        }
        if (checkpoint.id() == null) {
            throw refuseResume(task, jobId, "任务没有可用的 checkpoint/savepoint，不能从未知位点恢复，请重新初始化");
        }
        try {
            if (kafka) startBridge(task);
            restClient.submit(generated.jobName(), generated.config(), jobId, true);
            markRunning(task);
            return operation(task, "作业已从 savepoint 恢复");
        } catch (ServiceException ex) {
            // The Kafka bridge (if any) is started before restClient.submit() above, so a
            // ServiceException from submit() must stop it too - otherwise the bridge's
            // consumer thread and Kafka consumer-group membership are orphaned.
            if (kafka) kafkaTaskBridgeService.stop(taskId);
            if (EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) throw refuseResume(task, jobId, ex.getMessage());
            markFailed(task, jobId, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            if (kafka) kafkaTaskBridgeService.stop(taskId);
            markFailed(task, jobId, ex.getMessage());
            throw new ServiceException("恢复同步任务失败：" + SyncText.safeMessage(ex, "未知错误"));
        }
    }

    /** Persists REINITIALIZE_REQUIRED with the reason and returns the exception to throw. */
    private ServiceException refuseResume(SyncTask task, String jobId, String reason) {
        markReinitializeRequired(task, jobId, reason);
        return new ServiceException(reason);
    }

    @Override
    public SeaTunnelJobOperationResult stop(Long taskId) {
        return locks.withTaskLock(taskId, () -> doStop(taskId));
    }

    private SeaTunnelJobOperationResult doStop(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        if (SyncStatus.STOPPED.equals(task.getStatus()) || SyncStatus.DRAFT.equals(task.getStatus())) {
            throw new ServiceException("任务当前未运行");
        }
        restClient.stop(jobId, false, false);
        if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
        task.setStatus(SyncStatus.STOPPED);
        task.setLastError("");
        // SyncTaskScheduler treats STOPPED as an eligible status (a fresh STOPPED task can be
        // rearmed by editing its schedule), but a leftover past nextRunTime from the run that
        // just got stopped would make the very next poll immediately restart this job. Clear it
        // so a manual stop actually stops the task until the user restarts or reschedules it.
        task.setNextRunTime(null);
        syncTaskMapper.updateById(task);
        return operation(task, "作业已停止");
    }

    @Override
    public SeaTunnelJobOperationResult reinitialize(Long taskId) {
        return locks.withTaskLock(taskId, () -> doReinitialize(taskId));
    }

    private SeaTunnelJobOperationResult doReinitialize(Long taskId) {
        SyncTask task = requireTask(taskId);
        if (SyncMode.INCREMENTAL.equalsIgnoreCase(task.getSyncMode())) {
            throw new ServiceException("纯增量任务没有全量初始化语义，请新建全量 + CDC 任务并先建立目标基线");
        }
        if (task.getStatus() == null || !REINITIALIZABLE_STATUSES.contains(task.getStatus())) {
            throw new ServiceException("只有需重新初始化、失败或已停止任务可以重新初始化");
        }
        prepareResourceProtection(task);
        // Every refusal is decided before the old job is destroyed: a FAILED task can still be
        // resumed from its savepoint, and a reinitialize that is then refused (target no longer
        // compatible, source unreachable) used to throw that away and leave the task FAILED for
        // a reason that had nothing to do with its job. A refusal now changes nothing.
        var validation = syncTaskService.validate(taskId);
        if (!validation.isValid()) {
            throw new ServiceException("重新初始化前校验未通过：" + validation.getMessage());
        }
        SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
        boolean kafka = isKafkaTask(task);
        try {
            if (StringUtils.isNotBlank(task.getEngineJobId())) {
                try {
                    restClient.stop(task.getEngineJobId(), false, true);
                } catch (ServiceException ignored) {
                    // The old engine job may already be gone; clearing its recovery state is still valid.
                }
            }
            prepareTarget(task, generated);
            if (kafka) startBridge(task);
            SeaTunnelRestClient.SubmitResult submitted = restClient.submit(generated.jobName(), generated.config(), null, false);
            markRunning(task, submitted.jobId(), generated);
            clearCheckpoint(task);
            return operation(task, "已丢弃旧恢复状态并重新启动全量初始化");
        } catch (ServiceException ex) {
            if (kafka) kafkaTaskBridgeService.stop(taskId);
            markFailed(task, task.getEngineJobId(), ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            if (kafka) kafkaTaskBridgeService.stop(taskId);
            markFailed(task, task.getEngineJobId(), ex.getMessage());
            throw new ServiceException("重新初始化任务失败：" + SyncText.safeMessage(ex, "未知错误"));
        }
    }

    private SeaTunnelJobConfigGenerator.GeneratedConfig generate(SyncTask task) {
        DataSource source = dataSourceService.requireUsable(task.getSourceId(), "源");
        DataSource target = dataSourceService.requireUsable(task.getTargetId(), "目标");
        return SeaTunnelJobConfigGenerator.generate(task, source, target, properties, SourceColumns.fromMetadata(metadataService, source));
    }

    /** Pre-creates a MySQL FULL target from the source DDL where SeaTunnel's inference would get the key types wrong. */
    private void prepareTarget(SyncTask task, SeaTunnelJobConfigGenerator.GeneratedConfig generated) {
        DataSource source = dataSourceService.requireUsable(task.getSourceId(), "源");
        DataSource target = dataSourceService.requireUsable(task.getTargetId(), "目标");
        SeaTunnelJobConfigGenerator.prepareTarget(task, source, target, generated, SourceColumns.fromMetadata(metadataService, source));
    }

    /** Lenient: a task whose target row is missing is simply not a Kafka task here; the start paths fail loudly later. */
    private boolean isKafkaTask(SyncTask task) {
        return task != null && DataSourceType.isKafka(dataSourceMapper.selectById(task.getTargetId()));
    }

    /**
     * Start / resume / reinitialize call this just before submitting a job (the task is not active
     * yet), so a full bridge pool refuses them while nothing has been submitted. An active task -
     * status-refresh heal, startup recovery - already has its engine job: a full pool only parks
     * its bridge until a slot frees, failing the task would not stop the job.
     */
    private void startBridge(SyncTask task) {
        DataSource target = dataSourceService.requireUsable(task.getTargetId(), "目标");
        String sourceDatabase = dataSourceService.requireUsable(task.getSourceId(), "源").getDatabaseName();
        if (SyncStatus.isActive(task.getStatus())) kafkaTaskBridgeService.tryStart(task, target, sourceDatabase);
        else kafkaTaskBridgeService.start(task, target, sourceDatabase);
    }

    private void prepareResourceProtection(SyncTask task) {
        resourceProtectionPolicy.applyDefaultsAndValidate(task);
        syncTaskMapper.updateById(task);
    }

    /** FULL + OVERWRITE writes into a versioned staging table that {@link #finalizeOverwrite} swaps in on completion. */
    private void prepareOverwriteStage(SyncTask task) {
        if (SyncMode.isFull(task.getSyncMode()) && "OVERWRITE".equalsIgnoreCase(task.getFullDataMode())) {
            task.setOverwriteStageTable("__ds_stage_" + task.getTaskId() + "_v" + configVersionOf(task));
            task.setLastError("");
            syncTaskMapper.updateById(task);
        } else {
            task.setOverwriteStageTable(null);
        }
    }

    private void finalizeOverwrite(SyncTask task) throws SQLException {
        DataSource target = dataSourceService.requireUsable(task.getTargetId(), "目标");
        String schema = StringUtils.defaultIfBlank(task.getTargetSchema(), TableNames.DEFAULT_POSTGRES_SCHEMA);
        String backupName = "__ds_backup_" + task.getTaskId() + "_v" + configVersionOf(task);
        TargetTableSwap.swap(target, schema, task.getTargetTable(), task.getOverwriteStageTable(), backupName);
        task.setOverwriteStageTable(null);
        // updateById drops nulls, so the stage marker is cleared explicitly - a later refresh must be idempotent.
        syncTaskMapper.clearOverwriteStage(task.getTaskId());
    }

    /**
     * The old checkpoint belongs to the job that was just discarded. The global not_null
     * update strategy would silently drop these nulls from {@code updateById}, leaving the
     * stale checkpoint on display until the new job happens to report one.
     */
    private void clearCheckpoint(SyncTask task) {
        task.setLastCheckpointId(null);
        task.setLastCheckpointTime(null);
        task.setLastCheckpointStatus(null);
        syncTaskMapper.clearCheckpoint(task.getTaskId());
    }

    private static int configVersionOf(SyncTask task) {
        return task.getConfigVersion() == null ? 1 : task.getConfigVersion();
    }

    private SyncTask requireTask(Long taskId) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) throw new ServiceException("同步任务不存在");
        return task;
    }

    private static void ensureStartable(SyncTask task) {
        if (SyncStatus.isActive(task.getStatus())) {
            throw new ServiceException("任务当前正在运行");
        }
        if (SyncStatus.FAILED.equals(task.getStatus()) || SyncStatus.REINITIALIZE_REQUIRED.equals(task.getStatus())) {
            throw new ServiceException("任务存在恢复风险，请使用恢复任务或重新初始化");
        }
        if (SyncStatus.PAUSED.equals(task.getStatus())) {
            // A fresh submit would silently throw the savepoint away and re-snapshot.
            throw new ServiceException("任务处于暂停状态，请使用恢复任务从 savepoint 继续；如需重新全量同步，请先停止任务");
        }
    }

    private static String requireJobId(SyncTask task) {
        if (StringUtils.isBlank(task.getEngineJobId())) throw new ServiceException("任务尚未提交 SeaTunnel 作业");
        return task.getEngineJobId();
    }

    private void markRunning(SyncTask task, String jobId, SeaTunnelJobConfigGenerator.GeneratedConfig submitted) {
        task.setEngineJobId(jobId);
        task.setEngineConfigHash(submitted.fingerprint());
        markRunning(task);
    }

    /** RUNNING plus a re-armed CRON schedule: a parked schedule resumes once an operator restarts the task. */
    private void markRunning(SyncTask task) {
        task.setStatus(SyncStatus.RUNNING);
        task.setLastError("");
        SyncSchedules.rearm(task, LocalDateTime.now());
        syncTaskMapper.updateById(task);
    }

    /** A status poll failed but not enough times to give up - report last-known state, persist nothing. */
    private SeaTunnelJobStatus transientStatus(SyncTask task, String jobId, String error, int streak) {
        SeaTunnelJobStatus result = statusOf(task, jobId, "UNREACHABLE_TRANSIENT", task.getStatus(),
            "SeaTunnel 状态暂不可达（第 " + streak + "/" + STATUS_FAILURE_TOLERANCE
                + " 次），作业与 Kafka 桥接保持运行：" + SyncText.truncateForColumn(StringUtils.defaultIfBlank(error, "")));
        copyKafkaMetrics(result, task);
        return result;
    }

    private SeaTunnelJobStatus markFailed(SyncTask task, String jobId, String error) {
        persistTerminalError(task, SyncStatus.FAILED, StringUtils.isBlank(error) ? "SeaTunnel 作业状态不可用" : error);
        return statusOf(task, jobId, "UNREACHABLE", SyncStatus.FAILED, task.getLastError());
    }

    /** Keep the terminal engine state visible when post-processing fails. */
    private SeaTunnelJobStatus markFailedAfterEngineCompletion(SyncTask task, String jobId, String error) {
        persistTerminalError(task, SyncStatus.FAILED, error);
        SeaTunnelJobStatus result = statusOf(task, jobId, "FINISHED", SyncStatus.FAILED, task.getLastError());
        result.setPhase(EngineJobStates.PHASE_SNAPSHOT);
        return result;
    }

    private SeaTunnelJobStatus markReinitializeRequired(SyncTask task, String jobId, String error) {
        persistTerminalError(task, SyncStatus.REINITIALIZE_REQUIRED,
            StringUtils.isBlank(error) ? "checkpoint/savepoint 或 binlog 位点不可恢复，请重新初始化" : error);
        return statusOf(task, jobId, "RECOVERY_REQUIRED", SyncStatus.REINITIALIZE_REQUIRED, task.getLastError());
    }

    private void persistTerminalError(SyncTask task, String status, String error) {
        task.setStatus(status);
        task.setLastError(SyncText.truncateForColumn(error));
        syncTaskMapper.updateById(task);
    }

    /** Identity + checkpoint fields every status projection carries; callers add metrics as appropriate. */
    private static SeaTunnelJobStatus statusOf(SyncTask task, String jobId, String engineStatus, String platformStatus,
                                               String errorMessage) {
        SeaTunnelJobStatus result = new SeaTunnelJobStatus();
        result.setTaskId(task.getTaskId());
        result.setEngineJobId(jobId);
        result.setEngineStatus(engineStatus);
        result.setStatus(platformStatus);
        result.setErrorMessage(errorMessage);
        result.setLastCheckpointId(task.getLastCheckpointId());
        result.setLastCheckpointTime(task.getLastCheckpointTime());
        result.setLastCheckpointStatus(task.getLastCheckpointStatus());
        return result;
    }

    private static void copyKafkaMetrics(SeaTunnelJobStatus result, SyncTask task) {
        result.setKafkaPublishedCount(task.getKafkaPublishedCount());
        result.setKafkaLastPartition(task.getKafkaLastPartition());
        result.setKafkaLastOffset(task.getKafkaLastOffset());
        result.setKafkaLastSourceEventTime(task.getKafkaLastSourceEventTime());
        result.setKafkaLastBrokerAckTime(task.getKafkaLastBrokerAckTime());
        result.setKafkaLagSeconds(task.getKafkaLagSeconds());
    }

    private static SeaTunnelJobOperationResult operation(SyncTask task, String message) {
        SeaTunnelJobOperationResult result = new SeaTunnelJobOperationResult();
        result.setTaskId(task.getTaskId());
        result.setEngineJobId(task.getEngineJobId());
        result.setStatus(task.getStatus());
        result.setMessage(message);
        return result;
    }
}
