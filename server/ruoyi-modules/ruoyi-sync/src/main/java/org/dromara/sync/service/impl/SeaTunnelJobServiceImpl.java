package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.vo.SeaTunnelJobConfigPreview;
import org.dromara.sync.domain.vo.SeaTunnelJobOperationResult;
import org.dromara.sync.domain.vo.SeaTunnelJobStatus;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.ISeaTunnelJobService;
import org.dromara.sync.service.ISyncTaskService;
import org.springframework.stereotype.Service;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/** Builds engine configuration without exposing credentials to API callers. */
@RequiredArgsConstructor
@Service
public class SeaTunnelJobServiceImpl implements ISeaTunnelJobService {

    private final SyncTaskMapper syncTaskMapper;
    private final DataSourceMapper dataSourceMapper;
    private final SeaTunnelProperties properties;
    private final SeaTunnelRestClient restClient;
    private final ISyncTaskService syncTaskService;
    private final ResourceProtectionPolicy resourceProtectionPolicy;
    private final RedissonClient redissonClient;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;

    @Override
    public SeaTunnelJobConfigPreview previewConfig(Long taskId) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) {
            throw new ServiceException("同步任务不存在");
        }
        DataSource source = requireSource(task.getSourceId(), "源");
        DataSource target = requireSource(task.getTargetId(), "目标");

        SeaTunnelJobConfigGenerator.GeneratedConfig generated =
            SeaTunnelJobConfigGenerator.generate(task, source, target, properties);
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
        RLock lock = redissonClient.getLock("sync:task:start:" + taskId);
        boolean acquired = false;
        boolean submissionStarted = false;
        boolean kafkaBridgeStarted = false;
        try {
            acquired = lock.tryLock(0, 60, TimeUnit.SECONDS);
            if (!acquired) throw new ServiceException("任务正在提交运行实例，请勿重复启动");
            SyncTask task = requireTask(taskId);
            ensureStartable(task);
            prepareResourceProtection(task);
            prepareOverwriteStage(task);
            var validation = syncTaskService.validate(taskId);
            if (!validation.isValid()) throw new ServiceException("启动前校验未通过：" + validation.getMessage());
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
            if (isKafkaTask(task)) {
                kafkaTaskBridgeService.start(task, requireSource(task.getTargetId(), "目标"),
                    requireSource(task.getSourceId(), "源").getDatabaseName());
                kafkaBridgeStarted = true;
            }
            submissionStarted = true;
            SeaTunnelRestClient.SubmitResult submitted = restClient.submit(generated.jobName(), generated.config(), null, false);
            task.setEngineJobId(submitted.jobId());
            task.setEngineConfigHash(hash(generated.config()));
            task.setStatus("RUNNING");
            task.setLastError("");
            syncTaskMapper.updateById(task);
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
            throw new ServiceException("启动同步任务失败：" + safeMessage(ex));
        } finally {
            if (acquired && lock.isHeldByCurrentThread()) lock.unlock();
        }
    }

    @Override
    public SeaTunnelJobStatus refreshStatus(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        SeaTunnelRestClient.JobSnapshot snapshot;
        SeaTunnelRestClient.CheckpointSnapshot checkpoint = SeaTunnelRestClient.CheckpointSnapshot.empty();
        try {
            snapshot = restClient.status(jobId);
        } catch (ServiceException ex) {
            if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
            if (isRecoveryBoundaryError(ex.getMessage())) {
                return markReinitializeRequired(task, jobId, ex.getMessage());
            }
            return markFailed(task, jobId, ex.getMessage());
        }
        String checkpointError = null;
        try {
            checkpoint = restClient.checkpoints(jobId);
        } catch (ServiceException ex) {
            if (isRecoveryBoundaryError(ex.getMessage())) {
                return markReinitializeRequired(task, jobId, ex.getMessage());
            }
            checkpointError = ex.getMessage();
        }
        String platformStatus = mapStatus(snapshot.status());
        if (isKafkaTask(task) && Set.of("STOPPED", "FAILED", "FINISHED").contains(platformStatus)) {
            kafkaTaskBridgeService.stop(taskId);
        }
        if ("FAILED".equals(platformStatus) && isRecoveryBoundaryError(snapshot.errorMessage())) {
            return markReinitializeRequired(task, jobId, snapshot.errorMessage());
        }
        if ("FINISHED".equals(platformStatus) && StringUtils.isNotBlank(task.getOverwriteStageTable())) {
            try {
                finalizeOverwrite(task);
            } catch (Exception ex) {
                return markFailedAfterEngineCompletion(task, jobId, "覆盖刷新替换正式表失败：" + safeMessage(ex));
            }
        }
        task.setStatus(platformStatus);
        String statusError = "FAILED".equals(platformStatus) && StringUtils.isBlank(snapshot.errorMessage())
            ? "SeaTunnel 作业状态为 FAILED" : "";
        String lastError = StringUtils.defaultIfBlank(snapshot.errorMessage(),
            StringUtils.defaultIfBlank(checkpointError, StringUtils.defaultIfBlank(statusError, "")));
        task.setLastError(truncateForColumn(lastError));
        if (checkpoint.id() != null) {
            task.setLastCheckpointId(checkpoint.id());
            task.setLastCheckpointTime(checkpoint.time());
            task.setLastCheckpointStatus(checkpoint.status());
        }
        syncTaskMapper.updateById(task);
        SeaTunnelJobStatus result = new SeaTunnelJobStatus();
        result.setTaskId(taskId);
        result.setEngineJobId(jobId);
        result.setEngineStatus(snapshot.status());
        result.setStatus(platformStatus);
        result.setErrorMessage(StringUtils.isBlank(snapshot.errorMessage())
            ? null : truncateForColumn(snapshot.errorMessage()));
        result.setLastCheckpointId(task.getLastCheckpointId());
        result.setLastCheckpointTime(task.getLastCheckpointTime());
        result.setLastCheckpointStatus(task.getLastCheckpointStatus());
        result.setKafkaPublishedCount(task.getKafkaPublishedCount());
        result.setKafkaLastPartition(task.getKafkaLastPartition());
        result.setKafkaLastOffset(task.getKafkaLastOffset());
        result.setKafkaLastSourceEventTime(task.getKafkaLastSourceEventTime());
        result.setKafkaLastBrokerAckTime(task.getKafkaLastBrokerAckTime());
        result.setKafkaLagSeconds(task.getKafkaLagSeconds());
        applyMetrics(result, snapshot);
        return result;
    }

    private void applyMetrics(SeaTunnelJobStatus result, SeaTunnelRestClient.JobSnapshot snapshot) {
        var metrics = snapshot.metrics();
        String engineStatus = snapshot.status();
        result.setPhase(engineStatus != null && Set.of("INITIALIZING", "CREATED", "PENDING", "STARTING").contains(engineStatus)
            ? "SNAPSHOT" : "CDC");
        result.setSourceReceivedCount(metricLong(metrics, "SourceReceivedCount"));
        result.setSinkCommittedCount(metricLong(metrics, "SinkCommittedCount"));
        result.setSourceReceivedBytes(metricLong(metrics, "SourceReceivedBytes"));
        result.setSinkCommittedBytes(metricLong(metrics, "SinkCommittedBytes"));
        result.setSourceQps(metricDouble(metrics, "SourceReceivedQPS"));
        result.setSinkQps(metricDouble(metrics, "SinkCommittedQPS"));
        Long sourceEvent = metricLong(metrics, "SourceLatestEventTime");
        Long sinkCommit = metricLong(metrics, "SinkLatestCommitTime");
        if (sourceEvent != null && sinkCommit != null && sinkCommit >= sourceEvent) {
            result.setCdcLagSeconds((sinkCommit - sourceEvent) / 1000L);
        } else {
            result.setMetricsMessage("SeaTunnel 未返回源事件时间，CDC 延迟暂不可计算");
        }
    }

    private static Long metricLong(tools.jackson.databind.JsonNode metrics, String key) {
        var node = metrics == null ? null : metrics.get(key);
        if (node == null || !node.canConvertToLong()) return null;
        return node.asLong();
    }

    private static Double metricDouble(tools.jackson.databind.JsonNode metrics, String key) {
        var node = metrics == null ? null : metrics.get(key);
        if (node == null || !node.isNumber()) return null;
        return node.asDouble();
    }

    @Override
    @EventListener(ApplicationReadyEvent.class)
    public void recoverRunningTasks() {
        syncTaskMapper.selectList(new LambdaQueryWrapper<SyncTask>().in(SyncTask::getStatus, "RUNNING", "PAUSING"))
            .forEach(task -> {
                try {
                    if (isKafkaTask(task)) kafkaTaskBridgeService.start(task, requireSource(task.getTargetId(), "目标"),
                        requireSource(task.getSourceId(), "源").getDatabaseName());
                    refreshStatus(task.getTaskId());
                } catch (Exception ex) {
                    markFailed(task, task.getEngineJobId(), ex.getMessage());
                }
            });
    }

    @Override
    public SeaTunnelJobOperationResult pause(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        if (!"RUNNING".equals(task.getStatus()) && !"PAUSING".equals(task.getStatus())) {
            throw new ServiceException("只有运行中的任务可以暂停");
        }
        restClient.stop(jobId, true, false);
        if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
        task.setStatus("PAUSING");
        task.setLastError("");
        syncTaskMapper.updateById(task);
        return operation(task, "暂停请求已提交，请刷新状态确认 savepoint 完成");
    }

    @Override
    public SeaTunnelJobOperationResult resume(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        if (!"PAUSED".equals(task.getStatus()) && !"FAILED".equals(task.getStatus())) {
            throw new ServiceException("只有已暂停或可恢复失败任务可以恢复");
        }
        try {
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
            String configHash = hash(generated.config());
            if (StringUtils.isNotBlank(task.getEngineConfigHash()) && !task.getEngineConfigHash().equals(configHash)) {
                throw new ServiceException("任务配置已变化，不能使用原 checkpoint 恢复，请重新初始化");
            }
            SeaTunnelRestClient.CheckpointSnapshot checkpoint = restClient.checkpoints(jobId);
            if (checkpoint.id() == null) {
                throw new ServiceException("任务没有可用的 checkpoint/savepoint，不能从未知位点恢复，请重新初始化");
            }
            if (isKafkaTask(task)) kafkaTaskBridgeService.start(task, requireSource(task.getTargetId(), "目标"),
                requireSource(task.getSourceId(), "源").getDatabaseName());
            restClient.submit(generated.jobName(), generated.config(), jobId, true);
            task.setStatus("RUNNING");
            task.setLastError("");
            syncTaskMapper.updateById(task);
            return operation(task, "作业已从 savepoint 恢复");
        } catch (ServiceException ex) {
            if (isRecoveryBoundaryError(ex.getMessage())) {
                markReinitializeRequired(task, jobId, ex.getMessage());
                throw new ServiceException(ex.getMessage());
            }
            markFailed(task, jobId, ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
            markFailed(task, jobId, ex.getMessage());
            throw new ServiceException("恢复同步任务失败：" + safeMessage(ex));
        }
    }

    @Override
    public SeaTunnelJobOperationResult stop(Long taskId) {
        SyncTask task = requireTask(taskId);
        String jobId = requireJobId(task);
        if ("STOPPED".equals(task.getStatus()) || "DRAFT".equals(task.getStatus())) {
            throw new ServiceException("任务当前未运行");
        }
        restClient.stop(jobId, false, false);
        if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
        task.setStatus("STOPPED");
        task.setLastError("");
        syncTaskMapper.updateById(task);
        return operation(task, "作业已停止");
    }

    @Override
    public SeaTunnelJobOperationResult reinitialize(Long taskId) {
        SyncTask task = requireTask(taskId);
        if ("INCREMENTAL".equalsIgnoreCase(task.getSyncMode())) {
            throw new ServiceException("纯增量任务没有全量初始化语义，请新建全量 + CDC 任务并先建立目标基线");
        }
        if (!"REINITIALIZE_REQUIRED".equals(task.getStatus()) && !"FAILED".equals(task.getStatus())
            && !"STOPPED".equals(task.getStatus())) {
            throw new ServiceException("只有需重新初始化、失败或已停止任务可以重新初始化");
        }
        prepareResourceProtection(task);
        try {
            if (StringUtils.isNotBlank(task.getEngineJobId())) {
                try {
                    restClient.stop(task.getEngineJobId(), false, true);
                } catch (ServiceException ignored) {
                    // The old engine job may already be gone; clearing its recovery state is still valid.
                }
            }
            var validation = syncTaskService.validate(taskId);
            if (!validation.isValid()) {
                throw new ServiceException("重新初始化前校验未通过：" + validation.getMessage());
            }
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = generate(task);
            if (isKafkaTask(task)) kafkaTaskBridgeService.start(task, requireSource(task.getTargetId(), "目标"),
                requireSource(task.getSourceId(), "源").getDatabaseName());
            SeaTunnelRestClient.SubmitResult submitted = restClient.submit(generated.jobName(), generated.config(), null, false);
            task.setEngineJobId(submitted.jobId());
            task.setEngineConfigHash(hash(generated.config()));
            task.setLastCheckpointId(null);
            task.setLastCheckpointTime(null);
            task.setLastCheckpointStatus(null);
            task.setLastError("");
            task.setStatus("RUNNING");
            syncTaskMapper.updateById(task);
            return operation(task, "已丢弃旧恢复状态并重新启动全量初始化");
        } catch (ServiceException ex) {
            if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
            markFailed(task, task.getEngineJobId(), ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            if (isKafkaTask(task)) kafkaTaskBridgeService.stop(taskId);
            markFailed(task, task.getEngineJobId(), ex.getMessage());
            throw new ServiceException("重新初始化任务失败：" + safeMessage(ex));
        }
    }

    private SeaTunnelJobConfigGenerator.GeneratedConfig generate(SyncTask task) {
        DataSource source = requireSource(task.getSourceId(), "源");
        DataSource target = requireSource(task.getTargetId(), "目标");
        return SeaTunnelJobConfigGenerator.generate(task, source, target, properties);
    }

    private boolean isKafkaTask(SyncTask task) {
        DataSource target = task == null ? null : dataSourceMapper.selectById(task.getTargetId());
        return target != null && "KAFKA".equalsIgnoreCase(target.getSourceType());
    }

    private void prepareResourceProtection(SyncTask task) {
        resourceProtectionPolicy.applyDefaultsAndValidate(task);
        syncTaskMapper.updateById(task);
    }

    private void prepareOverwriteStage(SyncTask task) {
        if ("FULL".equalsIgnoreCase(task.getSyncMode()) && "OVERWRITE".equalsIgnoreCase(task.getFullDataMode())) {
            String schema = StringUtils.defaultIfBlank(task.getTargetSchema(), "public");
            task.setOverwriteStageTable("__ds_stage_" + task.getTaskId() + "_v" + (task.getConfigVersion() == null ? 1 : task.getConfigVersion()));
            task.setLastError("");
            syncTaskMapper.updateById(task);
        } else {
            task.setOverwriteStageTable(null);
        }
    }

    /** Atomically swaps a completed staging table into place on PostgreSQL. */
    private void finalizeOverwrite(SyncTask task) throws SQLException {
        DataSource target = requireSource(task.getTargetId(), "目标");
        String schema = StringUtils.defaultIfBlank(task.getTargetSchema(), "public");
        String targetName = task.getTargetTable();
        String stageName = task.getOverwriteStageTable();
        String backupName = "__ds_backup_" + task.getTaskId() + "_v" + (task.getConfigVersion() == null ? 1 : task.getConfigVersion());
        try (Connection connection = DriverManager.getConnection(postgresJdbcUrl(target), target.getUsername(), target.getPassword())) {
            connection.setAutoCommit(false);
            try {
                if (tableExists(connection, schema, targetName)) {
                    executeRename(connection, schema, targetName, backupName);
                }
                executeRename(connection, schema, stageName, targetName);
                if (tableExists(connection, schema, backupName)) {
                    execute(connection, "drop table " + quote(schema) + "." + quote(backupName));
                }
                connection.commit();
                task.setOverwriteStageTable(null);
                // MyBatis-Plus skips null fields for ordinary updates. Explicitly
                // clear the stage marker so a later status refresh is idempotent.
                syncTaskMapper.update(null, new LambdaUpdateWrapper<SyncTask>()
                    .eq(SyncTask::getTaskId, task.getTaskId())
                    .set(SyncTask::getOverwriteStageTable, null));
            } catch (SQLException ex) {
                connection.rollback();
                throw ex;
            } finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private static boolean tableExists(Connection connection, String schema, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("select 1 from information_schema.tables where table_schema=? and table_name=?")) {
            statement.setString(1, schema); statement.setString(2, table);
            try (ResultSet result = statement.executeQuery()) { return result.next(); }
        }
    }

    private static void executeRename(Connection connection, String schema, String from, String to) throws SQLException {
        execute(connection, "alter table " + quote(schema) + "." + quote(from) + " rename to " + quote(to));
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) { statement.execute(sql); }
    }

    private static String quote(String value) { return "\"" + value.replace("\"", "\"\"") + "\""; }

    private static String postgresJdbcUrl(DataSource source) {
        return "jdbc:postgresql://" + source.getHost() + ':' + source.getPort() + '/' + source.getDatabaseName()
            + "?connectTimeout=5&socketTimeout=5&ssl=" + ("1".equals(source.getSslEnabled()));
    }

    private SyncTask requireTask(Long taskId) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) throw new ServiceException("同步任务不存在");
        return task;
    }

    private static void ensureStartable(SyncTask task) {
        if ("RUNNING".equals(task.getStatus()) || "PAUSING".equals(task.getStatus())) {
            throw new ServiceException("任务当前正在运行");
        }
        if ("FAILED".equals(task.getStatus()) || "REINITIALIZE_REQUIRED".equals(task.getStatus())) {
            throw new ServiceException("任务存在恢复风险，请使用恢复任务或重新初始化");
        }
    }

    private static String requireJobId(SyncTask task) {
        if (StringUtils.isBlank(task.getEngineJobId())) throw new ServiceException("任务尚未提交 SeaTunnel 作业");
        return task.getEngineJobId();
    }

    private static String mapStatus(String engineStatus) {
        if (engineStatus == null) return "FAILED";
        return switch (engineStatus.toUpperCase()) {
            case "RUNNING", "STARTING", "INITIALIZING", "CREATED", "PENDING", "RESTARTING" -> "RUNNING";
            case "DOING_SAVEPOINT" -> "PAUSING";
            case "SAVEPOINT_DONE" -> "PAUSED";
            case "CANCELED", "CANCELLED" -> "STOPPED";
            case "FINISHED" -> "FINISHED";
            case "FAILED" -> "FAILED";
            default -> "FAILED";
        };
    }

    private static String hash(String config) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(config.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) result.append(String.format("%02x", value));
            return result.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 不可用", ex);
        }
    }

    private SeaTunnelJobStatus markFailed(SyncTask task, String jobId, String error) {
        String safeError = StringUtils.isBlank(error) ? "SeaTunnel 作业状态不可用" : error;
        task.setStatus("FAILED");
        task.setLastError(truncateForColumn(safeError));
        syncTaskMapper.updateById(task);
        SeaTunnelJobStatus result = new SeaTunnelJobStatus();
        result.setTaskId(task.getTaskId());
        result.setEngineJobId(jobId);
        result.setEngineStatus("UNREACHABLE");
        result.setStatus("FAILED");
        result.setErrorMessage(task.getLastError());
        result.setLastCheckpointId(task.getLastCheckpointId());
        result.setLastCheckpointTime(task.getLastCheckpointTime());
        result.setLastCheckpointStatus(task.getLastCheckpointStatus());
        return result;
    }

    /** Keep the terminal engine state visible when post-processing fails. */
    private SeaTunnelJobStatus markFailedAfterEngineCompletion(SyncTask task, String jobId, String error) {
        task.setStatus("FAILED");
        task.setLastError(truncateForColumn(error));
        syncTaskMapper.updateById(task);
        SeaTunnelJobStatus result = new SeaTunnelJobStatus();
        result.setTaskId(task.getTaskId());
        result.setEngineJobId(jobId);
        result.setEngineStatus("FINISHED");
        result.setStatus("FAILED");
        result.setErrorMessage(task.getLastError());
        result.setPhase("SNAPSHOT");
        result.setLastCheckpointId(task.getLastCheckpointId());
        result.setLastCheckpointTime(task.getLastCheckpointTime());
        result.setLastCheckpointStatus(task.getLastCheckpointStatus());
        return result;
    }

    private SeaTunnelJobStatus markReinitializeRequired(SyncTask task, String jobId, String error) {
        String safeError = StringUtils.isBlank(error)
            ? "checkpoint/savepoint 或 binlog 位点不可恢复，请重新初始化"
            : error;
        task.setStatus("REINITIALIZE_REQUIRED");
        task.setLastError(truncateForColumn(safeError));
        syncTaskMapper.updateById(task);
        SeaTunnelJobStatus result = new SeaTunnelJobStatus();
        result.setTaskId(task.getTaskId());
        result.setEngineJobId(jobId);
        result.setEngineStatus("RECOVERY_REQUIRED");
        result.setStatus("REINITIALIZE_REQUIRED");
        result.setErrorMessage(task.getLastError());
        result.setLastCheckpointId(task.getLastCheckpointId());
        result.setLastCheckpointTime(task.getLastCheckpointTime());
        result.setLastCheckpointStatus(task.getLastCheckpointStatus());
        return result;
    }

    private static boolean isRecoveryBoundaryError(String error) {
        if (StringUtils.isBlank(error)) return false;
        String normalized = error.toLowerCase(java.util.Locale.ROOT);
        return normalized.contains("checkpoint")
            || normalized.contains("savepoint")
            || normalized.contains("binlog")
            || normalized.contains("offset")
            || normalized.contains("restore")
            || normalized.contains("恢复")
            || normalized.contains("位点")
            || normalized.contains("日志已过期");
    }

    private static String safeMessage(Exception ex) {
        String message = ex == null ? null : ex.getMessage();
        return StringUtils.isBlank(message) ? "未知错误" : message;
    }

    /** Keep multibyte database error text within the varchar(2000) byte limit. */
    private static String truncateForColumn(String value) {
        if (value == null) return null;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 1800) return value;
        int end = 1800;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) end--;
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }

    private static SeaTunnelJobOperationResult operation(SyncTask task, String message) {
        SeaTunnelJobOperationResult result = new SeaTunnelJobOperationResult();
        result.setTaskId(task.getTaskId());
        result.setEngineJobId(task.getEngineJobId());
        result.setStatus(task.getStatus());
        result.setMessage(message);
        return result;
    }

    private DataSource requireSource(Long sourceId, String side) {
        if (sourceId == null) {
            throw new ServiceException(side + "数据源不能为空");
        }
        DataSource source = dataSourceMapper.selectById(sourceId);
        if (source == null) {
            throw new ServiceException(side + "数据源不存在");
        }
        if (StringUtils.isBlank(source.getPassword())) {
            throw new ServiceException(side + "数据源密码未配置");
        }
        return source;
    }
}
