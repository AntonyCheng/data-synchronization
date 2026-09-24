package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.MapstructUtils;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.common.json.utils.JsonUtils;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskConfigVersion;
import org.dromara.sync.domain.bo.SyncTaskBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncMetricsSampleVo;
import org.dromara.sync.domain.vo.SyncTaskValidationResult;
import org.dromara.sync.domain.vo.SyncTaskVo;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskConfigVersionMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.service.ISyncTaskService;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.TableNames;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Synchronization task service implementation.
 */
@RequiredArgsConstructor
@Service
public class SyncTaskServiceImpl implements ISyncTaskService {

    private static final Set<String> EDITABLE_STATUSES = Set.of(SyncStatus.DRAFT, SyncStatus.STOPPED, SyncStatus.REINITIALIZE_REQUIRED);
    private static final Set<String> DELETABLE_STATUSES = Set.of(SyncStatus.DRAFT, SyncStatus.STOPPED, SyncStatus.FAILED,
        SyncStatus.FINISHED, SyncStatus.REINITIALIZE_REQUIRED);
    private static final Set<String> FULL_DATA_MODES = Set.of("UPSERT", "OVERWRITE");
    private static final Set<String> INCREMENTAL_STARTUP_MODES = Set.of("LATEST", "TIMESTAMP", "SPECIFIC");
    private static final Set<String> SCHEDULE_MODES = Set.of("MANUAL", "ONCE", "CRON", "REALTIME");

    private final SyncTaskMapper syncTaskMapper;
    private final SyncTaskConfigVersionMapper configVersionMapper;
    private final DataSourceMapper dataSourceMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final ResourceProtectionPolicy resourceProtectionPolicy;
    private final ISyncMetricsService metricsService;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;
    private final SyncLocks locks;
    private final TransactionTemplate transactionTemplate;

    @Override
    public PageResult<SyncTaskVo> queryPageList(SyncTaskBo bo, PageQuery pageQuery) {
        LambdaQueryWrapper<SyncTask> wrapper = new LambdaQueryWrapper<>();
        wrapper.like(StringUtils.isNotBlank(bo.getTaskName()), SyncTask::getTaskName, bo.getTaskName())
            .eq(bo.getSourceId() != null, SyncTask::getSourceId, bo.getSourceId())
            .eq(bo.getTargetId() != null, SyncTask::getTargetId, bo.getTargetId())
            .eq(StringUtils.isNotBlank(bo.getStatus()), SyncTask::getStatus, bo.getStatus())
            .orderByDesc(SyncTask::getTaskId);
        Page<SyncTaskVo> page = syncTaskMapper.selectVoPage(pageQuery.build(), wrapper);
        attachRuntimeState(page.getRecords());
        return PageResult.build(page.getRecords(), page.getTotal());
    }

    @Override
    public SyncTaskVo queryById(Long taskId) {
        SyncTaskVo task = syncTaskMapper.selectVoById(taskId);
        if (task != null) attachRuntimeState(List.of(task));
        return task;
    }

    /** Latest engine metrics, and a notice on a RUNNING Kafka task this instance holds without a bridge. */
    private void attachRuntimeState(List<SyncTaskVo> tasks) {
        Map<Long, SyncMetricsSampleVo> latest = metricsService.latestForTasks(tasks.stream().map(SyncTaskVo::getTaskId).toList());
        tasks.forEach(task -> {
            task.setLatestMetrics(latest.get(task.getTaskId()));
            task.setLastError(kafkaTaskBridgeService.decorateLastError(task.getTaskId(), task.getLastError()));
        });
    }

    @Override
    @Transactional
    public Boolean insertByBo(SyncTaskBo bo) {
        SyncTask entity = MapstructUtils.convert(bo, SyncTask.class);
        normalizeAndValidate(entity);
        entity.setStatus(SyncStatus.DRAFT);
        entity.setConfigVersion(1);
        initializeSchedule(entity);
        boolean inserted = syncTaskMapper.insert(entity) > 0;
        if (inserted) persistConfigVersion(entity);
        return inserted;
    }

    /**
     * Taken under the task's lifecycle lock, with the transaction inside it: a start that had
     * already read the task as STOPPED must not go on to submit the config this edit replaces.
     */
    @Override
    public Boolean updateByBo(SyncTaskBo bo) {
        return inLockedTransaction(bo.getTaskId(), () -> doUpdate(bo));
    }

    private Boolean doUpdate(SyncTaskBo bo) {
        SyncTask current = syncTaskMapper.selectById(bo.getTaskId());
        if (current == null) throw new ServiceException("同步任务不存在");
        if (current.getStatus() == null || !EDITABLE_STATUSES.contains(current.getStatus())) {
            throw new ServiceException("只有草稿或已停止任务允许修改");
        }
        SyncTask entity = MapstructUtils.convert(bo, SyncTask.class);
        normalizeAndValidate(entity);
        entity.setStatus(current.getStatus());
        entity.setConfigVersion((current.getConfigVersion() == null ? 1 : current.getConfigVersion()) + 1);
        initializeSchedule(entity);
        boolean updated = syncTaskMapper.updateById(entity) > 0;
        if (updated) persistConfigVersion(entity);
        return updated;
    }

    /**
     * Locked like an edit: a start racing a delete would submit a {@code ds-task-<id>} job whose
     * row is already gone - a job nothing, not even the orphan sweeper, can map back to an owner.
     */
    @Override
    public Boolean deleteById(Long taskId) {
        return inLockedTransaction(taskId, () -> doDelete(taskId));
    }

    private Boolean doDelete(Long taskId) {
        SyncTask current = syncTaskMapper.selectById(taskId);
        if (current == null) return false;
        String status = StringUtils.defaultIfBlank(current.getStatus(), "").trim().toUpperCase(Locale.ROOT);
        if (!DELETABLE_STATUSES.contains(status)) {
            throw new ServiceException("运行中的任务不能删除");
        }
        boolean deleted = syncTaskMapper.deleteById(taskId) > 0;
        if (deleted) {
            configVersionMapper.deleteByTaskId(taskId);
            metricsService.deleteForTask(taskId);
        }
        return deleted;
    }

    /** Lock first, transaction inside, so the change is committed before the lock is released. */
    private <T> T inLockedTransaction(Long taskId, Supplier<T> action) {
        return locks.withTaskLock(taskId, () -> transactionTemplate.execute(status -> action.get()));
    }

    @Override
    public SyncTaskValidationResult validate(Long taskId) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) throw new ServiceException("同步任务不存在");
        DataSource source = dataSourceMapper.selectById(task.getSourceId());
        DataSource target = dataSourceMapper.selectById(task.getTargetId());
        SyncTaskValidationResult result = new SyncTaskValidationResult();
        result.setSource(source == null ? ConnectionTestResult.failure("源数据源不存在") : dataSourceService.testConnection(source.getSourceId(), null));
        result.setTarget(target == null ? ConnectionTestResult.failure("目标数据源不存在") : dataSourceService.testConnection(target.getSourceId(), null));
        if (source != null && target != null && result.getSource().isSuccess() && result.getTarget().isSuccess()) {
            if (!SyncMode.isFull(task.getSyncMode())) {
                result.setCdcPrecheck(metadataService.checkMysqlCdc(source.getSourceId()));
            }
            result.setTargetCompatibility(metadataService.checkTargetCompatibility(taskId));
        }
        boolean cdcPassed = result.getCdcPrecheck() == null || Boolean.TRUE.equals(result.getCdcPrecheck().getPassed());
        boolean targetPassed = result.getTargetCompatibility() == null || result.getTargetCompatibility().isPassed();
        result.setValid(result.getSource().isSuccess() && result.getTarget().isSuccess() && cdcPassed && targetPassed);
        result.setMessage(result.isValid() ? "源端、目标端、CDC 前置条件和目标表兼容性校验通过" : validationFailureMessage(result));
        return result;
    }

    private String validationFailureMessage(SyncTaskValidationResult result) {
        if (!result.getSource().isSuccess() || !result.getTarget().isSuccess()) return "连接校验未通过，请检查数据源配置";
        if (result.getCdcPrecheck() != null && !Boolean.TRUE.equals(result.getCdcPrecheck().getPassed())) return result.getCdcPrecheck().getMessage();
        if (result.getTargetCompatibility() != null && !result.getTargetCompatibility().isPassed()) return result.getTargetCompatibility().getMessage();
        return "任务校验未通过，请检查任务配置";
    }

    private void normalizeAndValidate(SyncTask entity) {
        if (entity == null || entity.getSourceId().equals(entity.getTargetId())) {
            throw new ServiceException("源数据源和目标数据源不能相同");
        }
        DataSource source = dataSourceMapper.selectById(entity.getSourceId());
        DataSource target = dataSourceMapper.selectById(entity.getTargetId());
        if (source == null || target == null) throw new ServiceException("源端或目标端数据源不存在");
        if (!DataSourceType.isMysql(source)) throw new ServiceException("MVP 源端必须是 MySQL");
        if (!DataSourceType.isSupportedTarget(target.getSourceType())) {
            throw new ServiceException("当前任务目标端必须是 PostgreSQL、MySQL 或 Kafka");
        }
        boolean kafkaTarget = DataSourceType.isKafka(target);
        if (DataSourceType.isPostgres(target) && StringUtils.isBlank(entity.getTargetSchema())) {
            entity.setTargetSchema(TableNames.DEFAULT_POSTGRES_SCHEMA);
        }
        if (kafkaTarget) entity.setTargetSchema(null);
        entity.setSyncMode(SyncMode.normalize(entity.getSyncMode()));
        if (kafkaTarget) {
            entity.setFullDataMode(null);
            entity.setKafkaOutputFormat(KafkaOutputFormat.parse(entity.getKafkaOutputFormat()).name());
        } else {
            entity.setKafkaOutputFormat(null);
            normalizeFullDataMode(entity);
        }
        normalizeIncrementalStartup(entity);
        if (StringUtils.isBlank(entity.getDdlPolicy())) entity.setDdlPolicy("FAIL");
        DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(), source.getDatabaseName(),
            TableNames.unqualified(entity.getSourceTable()));
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(metadata,
            entity.getSelectedColumns(), entity.getSyncKeyColumns());
        entity.setSelectedColumns(SyncColumnSelectionValidator.serialize(selection.selectedColumns()));
        entity.setSyncKeyColumns(SyncColumnSelectionValidator.serialize(selection.syncKeyColumns()));
        if ((kafkaTarget || !SyncMode.isFull(entity.getSyncMode())) && selection.syncKeyColumns().isEmpty()) {
            throw new ServiceException("源表没有可靠同步键，只能创建全量任务");
        }
        resourceProtectionPolicy.applyDefaultsAndValidate(entity);
        normalizeSchedule(entity);
    }

    /** Relational targets only: UPSERT (default) or the FULL-only atomic OVERWRITE. */
    private static void normalizeFullDataMode(SyncTask entity) {
        if (StringUtils.isBlank(entity.getFullDataMode())) entity.setFullDataMode("UPSERT");
        if (!FULL_DATA_MODES.contains(entity.getFullDataMode().toUpperCase(Locale.ROOT))) {
            throw new ServiceException("不支持的全量目标数据模式：" + entity.getFullDataMode());
        }
        if (!"OVERWRITE".equalsIgnoreCase(entity.getFullDataMode())) return;
        if (SyncMode.INCREMENTAL.equalsIgnoreCase(entity.getSyncMode())) {
            throw new ServiceException("纯增量任务不能使用覆盖刷新模式");
        }
        if (SyncMode.FULL_CDC.equalsIgnoreCase(entity.getSyncMode())) {
            throw new ServiceException("全量 + CDC 的覆盖刷新需要一致性切换水位，当前 MVP 仅支持全量任务使用原子覆盖；请改用合并（upsert）或创建纯全量任务");
        }
    }

    private void initializeSchedule(SyncTask entity) {
        if ("ONCE".equalsIgnoreCase(entity.getScheduleMode())) {
            entity.setNextRunTime(entity.getNextRunTime() == null ? LocalDateTime.now() : entity.getNextRunTime());
        } else if (!"CRON".equalsIgnoreCase(entity.getScheduleMode())) {
            entity.setNextRunTime(null);
        }
    }

    private void normalizeIncrementalStartup(SyncTask entity) {
        if (!SyncMode.INCREMENTAL.equalsIgnoreCase(entity.getSyncMode())) {
            entity.setIncrementalStartupMode("LATEST");
            entity.setIncrementalStartupTimestamp(null);
            entity.setIncrementalStartupBinlogFile(null);
            entity.setIncrementalStartupBinlogPosition(null);
            return;
        }

        String mode = StringUtils.defaultIfBlank(entity.getIncrementalStartupMode(), "LATEST").toUpperCase(Locale.ROOT);
        if (!INCREMENTAL_STARTUP_MODES.contains(mode)) {
            throw new ServiceException("不支持的纯增量启动位点策略：" + mode);
        }
        entity.setIncrementalStartupMode(mode);
        if ("TIMESTAMP".equals(mode)) {
            if (entity.getIncrementalStartupTimestamp() == null) {
                throw new ServiceException("按时间启动纯增量任务必须填写启动时间");
            }
            if (entity.getIncrementalStartupTimestamp().isAfter(LocalDateTime.now())) {
                throw new ServiceException("纯增量启动时间不能晚于当前时间");
            }
            entity.setIncrementalStartupBinlogFile(null);
            entity.setIncrementalStartupBinlogPosition(null);
            return;
        }
        if ("SPECIFIC".equals(mode)) {
            String file = entity.getIncrementalStartupBinlogFile();
            Long position = entity.getIncrementalStartupBinlogPosition();
            if (StringUtils.isBlank(file) || !file.matches("[A-Za-z0-9._-]+")) {
                throw new ServiceException("指定 binlog 文件名格式无效");
            }
            if (position == null || position < 4) {
                throw new ServiceException("指定 binlog 位置必须大于或等于 4");
            }
            entity.setIncrementalStartupTimestamp(null);
            return;
        }
        entity.setIncrementalStartupTimestamp(null);
        entity.setIncrementalStartupBinlogFile(null);
        entity.setIncrementalStartupBinlogPosition(null);
    }

    private void normalizeSchedule(SyncTask entity) {
        String mode = StringUtils.defaultIfBlank(entity.getScheduleMode(), "ONCE").toUpperCase(Locale.ROOT);
        if (!SCHEDULE_MODES.contains(mode)) {
            throw new ServiceException("不支持的调度模式：" + mode);
        }
        entity.setScheduleMode(mode);
        if ("CRON".equals(mode)) {
            if (StringUtils.isBlank(entity.getCronExpression())) throw new ServiceException("Cron 调度必须填写表达式");
            CronExpression cron;
            try {
                cron = CronExpression.parse(entity.getCronExpression());
            } catch (IllegalArgumentException ex) {
                throw new ServiceException("Cron 表达式无效：" + ex.getMessage());
            }
            entity.setNextRunTime(cron.next(LocalDateTime.now()));
        } else if (!"ONCE".equals(mode)) {
            entity.setCronExpression(null);
            entity.setNextRunTime(null);
        }
    }

    /**
     * Snapshot of the semantic (checkpoint-relevant) configuration, deliberately excluding
     * status, engine ids and credentials. A checkpoint is bound to the version that produced it.
     */
    private void persistConfigVersion(SyncTask task) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("taskName", task.getTaskName());
        snapshot.put("sourceId", task.getSourceId());
        snapshot.put("targetId", task.getTargetId());
        snapshot.put("sourceTable", task.getSourceTable());
        snapshot.put("targetSchema", task.getTargetSchema());
        snapshot.put("targetTable", task.getTargetTable());
        snapshot.put("syncMode", task.getSyncMode());
        snapshot.put("incrementalStartupMode", task.getIncrementalStartupMode());
        snapshot.put("incrementalStartupTimestamp", task.getIncrementalStartupTimestamp());
        snapshot.put("incrementalStartupBinlogFile", task.getIncrementalStartupBinlogFile());
        snapshot.put("incrementalStartupBinlogPosition", task.getIncrementalStartupBinlogPosition());
        snapshot.put("fullDataMode", task.getFullDataMode());
        snapshot.put("ddlPolicy", task.getDdlPolicy());
        snapshot.put("selectedColumns", task.getSelectedColumns());
        snapshot.put("syncKeyColumns", task.getSyncKeyColumns());
        snapshot.put("kafkaOutputFormat", task.getKafkaOutputFormat());
        snapshot.put("scheduleMode", task.getScheduleMode());
        snapshot.put("cronExpression", task.getCronExpression());
        snapshot.put("readLimitRowsPerSecond", task.getReadLimitRowsPerSecond());
        snapshot.put("readLimitBytesPerSecond", task.getReadLimitBytesPerSecond());
        snapshot.put("snapshotParallelism", task.getSnapshotParallelism());
        snapshot.put("sourceConnectionLimit", task.getSourceConnectionLimit());
        SyncTaskConfigVersion version = new SyncTaskConfigVersion();
        version.setTaskId(task.getTaskId());
        version.setConfigVersion(task.getConfigVersion());
        version.setConfigSnapshot(JsonUtils.toJsonString(snapshot));
        configVersionMapper.insert(version);
    }
}
