package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.MapstructUtils;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.bo.SyncTaskGroupBo;
import org.dromara.sync.domain.bo.SyncTaskGroupItemBo;
import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncMetricsSampleVo;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupConfigPreview;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckItemResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupItemStatus;
import org.dromara.sync.domain.vo.SyncTaskGroupItemValidationVo;
import org.dromara.sync.domain.vo.SyncTaskGroupItemVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;
import org.dromara.sync.domain.vo.SyncTaskGroupValidationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.EngineJobStates;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.engine.SourceColumns;
import org.dromara.sync.engine.SyncTaskGroupConfigGenerator;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.SyncTaskGroupDdlEventMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.service.ISyncTaskGroupService;
import org.dromara.sync.support.GroupStatuses;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.dromara.sync.support.TableSchemaSnapshot;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Multi-table / whole-database release lifecycle. One SeaTunnel job per table item; the
 * group status is an aggregate of its items (see {@link GroupStatuses#aggregate}). DDL drift
 * handling lives in {@link SyncTaskGroupDdlServiceImpl}.
 */
@RequiredArgsConstructor
@Service
public class SyncTaskGroupServiceImpl implements ISyncTaskGroupService {

    private static final String SCOPE_MULTI_TABLE = "MULTI_TABLE";
    private static final String SCOPE_DATABASE = "DATABASE";
    private static final int MAX_TABLES_PER_GROUP = 20;
    private static final String DEFAULT_DDL_POLICY = "FAIL";

    private static final Set<String> EDITABLE_STATUSES = Set.of(SyncStatus.DRAFT, SyncStatus.STOPPED);
    private static final Set<String> DELETABLE_STATUSES = Set.of(SyncStatus.DRAFT, SyncStatus.STOPPED, SyncStatus.FAILED, SyncStatus.FINISHED);
    private static final Set<String> RESUMABLE_STATUSES = Set.of(SyncStatus.PAUSED, SyncStatus.FAILED);
    /** Item states a single table may be rebuilt from without touching its siblings. */
    private static final Set<String> REINITIALIZABLE_ITEM_STATUSES = Set.of(SyncStatus.FAILED, SyncStatus.DDL_BLOCKED, SyncStatus.STOPPED, SyncStatus.FINISHED);
    /** Group states in which table jobs are live on the engine. */
    private static final Set<String> LIVE_STATUSES = Set.of(SyncStatus.RUNNING, SyncStatus.DEGRADED);

    /** Consecutive engine status-poll failures tolerated per item; see the task-side counterpart. */
    private static final int ITEM_STATUS_FAILURE_TOLERANCE = 3;

    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SyncTaskGroupDdlEventMapper ddlEventMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final IDataConsistencyService dataConsistencyService;
    private final SeaTunnelProperties properties;
    private final SeaTunnelRestClient restClient;
    private final ResourceProtectionPolicy resourceProtectionPolicy;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;
    private final ISyncMetricsService metricsService;
    private final SyncLocks locks;

    private final ConcurrentMap<Long, Integer> itemStatusFailureStreak = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------ CRUD

    @Override
    public PageResult<SyncTaskGroupVo> queryPageList(SyncTaskGroupBo bo, PageQuery pageQuery) {
        LambdaQueryWrapper<SyncTaskGroup> wrapper = new LambdaQueryWrapper<SyncTaskGroup>()
            .like(StringUtils.isNotBlank(bo.getGroupName()), SyncTaskGroup::getGroupName, bo.getGroupName())
            .eq(StringUtils.isNotBlank(bo.getStatus()), SyncTaskGroup::getStatus, bo.getStatus())
            .orderByDesc(SyncTaskGroup::getGroupId);
        Page<SyncTaskGroupVo> page = groupMapper.selectVoPage(pageQuery.build(), wrapper);
        page.getRecords().forEach(this::attachItems);
        return PageResult.build(page.getRecords(), page.getTotal());
    }

    @Override
    public SyncTaskGroupVo queryById(Long groupId) {
        SyncTaskGroupVo result = groupMapper.selectVoById(groupId);
        if (result != null) attachItems(result);
        return result;
    }

    @Override
    @Transactional
    public Boolean insertByBo(SyncTaskGroupBo bo) {
        SyncTaskGroup entity = normalize(bo, null);
        entity.setStatus(SyncStatus.DRAFT);
        entity.setConfigVersion(1);
        groupMapper.insert(entity);
        replaceItems(entity, bo.getItems());
        if (isDatabaseScope(entity)) discover(entity.getGroupId());
        return true;
    }

    @Override
    @Transactional
    public Boolean updateByBo(SyncTaskGroupBo bo) {
        SyncTaskGroup current = requireGroup(bo.getGroupId());
        if (current.getStatus() == null || !EDITABLE_STATUSES.contains(current.getStatus())) {
            throw new ServiceException("只有草稿或已停止任务组允许修改");
        }
        SyncTaskGroup entity = normalize(bo, current);
        entity.setConfigVersion(nextConfigVersion(current));
        groupMapper.updateById(entity);
        replaceItems(entity, bo.getItems());
        if (isDatabaseScope(entity)) discover(entity.getGroupId());
        return true;
    }

    @Override
    @Transactional
    public Boolean deleteById(Long groupId) {
        SyncTaskGroup current = requireGroup(groupId);
        if (current.getStatus() == null || !DELETABLE_STATUSES.contains(current.getStatus())) {
            throw new ServiceException("运行中的任务组不能删除");
        }
        forgetFailureStreaks(groupId);
        itemMapper.deleteByGroupId(groupId);
        metricsService.deleteForGroup(groupId);
        return groupMapper.deleteById(groupId) > 0;
    }

    // ------------------------------------------------------------------ validation & preview

    @Override
    public SyncTaskGroupValidationResult validate(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        SyncTaskGroupValidationResult result = new SyncTaskGroupValidationResult();
        result.setGroupId(groupId);
        result.setSource(dataSourceService.testConnection(source.getSourceId(), null));
        result.setTarget(dataSourceService.testConnection(target.getSourceId(), null));
        if (!result.getSource().isSuccess() || !result.getTarget().isSuccess()) {
            result.setMessage("源端或目标端连接校验未通过");
            return result;
        }
        DataSourceCdcPrecheckVo cdc = metadataService.checkMysqlCdc(source.getSourceId());
        result.setCdcPrecheck(cdc);
        boolean valid = Boolean.TRUE.equals(cdc.getPassed());
        boolean keyRequired = !SyncMode.isFull(group.getSyncMode()) || DataSourceType.isKafka(target);
        for (SyncTaskGroupItem item : items(groupId)) {
            SyncTaskGroupItemValidationVo itemResult = new SyncTaskGroupItemValidationVo();
            itemResult.setItemId(item.getItemId());
            itemResult.setSourceTable(item.getSourceTable());
            itemResult.setTargetTable(TableNames.display(target, item.getTargetSchema(), item.getTargetTable()));
            try {
                DataSourceMetadataVo metadata = readSourceMetadata(item, source);
                boolean hasKey = !SyncColumnSelectionValidator.validate(metadata,
                    item.getSelectedColumns(), item.getSyncKeyColumns()).syncKeyColumns().isEmpty();
                TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                    item.getSourceTable(), item.getTargetSchema(), item.getTargetTable(), item.getSelectedColumns(), item.getSyncKeyColumns());
                itemResult.setTargetCompatibility(compatibility);
                itemResult.setPassed((!keyRequired || hasKey) && compatibility.isPassed());
                itemResult.setMessage(itemResult.isPassed() ? "表结构和同步键校验通过"
                    : (keyRequired && !hasKey ? "源表没有可用同步键" : compatibility.getMessage()));
            } catch (RuntimeException ex) {
                itemResult.setPassed(false);
                itemResult.setMessage(ex.getMessage());
            }
            result.getItems().add(itemResult);
            valid &= itemResult.isPassed();
        }
        result.setValid(valid);
        result.setMessage(valid ? "任务组连接、CDC 和全部表兼容性校验通过" : "任务组存在必须修复的表级问题");
        return result;
    }

    @Override
    public SyncTaskGroupConfigPreview previewConfig(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        List<SyncTaskGroupItem> groupItems = items(groupId);
        SyncTaskGroupConfigGenerator.GeneratedConfig generated =
            SyncTaskGroupConfigGenerator.generate(group, groupItems, source, target, properties, sourceColumns(source));
        SyncTaskGroupConfigPreview result = new SyncTaskGroupConfigPreview();
        result.setGroupId(groupId);
        result.setGroupName(group.getGroupName());
        result.setConfigVersion(group.getConfigVersion());
        result.setEngineJobName(generated.jobName());
        result.setTableCount(groupItems.size());
        result.setConfig(generated.redactedConfig());
        return result;
    }

    // ------------------------------------------------------------------ lifecycle

    /**
     * A refused start (validation) is not rolled back on purpose: by then the only writes
     * are the resource-protection defaults, recovered Kafka topics and the re-derived column
     * selections - all true statements about the group that the operator needs to see
     * (e.g. "the target lacks the column the source just gained") when they open validate.
     */
    @Override
    @Transactional(noRollbackFor = ServiceException.class)
    public SyncTaskGroupOperationResult start(Long groupId) {
        return locks.withGroupLock(groupId, () -> doStart(groupId));
    }

    private SyncTaskGroupOperationResult doStart(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (SyncStatus.isActive(group.getStatus())) throw new ServiceException("任务组当前正在运行");
        if (SyncStatus.PAUSED.equals(group.getStatus())) {
            throw new ServiceException("任务组处于暂停状态，请使用恢复任务组从 savepoint 继续；如需重新全量同步，请先停止任务组");
        }
        resourceProtectionPolicy.applyDefaultsAndValidate(group);
        groupMapper.updateById(group);
        boolean databaseScope = isDatabaseScope(group);
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        if (databaseScope && DataSourceType.isKafka(target)) recoverDatabaseKafkaTopics(groupId, source, target);
        // Tables that were selected in full keep following the source: a column added since
        // the last baseline joins the projection here, so the validation below checks the
        // target against what will actually be synced. An explicit subset stays as it is.
        Map<Long, DataSourceMetadataVo> liveSchemas = new HashMap<>();
        for (SyncTaskGroupItem item : items(groupId)) {
            if (hasLiveJob(item)) continue;
            try {
                DataSourceMetadataVo metadata = readSourceMetadata(item, source);
                liveSchemas.put(item.getItemId(), metadata);
                if (followSourceColumns(item, metadata)) itemMapper.updateById(item);
            } catch (RuntimeException ignored) {
                // Unreadable source table - validate() reports it per table below.
            }
        }
        SyncTaskGroupValidationResult validation = validate(groupId);
        boolean infrastructureValid = validation.getSource() != null && validation.getSource().isSuccess()
            && validation.getTarget() != null && validation.getTarget().isSuccess()
            && validation.getCdcPrecheck() != null && Boolean.TRUE.equals(validation.getCdcPrecheck().getPassed());
        // A whole-database group starts as long as the infrastructure is sound; per-table
        // failures are isolated below. A multi-table group must be fully valid.
        if (!validation.isValid() && (!databaseScope || !infrastructureValid)) {
            throw new ServiceException("启动前校验未通过：" + describeFailures(validation));
        }
        List<String> jobIds = new ArrayList<>();
        List<SyncTaskGroupItem> submittedItems = new ArrayList<>();
        SyncTaskGroupItem currentItem = null;
        int isolatedFailures = 0;
        try {
            for (SyncTaskGroupItem item : items(groupId)) {
                if (hasLiveJob(item)) {
                    // Already has a live engine job (a DEGRADED group, or a table resumed /
                    // reinitialized on its own) - keep it rather than submitting a duplicate.
                    jobIds.add(item.getEngineJobId());
                    continue;
                }
                if (databaseScope && GroupStatuses.isIsolated(item.getStatus())) {
                    isolatedFailures++;
                    continue;
                }
                currentItem = item;
                try {
                    DataSourceMetadataVo metadata = liveSchemas.get(item.getItemId());
                    TableSchemaSnapshot.baseline(item, metadata != null ? metadata : readSourceMetadata(item, source));
                    jobIds.add(submitItem(group, item, source, target));
                    submittedItems.add(item);
                    // A fresh job on a fresh baseline supersedes any drift event still open on this
                    // table; leaving it open would make the next status refresh flag the running
                    // table DDL_BLOCKED again.
                    ddlEventMapper.resolveOpen(item.getItemId(), "已通过重新启动任务组处理。");
                } catch (RuntimeException ex) {
                    if (!databaseScope) throw ex;
                    isolateItem(item, ex.getMessage());
                    isolatedFailures++;
                }
            }
            group.setEngineJobId(String.join(",", jobIds));
            group.setEngineConfigHash(SyncText.sha256Hex(String.join("\n", jobIds)));
            group.setStatus(databaseScope && isolatedFailures > 0 ? SyncStatus.DEGRADED : SyncStatus.RUNNING);
            group.setLastError(isolatedFailures == 0 ? "" : "已隔离 " + isolatedFailures + " 张失败表，其他表继续运行");
            groupMapper.updateById(group);
            return SyncTaskGroupOperationResult.of(group, databaseScope
                ? "整库任务已提交 " + submittedItems.size() + " 个作业，隔离失败 " + isolatedFailures + " 张"
                : "任务组已提交 " + submittedItems.size() + " 个作业");
        } catch (RuntimeException ex) {
            String error = StringUtils.defaultIfBlank(ex.getMessage(), "SeaTunnel 作业提交失败");
            for (SyncTaskGroupItem submittedItem : submittedItems) {
                try {
                    restClient.stop(submittedItem.getEngineJobId(), false, false);
                    if (DataSourceType.isKafka(target)) kafkaTaskBridgeService.stop(submittedItem.getItemId());
                    submittedItem.setStatus(SyncStatus.STOPPED);
                } catch (RuntimeException stopError) {
                    submittedItem.setStatus(SyncStatus.FAILED);
                    error = error + "; 补偿停止失败: " + StringUtils.defaultIfBlank(stopError.getMessage(), "引擎不可达");
                }
                submittedItem.setLastError(SyncText.truncateForColumn("组启动失败，已执行补偿停止"));
                itemMapper.updateById(submittedItem);
            }
            if (currentItem != null && !submittedItems.contains(currentItem)) {
                isolateItem(currentItem, error);
            }
            group.setEngineJobId(String.join(",", jobIds));
            group.setStatus(SyncStatus.FAILED);
            group.setLastError(SyncText.truncateForColumn(error));
            groupMapper.updateById(group);
            return SyncTaskGroupOperationResult.of(group, "任务组启动失败，已补偿停止已提交作业：" + error);
        }
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult discover(Long groupId) {
        return locks.withGroupLock(groupId, () -> doDiscover(groupId));
    }

    private SyncTaskGroupOperationResult doDiscover(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!isDatabaseScope(group)) {
            throw new ServiceException("仅整库同步任务组支持发现新增表");
        }
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        String database = StringUtils.defaultIfBlank(group.getSourceDatabase(), source.getDatabaseName());
        Set<String> existing = new HashSet<>();
        // A discovered table follows the schema the operator chose for the group's existing
        // tables (they all share one on a whole-database group), else the target's default.
        String targetSchema = TableNames.defaultSchema(target);
        for (SyncTaskGroupItem item : items(groupId)) {
            existing.add(item.getSourceTable().toLowerCase(Locale.ROOT));
            if (StringUtils.isNotBlank(item.getTargetSchema())) targetSchema = item.getTargetSchema();
        }

        int discovered = 0;
        int started = 0;
        int failed = 0;
        List<String> jobIds = new ArrayList<>();
        for (String table : metadataService.queryTables(source.getSourceId(), database)) {
            if (!existing.add(table.toLowerCase(Locale.ROOT))) continue;
            SyncTaskGroupItem item = new SyncTaskGroupItem();
            item.setGroupId(groupId);
            item.setSourceDatabase(database);
            item.setSourceTable(table);
            item.setTargetSchema(targetSchema);
            item.setTargetTable(table);
            item.setDdlPolicy(group.getDdlPolicy());
            item.setStatus(SyncStatus.PENDING);
            String validationError;
            try {
                DataSourceMetadataVo metadata = readSourceMetadata(item, source);
                applySelection(item, metadata);
                TableSchemaSnapshot.baseline(item, metadata);
                if (DataSourceType.isKafka(target)) kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());
                validationError = validateDiscoveredItem(source, target, item);
            } catch (RuntimeException ex) {
                validationError = ex.getMessage();
            }
            if (validationError != null) {
                item.setStatus(SyncStatus.FAILED);
                item.setLastError(SyncText.truncateForColumn(validationError));
                failed++;
            }
            itemMapper.insert(item);
            discovered++;
            if (validationError == null && isLive(group)) {
                try {
                    jobIds.add(submitItem(group, item, source, target));
                    started++;
                } catch (RuntimeException ex) {
                    isolateItem(item, ex.getMessage());
                    failed++;
                }
            }
        }
        if (discovered > 0) {
            group.setConfigVersion(nextConfigVersion(group));
            if (!jobIds.isEmpty()) group.setEngineJobId(appendJobIds(group.getEngineJobId(), jobIds));
            group.setLastError(failed == 0 ? "" : "新增表发现完成，其中 " + failed + " 张表校验或提交失败，请查看表项错误");
            groupMapper.updateById(group);
        }
        return SyncTaskGroupOperationResult.of(group, discovered == 0 ? "未发现新增表"
            : "发现 " + discovered + " 张新表，已启动 " + started + " 张，失败 " + failed + " 张");
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult pause(Long groupId) {
        return locks.withGroupLock(groupId, () -> doPause(groupId));
    }

    private SyncTaskGroupOperationResult doPause(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!SyncStatus.RUNNING.equals(group.getStatus())) throw new ServiceException("只有运行中的任务组可以暂停");
        DataSource target = requireTarget(group);
        for (SyncTaskGroupItem item : items(groupId)) {
            if (StringUtils.isNotBlank(item.getEngineJobId())) stopItem(item, target, true, SyncStatus.PAUSING);
        }
        group.setStatus(SyncStatus.PAUSING);
        groupMapper.updateById(group);
        return SyncTaskGroupOperationResult.of(group, "暂停请求已提交，请刷新状态确认 savepoint");
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult resume(Long groupId) {
        return locks.withGroupLock(groupId, () -> doResume(groupId));
    }

    private SyncTaskGroupOperationResult doResume(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (group.getStatus() == null || !RESUMABLE_STATUSES.contains(group.getStatus())) {
            throw new ServiceException("只有已暂停或失败任务组可以恢复");
        }
        if (items(groupId).stream().anyMatch(item -> ddlEventMapper.selectLatestOpen(item.getItemId()) != null)) {
            throw new ServiceException("任务组存在待处理的表结构变更，请在结构检查结果中逐表修复并恢复");
        }
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        for (SyncTaskGroupItem item : items(groupId)) {
            if (StringUtils.isBlank(item.getEngineJobId())) continue;
            var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties, sourceColumns(source));
            if (configChanged(item, generated)) {
                throw new ServiceException("表 " + item.getSourceTable() + " 配置已变化，不能直接恢复");
            }
            submitWithBridge(group, item, source, target, generated, item.getEngineJobId(), true);
            item.setStatus(SyncStatus.RUNNING);
            itemMapper.updateById(item);
        }
        group.setStatus(SyncStatus.RUNNING);
        groupMapper.updateById(group);
        return SyncTaskGroupOperationResult.of(group, "任务组已从 savepoint 恢复");
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult resumeItem(Long groupId, Long itemId) {
        return locks.withGroupLock(groupId, () -> doResumeItem(groupId, itemId));
    }

    private SyncTaskGroupOperationResult doResumeItem(Long groupId, Long itemId) {
        SyncTaskGroup group = requireGroup(groupId);
        SyncTaskGroupItem item = itemMapper.selectOneOfGroup(groupId, itemId);
        if (item == null) throw new ServiceException("表项不存在或不属于当前任务组");
        if (StringUtils.isBlank(item.getEngineJobId())) throw new ServiceException("表项没有可恢复的引擎作业");
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties, sourceColumns(source));
        if (configChanged(item, generated)) {
            throw new ServiceException("表 " + item.getSourceTable() + " 的引擎配置已变化，不能直接从原 savepoint 恢复，请创建新配置版本并重新初始化该表");
        }
        submitWithBridge(group, item, source, target, generated, item.getEngineJobId(), true);
        TableSchemaSnapshot.baseline(item, readSourceMetadata(item, source));
        item.setStatus(SyncStatus.RUNNING);
        item.setLastError("");
        itemMapper.updateById(item);
        group.setStatus(GroupStatuses.aggregate(itemStatuses(groupId)));
        group.setLastError("");
        groupMapper.updateById(group);
        return SyncTaskGroupOperationResult.of(group, "表 " + item.getSourceTable() + " 已从 savepoint 恢复");
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult reinitializeItem(Long groupId, Long itemId) {
        return locks.withGroupLock(groupId, () -> doReinitializeItem(groupId, itemId));
    }

    private SyncTaskGroupOperationResult doReinitializeItem(Long groupId, Long itemId) {
        SyncTaskGroup group = requireGroup(groupId);
        SyncTaskGroupItem item = itemMapper.selectOneOfGroup(groupId, itemId);
        if (item == null) throw new ServiceException("表项不存在或不属于当前任务组");
        if (SyncStatus.DRAFT.equals(group.getStatus())) throw new ServiceException("任务组尚未启动，请直接启动任务组");
        if (SyncStatus.PAUSING.equals(group.getStatus())) throw new ServiceException("任务组正在暂停，请等待 savepoint 完成后再重新初始化表项");
        if (item.getStatus() == null || !REINITIALIZABLE_ITEM_STATUSES.contains(item.getStatus())) {
            throw new ServiceException("只有失败、结构阻塞、已停止或已完成的表项可以重新初始化");
        }
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);

        // 1. Discard the old job and its recovery state. It may already be gone; that is fine.
        String oldJobId = item.getEngineJobId();
        if (DataSourceType.isKafka(target)) kafkaTaskBridgeService.stop(itemId);
        if (StringUtils.isNotBlank(oldJobId)) {
            try {
                restClient.stop(oldJobId, false, true);
            } catch (ServiceException ignored) {
                // Nothing to stop or the engine no longer knows the job - the rebuild is still valid.
            }
        }

        // 2. Re-derive the projection against the live source schema (see followSourceColumns);
        // an explicit subset is re-validated as-is. The sync key is never re-chosen here.
        DataSourceMetadataVo metadata = readSourceMetadata(item, source);
        boolean widened = followSourceColumns(item, metadata);
        if (!widened) applySelection(item, metadata);
        TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
            item.getSourceTable(), item.getTargetSchema(), item.getTargetTable(), item.getSelectedColumns(), item.getSyncKeyColumns());
        if (!compatibility.isPassed()) throw new ServiceException("目标表兼容性未通过：" + compatibility.getMessage());
        if (isDatabaseScope(group) && DataSourceType.isKafka(target)) kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());

        // 3. Fresh baseline, fresh job, no inherited checkpoint.
        TableSchemaSnapshot.baseline(item, metadata);
        String newJobId = submitItem(group, item, source, target);
        itemMapper.clearCheckpoint(itemId);
        ddlEventMapper.resolveOpen(itemId, "已通过重新初始化该表处理。");

        group.setEngineJobId(replaceJobId(group.getEngineJobId(), oldJobId, newJobId));
        group.setStatus(GroupStatuses.aggregate(itemStatuses(groupId)));
        group.setLastError("");
        groupMapper.updateById(group);
        return SyncTaskGroupOperationResult.of(group, "表 " + item.getSourceTable() + " 已重新初始化，正在重新全量同步"
            + (widened ? "（已纳入源表新增字段）" : ""));
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult stop(Long groupId) {
        return locks.withGroupLock(groupId, () -> doStop(groupId));
    }

    private SyncTaskGroupOperationResult doStop(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource target = requireTarget(group);
        for (SyncTaskGroupItem item : items(groupId)) {
            if (StringUtils.isNotBlank(item.getEngineJobId())) stopItem(item, target, false, SyncStatus.STOPPED);
        }
        group.setStatus(SyncStatus.STOPPED);
        groupMapper.updateById(group);
        return SyncTaskGroupOperationResult.of(group, "任务组已停止");
    }

    @Override
    @Transactional
    public SyncTaskGroupStatus refreshStatus(Long groupId) {
        return locks.withGroupLock(groupId, () -> doRefreshStatus(groupId));
    }

    private SyncTaskGroupStatus doRefreshStatus(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource target = requireTarget(group);
        boolean kafkaGroup = DataSourceType.isKafka(target);
        DataSource source = kafkaGroup ? requireSource(group) : null;
        List<String> statuses = new ArrayList<>();
        SyncTaskGroupStatus result = new SyncTaskGroupStatus();
        result.setGroupId(groupId);
        for (SyncTaskGroupItem item : items(groupId)) {
            SyncTaskGroupItemStatus itemStatus = new SyncTaskGroupItemStatus();
            itemStatus.setItemId(item.getItemId());
            itemStatus.setSourceTable(item.getSourceTable());
            itemStatus.setEngineJobId(item.getEngineJobId());
            if (StringUtils.isBlank(item.getEngineJobId())) {
                itemStatus.setStatus(item.getStatus());
                statuses.add(item.getStatus());
            } else {
                try {
                    var snapshot = restClient.status(item.getEngineJobId());
                    itemStatusFailureStreak.remove(item.getItemId());
                    String status = EngineJobStates.toPlatformStatus(snapshot.status());
                    boolean ddlBlocked = ddlEventMapper.selectLatestOpen(item.getItemId()) != null;
                    item.setStatus(ddlBlocked ? SyncStatus.DDL_BLOCKED : status);
                    if (kafkaGroup && SyncStatus.RUNNING.equals(status) && !ddlBlocked
                        && !kafkaTaskBridgeService.isRunning(item.getItemId())) {
                        // Engine job alive, bridge dead - heal it (mirrors the task-side reconcile).
                        startItemBridge(group, item, source, target);
                    }
                    item.setLastError(SyncText.truncateForColumn(StringUtils.defaultIfBlank(snapshot.errorMessage(), "")));
                    refreshItemCheckpoint(item);
                    itemMapper.updateById(item);
                    itemStatus.setEngineStatus(snapshot.status());
                    itemStatus.setStatus(item.getStatus());
                    itemStatus.setErrorMessage(StringUtils.isBlank(snapshot.errorMessage())
                        ? null : SyncText.truncateForColumn(snapshot.errorMessage()));
                    EngineJobStates.applyMetrics(itemStatus, snapshot, group.getSyncMode());
                    metricsService.recordGroupItem(item, snapshot.status(), itemStatus);
                    statuses.add(item.getStatus());
                } catch (RuntimeException ex) {
                    int streak = itemStatusFailureStreak.merge(item.getItemId(), 1, Integer::sum);
                    if (streak < ITEM_STATUS_FAILURE_TOLERANCE && !EngineJobStates.isRecoveryBoundaryError(ex.getMessage())) {
                        // Transient engine unreachability - hold the item's last-known status
                        // and its bridge; only fail it after ITEM_STATUS_FAILURE_TOLERANCE.
                        itemStatus.setStatus(item.getStatus());
                        itemStatus.setErrorMessage("SeaTunnel 状态暂不可达（第 " + streak + "/" + ITEM_STATUS_FAILURE_TOLERANCE
                            + " 次）：" + SyncText.truncateForColumn(StringUtils.defaultIfBlank(ex.getMessage(), "")));
                        statuses.add(item.getStatus());
                    } else {
                        itemStatusFailureStreak.remove(item.getItemId());
                        isolateItem(item, ex.getMessage());
                        itemStatus.setStatus(SyncStatus.FAILED);
                        itemStatus.setErrorMessage(SyncText.truncateForColumn(ex.getMessage()));
                        statuses.add(SyncStatus.FAILED);
                    }
                }
            }
            result.getItems().add(itemStatus);
        }
        String aggregate = GroupStatuses.aggregate(statuses);
        group.setStatus(aggregate);
        groupMapper.updateById(group);
        result.setStatus(aggregate);
        result.setMessage(SyncStatus.FAILED.equals(aggregate) ? "任务组存在失败表项" : "任务组状态已刷新");
        return result;
    }

    /** Reconcile persisted group state with SeaTunnel after a platform restart. */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverRunningGroups() {
        groupMapper.selectActive().forEach(group -> {
            try {
                SyncTaskGroupStatus status = refreshStatus(group.getGroupId());
                if (!SyncStatus.RUNNING.equals(status.getStatus())) return;
                DataSource source = requireSource(group);
                DataSource target = requireTarget(group);
                if (!DataSourceType.isKafka(target)) return;
                for (SyncTaskGroupItem item : items(group.getGroupId())) {
                    if (SyncStatus.RUNNING.equals(item.getStatus())) startItemBridge(group, item, source, target);
                }
            } catch (RuntimeException ex) {
                markGroupFailed(group, ex.getMessage());
            }
        });
    }

    /**
     * Mirrors SeaTunnelJobServiceImpl.refreshRunningTaskStatus() for groups: without this,
     * nothing but the one-time startup reconciliation above ever polled engine state for a
     * RUNNING/PAUSING group, so a FULL-mode group (which finishes at the engine within
     * seconds) stayed stuck showing RUNNING indefinitely unless a user happened to open its
     * detail and click "刷新状态".
     */
    @Scheduled(fixedDelayString = "${sync.status-refresh.interval-ms:30000}", initialDelayString = "${sync.status-refresh.initial-delay-ms:25000}")
    public void refreshRunningGroupStatus() {
        groupMapper.selectActive().forEach(group -> {
            if (locks.isGroupBusy(group.getGroupId())) return;
            try {
                refreshStatus(group.getGroupId());
            } catch (RuntimeException ex) {
                markGroupFailed(group, ex.getMessage());
            }
        });
    }

    /** Runs only for database-scope groups that explicitly opted into new-table discovery. */
    @Scheduled(fixedDelayString = "${sync.discovery.interval-ms:60000}", initialDelayString = "${sync.discovery.initial-delay-ms:30000}")
    public void discoverDatabaseGroups() {
        groupMapper.selectLiveAutoDiscoverDatabaseGroups().forEach(group -> {
            if (locks.isGroupBusy(group.getGroupId())) return;
            try {
                discover(group.getGroupId());
            } catch (RuntimeException ignored) {
                // An individual discovery pass must not prevent later scans or affect other groups.
            }
        });
    }

    // ------------------------------------------------------------------ data check

    /** Executes independent, bounded COUNT(*) comparisons for all task-group table items. */
    @Override
    public SyncTaskGroupDataCheckResult checkData(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        List<SyncTaskGroupItem> groupItems = items(groupId);
        SyncTaskGroupDataCheckResult result = new SyncTaskGroupDataCheckResult();
        result.setGroupId(groupId);
        result.setTableCount(groupItems.size());
        if (DataSourceType.isKafka(target)) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setConsistencyNote("Kafka 目标使用事件核对口径，不执行关系型目标行数核对。");
            result.setMessage("Kafka 任务组请通过 topic 的 key、offset、分区和事件信封进行核对");
            return result;
        }
        result.setConsistencyNote(isLive(group) || SyncStatus.PAUSING.equals(group.getStatus())
            ? "任务仍在持续同步，当前为非同水位的行数检查；暂停或确认两端水位稳定后再做严格验收。"
            : "当前结果为源端与目标端的只读行数检查，不替代基于同步键的逐行校验。");

        for (SyncTaskGroupItem item : groupItems) {
            SyncTaskGroupDataCheckItemResult itemResult = new SyncTaskGroupDataCheckItemResult();
            itemResult.setItemId(item.getItemId());
            try {
                SyncTaskDataCheckResult tableResult = dataConsistencyService.check(source, target,
                    sourceDatabaseOf(item, source), item.getSourceTable(),
                    StringUtils.defaultIfBlank(item.getTargetSchema(), TableNames.defaultSchema(target)), item.getTargetTable());
                itemResult.setSourceTable(tableResult.getSourceTable());
                itemResult.setTargetTable(tableResult.getTargetTable());
                itemResult.setSourceRows(tableResult.getSourceRows());
                itemResult.setTargetRows(tableResult.getTargetRows());
                itemResult.setDifference(tableResult.getDifference());
                itemResult.setMatched(tableResult.isMatched());
                itemResult.setSuccess(tableResult.isSuccess());
                itemResult.setMessage(tableResult.getMessage());
            } catch (RuntimeException ex) {
                itemResult.setSourceTable(sourceDatabaseOf(item, source) + "." + item.getSourceTable());
                itemResult.setTargetTable(TableNames.display(target, item.getTargetSchema(), item.getTargetTable()));
                itemResult.setSuccess(false);
                itemResult.setMatched(false);
                itemResult.setMessage("数据核对失败：" + StringUtils.defaultIfBlank(ex.getMessage(), "未知错误"));
            }
            persistCheck(item, itemResult);
            result.getItems().add(itemResult);
            if (!itemResult.isSuccess()) result.setFailedTableCount(result.getFailedTableCount() + 1);
            else if (itemResult.isMatched()) result.setMatchedTableCount(result.getMatchedTableCount() + 1);
            else result.setMismatchedTableCount(result.getMismatchedTableCount() + 1);
        }
        result.setSuccess(result.getFailedTableCount() == 0);
        result.setMatched(result.isSuccess() && result.getMismatchedTableCount() == 0);
        result.setMessage(result.isMatched()
            ? "全部 " + result.getTableCount() + " 张表行数一致"
            : "已核对 " + result.getTableCount() + " 张表：一致 " + result.getMatchedTableCount()
                + "，不一致 " + result.getMismatchedTableCount() + "，失败 " + result.getFailedTableCount());
        return result;
    }

    // ------------------------------------------------------------------ save helpers

    private SyncTaskGroup normalize(SyncTaskGroupBo bo, SyncTaskGroup current) {
        SyncTaskGroup entity = current == null ? new SyncTaskGroup() : current;
        entity.setGroupName(bo.getGroupName());
        entity.setSourceId(bo.getSourceId());
        entity.setTargetId(bo.getTargetId());
        entity.setSyncScope(StringUtils.defaultIfBlank(bo.getSyncScope(), SCOPE_MULTI_TABLE).toUpperCase(Locale.ROOT));
        entity.setSourceDatabase(bo.getSourceDatabase());
        entity.setAutoDiscover(isDatabaseScope(entity) && "1".equals(bo.getAutoDiscover()) ? "1" : "0");
        entity.setSyncMode(StringUtils.defaultIfBlank(bo.getSyncMode(), SyncMode.FULL_CDC));
        entity.setDdlPolicy(StringUtils.defaultIfBlank(bo.getDdlPolicy(), DEFAULT_DDL_POLICY));
        entity.setReadLimitRowsPerSecond(bo.getReadLimitRowsPerSecond());
        entity.setReadLimitBytesPerSecond(bo.getReadLimitBytesPerSecond());
        entity.setSnapshotParallelism(bo.getSnapshotParallelism());
        entity.setSourceConnectionLimit(bo.getSourceConnectionLimit());
        if (entity.getSourceId().equals(entity.getTargetId())) throw new ServiceException("源数据源和目标数据源不能相同");
        DataSource source = requireSource(entity);
        DataSource target = requireTarget(entity);
        if (!DataSourceType.isMysql(source)) throw new ServiceException("多表 MVP 源端必须是 MySQL");
        if (!DataSourceType.isSupportedTarget(target.getSourceType())) {
            throw new ServiceException("多表 MVP 目标端必须是 PostgreSQL、MySQL 或 Kafka");
        }
        entity.setKafkaOutputFormat(DataSourceType.isKafka(target) ? KafkaOutputFormat.parse(bo.getKafkaOutputFormat()).name() : null);
        entity.setSyncMode(SyncMode.normalize(entity.getSyncMode()));
        if (!List.of(SCOPE_MULTI_TABLE, SCOPE_DATABASE).contains(entity.getSyncScope())) throw new ServiceException("同步粒度仅支持多表或整库");
        if (!isDatabaseScope(entity) && (bo.getItems() == null || bo.getItems().isEmpty())) {
            throw new ServiceException("至少选择一张表");
        }
        if (isDatabaseScope(entity) && StringUtils.isBlank(entity.getSourceDatabase())) {
            entity.setSourceDatabase(source.getDatabaseName());
        }
        if (bo.getItems() != null && bo.getItems().size() > MAX_TABLES_PER_GROUP) {
            throw new ServiceException("单个任务组最多支持 " + MAX_TABLES_PER_GROUP + " 张表");
        }
        resourceProtectionPolicy.applyDefaultsAndValidate(entity);
        return entity;
    }

    private void replaceItems(SyncTaskGroup group, List<SyncTaskGroupItemBo> itemBos) {
        forgetFailureStreaks(group.getGroupId());
        itemMapper.deleteByGroupId(group.getGroupId());
        if (itemBos == null) return;
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        for (SyncTaskGroupItemBo bo : itemBos) {
            SyncTaskGroupItem item = MapstructUtils.convert(bo, SyncTaskGroupItem.class);
            // Items are always re-created on save so a stale DDL event never re-attaches to an edited table.
            item.setItemId(null);
            item.setGroupId(group.getGroupId());
            item.setTargetSchema(StringUtils.defaultIfBlank(item.getTargetSchema(), TableNames.defaultSchema(target)));
            item.setDdlPolicy(StringUtils.defaultIfBlank(item.getDdlPolicy(), DEFAULT_DDL_POLICY));
            // Selection and baseline are captured together so "selected the whole table" is a
            // property start / reinitialize can read back later (followSourceColumns).
            DataSourceMetadataVo metadata = readSourceMetadata(item, source);
            applySelection(item, metadata);
            TableSchemaSnapshot.baseline(item, metadata);
            item.setStatus(SyncStatus.PENDING);
            itemMapper.insert(item);
        }
    }

    private void attachItems(SyncTaskGroupVo group) {
        List<SyncTaskGroupItemVo> items = MapstructUtils.convert(itemMapper.selectByGroupId(group.getGroupId()), SyncTaskGroupItemVo.class);
        Map<Long, SyncMetricsSampleVo> latest = metricsService.latestForGroupItems(items.stream().map(SyncTaskGroupItemVo::getItemId).toList());
        items.forEach(item -> item.setLatestMetrics(latest.get(item.getItemId())));
        group.setItems(items);
    }

    /** Expands / validates the column projection and sync key against live source metadata. */
    private static void applySelection(SyncTaskGroupItem item, DataSourceMetadataVo metadata) {
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(metadata,
            item.getSelectedColumns(), item.getSyncKeyColumns());
        item.setSelectedColumns(SyncColumnSelectionValidator.serialize(selection.selectedColumns()));
        item.setSyncKeyColumns(SyncColumnSelectionValidator.serialize(selection.syncKeyColumns()));
    }

    /**
     * A selection that covered every column of the previous baseline means "the whole table",
     * so it is re-derived from the live schema and picks up columns added since; an explicit
     * subset is left alone. The sync key is never re-chosen. Returns true when columns were added.
     */
    private static boolean followSourceColumns(SyncTaskGroupItem item, DataSourceMetadataVo metadata) {
        List<String> configured = SyncColumnSelectionValidator.parseColumns(item.getSelectedColumns());
        TableSchemaSnapshot.Snapshot baseline = StringUtils.isBlank(item.getSchemaSnapshot()) ? null : TableSchemaSnapshot.fromJson(item.getSchemaSnapshot());
        if (!TableSchemaSnapshot.coversAllColumns(baseline, configured)) return false;
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(metadata, null, item.getSyncKeyColumns());
        item.setSelectedColumns(SyncColumnSelectionValidator.serialize(selection.selectedColumns()));
        item.setSyncKeyColumns(SyncColumnSelectionValidator.serialize(selection.syncKeyColumns()));
        return selection.selectedColumns().size() > configured.size();
    }

    private static boolean hasLiveJob(SyncTaskGroupItem item) {
        return SyncStatus.isActive(item.getStatus()) && StringUtils.isNotBlank(item.getEngineJobId());
    }

    private void persistCheck(SyncTaskGroupItem item, SyncTaskGroupDataCheckItemResult result) {
        item.setLastCheckSourceRows(result.getSourceRows());
        item.setLastCheckTargetRows(result.getTargetRows());
        item.setLastCheckDifference(result.getDifference());
        item.setLastCheckMatched(result.isSuccess() ? (result.isMatched() ? "1" : "0") : null);
        item.setLastCheckTime(LocalDateTime.now());
        item.setLastCheckMessage(SyncText.truncateForColumn(result.getMessage()));
        itemMapper.updateById(item);
    }

    // ------------------------------------------------------------------ engine helpers

    /**
     * Submits one table item as its own SeaTunnel job. Returns the engine job id.
     * <p>For Kafka targets the bridge is started first, mirroring the single-task path:
     * its topic precheck fails before any engine job exists (no orphan to clean up), and
     * it creates the single-partition raw topic before the engine can auto-create it with
     * broker defaults. A failed submit tears the bridge down again.
     */
    private String submitItem(SyncTaskGroup group, SyncTaskGroupItem item, DataSource source, DataSource target) {
        var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties, sourceColumns(source));
        SeaTunnelJobConfigGenerator.prepareTarget(SyncTaskGroupConfigGenerator.toTask(group, item), source, target, generated, sourceColumns(source));
        String jobId = submitWithBridge(group, item, source, target, generated, null, false);
        item.setEngineJobId(jobId);
        item.setEngineConfigHash(generated.fingerprint());
        item.setStatus(SyncStatus.RUNNING);
        item.setLastError("");
        itemMapper.updateById(item);
        return jobId;
    }

    /** Bridge (Kafka only) -> engine submit; the bridge is stopped again if the submit fails. */
    private String submitWithBridge(SyncTaskGroup group, SyncTaskGroupItem item, DataSource source, DataSource target,
                                    SeaTunnelJobConfigGenerator.GeneratedConfig generated, String existingJobId, boolean withSavepoint) {
        boolean kafka = DataSourceType.isKafka(target);
        if (kafka) startItemBridge(group, item, source, target);
        try {
            return restClient.submit(generated.jobName(), generated.config(), existingJobId, withSavepoint).jobId();
        } catch (RuntimeException ex) {
            if (kafka) kafkaTaskBridgeService.stop(item.getItemId());
            throw ex;
        }
    }

    private void stopItem(SyncTaskGroupItem item, DataSource target, boolean withSavepoint, String newStatus) {
        if (DataSourceType.isKafka(target)) kafkaTaskBridgeService.stop(item.getItemId());
        restClient.stop(item.getEngineJobId(), withSavepoint, false);
        item.setStatus(newStatus);
        itemMapper.updateById(item);
    }

    private void startItemBridge(SyncTaskGroup group, SyncTaskGroupItem item, DataSource source, DataSource target) {
        kafkaTaskBridgeService.startGroupItem(SyncTaskGroupConfigGenerator.toTask(group, item), target, source.getDatabaseName());
    }

    private void refreshItemCheckpoint(SyncTaskGroupItem item) {
        try {
            var checkpoint = restClient.checkpoints(item.getEngineJobId());
            if (checkpoint.id() != null) {
                item.setLastCheckpointId(checkpoint.id());
                item.setLastCheckpointTime(checkpoint.time() == null ? null : checkpoint.time().toString());
                item.setLastCheckpointStatus(checkpoint.status());
            }
        } catch (RuntimeException checkpointError) {
            if (StringUtils.isBlank(item.getLastError())) {
                item.setLastError(SyncText.truncateForColumn(checkpointError.getMessage()));
            }
        }
    }

    private static boolean configChanged(SyncTaskGroupItem item, SeaTunnelJobConfigGenerator.GeneratedConfig generated) {
        return StringUtils.isNotBlank(item.getEngineConfigHash()) && !generated.matchesFingerprint(item.getEngineConfigHash());
    }

    /**
     * Drops the in-memory poll-failure counters of a group's items. Items are re-created with
     * fresh ids on every save and deleted with the group, so without this the map would keep
     * an entry per item that ever existed in this process.
     */
    private void forgetFailureStreaks(Long groupId) {
        items(groupId).forEach(item -> itemStatusFailureStreak.remove(item.getItemId()));
    }

    private void isolateItem(SyncTaskGroupItem item, String error) {
        item.setStatus(SyncStatus.FAILED);
        item.setLastError(SyncText.truncateForColumn(error));
        itemMapper.updateById(item);
    }

    private void markGroupFailed(SyncTaskGroup group, String error) {
        group.setStatus(SyncStatus.FAILED);
        group.setLastError(SyncText.truncateForColumn(error));
        groupMapper.updateById(group);
    }

    /**
     * A whole-database Kafka group creates one topic per table on discovery. Re-running this
     * on start recreates a topic that was dropped and clears the isolation on any table that
     * had been marked FAILED only because its topic did not exist yet.
     */
    private void recoverDatabaseKafkaTopics(Long groupId, DataSource source, DataSource target) {
        for (SyncTaskGroupItem item : items(groupId)) {
            try {
                kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());
                if (SyncStatus.FAILED.equals(item.getStatus()) && validateDiscoveredItem(source, target, item) == null) {
                    TableSchemaSnapshot.baseline(item, readSourceMetadata(item, source));
                    item.setStatus(SyncStatus.PENDING);
                    item.setLastError("");
                    itemMapper.updateById(item);
                }
            } catch (RuntimeException ignored) {
                // Leave the table isolated; the start loop surfaces the reason per table.
            }
        }
    }

    /** Null when the discovered table has a usable sync key and a compatible target, otherwise the reason. */
    private String validateDiscoveredItem(DataSource source, DataSource target, SyncTaskGroupItem item) {
        try {
            DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(), item.getSourceDatabase(), item.getSourceTable());
            boolean hasKey = !metadata.getPrimaryKeys().isEmpty()
                || metadata.getUniqueKeys().stream().anyMatch(key -> Boolean.TRUE.equals(key.getAllNotNull()));
            if (!hasKey) return "源表没有可用同步键";
            TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                item.getSourceTable(), item.getTargetSchema(), item.getTargetTable());
            return compatibility.isPassed() ? null : compatibility.getMessage();
        } catch (RuntimeException ex) {
            return StringUtils.defaultIfBlank(ex.getMessage(), "新增表校验失败");
        }
    }

    // ------------------------------------------------------------------ lookups & small helpers


    private List<SyncTaskGroupItem> items(Long groupId) {
        return itemMapper.selectByGroupId(groupId);
    }

    private List<String> itemStatuses(Long groupId) {
        return items(groupId).stream().map(SyncTaskGroupItem::getStatus).toList();
    }

    private SyncTaskGroup requireGroup(Long groupId) {
        SyncTaskGroup group = groupMapper.selectById(groupId);
        if (group == null) throw new ServiceException("同步任务组不存在");
        return group;
    }

    private DataSource requireSource(SyncTaskGroup group) {
        return dataSourceService.requireById(group.getSourceId(), "源");
    }

    private DataSource requireTarget(SyncTaskGroup group) {
        return dataSourceService.requireById(group.getTargetId(), "目标");
    }

    /** The per-table reasons, so a refused start names the table and the problem instead of "表级问题". */
    private static String describeFailures(SyncTaskGroupValidationResult validation) {
        List<String> reasons = validation.getItems().stream()
            .filter(item -> !item.isPassed())
            .map(item -> item.getSourceTable() + "：" + StringUtils.defaultIfBlank(item.getMessage(), "校验未通过"))
            .toList();
        return reasons.isEmpty() ? validation.getMessage() : SyncText.truncateForColumn(String.join("；", reasons));
    }

    private SourceColumns sourceColumns(DataSource source) {
        return SourceColumns.fromMetadata(metadataService, source);
    }

    private DataSourceMetadataVo readSourceMetadata(SyncTaskGroupItem item, DataSource source) {
        return metadataService.queryTableMetadata(source.getSourceId(), sourceDatabaseOf(item, source), item.getSourceTable());
    }

    private static String sourceDatabaseOf(SyncTaskGroupItem item, DataSource source) {
        return StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName());
    }

    private static boolean isDatabaseScope(SyncTaskGroup group) {
        return SCOPE_DATABASE.equals(group.getSyncScope());
    }

    private static boolean isLive(SyncTaskGroup group) {
        return group.getStatus() != null && LIVE_STATUSES.contains(group.getStatus());
    }

    private static int nextConfigVersion(SyncTaskGroup group) {
        return (group.getConfigVersion() == null ? 1 : group.getConfigVersion()) + 1;
    }

    /** Swaps one job id for another in the group's comma-separated list (appends when the old one is absent). */
    private static String replaceJobId(String current, String oldJobId, String newJobId) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(current)) values.addAll(List.of(current.split(",")));
        if (StringUtils.isNotBlank(oldJobId)) values.remove(oldJobId);
        values.add(newJobId);
        return String.join(",", values);
    }

    private static String appendJobIds(String current, List<String> appended) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(current)) values.addAll(List.of(current.split(",")));
        values.addAll(appended);
        return String.join(",", values);
    }
}
