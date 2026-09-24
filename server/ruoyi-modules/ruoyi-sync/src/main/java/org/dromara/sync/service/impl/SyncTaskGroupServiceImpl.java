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
import org.dromara.sync.config.SyncGroupProperties;
import org.dromara.sync.config.SyncSchedulingConfig;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.constant.SyncScope;
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
import org.dromara.sync.domain.vo.SyncTaskGroupConfigPreview;
import org.dromara.sync.domain.vo.SyncTaskGroupItemStatus;
import org.dromara.sync.domain.vo.SyncTaskGroupItemValidationVo;
import org.dromara.sync.domain.vo.SyncTaskGroupItemVo;
import org.dromara.sync.domain.vo.SyncTaskGroupLimitsVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;
import org.dromara.sync.domain.vo.SyncTaskGroupValidationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.EngineJobRunner;
import org.dromara.sync.engine.EngineJobStates;
import org.dromara.sync.engine.SeaTunnelJobConfigGenerator;
import org.dromara.sync.engine.SourceColumns;
import org.dromara.sync.engine.SyncTaskGroupConfigGenerator;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.SyncTaskGroupDdlEventMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.service.ISyncTaskGroupDiscoveryService;
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
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Multi-table / whole-database release: CRUD, validation / preview and the lifecycle. One
 * SeaTunnel job per table item; the group status is an aggregate of its items (see
 * {@link GroupStatuses#aggregate}). DDL drift handling lives in {@link SyncTaskGroupDdlServiceImpl},
 * whole-database table discovery in {@link SyncTaskGroupDiscoveryServiceImpl}, the row-count check in
 * {@link SyncTaskGroupDataCheckServiceImpl}; the steps on a single table item they share with this
 * lifecycle (read its schema, derive its selection, submit its job, park it) in {@link GroupItemOperations}.
 *
 * <p><b>Transactions.</b> The lifecycle methods (start / pause / resume / stop / refresh /
 * per-item resume and reinitialize, and discovery) deliberately run <em>without</em> a database
 * transaction. They talk to the engine, and a rollback cannot un-submit or un-stop a job: under
 * a transaction a failure half-way left the rows describing a state the engine was no longer in,
 * while the transaction held a pooled connection and row locks for as long as N serial REST calls
 * took. Worse, the group lock was released when the method body returned but the transaction
 * only committed after that, so another instance could take the lock and read pre-commit rows.
 * Instead each write commits on its own, every refusal is decided before the first engine call,
 * and each method leaves an honest partial outcome (per-item status + error) when the engine fails
 * part-way. Edits and deletes, which only touch the database, run in a transaction <em>inside</em>
 * the group lock ({@link #inLockedTransaction}) so they commit before the lock is released.
 */
@RequiredArgsConstructor
@Service
public class SyncTaskGroupServiceImpl implements ISyncTaskGroupService {

    private static final String DEFAULT_DDL_POLICY = "FAIL";

    private static final Set<String> EDITABLE_STATUSES = Set.of(SyncStatus.DRAFT, SyncStatus.STOPPED);
    private static final Set<String> DELETABLE_STATUSES = Set.of(SyncStatus.DRAFT, SyncStatus.STOPPED, SyncStatus.FAILED, SyncStatus.FINISHED);
    private static final Set<String> RESUMABLE_STATUSES = Set.of(SyncStatus.PAUSED, SyncStatus.FAILED);
    /** Item states a single table may be rebuilt from without touching its siblings. */
    private static final Set<String> REINITIALIZABLE_ITEM_STATUSES = Set.of(SyncStatus.FAILED, SyncStatus.DDL_BLOCKED, SyncStatus.STOPPED, SyncStatus.FINISHED);

    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SyncTaskGroupDdlEventMapper ddlEventMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final ISyncTaskGroupDiscoveryService discoveryService;
    private final SeaTunnelProperties properties;
    private final ResourceProtectionPolicy resourceProtectionPolicy;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;
    private final ISyncMetricsService metricsService;
    private final SyncLocks locks;
    private final TransactionTemplate transactionTemplate;
    private final EngineJobRunner runner;
    private final GroupItemOperations itemOps;
    private final SyncGroupProperties groupProperties;


    // ------------------------------------------------------------------ CRUD

    @Override
    public PageResult<SyncTaskGroupVo> queryPageList(SyncTaskGroupBo bo, PageQuery pageQuery) {
        LambdaQueryWrapper<SyncTaskGroup> wrapper = new LambdaQueryWrapper<SyncTaskGroup>()
            .like(StringUtils.isNotBlank(bo.getGroupName()), SyncTaskGroup::getGroupName, bo.getGroupName())
            .eq(StringUtils.isNotBlank(bo.getStatus()), SyncTaskGroup::getStatus, bo.getStatus())
            .orderByDesc(SyncTaskGroup::getGroupId);
        Page<SyncTaskGroupVo> page = groupMapper.selectVoPage(pageQuery.build(), wrapper);
        attachItems(page.getRecords());
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
        if (isDatabaseScope(entity)) discoveryService.discover(entity.getGroupId());
        return true;
    }

    /**
     * Locked like the lifecycle: without it a start that had already read the group as STOPPED
     * would go on to submit jobs for items this edit is replacing.
     */
    @Override
    public Boolean updateByBo(SyncTaskGroupBo bo) {
        return inLockedTransaction(bo.getGroupId(), () -> doUpdate(bo));
    }

    private Boolean doUpdate(SyncTaskGroupBo bo) {
        SyncTaskGroup current = requireGroup(bo.getGroupId());
        if (current.getStatus() == null || !EDITABLE_STATUSES.contains(current.getStatus())) {
            throw new ServiceException("只有草稿或已停止任务组允许修改");
        }
        SyncTaskGroup entity = normalize(bo, current);
        entity.setConfigVersion(nextConfigVersion(current));
        groupMapper.updateById(entity);
        replaceItems(entity, bo.getItems());
        if (isDatabaseScope(entity)) discoveryService.discover(entity.getGroupId());
        return true;
    }

    /**
     * Locked for the same reason as edits, with a worse failure mode: a start racing a delete
     * submits {@code ds-task-<itemId>} jobs whose rows are gone, which nothing - not even
     * {@link EngineOrphanSweeper} - can ever map back to an owner.
     */
    @Override
    public Boolean deleteById(Long groupId) {
        return inLockedTransaction(groupId, () -> doDelete(groupId));
    }

    private Boolean doDelete(Long groupId) {
        SyncTaskGroup current = requireGroup(groupId);
        if (current.getStatus() == null || !DELETABLE_STATUSES.contains(current.getStatus())) {
            throw new ServiceException("运行中的任务组不能删除");
        }
        forgetFailureStreaks(groupId);
        ddlEventMapper.deleteByGroupId(groupId);
        itemMapper.deleteByGroupId(groupId);
        metricsService.deleteForGroup(groupId);
        return groupMapper.deleteById(groupId) > 0;
    }

    // ------------------------------------------------------------------ validation & preview

    @Override
    public SyncTaskGroupLimitsVo limits() {
        SyncTaskGroupLimitsVo limits = new SyncTaskGroupLimitsVo();
        limits.setMaxTables(groupProperties.effectiveMaxTables());
        return limits;
    }

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
                DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
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
     * The writes a refused start leaves behind are intentional: the resource-protection
     * defaults, recovered Kafka topics and the re-derived column selections are all true
     * statements about the group that the operator needs to see (e.g. "the target lacks the
     * column the source just gained") when they open validate. Once jobs are being submitted,
     * a multi-table start is all-or-nothing by compensation (already-submitted jobs are stopped
     * again), a whole-database start isolates the failing tables - see the class comment for
     * why neither leans on a rollback.
     */
    @Override
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
                DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
                liveSchemas.put(item.getItemId(), metadata);
                if (GroupItemOperations.followSourceColumns(item, metadata)) itemMapper.updateById(item);
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
                    TableSchemaSnapshot.baseline(item, metadata != null ? metadata : itemOps.readSourceMetadata(item, source));
                    jobIds.add(itemOps.submit(group, item, source, target));
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
                    runner.halt(submittedItem.getItemId(), DataSourceType.isKafka(target), submittedItem.getEngineJobId(), false);
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
    public SyncTaskGroupOperationResult pause(Long groupId) {
        return locks.withGroupLock(groupId, () -> doPause(groupId));
    }

    /**
     * A table whose savepoint request fails is parked FAILED with the reason and the others
     * keep pausing; the job id stays on the row, so the next status refresh still sees that job
     * if the engine was only briefly unreachable and it is in fact still running.
     */
    private SyncTaskGroupOperationResult doPause(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!SyncStatus.RUNNING.equals(group.getStatus())) throw new ServiceException("只有运行中的任务组可以暂停");
        DataSource target = requireTarget(group);
        List<String> failed = new ArrayList<>();
        for (SyncTaskGroupItem item : items(groupId)) {
            if (StringUtils.isBlank(item.getEngineJobId()) || !SyncStatus.RUNNING.equals(item.getStatus())) continue;
            try {
                stopItem(item, target, true, SyncStatus.PAUSING);
            } catch (RuntimeException ex) {
                isolateItem(item, "savepoint 暂停请求失败：" + StringUtils.defaultIfBlank(ex.getMessage(), "引擎不可达"));
                failed.add(item.getSourceTable());
            }
        }
        // PAUSING while anything is still taking its savepoint, so the status refresh keeps
        // polling the group and settles the aggregate (PAUSED, or FAILED if a table failed).
        boolean anyPausing = itemStatuses(groupId).stream().anyMatch(SyncStatus.PAUSING::equals);
        group.setStatus(anyPausing ? SyncStatus.PAUSING : GroupStatuses.aggregate(itemStatuses(groupId)));
        group.setLastError(failed.isEmpty() ? "" : SyncText.truncateForColumn("以下表 savepoint 暂停请求失败：" + String.join("、", failed)));
        groupMapper.updateById(group);
        if (!failed.isEmpty()) {
            return SyncTaskGroupOperationResult.partial(group,
                "暂停请求已提交，但 " + failed.size() + " 张表的 savepoint 请求失败：" + String.join("、", failed));
        }
        return SyncTaskGroupOperationResult.of(group, "暂停请求已提交，请刷新状态确认 savepoint");
    }

    @Override
    public SyncTaskGroupOperationResult resume(Long groupId) {
        return locks.withGroupLock(groupId, () -> doResume(groupId));
    }

    /**
     * Every refusal is decided before the first submit: once one table is running again there
     * is nothing to roll that back, so a "config changed" on the third table must not be found
     * after the first two are already live. A table the engine then refuses is parked FAILED with
     * its savepoint untouched (恢复该表 retries it alone) while the tables that did resume keep
     * running - the group aggregates to DEGRADED instead of pretending to be RUNNING or PAUSED.
     */
    private SyncTaskGroupOperationResult doResume(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (group.getStatus() == null || !RESUMABLE_STATUSES.contains(group.getStatus())) {
            throw new ServiceException("只有已暂停或失败任务组可以恢复");
        }
        List<SyncTaskGroupItem> groupItems = items(groupId);
        if (groupItems.stream().anyMatch(item -> ddlEventMapper.selectLatestOpen(item.getItemId()) != null)) {
            throw new ServiceException("任务组存在待处理的表结构变更，请在结构检查结果中逐表修复并恢复");
        }
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        record Resumable(SyncTaskGroupItem item, SeaTunnelJobConfigGenerator.GeneratedConfig config) {
        }
        List<Resumable> resumable = new ArrayList<>();
        List<String> refused = new ArrayList<>();
        for (SyncTaskGroupItem item : groupItems) {
            if (StringUtils.isBlank(item.getEngineJobId()) || hasLiveJob(item)) continue;
            var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties, sourceColumns(source));
            String refusal = runner.resumeRefusal(generated, item.getEngineConfigHash(), item.getEngineJobId());
            if (refusal != null) {
                // Final for this table's savepoint. Parked FAILED because that is where 重新初始化该表
                // accepts it - a PAUSED table cannot be rebuilt, which would leave no way forward.
                isolateItem(item, refusal);
                refused.add(item.getSourceTable());
                continue;
            }
            resumable.add(new Resumable(item, generated));
        }
        if (!refused.isEmpty()) {
            group.setStatus(GroupStatuses.aggregate(itemStatuses(groupId)));
            group.setLastError(SyncText.truncateForColumn("以下表不能从 savepoint 恢复，请逐表重新初始化后再恢复任务组：" + String.join("、", refused)));
            groupMapper.updateById(group);
            throw new ServiceException("任务组未恢复：表 " + String.join("、", refused)
                + " 不能从原 savepoint 恢复（配置已变化或没有可用的 checkpoint/savepoint），请先在表项上重新初始化");
        }
        if (DataSourceType.isKafka(target)) {
            // A full bridge pool is a refusal too: one check for every table about to be resubmitted.
            kafkaTaskBridgeService.requireCapacity(resumable.stream().map(next -> next.item().getItemId()).toList());
        }
        List<String> failed = new ArrayList<>();
        for (Resumable next : resumable) {
            SyncTaskGroupItem item = next.item();
            try {
                runner.resume(GroupItemOperations.job(group, item, source, target), next.config(), item.getEngineJobId());
                item.setStatus(SyncStatus.RUNNING);
                item.setLastError("");
                itemMapper.updateById(item);
            } catch (RuntimeException ex) {
                isolateItem(item, "从 savepoint 恢复失败：" + StringUtils.defaultIfBlank(ex.getMessage(), "引擎不可达"));
                failed.add(item.getSourceTable());
            }
        }
        group.setStatus(GroupStatuses.aggregate(itemStatuses(groupId)));
        group.setLastError(failed.isEmpty() ? "" : SyncText.truncateForColumn(
            "以下表从 savepoint 恢复失败，可在表项上单独恢复：" + String.join("、", failed)));
        groupMapper.updateById(group);
        if (!failed.isEmpty()) {
            return SyncTaskGroupOperationResult.partial(group, "已恢复 " + (resumable.size() - failed.size()) + " 张表，"
                + failed.size() + " 张恢复失败：" + String.join("、", failed));
        }
        return SyncTaskGroupOperationResult.of(group, "任务组已从 savepoint 恢复");
    }

    @Override
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
        String refusal = runner.resumeRefusal(generated, item.getEngineConfigHash(), item.getEngineJobId());
        if (refusal != null) {
            // The table is already parked (DDL_BLOCKED / FAILED), where 重新初始化该表 takes it.
            throw new ServiceException("表 " + item.getSourceTable() + " 不能从原 savepoint 恢复：" + refusal + "（使用「重新初始化该表」）");
        }
        // Read the new baseline before the submit: failing after it would leave a running job
        // behind a row that still says the table is blocked.
        DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
        runner.resume(GroupItemOperations.job(group, item, source, target), generated, item.getEngineJobId());
        TableSchemaSnapshot.baseline(item, metadata);
        item.setStatus(SyncStatus.RUNNING);
        item.setLastError("");
        itemMapper.updateById(item);
        group.setStatus(GroupStatuses.aggregate(itemStatuses(groupId)));
        group.setLastError("");
        groupMapper.updateById(group);
        return SyncTaskGroupOperationResult.of(group, "表 " + item.getSourceTable() + " 已从 savepoint 恢复");
    }

    @Override
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

        // 1. Decide whether the rebuild can happen at all BEFORE touching the old job: a refused
        // rebuild of a DDL_BLOCKED table must leave its savepoint, and so 恢复该表, intact.
        // The projection is re-derived against the live source schema (see followSourceColumns);
        // an explicit subset is re-validated as-is. The sync key is never re-chosen here.
        DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
        boolean widened = GroupItemOperations.followSourceColumns(item, metadata);
        if (!widened) GroupItemOperations.applySelection(item, metadata);
        TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
            item.getSourceTable(), item.getTargetSchema(), item.getTargetTable(), item.getSelectedColumns(), item.getSyncKeyColumns());
        if (!compatibility.isPassed()) throw new ServiceException("目标表兼容性未通过：" + compatibility.getMessage());
        if (isDatabaseScope(group) && DataSourceType.isKafka(target)) kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());
        runner.requireBridgeCapacity(GroupItemOperations.job(group, item, source, target));

        // 2. Discard the old job and its recovery state. It may already be gone; that is fine.
        String oldJobId = item.getEngineJobId();
        runner.discard(itemId, DataSourceType.isKafka(target), oldJobId);

        // 3. Fresh baseline, fresh job, no inherited checkpoint.
        TableSchemaSnapshot.baseline(item, metadata);
        String newJobId;
        try {
            newJobId = itemOps.submit(group, item, source, target);
        } catch (RuntimeException ex) {
            // The old job is gone by now, so the row must stop pointing at it: park the table
            // FAILED with no job and the reason, from where 重新初始化 can simply be retried. If
            // the submit was in fact accepted, EngineOrphanSweeper adopts it by its job name.
            itemMapper.detachEngineJob(itemId);
            item.setEngineJobId(null);
            isolateItem(item, "重新初始化提交失败：" + StringUtils.defaultIfBlank(ex.getMessage(), "引擎不可达"));
            group.setEngineJobId(replaceJobId(group.getEngineJobId(), oldJobId, null));
            group.setStatus(GroupStatuses.aggregate(itemStatuses(groupId)));
            groupMapper.updateById(group);
            throw ex;
        }
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
    public SyncTaskGroupOperationResult stop(Long groupId) {
        return locks.withGroupLock(groupId, () -> doStop(groupId));
    }

    /**
     * Stops every table that may still have a live job and keeps going past one that fails, so a
     * single unreachable job does not leave its siblings running. A failed table is parked FAILED
     * with the reason (the group then aggregates to FAILED, which raises an alert); stop can simply
     * be retried, and tables already STOPPED / FINISHED are skipped on the retry.
     */
    private SyncTaskGroupOperationResult doStop(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource target = requireTarget(group);
        List<String> failed = new ArrayList<>();
        for (SyncTaskGroupItem item : items(groupId)) {
            if (StringUtils.isBlank(item.getEngineJobId())
                || SyncStatus.STOPPED.equals(item.getStatus()) || SyncStatus.FINISHED.equals(item.getStatus())) continue;
            try {
                stopItem(item, target, false, SyncStatus.STOPPED);
            } catch (RuntimeException ex) {
                isolateItem(item, "停止失败：" + StringUtils.defaultIfBlank(ex.getMessage(), "引擎不可达"));
                failed.add(item.getSourceTable());
            }
        }
        group.setStatus(failed.isEmpty() ? SyncStatus.STOPPED : GroupStatuses.aggregate(itemStatuses(groupId)));
        group.setLastError(failed.isEmpty() ? "" : SyncText.truncateForColumn(
            "以下表停止失败，请确认引擎状态后重试停止：" + String.join("、", failed)));
        groupMapper.updateById(group);
        if (!failed.isEmpty()) {
            return SyncTaskGroupOperationResult.partial(group, failed.size() + " 张表停止失败，其余已停止：" + String.join("、", failed));
        }
        return SyncTaskGroupOperationResult.of(group, "任务组已停止");
    }

    @Override
    public SyncTaskGroupStatus refreshStatus(Long groupId) {
        return locks.withGroupLock(groupId, () -> doRefreshStatus(groupId));
    }

    private SyncTaskGroupStatus doRefreshStatus(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource target = requireTarget(group);
        boolean kafkaGroup = DataSourceType.isKafka(target);
        DataSource source = kafkaGroup ? requireSource(group) : null;
        // Items that are gone with their group are forgotten by the runner on delete; see forgetFailureStreaks.
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
                EngineJobRunner.Poll poll = runner.poll(item.getItemId(), item.getEngineJobId());
                if (poll instanceof EngineJobRunner.Poll.Observed observed) {
                    statuses.add(applyObserved(group, item, source, target, kafkaGroup, observed, itemStatus));
                } else if (poll instanceof EngineJobRunner.Poll.Unreachable unreachable) {
                    // Transient engine unreachability - hold the item's last-known status and its
                    // bridge; only give up after EngineJobRunner.STATUS_FAILURE_TOLERANCE.
                    itemStatus.setStatus(item.getStatus());
                    itemStatus.setErrorMessage("SeaTunnel 状态暂不可达（第 " + unreachable.streak() + "/" + EngineJobRunner.STATUS_FAILURE_TOLERANCE
                        + " 次）：" + SyncText.truncateForColumn(StringUtils.defaultIfBlank(unreachable.error(), "")));
                    statuses.add(item.getStatus());
                } else {
                    // Given up on (Lost) or its recovery state is gone (Boundary): the table is parked.
                    String error = poll instanceof EngineJobRunner.Poll.Lost lost ? lost.error() : ((EngineJobRunner.Poll.Boundary) poll).error();
                    if (kafkaGroup) runner.dropBridge(item.getItemId());
                    isolateItem(item, error);
                    itemStatus.setStatus(SyncStatus.FAILED);
                    itemStatus.setErrorMessage(SyncText.truncateForColumn(error));
                    statuses.add(SyncStatus.FAILED);
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

    /**
     * The engine answered for this item: status (an open DDL event keeps it DDL_BLOCKED whatever
     * the engine says), bridge iff RUNNING, error, checkpoint, metrics. Returns the status to
     * aggregate. A failure in here is not an engine failure - the metadata database or the
     * metrics store hiccuped - so it holds the item's last-known status instead of counting
     * toward giving the job up, and the next pass retries.
     */
    private String applyObserved(SyncTaskGroup group, SyncTaskGroupItem item, DataSource source, DataSource target,
                                 boolean kafkaGroup, EngineJobRunner.Poll.Observed observed, SyncTaskGroupItemStatus itemStatus) {
        String known = item.getStatus();
        var snapshot = observed.snapshot();
        try {
            boolean ddlBlocked = ddlEventMapper.selectLatestOpen(item.getItemId()) != null;
            item.setStatus(ddlBlocked ? SyncStatus.DDL_BLOCKED : observed.platformStatus());
            String bridgeError = null;
            if (kafkaGroup) {
                if (SyncStatus.RUNNING.equals(item.getStatus())) bridgeError = runner.healBridge(GroupItemOperations.job(group, item, source, target));
                else runner.dropBridge(item.getItemId());
            }
            item.setLastError(SyncText.truncateForColumn(StringUtils.defaultIfBlank(snapshot.errorMessage(),
                StringUtils.defaultIfBlank(bridgeError, StringUtils.defaultIfBlank(observed.checkpointError(), "")))));
            var checkpoint = observed.checkpoint();
            if (checkpoint.id() != null) {
                item.setLastCheckpointId(checkpoint.id());
                item.setLastCheckpointTime(checkpoint.time() == null ? null : checkpoint.time().toString());
                item.setLastCheckpointStatus(checkpoint.status());
            }
            itemMapper.updateById(item);
            itemStatus.setEngineStatus(snapshot.status());
            itemStatus.setStatus(item.getStatus());
            itemStatus.setErrorMessage(StringUtils.isBlank(item.getLastError()) ? null : item.getLastError());
            EngineJobStates.applyMetrics(itemStatus, snapshot, group.getSyncMode());
            metricsService.recordGroupItem(item, snapshot.status(), itemStatus);
            return item.getStatus();
        } catch (RuntimeException ex) {
            item.setStatus(known);
            itemStatus.setEngineStatus(snapshot.status());
            itemStatus.setStatus(known);
            itemStatus.setErrorMessage("已读取引擎状态，但未能保存（下一轮重试）：" + SyncText.safeMessage(ex, "未知错误"));
            return known;
        }
    }

    /** Reconcile persisted group state with SeaTunnel after a platform restart. */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverRunningGroups() {
        groupMapper.selectActive().forEach(group -> {
            try {
                SyncTaskGroupStatus status = refreshStatus(group.getGroupId());
                if (!GroupStatuses.isLive(status.getStatus())) return;
                DataSource source = requireSource(group);
                DataSource target = requireTarget(group);
                if (!DataSourceType.isKafka(target)) return;
                for (SyncTaskGroupItem item : items(group.getGroupId())) {
                    if (SyncStatus.RUNNING.equals(item.getStatus())) runner.healBridge(GroupItemOperations.job(group, item, source, target));
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
    @Scheduled(fixedDelayString = "${sync.status-refresh.interval-ms:30000}", initialDelayString = "${sync.status-refresh.initial-delay-ms:25000}", scheduler = SyncSchedulingConfig.SCHEDULER)
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

    // ------------------------------------------------------------------ save helpers

    private SyncTaskGroup normalize(SyncTaskGroupBo bo, SyncTaskGroup current) {
        SyncTaskGroup entity = current == null ? new SyncTaskGroup() : current;
        entity.setGroupName(bo.getGroupName());
        entity.setSourceId(bo.getSourceId());
        entity.setTargetId(bo.getTargetId());
        entity.setSyncScope(StringUtils.defaultIfBlank(bo.getSyncScope(), SyncScope.MULTI_TABLE).toUpperCase(Locale.ROOT));
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
        if (!List.of(SyncScope.MULTI_TABLE, SyncScope.DATABASE).contains(entity.getSyncScope())) throw new ServiceException("同步粒度仅支持多表或整库");
        if (!isDatabaseScope(entity) && (bo.getItems() == null || bo.getItems().isEmpty())) {
            throw new ServiceException("至少选择一张表");
        }
        if (isDatabaseScope(entity) && StringUtils.isBlank(entity.getSourceDatabase())) {
            entity.setSourceDatabase(source.getDatabaseName());
        }
        int maxTables = groupProperties.effectiveMaxTables();
        if (bo.getItems() != null && bo.getItems().size() > maxTables) {
            throw new ServiceException("单个任务组最多支持 " + maxTables + " 张表（sync.group.max-tables）");
        }
        resourceProtectionPolicy.applyDefaultsAndValidate(entity);
        return entity;
    }

    private void replaceItems(SyncTaskGroup group, List<SyncTaskGroupItemBo> itemBos) {
        forgetFailureStreaks(group.getGroupId());
        // The events belong to the item ids that are about to disappear.
        ddlEventMapper.deleteByGroupId(group.getGroupId());
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
            DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
            GroupItemOperations.applySelection(item, metadata);
            TableSchemaSnapshot.baseline(item, metadata);
            item.setStatus(SyncStatus.PENDING);
            itemMapper.insert(item);
        }
    }

    private void attachItems(SyncTaskGroupVo group) {
        attachItems(List.of(group));
    }

    /**
     * Fills in every group's table items and their latest metrics with two queries in total.
     * Doing it per row made a page of ten groups issue twenty-one queries.
     */
    private void attachItems(List<SyncTaskGroupVo> groups) {
        if (groups.isEmpty()) return;
        List<SyncTaskGroupItemVo> items = MapstructUtils.convert(
            itemMapper.selectByGroupIds(groups.stream().map(SyncTaskGroupVo::getGroupId).toList()),
            SyncTaskGroupItemVo.class);
        if (items == null) items = List.of();
        Map<Long, SyncMetricsSampleVo> latest =
            metricsService.latestForGroupItems(items.stream().map(SyncTaskGroupItemVo::getItemId).toList());
        items.forEach(item -> {
            item.setLatestMetrics(latest.get(item.getItemId()));
            item.setLastError(kafkaTaskBridgeService.decorateLastError(item.getItemId(), item.getLastError()));
        });
        Map<Long, List<SyncTaskGroupItemVo>> byGroup = items.stream()
            .collect(Collectors.groupingBy(SyncTaskGroupItemVo::getGroupId));
        groups.forEach(group -> group.setItems(byGroup.getOrDefault(group.getGroupId(), List.of())));
    }

    private static boolean hasLiveJob(SyncTaskGroupItem item) {
        return SyncStatus.isActive(item.getStatus()) && StringUtils.isNotBlank(item.getEngineJobId());
    }

    // ------------------------------------------------------------------ engine helpers

    /** Engine-first stop / savepoint pause of one table, then its new status. */
    private void stopItem(SyncTaskGroupItem item, DataSource target, boolean withSavepoint, String newStatus) {
        runner.halt(item.getItemId(), DataSourceType.isKafka(target), item.getEngineJobId(), withSavepoint);
        item.setStatus(newStatus);
        itemMapper.updateById(item);
    }

    /**
     * Drops the in-memory poll-failure counters of a group's items. Items are re-created with
     * fresh ids on every save and deleted with the group, so without this the map would keep
     * an entry per item that ever existed in this process.
     */
    private void forgetFailureStreaks(Long groupId) {
        items(groupId).forEach(item -> runner.forget(item.getItemId()));
    }

    /**
     * A database-only mutation of one group: the group lock is taken first and the transaction
     * runs inside it, so the change is committed before the lock is released and whoever takes
     * the lock next reads it. (An {@code @Transactional} method wrapping the lock does the
     * opposite: it commits after the lock is already free.)
     */
    private <T> T inLockedTransaction(Long groupId, Supplier<T> action) {
        return locks.withGroupLock(groupId, () -> transactionTemplate.execute(status -> action.get()));
    }

    private void isolateItem(SyncTaskGroupItem item, String error) {
        itemOps.isolate(item, error);
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
                if (SyncStatus.FAILED.equals(item.getStatus()) && itemOps.validateDiscoveredItem(source, target, item) == null) {
                    TableSchemaSnapshot.baseline(item, itemOps.readSourceMetadata(item, source));
                    item.setStatus(SyncStatus.PENDING);
                    item.setLastError("");
                    itemMapper.updateById(item);
                }
            } catch (RuntimeException ignored) {
                // Leave the table isolated; the start loop surfaces the reason per table.
            }
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

    private static boolean isDatabaseScope(SyncTaskGroup group) {
        return SyncScope.isDatabase(group.getSyncScope());
    }

    private static int nextConfigVersion(SyncTaskGroup group) {
        return (group.getConfigVersion() == null ? 1 : group.getConfigVersion()) + 1;
    }

    /** Swaps one job id for another in the group's comma-separated list (appends when the old one is absent). */
    private static String replaceJobId(String current, String oldJobId, String newJobId) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(current)) values.addAll(List.of(current.split(",")));
        if (StringUtils.isNotBlank(oldJobId)) values.remove(oldJobId);
        if (StringUtils.isNotBlank(newJobId)) values.add(newJobId);
        return String.join(",", values);
    }
}
