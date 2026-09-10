package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.domain.PageResult;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.common.json.utils.JsonUtils;
import org.dromara.common.mybatis.core.page.PageQuery;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.KafkaOutputFormat;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupDdlEvent;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.bo.SyncTaskGroupBo;
import org.dromara.sync.domain.bo.SyncTaskGroupItemBo;
import org.dromara.sync.domain.vo.ConnectionTestResult;
import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskGroupConfigPreview;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckItemResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlEventVo;
import org.dromara.sync.domain.vo.SyncTaskGroupItemStatus;
import org.dromara.sync.domain.vo.SyncTaskGroupItemVo;
import org.dromara.sync.domain.vo.SyncTaskGroupItemValidationVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupStatus;
import org.dromara.sync.domain.vo.SyncTaskGroupValidationResult;
import org.dromara.sync.domain.vo.SyncTaskGroupVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskGroupDdlEventMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.ISyncTaskGroupService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Metadata and validation boundary for multi-table releases. */
@RequiredArgsConstructor
@Service
public class SyncTaskGroupServiceImpl implements ISyncTaskGroupService {

    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SyncTaskGroupDdlEventMapper ddlEventMapper;
    private final DataSourceMapper dataSourceMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final IDataConsistencyService dataConsistencyService;
    private final SeaTunnelProperties properties;
    private final SeaTunnelRestClient restClient;
    private final ResourceProtectionPolicy resourceProtectionPolicy;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;

    /** Consecutive engine status-poll failures per group item; see the task-side counterpart. */
    private final java.util.concurrent.ConcurrentMap<Long, Integer> itemStatusFailureStreak =
        new java.util.concurrent.ConcurrentHashMap<>();
    private static final int ITEM_STATUS_FAILURE_TOLERANCE = 3;

    @Override
    public PageResult<SyncTaskGroupVo> queryPageList(String groupName, String status, PageQuery pageQuery) {
        LambdaQueryWrapper<SyncTaskGroup> wrapper = new LambdaQueryWrapper<SyncTaskGroup>()
            .like(StringUtils.isNotBlank(groupName), SyncTaskGroup::getGroupName, groupName)
            .eq(StringUtils.isNotBlank(status), SyncTaskGroup::getStatus, status)
            .orderByDesc(SyncTaskGroup::getGroupId);
        Page<SyncTaskGroup> page = groupMapper.selectPage(pageQuery.build(), wrapper);
        List<SyncTaskGroupVo> records = page.getRecords().stream().map(this::toVo).toList();
        records.forEach(this::attachItems);
        return PageResult.build(records, page.getTotal());
    }

    @Override
    public SyncTaskGroupVo queryById(Long groupId) {
        SyncTaskGroupVo result = toVo(groupMapper.selectById(groupId));
        if (result != null) attachItems(result);
        return result;
    }

    @Override
    @Transactional
    public Boolean insertByBo(SyncTaskGroupBo bo) {
        SyncTaskGroup entity = normalize(bo, null);
        entity.setStatus("DRAFT");
        entity.setConfigVersion(1);
        groupMapper.insert(entity);
        replaceItems(entity.getGroupId(), bo.getItems());
        if ("DATABASE".equals(entity.getSyncScope())) discover(entity.getGroupId());
        return true;
    }

    @Override
    @Transactional
    public Boolean updateByBo(SyncTaskGroupBo bo) {
        SyncTaskGroup current = requireGroup(bo.getGroupId());
        if (!List.of("DRAFT", "STOPPED").contains(current.getStatus())) {
            throw new ServiceException("只有草稿或已停止任务组允许修改");
        }
        SyncTaskGroup entity = normalize(bo, current);
        entity.setConfigVersion((current.getConfigVersion() == null ? 1 : current.getConfigVersion()) + 1);
        groupMapper.updateById(entity);
        replaceItems(entity.getGroupId(), bo.getItems());
        if ("DATABASE".equals(entity.getSyncScope())) discover(entity.getGroupId());
        return true;
    }

    @Override
    @Transactional
    public Boolean deleteById(Long groupId) {
        SyncTaskGroup current = requireGroup(groupId);
        if (!List.of("DRAFT", "STOPPED", "FAILED", "FINISHED").contains(current.getStatus())) {
            throw new ServiceException("运行中的任务组不能删除");
        }
        itemMapper.delete(new LambdaQueryWrapper<SyncTaskGroupItem>().eq(SyncTaskGroupItem::getGroupId, groupId));
        return groupMapper.deleteById(groupId) > 0;
    }

    @Override
    public SyncTaskGroupValidationResult validate(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
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
        for (SyncTaskGroupItem item : items(groupId)) {
            SyncTaskGroupItemValidationVo itemResult = new SyncTaskGroupItemValidationVo();
            itemResult.setItemId(item.getItemId());
            itemResult.setSourceTable(item.getSourceTable());
            itemResult.setTargetTable(displayTargetTable(target, item.getTargetSchema(), item.getTargetTable()));
            try {
                DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(),
                    StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName()), item.getSourceTable());
                boolean hasKey = !SyncColumnSelectionValidator.validate(metadata,
                    item.getSelectedColumns(), item.getSyncKeyColumns()).syncKeyColumns().isEmpty();
                boolean keyRequired = !"FULL".equalsIgnoreCase(group.getSyncMode()) || isKafkaTarget(target);
                TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                    item.getSourceTable(), item.getTargetSchema(), item.getTargetTable(), item.getSelectedColumns(), item.getSyncKeyColumns());
                itemResult.setTargetCompatibility(compatibility);
                itemResult.setPassed((!keyRequired || hasKey) && compatibility.isPassed());
                itemResult.setMessage(itemResult.isPassed() ? "表结构和同步键校验通过" : (keyRequired && !hasKey ? "源表没有可用同步键" : compatibility.getMessage()));
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
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        SyncTaskGroupConfigGenerator.GeneratedConfig generated = SyncTaskGroupConfigGenerator.generate(group, items(groupId), source, target, properties);
        SyncTaskGroupConfigPreview result = new SyncTaskGroupConfigPreview();
        result.setGroupId(groupId);
        result.setGroupName(group.getGroupName());
        result.setConfigVersion(group.getConfigVersion());
        result.setEngineJobName(generated.jobName());
        result.setTableCount(items(groupId).size());
        result.setConfig(generated.redactedConfig());
        return result;
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult start(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (List.of("RUNNING", "PAUSING").contains(group.getStatus())) throw new ServiceException("任务组当前正在运行");
        resourceProtectionPolicy.applyDefaultsAndValidate(group);
        groupMapper.updateById(group);
        boolean databaseScope = "DATABASE".equals(group.getSyncScope());
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        if (databaseScope && isKafkaTarget(target)) recoverDatabaseKafkaTopics(groupId, source, target);
        SyncTaskGroupValidationResult validation = validate(groupId);
        boolean infrastructureValid = validation.getSource() != null && validation.getSource().isSuccess()
            && validation.getTarget() != null && validation.getTarget().isSuccess()
            && validation.getCdcPrecheck() != null && Boolean.TRUE.equals(validation.getCdcPrecheck().getPassed());
        if (!validation.isValid() && (!databaseScope || !infrastructureValid)) {
            throw new ServiceException("启动前校验未通过：" + validation.getMessage());
        }
        List<String> jobIds = new java.util.ArrayList<>();
        List<SyncTaskGroupItem> submittedItems = new java.util.ArrayList<>();
        SyncTaskGroupItem currentItem = null;
        int isolatedFailures = 0;
        try {
            for (SyncTaskGroupItem item : items(groupId)) {
                if (databaseScope && ("FAILED".equals(item.getStatus()) || "DDL_BLOCKED".equals(item.getStatus()))) {
                    isolatedFailures++;
                    continue;
                }
                currentItem = item;
                try {
                    captureSchemaBaseline(item, source);
                    var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties);
                    var submitted = restClient.submit(generated.jobName(), generated.config(), null, false);
                    item.setEngineJobId(submitted.jobId());
                    item.setEngineConfigHash(hash(generated.config()));
                    item.setStatus("RUNNING");
                    item.setLastError("");
                    itemMapper.updateById(item);
                    if (isKafkaTarget(target)) {
                        kafkaTaskBridgeService.startGroupItem(SyncTaskGroupConfigGenerator.toTask(group, item), target,
                            source.getDatabaseName());
                    }
                    jobIds.add(submitted.jobId());
                    submittedItems.add(item);
                } catch (RuntimeException ex) {
                    if (!databaseScope) throw ex;
                    item.setStatus("FAILED");
                    item.setLastError(truncateForColumn(ex.getMessage()));
                    itemMapper.updateById(item);
                    isolatedFailures++;
                }
            }
            group.setEngineJobId(String.join(",", jobIds));
            group.setEngineConfigHash(hash(String.join("\n", jobIds)));
            group.setStatus(databaseScope && isolatedFailures > 0 ? "DEGRADED" : "RUNNING");
            group.setLastError(isolatedFailures == 0 ? "" : "已隔离 " + isolatedFailures + " 张失败表，其他表继续运行");
            groupMapper.updateById(group);
            return operation(group, databaseScope
                ? "整库任务已提交 " + jobIds.size() + " 个作业，隔离失败 " + isolatedFailures + " 张"
                : "任务组已提交 " + jobIds.size() + " 个作业");
        } catch (RuntimeException ex) {
            String error = StringUtils.defaultIfBlank(ex.getMessage(), "SeaTunnel 作业提交失败");
            for (SyncTaskGroupItem submittedItem : submittedItems) {
                try {
                    restClient.stop(submittedItem.getEngineJobId(), false, false);
                    if (isKafkaTarget(target)) kafkaTaskBridgeService.stop(submittedItem.getItemId());
                    submittedItem.setStatus("STOPPED");
                } catch (RuntimeException stopError) {
                    submittedItem.setStatus("FAILED");
                    error = error + "; 补偿停止失败: " + StringUtils.defaultIfBlank(stopError.getMessage(), "引擎不可达");
                }
                submittedItem.setLastError(truncateForColumn("组启动失败，已执行补偿停止"));
                itemMapper.updateById(submittedItem);
            }
            if (currentItem != null && !submittedItems.contains(currentItem)) {
                currentItem.setStatus("FAILED");
                currentItem.setLastError(truncateForColumn(error));
                itemMapper.updateById(currentItem);
            }
            group.setEngineJobId(String.join(",", jobIds));
            group.setStatus("FAILED");
            group.setLastError(truncateForColumn(error));
            groupMapper.updateById(group);
            return operation(group, "任务组启动失败，已补偿停止已提交作业：" + error);
        }
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult discover(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!"DATABASE".equals(group.getSyncScope())) {
            throw new ServiceException("仅整库同步任务组支持发现新增表");
        }
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        String database = StringUtils.defaultIfBlank(group.getSourceDatabase(), source.getDatabaseName());
        Set<String> existing = new HashSet<>();
        for (SyncTaskGroupItem item : items(groupId)) existing.add(item.getSourceTable().toLowerCase(Locale.ROOT));

        int discovered = 0;
        int started = 0;
        int failed = 0;
        List<String> jobIds = new java.util.ArrayList<>();
        for (String table : metadataService.queryTables(source.getSourceId(), database)) {
            if (!existing.add(table.toLowerCase(Locale.ROOT))) continue;
            SyncTaskGroupItem item = new SyncTaskGroupItem();
            item.setGroupId(groupId);
            item.setSourceDatabase(database);
            item.setSourceTable(table);
            item.setTargetSchema("public");
            item.setTargetTable(table);
            item.setDdlPolicy(group.getDdlPolicy());
            item.setStatus("PENDING");
            String validationError;
            try {
                applySelection(item, source);
                if (isKafkaTarget(target)) kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());
                validationError = validateDiscoveredItem(source, target, item);
            } catch (RuntimeException ex) {
                validationError = ex.getMessage();
            }
            if (validationError != null) {
                item.setStatus("FAILED");
                item.setLastError(truncateForColumn(validationError));
                failed++;
            } else {
                captureSchemaBaseline(item, source);
            }
            itemMapper.insert(item);
            discovered++;
            if (validationError == null && isGroupActive(group)) {
                try {
                    var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties);
                    var submitted = restClient.submit(generated.jobName(), generated.config(), null, false);
                    item.setEngineJobId(submitted.jobId());
                    item.setEngineConfigHash(hash(generated.config()));
                    item.setStatus("RUNNING");
                    item.setLastError("");
                    itemMapper.updateById(item);
                    if (isKafkaTarget(target)) {
                        kafkaTaskBridgeService.startGroupItem(SyncTaskGroupConfigGenerator.toTask(group, item), target,
                            source.getDatabaseName());
                    }
                    jobIds.add(submitted.jobId());
                    started++;
                } catch (RuntimeException ex) {
                    item.setStatus("FAILED");
                    item.setLastError(truncateForColumn(ex.getMessage()));
                    itemMapper.updateById(item);
                    failed++;
                }
            }
        }
        if (discovered > 0) {
            group.setConfigVersion((group.getConfigVersion() == null ? 1 : group.getConfigVersion()) + 1);
            if (!jobIds.isEmpty()) group.setEngineJobId(appendJobIds(group.getEngineJobId(), jobIds));
            group.setLastError(failed == 0 ? "" : "新增表发现完成，其中 " + failed + " 张表校验或提交失败，请查看表项错误");
            groupMapper.updateById(group);
        }
        return operation(group, discovered == 0 ? "未发现新增表" : "发现 " + discovered + " 张新表，已启动 " + started + " 张，失败 " + failed + " 张");
    }

    /**
     * Compare each table with the source schema captured at its last successful start.
     * The MVP intentionally never changes the target schema automatically.
     */
    @Override
    @Transactional
    public SyncTaskGroupDdlCheckResult checkDdl(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        SyncTaskGroupDdlCheckResult result = new SyncTaskGroupDdlCheckResult();
        result.setGroupId(groupId);
        Map<Long, SyncTaskGroupItem> itemIndex = items(groupId).stream()
            .collect(Collectors.toMap(SyncTaskGroupItem::getItemId, item -> item));

        int newlyDetected = 0;
        int readyToResume = 0;
        for (SyncTaskGroupItem item : itemIndex.values()) {
            DataSourceMetadataVo current;
            try {
                current = metadataService.queryTableMetadata(source.getSourceId(),
                    StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName()), item.getSourceTable());
            } catch (RuntimeException ex) {
                SyncTaskGroupDdlEvent event = upsertDdlEvent(group, item, null, "TABLE_UNAVAILABLE", "HIGH",
                    "无法读取源表结构：" + safeMessage(ex),
                    "确认源表仍存在且同步账号具有读取元数据权限后重新执行结构检查。", "PENDING_FIX");
                isolateDdlItem(item, event);
                result.getEvents().add(toDdlEventVo(event, item, target));
                newlyDetected++;
                continue;
            }

            SchemaSnapshot snapshot = snapshotOf(current);
            String currentJson = JsonUtils.toJsonString(snapshot);
            String currentHash = hash(currentJson);
            if (StringUtils.isBlank(item.getSchemaSnapshot())) {
                // Existing tasks created before migration 008 receive a baseline on their
                // first successful check; subsequent checks are real runtime DDL checks.
                TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                    item.getSourceTable(), item.getTargetSchema(), item.getTargetTable());
                if (compatibility.isPassed()) {
                    item.setSchemaSnapshot(currentJson);
                    item.setSchemaHash(currentHash);
                    itemMapper.updateById(item);
                }
                continue;
            }

            SchemaDiff diff = diff(JsonUtils.parseObject(item.getSchemaSnapshot(), SchemaSnapshot.class), snapshot);
            SyncTaskGroupDdlEvent existing = latestOpenDdlEvent(item.getItemId());
            if (!diff.changed()) {
                if (existing != null) {
                    existing.setStatus("READY_TO_RESUME");
                    existing.setDetails("源表结构已恢复为启动快照。请确认目标端结构后恢复该表。" );
                    existing.setRemediation("确认目标端仍与源端兼容后，使用“恢复该表”继续从 savepoint 同步。");
                    ddlEventMapper.updateById(existing);
                    result.getEvents().add(toDdlEventVo(existing, item, target));
                    readyToResume++;
                }
                continue;
            }

            TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                item.getSourceTable(), item.getTargetSchema(), item.getTargetTable(), item.getSelectedColumns(), item.getSyncKeyColumns());
            String eventStatus = compatibility.isPassed() ? "READY_TO_RESUME" : "PENDING_FIX";
            SyncTaskGroupDdlEvent event = upsertDdlEvent(group, item, currentHash, diff.changeType(), diff.riskLevel(),
                diff.details(), remediation(diff, compatibility), eventStatus);
            isolateDdlItem(item, event);
            result.getEvents().add(toDdlEventVo(event, item, target));
            if ("READY_TO_RESUME".equals(eventStatus)) readyToResume++; else newlyDetected++;
        }

        List<String> statuses = items(groupId).stream().map(SyncTaskGroupItem::getStatus).toList();
        group.setStatus(aggregateStatus(statuses));
        group.setLastError(newlyDetected == 0 ? "" : "检测到 " + newlyDetected + " 张表存在待修复的结构变更");
        groupMapper.updateById(group);
        result.setStatus(group.getStatus());
        result.setMessage(result.getEvents().isEmpty() ? "未发现运行中表结构变更"
            : "检测到 " + result.getEvents().size() + " 条结构变更事件，其中 " + readyToResume + " 条已可恢复");
        return result;
    }

    /** Executes independent, bounded COUNT(*) comparisons for all task-group table items. */
    @Override
    public SyncTaskGroupDataCheckResult checkData(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        List<SyncTaskGroupItem> groupItems = items(groupId);
        SyncTaskGroupDataCheckResult result = new SyncTaskGroupDataCheckResult();
        result.setGroupId(groupId);
        result.setTableCount(groupItems.size());
        if (isKafkaTarget(target)) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setConsistencyNote("Kafka 目标使用事件核对口径，不执行关系型目标行数核对。");
            result.setMessage("Kafka 任务组请通过 topic 的 key、offset、分区和事件信封进行核对");
            return result;
        }
        result.setConsistencyNote(List.of("RUNNING", "DEGRADED", "PAUSING").contains(group.getStatus())
            ? "任务仍在持续同步，当前为非同水位的行数检查；暂停或确认两端水位稳定后再做严格验收。"
            : "当前结果为源端与目标端的只读行数检查，不替代基于同步键的逐行校验。");

        for (SyncTaskGroupItem item : groupItems) {
            SyncTaskGroupDataCheckItemResult itemResult = new SyncTaskGroupDataCheckItemResult();
            itemResult.setItemId(item.getItemId());
            try {
                SyncTaskDataCheckResult tableResult = dataConsistencyService.check(source, target,
                    StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName()), item.getSourceTable(),
                    StringUtils.defaultIfBlank(item.getTargetSchema(), "public"), item.getTargetTable());
                itemResult.setSourceTable(tableResult.getSourceTable());
                itemResult.setTargetTable(tableResult.getTargetTable());
                itemResult.setSourceRows(tableResult.getSourceRows());
                itemResult.setTargetRows(tableResult.getTargetRows());
                itemResult.setDifference(tableResult.getDifference());
                itemResult.setMatched(tableResult.isMatched());
                itemResult.setSuccess(tableResult.isSuccess());
                itemResult.setMessage(tableResult.getMessage());
            } catch (RuntimeException ex) {
                itemResult.setSourceTable(StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName()) + "." + item.getSourceTable());
                itemResult.setTargetTable(displayTargetTable(target, item.getTargetSchema(), item.getTargetTable()));
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

    @Override
    @Transactional
    public SyncTaskGroupOperationResult resumeDdlItem(Long groupId, Long itemId) {
        SyncTaskGroup group = requireGroup(groupId);
        SyncTaskGroupItem item = itemMapper.selectById(itemId);
        if (item == null || !groupId.equals(item.getGroupId())) throw new ServiceException("表项不存在或不属于当前任务组");
        checkDdl(groupId);
        SyncTaskGroupDdlEvent event = latestOpenDdlEvent(itemId);
        if (event == null || !"READY_TO_RESUME".equals(event.getStatus())) {
            throw new ServiceException("表结构尚未修复或未通过兼容性检查，请先执行结构检查并按修复建议处理");
        }
        if (StringUtils.isBlank(item.getEngineJobId())) throw new ServiceException("表项没有可恢复的引擎作业");
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties);
        if (StringUtils.isNotBlank(item.getEngineConfigHash()) && !item.getEngineConfigHash().equals(hash(generated.config()))) {
            throw new ServiceException("表结构变更导致引擎配置版本变化，不能直接从原 savepoint 恢复，请创建新配置版本并重新初始化该表");
        }
        restClient.submit(generated.jobName(), generated.config(), item.getEngineJobId(), true);
        captureSchemaBaseline(item, source);
        item.setStatus("RUNNING");
        item.setLastError("");
        itemMapper.updateById(item);
        event.setStatus("RESOLVED");
        event.setResolvedAt(LocalDateTime.now());
        ddlEventMapper.updateById(event);
        group.setStatus(aggregateStatus(items(groupId).stream().map(SyncTaskGroupItem::getStatus).toList()));
        group.setLastError("");
        groupMapper.updateById(group);
        return operation(group, "表 " + item.getSourceTable() + " 已通过结构校验并恢复");
    }

    @Override
    @Transactional
    public SyncTaskGroupStatus refreshStatus(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource groupTarget = dataSourceMapper.selectById(group.getTargetId());
        boolean kafkaGroup = isKafkaTarget(groupTarget);
        String sourceDatabase = kafkaGroup
            ? requireSource(group.getSourceId(), "源").getDatabaseName() : null;
        List<String> statuses = new java.util.ArrayList<>();
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
                    String status = mapStatus(snapshot.status());
                    boolean ddlBlocked = latestOpenDdlEvent(item.getItemId()) != null;
                    item.setStatus(ddlBlocked ? "DDL_BLOCKED" : status);
                    if (kafkaGroup && "RUNNING".equals(status) && !ddlBlocked
                        && !kafkaTaskBridgeService.isRunning(item.getItemId())) {
                        // Engine job alive, bridge dead - heal it (mirrors the task-side reconcile).
                        kafkaTaskBridgeService.startGroupItem(
                            SyncTaskGroupConfigGenerator.toTask(group, item), groupTarget, sourceDatabase);
                    }
                    item.setLastError(truncateForColumn(StringUtils.defaultIfBlank(snapshot.errorMessage(), "")));
                    try {
                        var checkpoint = restClient.checkpoints(item.getEngineJobId());
                        if (checkpoint.id() != null) {
                            item.setLastCheckpointId(checkpoint.id());
                            item.setLastCheckpointTime(checkpoint.time() == null ? null : checkpoint.time().toString());
                            item.setLastCheckpointStatus(checkpoint.status());
                        }
                    } catch (RuntimeException checkpointError) {
                        if (StringUtils.isBlank(item.getLastError())) {
                            item.setLastError(truncateForColumn(checkpointError.getMessage()));
                        }
                    }
                    itemMapper.updateById(item);
                    itemStatus.setEngineStatus(snapshot.status());
                    itemStatus.setStatus(item.getStatus());
                    itemStatus.setErrorMessage(StringUtils.isBlank(snapshot.errorMessage())
                        ? null : truncateForColumn(snapshot.errorMessage()));
                    applyMetrics(itemStatus, snapshot);
                    statuses.add(status);
                } catch (RuntimeException ex) {
                    int streak = itemStatusFailureStreak.merge(item.getItemId(), 1, Integer::sum);
                    if (streak < ITEM_STATUS_FAILURE_TOLERANCE && !isRecoveryBoundaryError(ex.getMessage())) {
                        // Transient engine unreachability - hold the item's last-known status
                        // and its bridge; only fail it after ITEM_STATUS_FAILURE_TOLERANCE.
                        itemStatus.setStatus(item.getStatus());
                        itemStatus.setErrorMessage("SeaTunnel 状态暂不可达（第 " + streak + "/"
                            + ITEM_STATUS_FAILURE_TOLERANCE + " 次）：" + truncateForColumn(StringUtils.defaultIfBlank(ex.getMessage(), "")));
                        statuses.add(item.getStatus());
                    } else {
                        itemStatusFailureStreak.remove(item.getItemId());
                        item.setStatus("FAILED");
                        item.setLastError(truncateForColumn(ex.getMessage()));
                        itemMapper.updateById(item);
                        itemStatus.setStatus("FAILED");
                        itemStatus.setErrorMessage(truncateForColumn(ex.getMessage()));
                        statuses.add("FAILED");
                    }
                }
            }
            result.getItems().add(itemStatus);
        }
        String aggregate = aggregateStatus(statuses);
        group.setStatus(aggregate);
        groupMapper.updateById(group);
        result.setStatus(aggregate);
        result.setMessage(aggregate.equals("FAILED") ? "任务组存在失败表项" : "任务组状态已刷新");
        return result;
    }

    /** Reconcile persisted group state with SeaTunnel after a platform restart. */
    @EventListener(ApplicationReadyEvent.class)
    public void recoverRunningGroups() {
        groupMapper.selectList(new LambdaQueryWrapper<SyncTaskGroup>()
                .in(SyncTaskGroup::getStatus, "RUNNING", "PAUSING"))
            .forEach(group -> {
                try {
                    SyncTaskGroupStatus status = refreshStatus(group.getGroupId());
                    if ("RUNNING".equals(status.getStatus())) {
                        DataSource source = requireSource(group.getSourceId(), "源");
                        DataSource target = requireSource(group.getTargetId(), "目标");
                        if (isKafkaTarget(target)) {
                            for (SyncTaskGroupItem item : items(group.getGroupId())) {
                                if ("RUNNING".equals(item.getStatus())) {
                                    kafkaTaskBridgeService.startGroupItem(SyncTaskGroupConfigGenerator.toTask(group, item), target,
                                        source.getDatabaseName());
                                }
                            }
                        }
                    }
                } catch (RuntimeException ex) {
                    group.setStatus("FAILED");
                    group.setLastError(truncateForColumn(ex.getMessage()));
                    groupMapper.updateById(group);
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
        groupMapper.selectList(new LambdaQueryWrapper<SyncTaskGroup>()
                .in(SyncTaskGroup::getStatus, "RUNNING", "PAUSING"))
            .forEach(group -> {
                try {
                    refreshStatus(group.getGroupId());
                } catch (RuntimeException ex) {
                    group.setStatus("FAILED");
                    group.setLastError(truncateForColumn(ex.getMessage()));
                    groupMapper.updateById(group);
                }
            });
    }

    /** Runs only for database-scope groups that explicitly opted into new-table discovery. */
    @Scheduled(fixedDelayString = "${sync.discovery.interval-ms:60000}", initialDelayString = "${sync.discovery.initial-delay-ms:30000}")
    public void discoverDatabaseGroups() {
        groupMapper.selectList(new LambdaQueryWrapper<SyncTaskGroup>()
                .eq(SyncTaskGroup::getSyncScope, "DATABASE")
                .eq(SyncTaskGroup::getAutoDiscover, "1")
                .in(SyncTaskGroup::getStatus, "RUNNING", "DEGRADED"))
            .forEach(group -> {
                try {
                    discover(group.getGroupId());
                } catch (RuntimeException ignored) {
                    // An individual discovery pass must not prevent later scans or affect other groups.
                }
            });
    }

    /** Periodic DDL checks isolate only the changed table and leave healthy tables running. */
    @Scheduled(fixedDelayString = "${sync.ddl-check.interval-ms:60000}", initialDelayString = "${sync.ddl-check.initial-delay-ms:45000}")
    public void checkRunningGroupDdl() {
        groupMapper.selectList(new LambdaQueryWrapper<SyncTaskGroup>()
                .in(SyncTaskGroup::getStatus, "RUNNING", "DEGRADED"))
            .forEach(group -> {
                try {
                    checkDdl(group.getGroupId());
                } catch (RuntimeException ignored) {
                    // A metadata failure for one group cannot block checks for other groups.
                }
            });
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult pause(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!"RUNNING".equals(group.getStatus())) throw new ServiceException("只有运行中的任务组可以暂停");
        DataSource target = requireSource(group.getTargetId(), "目标");
        for (SyncTaskGroupItem item : items(groupId)) if (StringUtils.isNotBlank(item.getEngineJobId())) {
            if (isKafkaTarget(target)) kafkaTaskBridgeService.stop(item.getItemId());
            restClient.stop(item.getEngineJobId(), true, false);
            item.setStatus("PAUSING");
            itemMapper.updateById(item);
        }
        group.setStatus("PAUSING");
        groupMapper.updateById(group);
        return operation(group, "暂停请求已提交，请刷新状态确认 savepoint");
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult resume(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!List.of("PAUSED", "FAILED").contains(group.getStatus())) throw new ServiceException("只有已暂停或失败任务组可以恢复");
        if (items(groupId).stream().anyMatch(item -> latestOpenDdlEvent(item.getItemId()) != null)) {
            throw new ServiceException("任务组存在待处理的表结构变更，请在结构检查结果中逐表修复并恢复");
        }
        DataSource source = requireSource(group.getSourceId(), "源");
        DataSource target = requireSource(group.getTargetId(), "目标");
        for (SyncTaskGroupItem item : items(groupId)) if (StringUtils.isNotBlank(item.getEngineJobId())) {
            var generated = SyncTaskGroupConfigGenerator.generateItem(group, item, source, target, properties);
            if (StringUtils.isNotBlank(item.getEngineConfigHash()) && !item.getEngineConfigHash().equals(hash(generated.config()))) {
                throw new ServiceException("表 " + item.getSourceTable() + " 配置已变化，不能直接恢复");
            }
            restClient.submit(generated.jobName(), generated.config(), item.getEngineJobId(), true);
            item.setStatus("RUNNING");
            itemMapper.updateById(item);
            if (isKafkaTarget(target)) {
                kafkaTaskBridgeService.startGroupItem(SyncTaskGroupConfigGenerator.toTask(group, item), target,
                    source.getDatabaseName());
            }
        }
        group.setStatus("RUNNING");
        groupMapper.updateById(group);
        return operation(group, "任务组已从 savepoint 恢复");
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult stop(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource target = requireSource(group.getTargetId(), "目标");
        for (SyncTaskGroupItem item : items(groupId)) if (StringUtils.isNotBlank(item.getEngineJobId())) {
            if (isKafkaTarget(target)) kafkaTaskBridgeService.stop(item.getItemId());
            restClient.stop(item.getEngineJobId(), false, false);
            item.setStatus("STOPPED");
            itemMapper.updateById(item);
        }
        group.setStatus("STOPPED");
        groupMapper.updateById(group);
        return operation(group, "任务组已停止");
    }

    private SyncTaskGroup normalize(SyncTaskGroupBo bo, SyncTaskGroup current) {
        SyncTaskGroup entity = current == null ? new SyncTaskGroup() : current;
        entity.setGroupName(bo.getGroupName());
        entity.setSourceId(bo.getSourceId());
        entity.setTargetId(bo.getTargetId());
        entity.setSyncScope(StringUtils.defaultIfBlank(bo.getSyncScope(), "MULTI_TABLE").toUpperCase(Locale.ROOT));
        entity.setSourceDatabase(bo.getSourceDatabase());
        entity.setAutoDiscover("DATABASE".equals(entity.getSyncScope())
            && "1".equals(bo.getAutoDiscover()) ? "1" : "0");
        entity.setSyncMode(StringUtils.defaultIfBlank(bo.getSyncMode(), "FULL_CDC"));
        entity.setDdlPolicy(StringUtils.defaultIfBlank(bo.getDdlPolicy(), "FAIL"));
        entity.setReadLimitRowsPerSecond(bo.getReadLimitRowsPerSecond());
        entity.setReadLimitBytesPerSecond(bo.getReadLimitBytesPerSecond());
        entity.setSnapshotParallelism(bo.getSnapshotParallelism());
        entity.setSourceConnectionLimit(bo.getSourceConnectionLimit());
        if (entity.getSourceId().equals(entity.getTargetId())) throw new ServiceException("源数据源和目标数据源不能相同");
        DataSource source = requireSource(entity.getSourceId(), "源");
        DataSource target = requireSource(entity.getTargetId(), "目标");
        if (!"MYSQL".equalsIgnoreCase(source.getSourceType())) throw new ServiceException("多表 MVP 源端必须是 MySQL");
        if (!Set.of("POSTGRESQL", "MYSQL", "KAFKA").contains(StringUtils.defaultIfBlank(target.getSourceType(), "").toUpperCase())) {
            throw new ServiceException("多表 MVP 目标端必须是 PostgreSQL、MySQL 或 Kafka");
        }
        entity.setKafkaOutputFormat("KAFKA".equalsIgnoreCase(target.getSourceType())
            ? KafkaOutputFormat.parse(bo.getKafkaOutputFormat()).name() : null);
        String syncMode = entity.getSyncMode().toUpperCase(Locale.ROOT);
        if (!Set.of("FULL", "INCREMENTAL", "FULL_CDC").contains(syncMode)) {
            throw new ServiceException("不支持的同步模式：" + syncMode);
        }
        if (!List.of("MULTI_TABLE", "DATABASE").contains(entity.getSyncScope())) throw new ServiceException("同步粒度仅支持多表或整库");
        if (!"DATABASE".equals(entity.getSyncScope()) && (bo.getItems() == null || bo.getItems().isEmpty())) {
            throw new ServiceException("至少选择一张表");
        }
        if ("DATABASE".equals(entity.getSyncScope()) && StringUtils.isBlank(entity.getSourceDatabase())) {
            entity.setSourceDatabase(source.getDatabaseName());
        }
        if (bo.getItems() != null && bo.getItems().size() > 20) throw new ServiceException("单个任务组最多支持 20 张表");
        resourceProtectionPolicy.applyDefaultsAndValidate(entity);
        return entity;
    }

    private void replaceItems(Long groupId, List<SyncTaskGroupItemBo> itemBos) {
        itemMapper.delete(new LambdaQueryWrapper<SyncTaskGroupItem>().eq(SyncTaskGroupItem::getGroupId, groupId));
        if (itemBos == null) return;
        DataSource source = requireSource(requireGroup(groupId).getSourceId(), "源");
        for (SyncTaskGroupItemBo bo : itemBos) {
            // Group item BOs are nested in the group request and are not registered
            // as standalone MapStruct mappings by the code generator.
            SyncTaskGroupItem item = new SyncTaskGroupItem();
            item.setSourceDatabase(bo.getSourceDatabase());
            item.setSourceTable(bo.getSourceTable());
            item.setTargetSchema(bo.getTargetSchema());
            item.setTargetTable(bo.getTargetTable());
            item.setPrimaryKeys(bo.getPrimaryKeys());
            item.setDdlPolicy(bo.getDdlPolicy());
            item.setSelectedColumns(bo.getSelectedColumns());
            item.setSyncKeyColumns(bo.getSyncKeyColumns());
            item.setGroupId(groupId);
            item.setTargetSchema(StringUtils.defaultIfBlank(item.getTargetSchema(), "public"));
            item.setDdlPolicy(StringUtils.defaultIfBlank(item.getDdlPolicy(), "FAIL"));
            applySelection(item, source);
            item.setStatus("PENDING");
            itemMapper.insert(item);
        }
    }

    private void attachItems(SyncTaskGroupVo group) {
        group.setItems(itemMapper.selectList(new LambdaQueryWrapper<SyncTaskGroupItem>()
            .eq(SyncTaskGroupItem::getGroupId, group.getGroupId()))
            .stream().map(this::toItemVo).toList());
    }

    private SyncTaskGroupVo toVo(SyncTaskGroup entity) {
        if (entity == null) return null;
        SyncTaskGroupVo vo = new SyncTaskGroupVo();
        vo.setGroupId(entity.getGroupId());
        vo.setGroupName(entity.getGroupName());
        vo.setSourceId(entity.getSourceId());
        vo.setTargetId(entity.getTargetId());
        vo.setSyncScope(entity.getSyncScope());
        vo.setSourceDatabase(entity.getSourceDatabase());
        vo.setAutoDiscover(entity.getAutoDiscover());
        vo.setSyncMode(entity.getSyncMode());
        vo.setDdlPolicy(entity.getDdlPolicy());
        vo.setKafkaOutputFormat(entity.getKafkaOutputFormat());
        vo.setReadLimitRowsPerSecond(entity.getReadLimitRowsPerSecond());
        vo.setReadLimitBytesPerSecond(entity.getReadLimitBytesPerSecond());
        vo.setSnapshotParallelism(entity.getSnapshotParallelism());
        vo.setSourceConnectionLimit(entity.getSourceConnectionLimit());
        vo.setStatus(entity.getStatus());
        vo.setConfigVersion(entity.getConfigVersion());
        vo.setEngineJobId(entity.getEngineJobId());
        vo.setEngineConfigHash(entity.getEngineConfigHash());
        vo.setLastCheckpointId(entity.getLastCheckpointId());
        vo.setLastCheckpointTime(entity.getLastCheckpointTime());
        vo.setLastCheckpointStatus(entity.getLastCheckpointStatus());
        vo.setLastError(entity.getLastError());
        vo.setCreateTime(entity.getCreateTime() == null ? null : entity.getCreateTime().toString());
        vo.setUpdateTime(entity.getUpdateTime() == null ? null : entity.getUpdateTime().toString());
        return vo;
    }

    private SyncTaskGroupItemVo toItemVo(SyncTaskGroupItem entity) {
        SyncTaskGroupItemVo vo = new SyncTaskGroupItemVo();
        vo.setItemId(entity.getItemId());
        vo.setGroupId(entity.getGroupId());
        vo.setSourceDatabase(entity.getSourceDatabase());
        vo.setSourceTable(entity.getSourceTable());
        vo.setTargetSchema(entity.getTargetSchema());
        vo.setTargetTable(entity.getTargetTable());
        vo.setPrimaryKeys(entity.getPrimaryKeys());
        vo.setDdlPolicy(entity.getDdlPolicy());
        vo.setSelectedColumns(entity.getSelectedColumns());
        vo.setSyncKeyColumns(entity.getSyncKeyColumns());
        vo.setStatus(entity.getStatus());
        vo.setEngineJobId(entity.getEngineJobId());
        vo.setEngineConfigHash(entity.getEngineConfigHash());
        vo.setLastCheckpointId(entity.getLastCheckpointId());
        vo.setLastCheckpointTime(entity.getLastCheckpointTime());
        vo.setLastCheckpointStatus(entity.getLastCheckpointStatus());
        vo.setLastCheckSourceRows(entity.getLastCheckSourceRows());
        vo.setLastCheckTargetRows(entity.getLastCheckTargetRows());
        vo.setLastCheckDifference(entity.getLastCheckDifference());
        vo.setLastCheckMatched(entity.getLastCheckMatched());
        vo.setLastCheckTime(entity.getLastCheckTime());
        vo.setLastCheckMessage(entity.getLastCheckMessage());
        vo.setLastError(entity.getLastError());
        return vo;
    }

    private void applySelection(SyncTaskGroupItem item, DataSource source) {
        String database = StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName());
        DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(), database, item.getSourceTable());
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(metadata,
            item.getSelectedColumns(), item.getSyncKeyColumns());
        item.setSelectedColumns(SyncColumnSelectionValidator.serialize(selection.selectedColumns()));
        item.setSyncKeyColumns(SyncColumnSelectionValidator.serialize(selection.syncKeyColumns()));
    }

    private void persistCheck(SyncTaskGroupItem item, SyncTaskGroupDataCheckItemResult result) {
        item.setLastCheckSourceRows(result.getSourceRows());
        item.setLastCheckTargetRows(result.getTargetRows());
        item.setLastCheckDifference(result.getDifference());
        item.setLastCheckMatched(result.isSuccess() ? (result.isMatched() ? "1" : "0") : null);
        item.setLastCheckTime(LocalDateTime.now());
        item.setLastCheckMessage(truncateForColumn(result.getMessage()));
        itemMapper.updateById(item);
    }

    private List<SyncTaskGroupItem> items(Long groupId) {
        return itemMapper.selectList(new LambdaQueryWrapper<SyncTaskGroupItem>().eq(SyncTaskGroupItem::getGroupId, groupId).orderByAsc(SyncTaskGroupItem::getItemId));
    }

    private SyncTaskGroup requireGroup(Long groupId) {
        SyncTaskGroup group = groupMapper.selectById(groupId);
        if (group == null) throw new ServiceException("同步任务组不存在");
        return group;
    }

    private DataSource requireSource(Long sourceId, String side) {
        DataSource source = dataSourceMapper.selectById(sourceId);
        if (source == null) throw new ServiceException(side + "数据源不存在");
        return source;
    }

    private static boolean isKafkaTarget(DataSource target) {
        return target != null && "KAFKA".equalsIgnoreCase(target.getSourceType());
    }

    /**
     * Display-only qualified target table name. The "schema." prefix only means anything for
     * PostgreSQL targets - MySQL and Kafka have no such concept, so prefixing them with the
     * PostgreSQL-only "public" default was misleading (see [[public-schema-prefix-display-bug]]).
     */
    private static String displayTargetTable(DataSource target, String targetSchema, String targetTable) {
        boolean postgres = target != null && "POSTGRESQL".equalsIgnoreCase(target.getSourceType());
        return postgres ? (StringUtils.isBlank(targetSchema) ? "public" : targetSchema) + "." + targetTable : targetTable;
    }

    private static String mapStatus(String status) {
        if (status == null) return "FAILED";
        return switch (status.toUpperCase()) {
            case "RUNNING", "STARTING", "INITIALIZING", "CREATED", "PENDING", "RESTARTING" -> "RUNNING";
            case "DOING_SAVEPOINT" -> "PAUSING";
            case "SAVEPOINT_DONE" -> "PAUSED";
            case "CANCELED", "CANCELLED" -> "STOPPED";
            case "FINISHED" -> "FINISHED";
            default -> "FAILED";
        };
    }

    private static String aggregateStatus(List<String> statuses) {
        boolean hasFailure = statuses.stream().anyMatch(status -> "FAILED".equals(status) || "DDL_BLOCKED".equals(status));
        if (hasFailure && statuses.stream().anyMatch("RUNNING"::equals)) return "DEGRADED";
        if (hasFailure) return "FAILED";
        if (statuses.stream().anyMatch("RUNNING"::equals)) return "RUNNING";
        if (statuses.stream().anyMatch("PAUSING"::equals)) return "PAUSING";
        if (!statuses.isEmpty() && statuses.stream().allMatch("PAUSED"::equals)) return "PAUSED";
        if (!statuses.isEmpty() && statuses.stream().allMatch(status -> "STOPPED".equals(status) || "FINISHED".equals(status))) return "STOPPED";
        return "DRAFT";
    }

    private static String hash(String value) {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256").digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            var result = new StringBuilder(digest.length * 2);
            for (byte item : digest) result.append(String.format("%02x", item));
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 不可用", ex);
        }
    }

    private static SyncTaskGroupOperationResult operation(SyncTaskGroup group, String message) {
        SyncTaskGroupOperationResult result = new SyncTaskGroupOperationResult();
        result.setGroupId(group.getGroupId());
        result.setStatus(group.getStatus());
        result.setEngineJobIds(group.getEngineJobId());
        result.setMessage(message);
        return result;
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
                if ("FAILED".equals(item.getStatus()) && validateDiscoveredItem(source, target, item) == null) {
                    captureSchemaBaseline(item, source);
                    item.setStatus("PENDING");
                    item.setLastError("");
                    itemMapper.updateById(item);
                }
            } catch (RuntimeException ignored) {
                // Leave the table isolated; the start loop surfaces the reason per table.
            }
        }
    }

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

    private static boolean isGroupActive(SyncTaskGroup group) {
        return "RUNNING".equals(group.getStatus()) || "DEGRADED".equals(group.getStatus());
    }

    private void captureSchemaBaseline(SyncTaskGroupItem item, DataSource source) {
        DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(),
            StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName()), item.getSourceTable());
        String snapshot = JsonUtils.toJsonString(snapshotOf(metadata));
        item.setSchemaSnapshot(snapshot);
        item.setSchemaHash(hash(snapshot));
    }

    private SyncTaskGroupDdlEvent latestOpenDdlEvent(Long itemId) {
        return ddlEventMapper.selectOne(new LambdaQueryWrapper<SyncTaskGroupDdlEvent>()
            .eq(SyncTaskGroupDdlEvent::getItemId, itemId)
            .in(SyncTaskGroupDdlEvent::getStatus, "PENDING_FIX", "READY_TO_RESUME")
            .orderByDesc(SyncTaskGroupDdlEvent::getDetectedAt)
            .last("limit 1"));
    }

    private SyncTaskGroupDdlEvent upsertDdlEvent(SyncTaskGroup group, SyncTaskGroupItem item, String schemaHash,
                                                  String changeType, String riskLevel, String details,
                                                  String remediation, String status) {
        SyncTaskGroupDdlEvent event = latestOpenDdlEvent(item.getItemId());
        if (event == null) {
            event = new SyncTaskGroupDdlEvent();
            event.setGroupId(group.getGroupId());
            event.setItemId(item.getItemId());
            event.setDetectedAt(LocalDateTime.now());
            event.setChangeType(changeType);
            event.setRiskLevel(riskLevel);
            event.setStatus(status);
            event.setDetails(truncateForColumn(details));
            event.setRemediation(truncateForColumn(remediation));
            event.setSourceSchemaHash(schemaHash);
            ddlEventMapper.insert(event);
        } else {
            event.setChangeType(changeType);
            event.setRiskLevel(riskLevel);
            event.setStatus(status);
            event.setDetails(truncateForColumn(details));
            event.setRemediation(truncateForColumn(remediation));
            event.setSourceSchemaHash(schemaHash);
            ddlEventMapper.updateById(event);
        }
        return event;
    }

    private void isolateDdlItem(SyncTaskGroupItem item, SyncTaskGroupDdlEvent event) {
        if ("DDL_BLOCKED".equals(item.getStatus())) return;
        try {
            if (StringUtils.isNotBlank(item.getEngineJobId())) restClient.stop(item.getEngineJobId(), true, false);
            item.setStatus("DDL_BLOCKED");
            item.setLastError(truncateForColumn("检测到表结构变更：" + event.getChangeType() + "。请按结构检查结果修复后逐表恢复。"));
        } catch (RuntimeException ex) {
            item.setStatus("FAILED");
            item.setLastError(truncateForColumn("检测到表结构变更，但 savepoint 暂停请求失败：" + safeMessage(ex)));
            event.setStatus("PENDING_FIX");
            event.setRemediation(event.getRemediation() + " 暂停请求失败，请先确认引擎状态后手动停止该表。");
            ddlEventMapper.updateById(event);
        }
        itemMapper.updateById(item);
    }

    private static SchemaSnapshot snapshotOf(DataSourceMetadataVo metadata) {
        SchemaSnapshot snapshot = new SchemaSnapshot();
        snapshot.setPrimaryKeys(metadata.getPrimaryKeys().stream().map(SyncTaskGroupServiceImpl::normalizeColumn).toList());
        snapshot.setColumns(metadata.getColumns().stream()
            .map(column -> new SchemaColumn(normalizeColumn(column.getName()), normalizeType(column.getTypeName()),
                column.getJdbcType(), column.getSize(), column.getScale(), column.getNullable()))
            .sorted(Comparator.comparing(SchemaColumn::getName)).toList());
        return snapshot;
    }

    private static SchemaDiff diff(SchemaSnapshot baseline, SchemaSnapshot current) {
        if (baseline == null) return new SchemaDiff("TABLE_UNAVAILABLE", "HIGH", "无法读取原始表结构快照", List.of(), List.of(), List.of(), true);
        Map<String, SchemaColumn> before = baseline.getColumns().stream()
            .collect(Collectors.toMap(SchemaColumn::getName, column -> column, (left, right) -> left, TreeMap::new));
        Map<String, SchemaColumn> after = current.getColumns().stream()
            .collect(Collectors.toMap(SchemaColumn::getName, column -> column, (left, right) -> left, TreeMap::new));
        List<String> added = after.keySet().stream().filter(name -> !before.containsKey(name)).toList();
        List<String> removed = before.keySet().stream().filter(name -> !after.containsKey(name)).toList();
        List<String> altered = before.keySet().stream().filter(after::containsKey)
            .filter(name -> !before.get(name).equals(after.get(name))).map(name -> name + " (" + before.get(name).display() + " -> " + after.get(name).display() + ")").toList();
        boolean keyChanged = !baseline.getPrimaryKeys().equals(current.getPrimaryKeys());
        if (added.isEmpty() && removed.isEmpty() && altered.isEmpty() && !keyChanged) {
            return new SchemaDiff("", "", "", added, removed, altered, false);
        }
        List<String> kinds = new ArrayList<>();
        if (!added.isEmpty()) kinds.add("ADD_COLUMN");
        if (!removed.isEmpty()) kinds.add("DROP_COLUMN");
        if (!altered.isEmpty()) kinds.add("ALTER_COLUMN");
        if (keyChanged) kinds.add("KEY_CHANGED");
        boolean lowRisk = removed.isEmpty() && altered.isEmpty() && !keyChanged
            && added.stream().allMatch(name -> Boolean.TRUE.equals(after.get(name).getNullable()));
        List<String> details = new ArrayList<>();
        if (!added.isEmpty()) details.add("新增字段：" + String.join(", ", added));
        if (!removed.isEmpty()) details.add("删除字段：" + String.join(", ", removed));
        if (!altered.isEmpty()) details.add("字段定义变化：" + String.join("；", altered));
        if (keyChanged) details.add("同步键变化：" + String.join(",", baseline.getPrimaryKeys()) + " -> " + String.join(",", current.getPrimaryKeys()));
        return new SchemaDiff(String.join(",", kinds), lowRisk ? "LOW" : "HIGH", String.join("；", details), added, removed, altered, true);
    }

    private static String remediation(SchemaDiff diff, TargetCompatibilityVo compatibility) {
        String prefix = "LOW".equals(diff.riskLevel())
            ? "平台未启用自动 DDL。请在目标表补齐新增的可空字段，或确认目标已具备等价字段。"
            : "高风险结构变更已隔离该表。请评估字段和同步键语义，备份目标数据后将目标表调整为兼容结构。";
        if (!compatibility.isPassed()) return prefix + " 当前兼容性检查未通过：" + compatibility.getMessage() + "。修复后重新执行结构检查。";
        return prefix + " 当前目标表兼容性已通过；请确认业务影响后使用“恢复该表”继续同步。";
    }

    private static SyncTaskGroupDdlEventVo toDdlEventVo(SyncTaskGroupDdlEvent event, SyncTaskGroupItem item, DataSource target) {
        SyncTaskGroupDdlEventVo vo = new SyncTaskGroupDdlEventVo();
        vo.setEventId(event.getEventId());
        vo.setItemId(event.getItemId());
        vo.setSourceTable(item.getSourceTable());
        vo.setTargetTable(displayTargetTable(target, item.getTargetSchema(), item.getTargetTable()));
        vo.setChangeType(event.getChangeType());
        vo.setRiskLevel(event.getRiskLevel());
        vo.setStatus(event.getStatus());
        vo.setDetails(event.getDetails());
        vo.setRemediation(event.getRemediation());
        vo.setDetectedAt(event.getDetectedAt());
        return vo;
    }

    private static String normalizeColumn(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private static String normalizeType(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private static String safeMessage(RuntimeException ex) {
        return StringUtils.defaultIfBlank(ex.getMessage(), "元数据读取失败");
    }

    /** A checkpoint/binlog/offset boundary error must fail fast, not sit in the transient grace window. */
    private static boolean isRecoveryBoundaryError(String error) {
        if (StringUtils.isBlank(error)) return false;
        String normalized = error.toLowerCase(java.util.Locale.ROOT);
        return normalized.contains("checkpoint") || normalized.contains("savepoint")
            || normalized.contains("binlog") || normalized.contains("offset")
            || normalized.contains("restore") || normalized.contains("恢复")
            || normalized.contains("位点") || normalized.contains("日志已过期");
    }

    private static void applyMetrics(SyncTaskGroupItemStatus result, SeaTunnelRestClient.JobSnapshot snapshot) {
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
        if (sourceEvent != null && sinkCommit != null && sinkCommit >= sourceEvent) result.setCdcLagSeconds((sinkCommit - sourceEvent) / 1000L);
        else result.setMetricsMessage("SeaTunnel 未返回源事件时间，CDC 延迟暂不可计算");
    }

    private static Long metricLong(tools.jackson.databind.JsonNode metrics, String key) {
        var node = metrics == null ? null : metrics.get(key);
        return node != null && node.canConvertToLong() ? node.asLong() : null;
    }

    private static Double metricDouble(tools.jackson.databind.JsonNode metrics, String key) {
        var node = metrics == null ? null : metrics.get(key);
        return node != null && node.isNumber() ? node.asDouble() : null;
    }

    @lombok.Data
    public static class SchemaSnapshot {
        private List<SchemaColumn> columns = new ArrayList<>();
        private List<String> primaryKeys = new ArrayList<>();
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    public static class SchemaColumn {
        private String name;
        private String typeName;
        private Integer jdbcType;
        private Integer size;
        private Integer scale;
        private Boolean nullable;

        private String display() {
            return typeName + "(" + (size == null ? "" : size) + (scale == null ? "" : "," + scale) + ")"
                + (Boolean.TRUE.equals(nullable) ? " NULL" : " NOT NULL");
        }
    }

    private record SchemaDiff(String changeType, String riskLevel, String details, List<String> added,
                              List<String> removed, List<String> altered, boolean changed) {
    }

    private static String appendJobIds(String current, List<String> appended) {
        java.util.LinkedHashSet<String> values = new java.util.LinkedHashSet<>();
        if (StringUtils.isNotBlank(current)) values.addAll(List.of(current.split(",")));
        values.addAll(appended);
        return String.join(",", values);
    }

    /**
     * Keep multibyte engine errors within the varchar(2000) metadata column. Walks back
     * off any UTF-8 continuation byte at the cut point so a truncated Chinese message
     * doesn't end in a garbled (replacement-character) trailing byte sequence - see the
     * equivalent helper in SeaTunnelJobServiceImpl.
     */
    private static String truncateForColumn(String value) {
        if (value == null) return null;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 1800) return value;
        int end = 1800;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) end--;
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }
}
