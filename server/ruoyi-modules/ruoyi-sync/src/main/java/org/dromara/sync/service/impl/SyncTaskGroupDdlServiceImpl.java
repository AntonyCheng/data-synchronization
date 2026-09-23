package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupDdlEvent;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDdlEventVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.mapper.SyncTaskGroupDdlEventMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncTaskGroupDdlService;
import org.dromara.sync.service.ISyncTaskGroupService;
import org.dromara.sync.support.GroupStatuses;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.dromara.sync.support.TableSchemaSnapshot;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Periodic and on-demand DDL drift checks for task groups. Isolates only the changed table
 * (savepoint pause -> DDL_BLOCKED) and leaves healthy tables running; the actual resume
 * of a fixed table is delegated to {@link ISyncTaskGroupService#resumeItem}.
 */
@RequiredArgsConstructor
@Service
public class SyncTaskGroupDdlServiceImpl implements ISyncTaskGroupDdlService {

    private static final String EVENT_PENDING_FIX = SyncTaskGroupDdlEvent.STATUS_PENDING_FIX;
    private static final String EVENT_READY_TO_RESUME = SyncTaskGroupDdlEvent.STATUS_READY_TO_RESUME;

    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final SyncTaskGroupDdlEventMapper ddlEventMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final ISyncTaskGroupService groupService;
    private final SeaTunnelRestClient restClient;
    private final SyncLocks locks;

    @Override
    @Transactional
    public SyncTaskGroupDdlCheckResult checkDdl(Long groupId) {
        return locks.withGroupLock(groupId, () -> doCheckDdl(groupId));
    }

    @Override
    @Transactional
    public SyncTaskGroupOperationResult resumeDdlItem(Long groupId, Long itemId) {
        return locks.withGroupLock(groupId, () -> doResumeDdlItem(groupId, itemId));
    }

    /** Periodic DDL checks isolate only the changed table and leave healthy tables running. */
    @Scheduled(fixedDelayString = "${sync.ddl-check.interval-ms:60000}", initialDelayString = "${sync.ddl-check.initial-delay-ms:45000}")
    public void checkRunningGroupDdl() {
        groupMapper.selectLive().forEach(group -> {
            if (locks.isGroupBusy(group.getGroupId())) return;
            try {
                checkDdl(group.getGroupId());
            } catch (RuntimeException ignored) {
                // A metadata failure for one group cannot block checks for other groups.
            }
        });
    }

    private SyncTaskGroupDdlCheckResult doCheckDdl(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = dataSourceService.requireById(group.getSourceId(), "源");
        DataSource target = dataSourceService.requireById(group.getTargetId(), "目标");
        SyncTaskGroupDdlCheckResult result = new SyncTaskGroupDdlCheckResult();
        result.setGroupId(groupId);

        int newlyDetected = 0;
        int readyToResume = 0;
        List<SyncTaskGroupItem> items = itemMapper.selectByGroupId(groupId);
        // One connection per source database for the whole pass rather than one per table: this
        // runs every minute against the customer's production database.
        Map<String, IDataSourceMetadataService.TableMetadata> schemas = readSourceSchemas(source, items);
        for (SyncTaskGroupItem item : items) {
            IDataSourceMetadataService.TableMetadata read = schemas.get(item.getSourceTable());
            DataSourceMetadataVo current = read == null ? null : read.metadata();
            if (current == null) {
                SyncTaskGroupDdlEvent event = upsertEvent(group, item, null, "TABLE_UNAVAILABLE", "HIGH",
                    "无法读取源表结构：" + StringUtils.defaultIfBlank(read == null ? null : read.error(), "元数据读取失败"),
                    "确认源表仍存在且同步账号具有读取元数据权限后重新执行结构检查。", EVENT_PENDING_FIX);
                isolate(item, event);
                result.getEvents().add(toVo(event, item, target));
                newlyDetected++;
                continue;
            }

            TableSchemaSnapshot.Snapshot snapshot = TableSchemaSnapshot.of(current);
            String currentJson = TableSchemaSnapshot.toJson(snapshot);
            String currentHash = SyncText.sha256Hex(currentJson);
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

            // schema_hash is the hash of the stored snapshot's exact bytes, so an equal hash
            // means an identical snapshot and a guaranteed-empty diff. Skipping the parse keeps
            // the common case (nothing changed) cheap; a blank hash falls through to the diff so
            // rows written before the column was populated still behave.
            boolean unchanged = StringUtils.isNotBlank(item.getSchemaHash()) && currentHash.equals(item.getSchemaHash());
            TableSchemaSnapshot.Diff diff = unchanged ? null
                : TableSchemaSnapshot.diff(TableSchemaSnapshot.fromJson(item.getSchemaSnapshot()), snapshot);
            SyncTaskGroupDdlEvent existing = ddlEventMapper.selectLatestOpen(item.getItemId());
            if (unchanged || !diff.changed()) {
                if (existing != null) {
                    existing.setStatus(EVENT_READY_TO_RESUME);
                    existing.setDetails("源表结构已恢复为启动快照。请确认目标端结构后恢复该表。");
                    existing.setRemediation("确认目标端仍与源端兼容后，使用“恢复该表”继续从 savepoint 同步；若被拒绝，使用“重新初始化该表”重建。");
                    ddlEventMapper.updateById(existing);
                    result.getEvents().add(toVo(existing, item, target));
                    readyToResume++;
                }
                continue;
            }

            TargetCompatibilityVo compatibility = metadataService.checkTargetCompatibility(source.getSourceId(), target.getSourceId(),
                item.getSourceTable(), item.getTargetSchema(), item.getTargetTable(), item.getSelectedColumns(), item.getSyncKeyColumns());
            String eventStatus = compatibility.isPassed() ? EVENT_READY_TO_RESUME : EVENT_PENDING_FIX;
            SyncTaskGroupDdlEvent event = upsertEvent(group, item, currentHash, diff.changeType(), diff.riskLevel(),
                diff.details(), TableSchemaSnapshot.remediation(diff, compatibility), eventStatus);
            isolate(item, event);
            result.getEvents().add(toVo(event, item, target));
            if (EVENT_READY_TO_RESUME.equals(eventStatus)) readyToResume++; else newlyDetected++;
        }

        group.setStatus(GroupStatuses.aggregate(itemMapper.selectByGroupId(groupId).stream().map(SyncTaskGroupItem::getStatus).toList()));
        group.setLastError(newlyDetected == 0 ? "" : "检测到 " + newlyDetected + " 张表存在待修复的结构变更");
        groupMapper.updateById(group);
        result.setStatus(group.getStatus());
        result.setMessage(result.getEvents().isEmpty() ? "未发现运行中表结构变更"
            : "检测到 " + result.getEvents().size() + " 条结构变更事件，其中 " + readyToResume + " 条已可恢复");
        return result;
    }

    /**
     * Current source structure for every table of the group, keyed by table name. Items normally
     * share one database, so this is one connection; a group whose items were discovered against
     * different databases gets one per database. A batch that cannot even connect degrades to a
     * per-table error rather than aborting the pass, which is how the old per-table read behaved.
     */
    private Map<String, IDataSourceMetadataService.TableMetadata> readSourceSchemas(DataSource source,
                                                                                    List<SyncTaskGroupItem> items) {
        Map<String, List<String>> tablesByDatabase = items.stream().collect(Collectors.groupingBy(
            item -> StringUtils.defaultIfBlank(item.getSourceDatabase(), source.getDatabaseName()),
            LinkedHashMap::new,
            Collectors.mapping(SyncTaskGroupItem::getSourceTable, Collectors.toList())));
        Map<String, IDataSourceMetadataService.TableMetadata> schemas = new LinkedHashMap<>();
        tablesByDatabase.forEach((database, tables) -> {
            try {
                schemas.putAll(metadataService.queryTablesMetadata(source.getSourceId(), database, tables));
            } catch (RuntimeException ex) {
                String message = SyncText.safeMessage(ex, "元数据读取失败");
                tables.forEach(table -> schemas.put(table, new IDataSourceMetadataService.TableMetadata(null, message)));
            }
        });
        return schemas;
    }

    private SyncTaskGroupOperationResult doResumeDdlItem(Long groupId, Long itemId) {
        requireGroup(groupId);
        SyncTaskGroupItem item = itemMapper.selectOneOfGroup(groupId, itemId);
        if (item == null) throw new ServiceException("表项不存在或不属于当前任务组");
        doCheckDdl(groupId);
        SyncTaskGroupDdlEvent event = ddlEventMapper.selectLatestOpen(itemId);
        if (event == null || !EVENT_READY_TO_RESUME.equals(event.getStatus())) {
            throw new ServiceException("表结构尚未修复或未通过兼容性检查，请先执行结构检查并按修复建议处理");
        }
        // The lock is reentrant, so the group service's own locking simply nests here.
        groupService.resumeItem(groupId, itemId);
        ddlEventMapper.resolveOpen(itemId, null);
        return SyncTaskGroupOperationResult.of(requireGroup(groupId), "表 " + item.getSourceTable() + " 已通过结构校验并恢复");
    }

    private SyncTaskGroupDdlEvent upsertEvent(SyncTaskGroup group, SyncTaskGroupItem item, String schemaHash,
                                              String changeType, String riskLevel, String details,
                                              String remediation, String status) {
        SyncTaskGroupDdlEvent event = ddlEventMapper.selectLatestOpen(item.getItemId());
        boolean created = event == null;
        if (created) {
            event = new SyncTaskGroupDdlEvent();
            event.setGroupId(group.getGroupId());
            event.setItemId(item.getItemId());
            event.setDetectedAt(LocalDateTime.now());
        }
        event.setChangeType(changeType);
        event.setRiskLevel(riskLevel);
        event.setStatus(status);
        event.setDetails(SyncText.truncateForColumn(details));
        event.setRemediation(SyncText.truncateForColumn(remediation));
        event.setSourceSchemaHash(schemaHash);
        if (created) ddlEventMapper.insert(event); else ddlEventMapper.updateById(event);
        return event;
    }

    /** Pause the table's job with a savepoint and park the item until an operator resolves the event. */
    private void isolate(SyncTaskGroupItem item, SyncTaskGroupDdlEvent event) {
        if (SyncStatus.DDL_BLOCKED.equals(item.getStatus())) return;
        try {
            if (StringUtils.isNotBlank(item.getEngineJobId())) restClient.stop(item.getEngineJobId(), true, false);
            item.setStatus(SyncStatus.DDL_BLOCKED);
            item.setLastError(SyncText.truncateForColumn("检测到表结构变更：" + event.getChangeType() + "。请按结构检查结果修复后逐表恢复。"));
        } catch (RuntimeException ex) {
            item.setStatus(SyncStatus.FAILED);
            item.setLastError(SyncText.truncateForColumn("检测到表结构变更，但 savepoint 暂停请求失败："
                + StringUtils.defaultIfBlank(ex.getMessage(), "元数据读取失败")));
            event.setStatus(EVENT_PENDING_FIX);
            event.setRemediation(event.getRemediation() + " 暂停请求失败，请先确认引擎状态后手动停止该表。");
            ddlEventMapper.updateById(event);
        }
        itemMapper.updateById(item);
    }

    private static SyncTaskGroupDdlEventVo toVo(SyncTaskGroupDdlEvent event, SyncTaskGroupItem item, DataSource target) {
        SyncTaskGroupDdlEventVo vo = new SyncTaskGroupDdlEventVo();
        vo.setEventId(event.getEventId());
        vo.setItemId(event.getItemId());
        vo.setSourceTable(item.getSourceTable());
        vo.setTargetTable(TableNames.display(target, item.getTargetSchema(), item.getTargetTable()));
        vo.setChangeType(event.getChangeType());
        vo.setRiskLevel(event.getRiskLevel());
        vo.setStatus(event.getStatus());
        vo.setDetails(event.getDetails());
        vo.setRemediation(event.getRemediation());
        vo.setDetectedAt(event.getDetectedAt());
        return vo;
    }

    private SyncTaskGroup requireGroup(Long groupId) {
        SyncTaskGroup group = groupMapper.selectById(groupId);
        if (group == null) throw new ServiceException("同步任务组不存在");
        return group;
    }
}
