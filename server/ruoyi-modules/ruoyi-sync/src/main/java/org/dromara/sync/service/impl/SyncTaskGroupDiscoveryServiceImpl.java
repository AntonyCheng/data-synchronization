package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SyncGroupProperties;
import org.dromara.sync.config.SyncSchedulingConfig;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncScope;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskGroupOperationResult;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncTaskGroupDiscoveryService;
import org.dromara.sync.support.GroupStatuses;
import org.dromara.sync.support.SyncLocks;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.dromara.sync.support.TableSchemaSnapshot;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * New-table discovery for whole-database task groups: on demand, periodically for groups that
 * opted into {@code autoDiscover}, and on every save of a whole-database group. A new table's
 * item gets its column selection and schema baseline from the live source and, for a Kafka target,
 * its topic; a table that fails validation is inserted FAILED with the reason rather than skipped,
 * so the operator sees it. On a live group the new tables' jobs are submitted at once, and one the
 * engine refuses is isolated while the others keep going.
 *
 * <p>Runs under the group lock and - like the rest of the group lifecycle, see
 * {@link SyncTaskGroupServiceImpl} - without a transaction of its own. A save runs it inside the
 * save's transaction, but a group being saved is never live, so nothing is submitted there.
 */
@RequiredArgsConstructor
@Service
public class SyncTaskGroupDiscoveryServiceImpl implements ISyncTaskGroupDiscoveryService {

    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;
    private final KafkaTaskBridgeService kafkaTaskBridgeService;
    private final GroupItemOperations itemOps;
    private final SyncLocks locks;
    private final SyncGroupProperties groupProperties;

    /**
     * Each discovered table is inserted before its job is submitted, and stays inserted if a
     * later table fails: a rolled-back row would have left its already-submitted job with no
     * owner at all.
     */
    @Override
    public SyncTaskGroupOperationResult discover(Long groupId) {
        return locks.withGroupLock(groupId, () -> doDiscover(groupId));
    }

    /** Runs only for database-scope groups that explicitly opted into new-table discovery. */
    @Scheduled(fixedDelayString = "${sync.discovery.interval-ms:60000}", initialDelayString = "${sync.discovery.initial-delay-ms:30000}", scheduler = SyncSchedulingConfig.SCHEDULER)
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

    private SyncTaskGroupOperationResult doDiscover(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!SyncScope.isDatabase(group.getSyncScope())) {
            throw new ServiceException("仅整库同步任务组支持发现新增表");
        }
        DataSource source = dataSourceService.requireById(group.getSourceId(), "源");
        DataSource target = dataSourceService.requireById(group.getTargetId(), "目标");
        String database = StringUtils.defaultIfBlank(group.getSourceDatabase(), source.getDatabaseName());
        Set<String> existing = new HashSet<>();
        // A discovered table follows the schema the operator chose for the group's existing
        // tables (they all share one on a whole-database group), else the target's default.
        String targetSchema = TableNames.defaultSchema(target);
        for (SyncTaskGroupItem item : itemMapper.selectByGroupId(groupId)) {
            existing.add(item.getSourceTable().toLowerCase(Locale.ROOT));
            if (StringUtils.isNotBlank(item.getTargetSchema())) targetSchema = item.getTargetSchema();
        }

        int discovered = 0;
        int started = 0;
        int failed = 0;
        // Whole-database discovery used to have no cap at all: a 500-table schema became 500
        // items, and on a live group 500 engine jobs, each with its own binlog connection to the
        // customer's database. The group limit applies to discovered tables like to listed ones;
        // tables already in the group stay (a group saved before the limit may exceed it).
        int maxTables = groupProperties.effectiveMaxTables();
        int capacity = maxTables - existing.size();
        int overLimit = 0;
        List<String> jobIds = new ArrayList<>();
        for (String table : metadataService.queryTables(source.getSourceId(), database)) {
            if (!existing.add(table.toLowerCase(Locale.ROOT))) continue;
            if (discovered >= capacity) {
                overLimit++;
                continue;
            }
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
                DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
                GroupItemOperations.applySelection(item, metadata);
                TableSchemaSnapshot.baseline(item, metadata);
                if (DataSourceType.isKafka(target)) kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());
                validationError = itemOps.validateDiscoveredItem(source, target, item);
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
            if (validationError == null && GroupStatuses.isLive(group.getStatus())) {
                try {
                    jobIds.add(itemOps.submit(group, item, source, target));
                    started++;
                } catch (RuntimeException ex) {
                    itemOps.isolate(item, ex.getMessage());
                    failed++;
                }
            }
        }
        String limitNote = overLimit == 0 ? ""
            : "已达任务组上限 " + maxTables + " 张，另有 " + overLimit + " 张表未纳入（sync.group.max-tables）";
        if (discovered > 0) {
            group.setConfigVersion((group.getConfigVersion() == null ? 1 : group.getConfigVersion()) + 1);
            if (!jobIds.isEmpty()) group.setEngineJobId(appendJobIds(group.getEngineJobId(), jobIds));
        }
        String lastError = joinNotes(failed == 0 ? "" : "新增表发现完成，其中 " + failed + " 张表校验或提交失败，请查看表项错误", limitNote);
        // The periodic pass repeats an unchanged "over the limit" every minute - only write a change.
        if (discovered > 0 || !lastError.equals(StringUtils.defaultString(group.getLastError()))) {
            group.setLastError(lastError);
            groupMapper.updateById(group);
        }
        String summary = joinNotes(discovered == 0 ? "未发现新增表"
            : "发现 " + discovered + " 张新表，已启动 " + started + " 张，失败 " + failed + " 张", limitNote);
        return failed > 0 || overLimit > 0
            ? SyncTaskGroupOperationResult.partial(group, summary) : SyncTaskGroupOperationResult.of(group, summary);
    }

    private static String joinNotes(String first, String second) {
        if (StringUtils.isBlank(first)) return second;
        if (StringUtils.isBlank(second)) return first;
        return first + "；" + second;
    }

    private SyncTaskGroup requireGroup(Long groupId) {
        SyncTaskGroup group = groupMapper.selectById(groupId);
        if (group == null) throw new ServiceException("同步任务组不存在");
        return group;
    }

    private static String appendJobIds(String current, List<String> appended) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(current)) values.addAll(List.of(current.split(",")));
        values.addAll(appended);
        return String.join(",", values);
    }
}
