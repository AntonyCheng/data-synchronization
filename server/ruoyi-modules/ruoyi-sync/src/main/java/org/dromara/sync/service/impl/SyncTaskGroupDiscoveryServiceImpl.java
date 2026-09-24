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
 * <p>The remote half ({@link #plan}, {@link #readmitRejected}) writes nothing. A save of a
 * whole-database group ({@link SyncTaskGroupServiceImpl}) runs it before its transaction opens and
 * writes the planned rows itself; a group being saved is never live, so nothing is submitted there.
 * {@link #discover} runs under the group lock and - like the rest of the group lifecycle - without a
 * transaction of its own: it plans, then inserts and submits table by table.
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

    @Override
    public TablePlan plan(SyncTaskGroup group, List<SyncTaskGroupItem> present) {
        return plan(group, requireSource(group), requireTarget(group), present);
    }

    @Override
    public List<SyncTaskGroupItem> readmitRejected(SyncTaskGroup group, List<SyncTaskGroupItem> items) {
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        List<SyncTaskGroupItem> rechecked = new ArrayList<>();
        for (SyncTaskGroupItem item : items) {
            // A table with a job did run; what parked it is not a discovery rule and is not undone here.
            if (!SyncStatus.FAILED.equals(item.getStatus()) || StringUtils.isNotBlank(item.getEngineJobId())) continue;
            String rejection = admit(item, source, target);
            if (rejection == null) {
                item.setStatus(SyncStatus.PENDING);
                item.setLastError("");
            } else {
                item.setLastError(SyncText.truncateForColumn(rejection));
            }
            rechecked.add(item);
        }
        return rechecked;
    }

    private SyncTaskGroupOperationResult doDiscover(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        if (!SyncScope.isDatabase(group.getSyncScope())) {
            throw new ServiceException("仅整库同步任务组支持发现新增表");
        }
        DataSource source = requireSource(group);
        DataSource target = requireTarget(group);
        TablePlan plan = plan(group, source, target, itemMapper.selectByGroupId(groupId));

        int started = 0;
        int failed = plan.rejected();
        List<String> jobIds = new ArrayList<>();
        boolean live = GroupStatuses.isLive(group.getStatus());
        for (SyncTaskGroupItem item : plan.items()) {
            itemMapper.insert(item);
            if (!live || SyncStatus.FAILED.equals(item.getStatus())) continue;
            try {
                jobIds.add(itemOps.submit(group, item, source, target));
                started++;
            } catch (RuntimeException ex) {
                itemOps.isolate(item, ex.getMessage());
                failed++;
            }
        }
        int discovered = plan.items().size();
        if (discovered > 0) {
            group.setConfigVersion((group.getConfigVersion() == null ? 1 : group.getConfigVersion()) + 1);
            if (!jobIds.isEmpty()) group.setEngineJobId(appendJobIds(group.getEngineJobId(), jobIds));
        }
        String lastError = plan.note(failed);
        // The periodic pass repeats an unchanged "over the limit" every minute - only write a change.
        if (discovered > 0 || !lastError.equals(StringUtils.defaultString(group.getLastError()))) {
            group.setLastError(lastError);
            groupMapper.updateById(group);
        }
        String summary = TablePlan.join(discovered == 0 ? "未发现新增表"
            : "发现 " + discovered + " 张新表，已启动 " + started + " 张，失败 " + failed + " 张", plan.limitNote());
        return failed > 0 || plan.overLimit() > 0
            ? SyncTaskGroupOperationResult.partial(group, summary) : SyncTaskGroupOperationResult.of(group, summary);
    }

    /**
     * Whole-database discovery used to have no cap at all: a 500-table schema became 500 items,
     * and on a live group 500 engine jobs, each with its own binlog connection to the customer's
     * database. The group limit applies to discovered tables like to listed ones; tables already
     * in the group stay (a group saved before the limit may exceed it).
     */
    private TablePlan plan(SyncTaskGroup group, DataSource source, DataSource target, List<SyncTaskGroupItem> present) {
        String database = StringUtils.defaultIfBlank(group.getSourceDatabase(), source.getDatabaseName());
        Set<String> known = new HashSet<>();
        // A discovered table follows the schema the operator chose for the group's existing
        // tables (they all share one on a whole-database group), else the target's default.
        String targetSchema = TableNames.defaultSchema(target);
        for (SyncTaskGroupItem item : present) {
            known.add(item.getSourceTable().toLowerCase(Locale.ROOT));
            if (StringUtils.isNotBlank(item.getTargetSchema())) targetSchema = item.getTargetSchema();
        }
        int maxTables = groupProperties.effectiveMaxTables();
        int capacity = maxTables - known.size();
        int overLimit = 0;
        List<SyncTaskGroupItem> planned = new ArrayList<>();
        for (String table : metadataService.queryTables(source.getSourceId(), database)) {
            if (!known.add(table.toLowerCase(Locale.ROOT))) continue;
            if (planned.size() >= capacity) {
                overLimit++;
                continue;
            }
            SyncTaskGroupItem item = new SyncTaskGroupItem();
            item.setGroupId(group.getGroupId());
            item.setSourceDatabase(database);
            item.setSourceTable(table);
            item.setTargetSchema(targetSchema);
            item.setTargetTable(table);
            item.setDdlPolicy(group.getDdlPolicy());
            item.setStatus(SyncStatus.PENDING);
            String rejection = admit(item, source, target);
            if (rejection != null) {
                item.setStatus(SyncStatus.FAILED);
                item.setLastError(SyncText.truncateForColumn(rejection));
            }
            planned.add(item);
        }
        return new TablePlan(planned, overLimit, maxTables);
    }

    /**
     * The discovery rules for one table: selection and baseline from the live schema, its topic on
     * a Kafka target, a usable sync key and a compatible target. Null when the table passes, else
     * the reason. A new table has no baseline yet, so its selection is simply derived (every column,
     * the first usable key); a re-checked one that covered its whole table follows it, and an
     * explicit subset is validated as it is (see {@link GroupItemOperations#followSourceColumns}).
     */
    private String admit(SyncTaskGroupItem item, DataSource source, DataSource target) {
        try {
            DataSourceMetadataVo metadata = itemOps.readSourceMetadata(item, source);
            if (!GroupItemOperations.followSourceColumns(item, metadata)) GroupItemOperations.applySelection(item, metadata);
            TableSchemaSnapshot.baseline(item, metadata);
            if (DataSourceType.isKafka(target)) kafkaTaskBridgeService.ensureTopicExists(target, item.getTargetTable());
            return itemOps.validateDiscoveredItem(source, target, item);
        } catch (RuntimeException ex) {
            return StringUtils.defaultIfBlank(ex.getMessage(), "新增表校验失败");
        }
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

    private static String appendJobIds(String current, List<String> appended) {
        LinkedHashSet<String> values = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(current)) values.addAll(List.of(current.split(",")));
        values.addAll(appended);
        return String.join(",", values);
    }
}
