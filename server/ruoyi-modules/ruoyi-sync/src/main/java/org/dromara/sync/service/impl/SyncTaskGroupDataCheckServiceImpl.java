package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckItemResult;
import org.dromara.sync.domain.vo.SyncTaskGroupDataCheckResult;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncTaskGroupDataCheckService;
import org.dromara.sync.support.GroupStatuses;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Row-count reconciliation of a task group: the single-table read-only comparison of
 * {@link IDataConsistencyService} run for every table item, each outcome kept in that item's
 * {@code last_check_*} columns. A table whose check fails is counted as failed and never stops
 * the others; the summary keeps failed tables apart so a failure cannot pass for a match.
 */
@RequiredArgsConstructor
@Service
public class SyncTaskGroupDataCheckServiceImpl implements ISyncTaskGroupDataCheckService {

    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final IDataSourceService dataSourceService;
    private final IDataConsistencyService dataConsistencyService;

    /** Executes independent, bounded COUNT(*) comparisons for all task-group table items. */
    @Override
    public SyncTaskGroupDataCheckResult checkData(Long groupId) {
        SyncTaskGroup group = requireGroup(groupId);
        DataSource source = dataSourceService.requireById(group.getSourceId(), "源");
        DataSource target = dataSourceService.requireById(group.getTargetId(), "目标");
        List<SyncTaskGroupItem> groupItems = itemMapper.selectByGroupId(groupId);
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
        result.setConsistencyNote(GroupStatuses.isLive(group.getStatus()) || SyncStatus.PAUSING.equals(group.getStatus())
            ? "任务仍在持续同步，当前为非同水位的行数检查；暂停或确认两端水位稳定后再做严格验收。"
            : "当前结果为源端与目标端的只读行数检查，不替代基于同步键的逐行校验。");

        for (SyncTaskGroupItem item : groupItems) {
            SyncTaskGroupDataCheckItemResult itemResult = new SyncTaskGroupDataCheckItemResult();
            itemResult.setItemId(item.getItemId());
            try {
                SyncTaskDataCheckResult tableResult = dataConsistencyService.check(source, target,
                    GroupItemOperations.sourceDatabase(item, source), item.getSourceTable(),
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
                itemResult.setSourceTable(GroupItemOperations.sourceDatabase(item, source) + "." + item.getSourceTable());
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

    private void persistCheck(SyncTaskGroupItem item, SyncTaskGroupDataCheckItemResult result) {
        item.setLastCheckSourceRows(result.getSourceRows());
        item.setLastCheckTargetRows(result.getTargetRows());
        item.setLastCheckDifference(result.getDifference());
        item.setLastCheckMatched(result.isSuccess() ? (result.isMatched() ? "1" : "0") : null);
        item.setLastCheckTime(LocalDateTime.now());
        item.setLastCheckMessage(SyncText.truncateForColumn(result.getMessage()));
        itemMapper.updateById(item);
    }

    private SyncTaskGroup requireGroup(Long groupId) {
        SyncTaskGroup group = groupMapper.selectById(groupId);
        if (group == null) throw new ServiceException("同步任务组不存在");
        return group;
    }
}
