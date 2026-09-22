package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dromara.common.core.enums.PushSourceEnum;
import org.dromara.common.core.enums.PushTypeEnum;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.system.api.MessageService;
import org.dromara.system.api.domain.PushPayloadDTO;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Turns "something needs an operator" states into message-center notices, once per
 * transition. Nothing in the state machines calls this: every pass compares each row's
 * current status with the status it was last alerted about ({@code alerted_status}), so a
 * FAILED task raises exactly one notice however it got there (engine poll, scheduler,
 * DDL check, a manual action) and raises another only after it recovered and failed
 * again. The marker lives on the row, so restarts and multiple instances do not repeat.
 *
 * <p>Notices go through RuoYi's {@link MessageService}, which persists them in the
 * message box and pushes them over SSE / WebSocket to every online user.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SyncAlertNotifier {

    static final String LEVEL_ERROR = "error";
    static final String LEVEL_WARNING = "warning";
    static final String TITLE = "数据同步告警";

    /** Task states an operator has to act on; anything else clears the marker. */
    private static final Map<String, String> TASK_LABELS = Map.of(
        SyncStatus.FAILED, "运行失败",
        SyncStatus.REINITIALIZE_REQUIRED, "需要重新初始化");
    private static final Map<String, String> GROUP_LABELS = Map.of(
        SyncStatus.FAILED, "运行失败",
        SyncStatus.REINITIALIZE_REQUIRED, "需要重新初始化",
        SyncStatus.DEGRADED, "部分表已隔离（降级运行）");
    private static final Map<String, String> ITEM_LABELS = Map.of(
        SyncStatus.FAILED, "已被隔离（失败）",
        SyncStatus.DDL_BLOCKED, "因表结构变更暂停");
    private static final Set<String> WARNING_LEVEL = Set.of(SyncStatus.DEGRADED, SyncStatus.DDL_BLOCKED);
    /** A table's own notice only makes sense while its group is otherwise running. */
    private static final Set<String> LIVE_GROUP_STATUSES = Set.of(SyncStatus.RUNNING, SyncStatus.DEGRADED);

    private final SyncTaskMapper taskMapper;
    private final SyncTaskGroupMapper groupMapper;
    private final SyncTaskGroupItemMapper itemMapper;
    private final ObjectProvider<MessageService> messageService;

    @Value("${sync.alerts.enabled:true}")
    private boolean enabled = true;

    @Scheduled(fixedDelayString = "${sync.alerts.interval-ms:30000}", initialDelayString = "${sync.alerts.initial-delay-ms:45000}")
    public void notifyDueAlerts() {
        try {
            notifyOnce();
        } catch (RuntimeException ex) {
            log.warn("Sync alert pass failed: {}", ex.getMessage());
        }
    }

    /** One pass over tasks, groups and items; returns the number of notices raised. */
    int notifyOnce() {
        if (!enabled) return 0;
        MessageService messages = messageService.getIfAvailable();
        if (messages == null) return 0;
        int raised = 0;

        for (SyncTask task : taskMapper.selectAlertCandidates()) {
            String due = TASK_LABELS.containsKey(task.getStatus()) ? task.getStatus() : "";
            if (due.equals(StringUtils.defaultIfBlank(task.getAlertedStatus(), ""))) continue;
            if (!due.isEmpty()) {
                publish(messages, "同步任务「" + task.getTaskName() + "」" + TASK_LABELS.get(due), task.getLastError(),
                    due, "/sync/task", data("TASK", task.getTaskId(), null, due));
                raised++;
            }
            taskMapper.updateAlertedStatus(task.getTaskId(), due);
        }

        Map<Long, SyncTaskGroup> groups = new LinkedHashMap<>();
        for (SyncTaskGroup group : groupMapper.selectAlertCandidates()) {
            groups.put(group.getGroupId(), group);
            String due = GROUP_LABELS.containsKey(group.getStatus()) ? group.getStatus() : "";
            if (due.equals(StringUtils.defaultIfBlank(group.getAlertedStatus(), ""))) continue;
            if (!due.isEmpty()) {
                publish(messages, "任务组「" + group.getGroupName() + "」" + GROUP_LABELS.get(due), group.getLastError(),
                    due, "/sync/group", data("GROUP", group.getGroupId(), group.getGroupId(), due));
                raised++;
            }
            groupMapper.updateAlertedStatus(group.getGroupId(), due);
        }

        Function<Long, SyncTaskGroup> groupOf = groupId -> groups.computeIfAbsent(groupId, groupMapper::selectById);
        for (SyncTaskGroupItem item : itemMapper.selectAlertCandidates()) {
            SyncTaskGroup group = groupOf.apply(item.getGroupId());
            boolean groupLive = group != null && LIVE_GROUP_STATUSES.contains(group.getStatus());
            String due = groupLive && ITEM_LABELS.containsKey(item.getStatus()) ? item.getStatus() : "";
            if (due.equals(StringUtils.defaultIfBlank(item.getAlertedStatus(), ""))) continue;
            if (!due.isEmpty()) {
                publish(messages, "任务组「" + group.getGroupName() + "」的表 " + item.getSourceTable() + " " + ITEM_LABELS.get(due),
                    item.getLastError(), due, "/sync/group", data("GROUP_ITEM", item.getItemId(), item.getGroupId(), due));
                raised++;
            }
            itemMapper.updateAlertedStatus(item.getItemId(), due);
        }
        return raised;
    }

    private static void publish(MessageService messages, String headline, String detail, String status, String path, Map<String, Object> data) {
        String text = StringUtils.isBlank(detail) ? headline : headline + "：" + detail;
        PushPayloadDTO payload = PushPayloadDTO.of(PushTypeEnum.MESSAGE, PushSourceEnum.BACKEND, StringUtils.substring(text, 0, 480), data);
        payload.setPath(path);
        messages.publishAll(payload);
        log.info("Sync alert [{}] {}", status, text);
    }

    private static Map<String, Object> data(String scope, Long refId, Long groupId, String status) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("title", TITLE);
        data.put("level", WARNING_LEVEL.contains(status) ? LEVEL_WARNING : LEVEL_ERROR);
        data.put("scope", scope);
        data.put("refId", refId);
        if (groupId != null) data.put("groupId", groupId);
        data.put("status", status);
        return data;
    }
}
