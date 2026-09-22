package org.dromara.sync.service.impl;

import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.system.api.MessageService;
import org.dromara.system.api.domain.PushPayloadDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("dev")
class SyncAlertNotifierTest {

    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final MessageService messages = mock(MessageService.class);
    @SuppressWarnings("unchecked")
    private final ObjectProvider<MessageService> provider = mock(ObjectProvider.class);
    private final SyncAlertNotifier notifier = new SyncAlertNotifier(taskMapper, groupMapper, itemMapper, provider);

    @BeforeEach
    void messageCenterIsPresent() {
        when(provider.getIfAvailable()).thenReturn(messages);
        when(taskMapper.selectAlertCandidates()).thenReturn(List.of());
        when(groupMapper.selectAlertCandidates()).thenReturn(List.of());
        when(itemMapper.selectAlertCandidates()).thenReturn(List.of());
    }

    @Test
    void aFailureIsAnnouncedOncePerTransitionAndTheMarkerFollowsTheStatus() {
        SyncTask failed = task(1L, "FAILED", "", "SeaTunnel 作业异常退出");
        SyncTask alreadyTold = task(2L, "FAILED", "FAILED", "still broken");
        SyncTask recovered = task(3L, "RUNNING", "FAILED", "");
        SyncTask escalated = task(4L, "REINITIALIZE_REQUIRED", "FAILED", "binlog 已过期");
        when(taskMapper.selectAlertCandidates()).thenReturn(List.of(failed, alreadyTold, recovered, escalated));

        assertEquals(2, notifier.notifyOnce());

        ArgumentCaptor<PushPayloadDTO> sent = ArgumentCaptor.forClass(PushPayloadDTO.class);
        verify(messages, times(2)).publishAll(sent.capture());
        PushPayloadDTO first = sent.getAllValues().get(0);
        assertEquals("同步任务「t1」运行失败：SeaTunnel 作业异常退出", first.getMessage());
        assertEquals("/sync/task", first.getPath());
        assertEquals("error", ((Map<?, ?>) first.getData()).get("level"));
        assertEquals("数据同步告警", ((Map<?, ?>) first.getData()).get("title"));
        assertTrue(sent.getAllValues().get(1).getMessage().contains("需要重新初始化：binlog 已过期"));

        verify(taskMapper).updateAlertedStatus(1L, "FAILED");
        verify(taskMapper, never()).updateAlertedStatus(2L, "FAILED");
        verify(taskMapper).updateAlertedStatus(3L, "");
        verify(taskMapper).updateAlertedStatus(4L, "REINITIALIZE_REQUIRED");
    }

    @Test
    void groupsWarnOnDegradedAndTablesOnlySpeakWhileTheirGroupIsLive() {
        SyncTaskGroup degraded = group(7L, "DEGRADED", "");
        SyncTaskGroup deadGroup = group(8L, "FAILED", "FAILED");
        when(groupMapper.selectAlertCandidates()).thenReturn(List.of(degraded, deadGroup));
        SyncTaskGroupItem blocked = item(71L, 7L, "DDL_BLOCKED", "", "检测到表结构变更：ADD_COLUMN");
        SyncTaskGroupItem isolatedInDeadGroup = item(81L, 8L, "FAILED", "", "boom");
        SyncTaskGroupItem healedTable = item(72L, 7L, "RUNNING", "FAILED", "");
        when(itemMapper.selectAlertCandidates()).thenReturn(List.of(blocked, isolatedInDeadGroup, healedTable));

        assertEquals(2, notifier.notifyOnce());

        ArgumentCaptor<PushPayloadDTO> sent = ArgumentCaptor.forClass(PushPayloadDTO.class);
        verify(messages, times(2)).publishAll(sent.capture());
        assertTrue(sent.getAllValues().get(0).getMessage().startsWith("任务组「g7」部分表已隔离"));
        assertEquals("warning", ((Map<?, ?>) sent.getAllValues().get(0).getData()).get("level"));
        assertEquals("任务组「g7」的表 t71 因表结构变更暂停：检测到表结构变更：ADD_COLUMN", sent.getAllValues().get(1).getMessage());
        assertEquals("/sync/group", sent.getAllValues().get(1).getPath());

        verify(groupMapper).updateAlertedStatus(7L, "DEGRADED");
        verify(groupMapper, never()).updateAlertedStatus(8L, "FAILED"); // already told
        verify(itemMapper).updateAlertedStatus(71L, "DDL_BLOCKED");
        verify(itemMapper, never()).updateAlertedStatus(81L, "FAILED"); // the group's own notice covers it
        verify(itemMapper).updateAlertedStatus(72L, "");
    }

    @Test
    void nothingHappensWithoutAMessageCenter() {
        when(provider.getIfAvailable()).thenReturn(null);
        when(taskMapper.selectAlertCandidates()).thenReturn(List.of(task(1L, "FAILED", "", "x")));
        assertEquals(0, notifier.notifyOnce());
        verify(taskMapper, never()).updateAlertedStatus(any(), any());
    }

    private static SyncTask task(long id, String status, String alerted, String error) {
        SyncTask task = new SyncTask();
        task.setTaskId(id);
        task.setTaskName("t" + id);
        task.setStatus(status);
        task.setAlertedStatus(alerted);
        task.setLastError(error);
        return task;
    }

    private static SyncTaskGroup group(long id, String status, String alerted) {
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(id);
        group.setGroupName("g" + id);
        group.setStatus(status);
        group.setAlertedStatus(alerted);
        group.setLastError("");
        return group;
    }

    private static SyncTaskGroupItem item(long id, long groupId, String status, String alerted, String error) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(groupId);
        item.setSourceTable("t" + id);
        item.setStatus(status);
        item.setAlertedStatus(alerted);
        item.setLastError(error);
        return item;
    }
}
