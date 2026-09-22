package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskGroupMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("dev")
class KafkaBridgeReconcilerTest {

    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskGroupMapper groupMapper = mock(SyncTaskGroupMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final DataSourceMapper dataSourceMapper = mock(DataSourceMapper.class);
    private final KafkaTaskBridgeService bridge = mock(KafkaTaskBridgeService.class);
    private final KafkaBridgeReconciler reconciler = new KafkaBridgeReconciler(taskMapper, groupMapper, itemMapper, dataSourceMapper, bridge);

    @Test
    void startsMissingBridgesAndRetiresStaleOnes() {
        stubWorld();
        // Locally: task 1 already bridged, 99 is a leftover from a task stopped elsewhere.
        when(bridge.localOwnerIds()).thenReturn(Set.of(1L, 99L));
        when(bridge.isRunning(1L)).thenReturn(true);

        int changed = reconciler.reconcileOnce();

        assertEquals(2, changed);
        verify(bridge).stop(99L);
        verify(bridge, never()).stop(1L);
        // The PG task (2) and the PAUSING Kafka task (3) never get a bridge; the RUNNING group item (5) does.
        verify(bridge, never()).start(any(), any(), anyString());
        ArgumentCaptor<SyncTask> started = ArgumentCaptor.forClass(SyncTask.class);
        verify(bridge, times(1)).startGroupItem(started.capture(), any(), eq("src_db"));
        assertEquals(5L, started.getValue().getTaskId());
    }

    @Test
    void aBridgeThatCannotStartDoesNotBreakThePass() {
        stubWorld();
        when(bridge.localOwnerIds()).thenReturn(Set.of(1L));
        when(bridge.isRunning(1L)).thenReturn(true);
        doThrow(new ServiceException("Kafka 目标 topic 不存在")).when(bridge).startGroupItem(any(), any(), anyString());

        assertEquals(0, reconciler.reconcileOnce());
        assertEquals(0, reconciler.reconcileOnce());
        verify(bridge, times(2)).startGroupItem(any(), any(), anyString());
    }

    private void stubWorld() {
        DataSource kafka = dataSource(10L, "KAFKA", null);
        DataSource postgres = dataSource(11L, "POSTGRESQL", "sink");
        DataSource mysql = dataSource(12L, "MYSQL", "src_db");
        when(dataSourceMapper.selectById(10L)).thenReturn(kafka);
        when(dataSourceMapper.selectById(11L)).thenReturn(postgres);
        when(dataSourceMapper.selectById(12L)).thenReturn(mysql);
        when(taskMapper.selectActive()).thenReturn(List.of(
            task(1L, "RUNNING", 10L), task(2L, "RUNNING", 11L), task(3L, "PAUSING", 10L)));
        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(7L);
        group.setSourceId(12L);
        group.setTargetId(10L);
        group.setStatus("DEGRADED");
        group.setConfigVersion(1);
        when(groupMapper.selectLive()).thenReturn(List.of(group));
        when(itemMapper.selectByGroupId(7L)).thenReturn(List.of(item(5L, "RUNNING"), item(6L, "DDL_BLOCKED")));
    }

    private static SyncTask task(long id, String status, long targetId) {
        SyncTask task = new SyncTask();
        task.setTaskId(id);
        task.setStatus(status);
        task.setSourceId(12L);
        task.setTargetId(targetId);
        task.setTargetTable("topic-" + id);
        task.setSyncKeyColumns("id");
        return task;
    }

    private static SyncTaskGroupItem item(long id, String status) {
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(id);
        item.setGroupId(7L);
        item.setStatus(status);
        item.setSourceTable("t" + id);
        item.setTargetTable("topic-" + id);
        item.setSyncKeyColumns("id");
        return item;
    }

    private static DataSource dataSource(long id, String type, String database) {
        DataSource dataSource = new DataSource();
        dataSource.setSourceId(id);
        dataSource.setSourceType(type);
        dataSource.setDatabaseName(database);
        dataSource.setHost("h");
        dataSource.setPort(1);
        return dataSource;
    }
}
