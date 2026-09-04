package org.dromara.sync.service.impl;

import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskConfigVersionMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@Tag("dev")
class SyncTaskServiceImplDeleteTest {

    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskConfigVersionMapper configVersionMapper = mock(SyncTaskConfigVersionMapper.class);
    private final SyncTaskServiceImpl service = new SyncTaskServiceImpl(
        taskMapper,
        configVersionMapper,
        mock(DataSourceMapper.class),
        mock(IDataSourceService.class),
        mock(IDataSourceMetadataService.class),
        mock(ResourceProtectionPolicy.class)
    );

    @Test
    void finishedTaskCanBeDeleted() {
        SyncTask task = task(" FINISHED ");
        when(taskMapper.selectById(task.getTaskId())).thenReturn(task);
        when(taskMapper.deleteById(task.getTaskId())).thenReturn(1);

        assertEquals(true, service.deleteById(task.getTaskId()));
        verify(taskMapper).deleteById(task.getTaskId());
        verify(configVersionMapper).delete(any());
    }

    @Test
    void runningTaskRemainsProtected() {
        SyncTask task = task("RUNNING");
        when(taskMapper.selectById(task.getTaskId())).thenReturn(task);

        assertThrows(RuntimeException.class, () -> service.deleteById(task.getTaskId()));
        verify(taskMapper, never()).deleteById(task.getTaskId());
        verifyNoInteractions(configVersionMapper);
    }

    private static SyncTask task(String status) {
        SyncTask task = new SyncTask();
        task.setTaskId(100L);
        task.setStatus(status);
        return task;
    }
}
