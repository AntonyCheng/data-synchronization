package org.dromara.sync.service.impl;

import org.dromara.sync.config.ResourceProtectionPolicy;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskConfigVersionMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.ISyncMetricsService;
import org.dromara.sync.support.SyncLocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@Tag("dev")
class SyncTaskServiceImplDeleteTest {

    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskConfigVersionMapper configVersionMapper = mock(SyncTaskConfigVersionMapper.class);
    private final SyncLocks locks = mock(SyncLocks.class);
    private final PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
    private final SyncTaskServiceImpl service = new SyncTaskServiceImpl(
        taskMapper,
        configVersionMapper,
        mock(DataSourceMapper.class),
        mock(IDataSourceService.class),
        mock(IDataSourceMetadataService.class),
        mock(ResourceProtectionPolicy.class),
        mock(ISyncMetricsService.class),
        locks,
        new TransactionTemplate(transactionManager)
    );

    /** True only while the fake task lock is held. */
    private final AtomicBoolean lockHeld = new AtomicBoolean();

    @BeforeEach
    void wireLock() {
        when(locks.withTaskLock(any(), any())).thenAnswer(invocation -> {
            lockHeld.set(true);
            try {
                return ((Supplier<?>) invocation.getArgument(1)).get();
            } finally {
                lockHeld.set(false);
            }
        });
    }

    @Test
    void finishedTaskCanBeDeleted() {
        SyncTask task = task(" FINISHED ");
        when(taskMapper.selectById(task.getTaskId())).thenReturn(task);
        when(taskMapper.deleteById(task.getTaskId())).thenReturn(1);

        assertEquals(true, service.deleteById(task.getTaskId()));
        verify(taskMapper).deleteById(task.getTaskId());
        verify(configVersionMapper).deleteByTaskId(task.getTaskId());
    }

    @Test
    void runningTaskRemainsProtected() {
        SyncTask task = task("RUNNING");
        when(taskMapper.selectById(task.getTaskId())).thenReturn(task);

        assertThrows(RuntimeException.class, () -> service.deleteById(task.getTaskId()));
        verify(taskMapper, never()).deleteById(task.getTaskId());
        verifyNoInteractions(configVersionMapper);
    }

    /**
     * The point of taking the lifecycle lock for a delete: a concurrent start either finished
     * first (and the delete is refused) or runs after the delete is committed. Committing after
     * the lock is released - what an @Transactional method wrapping the lock does - reopens the
     * race, so the commit has to happen while the lock is still held.
     */
    @Test
    void aDeleteCommitsWhileItStillHoldsTheTaskLock() {
        SyncTask task = task("STOPPED");
        when(taskMapper.selectById(task.getTaskId())).thenReturn(task);
        when(taskMapper.deleteById(task.getTaskId())).thenReturn(1);
        AtomicBoolean committedUnderLock = new AtomicBoolean();
        doAnswer(invocation -> {
            committedUnderLock.set(lockHeld.get());
            return null;
        }).when(transactionManager).commit(any());

        service.deleteById(task.getTaskId());

        verify(locks).withTaskLock(eq(task.getTaskId()), any());
        verify(transactionManager).commit(any());
        assertTrue(committedUnderLock.get(), "the transaction must commit before the task lock is released");
    }

    private static SyncTask task(String status) {
        SyncTask task = new SyncTask();
        task.setTaskId(100L);
        task.setStatus(status);
        return task;
    }
}
