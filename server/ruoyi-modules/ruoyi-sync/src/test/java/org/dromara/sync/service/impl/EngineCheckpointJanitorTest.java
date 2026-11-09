package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.engine.EngineCheckpointFiles;
import org.dromara.sync.engine.SeaTunnelRestClient;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.support.SyncLocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Which engine checkpoint directories go: only those of jobs no task or table uses, idle past the
 * grace, and not running on the engine - the engine itself never deletes them for a job it does
 * not run.
 */
@Tag("dev")
class EngineCheckpointJanitorTest {

    private static final Instant NOW = Instant.parse("2026-11-09T12:00:00Z");
    private static final Instant IDLE = NOW.minus(Duration.ofHours(3));

    @TempDir
    Path dir;

    private final SeaTunnelRestClient restClient = mock(SeaTunnelRestClient.class);
    private final SyncTaskMapper taskMapper = mock(SyncTaskMapper.class);
    private final SyncTaskGroupItemMapper itemMapper = mock(SyncTaskGroupItemMapper.class);
    private final RedissonClient redisson = mock(RedissonClient.class);
    private final RLock lock = mock(RLock.class);
    private final SeaTunnelProperties properties = new SeaTunnelProperties();
    private EngineCheckpointJanitor janitor;

    @BeforeEach
    void world() throws Exception {
        when(redisson.getLock(SyncLocks.CHECKPOINT_CLEANUP_LOCK)).thenReturn(lock);
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        when(taskMapper.selectEngineJobIds()).thenReturn(Set.of());
        when(itemMapper.selectEngineJobIds()).thenReturn(Set.of());
        janitor = new EngineCheckpointJanitor(new EngineCheckpointFiles(dir), restClient, taskMapper, itemMapper, properties,
            new SyncLocks(redisson));
    }

    @Test
    void onlyIdleCheckpointsOfJobsNoOwnerUsesAndTheEngineDoesNotRunAreDeleted() throws IOException {
        checkpoints("101", IDLE);  // a task still points at it (paused, say)
        checkpoints("102", IDLE);  // a group item still points at it
        checkpoints("103", IDLE);  // given up; the restarted engine has no record of it
        checkpoints("104", IDLE);  // given up while paused; the engine still lists it SAVEPOINT_DONE
        checkpoints("105", IDLE);  // unreferenced but running: an orphan the sweeper is about to adopt
        checkpoints("106", NOW.minusSeconds(60)); // unreferenced and fresh: just submitted
        when(taskMapper.selectEngineJobIds()).thenReturn(Set.of("101"));
        when(itemMapper.selectEngineJobIds()).thenReturn(Set.of("102"));
        engine("103", null);
        engine("104", "SAVEPOINT_DONE");
        engine("105", "RUNNING");

        assertEquals(List.of("103", "104"), janitor.cleanOnce(NOW));

        assertTrue(Files.exists(dir.resolve("101")));
        assertTrue(Files.exists(dir.resolve("102")));
        assertFalse(Files.exists(dir.resolve("103")));
        assertFalse(Files.exists(dir.resolve("104")));
        assertTrue(Files.exists(dir.resolve("105")));
        assertTrue(Files.exists(dir.resolve("106")));
        // Referenced and fresh directories are never even asked about.
        verify(restClient, never()).status("101");
        verify(restClient, never()).status("102");
        verify(restClient, never()).status("106");
    }

    @Test
    void anEngineThatDoesNotAnswerEndsThePassWithoutDeletingOnAGuess() throws IOException {
        checkpoints("103", IDLE);
        when(restClient.status("103")).thenThrow(new ServiceException("SeaTunnel 接口不可用：timeout"));

        assertThrows(ServiceException.class, () -> janitor.cleanOnce(NOW));
        assertTrue(Files.exists(dir.resolve("103")));
    }

    @Test
    void nothingHappensWithoutAVisibleStorageOrAnythingIdle() throws IOException {
        EngineCheckpointJanitor blind = new EngineCheckpointJanitor(EngineCheckpointFiles.none(), restClient, taskMapper, itemMapper,
            properties, new SyncLocks(redisson));
        assertEquals(List.of(), blind.cleanOnce(NOW));

        checkpoints("106", NOW.minusSeconds(60));
        assertEquals(List.of(), janitor.cleanOnce(NOW));
        verifyNoInteractions(taskMapper, itemMapper, restClient);
    }

    @Test
    void aPassIsSkippedWhileAnotherInstanceRunsIt() throws Exception {
        checkpoints("103", IDLE);
        when(lock.tryLock(0, -1, TimeUnit.SECONDS)).thenReturn(false);

        assertFalse(janitor.cleanExclusively(NOW));

        assertTrue(Files.exists(dir.resolve("103")));
        verifyNoInteractions(taskMapper, itemMapper);
        verify(restClient, never()).status(anyString());
    }

    @Test
    void aPassRunsUnderTheLock() throws IOException {
        checkpoints("103", IDLE);
        engine("103", null);

        assertTrue(janitor.cleanExclusively(NOW));

        assertFalse(Files.exists(dir.resolve("103")));
    }

    /** A job directory with one checkpoint file, everything in it last written at {@code written}. */
    private void checkpoints(String jobId, Instant written) throws IOException {
        Path job = Files.createDirectories(dir.resolve(jobId));
        Path file = Files.writeString(job.resolve("1794180593644-606-1-41.ser"), "state");
        Files.setLastModifiedTime(file, FileTime.from(written));
        Files.setLastModifiedTime(job, FileTime.from(written));
    }

    private void engine(String jobId, String status) {
        when(restClient.status(jobId)).thenReturn(new SeaTunnelRestClient.JobSnapshot(jobId, null, status, null, null));
    }
}
