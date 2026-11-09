package org.dromara.sync.engine;

import org.dromara.sync.config.SeaTunnelProperties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

/** What counts as a checkpoint on disk - the evidence a resume needs once the engine forgot the job. */
@Tag("dev")
class EngineCheckpointFilesTest {

    @TempDir
    Path dir;

    @Test
    void theNewestCheckpointOfTheJobIsFoundAmongTheStorageFiles() throws IOException {
        Path job = Files.createDirectories(dir.resolve("1171952871948877825"));
        Files.writeString(job.resolve("1794180593644-606-1-41.ser"), "state");
        Files.writeString(job.resolve("1794180643650-413-1-51.ser"), "state");
        Files.writeString(job.resolve("1794180598645-908-1-42.ser"), "state");
        // Hadoop's checksum side files, an empty (torn) file and anything else are not checkpoints.
        Files.writeString(job.resolve(".1794180643650-413-1-99.ser.crc"), "crc");
        Files.createFile(job.resolve("1794180700000-1-1-77.ser"));
        Files.writeString(job.resolve("notes.txt"), "x");

        assertEquals(51L, new EngineCheckpointFiles(dir).latestCheckpointId("1171952871948877825"));
    }

    @Test
    void noFilesNoDirectoryOrNoStorageIsNoCheckpoint() throws IOException {
        Files.createDirectories(dir.resolve("7"));
        EngineCheckpointFiles files = new EngineCheckpointFiles(dir);

        assertNull(files.latestCheckpointId("7"));
        assertNull(files.latestCheckpointId("8"));
        assertNull(files.latestCheckpointId(null));
        assertNull(EngineCheckpointFiles.none().latestCheckpointId("7"));
        assertNull(new EngineCheckpointFiles(dir.resolve("not-mounted")).latestCheckpointId("7"));
        assertNull(new EngineCheckpointFiles(new SeaTunnelProperties()).latestCheckpointId("7"), "blank sync.engine.checkpoint-dir");
    }

    @Test
    void aJobIdThatWouldLeaveTheStorageDirectoryIsNeverLookedUp() throws IOException {
        Path outside = Files.createDirectories(dir.resolve("outside"));
        Files.writeString(outside.resolve("1-1-1-5.ser"), "state");
        EngineCheckpointFiles files = new EngineCheckpointFiles(dir.resolve("storage"));
        Files.createDirectories(dir.resolve("storage"));

        assertNull(files.latestCheckpointId("../outside"));
    }

    @Test
    void theStorageDirectoryComesFromTheEngineProperties() throws IOException {
        Path job = Files.createDirectories(dir.resolve("9"));
        Files.writeString(job.resolve("1-2-1-3.ser"), "state");
        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setCheckpointDir(" " + dir + " ");

        assertEquals(3L, new EngineCheckpointFiles(properties).latestCheckpointId("9"));
    }

    @Test
    void theJobDirectoriesTheirLastWriteAndTheirDeletion() throws IOException {
        Path job = Files.createDirectories(dir.resolve("1171952871948877825"));
        Path file = Files.writeString(job.resolve("1794180593644-606-1-41.ser"), "state");
        Files.createDirectories(dir.resolve("not a job id"));
        Files.writeString(dir.resolve("stray.txt"), "x");
        Instant written = Instant.parse("2026-11-01T00:00:00Z");
        Files.setLastModifiedTime(file, FileTime.from(written));
        Files.setLastModifiedTime(job, FileTime.from(written.minusSeconds(60)));
        EngineCheckpointFiles files = new EngineCheckpointFiles(dir);

        assertTrue(files.visible());
        assertEquals(List.of("1171952871948877825"), files.jobIds());
        assertEquals(written, files.lastWritten("1171952871948877825"));
        assertNull(files.lastWritten("404"));

        assertTrue(files.delete("1171952871948877825"));
        assertFalse(Files.exists(job));
        assertFalse(files.delete("1171952871948877825"));
        assertTrue(Files.exists(dir.resolve("stray.txt")), "only the job's own directory goes");
    }

    @Test
    void nothingIsListedOrDeletedOutsideAVisibleStorage() throws IOException {
        Files.createDirectories(dir.resolve("outside"));
        EngineCheckpointFiles files = new EngineCheckpointFiles(dir.resolve("storage"));

        assertFalse(files.visible());
        assertEquals(List.of(), files.jobIds());
        assertEquals(List.of(), EngineCheckpointFiles.none().jobIds());
        Files.createDirectories(dir.resolve("storage"));
        assertFalse(files.delete("../outside"));
        assertTrue(Files.exists(dir.resolve("outside")));
    }
}
