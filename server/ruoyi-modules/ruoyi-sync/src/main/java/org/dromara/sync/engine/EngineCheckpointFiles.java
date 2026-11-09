package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The engine's checkpoint files, read straight from its storage directory
 * ({@code sync.engine.checkpoint-dir}).
 *
 * <p>Zeta keeps its jobs - and the checkpoint overview the REST API answers from - only in memory,
 * so after an engine restart it no longer knows any job, while the checkpoints it wrote stay on
 * disk: it deletes a job's checkpoints only when the job ends FINISHED or CANCELED. A submit
 * with the old job id and {@code isStartWithSavePoint} restores from the newest of them; with none
 * there, the engine silently starts the job from scratch. So the files are the evidence a resume
 * needs once the engine has forgotten the job, and without them it must be refused.
 *
 * <p>The same rule leaves a job's checkpoints behind for good once the platform gives the job up
 * while it does not run on the engine (stopped or reinitialized when paused or failed, its task
 * deleted, or forgotten by a restarted engine): a stop of such a job does nothing on the engine.
 * {@code EngineCheckpointJanitor} deletes those directories ({@link #jobIds}, {@link #lastWritten},
 * {@link #delete}).
 *
 * <p>Layout (the HDFS / local-file checkpoint storage): {@code <dir>/<jobId>/<timestamp>-<random>-<pipelineId>-<checkpointId>.ser},
 * next to Hadoop's hidden {@code .crc} side files.
 */
@Component
public class EngineCheckpointFiles {

    /** Engine job ids are numbers; anything that could leave the storage directory is never looked up. */
    private static final Pattern JOB_ID = Pattern.compile("[0-9A-Za-z_-]{1,64}");
    private static final Pattern CHECKPOINT_FILE = Pattern.compile("\\d+-\\d+-\\d+-(\\d+)\\.ser");

    /** {@code null}: the storage is not visible to this process. */
    private final Path dir;

    @Autowired
    public EngineCheckpointFiles(SeaTunnelProperties properties) {
        this(StringUtils.isBlank(properties.getCheckpointDir()) ? null : Path.of(properties.getCheckpointDir().trim()));
    }

    public EngineCheckpointFiles(Path dir) {
        this.dir = dir;
    }

    /** No storage visible: every lookup finds nothing. */
    public static EngineCheckpointFiles none() {
        return new EngineCheckpointFiles((Path) null);
    }

    /**
     * The newest checkpoint id stored for {@code jobId}, or {@code null} when there is none - or the
     * storage is not configured, not mounted or not readable, which is not evidence of a checkpoint
     * either. A read error is thrown: that is no answer about the checkpoint.
     */
    public Long latestCheckpointId(String jobId) {
        Path jobDir = jobDir(jobId);
        if (jobDir == null || !Files.isDirectory(jobDir)) return null;
        try (Stream<Path> files = Files.list(jobDir)) {
            return files.filter(EngineCheckpointFiles::isNonEmptyFile)
                .map(file -> CHECKPOINT_FILE.matcher(file.getFileName().toString()))
                .filter(Matcher::matches)
                .map(match -> Long.parseLong(match.group(1)))
                .max(Long::compare)
                .orElse(null);
        } catch (IOException ex) {
            throw new ServiceException("读取引擎 checkpoint 目录失败：" + jobDir + "（" + ex.getMessage() + "）");
        }
    }

    /** Whether the storage directory is configured and visible to this process. */
    public boolean visible() {
        return dir != null && Files.isDirectory(dir);
    }

    /** The job ids that have a directory in the storage; empty when it is not visible. */
    public List<String> jobIds() {
        if (!visible()) return List.of();
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.filter(Files::isDirectory)
                .map(entry -> entry.getFileName().toString())
                .filter(name -> JOB_ID.matcher(name).matches())
                .sorted()
                .toList();
        } catch (IOException ex) {
            throw new ServiceException("读取引擎 checkpoint 目录失败：" + dir + "（" + ex.getMessage() + "）");
        }
    }

    /**
     * When anything in the job's directory was last written - a running job writes a checkpoint every
     * interval; {@code null} when there is no such directory.
     */
    public Instant lastWritten(String jobId) {
        Path jobDir = jobDir(jobId);
        if (jobDir == null || !Files.isDirectory(jobDir)) return null;
        try (Stream<Path> entries = Files.walk(jobDir)) {
            return entries.map(EngineCheckpointFiles::modified)
                .max(Comparator.naturalOrder())
                .map(FileTime::toInstant)
                .orElse(null);
        } catch (IOException ex) {
            throw new ServiceException("读取引擎 checkpoint 目录失败：" + jobDir + "（" + ex.getMessage() + "）");
        }
    }

    /**
     * Deletes the job's checkpoint directory - only for a job the platform has given up for good,
     * whose checkpoints nothing will restore from. Returns whether there was one.
     */
    public boolean delete(String jobId) {
        Path jobDir = jobDir(jobId);
        if (jobDir == null || !Files.isDirectory(jobDir)) return false;
        try (Stream<Path> entries = Files.walk(jobDir)) {
            for (Path entry : entries.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(entry);
            return true;
        } catch (IOException ex) {
            throw new ServiceException("删除引擎 checkpoint 目录失败：" + jobDir + "（" + ex.getMessage() + "）");
        }
    }

    private Path jobDir(String jobId) {
        if (dir == null || StringUtils.isBlank(jobId) || !JOB_ID.matcher(jobId).matches()) return null;
        return dir.resolve(jobId);
    }

    private static FileTime modified(Path path) {
        try {
            return Files.getLastModifiedTime(path);
        } catch (IOException ex) {
            return FileTime.fromMillis(0);
        }
    }

    private static boolean isNonEmptyFile(Path file) {
        try {
            return Files.isRegularFile(file) && Files.size(file) > 0;
        } catch (IOException ex) {
            return false;
        }
    }
}
