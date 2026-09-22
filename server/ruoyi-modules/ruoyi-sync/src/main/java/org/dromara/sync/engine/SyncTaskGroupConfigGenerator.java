package org.dromara.sync.engine;

import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;

import java.util.List;

/**
 * Task groups run one SeaTunnel job per table item. Each item is projected onto a
 * {@link SyncTask} view so the single-table generator produces its document; the group
 * preview simply concatenates them.
 */
public final class SyncTaskGroupConfigGenerator {

    private SyncTaskGroupConfigGenerator() {
    }

    public static GeneratedConfig generate(SyncTaskGroup group, List<SyncTaskGroupItem> items,
                                    DataSource source, DataSource target, SeaTunnelProperties properties, SourceColumns sourceColumns) {
        StringBuilder config = new StringBuilder();
        StringBuilder redacted = new StringBuilder();
        for (SyncTaskGroupItem item : items) {
            SyncTask task = toTask(group, item);
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = SeaTunnelJobConfigGenerator.generate(task, source, target, properties, sourceColumns);
            config.append("# item ").append(item.getItemId()).append(' ').append(item.getSourceTable()).append("\n")
                .append(generated.config()).append("\n");
            redacted.append("# item ").append(item.getItemId()).append(' ').append(item.getSourceTable()).append("\n")
                .append(generated.redactedConfig()).append("\n");
        }
        return new GeneratedConfig("ds-group-" + group.getGroupId() + "-v" + group.getConfigVersion(), config.toString(), redacted.toString());
    }

    public static SeaTunnelJobConfigGenerator.GeneratedConfig generateItem(SyncTaskGroup group, SyncTaskGroupItem item,
                                                                      DataSource source, DataSource target,
                                                                      SeaTunnelProperties properties, SourceColumns sourceColumns) {
        return SeaTunnelJobConfigGenerator.generate(toTask(group, item), source, target, properties, sourceColumns);
    }

    /** Projects a group + item onto the single-table task shape the generator and Kafka bridge consume. */
    public static SyncTask toTask(SyncTaskGroup group, SyncTaskGroupItem item) {
        SyncTask task = new SyncTask();
        task.setTaskId(item.getItemId());
        task.setTaskName(group.getGroupName() + " / " + item.getSourceTable());
        task.setSourceId(group.getSourceId());
        task.setTargetId(group.getTargetId());
        task.setSourceTable(item.getSourceTable());
        task.setTargetSchema(item.getTargetSchema());
        task.setTargetTable(item.getTargetTable());
        task.setSyncMode(group.getSyncMode());
        task.setConfigVersion(group.getConfigVersion());
        task.setKafkaOutputFormat(group.getKafkaOutputFormat());
        task.setDdlPolicy(item.getDdlPolicy());
        task.setSelectedColumns(item.getSelectedColumns());
        task.setSyncKeyColumns(item.getSyncKeyColumns());
        task.setReadLimitRowsPerSecond(group.getReadLimitRowsPerSecond());
        task.setReadLimitBytesPerSecond(group.getReadLimitBytesPerSecond());
        task.setSnapshotParallelism(group.getSnapshotParallelism());
        task.setSourceConnectionLimit(group.getSourceConnectionLimit());
        return task;
    }

    public record GeneratedConfig(String jobName, String config, String redactedConfig) {
    }
}
