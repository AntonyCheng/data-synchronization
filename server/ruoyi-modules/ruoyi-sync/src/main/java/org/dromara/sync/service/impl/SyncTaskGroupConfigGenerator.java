package org.dromara.sync.service.impl;

import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;

import java.util.List;

/** Generates one safe single-table engine document per selected table. */
final class SyncTaskGroupConfigGenerator {

    private SyncTaskGroupConfigGenerator() {
    }

    static GeneratedConfig generate(SyncTaskGroup group, List<SyncTaskGroupItem> items,
                                    DataSource source, DataSource target, SeaTunnelProperties properties) {
        StringBuilder config = new StringBuilder();
        StringBuilder redacted = new StringBuilder();
        for (SyncTaskGroupItem item : items) {
            SyncTask task = toTask(group, item);
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = SeaTunnelJobConfigGenerator.generate(task, source, target, properties);
            config.append("# item ").append(item.getItemId()).append(' ').append(item.getSourceTable()).append("\n")
                .append(generated.config()).append("\n");
            redacted.append("# item ").append(item.getItemId()).append(' ').append(item.getSourceTable()).append("\n")
                .append(generated.redactedConfig()).append("\n");
        }
        return new GeneratedConfig("ds-group-" + group.getGroupId() + "-v" + group.getConfigVersion(), config.toString(), redacted.toString());
    }

    static SeaTunnelJobConfigGenerator.GeneratedConfig generateItem(SyncTaskGroup group, SyncTaskGroupItem item,
                                                                      DataSource source, DataSource target,
                                                                      SeaTunnelProperties properties) {
        return SeaTunnelJobConfigGenerator.generate(toTask(group, item), source, target, properties);
    }

    private static SyncTask toTask(SyncTaskGroup group, SyncTaskGroupItem item) {
        SyncTask task = new SyncTask();
        task.setTaskId(item.getItemId());
        task.setTaskName(group.getGroupName() + " / " + item.getSourceTable());
        task.setSourceId(group.getSourceId());
        task.setTargetId(group.getTargetId());
        task.setSourceTable(item.getSourceTable());
        task.setTargetSchema(item.getTargetSchema());
        task.setTargetTable(item.getTargetTable());
        task.setSyncMode(group.getSyncMode());
        task.setDdlPolicy(item.getDdlPolicy());
        task.setSelectedColumns(item.getSelectedColumns());
        task.setSyncKeyColumns(item.getSyncKeyColumns());
        task.setReadLimitRowsPerSecond(group.getReadLimitRowsPerSecond());
        task.setReadLimitBytesPerSecond(group.getReadLimitBytesPerSecond());
        task.setSnapshotParallelism(group.getSnapshotParallelism());
        task.setSourceConnectionLimit(group.getSourceConnectionLimit());
        return task;
    }

    record GeneratedConfig(String jobName, String config, String redactedConfig) {
    }
}
