package org.dromara.sync.config;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.springframework.stereotype.Component;

/** Applies conservative source-protection defaults and enforces platform hard limits. */
@Component
@RequiredArgsConstructor
public class ResourceProtectionPolicy {

    private final SeaTunnelProperties properties;

    public void applyDefaultsAndValidate(SyncTask task) {
        task.setReadLimitRowsPerSecond(normalize(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond(),
            properties.getMaxReadLimitRowsPerSecond(), "最大行数/秒"));
        task.setReadLimitBytesPerSecond(normalize(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond(),
            properties.getMaxReadLimitBytesPerSecond(), "最大字节数/秒"));
        task.setSnapshotParallelism(normalize(task.getSnapshotParallelism(), properties.getSnapshotParallelism(),
            properties.getMaxSnapshotParallelism(), "快照并行度"));
        task.setSourceConnectionLimit(normalize(task.getSourceConnectionLimit(), properties.getSourceConnectionLimit(),
            properties.getMaxSourceConnectionLimit(), "源端连接池上限"));
    }

    public void applyDefaultsAndValidate(SyncTaskGroup group) {
        group.setReadLimitRowsPerSecond(normalize(group.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond(),
            properties.getMaxReadLimitRowsPerSecond(), "最大行数/秒"));
        group.setReadLimitBytesPerSecond(normalize(group.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond(),
            properties.getMaxReadLimitBytesPerSecond(), "最大字节数/秒"));
        group.setSnapshotParallelism(normalize(group.getSnapshotParallelism(), properties.getSnapshotParallelism(),
            properties.getMaxSnapshotParallelism(), "快照并行度"));
        group.setSourceConnectionLimit(normalize(group.getSourceConnectionLimit(), properties.getSourceConnectionLimit(),
            properties.getMaxSourceConnectionLimit(), "源端连接池上限"));
    }

    private static int normalize(Integer requested, int fallback, int maximum, String label) {
        int value = requested == null ? fallback : requested;
        validate(value, maximum, label);
        return value;
    }

    private static long normalize(Long requested, long fallback, long maximum, String label) {
        long value = requested == null ? fallback : requested;
        validate(value, maximum, label);
        return value;
    }

    private static void validate(long value, long maximum, String label) {
        if (value < 1 || value > maximum) {
            throw new ServiceException(label + "必须在 1 到 " + maximum + " 之间");
        }
    }
}
