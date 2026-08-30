package org.dromara.sync.config;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.SyncTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("dev")
class ResourceProtectionPolicyTest {

    @Test
    void appliesDefaultsForTask() {
        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setReadLimitRowsPerSecond(1000);
        properties.setReadLimitBytesPerSecond(10_485_760L);
        properties.setSnapshotParallelism(1);
        properties.setSourceConnectionLimit(2);
        properties.setMaxReadLimitRowsPerSecond(100_000);
        properties.setMaxReadLimitBytesPerSecond(1_073_741_824L);
        properties.setMaxSnapshotParallelism(4);
        properties.setMaxSourceConnectionLimit(8);

        SyncTask task = new SyncTask();
        new ResourceProtectionPolicy(properties).applyDefaultsAndValidate(task);

        assertEquals(1000, task.getReadLimitRowsPerSecond());
        assertEquals(10_485_760L, task.getReadLimitBytesPerSecond());
        assertEquals(1, task.getSnapshotParallelism());
        assertEquals(2, task.getSourceConnectionLimit());
    }

    @Test
    void rejectsValuesOutsideHardLimits() {
        SeaTunnelProperties properties = new SeaTunnelProperties();
        properties.setReadLimitRowsPerSecond(1000);
        properties.setReadLimitBytesPerSecond(10_485_760L);
        properties.setSnapshotParallelism(1);
        properties.setSourceConnectionLimit(2);
        properties.setMaxReadLimitRowsPerSecond(100_000);
        properties.setMaxReadLimitBytesPerSecond(1_073_741_824L);
        properties.setMaxSnapshotParallelism(4);
        properties.setMaxSourceConnectionLimit(8);

        SyncTask task = new SyncTask();
        task.setSnapshotParallelism(5);
        ServiceException exception = assertThrows(ServiceException.class,
            () -> new ResourceProtectionPolicy(properties).applyDefaultsAndValidate(task));

        org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains("快照并行度"));
    }
}
