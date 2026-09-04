package org.dromara.sync.service.impl;

import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class SeaTunnelJobConfigGeneratorTest {

    private static final Pattern SERVER_ID = Pattern.compile("server-id = \"(\\d+)-(\\d+)\"");

    /**
     * Regression for a server-id collision: sequential (or same-millisecond Snowflake,
     * as task-group items get) task IDs previously mapped to overlapping "X-(X+3)"
     * ranges, which MySQL's replication protocol rejects when both CDC jobs connect
     * to the same source concurrently.
     */
    @Test
    void adjacentTaskIdsGetNonOverlappingServerIdRanges() {
        long[] taskIds = {1L, 2L, 3L, 4L, 5L, 100L, 101L};
        int[][] ranges = new int[taskIds.length][];
        for (int i = 0; i < taskIds.length; i++) {
            String config = SeaTunnelJobConfigGenerator.generate(cdcTask(taskIds[i]), mysql(), postgres(), new SeaTunnelProperties()).config();
            ranges[i] = serverIdRange(config);
        }
        for (int i = 0; i < ranges.length; i++) {
            for (int j = i + 1; j < ranges.length; j++) {
                assertFalse(overlaps(ranges[i], ranges[j]),
                    "task " + taskIds[i] + " range " + Arrays.toString(ranges[i])
                        + " overlaps task " + taskIds[j] + " range " + Arrays.toString(ranges[j]));
            }
        }
    }

    /** Regression: the FULL-mode source query concatenated the table name unquoted. */
    @Test
    void fullModeQueryQuotesTheSourceTableIdentifier() {
        SyncTask task = cdcTask(1L);
        task.setSyncMode("FULL");
        String config = SeaTunnelJobConfigGenerator.generate(task, mysql(), postgres(), new SeaTunnelProperties()).config();
        assertTrue(config.contains("FROM `source_db`.`customers`"), config);
    }

    /**
     * A partial column selection still needs the projection transform to actually narrow
     * the columns the CDC connector emits. This also exercises the "can't reach the
     * source to check" fallback in isFullColumnSelection() (mysql.example is unreachable
     * here) - it must default to keeping the transform rather than silently dropping the
     * column restriction.
     */
    @Test
    void partialColumnSelectionKeepsTheProjectionTransform() {
        String config = SeaTunnelJobConfigGenerator.generate(cdcTask(1L), mysql(), postgres(), new SeaTunnelProperties()).config();
        assertTrue(config.contains("transform {"), config);
        assertTrue(config.contains("SELECT `id`, `display_name` FROM ds_source_1"), config);
    }

    private static int[] serverIdRange(String config) {
        Matcher matcher = SERVER_ID.matcher(config);
        if (!matcher.find()) throw new AssertionError("no server-id in generated config:\n" + config);
        return new int[] {Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2))};
    }

    private static boolean overlaps(int[] left, int[] right) {
        return left[0] <= right[1] && right[0] <= left[1];
    }

    private static SyncTask cdcTask(long taskId) {
        SyncTask task = new SyncTask();
        task.setTaskId(taskId);
        task.setConfigVersion(1);
        task.setSourceTable("source_db.customers");
        task.setTargetTable("customers");
        task.setSyncMode("FULL_CDC");
        task.setSelectedColumns("id,display_name");
        task.setSyncKeyColumns("id");
        task.setSnapshotParallelism(1);
        task.setReadLimitRowsPerSecond(100);
        task.setReadLimitBytesPerSecond(1024L);
        task.setSourceConnectionLimit(2);
        return task;
    }

    private static DataSource mysql() {
        DataSource source = new DataSource();
        source.setSourceType("MYSQL");
        source.setHost("mysql.example");
        source.setPort(3306);
        source.setDatabaseName("source_db");
        source.setUsername("reader");
        source.setPassword("secret");
        source.setSslEnabled("0");
        return source;
    }

    private static DataSource postgres() {
        DataSource target = new DataSource();
        target.setSourceType("POSTGRESQL");
        target.setHost("target.example");
        target.setPort(5432);
        target.setDatabaseName("sink_db");
        target.setUsername("writer");
        target.setPassword("secret");
        target.setSslEnabled("0");
        return target;
    }
}
