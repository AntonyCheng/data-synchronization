package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("dev")
class SeaTunnelJobConfigGeneratorTest {

    private static final SourceColumns COLUMNS = SourceColumns.fixed(List.of("id", "display_name", "email"));

    private static final Pattern SERVER_ID = Pattern.compile("server-id = \"(\\d+)-(\\d+)\"");

    private static final String MYSQL_URL_OPTIONS = "?connectTimeout=5000&socketTimeout=5000&useSSL=false&allowPublicKeyRetrieval=true&";
    private static final String CDC_SOURCE_URL = "jdbc:mysql://mysql.example:3306/source_db" + MYSQL_URL_OPTIONS
        + "serverTimezone=Asia%2FShanghai&useInformationSchema=true";
    private static final String FULL_SOURCE_URL = "jdbc:mysql://mysql.example:3306/source_db" + MYSQL_URL_OPTIONS
        + "serverTimezone=UTC&useInformationSchema=true";
    private static final String MYSQL_SINK_URL = "jdbc:mysql://mysql-target.example:3306/sink_mysql" + MYSQL_URL_OPTIONS
        + "preserveInstants=false&useInformationSchema=true";

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
            String config = SeaTunnelJobConfigGenerator.generate(cdcTask(taskIds[i]), mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
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
        String config = SeaTunnelJobConfigGenerator.generate(task, mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
        assertTrue(config.contains("FROM `source_db`.`customers`"), config);
    }

    /**
     * Regression: the FULL-mode Jdbc source carried serverTimezone=Asia/Shanghai, which
     * makes Connector/J reinterpret a zoneless DATETIME as Shanghai and shift it to the
     * (UTC) engine JVM zone - source 00:01:00 landed downstream as the previous day
     * 16:01:00, and disagreed with the CDC path. The FULL Jdbc source must pin
     * serverTimezone=UTC so DATETIME round-trips; the MySQL-CDC path keeps Asia/Shanghai
     * (Debezium needs it to resolve GoldenDB's ambiguous CST and does not shift DATETIME).
     */
    @Test
    void fullModeJdbcSourcePinsUtcTimezoneSoDatetimeDoesNotShift() {
        SyncTask full = cdcTask(1L);
        full.setSyncMode("FULL");
        String fullConfig = SeaTunnelJobConfigGenerator.generate(full, mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
        String jdbcSourceLine = fullConfig.lines()
            .filter(line -> line.contains("url = \"jdbc:mysql://"))
            .findFirst().orElseThrow(() -> new AssertionError("no Jdbc source url:\n" + fullConfig));
        assertTrue(jdbcSourceLine.contains("serverTimezone=UTC"), jdbcSourceLine);
        assertFalse(jdbcSourceLine.contains("serverTimezone=Asia"), jdbcSourceLine);

        String cdcConfig = SeaTunnelJobConfigGenerator.generate(cdcTask(1L), mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
        assertTrue(cdcConfig.contains("server-time-zone = \"Asia/Shanghai\""), cdcConfig);
    }

    /**
     * Regression: a MySQL target's Jdbc sink shared the CDC source's URL. Its
     * serverTimezone=Asia/Shanghai made Connector/J (preserveInstants=true by default)
     * re-render every bound Timestamp from the UTC engine JVM zone into Shanghai. As a
     * result, DATETIME, TIMESTAMP and TIME all landed 8 hours late, in FULL and FULL_CDC
     * alike. The sink must write the values unchanged and set no connection time zone.
     */
    @Test
    void mysqlTargetSinkWritesTemporalValuesVerbatim() {
        for (String mode : List.of("FULL", "FULL_CDC", "INCREMENTAL")) {
            SyncTask task = cdcTask(1L);
            task.setSyncMode(mode);
            String config = SeaTunnelJobConfigGenerator.generate(task, mysql(), mysqlTarget(), new SeaTunnelProperties(), COLUMNS).config();
            assertEquals(MYSQL_SINK_URL, urlOf(config, "mysql-target.example"), mode);
        }

        SyncTaskGroup group = new SyncTaskGroup();
        group.setGroupId(7L);
        group.setGroupName("orders");
        group.setSyncMode("FULL_CDC");
        group.setConfigVersion(1);
        SyncTaskGroupItem item = new SyncTaskGroupItem();
        item.setItemId(8L);
        item.setSourceTable("customers");
        item.setTargetTable("customers");
        item.setSyncKeyColumns("id");
        String itemConfig = SyncTaskGroupConfigGenerator.generateItem(group, item, mysql(), mysqlTarget(), new SeaTunnelProperties(), COLUMNS).config();
        assertEquals(MYSQL_SINK_URL, urlOf(itemConfig, "mysql-target.example"), "group item");
    }

    /**
     * The sink fix must leave the source side and the PostgreSQL sink unchanged. Every CDC
     * config and every FULL config contains these URLs, and the fingerprint covers them. A
     * change here would therefore make every paused PostgreSQL and Kafka task refuse to resume.
     */
    @Test
    void sourceAndPostgresUrlsStayByteStable() {
        for (DataSource target : List.of(postgres(), kafka(), mysqlTarget())) {
            String cdc = SeaTunnelJobConfigGenerator.generate(cdcTask(1L), mysql(), target, new SeaTunnelProperties(), COLUMNS).config();
            assertEquals(CDC_SOURCE_URL, urlOf(cdc, "mysql.example"), target.getSourceType());
            SyncTask full = cdcTask(1L);
            full.setSyncMode("FULL");
            String fullConfig = SeaTunnelJobConfigGenerator.generate(full, mysql(), target, new SeaTunnelProperties(), COLUMNS).config();
            assertEquals(FULL_SOURCE_URL, urlOf(fullConfig, "mysql.example"), target.getSourceType());
        }
        String toPostgres = SeaTunnelJobConfigGenerator.generate(cdcTask(1L), mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
        assertEquals("jdbc:postgresql://target.example:5432/sink_db?connectTimeout=5&socketTimeout=5&ssl=false",
            urlOf(toPostgres, "target.example"));
    }

    /**
     * A partial column selection needs the projection transform to actually narrow the
     * columns the CDC connector emits; a selection that covers the whole table must not
     * get one (the generic Sql transform drops NOT NULL / key metadata the sink needs).
     */
    @Test
    void projectionTransformFollowsWhetherTheSelectionNarrowsTheTable() {
        String narrowed = SeaTunnelJobConfigGenerator.generate(cdcTask(1L), mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
        assertTrue(narrowed.contains("transform {"), narrowed);
        assertTrue(narrowed.contains("SELECT `id`, `display_name` FROM ds_source_1"), narrowed);

        SyncTask whole = cdcTask(1L);
        whole.setSelectedColumns("id,display_name,email");
        String direct = SeaTunnelJobConfigGenerator.generate(whole, mysql(), postgres(), new SeaTunnelProperties(), COLUMNS).config();
        assertFalse(direct.contains("transform {"), direct);
        assertTrue(direct.contains("plugin_input = [\"ds_source_1\"]"), direct);
    }

    /** The column lookup may fail but never guess - a guess would change the fingerprint. */
    @Test
    void anUnreadableSourceSchemaIsAnErrorNotASilentProjection() {
        SourceColumns broken = table -> { throw new ServiceException("读取源表字段失败：connection refused"); };
        assertThrows(ServiceException.class,
            () -> SeaTunnelJobConfigGenerator.generate(cdcTask(1L), mysql(), postgres(), new SeaTunnelProperties(), broken));
    }

    /** The {@code url = "..."} value that points at {@code host}. */
    private static String urlOf(String config, String host) {
        String prefix = "url = \"";
        return config.lines().map(String::trim)
            .filter(line -> line.startsWith(prefix) && line.contains("//" + host + ":"))
            .map(line -> line.substring(prefix.length(), line.length() - 1))
            .findFirst().orElseThrow(() -> new AssertionError("no url for " + host + " in:\n" + config));
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

    private static DataSource kafka() {
        DataSource target = new DataSource();
        target.setSourceType("KAFKA");
        target.setHost("broker.example");
        target.setPort(9092);
        return target;
    }

    private static DataSource mysqlTarget() {
        DataSource target = new DataSource();
        target.setSourceType("MYSQL");
        target.setHost("mysql-target.example");
        target.setPort(3306);
        target.setDatabaseName("sink_mysql");
        target.setUsername("writer");
        target.setPassword("secret");
        target.setSslEnabled("0");
        return target;
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
