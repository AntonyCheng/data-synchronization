package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.SyncTaskGroup;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
        + "serverTimezone=UTC&preserveInstants=false&useInformationSchema=true";
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
     * Regression, twice over. The FULL-mode Jdbc source once carried serverTimezone=Asia/Shanghai,
     * which shifted DATETIME by -8 h on the UTC engine; serverTimezone=UTC fixed that only for a
     * UTC engine JVM (Connector/J 8.0.33 measured +8 h on Asia/Shanghai, -5 h on New_York).
     * preserveInstants=false reads DATETIME / TIMESTAMP unchanged on any JVM zone, and the FULL
     * source ignores the data source's configured zone: it has nothing to convert.
     */
    @Test
    void fullModeJdbcSourceReadsTemporalValuesIndependentlyOfTheEngineZone() {
        SyncTask full = cdcTask(1L);
        full.setSyncMode("FULL");
        for (String zone : new String[]{null, "", "UTC", "America/New_York"}) {
            DataSource source = mysql();
            source.setServerTimeZone(zone);
            String fullConfig = SeaTunnelJobConfigGenerator.generate(full, source, postgres(), new SeaTunnelProperties(), COLUMNS).config();
            assertEquals(FULL_SOURCE_URL, urlOf(fullConfig, "mysql.example"), "zone " + zone);
        }
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
     * change here would therefore make every paused PostgreSQL and Kafka task refuse to resume
     * (the FULL source URL did change once, deliberately - see
     * {@link #aFullTaskFingerprintedBeforePreserveInstantsStillResumes}).
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
     * Fingerprints generated by main@1f59500, before the source zone became configurable, for one
     * task in every mode against every target type. A data source whose zone is blank - every
     * data source that existed before migration 023 - must keep generating exactly these CDC
     * configs, or its paused / failed CDC tasks would be refused on resume. Writing the old
     * default out explicitly must not change them either.
     */
    private static final Map<String, String> PRE_ZONE_FINGERPRINTS = Map.ofEntries(
        Map.entry("POSTGRESQL FULL_CDC", "9e84af42468a27bad0b0650cb4931d6f9e687336f6a9198f469f8491a72aa57b"),
        Map.entry("POSTGRESQL INCREMENTAL", "8ffa90df0dc5dba637595f0a6fcabd4cd4b0196dc8a78c3de7d5ae3953ac1bc3"),
        Map.entry("POSTGRESQL INCREMENTAL_TS", "0c138779bddbabc21cd9787d90d84658dfe81c298d4d5d77940d5c45da309833"),
        Map.entry("MYSQL FULL_CDC", "5a82aa43900c108139dda3dc4938353c13b73f8f191f9d49349cc1e0527d57bb"),
        Map.entry("MYSQL INCREMENTAL", "6ae9b06c6cb5d472113b3cd9da506516156e3232718728361ab14bdebc7e397d"),
        Map.entry("MYSQL INCREMENTAL_TS", "8793dd6099a80e060cbea1ca114b7f41f469e550d6c11d9d4ea6ddfe407bd0a5"),
        Map.entry("KAFKA FULL_CDC", "545a5ea23484e1aa83be42553a46adf4a5401cbf4319c14b7eacbd4a58d77ee9"),
        Map.entry("KAFKA INCREMENTAL", "6e69c80ebdea8f9857f88f1a70bbd82ccf291db1685b8ca0ba279a91df6c66d5"),
        Map.entry("KAFKA INCREMENTAL_TS", "b78a879a56e13f972486213c74fcba6dcf26c5aae920875e504b2a6911f5b35d"));

    /** FULL fingerprints from the same commit: the FULL source URL changed on purpose since. */
    private static final Map<String, String> PRE_PRESERVE_INSTANTS_FULL_FINGERPRINTS = Map.of(
        "POSTGRESQL FULL", "1fcaf97e43c757f8478d1a9443053c0b5c7e153354d1d35c6aecaff2bf89e71d",
        "MYSQL FULL", "0e0b4e69d3d398ede3c2e1435da7e1b872f0254ad4637403d344264532954f9a",
        "KAFKA FULL", "226e6267c2aa4188be2e68d6e72391c83d2927c544c16ccb630942a96f499d6b");

    @Test
    void aSourceWithoutAConfiguredZoneGeneratesTheCdcConfigsItDidBefore() {
        for (String zone : new String[]{null, "", "  ", "Asia/Shanghai"}) {
            DataSource source = mysql();
            source.setServerTimeZone(zone);
            PRE_ZONE_FINGERPRINTS.forEach((key, fingerprint) -> {
                String[] parts = key.split(" ");
                SeaTunnelJobConfigGenerator.GeneratedConfig generated = SeaTunnelJobConfigGenerator.generate(
                    goldenTask(parts[1]), source, targetOfType(parts[0]), new SeaTunnelProperties(), COLUMNS);
                assertEquals(fingerprint, generated.fingerprint(), key + " with zone '" + zone + "'");
            });
        }
    }

    /**
     * A FULL job can be paused with a savepoint and resumed (nothing restricts pause to CDC), so
     * the FULL source URL change must not strand one: the fingerprint it was stored under before
     * preserveInstants=false is still accepted. The rewrite is that exact URL fragment and
     * nothing else - a CDC fingerprint from another zone must still be refused.
     */
    @Test
    void aFullTaskFingerprintedBeforePreserveInstantsStillResumes() {
        PRE_PRESERVE_INSTANTS_FULL_FINGERPRINTS.forEach((key, fingerprint) -> {
            SeaTunnelJobConfigGenerator.GeneratedConfig generated = SeaTunnelJobConfigGenerator.generate(
                goldenTask("FULL"), mysql(), targetOfType(key.split(" ")[0]), new SeaTunnelProperties(), COLUMNS);
            assertFalse(fingerprint.equals(generated.fingerprint()), key + " fingerprint changed on purpose");
            assertTrue(generated.matchesFingerprint(fingerprint), key);
            assertTrue(generated.matchesFingerprint(generated.fingerprint()), key);
        });

        DataSource utc = mysql();
        utc.setServerTimeZone("UTC");
        SeaTunnelJobConfigGenerator.GeneratedConfig cdc = SeaTunnelJobConfigGenerator.generate(
            goldenTask("FULL_CDC"), utc, postgres(), new SeaTunnelProperties(), COLUMNS);
        assertFalse(cdc.matchesFingerprint(PRE_ZONE_FINGERPRINTS.get("POSTGRESQL FULL_CDC")),
            "the binlog position was recorded under Asia/Shanghai: a zone change needs a reinitialize");
        assertFalse(cdc.matchesFingerprint(null));
    }

    @Test
    void aConfiguredZoneReachesServerTimeZoneTheSourceUrlAndTheStartupTimestamp() {
        record Expected(String zone, String urlValue, long startupMillis) {
        }
        // goldenTask's INCREMENTAL_TS start is the source wall clock 2026-09-01 08:30.
        List<Expected> cases = List.of(
            new Expected("UTC", "UTC", 1788251400000L),
            new Expected("America/New_York", "America%2FNew_York", 1788265800000L),
            new Expected("Etc/GMT+5", "Etc%2FGMT%2B5", 1788269400000L),
            new Expected("Asia/Shanghai", "Asia%2FShanghai", 1788222600000L));
        for (Expected expected : cases) {
            DataSource source = mysql();
            source.setServerTimeZone(expected.zone());
            for (DataSource target : List.of(postgres(), mysqlTarget(), kafka())) {
                for (String mode : List.of("FULL_CDC", "INCREMENTAL_TS")) {
                    String config = SeaTunnelJobConfigGenerator.generate(goldenTask(mode), source, target, new SeaTunnelProperties(), COLUMNS).config();
                    String context = expected.zone() + " -> " + target.getSourceType() + " " + mode;
                    assertTrue(config.contains("    server-time-zone = \"" + expected.zone() + "\"\n"), context + "\n" + config);
                    assertEquals("jdbc:mysql://mysql.example:3306/source_db" + MYSQL_URL_OPTIONS + "serverTimezone="
                        + expected.urlValue() + "&useInformationSchema=true", urlOf(config, "mysql.example"), context);
                    if (mode.equals("INCREMENTAL_TS")) {
                        assertTrue(config.contains("startup.timestamp = " + expected.startupMillis() + "\n"), context + "\n" + config);
                    }
                }
            }
        }
    }

    /** Saving validates the zone; a value written around the service must not reach the engine. */
    @Test
    void anInvalidStoredZoneIsRefusedInsteadOfSubmitted() {
        DataSource source = mysql();
        source.setServerTimeZone("CST");
        ServiceException ex = assertThrows(ServiceException.class,
            () -> SeaTunnelJobConfigGenerator.generate(goldenTask("FULL_CDC"), source, postgres(), new SeaTunnelProperties(), COLUMNS));
        assertTrue(ex.getMessage().contains("CST"), ex.getMessage());
    }

    /** Exactly the task the fingerprints above were captured with; any change here invalidates them. */
    private static SyncTask goldenTask(String mode) {
        SyncTask task = new SyncTask();
        task.setTaskId(42L);
        task.setConfigVersion(3);
        task.setSourceTable("source_db.customers");
        task.setTargetTable("customers");
        task.setSyncMode(mode.startsWith("INCREMENTAL") ? "INCREMENTAL" : mode);
        if (mode.equals("INCREMENTAL_TS")) {
            task.setIncrementalStartupMode("TIMESTAMP");
            task.setIncrementalStartupTimestamp(LocalDateTime.of(2026, 9, 1, 8, 30));
        }
        task.setSelectedColumns("id,display_name");
        task.setSyncKeyColumns("id");
        task.setSnapshotParallelism(1);
        task.setReadLimitRowsPerSecond(100);
        task.setReadLimitBytesPerSecond(1024L);
        task.setSourceConnectionLimit(2);
        return task;
    }

    private static DataSource targetOfType(String type) {
        return switch (type) {
            case "POSTGRESQL" -> postgres();
            case "MYSQL" -> mysqlTarget();
            default -> kafka();
        };
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
