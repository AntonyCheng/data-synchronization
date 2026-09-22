package org.dromara.sync.engine;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.kafka.KafkaTaskBridgeService;
import org.dromara.sync.support.JdbcUrls;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates the HOCON job document for a MySQL -> PostgreSQL/MySQL/Kafka task.
 *
 * <p>The generated text is fingerprinted (SHA-256) and persisted as {@code engine_config_hash};
 * a task can only resume from its checkpoint while the regenerated config still hashes the
 * same. Any change to the emitted bytes therefore invalidates every running task's resume
 * path - keep output byte-stable unless that is intended.
 */
public final class SeaTunnelJobConfigGenerator {

    private static final String SOURCE_TIME_ZONE = "Asia/Shanghai";

    /**
     * Each CDC config claims a {@code server-id} range of {@link #SERVER_ID_RANGE_WIDTH}
     * consecutive values (see the {@code server-id = "X-(X+3)"} field). Bucketing by
     * multiplying the modulo result by the range width - instead of adding it directly -
     * guarantees two different buckets never produce overlapping ranges; the previous
     * "5400 + taskId % 100000" scheme let adjacent task IDs (the common case for
     * Snowflake IDs assigned to task-group items created in the same batch) claim
     * overlapping ranges, which MySQL's replication protocol rejects as duplicate
     * server IDs when both jobs connect concurrently.
     */
    private static final int SERVER_ID_RANGE_WIDTH = 4;
    private static final long SERVER_ID_BUCKET_COUNT = 900_000L;
    private static final int SERVER_ID_BASE = 10_000;

    private SeaTunnelJobConfigGenerator() {
    }

    public static GeneratedConfig generate(SyncTask task, DataSource source, DataSource target,
                                           SeaTunnelProperties properties, SourceColumns sourceColumns) {
        if (!DataSourceType.isMysql(source)) throw new ServiceException("源数据源必须是 MYSQL");
        if (!DataSourceType.isSupportedTarget(target.getSourceType())) {
            throw new ServiceException("目标数据源必须是 PostgreSQL、MySQL 或 Kafka");
        }
        String syncMode = SyncMode.normalize(task.getSyncMode());
        if (StringUtils.isBlank(task.getSourceTable()) || StringUtils.isBlank(task.getTargetTable())) {
            throw new ServiceException("源表和目标表不能为空");
        }

        List<String> primaryKeys = resolveSyncKeys(source, task);
        List<String> selectedColumns = resolveSelectedColumns(sourceColumns, task);
        boolean full = SyncMode.FULL.equals(syncMode);
        if (primaryKeys.isEmpty() && !full) {
            throw new ServiceException("源表没有主键，无法生成可恢复的 CDC 任务");
        }

        String sourceTable = TableNames.qualified(source.getDatabaseName(), task.getSourceTable());
        String configuredTargetTable = overwriteTargetTable(task, syncMode);
        boolean kafkaTarget = DataSourceType.isKafka(target);
        String targetTable = kafkaTarget || DataSourceType.isMysql(target)
            ? configuredTargetTable
            : TableNames.qualified(defaultValue(task.getTargetSchema(), TableNames.DEFAULT_POSTGRES_SCHEMA), configuredTargetTable);
        String jobName = "ds-task-" + task.getTaskId();
        String config;
        if (kafkaTarget) {
            config = buildKafkaConfig(task, source, target, sourceTable, primaryKeys, selectedColumns, syncMode, properties);
        } else if (full) {
            config = buildFullConfig(task, source, target, sourceTable, targetTable, primaryKeys, selectedColumns, properties);
        } else {
            config = buildCdcConfig(task, source, target, sourceTable, targetTable, primaryKeys, selectedColumns, syncMode, properties, sourceColumns);
        }
        return new GeneratedConfig(jobName, sourceTable, targetTable, primaryKeys, selectedColumns, config, redact(config));
    }

    /**
     * The one write the generator family does against a customer database, kept out of
     * {@link #generate} so that previews and fingerprint checks stay read-only: the
     * start / reinitialize paths call it right before submitting a fresh FULL job.
     */
    public static void prepareTarget(SyncTask task, DataSource source, DataSource target,
                                     GeneratedConfig generated, SourceColumns sourceColumns) {
        if (!SyncMode.isFull(SyncMode.normalize(task.getSyncMode())) || DataSourceType.isKafka(target)) return;
        ensureMysqlFullModeTargetTable(task, source, target, generated.targetTable(), generated.selectedColumns(), sourceColumns);
    }

    /**
     * Use the same versioned staging table during preview and submission.
     * The start path persists this value before submitting the job; preview
     * must still expose the eventual target without mutating the draft.
     */
    private static String overwriteTargetTable(SyncTask task, String syncMode) {
        if (!SyncMode.FULL.equals(syncMode) || !"OVERWRITE".equalsIgnoreCase(defaultValue(task.getFullDataMode(), "UPSERT"))) {
            return task.getTargetTable();
        }
        if (StringUtils.isNotBlank(task.getOverwriteStageTable())) {
            return task.getOverwriteStageTable();
        }
        long taskId = task.getTaskId() == null ? 0L : task.getTaskId();
        int version = task.getConfigVersion() == null ? 1 : task.getConfigVersion();
        return "__ds_stage_" + taskId + "_v" + version;
    }

    // ------------------------------------------------------------------ document builders

    private static String buildCdcConfig(SyncTask task, DataSource source, DataSource target,
                                         String sourceTable, String targetTable, List<String> primaryKeys,
                                         List<String> selectedColumns, String syncMode, SeaTunnelProperties properties,
                                         SourceColumns sourceColumns) {
        int sourceConnectionLimit = positive(task.getSourceConnectionLimit(), properties.getSourceConnectionLimit());
        String sourceOutput = "ds_source_" + task.getTaskId();
        String projectedOutput = "ds_projected_" + task.getTaskId();
        // MySQL-CDC always emits every source column - it has no column-level projection
        // of its own - so a transform is only needed when the selection actually narrows
        // the columns. Skipping it otherwise matters: SeaTunnel's generic Sql transform
        // does not propagate NOT NULL/primary-key metadata, so a target auto-created
        // through it fails ("All parts of a PRIMARY KEY must be NOT NULL") for any table
        // whose sync key is NOT NULL - i.e. almost every well-formed table.
        boolean needsProjection = !isFullColumnSelection(sourceColumns, task.getSourceTable(), selectedColumns);
        String sinkInput = needsProjection ? projectedOutput : sourceOutput;
        StringBuilder builder = new StringBuilder(1800);
        appendStreamingEnv(builder, task, properties);
        appendMysqlCdcSourceHead(builder, task, source, sourceTable, properties);
        builder.append("    connection.pool.size = ").append(sourceConnectionLimit).append("\n")
            .append(startupOptions(task, syncMode))
            .append("    exactly_once = false\n")
            .append("    schema-changes.enabled = false\n")
            .append("    plugin_output = ").append(quote(sourceOutput)).append("\n")
            .append("  }\n}\n\nsink {\n  Jdbc {\n");
        appendJdbcSinkHead(builder, target, targetTable, primaryKeys, properties);
        builder.append("    data_save_mode = \"APPEND_DATA\"\n")
            .append("    enable_upsert = true\n")
            .append("    batch_size = 100\n")
            .append("    max_retries = 5\n")
            .append("    plugin_input = ").append(stringList(List.of(sinkInput))).append("\n")
            .append("  }\n}\n");
        return needsProjection ? insertProjection(builder.toString(), sourceOutput, projectedOutput, selectedColumns) : builder.toString();
    }

    private static String buildKafkaConfig(SyncTask task, DataSource source, DataSource target,
                                           String sourceTable, List<String> primaryKeys, List<String> selectedColumns,
                                           String syncMode, SeaTunnelProperties properties) {
        if (primaryKeys.isEmpty()) throw new ServiceException("Kafka 任务必须配置可靠同步键");
        if (SyncMode.FULL.equals(syncMode)) return buildKafkaFullConfig(task, source, target, sourceTable, selectedColumns, properties);
        StringBuilder builder = new StringBuilder(1600);
        appendStreamingEnv(builder, task, properties);
        appendMysqlCdcSourceHead(builder, task, source, sourceTable, properties);
        builder.append(startupOptions(task, syncMode))
            .append("    exactly_once = false\n")
            .append("    schema-changes.enabled = false\n")
            .append("  }\n}\n\nsink {\n  Kafka {\n")
            .append("    topic = ").append(quote(KafkaTaskBridgeService.rawTopic(task))).append('\n')
            .append("    bootstrap.servers = ").append(quote(properties.resolveEngineEndpoint(target.getHost(), target.getPort()))).append('\n')
            .append("    format = \"DEBEZIUM_JSON\"\n")
            .append("    partition_key_fields = ").append(stringList(primaryKeys)).append('\n')
            .append("    semantics = \"AT_LEAST_ONCE\"\n")
            .append("    kafka.config = { acks = \"all\", enable.idempotence = \"true\" }\n")
            .append("  }\n}\n");
        return builder.toString();
    }

    private static String buildKafkaFullConfig(SyncTask task, DataSource source, DataSource target,
                                               String sourceTable, List<String> selected, SeaTunnelProperties properties) {
        StringBuilder builder = new StringBuilder(1200);
        appendBatchEnv(builder, task, properties);
        appendJdbcFullSource(builder, source, sourceTable, selected, properties);
        builder.append("  }\n}\n\nsink {\n  Kafka {\n")
            .append("    topic = ").append(quote(KafkaTaskBridgeService.rawTopic(task))).append('\n')
            .append("    bootstrap.servers = ").append(quote(properties.resolveEngineEndpoint(target.getHost(), target.getPort()))).append('\n')
            .append("    format = \"JSON\"\n")
            .append("    semantics = \"AT_LEAST_ONCE\"\n")
            .append("    kafka.config = { acks = \"all\", enable.idempotence = \"true\" }\n")
            .append("  }\n}\n");
        return builder.toString();
    }

    private static String buildFullConfig(SyncTask task, DataSource source, DataSource target,
                                          String sourceTable, String targetTable, List<String> primaryKeys,
                                          List<String> selectedColumns, SeaTunnelProperties properties) {
        String targetMode = "OVERWRITE".equalsIgnoreCase(defaultValue(task.getFullDataMode(), "UPSERT"))
            ? "DROP_DATA" : "APPEND_DATA";
        StringBuilder builder = new StringBuilder(2000);
        appendBatchEnv(builder, task, properties);
        appendJdbcFullSource(builder, source, sourceTable, selectedColumns, properties);
        builder.append("  }\n}\n\nsink {\n  Jdbc {\n");
        appendJdbcSinkHead(builder, target, targetTable, primaryKeys, properties);
        builder.append("    data_save_mode = \"").append(targetMode).append("\"\n")
            .append("    enable_upsert = ").append(primaryKeys.isEmpty() ? "false" : "true").append('\n')
            .append("    batch_size = 100\n")
            .append("    max_retries = 5\n")
            .append("  }\n}\n");
        return builder.toString();
    }

    // ------------------------------------------------------------------ shared blocks

    /** {@code env {}} for STREAMING (CDC) jobs: parallelism, checkpointing and source read limits. */
    private static void appendStreamingEnv(StringBuilder builder, SyncTask task, SeaTunnelProperties properties) {
        builder.append("env {\n")
            .append("  job.mode = \"STREAMING\"\n")
            .append("  parallelism = ").append(positive(task.getSnapshotParallelism(), properties.getSnapshotParallelism())).append("\n")
            .append("  checkpoint.interval = ").append(Math.max(1000, properties.getCheckpointIntervalMs())).append("\n")
            .append("  checkpoint.timeout = 60000\n")
            .append("  read_limit.rows_per_second = ").append(positive(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond())).append("\n")
            .append("  read_limit.bytes_per_second = ").append(positive(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond())).append("\n")
            .append("}\n\nsource {\n  MySQL-CDC {\n");
    }

    /** {@code env {}} for BATCH (FULL snapshot) jobs; no checkpointing. */
    private static void appendBatchEnv(StringBuilder builder, SyncTask task, SeaTunnelProperties properties) {
        builder.append("env {\n")
            .append("  job.mode = \"BATCH\"\n")
            .append("  parallelism = ").append(positive(task.getSnapshotParallelism(), properties.getSnapshotParallelism())).append("\n")
            .append("  read_limit.rows_per_second = ").append(positive(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond())).append("\n")
            .append("  read_limit.bytes_per_second = ").append(positive(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond())).append("\n")
            .append("}\n\nsource {\n  Jdbc {\n");
    }

    /** Connection, table selection and replication identity of the MySQL-CDC source. */
    private static void appendMysqlCdcSourceHead(StringBuilder builder, SyncTask task, DataSource source, String sourceTable,
                                                 SeaTunnelProperties properties) {
        int serverId = stableServerId(task.getTaskId());
        builder.append("    url = ").append(quote(engineMysqlJdbcUrl(source, properties))).append('\n')
            .append("    username = ").append(quote(source.getUsername())).append('\n')
            .append("    password = ").append(quote(source.getPassword())).append('\n')
            .append("    database-names = [").append(quote(source.getDatabaseName())).append("]\n")
            .append("    table-names = [").append(quote(sourceTable)).append("]\n")
            .append("    server-id = \"").append(serverId).append('-').append(serverId + SERVER_ID_RANGE_WIDTH - 1).append("\"\n")
            .append("    server-time-zone = \"").append(SOURCE_TIME_ZONE).append("\"\n");
    }

    /** FULL-mode {@code Jdbc} source: a plain projected SELECT against the source table. */
    private static void appendJdbcFullSource(StringBuilder builder, DataSource source, String sourceTable,
                                             List<String> selectedColumns, SeaTunnelProperties properties) {
        builder.append("    url = ").append(quote(fullJdbcSourceUrl(source, properties))).append('\n')
            .append("    driver = \"com.mysql.cj.jdbc.Driver\"\n")
            .append("    user = ").append(quote(source.getUsername())).append('\n')
            .append("    password = ").append(quote(source.getPassword())).append('\n')
            .append("    query = ").append(quote("SELECT " + selectColumns(selectedColumns) + " FROM " + quoteQualifiedIdentifier(sourceTable))).append('\n')
            .append("    result_table_name = \"source_table\"\n");
    }

    /** Relational {@code Jdbc} sink: connection, target table, sync key and auto-DDL policy. */
    private static void appendJdbcSinkHead(StringBuilder builder, DataSource target, String targetTable,
                                           List<String> primaryKeys, SeaTunnelProperties properties) {
        builder.append("    url = ").append(quote(engineTargetJdbcUrl(target, properties))).append('\n')
            .append("    driver = \"").append(targetDriver(target)).append("\"\n")
            .append("    username = ").append(quote(target.getUsername())).append('\n')
            .append("    password = ").append(quote(target.getPassword())).append('\n')
            .append("    database = ").append(quote(target.getDatabaseName())).append('\n')
            .append("    table = ").append(quote(targetTable)).append('\n')
            .append("    primary_keys = ").append(stringList(primaryKeys)).append('\n')
            .append("    generate_sink_sql = true\n")
            .append("    schema_save_mode = \"CREATE_SCHEMA_WHEN_NOT_EXIST\"\n");
    }

    private static String startupOptions(SyncTask task, String syncMode) {
        if (!SyncMode.INCREMENTAL.equals(syncMode)) return "    startup.mode = \"initial\"\n";
        String mode = defaultValue(task.getIncrementalStartupMode(), "LATEST").toUpperCase(Locale.ROOT);
        return switch (mode) {
            case "TIMESTAMP" -> {
                if (task.getIncrementalStartupTimestamp() == null) {
                    throw new ServiceException("按时间启动纯增量任务必须填写启动时间");
                }
                long timestamp = task.getIncrementalStartupTimestamp().atZone(ZoneId.of(SOURCE_TIME_ZONE)).toInstant().toEpochMilli();
                yield "    startup.mode = \"timestamp\"\n    startup.timestamp = " + timestamp + "\n";
            }
            case "SPECIFIC" -> {
                if (StringUtils.isBlank(task.getIncrementalStartupBinlogFile()) || task.getIncrementalStartupBinlogPosition() == null) {
                    throw new ServiceException("指定 binlog 位点启动纯增量任务必须填写文件名和位置");
                }
                yield "    startup.mode = \"specific\"\n"
                    + "    startup.specific-offset.file = " + quote(task.getIncrementalStartupBinlogFile()) + "\n"
                    + "    startup.specific-offset.pos = " + task.getIncrementalStartupBinlogPosition() + "\n";
            }
            case "LATEST" -> "    startup.mode = \"latest\"\n";
            default -> throw new ServiceException("不支持的纯增量启动位点策略：" + mode);
        };
    }

    private static String insertProjection(String config, String sourceOutput, String projectedOutput, List<String> columns) {
        String transform = "transform {\n  Sql {\n"
            + "    plugin_input = " + stringList(List.of(sourceOutput)) + "\n"
            + "    plugin_output = " + quote(projectedOutput) + "\n"
            + "    query = " + quote("SELECT " + selectColumns(columns) + " FROM " + sourceOutput) + "\n"
            + "  }\n}\n\n";
        return config.replace("sink {", transform + "sink {");
    }

    // ------------------------------------------------------------------ source introspection (platform-side JDBC)

    /**
     * FULL mode's Jdbc source hands SeaTunnel a raw SELECT query rather than a real
     * table reference. SeaTunnel then infers each column's target DDL type from that
     * query's ResultSetMetaData rather than the source table's own DDL, and for MySQL
     * (unlike PostgreSQL, which has no such restriction) this can misclassify a bounded
     * VARCHAR as an unbounded TEXT - which the server then rejects the moment it's used
     * in a PRIMARY KEY/UNIQUE ("BLOB/TEXT column ... used in key specification without a
     * key length"). Confirmed via a MySQL->MySQL multi-table FULL run where the sync key
     * was a VARCHAR unique key (customers/orders, both integer-keyed, created fine;
     * inventory_by_sku, keyed on two VARCHAR columns, failed exactly this way).
     * <p>
     * Only MySQL->MySQL is fixed here by cloning the source's own verified-correct DDL
     * (stripping any FOREIGN KEY constraints, which would reference tables that don't
     * exist on the target) instead of trusting SeaTunnel's inference. Skipped for a
     * partial column selection (a straight DDL clone would create unselected columns
     * SeaTunnel never writes, which may carry NOT NULL/no-default constraints the insert
     * would then violate) and for MySQL->PostgreSQL (a different dialect needs real type
     * mapping, not a DDL clone, and Postgres does not hit this specific failure anyway).
     */
    private static void ensureMysqlFullModeTargetTable(SyncTask task, DataSource source, DataSource target,
                                                        String targetTable, List<String> selectedColumns, SourceColumns sourceColumns) {
        if (!DataSourceType.isMysql(target)) return;
        if (!isFullColumnSelection(sourceColumns, task.getSourceTable(), selectedColumns)) return;
        String targetTableName = TableNames.unqualified(targetTable);
        try (Connection targetConnection = JdbcUrls.open(target)) {
            try (ResultSet existing = targetConnection.getMetaData().getTables(target.getDatabaseName(), null, targetTableName, new String[]{"TABLE"})) {
                if (existing.next()) return;
            }
            String createTableSql;
            try (Connection sourceConnection = JdbcUrls.open(source);
                 Statement statement = sourceConnection.createStatement();
                 ResultSet showCreate = statement.executeQuery("SHOW CREATE TABLE `" + TableNames.unqualified(task.getSourceTable()) + "`")) {
                if (!showCreate.next()) return;
                createTableSql = showCreate.getString(2);
            }
            createTableSql = createTableSql.replaceFirst(
                "(?i)CREATE TABLE `[^`]+`", "CREATE TABLE `" + targetTableName + "`");
            createTableSql = createTableSql.replaceAll(
                ",\\s*CONSTRAINT `[^`]+` FOREIGN KEY[^,]*\\([^)]*\\)\\s*REFERENCES[^,]*\\([^)]*\\)[^,)]*", "");
            try (Statement statement = targetConnection.createStatement()) {
                statement.execute(createTableSql);
            }
        } catch (SQLException ex) {
            throw new ServiceException("预建目标表失败：" + SyncText.safeMessage(ex, "连接失败"));
        }
    }

    private static List<String> resolveSyncKeys(DataSource source, SyncTask task) {
        List<String> configured = SyncColumnSelectionValidator.parseColumns(task.getSyncKeyColumns());
        return configured.isEmpty() ? resolvePrimaryKeys(source, task.getSourceTable()) : configured;
    }

    private static List<String> resolveSelectedColumns(SourceColumns sourceColumns, SyncTask task) {
        List<String> configured = SyncColumnSelectionValidator.parseColumns(task.getSelectedColumns());
        if (!configured.isEmpty()) return configured;
        List<String> columns = sourceColumns.of(task.getSourceTable());
        if (columns == null || columns.isEmpty()) throw new ServiceException("源表没有可同步字段");
        return columns;
    }

    /**
     * True when the configured selection covers every column of the source table (in
     * which case a downstream projection transform would be redundant). The lookup is
     * allowed to throw but never to guess - see {@link SourceColumns}.
     */
    private static boolean isFullColumnSelection(SourceColumns sourceColumns, String tableReference, List<String> selectedColumns) {
        List<String> allColumns = sourceColumns.of(tableReference);
        if (allColumns == null || allColumns.isEmpty() || selectedColumns.size() != allColumns.size()) return false;
        Set<String> selectedNormalized = new HashSet<>();
        for (String column : selectedColumns) selectedNormalized.add(column.toLowerCase(Locale.ROOT));
        for (String column : allColumns) {
            if (!selectedNormalized.contains(column.toLowerCase(Locale.ROOT))) return false;
        }
        return true;
    }

    private static List<String> resolvePrimaryKeys(DataSource source, String tableReference) {
        String table = TableNames.unqualified(tableReference);
        try (Connection connection = JdbcUrls.open(source);
             ResultSet resultSet = connection.getMetaData().getPrimaryKeys(source.getDatabaseName(), null, table)) {
            Map<Short, String> ordered = new LinkedHashMap<>();
            while (resultSet.next()) {
                ordered.put(resultSet.getShort("KEY_SEQ"), resultSet.getString("COLUMN_NAME"));
            }
            List<String> primaryKeys = ordered.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getKey))
                .map(Map.Entry::getValue)
                .toList();
            return primaryKeys.isEmpty() ? resolveNotNullUniqueKey(connection, source, table) : primaryKeys;
        } catch (SQLException ex) {
            throw new ServiceException("读取源表主键失败：" + SyncText.safeMessage(ex, "连接失败"));
        }
    }

    private static List<String> resolveNotNullUniqueKey(Connection connection, DataSource source, String table) throws SQLException {
        Map<String, List<IndexedColumn>> indexes = new HashMap<>();
        try (ResultSet resultSet = connection.getMetaData().getIndexInfo(source.getDatabaseName(), null, table, true, false)) {
            while (resultSet.next()) {
                String indexName = resultSet.getString("INDEX_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                short position = resultSet.getShort("ORDINAL_POSITION");
                if (indexName != null && columnName != null && position > 0) {
                    indexes.computeIfAbsent(indexName, ignored -> new ArrayList<>())
                        .add(new IndexedColumn(position, columnName));
                }
            }
        }
        return indexes.entrySet().stream()
            .map(entry -> entry.getValue().stream().sorted(Comparator.comparingInt(IndexedColumn::position)).toList())
            .filter(columns -> columns.stream().allMatch(column -> isNotNull(connection, source, table, column.name())))
            .sorted(Comparator.comparing(columns -> columns.stream().map(IndexedColumn::name).toList().toString()))
            .map(columns -> columns.stream().map(IndexedColumn::name).toList())
            .findFirst()
            .orElseGet(List::of);
    }

    private static boolean isNotNull(Connection connection, DataSource source, String table, String column) {
        try (ResultSet resultSet = connection.getMetaData().getColumns(source.getDatabaseName(), null, table, column)) {
            return resultSet.next() && resultSet.getInt("NULLABLE") == ResultSetMetaData.columnNoNulls;
        } catch (SQLException ignored) {
            return false;
        }
    }

    // ------------------------------------------------------------------ engine-side JDBC URLs (part of the fingerprint)

    private static String engineTargetJdbcUrl(DataSource target, SeaTunnelProperties properties) {
        return DataSourceType.isMysql(target)
            ? engineMysqlJdbcUrl(target, properties) : enginePostgresJdbcUrl(target, properties);
    }

    private static String engineMysqlJdbcUrl(DataSource source, SeaTunnelProperties properties) {
        // useInformationSchema forces MySQL Connector/J to resolve column metadata (type,
        // precision) from information_schema instead of ResultSetMetaData off a prepared
        // statement. Without it, a plain-query Jdbc source (used by buildFullConfig - the
        // MySQL-CDC connector path is unaffected) can misreport a bounded VARCHAR's
        // precision, and SeaTunnel's auto-DDL then emits an unbounded TEXT/BLOB column -
        // which MySQL then rejects for a table using that column as its key.
        return "jdbc:mysql://" + properties.resolveEngineEndpoint(source.getHost(), source.getPort()) + '/' + source.getDatabaseName()
            + "?connectTimeout=5000&socketTimeout=5000&useSSL=" + ("1".equals(source.getSslEnabled()))
            + "&allowPublicKeyRetrieval=true&serverTimezone=Asia%2FShanghai&useInformationSchema=true";
    }

    /**
     * JDBC URL for the FULL-mode {@code Jdbc} source (a plain {@code SELECT}).
     *
     * <p>The generic {@link #engineMysqlJdbcUrl} carries {@code serverTimezone=Asia/Shanghai}
     * so Connector/J can resolve GoldenDB's ambiguous {@code CST} system zone. But for a
     * plain-SELECT source the connector reads {@code DATETIME} through the driver's
     * timezone machinery, so a zoneless wall-clock value gets reinterpreted as Shanghai
     * and converted to the engine JVM's zone (UTC in this stack) - source
     * {@code 2026-01-01 00:01:00} lands in Kafka as {@code 2025-12-31T16:01:00}. The
     * MySQL-CDC / Debezium path keeps the wall-clock value, so a FULL_CDC job's snapshot
     * rows and CDC rows would then disagree.
     *
     * <p>Pinning {@code serverTimezone=UTC} makes the driver read the stored value as UTC;
     * with the engine JVM also on UTC there is no net conversion and {@code DATETIME}
     * round-trips unchanged, matching the CDC path. {@code TIMESTAMP} columns are read in
     * the UTC session and so line up with Debezium's UTC instants too. This assumes the
     * SeaTunnel container runs on UTC (compose and the offline template both set
     * {@code TZ=UTC}); a non-UTC engine would reintroduce a DATETIME offset here.
     */
    private static String fullJdbcSourceUrl(DataSource source, SeaTunnelProperties properties) {
        return "jdbc:mysql://" + properties.resolveEngineEndpoint(source.getHost(), source.getPort()) + '/' + source.getDatabaseName()
            + "?connectTimeout=5000&socketTimeout=5000&useSSL=" + ("1".equals(source.getSslEnabled()))
            + "&allowPublicKeyRetrieval=true&serverTimezone=UTC&useInformationSchema=true";
    }

    private static String enginePostgresJdbcUrl(DataSource source, SeaTunnelProperties properties) {
        return "jdbc:postgresql://" + properties.resolveEngineEndpoint(source.getHost(), source.getPort()) + '/' + source.getDatabaseName()
            + "?connectTimeout=5&socketTimeout=5&ssl=" + ("1".equals(source.getSslEnabled()));
    }

    private static String targetDriver(DataSource target) {
        return DataSourceType.isMysql(target) ? "com.mysql.cj.jdbc.Driver" : "org.postgresql.Driver";
    }

    // ------------------------------------------------------------------ text helpers

    private static String selectColumns(List<String> columns) {
        if (columns == null || columns.isEmpty()) return "*";
        return columns.stream().map(SeaTunnelJobConfigGenerator::quoteIdentifier).collect(Collectors.joining(", "));
    }

    private static String quoteIdentifier(String identifier) {
        return '`' + identifier.replace("`", "``") + '`';
    }

    /** Backtick-quotes each segment of a possibly database-qualified table name. */
    private static String quoteQualifiedIdentifier(String qualifiedName) {
        int separator = qualifiedName == null ? -1 : qualifiedName.indexOf('.');
        return separator < 0 ? quoteIdentifier(qualifiedName)
            : quoteIdentifier(qualifiedName.substring(0, separator)) + '.' + quoteIdentifier(qualifiedName.substring(separator + 1));
    }

    private static int stableServerId(Long taskId) {
        long value = taskId == null ? 1L : Math.abs(taskId);
        long bucket = value % SERVER_ID_BUCKET_COUNT;
        return SERVER_ID_BASE + (int) (bucket * SERVER_ID_RANGE_WIDTH);
    }

    private static String defaultValue(String value, String fallback) {
        return StringUtils.isBlank(value) ? fallback : value;
    }

    private static int positive(Integer value, int fallback) {
        return value == null || value < 1 ? Math.max(1, fallback) : value;
    }

    private static long positive(Long value, long fallback) {
        return value == null || value < 1 ? Math.max(1L, fallback) : value;
    }

    private static String stringList(List<String> values) {
        List<String> quoted = new ArrayList<>();
        for (String value : values) quoted.add(quote(value));
        return '[' + String.join(", ", quoted) + ']';
    }

    private static String quote(String value) {
        String safe = value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
        return '"' + safe + '"';
    }

    private static String redact(String config) {
        return config.replaceAll("(?m)(password\\s*=\\s*)\\\"[^\\\"]*\\\"", "$1\"******\"");
    }

    private record IndexedColumn(short position, String name) {
    }

    /** {@code config} carries real credentials and is what gets submitted; {@code redactedConfig} is API-safe and is what gets fingerprinted. */
    public record GeneratedConfig(String jobName, String sourceTable, String targetTable,
                                  List<String> primaryKeys, List<String> selectedColumns, String config, String redactedConfig) {

        /**
         * The persisted {@code engine_config_hash}. It deliberately excludes credentials so a
         * password rotation on a data source never invalidates a checkpoint; everything else
         * (endpoints, columns, keys, limits) still does.
         */
        public String fingerprint() {
            return SyncText.sha256Hex(redactedConfig);
        }

        /** True when {@code stored} is this config's fingerprint, or the pre-rotation-safe hash of the raw config. */
        public boolean matchesFingerprint(String stored) {
            return stored != null && (stored.equals(fingerprint()) || stored.equals(SyncText.sha256Hex(config)));
        }
    }
}
