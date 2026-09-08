package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.config.SeaTunnelProperties;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Generates the MVP HOCON shape for MySQL -> PostgreSQL/MySQL/Kafka tasks. */
final class SeaTunnelJobConfigGenerator {

    private SeaTunnelJobConfigGenerator() {
    }

    static GeneratedConfig generate(SyncTask task, DataSource source, DataSource target,
                                    SeaTunnelProperties properties) {
        requireType(source, "MYSQL", "源");
        if (!Set.of("POSTGRESQL", "MYSQL", "KAFKA").contains(defaultValue(target.getSourceType(), "").toUpperCase())) {
            throw new ServiceException("目标数据源必须是 PostgreSQL、MySQL 或 Kafka");
        }
        String syncMode = defaultValue(task.getSyncMode(), "FULL_CDC").toUpperCase();
        if (!List.of("FULL", "INCREMENTAL", "FULL_CDC").contains(syncMode)) {
            throw new ServiceException("不支持的同步模式：" + syncMode);
        }
        if (StringUtils.isBlank(task.getSourceTable()) || StringUtils.isBlank(task.getTargetTable())) {
            throw new ServiceException("源表和目标表不能为空");
        }

        List<String> primaryKeys = resolveSyncKeys(source, task);
        List<String> selectedColumns = resolveSelectedColumns(source, task);
        if (primaryKeys.isEmpty() && !"FULL".equals(syncMode)) {
            throw new ServiceException("源表没有主键，无法生成可恢复的 CDC 任务");
        }

        String sourceTable = qualifiedSourceTable(source.getDatabaseName(), task.getSourceTable());
        String configuredTargetTable = overwriteTargetTable(task, syncMode);
        String targetTable = "KAFKA".equalsIgnoreCase(target.getSourceType()) || "MYSQL".equalsIgnoreCase(target.getSourceType())
            ? configuredTargetTable : qualifiedTargetTable(defaultValue(task.getTargetSchema(), "public"), configuredTargetTable);
        String jobName = "ds-task-" + task.getTaskId();
        int serverId = stableServerId(task.getTaskId());
        String config;
        if ("KAFKA".equalsIgnoreCase(target.getSourceType())) {
            config = buildKafkaConfig(task, source, target, sourceTable, primaryKeys, properties);
        } else {
            if ("FULL".equals(syncMode)) {
                ensureMysqlFullModeTargetTable(task, source, target, targetTable, selectedColumns);
            }
            config = "FULL".equals(syncMode)
                ? buildFullConfig(task, source, target, sourceTable, targetTable, primaryKeys, selectedColumns, properties)
                : buildCdcConfig(task, source, target, sourceTable, targetTable, primaryKeys, selectedColumns, serverId, syncMode, properties);
        }
        return new GeneratedConfig(jobName, sourceTable, targetTable, primaryKeys, config, redact(config));
    }

    /**
     * Use the same versioned staging table during preview and submission.
     * The start path persists this value before submitting the job; preview
     * must still expose the eventual target without mutating the draft.
     */
    private static String overwriteTargetTable(SyncTask task, String syncMode) {
        if (!"FULL".equals(syncMode) || !"OVERWRITE".equalsIgnoreCase(defaultValue(task.getFullDataMode(), "UPSERT"))) {
            return task.getTargetTable();
        }
        if (StringUtils.isNotBlank(task.getOverwriteStageTable())) {
            return task.getOverwriteStageTable();
        }
        long taskId = task.getTaskId() == null ? 0L : task.getTaskId();
        int version = task.getConfigVersion() == null ? 1 : task.getConfigVersion();
        return "__ds_stage_" + taskId + "_v" + version;
    }

    private static String buildCdcConfig(SyncTask task, DataSource source, DataSource target,
                                      String sourceTable, String targetTable, List<String> primaryKeys,
                                      List<String> selectedColumns,
                                      int serverId, String syncMode, SeaTunnelProperties properties) {
        String sourceDatabase = quote(source.getDatabaseName());
        String sourceJdbc = quote(engineMysqlJdbcUrl(source, properties));
        String targetJdbc = quote(engineTargetJdbcUrl(target, properties));
        int snapshotParallelism = positive(task.getSnapshotParallelism(), properties.getSnapshotParallelism());
        int rowsPerSecond = positive(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond());
        long bytesPerSecond = positive(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond());
        int sourceConnectionLimit = positive(task.getSourceConnectionLimit(), properties.getSourceConnectionLimit());
        String sourceOutput = "ds_source_" + task.getTaskId();
        String projectedOutput = "ds_projected_" + task.getTaskId();
        // MySQL-CDC always emits every source column - it has no column-level projection
        // of its own - so a transform is only needed when the selection actually narrows
        // the columns. Skipping it otherwise matters: SeaTunnel's generic Sql transform
        // does not propagate NOT NULL/primary-key metadata, so a target auto-created
        // through it fails ("All parts of a PRIMARY KEY must be NOT NULL") for any table
        // whose sync key is NOT NULL - i.e. almost every well-formed table.
        boolean needsProjection = !isFullColumnSelection(source, task.getSourceTable(), selectedColumns);
        String sinkInput = needsProjection ? projectedOutput : sourceOutput;
        StringBuilder builder = new StringBuilder(1800);
        builder.append("env {\n")
            .append("  job.mode = \"STREAMING\"\n")
            .append("  parallelism = ").append(snapshotParallelism).append("\n")
            .append("  checkpoint.interval = ").append(Math.max(1000, properties.getCheckpointIntervalMs())).append("\n")
            .append("  checkpoint.timeout = 60000\n")
            .append("  read_limit.rows_per_second = ").append(rowsPerSecond).append("\n")
            .append("  read_limit.bytes_per_second = ").append(bytesPerSecond).append("\n")
            .append("}\n\nsource {\n  MySQL-CDC {\n")
            .append("    url = ").append(sourceJdbc).append('\n')
            .append("    username = ").append(quote(source.getUsername())).append('\n')
            .append("    password = ").append(quote(source.getPassword())).append('\n')
            .append("    database-names = [").append(sourceDatabase).append("]\n")
            .append("    table-names = [").append(quote(sourceTable)).append("]\n")
            .append("    server-id = \"").append(serverId).append('-').append(serverId + SERVER_ID_RANGE_WIDTH - 1).append("\"\n")
            .append("    server-time-zone = \"Asia/Shanghai\"\n")
            .append("    connection.pool.size = ").append(sourceConnectionLimit).append("\n")
            .append(startupOptions(task, syncMode))
            .append("    exactly_once = false\n")
            .append("    schema-changes.enabled = false\n")
            .append("    plugin_output = ").append(quote(sourceOutput)).append("\n")
            .append("  }\n}\n\nsink {\n  Jdbc {\n")
            .append("    url = ").append(targetJdbc).append('\n')
            .append("    driver = \"").append(targetDriver(target)).append("\"\n")
            .append("    username = ").append(quote(target.getUsername())).append('\n')
            .append("    password = ").append(quote(target.getPassword())).append('\n')
            .append("    database = ").append(quote(target.getDatabaseName())).append('\n')
            .append("    table = ").append(quote(targetTable)).append('\n')
            .append("    primary_keys = ").append(stringList(primaryKeys)).append('\n')
            .append("    generate_sink_sql = true\n")
            .append("    schema_save_mode = \"CREATE_SCHEMA_WHEN_NOT_EXIST\"\n")
            .append("    data_save_mode = \"APPEND_DATA\"\n")
            .append("    enable_upsert = true\n")
            .append("    batch_size = 100\n")
            .append("    max_retries = 5\n")
            .append("    plugin_input = ").append(stringList(List.of(sinkInput))).append("\n")
            .append("  }\n}\n");
        return needsProjection ? insertProjection(builder.toString(), sourceOutput, projectedOutput, selectedColumns) : builder.toString();
    }

    private static String buildKafkaConfig(SyncTask task, DataSource source, DataSource target,
                                           String sourceTable, List<String> primaryKeys,
                                           SeaTunnelProperties properties) {
        String mode = defaultValue(task.getSyncMode(), "FULL_CDC").toUpperCase();
        if (primaryKeys.isEmpty()) throw new ServiceException("Kafka 任务必须配置可靠同步键");
        if ("FULL".equals(mode)) return buildKafkaFullConfig(task, source, target, sourceTable, primaryKeys, properties);
        if (!List.of("FULL_CDC", "INCREMENTAL").contains(mode)) throw new ServiceException("不支持的 Kafka 同步模式：" + mode);
        String rawTopic = KafkaTaskBridgeService.rawTopic(task);
        int parallelism = positive(task.getSnapshotParallelism(), properties.getSnapshotParallelism());
        int rowsPerSecond = positive(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond());
        long bytesPerSecond = positive(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond());
        int kafkaServerId = stableServerId(task.getTaskId());
        StringBuilder builder = new StringBuilder(1600);
        builder.append("env {\n")
            .append("  job.mode = \"STREAMING\"\n")
            .append("  parallelism = ").append(parallelism).append("\n")
            .append("  checkpoint.interval = ").append(Math.max(1000, properties.getCheckpointIntervalMs())).append("\n")
            .append("  checkpoint.timeout = 60000\n")
            .append("  read_limit.rows_per_second = ").append(rowsPerSecond).append("\n")
            .append("  read_limit.bytes_per_second = ").append(bytesPerSecond).append("\n")
            .append("}\n\nsource {\n  MySQL-CDC {\n")
            .append("    url = ").append(quote(engineMysqlJdbcUrl(source, properties))).append('\n')
            .append("    username = ").append(quote(source.getUsername())).append('\n')
            .append("    password = ").append(quote(source.getPassword())).append('\n')
            .append("    database-names = [").append(quote(source.getDatabaseName())).append("]\n")
            .append("    table-names = [").append(quote(sourceTable)).append("]\n")
            .append("    server-id = \"").append(kafkaServerId).append('-').append(kafkaServerId + SERVER_ID_RANGE_WIDTH - 1).append("\"\n")
            .append("    server-time-zone = \"Asia/Shanghai\"\n")
            .append(startupOptions(task, defaultValue(task.getSyncMode(), "FULL_CDC").toUpperCase()))
            .append("    exactly_once = false\n")
            .append("    schema-changes.enabled = false\n")
            .append("  }\n}\n\nsink {\n  Kafka {\n")
            .append("    topic = ").append(quote(rawTopic)).append('\n')
            .append("    bootstrap.servers = ").append(quote(properties.resolveEngineEndpoint(target.getHost(), target.getPort()))).append('\n')
            .append("    format = \"DEBEZIUM_JSON\"\n")
            .append("    partition_key_fields = ").append(stringList(primaryKeys)).append('\n')
            .append("    semantics = \"AT_LEAST_ONCE\"\n")
            .append("    kafka.config = { acks = \"all\", enable.idempotence = \"true\" }\n")
            .append("  }\n}\n");
        return builder.toString();
    }

    private static String buildKafkaFullConfig(SyncTask task, DataSource source, DataSource target,
                                               String sourceTable, List<String> primaryKeys,
                                               SeaTunnelProperties properties) {
        String rawTopic = KafkaTaskBridgeService.rawTopic(task);
        int parallelism = positive(task.getSnapshotParallelism(), properties.getSnapshotParallelism());
        int rowsPerSecond = positive(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond());
        long bytesPerSecond = positive(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond());
        List<String> selected = resolveSelectedColumns(source, task);
        return "env {\n"
            + "  job.mode = \"BATCH\"\n"
            + "  parallelism = " + parallelism + "\n"
            + "  read_limit.rows_per_second = " + rowsPerSecond + "\n"
            + "  read_limit.bytes_per_second = " + bytesPerSecond + "\n"
            + "}\n\nsource {\n  Jdbc {\n"
            + "    url = " + quote(fullJdbcSourceUrl(source, properties)) + "\n"
            + "    driver = \"com.mysql.cj.jdbc.Driver\"\n"
            + "    user = " + quote(source.getUsername()) + "\n"
            + "    password = " + quote(source.getPassword()) + "\n"
            + "    query = " + quote("SELECT " + selectColumns(selected) + " FROM " + quoteQualifiedIdentifier(sourceTable)) + "\n"
            + "    result_table_name = \"source_table\"\n"
            + "  }\n}\n\nsink {\n  Kafka {\n"
            + "    topic = " + quote(rawTopic) + "\n"
            + "    bootstrap.servers = " + quote(properties.resolveEngineEndpoint(target.getHost(), target.getPort())) + "\n"
            + "    format = \"JSON\"\n"
            + "    semantics = \"AT_LEAST_ONCE\"\n"
            + "    kafka.config = { acks = \"all\", enable.idempotence = \"true\" }\n"
            + "  }\n}\n";
    }

    private static String startupOptions(SyncTask task, String syncMode) {
        if (!"INCREMENTAL".equals(syncMode)) return "    startup.mode = \"initial\"\n";
        String mode = defaultValue(task.getIncrementalStartupMode(), "LATEST").toUpperCase();
        return switch (mode) {
            case "TIMESTAMP" -> {
                if (task.getIncrementalStartupTimestamp() == null) {
                    throw new ServiceException("按时间启动纯增量任务必须填写启动时间");
                }
                long timestamp = task.getIncrementalStartupTimestamp().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
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

    private static String buildFullConfig(SyncTask task, DataSource source, DataSource target,
                                           String sourceTable, String targetTable, List<String> primaryKeys,
                                           List<String> selectedColumns,
                                           SeaTunnelProperties properties) {
        String sourceJdbc = quote(fullJdbcSourceUrl(source, properties));
        String targetJdbc = quote(engineTargetJdbcUrl(target, properties));
        String targetMode = "OVERWRITE".equalsIgnoreCase(defaultValue(task.getFullDataMode(), "UPSERT"))
            ? "DROP_DATA" : "APPEND_DATA";
        int snapshotParallelism = positive(task.getSnapshotParallelism(), properties.getSnapshotParallelism());
        int rowsPerSecond = positive(task.getReadLimitRowsPerSecond(), properties.getReadLimitRowsPerSecond());
        long bytesPerSecond = positive(task.getReadLimitBytesPerSecond(), properties.getReadLimitBytesPerSecond());
        StringBuilder builder = new StringBuilder(2000);
        builder.append("env {\n")
            .append("  job.mode = \"BATCH\"\n")
            .append("  parallelism = ").append(snapshotParallelism).append("\n")
            .append("  read_limit.rows_per_second = ").append(rowsPerSecond).append("\n")
            .append("  read_limit.bytes_per_second = ").append(bytesPerSecond).append("\n")
            .append("}\n\nsource {\n  Jdbc {\n")
            .append("    url = ").append(sourceJdbc).append('\n')
            .append("    driver = \"com.mysql.cj.jdbc.Driver\"\n")
            .append("    user = ").append(quote(source.getUsername())).append('\n')
            .append("    password = ").append(quote(source.getPassword())).append('\n')
            .append("    query = ").append(quote("SELECT " + selectColumns(selectedColumns) + " FROM " + quoteQualifiedIdentifier(sourceTable))).append('\n')
            .append("    result_table_name = \"source_table\"\n")
            .append("  }\n}\n\nsink {\n  Jdbc {\n")
            .append("    url = ").append(targetJdbc).append('\n')
            .append("    driver = \"").append(targetDriver(target)).append("\"\n")
            .append("    username = ").append(quote(target.getUsername())).append('\n')
            .append("    password = ").append(quote(target.getPassword())).append('\n')
            .append("    database = ").append(quote(target.getDatabaseName())).append('\n')
            .append("    table = ").append(quote(targetTable)).append('\n')
            .append("    primary_keys = ").append(stringList(primaryKeys)).append('\n')
            .append("    generate_sink_sql = true\n")
            .append("    schema_save_mode = \"CREATE_SCHEMA_WHEN_NOT_EXIST\"\n")
            .append("    data_save_mode = \"").append(targetMode).append("\"\n")
            .append("    enable_upsert = ").append(primaryKeys.isEmpty() ? "false" : "true").append('\n')
            .append("    batch_size = 100\n")
            .append("    max_retries = 5\n")
            .append("  }\n}\n");
        return builder.toString();
    }

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
                                                         String targetTable, List<String> selectedColumns) {
        if (!"MYSQL".equalsIgnoreCase(target.getSourceType())) return;
        if (!isFullColumnSelection(source, task.getSourceTable(), selectedColumns)) return;
        String targetTableName = unqualifiedTable(targetTable);
        try (Connection targetConnection = DriverManager.getConnection(mysqlJdbcUrl(target), target.getUsername(), target.getPassword())) {
            try (ResultSet existing = targetConnection.getMetaData().getTables(target.getDatabaseName(), null, targetTableName, new String[]{"TABLE"})) {
                if (existing.next()) return;
            }
            String createTableSql;
            try (Connection sourceConnection = DriverManager.getConnection(mysqlJdbcUrl(source), source.getUsername(), source.getPassword());
                 java.sql.Statement statement = sourceConnection.createStatement();
                 ResultSet showCreate = statement.executeQuery("SHOW CREATE TABLE `" + unqualifiedTable(task.getSourceTable()) + "`")) {
                if (!showCreate.next()) return;
                createTableSql = showCreate.getString(2);
            }
            createTableSql = createTableSql.replaceFirst(
                "(?i)CREATE TABLE `[^`]+`", "CREATE TABLE `" + targetTableName + "`");
            createTableSql = createTableSql.replaceAll(
                ",\\s*CONSTRAINT `[^`]+` FOREIGN KEY[^,]*\\([^)]*\\)\\s*REFERENCES[^,]*\\([^)]*\\)[^,)]*", "");
            try (java.sql.Statement statement = targetConnection.createStatement()) {
                statement.execute(createTableSql);
            }
        } catch (SQLException ex) {
            throw new ServiceException("预建目标表失败：" + safeMessage(ex));
        }
    }

    private static List<String> resolveSyncKeys(DataSource source, SyncTask task) {
        List<String> configured = SyncColumnSelectionValidator.parseColumns(task.getSyncKeyColumns());
        return configured.isEmpty() ? resolvePrimaryKeys(source, task.getSourceTable()) : configured;
    }

    private static List<String> resolveSelectedColumns(DataSource source, SyncTask task) {
        List<String> configured = SyncColumnSelectionValidator.parseColumns(task.getSelectedColumns());
        if (!configured.isEmpty()) return configured;
        try {
            List<String> columns = queryAllColumns(source, task.getSourceTable());
            if (columns.isEmpty()) throw new ServiceException("源表没有可同步字段");
            return columns;
        } catch (SQLException ex) {
            throw new ServiceException("读取源表字段失败：" + safeMessage(ex));
        }
    }

    private static List<String> queryAllColumns(DataSource source, String tableReference) throws SQLException {
        String table = unqualifiedTable(tableReference);
        try (Connection connection = DriverManager.getConnection(mysqlJdbcUrl(source), source.getUsername(), source.getPassword());
             ResultSet resultSet = connection.getMetaData().getColumns(source.getDatabaseName(), null, table, "%")) {
            List<String> columns = new ArrayList<>();
            while (resultSet.next()) columns.add(resultSet.getString("COLUMN_NAME"));
            return columns;
        }
    }

    /**
     * True when the configured selection covers every column of the source table (in
     * which case a downstream projection transform would be redundant). Any lookup
     * failure conservatively returns false so the caller keeps the (slower but safe)
     * projection path rather than risk silently dropping a real column restriction.
     */
    private static boolean isFullColumnSelection(DataSource source, String tableReference, List<String> selectedColumns) {
        try {
            List<String> allColumns = queryAllColumns(source, tableReference);
            if (allColumns.isEmpty() || selectedColumns.size() != allColumns.size()) return false;
            Set<String> selectedNormalized = new java.util.HashSet<>();
            for (String column : selectedColumns) selectedNormalized.add(column.toLowerCase(Locale.ROOT));
            for (String column : allColumns) {
                if (!selectedNormalized.contains(column.toLowerCase(Locale.ROOT))) return false;
            }
            return true;
        } catch (SQLException ex) {
            return false;
        }
    }

    private static List<String> resolvePrimaryKeys(DataSource source, String tableReference) {
        String table = unqualifiedTable(tableReference);
        try (Connection connection = DriverManager.getConnection(mysqlJdbcUrl(source), source.getUsername(), source.getPassword());
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
            throw new ServiceException("读取源表主键失败：" + safeMessage(ex));
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

    private static String mysqlJdbcUrl(DataSource source) {
        return "jdbc:mysql://" + source.getHost() + ':' + source.getPort() + '/' + source.getDatabaseName()
            + "?connectTimeout=5000&socketTimeout=5000&useSSL=" + ("1".equals(source.getSslEnabled()))
            + "&allowPublicKeyRetrieval=true&serverTimezone=Asia%2FShanghai";
    }

    private static String postgresJdbcUrl(DataSource source) {
        return "jdbc:postgresql://" + source.getHost() + ':' + source.getPort() + '/' + source.getDatabaseName()
            + "?connectTimeout=5&socketTimeout=5&ssl=" + ("1".equals(source.getSslEnabled()));
    }

    private static String engineTargetJdbcUrl(DataSource target, SeaTunnelProperties properties) {
        return "MYSQL".equalsIgnoreCase(target.getSourceType())
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
        return "MYSQL".equalsIgnoreCase(target.getSourceType()) ? "com.mysql.cj.jdbc.Driver" : "org.postgresql.Driver";
    }

    private static String qualifiedSourceTable(String database, String table) {
        return table.indexOf('.') >= 0 ? table : database + '.' + table;
    }

    private static String unqualifiedTable(String tableReference) {
        int separator = tableReference == null ? -1 : tableReference.lastIndexOf('.');
        return separator < 0 ? tableReference : tableReference.substring(separator + 1);
    }

    private static String insertProjection(String config, String sourceOutput, String projectedOutput, List<String> columns) {
        String transform = "transform {\n  Sql {\n"
            + "    plugin_input = " + stringList(List.of(sourceOutput)) + "\n"
            + "    plugin_output = " + quote(projectedOutput) + "\n"
            + "    query = " + quote("SELECT " + selectColumns(columns) + " FROM " + sourceOutput) + "\n"
            + "  }\n}\n\n";
        return config.replace("sink {", transform + "sink {");
    }

    private static String selectColumns(List<String> columns) {
        if (columns == null || columns.isEmpty()) return "*";
        return columns.stream().map(SeaTunnelJobConfigGenerator::quoteIdentifier).collect(java.util.stream.Collectors.joining(", "));
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

    private static String qualifiedTargetTable(String schema, String table) {
        return table.indexOf('.') >= 0 ? table : schema + '.' + table;
    }

    /**
     * Each CDC config claims a {@code server-id} range of {@link #SERVER_ID_RANGE_WIDTH}
     * consecutive values (see the {@code server-id = "X-(X+3)"} field below). Bucketing by
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

    private static int stableServerId(Long taskId) {
        long value = taskId == null ? 1L : Math.abs(taskId);
        long bucket = value % SERVER_ID_BUCKET_COUNT;
        return SERVER_ID_BASE + (int) (bucket * SERVER_ID_RANGE_WIDTH);
    }

    private static void requireType(DataSource source, String expected, String side) {
        if (!expected.equalsIgnoreCase(source.getSourceType())) {
            throw new ServiceException(side + "数据源必须是 " + expected);
        }
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

    private static String safeMessage(SQLException ex) {
        String message = ex.getMessage();
        return StringUtils.isBlank(message) ? "连接失败" : message.replaceAll("(?i)(password\\s*[=:]\\s*)[^; ,]+", "$1******");
    }

    private record IndexedColumn(short position, String name) {
    }

    record GeneratedConfig(String jobName, String sourceTable, String targetTable,
                           List<String> primaryKeys, String config, String redactedConfig) {
    }
}
