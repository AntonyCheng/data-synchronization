package org.dromara.sync.service.impl;

import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.vo.DataSourceCdcPrecheckVo;
import org.dromara.sync.domain.vo.DataSourceCheckItemVo;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceIndexVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** JDBC-backed metadata and MySQL CDC prerequisite implementation. */
@RequiredArgsConstructor
@Service
public class DataSourceMetadataServiceImpl implements IDataSourceMetadataService {

    private static final Set<String> SUPPORTED_TYPES = Set.of("MYSQL", "POSTGRESQL");
    private static final Set<String> MYSQL_CDC_VARIABLES = Set.of(
        "log_bin", "binlog_format", "binlog_row_image", "gtid_mode", "binlog_expire_logs_seconds",
        "binlog_expire_logs_days", "time_zone", "system_time_zone"
    );

    private final DataSourceMapper dataSourceMapper;
    private final SyncTaskMapper syncTaskMapper;

    @Override
    public List<String> queryDatabases(Long sourceId) {
        DataSource source = requireSource(sourceId);
        try (Connection connection = openConnection(source, null)) {
            if ("MYSQL".equals(source.getSourceType())) {
                List<String> databases = new ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement(
                    "select schema_name from information_schema.schemata "
                        + "where schema_name not in ('information_schema','mysql','performance_schema','sys') "
                        + "order by schema_name")) {
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) databases.add(resultSet.getString(1));
                    }
                }
                return databases;
            }
            List<String> databases = new ArrayList<>();
            try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
                while (resultSet.next()) databases.add(resultSet.getString("TABLE_CAT"));
            }
            return databases;
        } catch (SQLException ex) {
            throw metadataFailure("读取数据库列表失败", ex);
        }
    }

    @Override
    public List<String> queryTables(Long sourceId, String databaseName) {
        DataSource source = requireSource(sourceId);
        String database = StringUtils.isBlank(databaseName) ? source.getDatabaseName() : databaseName.trim();
        try (Connection connection = openConnection(source, database)) {
            DatabaseMetaData metadata = connection.getMetaData();
            List<String> tables = new ArrayList<>();
            String catalog = "MYSQL".equals(source.getSourceType()) ? database : null;
            String schema = "POSTGRESQL".equals(source.getSourceType()) ? defaultSchema(source) : null;
            try (ResultSet resultSet = metadata.getTables(catalog, schema, "%", new String[]{"TABLE"})) {
                while (resultSet.next()) {
                    String table = resultSet.getString("TABLE_NAME");
                    if (StringUtils.isNotBlank(table)) tables.add(table);
                }
            }
            return tables;
        } catch (SQLException ex) {
            throw metadataFailure("读取数据表列表失败", ex);
        }
    }

    @Override
    public DataSourceMetadataVo queryTableMetadata(Long sourceId, String databaseName, String tableName) {
        return queryTableMetadata(sourceId, databaseName, null, tableName);
    }

    @Override
    public DataSourceMetadataVo queryTableMetadata(Long sourceId, String databaseName, String schemaName, String tableName) {
        if (StringUtils.isBlank(tableName)) throw new ServiceException("表名不能为空");
        DataSource source = requireSource(sourceId);
        String database = StringUtils.isBlank(databaseName) ? source.getDatabaseName() : databaseName.trim();
        String table = tableName.trim();
        try (Connection connection = openConnection(source, database)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String catalog = "MYSQL".equals(source.getSourceType()) ? database : null;
            String schema = "POSTGRESQL".equals(source.getSourceType())
                ? (StringUtils.isBlank(schemaName) ? defaultSchema(source) : schemaName.trim()) : null;
            DataSourceMetadataVo result = new DataSourceMetadataVo();
            result.setSourceId(sourceId);
            result.setSourceType(source.getSourceType());
            result.setDatabaseName(database);
            result.setTableName(table);

            Map<String, DataSourceColumnVo> columns = readColumns(metadata, catalog, schema, table, result);
            result.setPrimaryKeys(readPrimaryKeys(metadata, catalog, schema, table));
            result.setUniqueKeys(readUniqueKeys(metadata, catalog, schema, table, columns));
            readDatabaseEncoding(connection, source, database, result);
            return result;
        } catch (SQLException ex) {
            throw metadataFailure("读取表结构失败", ex);
        }
    }

    @Override
    public TargetCompatibilityVo checkTargetCompatibility(Long taskId) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) throw new ServiceException("同步任务不存在");
        return checkTargetCompatibility(task);
    }

    @Override
    public TargetCompatibilityVo checkTargetCompatibility(Long sourceId, Long targetId, String sourceTable,
                                                           String targetSchema, String targetTable) {
        return checkTargetCompatibility(sourceId, targetId, sourceTable, targetSchema, targetTable, null, null);
    }

    @Override
    public TargetCompatibilityVo checkTargetCompatibility(Long sourceId, Long targetId, String sourceTable,
                                                           String targetSchema, String targetTable,
                                                           String selectedColumns, String syncKeyColumns) {
        SyncTask task = new SyncTask();
        task.setTaskId(0L);
        task.setSourceId(sourceId);
        task.setTargetId(targetId);
        task.setSourceTable(sourceTable);
        task.setTargetSchema(targetSchema);
        task.setTargetTable(targetTable);
        task.setSelectedColumns(selectedColumns);
        task.setSyncKeyColumns(syncKeyColumns);
        return checkTargetCompatibility(task);
    }

    private TargetCompatibilityVo checkTargetCompatibility(SyncTask task) {
        DataSource source = requireSource(task.getSourceId());
        DataSource target = requireSource(task.getTargetId());
        if (!"MYSQL".equals(source.getSourceType()) || !"POSTGRESQL".equals(target.getSourceType())) {
            throw new ServiceException("MVP 目标兼容性检查仅支持 MySQL 到 PostgreSQL");
        }
        TargetCompatibilityVo result = new TargetCompatibilityVo();
        result.setTaskId(task.getTaskId());
        result.setSourceTable(task.getSourceTable());
        String targetTable = qualifiedTargetName(task.getTargetSchema(), task.getTargetTable());
        result.setTargetTable(targetTable);
        DataSourceMetadataVo sourceMetadata = queryTableMetadata(source.getSourceId(), source.getDatabaseName(), null, task.getSourceTable());
        SyncColumnSelectionValidator.Selection selection = SyncColumnSelectionValidator.validate(sourceMetadata,
            task.getSelectedColumns(), task.getSyncKeyColumns());
        List<String> syncKeys = selection.syncKeyColumns();
        String targetSchema = StringUtils.isBlank(task.getTargetSchema()) ? defaultSchema(target) : task.getTargetSchema().trim();
        boolean targetExists = tableExists(target, targetSchema, task.getTargetTable());
        result.setTargetExists(targetExists);
        if (!targetExists) {
            result.setPassed(true);
            result.setMessage("目标表不存在，将由 SeaTunnel 按源表结构自动创建");
            result.getChecks().add(new DataSourceCheckItemVo("target_table", "目标表", false, true, "不存在",
                "允许自动建表，创建阶段仍需确认类型转换和同步键"));
            result.getChecks().add(new DataSourceCheckItemVo("source_keys", "源同步键", true,
                !syncKeys.isEmpty(), String.join(", ", syncKeys),
                syncKeys.isEmpty() ? "源表没有可靠同步键，只能使用全量模式" : "同步键可用于 CDC"));
            setSuggestion(result, "source_keys", syncKeys.isEmpty()
                ? "请在源表增加主键或非空唯一键后重新执行检查" : "无需修改");
            result.setPassed("FULL".equalsIgnoreCase(task.getSyncMode()) || !syncKeys.isEmpty());
            if (!result.isPassed()) result.setMessage("源表没有可靠同步键，无法稳定执行 CDC");
            return result;
        }
        DataSourceMetadataVo targetMetadata = queryTableMetadata(target.getSourceId(), target.getDatabaseName(), targetSchema, task.getTargetTable());
        result.getChecks().add(new DataSourceCheckItemVo("target_table", "目标表", true, true, "已存在", "开始检查字段和同步键"));
        Map<String, DataSourceColumnVo> sourceColumns = byName(sourceMetadata.getColumns()).entrySet().stream()
            .filter(entry -> selection.selectedColumns().stream().anyMatch(column -> normalize(column).equals(entry.getKey())))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (left, right) -> left, LinkedHashMap::new));
        Map<String, DataSourceColumnVo> targetColumns = byName(targetMetadata.getColumns());
        List<String> missing = sourceColumns.keySet().stream().filter(name -> !targetColumns.containsKey(name)).toList();
        result.getChecks().add(new DataSourceCheckItemVo("missing_columns", "源字段缺失", true, missing.isEmpty(),
            missing.isEmpty() ? "无" : String.join(", ", missing), missing.isEmpty() ? "源字段均存在" : "目标表缺少源字段"));
        List<String> incompatible = sourceColumns.entrySet().stream().filter(entry -> targetColumns.containsKey(entry.getKey())
            && !compatible(entry.getValue(), targetColumns.get(entry.getKey()))).map(entry -> entry.getValue().getName()).toList();
        result.getChecks().add(new DataSourceCheckItemVo("incompatible_columns", "字段类型", true, incompatible.isEmpty(),
            incompatible.isEmpty() ? "兼容" : String.join(", ", incompatible), incompatible.isEmpty() ? "字段类型兼容" : "存在可能丢失精度或无法写入的类型"));
        boolean keysMatch = syncKeys.isEmpty() || sameColumns(syncKeys, targetMetadata.getPrimaryKeys());
        result.getChecks().add(new DataSourceCheckItemVo("primary_keys", "同步键", true, keysMatch,
            targetMetadata.getPrimaryKeys().isEmpty() ? "无" : String.join(", ", targetMetadata.getPrimaryKeys()),
            keysMatch ? "目标同步键一致" : "目标主键必须与已选择的源同步键一致"));
        List<String> requiredExtras = targetMetadata.getColumns().stream()
            .filter(column -> !sourceColumns.containsKey(normalize(column.getName())))
            .filter(column -> Boolean.FALSE.equals(column.getNullable()) && StringUtils.isBlank(column.getDefaultValue()))
            .map(DataSourceColumnVo::getName).toList();
        result.getChecks().add(new DataSourceCheckItemVo("extra_required_columns", "目标额外必填列", true, requiredExtras.isEmpty(),
            requiredExtras.isEmpty() ? "无" : String.join(", ", requiredExtras), requiredExtras.isEmpty() ? "额外列可安全写入" : "目标额外必填列无默认值"));
        setSuggestion(result, "missing_columns", missing.isEmpty() ? "无需修改" : "补齐目标表缺失列，或在确认无业务数据后删除目标表并重新自动建表");
        setSuggestion(result, "incompatible_columns", incompatible.isEmpty() ? "无需修改" : "将目标列调整为兼容类型；修改前请备份目标数据并确认转换不会丢失精度");
        setSuggestion(result, "primary_keys", keysMatch ? "无需修改" : "调整目标表主键，使其列顺序与已选择的源同步键一致");
        setSuggestion(result, "extra_required_columns", requiredExtras.isEmpty() ? "无需修改" : "将目标额外列改为可空或补充默认值");
        boolean passed = missing.isEmpty() && incompatible.isEmpty() && keysMatch && requiredExtras.isEmpty();
        result.setPassed(passed);
        result.setMessage(passed ? "目标表结构兼容，可以继续任务校验" : "目标表存在必须修复的兼容性问题");
        return result;
    }

    private boolean tableExists(DataSource source, String schema, String table) {
        try (Connection connection = openConnection(source, source.getDatabaseName());
             ResultSet resultSet = connection.getMetaData().getTables(null, schema, table, new String[]{"TABLE"})) {
            return resultSet.next();
        } catch (SQLException ex) {
            throw metadataFailure("检查目标表失败", ex);
        }
    }

    private Map<String, DataSourceColumnVo> byName(List<DataSourceColumnVo> columns) {
        return columns.stream().collect(Collectors.toMap(column -> normalize(column.getName()), column -> column, (left, right) -> left, LinkedHashMap::new));
    }

    private String normalize(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }

    private boolean sameColumns(List<String> left, List<String> right) {
        if (left == null || right == null || left.size() != right.size()) return false;
        for (int index = 0; index < left.size(); index++) {
            if (!normalize(left.get(index)).equals(normalize(right.get(index)))) return false;
        }
        return true;
    }

    private boolean compatible(DataSourceColumnVo source, DataSourceColumnVo target) {
        String sourceTypeName = source.getTypeName() == null ? "" : source.getTypeName().toLowerCase(Locale.ROOT);
        String targetTypeName = target.getTypeName() == null ? "" : target.getTypeName().toLowerCase(Locale.ROOT);
        // SeaTunnel JDBC binds MySQL JSON as a string. PostgreSQL json/jsonb columns
        // require an explicit cast, which the MVP generator does not emit; text
        // columns preserve the JSON payload without relying on an implicit cast.
        if ("json".equals(sourceTypeName)
            && Set.of("json", "jsonb").contains(targetTypeName)) return false;
        boolean sourceBoolean = (Integer.valueOf(java.sql.Types.BIT).equals(source.getJdbcType())
            && Integer.valueOf(1).equals(source.getSize()))
            || (sourceTypeName.contains("tinyint")
            && source.getSize() != null && source.getSize() <= 1);
        boolean targetBoolean = Integer.valueOf(java.sql.Types.BOOLEAN).equals(target.getJdbcType())
            || Set.of("bool", "boolean").contains(targetTypeName);
        if (sourceBoolean && targetBoolean) return true;
        String sourceType = typeFamily(source.getTypeName(), source.getJdbcType());
        String targetType = typeFamily(target.getTypeName(), target.getJdbcType());
        if (sourceType.equals(targetType)) return true;
        if ("numeric".equals(sourceType) && "boolean".equals(targetType)
            && sourceTypeName.contains("tinyint")) return true;
        return "numeric".equals(sourceType) && "numeric".equals(targetType)
            || "string".equals(sourceType) && "string".equals(targetType)
            || "temporal".equals(sourceType) && "temporal".equals(targetType);
    }

    private void setSuggestion(TargetCompatibilityVo result, String code, String suggestion) {
        result.getChecks().stream().filter(item -> code.equals(item.getCode())).findFirst()
            .ifPresent(item -> item.setSuggestion(suggestion));
    }

    private String typeFamily(String typeName, Integer jdbcType) {
        String type = typeName == null ? "" : typeName.toLowerCase(Locale.ROOT);
        if (type.matches(".*(tinyint|smallint|mediumint|int|integer|bigint|decimal|numeric|float|double|real|serial|money).*")) return "numeric";
        if (type.matches(".*(char|varchar|string|text|clob|json|enum|set).*")) return "string";
        if (type.matches(".*(date|time|year|timestamp).*")) return "temporal";
        if (type.matches(".*(blob|binary|bytea|bit).*")) return "binary";
        if (type.matches(".*(bool).*")) return "boolean";
        return String.valueOf(jdbcType);
    }

    private String qualifiedTargetName(String schema, String table) {
        return (StringUtils.isBlank(schema) ? "public" : schema.trim()) + "." + table;
    }

    @Override
    public DataSourceCdcPrecheckVo checkMysqlCdc(Long sourceId) {
        DataSource source = requireSource(sourceId);
        DataSourceCdcPrecheckVo result = new DataSourceCdcPrecheckVo();
        result.setSourceId(sourceId);
        result.setSourceType(source.getSourceType());
        if (!"MYSQL".equals(source.getSourceType())) {
            result.setPassed(false);
            result.setMessage("只有 MySQL 数据源支持 binlog CDC 前置检查");
            result.setChecks(List.of(new DataSourceCheckItemVo("source_type", "源端类型", true, false,
                source.getSourceType(), "源端必须是 MySQL")));
            return result;
        }

        try (Connection connection = openConnection(source, source.getDatabaseName())) {
            Map<String, String> variables = readMysqlVariables(connection);
            String serverId = querySingle(connection, "select @@server_id");
            String gtidMode = value(variables, "gtid_mode");
            result.setServerId(serverId);
            result.setGtidMode(gtidMode);
            result.setBinlogRetention(retention(variables));

            List<DataSourceCheckItemVo> checks = new ArrayList<>();
            addRequired(checks, "log_bin", "binlog 已开启", value(variables, "log_bin"), "ON", "必须开启 log_bin");
            addRequired(checks, "binlog_format", "binlog 格式", value(variables, "binlog_format"), "ROW", "binlog_format 必须为 ROW");
            addRequired(checks, "binlog_row_image", "binlog 行镜像", value(variables, "binlog_row_image"), "FULL", "binlog_row_image 必须为 FULL");
            addRequired(checks, "server_id", "MySQL server-id", serverId, null, "server-id 必须是大于 0 的数字");
            checks.get(checks.size() - 1).setPassed(isPositiveNumber(serverId));
            checks.get(checks.size() - 1).setMessage(isPositiveNumber(serverId) ? "server-id 可用" : "server-id 必须是大于 0 的数字");

            boolean replicationPrivilege = hasReplicationPrivilege(connection);
            checks.add(new DataSourceCheckItemVo("replication_privilege", "复制权限", true, replicationPrivilege,
                replicationPrivilege ? "已检测" : "未检测到", replicationPrivilege ? "具备复制相关权限" : "需要 REPLICATION CLIENT/SLAVE 权限"));
            String timezone = value(variables, "time_zone");
            checks.add(new DataSourceCheckItemVo("timezone", "源端时区", false, StringUtils.isNotBlank(timezone), timezone,
                StringUtils.isBlank(timezone) ? "无法读取源端时区" : "已读取源端时区"));
            result.setChecks(checks);
            boolean passed = checks.stream().filter(DataSourceCheckItemVo::getRequired).allMatch(DataSourceCheckItemVo::getPassed);
            result.setPassed(passed);
            result.setMessage(passed ? "MySQL binlog CDC 前置检查通过" : "存在必须修复的 MySQL binlog CDC 前置条件");
            return result;
        } catch (SQLException ex) {
            result.setPassed(false);
            result.setMessage("CDC 前置检查失败：" + safeMessage(ex));
            result.setChecks(List.of(new DataSourceCheckItemVo("connection", "连接检查", true, false, "连接失败", result.getMessage())));
            return result;
        }
    }

    private Map<String, DataSourceColumnVo> readColumns(DatabaseMetaData metadata, String catalog, String schema,
                                                         String table, DataSourceMetadataVo result) throws SQLException {
        Map<String, DataSourceColumnVo> columns = new LinkedHashMap<>();
        try (ResultSet resultSet = metadata.getColumns(catalog, schema, table, "%")) {
            while (resultSet.next()) {
                DataSourceColumnVo column = new DataSourceColumnVo();
                column.setName(resultSet.getString("COLUMN_NAME"));
                column.setTypeName(resultSet.getString("TYPE_NAME"));
                column.setJdbcType(resultSet.getInt("DATA_TYPE"));
                column.setSize(resultSet.getInt("COLUMN_SIZE"));
                column.setScale(resultSet.getInt("DECIMAL_DIGITS"));
                column.setNullable(resultSet.getInt("NULLABLE") != ResultSetMetaData.columnNoNulls);
                column.setAutoIncrement("YES".equalsIgnoreCase(resultSet.getString("IS_AUTOINCREMENT")));
                column.setDefaultValue(resultSet.getString("COLUMN_DEF"));
                columns.put(column.getName(), column);
                result.getColumns().add(column);
            }
        }
        return columns;
    }

    private List<String> readPrimaryKeys(DatabaseMetaData metadata, String catalog, String schema, String table) throws SQLException {
        Map<Short, String> keys = new TreeMap<>();
        try (ResultSet resultSet = metadata.getPrimaryKeys(catalog, schema, table)) {
            while (resultSet.next()) keys.put(resultSet.getShort("KEY_SEQ"), resultSet.getString("COLUMN_NAME"));
        }
        return new ArrayList<>(keys.values());
    }

    private List<DataSourceIndexVo> readUniqueKeys(DatabaseMetaData metadata, String catalog, String schema, String table,
                                                    Map<String, DataSourceColumnVo> columns) throws SQLException {
        Map<String, DataSourceIndexVo> indexes = new LinkedHashMap<>();
        Map<String, Map<Short, String>> indexColumns = new HashMap<>();
        try (ResultSet resultSet = metadata.getIndexInfo(catalog, schema, table, true, false)) {
            while (resultSet.next()) {
                String indexName = resultSet.getString("INDEX_NAME");
                String columnName = resultSet.getString("COLUMN_NAME");
                if (indexName == null || columnName == null) continue;
                DataSourceIndexVo index = indexes.computeIfAbsent(indexName, key -> {
                    DataSourceIndexVo value = new DataSourceIndexVo();
                    value.setName(key);
                    value.setUnique(true);
                    return value;
                });
                indexColumns.computeIfAbsent(indexName, key -> new TreeMap<>())
                    .put(resultSet.getShort("ORDINAL_POSITION"), columnName);
            }
        }
        indexes.forEach((name, index) -> {
            List<String> names = new ArrayList<>(indexColumns.get(name).values());
            index.setColumns(names);
            index.setAllNotNull(names.stream().allMatch(column -> !Boolean.TRUE.equals(columns.get(column).getNullable())));
        });
        return new ArrayList<>(indexes.values());
    }

    private void readDatabaseEncoding(Connection connection, DataSource source, String database, DataSourceMetadataVo result) throws SQLException {
        if ("MYSQL".equals(source.getSourceType())) {
            try (PreparedStatement statement = connection.prepareStatement(
                "select default_character_set_name, default_collation_name from information_schema.schemata where schema_name = ?")) {
                statement.setString(1, database);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        result.setCharset(resultSet.getString(1));
                        result.setCollation(resultSet.getString(2));
                    }
                }
            }
        }
    }

    private Map<String, String> readMysqlVariables(Connection connection) throws SQLException {
        Map<String, String> variables = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(
            "show variables where variable_name in ('log_bin','binlog_format','binlog_row_image','gtid_mode',"
                + "'binlog_expire_logs_seconds','binlog_expire_logs_days','time_zone','system_time_zone')")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String name = resultSet.getString(1);
                    if (MYSQL_CDC_VARIABLES.contains(name.toLowerCase(Locale.ROOT))) variables.put(name.toLowerCase(Locale.ROOT), resultSet.getString(2));
                }
            }
        }
        return variables;
    }

    private boolean hasReplicationPrivilege(Connection connection) {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("show grants")) {
            while (resultSet.next()) {
                String grant = resultSet.getString(1).toUpperCase(Locale.ROOT);
                if (grant.contains("ALL PRIVILEGES") || grant.contains("REPLICATION CLIENT") || grant.contains("REPLICATION SLAVE")
                    || grant.contains("REPLICATION REPLICA")) return true;
            }
        } catch (SQLException ignored) {
            // A restricted account may not inspect its grants; the check remains a hard failure.
        }
        return false;
    }

    private void addRequired(List<DataSourceCheckItemVo> checks, String code, String label, String actual, String expected, String failure) {
        boolean passed = expected == null ? isPositiveNumber(actual) : expected.equalsIgnoreCase(actual);
        checks.add(new DataSourceCheckItemVo(code, label, true, passed, actual, passed ? "配置符合要求" : failure));
    }

    private String retention(Map<String, String> variables) {
        String seconds = value(variables, "binlog_expire_logs_seconds");
        if (StringUtils.isNotBlank(seconds) && !"0".equals(seconds)) return seconds + " 秒";
        String days = value(variables, "binlog_expire_logs_days");
        return StringUtils.isBlank(days) ? "未配置" : days + " 天";
    }

    private String value(Map<String, String> values, String key) {
        return values.getOrDefault(key, "未知");
    }

    private boolean isPositiveNumber(String value) {
        try {
            return Long.parseLong(value) > 0;
        } catch (Exception ignored) {
            return false;
        }
    }

    private String querySingle(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSet.next() ? resultSet.getString(1) : "";
        }
    }

    private DataSource requireSource(Long sourceId) {
        if (sourceId == null) throw new ServiceException("数据源ID不能为空");
        DataSource source = dataSourceMapper.selectById(sourceId);
        if (source == null) throw new ServiceException("数据源不存在");
        source.setSourceType(source.getSourceType() == null ? null : source.getSourceType().trim().toUpperCase(Locale.ROOT));
        if (!SUPPORTED_TYPES.contains(source.getSourceType())) throw new ServiceException("暂不支持该数据源类型");
        return source;
    }

    private Connection openConnection(DataSource source, String database) throws SQLException {
        String db = StringUtils.isBlank(database) ? source.getDatabaseName() : database;
        String url;
        if ("MYSQL".equals(source.getSourceType())) {
            url = "jdbc:mysql://" + source.getHost() + ":" + source.getPort() + "/" + db
                + "?connectTimeout=5000&socketTimeout=10000&useSSL=" + "1".equals(source.getSslEnabled())
                + "&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        } else {
            url = "jdbc:postgresql://" + source.getHost() + ":" + source.getPort() + "/" + db
                + "?connectTimeout=5&socketTimeout=10&ssl=" + "1".equals(source.getSslEnabled());
        }
        return DriverManager.getConnection(url, source.getUsername(), source.getPassword());
    }

    private String defaultSchema(DataSource source) {
        return StringUtils.isBlank(source.getSchemaName()) ? "public" : source.getSchemaName();
    }

    private ServiceException metadataFailure(String message, SQLException ex) {
        return new ServiceException(message + "：" + safeMessage(ex));
    }

    private String safeMessage(Exception ex) {
        String message = ex.getMessage();
        return StringUtils.isBlank(message) ? "数据库连接或元数据读取失败" : message.replaceAll("(?i)(password|pwd)=[^&\\s]+", "$1=******");
    }
}
