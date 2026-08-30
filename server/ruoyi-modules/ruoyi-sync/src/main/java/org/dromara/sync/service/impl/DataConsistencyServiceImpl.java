package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.bo.SyncTaskDataCheckRequest;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskDataCheckBlockVo;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.mapper.DataSourceMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Performs a bounded, read-only row-count comparison for the MVP. */
@RequiredArgsConstructor
@Service
public class DataConsistencyServiceImpl implements IDataConsistencyService {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_$]+(\\.[A-Za-z0-9_$]+)?");
    private final SyncTaskMapper syncTaskMapper;
    private final DataSourceMapper dataSourceMapper;
    private final IDataSourceMetadataService metadataService;

    @Override
    public SyncTaskDataCheckResult check(Long taskId) {
        return check(taskId, null);
    }

    @Override
    public SyncTaskDataCheckResult check(Long taskId, SyncTaskDataCheckRequest request) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) throw new ServiceException("同步任务不存在");
        DataSource source = requireSource(task.getSourceId(), "源");
        DataSource target = requireSource(task.getTargetId(), "目标");
        SyncTaskDataCheckRequest options = request == null ? new SyncTaskDataCheckRequest() : request;
        SyncTaskDataCheckResult result = check(source, target, source.getDatabaseName(), task.getSourceTable(), task.getTargetSchema(), task.getTargetTable(), task, options);
        result.setTaskId(taskId);
        task.setLastCheckSourceRows(result.getSourceRows());
        task.setLastCheckTargetRows(result.getTargetRows());
        task.setLastCheckDifference(result.getDifference());
        task.setLastCheckMatched(result.isSuccess() ? (result.isMatched() ? "1" : "0") : null);
        task.setLastCheckTime(LocalDateTime.now());
        task.setLastCheckMessage(result.getMessage());
        syncTaskMapper.updateById(task);
        return result;
    }

    @Override
    public SyncTaskDataCheckResult check(DataSource source, DataSource target, String sourceDatabase, String sourceTableName,
                                         String targetSchema, String targetTableName) {
        return check(source, target, sourceDatabase, sourceTableName, targetSchema, targetTableName, null,
            new SyncTaskDataCheckRequest());
    }

    private SyncTaskDataCheckResult check(DataSource source, DataSource target, String sourceDatabase, String sourceTableName,
                                          String targetSchema, String targetTableName, SyncTask task,
                                          SyncTaskDataCheckRequest request) {
        String sourceTable = qualifiedSourceTable(sourceDatabase, sourceTableName);
        String targetTable = qualifiedTargetTable(StringUtils.isBlank(targetSchema) ? "public" : targetSchema, targetTableName);
        SyncTaskDataCheckResult result = new SyncTaskDataCheckResult();
        result.setSourceTable(sourceTable);
        result.setTargetTable(targetTable);
        String mode = normalizeMode(request.getMode());
        int blockSize = request.getBlockSize() == null ? 10000 : request.getBlockSize();
        if (blockSize < 1 || blockSize > 1_000_000) throw new ServiceException("核对分块步长必须在 1 到 1000000 之间");
        result.setCheckMode(mode);
        result.setBlockSize(blockSize);
        if (task != null && Boolean.TRUE.equals(request.getStrictWatermark()) && isMovingCdc(task)) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setWatermarkMessage("持续 CDC 任务处于运行状态，源端和目标端不是同一水位；请先暂停任务后执行严格核对");
            result.setMessage(result.getWatermarkMessage());
            return result;
        }
        try {
            if ("KEY_RANGE".equals(mode)) {
                return checkByKeyRange(source, target, sourceTable, targetTable, task, blockSize, result);
            }
            long sourceRows = count(source, sourceTable, false);
            long targetRows = count(target, targetTable, true);
            result.setSourceRows(sourceRows);
            result.setTargetRows(targetRows);
            result.setDifference(sourceRows - targetRows);
            result.setMatched(sourceRows == targetRows);
            result.setSuccess(true);
            result.setMessage(result.isMatched() ? "源端和目标端行数一致" : "源端和目标端行数不一致");
        } catch (SQLException ex) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setMessage("数据核对失败：" + safeMessage(ex));
        }
        return result;
    }

    private SyncTaskDataCheckResult checkByKeyRange(DataSource source, DataSource target, String sourceTable,
                                                    String targetTable, SyncTask task, int blockSize,
                                                    SyncTaskDataCheckResult result) throws SQLException {
        if (task == null || StringUtils.isBlank(task.getSyncKeyColumns())) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setMessage("未配置同步键，无法执行分块核对；请选择行数核对");
            return result;
        }
        List<String> keys = SyncColumnSelectionValidator.parseColumns(task.getSyncKeyColumns());
        if (keys.size() != 1) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setMessage("当前分块核对仅支持单列同步键，联合键请使用行数核对");
            return result;
        }
        DataSourceMetadataVo metadata = metadataService.queryTableMetadata(source.getSourceId(), source.getDatabaseName(), null, task.getSourceTable());
        DataSourceColumnVo key = metadata.getColumns().stream()
            .filter(column -> column.getName() != null && column.getName().equalsIgnoreCase(keys.get(0)))
            .findFirst().orElse(null);
        if (key == null || !isNumeric(key.getTypeName())) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setMessage("同步键不是数值类型，当前版本暂不支持范围分块核对；请选择行数核对");
            return result;
        }
        String keyColumn = quoteIdentifier(key.getName(), false);
        Range range = readRange(source, sourceTable, keyColumn);
        result.setSourceRows(range.count());
        if (range.count() == 0) {
            result.setTargetRows(count(target, targetTable, true));
            result.setDifference(result.getSourceRows() - result.getTargetRows());
            result.setMatched(result.getDifference() == 0);
            result.setSuccess(true);
            result.setMessage(result.isMatched() ? "源端和目标端均为空" : "源端和目标端行数不一致");
            result.setTotalBlocks(0);
            result.setMatchedBlocks(result.isMatched() ? 1 : 0);
            result.setMismatchedBlocks(result.isMatched() ? 0 : 1);
            return result;
        }
        BigDecimal step = BigDecimal.valueOf(blockSize);
        BigDecimal lower = range.min();
        int index = 1;
        int matched = 0;
        int mismatched = 0;
        while (lower.compareTo(range.max()) <= 0) {
            BigDecimal upper = lower.add(step);
            SyncTaskDataCheckBlockVo block = new SyncTaskDataCheckBlockVo();
            block.setIndex(index++);
            block.setLowerBound(lower.toPlainString());
            block.setUpperBound(upper.toPlainString());
            long sourceRows = countRange(source, sourceTable, keyColumn, lower, upper, false);
            long targetRows = countRange(target, targetTable, quoteIdentifier(key.getName(), true), lower, upper, true);
            block.setSourceRows(sourceRows);
            block.setTargetRows(targetRows);
            block.setDifference(sourceRows - targetRows);
            block.setMatched(sourceRows == targetRows);
            block.setSuccess(true);
            block.setMessage(block.isMatched() ? "一致" : "行数不一致");
            result.getBlocks().add(block);
            if (block.isMatched()) matched++; else mismatched++;
            lower = upper;
        }
        long targetRows = result.getBlocks().stream().mapToLong(block -> block.getTargetRows() == null ? 0 : block.getTargetRows()).sum();
        result.setTargetRows(targetRows);
        result.setDifference(result.getSourceRows() - targetRows);
        result.setMatched(result.getDifference() == 0 && mismatched == 0);
        result.setSuccess(true);
        result.setTotalBlocks(result.getBlocks().size());
        result.setMatchedBlocks(matched);
        result.setMismatchedBlocks(mismatched);
        result.setFailedBlocks(0);
        result.setMessage(result.isMatched() ? "所有同步键范围核对一致" : "存在同步键范围行数不一致");
        return result;
    }

    private Range readRange(DataSource source, String table, String keyColumn) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(source, false), source.getUsername(), source.getPassword());
             PreparedStatement statement = connection.prepareStatement("SELECT MIN(" + keyColumn + "), MAX(" + keyColumn + "), COUNT(*) FROM " + quoteTable(table, false));
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) throw new SQLException("未返回同步键范围");
            BigDecimal min = resultSet.getBigDecimal(1);
            BigDecimal max = resultSet.getBigDecimal(2);
            return new Range(min == null ? BigDecimal.ZERO : min, max == null ? BigDecimal.ZERO : max, resultSet.getLong(3));
        }
    }

    private long countRange(DataSource source, String table, String keyColumn, BigDecimal lower, BigDecimal upper, boolean postgres) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quoteTable(table, postgres) + " WHERE " + keyColumn + " >= ? AND " + keyColumn + " < ?";
        try (Connection connection = DriverManager.getConnection(jdbcUrl(source, postgres), source.getUsername(), source.getPassword());
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBigDecimal(1, lower);
            statement.setBigDecimal(2, upper);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) throw new SQLException("未返回分块行数");
                return resultSet.getLong(1);
            }
        }
    }

    private static String normalizeMode(String mode) {
        return "KEY_RANGE".equalsIgnoreCase(mode) ? "KEY_RANGE" : "COUNT";
    }

    private static boolean isMovingCdc(SyncTask task) {
        return "FULL_CDC".equalsIgnoreCase(task.getSyncMode()) && List.of("RUNNING", "PAUSING").contains(task.getStatus());
    }

    private static boolean isNumeric(String typeName) {
        String type = typeName == null ? "" : typeName.toLowerCase(Locale.ROOT);
        return type.matches(".*(tinyint|smallint|mediumint|int|integer|bigint|decimal|numeric|float|double|real|serial|money).*");
    }

    private static String quoteIdentifier(String value, boolean postgres) {
        String quote = postgres ? "\"" : "`";
        if (value == null || !Pattern.matches("[A-Za-z0-9_$]+", value)) throw new ServiceException("同步键包含不支持的字符");
        return quote + value + quote;
    }

    private record Range(BigDecimal min, BigDecimal max, long count) {
    }

    private long count(DataSource source, String table, boolean postgres) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl(source, postgres), source.getUsername(), source.getPassword());
             PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM " + quoteTable(table, postgres));
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) throw new SQLException("未返回行数");
            return resultSet.getLong(1);
        }
    }

    private DataSource requireSource(Long sourceId, String side) {
        if (sourceId == null) throw new ServiceException(side + "数据源不能为空");
        DataSource source = dataSourceMapper.selectById(sourceId);
        if (source == null) throw new ServiceException(side + "数据源不存在");
        if (StringUtils.isBlank(source.getPassword())) throw new ServiceException(side + "数据源密码未配置");
        return source;
    }

    private static String quoteTable(String table, boolean postgres) {
        String[] parts = table.split("\\.");
        String quote = postgres ? "\"" : "`";
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) result.append('.');
            result.append(quote).append(parts[i]).append(quote);
        }
        return result.toString();
    }

    private static String qualifiedSourceTable(String database, String table) {
        String value = StringUtils.isBlank(table) ? "" : table.trim();
        if (value.indexOf('.') < 0) value = database + '.' + value;
        validateIdentifier(value, "源表名");
        return value;
    }

    private static String qualifiedTargetTable(String schema, String table) {
        String value = StringUtils.isBlank(table) ? "" : table.trim();
        if (value.indexOf('.') < 0) value = schema + '.' + value;
        validateIdentifier(value, "目标表名");
        return value;
    }

    private static void validateIdentifier(String value, String label) {
        if (!IDENTIFIER.matcher(value).matches()) throw new ServiceException(label + "包含不支持的字符");
    }

    private static String jdbcUrl(DataSource source, boolean postgres) {
        if (postgres) {
            return "jdbc:postgresql://" + source.getHost() + ':' + source.getPort() + '/' + source.getDatabaseName()
                + "?connectTimeout=5&socketTimeout=5&ssl=" + ("1".equals(source.getSslEnabled()));
        }
        return "jdbc:mysql://" + source.getHost() + ':' + source.getPort() + '/' + source.getDatabaseName()
            + "?connectTimeout=5000&socketTimeout=5000&useSSL=" + ("1".equals(source.getSslEnabled()))
            + "&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    }

    private static String safeMessage(SQLException ex) {
        String message = StringUtils.isBlank(ex.getMessage()) ? "连接失败" : ex.getMessage();
        return message.replaceAll("(?i)(password\\s*[=:]\\s*)[^; ,]+", "$1******");
    }
}
