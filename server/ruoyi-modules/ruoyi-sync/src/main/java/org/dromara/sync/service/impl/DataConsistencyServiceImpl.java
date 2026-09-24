package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.constant.SyncMode;
import org.dromara.sync.constant.SyncStatus;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.SyncTask;
import org.dromara.sync.domain.bo.SyncTaskDataCheckRequest;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.SyncTaskDataCheckBlockVo;
import org.dromara.sync.domain.vo.SyncTaskDataCheckResult;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.dromara.sync.service.IDataConsistencyService;
import org.dromara.sync.service.IDataSourceMetadataService;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.support.JdbcUrls;
import org.dromara.sync.support.SyncColumnSelectionValidator;
import org.dromara.sync.support.SyncText;
import org.dromara.sync.support.TableNames;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Performs a bounded, read-only row-count comparison for the MVP. */
@RequiredArgsConstructor
@Service
public class DataConsistencyServiceImpl implements IDataConsistencyService {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_$]+(\\.[A-Za-z0-9_$]+)?");
    /**
     * Each block costs two sequential blocking JDBC round-trips (source + target) in the
     * request thread. At this cap a worst case (~100ms/block on a slow link) stays under
     * two minutes; callers needing to cover a wider key range should raise blockSize
     * instead of the platform silently running an effectively unbounded check.
     */
    static final int MAX_KEY_RANGE_BLOCKS = 1000;
    private final SyncTaskMapper syncTaskMapper;
    private final IDataSourceService dataSourceService;
    private final IDataSourceMetadataService metadataService;

    @Override
    public SyncTaskDataCheckResult check(Long taskId, SyncTaskDataCheckRequest request) {
        SyncTask task = syncTaskMapper.selectById(taskId);
        if (task == null) throw new ServiceException("同步任务不存在");
        DataSource source = dataSourceService.requireUsable(task.getSourceId(), "源");
        DataSource target = dataSourceService.requireUsable(task.getTargetId(), "目标");
        if (DataSourceType.isKafka(target)) {
            SyncTaskDataCheckResult kafkaResult = new SyncTaskDataCheckResult();
            kafkaResult.setTaskId(taskId);
            kafkaResult.setSourceTable(qualifiedSourceTable(source.getDatabaseName(), task.getSourceTable()));
            kafkaResult.setTargetTable(task.getTargetTable());
            kafkaResult.setSuccess(false);
            kafkaResult.setMatched(false);
            kafkaResult.setMessage("Kafka 任务使用事件核对口径，不执行关系型目标行数核对；请通过 topic 的 key、offset、分区和事件信封进行核对");
            return kafkaResult;
        }
        SyncTaskDataCheckRequest options = request == null ? new SyncTaskDataCheckRequest() : request;
        SyncTaskDataCheckResult result = check(source, target, source.getDatabaseName(), task.getSourceTable(), task.getTargetSchema(), task.getTargetTable(), task, options);
        result.setTaskId(taskId);
        // Only the check columns: the task row read above is minutes old by now (see recordCheck).
        syncTaskMapper.recordCheck(taskId, result.getSourceRows(), result.getTargetRows(), result.getDifference(),
            result.isSuccess() ? (result.isMatched() ? "1" : "0") : null, LocalDateTime.now(),
            SyncText.truncateForColumn(result.getMessage()));
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
        boolean targetPostgres = DataSourceType.isPostgres(target);
        String targetTable = qualifiedTable(targetPostgres
            ? StringUtils.defaultIfBlank(targetSchema, TableNames.DEFAULT_POSTGRES_SCHEMA) : target.getDatabaseName(),
            targetTableName, "目标表名");
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
                return checkByKeyRange(source, target, sourceTable, targetTable, targetPostgres, task, blockSize, result);
            }
            long sourceRows = count(source, sourceTable, false);
            long targetRows = count(target, targetTable, targetPostgres);
            result.setSourceRows(sourceRows);
            result.setTargetRows(targetRows);
            result.setDifference(sourceRows - targetRows);
            result.setMatched(sourceRows == targetRows);
            result.setSuccess(true);
            result.setMessage(result.isMatched() ? "源端和目标端行数一致" : "源端和目标端行数不一致");
        } catch (SQLException ex) {
            result.setSuccess(false);
            result.setMatched(false);
            result.setMessage("数据核对失败：" + SyncText.safeMessage(ex, "连接失败"));
        }
        return result;
    }

    private SyncTaskDataCheckResult checkByKeyRange(DataSource source, DataSource target, String sourceTable,
                                                    String targetTable, boolean targetPostgres, SyncTask task, int blockSize,
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
            result.setTargetRows(count(target, targetTable, targetPostgres));
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
        // The block SIZE is bounded above (see the blockSize check earlier in this
        // method), but the block COUNT (span / step) was not - a small blockSize on a
        // wide-ranging key made this "bounded, read-only" check run an effectively
        // unbounded number of sequential blocking round-trips in the request thread.
        BigDecimal span = range.max().subtract(range.min());
        BigDecimal estimatedBlocks = estimateBlockCount(range.min(), range.max(), step);
        if (estimatedBlocks.compareTo(BigDecimal.valueOf(MAX_KEY_RANGE_BLOCKS)) > 0) {
            BigDecimal suggestedBlockSize = span.divide(BigDecimal.valueOf(MAX_KEY_RANGE_BLOCKS), 0, RoundingMode.CEILING);
            result.setSuccess(false);
            result.setMatched(false);
            result.setMessage("同步键范围过大，预计分 " + estimatedBlocks.toPlainString() + " 块，超过单次核对上限 "
                + MAX_KEY_RANGE_BLOCKS + " 块；请将核对步长调整到 " + suggestedBlockSize.toPlainString() + " 或以上后重试");
            return result;
        }
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
            long targetRows = countRange(target, targetTable, quoteIdentifier(key.getName(), targetPostgres), lower, upper, targetPostgres);
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
        try (Connection connection = JdbcUrls.open(source);
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
        try (Connection connection = JdbcUrls.open(source);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBigDecimal(1, lower);
            statement.setBigDecimal(2, upper);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) throw new SQLException("未返回分块行数");
                return resultSet.getLong(1);
            }
        }
    }

    /**
     * A conservative (never-under) estimate of how many blocks the loop below will run -
     * it may overshoot the real count by one, which only makes the cap check stricter,
     * never looser. Package-private for direct unit testing without a live JDBC connection.
     */
    static BigDecimal estimateBlockCount(BigDecimal min, BigDecimal max, BigDecimal step) {
        return max.subtract(min).divide(step, 0, RoundingMode.CEILING).add(BigDecimal.ONE);
    }

    private static String normalizeMode(String mode) {
        return "KEY_RANGE".equalsIgnoreCase(mode) ? "KEY_RANGE" : "COUNT";
    }

    private static boolean isMovingCdc(SyncTask task) {
        return SyncMode.FULL_CDC.equalsIgnoreCase(task.getSyncMode()) && SyncStatus.isActive(task.getStatus());
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
        try (Connection connection = JdbcUrls.open(source);
             PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM " + quoteTable(table, postgres));
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) throw new SQLException("未返回行数");
            return resultSet.getLong(1);
        }
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
        return qualifiedTable(database, table, "源表名");
    }

    /** Trims, prefixes an unqualified name with {@code prefix.} and rejects anything but plain identifiers. */
    private static String qualifiedTable(String prefix, String table, String label) {
        String value = TableNames.qualified(prefix, StringUtils.isBlank(table) ? "" : table.trim());
        validateIdentifier(value, label);
        return value;
    }

    private static void validateIdentifier(String value, String label) {
        if (!IDENTIFIER.matcher(value).matches()) throw new ServiceException(label + "包含不支持的字符");
    }
}
