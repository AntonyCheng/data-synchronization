package org.dromara.sync.service.impl;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.vo.DataSourceColumnVo;
import org.dromara.sync.domain.vo.DataSourceIndexVo;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Validates the MVP's one-to-one column projection and reliable sync-key choice. */
final class SyncColumnSelectionValidator {

    private SyncColumnSelectionValidator() {
    }

    static Selection validate(DataSourceMetadataVo metadata, String configuredColumns, String configuredKeyColumns) {
        if (metadata == null || metadata.getColumns() == null || metadata.getColumns().isEmpty()) {
            throw new ServiceException("源表没有可同步字段");
        }
        Map<String, String> available = new LinkedHashMap<>();
        for (DataSourceColumnVo column : metadata.getColumns()) {
            if (StringUtils.isNotBlank(column.getName())) available.put(normalize(column.getName()), column.getName());
        }
        List<String> requestedColumns = resolveColumns(configuredColumns, available, "同步字段");
        List<String> selected = requestedColumns.isEmpty() ? new ArrayList<>(available.values()) : requestedColumns;

        List<List<String>> candidates = keyCandidates(metadata, available);
        List<String> requestedSyncKeys = resolveColumns(configuredKeyColumns, available, "同步键");
        List<String> syncKeys = requestedSyncKeys.isEmpty() && !candidates.isEmpty() ? candidates.get(0) : requestedSyncKeys;
        if (!syncKeys.isEmpty() && candidates.stream().noneMatch(candidate -> sameColumns(candidate, syncKeys))) {
            throw new ServiceException("同步键必须选择源表主键或所有字段均为非空的唯一索引");
        }
        for (String key : syncKeys) {
            if (selected.stream().noneMatch(column -> normalize(column).equals(normalize(key)))) {
                throw new ServiceException("同步键字段不能排除：" + key);
            }
        }
        return new Selection(List.copyOf(selected), List.copyOf(syncKeys));
    }

    static List<String> parseColumns(String configuredColumns) {
        if (StringUtils.isBlank(configuredColumns)) return List.of();
        List<String> result = new ArrayList<>();
        for (String value : configuredColumns.split(",")) {
            String column = value == null ? "" : value.trim();
            if (!column.isEmpty()) result.add(column);
        }
        return result;
    }

    static String serialize(Collection<String> columns) {
        return columns == null || columns.isEmpty() ? null : String.join(",", columns);
    }

    private static List<String> resolveColumns(String configuredColumns, Map<String, String> available, String label) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String requested : parseColumns(configuredColumns)) {
            String actual = available.get(normalize(requested));
            if (actual == null) throw new ServiceException(label + "不存在于源表：" + requested);
            result.putIfAbsent(normalize(actual), actual);
        }
        return new ArrayList<>(result.values());
    }

    private static List<List<String>> keyCandidates(DataSourceMetadataVo metadata, Map<String, String> available) {
        List<List<String>> result = new ArrayList<>();
        List<String> primaryKeys = resolveMetadataColumns(metadata.getPrimaryKeys(), available);
        if (!primaryKeys.isEmpty()) result.add(primaryKeys);
        if (metadata.getUniqueKeys() != null) {
            for (DataSourceIndexVo key : metadata.getUniqueKeys()) {
                if (!Boolean.TRUE.equals(key.getAllNotNull())) continue;
                List<String> candidate = resolveMetadataColumns(key.getColumns(), available);
                if (!candidate.isEmpty() && result.stream().noneMatch(existing -> sameColumns(existing, candidate))) {
                    result.add(candidate);
                }
            }
        }
        return result;
    }

    private static List<String> resolveMetadataColumns(List<String> columns, Map<String, String> available) {
        if (columns == null || columns.isEmpty()) return List.of();
        List<String> result = new ArrayList<>();
        for (String column : columns) {
            String actual = available.get(normalize(column));
            if (actual == null) return List.of();
            result.add(actual);
        }
        return result;
    }

    private static boolean sameColumns(List<String> left, List<String> right) {
        if (left.size() != right.size()) return false;
        for (int index = 0; index < left.size(); index++) {
            if (!normalize(left.get(index)).equals(normalize(right.get(index)))) return false;
        }
        return true;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    record Selection(List<String> selectedColumns, List<String> syncKeyColumns) {
    }
}
