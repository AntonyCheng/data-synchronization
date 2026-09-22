package org.dromara.sync.support;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.dromara.sync.domain.SyncTaskGroupItem;
import org.dromara.sync.domain.vo.DataSourceMetadataVo;
import org.dromara.sync.domain.vo.TargetCompatibilityVo;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Normalized picture of a source table's columns and sync key, persisted as JSON in
 * {@code ds_sync_task_group_item.schema_snapshot} when a table item starts, and diffed
 * against live metadata by the periodic DDL check. Field names and order are part of
 * the persisted format - do not reorder them.
 */
public final class TableSchemaSnapshot {

    /**
     * Own codec rather than the Spring-managed JsonUtils: the persisted shape is plain
     * fields in declaration order, which a default mapper renders identically, and this
     * keeps the helper usable without an application context.
     */
    private static final JsonMapper JSON = JsonMapper.builder()
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .build();

    private TableSchemaSnapshot() {
    }

    public static Snapshot of(DataSourceMetadataVo metadata) {
        Snapshot snapshot = new Snapshot();
        snapshot.setPrimaryKeys(metadata.getPrimaryKeys().stream().map(TableSchemaSnapshot::normalize).toList());
        snapshot.setColumns(metadata.getColumns().stream()
            .map(column -> new Column(normalize(column.getName()), normalize(column.getTypeName()),
                column.getJdbcType(), column.getSize(), column.getScale(), column.getNullable()))
            .sorted(Comparator.comparing(Column::getName)).toList());
        return snapshot;
    }

    public static String toJson(Snapshot snapshot) {
        return JSON.writeValueAsString(snapshot);
    }

    public static Snapshot fromJson(String json) {
        return JSON.readValue(json, Snapshot.class);
    }

    /** Records the current source schema on the item as the baseline later DDL checks diff against. */
    public static void baseline(SyncTaskGroupItem item, DataSourceMetadataVo metadata) {
        String snapshot = toJson(of(metadata));
        item.setSchemaSnapshot(snapshot);
        item.setSchemaHash(SyncText.sha256Hex(snapshot));
    }

    /**
     * True when {@code selectedColumns} covers every column recorded in the baseline snapshot -
     * i.e. the projection was "all columns" when the table was started. Used by table
     * reinitialization to decide whether a column added since should be picked up
     * automatically (a full selection follows the table) or the explicit selection kept.
     * Unknown (no snapshot yet) is treated as "not full".
     */
    public static boolean coversAllColumns(Snapshot baseline, List<String> selectedColumns) {
        if (baseline == null || baseline.getColumns().isEmpty() || selectedColumns == null) return false;
        Set<String> selected = selectedColumns.stream().map(TableSchemaSnapshot::normalize).collect(Collectors.toSet());
        return baseline.getColumns().stream().map(Column::getName).allMatch(selected::contains);
    }

    public static Diff diff(Snapshot baseline, Snapshot current) {
        if (baseline == null) return new Diff("TABLE_UNAVAILABLE", "HIGH", "无法读取原始表结构快照", true);
        Map<String, Column> before = byName(baseline);
        Map<String, Column> after = byName(current);
        List<String> added = after.keySet().stream().filter(name -> !before.containsKey(name)).toList();
        List<String> removed = before.keySet().stream().filter(name -> !after.containsKey(name)).toList();
        List<String> altered = before.keySet().stream().filter(after::containsKey)
            .filter(name -> !before.get(name).equals(after.get(name)))
            .map(name -> name + " (" + before.get(name).display() + " -> " + after.get(name).display() + ")").toList();
        boolean keyChanged = !baseline.getPrimaryKeys().equals(current.getPrimaryKeys());
        if (added.isEmpty() && removed.isEmpty() && altered.isEmpty() && !keyChanged) {
            return new Diff("", "", "", false);
        }
        List<String> kinds = new ArrayList<>();
        if (!added.isEmpty()) kinds.add("ADD_COLUMN");
        if (!removed.isEmpty()) kinds.add("DROP_COLUMN");
        if (!altered.isEmpty()) kinds.add("ALTER_COLUMN");
        if (keyChanged) kinds.add("KEY_CHANGED");
        boolean lowRisk = removed.isEmpty() && altered.isEmpty() && !keyChanged
            && added.stream().allMatch(name -> Boolean.TRUE.equals(after.get(name).getNullable()));
        List<String> details = new ArrayList<>();
        if (!added.isEmpty()) details.add("新增字段：" + String.join(", ", added));
        if (!removed.isEmpty()) details.add("删除字段：" + String.join(", ", removed));
        if (!altered.isEmpty()) details.add("字段定义变化：" + String.join("；", altered));
        if (keyChanged) {
            details.add("同步键变化：" + String.join(",", baseline.getPrimaryKeys()) + " -> " + String.join(",", current.getPrimaryKeys()));
        }
        return new Diff(String.join(",", kinds), lowRisk ? "LOW" : "HIGH", String.join("；", details), true);
    }

    /** Operator guidance for a detected drift; the platform never alters the target itself. */
    public static String remediation(Diff diff, TargetCompatibilityVo compatibility) {
        String prefix = "LOW".equals(diff.riskLevel())
            ? "平台未启用自动 DDL。请在目标表补齐新增的可空字段，或确认目标已具备等价字段。"
            : "高风险结构变更已隔离该表。请评估字段和同步键语义，备份目标数据后将目标表调整为兼容结构。";
        if (!compatibility.isPassed()) {
            return prefix + " 当前兼容性检查未通过：" + compatibility.getMessage() + "。修复后重新执行结构检查，或修复后使用“重新初始化该表”重建。";
        }
        return prefix + " 当前目标表兼容性已通过。若该表启动时选择了全部字段，请使用“重新初始化该表”以纳入新增字段（此时“恢复该表”会因引擎配置变化被拒绝）；仅当字段范围不变时可用“恢复该表”从 savepoint 继续。";
    }

    private static Map<String, Column> byName(Snapshot snapshot) {
        return snapshot.getColumns().stream()
            .collect(Collectors.toMap(Column::getName, column -> column, (left, right) -> left, TreeMap::new));
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    @Data
    public static class Snapshot {
        private List<Column> columns = new ArrayList<>();
        private List<String> primaryKeys = new ArrayList<>();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Column {
        private String name;
        private String typeName;
        private Integer jdbcType;
        private Integer size;
        private Integer scale;
        private Boolean nullable;

        private String display() {
            return typeName + "(" + (size == null ? "" : size) + (scale == null ? "" : "," + scale) + ")"
                + (Boolean.TRUE.equals(nullable) ? " NULL" : " NOT NULL");
        }
    }

    public record Diff(String changeType, String riskLevel, String details, boolean changed) {
    }
}
