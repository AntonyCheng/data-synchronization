package org.dromara.sync.support;

import org.dromara.sync.domain.vo.DataSourceColumnVo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Worst-case size of the Kafka records one MySQL source row turns into, from its column types - what
 * the Kafka-target precheck compares with the record limits. Deliberately pessimistic: a value counts
 * at its declared maximum, in JSON (base64 for binary), and an UPDATE carries two images. It answers
 * "can a row of this table exceed the limit", not "how large are the rows".
 */
public final class KafkaMessageSizes {

    /** A column whose value alone may exceed this is named in the precheck (a TEXT's 64 KB is not). */
    static final long LARGE_COLUMN_BYTES = 1024 * 1024L;
    /** JSON field name, quotes, separators and a null / number of the widest kind. */
    private static final long FIELD_OVERHEAD_BYTES = 32;
    /** One column's entry in the value schema each raw CDC record carries, per image. */
    private static final long SCHEMA_BYTES_PER_COLUMN = 160;
    /** Envelope, key, source block and the rest of the published event. */
    private static final long EVENT_OVERHEAD_BYTES = 512;
    private static final long UNBOUNDED_BYTES = 4L * 1024 * 1024 * 1024;
    private static final Set<String> UNBOUNDED_TYPES = Set.of("LONGTEXT", "LONGBLOB", "JSON", "GEOMETRY", "POINT", "LINESTRING",
        "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION");

    private KafkaMessageSizes() {
    }

    /**
     * {@code rawRecordBytes}: the engine's raw-topic record (a CDC record holds every column, both
     * images and the value schema; a FULL record the selected columns only). {@code eventBytes}: the
     * published event (selected columns, both images). {@code largeColumns}: the selected-or-not
     * columns that make a row large, as {@code name(TYPE)}.
     */
    public record Estimate(long rawRecordBytes, long eventBytes, List<String> largeColumns) {
    }

    /** {@code selectedColumns} empty = every column; names match case-insensitively. */
    public static Estimate estimate(List<DataSourceColumnVo> columns, Collection<String> selectedColumns, boolean cdc) {
        Set<String> selected = selectedColumns == null ? Set.of()
            : selectedColumns.stream().map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        long all = 0;
        long chosen = 0;
        List<String> large = new ArrayList<>();
        for (DataSourceColumnVo column : columns) {
            long valueBytes = Math.min(UNBOUNDED_BYTES, valueBytes(column));
            long value = valueBytes + FIELD_OVERHEAD_BYTES;
            all = Math.min(UNBOUNDED_BYTES, all + value);
            boolean isSelected = selected.isEmpty() || selected.contains(String.valueOf(column.getName()).toLowerCase(Locale.ROOT));
            if (isSelected) chosen = Math.min(UNBOUNDED_BYTES, chosen + value);
            if (valueBytes > LARGE_COLUMN_BYTES && (cdc || isSelected)) large.add(column.getName() + "(" + column.getTypeName() + ")");
        }
        long raw = cdc
            ? Math.min(UNBOUNDED_BYTES, 2 * all + 2 * SCHEMA_BYTES_PER_COLUMN * columns.size() + EVENT_OVERHEAD_BYTES)
            : Math.min(UNBOUNDED_BYTES, chosen + EVENT_OVERHEAD_BYTES);
        long event = Math.min(UNBOUNDED_BYTES, (cdc ? 2 * chosen : chosen) + EVENT_OVERHEAD_BYTES);
        return new Estimate(raw, event, List.copyOf(large));
    }

    /** Largest JSON form of one value of {@code column}. */
    static long valueBytes(DataSourceColumnVo column) {
        String type = baseType(column.getTypeName());
        long size = column.getSize() == null || column.getSize() <= 0 ? 0 : column.getSize();
        if (UNBOUNDED_TYPES.contains(type)) return UNBOUNDED_BYTES;
        return switch (type) {
            case "TINYTEXT" -> 255;
            case "TEXT" -> 65_535;
            case "MEDIUMTEXT" -> 16_777_215;
            case "TINYBLOB" -> base64(255);
            case "BLOB" -> base64(65_535);
            case "MEDIUMBLOB" -> base64(16_777_215);
            // Characters, up to 4 bytes each in utf8mb4.
            case "CHAR", "VARCHAR", "ENUM", "SET" -> 4 * Math.max(1, size);
            case "BINARY", "VARBINARY" -> base64(Math.max(1, size));
            case "BIT" -> base64(8);
            default -> 40;
        };
    }

    /** {@code 1.5 MB}, {@code 4.0 GB}, {@code 800 B}. */
    public static String human(long bytes) {
        if (bytes >= 1024L * 1024 * 1024) return String.format(Locale.ROOT, "%.1f GB", bytes / (1024.0 * 1024 * 1024));
        if (bytes >= 1024L * 1024) return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024));
        if (bytes >= 1024) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
        return bytes + " B";
    }

    private static long base64(long bytes) {
        return (bytes + 2) / 3 * 4;
    }

    private static String baseType(String typeName) {
        if (typeName == null) return "";
        String type = typeName.trim().toUpperCase(Locale.ROOT);
        int space = type.indexOf(' ');
        return space < 0 ? type : type.substring(0, space);
    }
}
