package org.dromara.sync.support;

import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;

/** Qualified / unqualified table-name handling shared across the sync module. */
public final class TableNames {

    public static final String DEFAULT_POSTGRES_SCHEMA = "public";

    private TableNames() {
    }

    /** {@code db.table} -> {@code table}; an unqualified name is returned unchanged. */
    public static String unqualified(String tableReference) {
        int separator = tableReference == null ? -1 : tableReference.lastIndexOf('.');
        return separator < 0 ? tableReference : tableReference.substring(separator + 1);
    }

    /** Prefixes {@code table} with {@code prefix.} unless it is already qualified. */
    public static String qualified(String prefix, String table) {
        return table.indexOf('.') >= 0 ? table : prefix + '.' + table;
    }

    /**
     * Display-only target name. The {@code schema.} prefix only means something for
     * PostgreSQL targets - MySQL and Kafka have no schema concept, so prefixing them with
     * the PostgreSQL-only {@code public} default was misleading.
     */
    public static String display(DataSource target, String targetSchema, String targetTable) {
        if (!DataSourceType.isPostgres(target)) return targetTable;
        return (StringUtils.isBlank(targetSchema) ? DEFAULT_POSTGRES_SCHEMA : targetSchema.trim()) + "." + targetTable;
    }
}
