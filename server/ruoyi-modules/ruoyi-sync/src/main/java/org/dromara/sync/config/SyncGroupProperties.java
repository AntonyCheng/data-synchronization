package org.dromara.sync.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Limits of task groups ({@code sync.group.*}). */
@Data
@Component
@ConfigurationProperties(prefix = "sync.group")
public class SyncGroupProperties {

    /** Hard ceiling for {@link #maxTables}, whatever is configured. */
    public static final int MAX_TABLES_CEILING = 200;

    /**
     * Most tables one group may hold, listed explicitly (multi-table) or discovered
     * (whole-database). Every table is its own SeaTunnel job with its own binlog reader, so this
     * bounds what one group puts on the customer's source database (one replication connection
     * per table) and on the engine. The platform-side reasons for the old hard-coded 20 are gone
     * (no transaction around engine calls since P1-3, the module's own scheduler since P1-4), so
     * it is now the operator's call; the default stays 20 because the source is the real limit.
     */
    private int maxTables = 20;

    /** {@link #maxTables} clamped to 1..{@value #MAX_TABLES_CEILING}. */
    public int effectiveMaxTables() {
        return Math.max(1, Math.min(MAX_TABLES_CEILING, maxTables));
    }
}
