package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * A MySQL source server's time zone next to the one the engine is told for it
 * ({@code server-time-zone}). They must agree, or every binlog-phase {@code TIMESTAMP} is
 * shifted by the difference. Returned by the connection test and the CDC precheck.
 */
@Data
public class DataSourceTimeZoneVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** What the server reports, e.g. {@code SYSTEM（UTC）}, {@code +08:00}, {@code Europe/Berlin}; null when unreadable. */
    private String serverTimeZone;

    /** The server's current offset, e.g. {@code UTC+00:00}; null when unreadable. */
    private String serverUtcOffset;

    /** The IANA id to configure for this server; null when none can be inferred. */
    private String suggestedTimeZone;

    /** The data source's configured zone; blank in compatibility mode. */
    private String configuredTimeZone;

    /** The zone the engine is told: the configured one, else {@code Asia/Shanghai}. */
    private String effectiveTimeZone;

    /** The effective zone's current offset, e.g. {@code UTC+08:00}; null when the stored zone is not a valid id. */
    private String effectiveUtcOffset;

    /** True when both offsets agree now; null when the server's zone could not be read. */
    private Boolean matched;

    /** Effective minus server offset in hours (e.g. {@code 8}, {@code -5}, {@code 5.5}): how far binlog-phase TIMESTAMP values drift. */
    private String shiftHours;

    /** One-line verdict for the operator. */
    private String message;
}
