package org.dromara.sync.domain.vo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of a JDBC connectivity test.
 */
@Data
@NoArgsConstructor
public class ConnectionTestResult {

    private boolean success;
    private String message;
    private long latencyMs;

    /** MySQL only: the server's time zone against the data source's; null when not read. */
    private DataSourceTimeZoneVo timeZone;

    public ConnectionTestResult(boolean success, String message, long latencyMs) {
        this.success = success;
        this.message = message;
        this.latencyMs = latencyMs;
    }

    public static ConnectionTestResult success(long latencyMs) {
        return new ConnectionTestResult(true, "连接成功", latencyMs);
    }

    public static ConnectionTestResult failure(String message) {
        return new ConnectionTestResult(false, message, 0L);
    }
}
