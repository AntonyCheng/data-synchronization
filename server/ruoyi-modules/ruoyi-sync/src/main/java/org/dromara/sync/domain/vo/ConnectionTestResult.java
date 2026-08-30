package org.dromara.sync.domain.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Result of a JDBC connectivity test.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionTestResult {

    private boolean success;
    private String message;
    private long latencyMs;

    public static ConnectionTestResult success(long latencyMs) {
        return new ConnectionTestResult(true, "连接成功", latencyMs);
    }

    public static ConnectionTestResult failure(String message) {
        return new ConnectionTestResult(false, message, 0L);
    }
}
