package org.dromara.sync.domain.vo;

import lombok.Data;

/**
 * Result of validating both endpoints of a task.
 */
@Data
public class SyncTaskValidationResult {

    private boolean valid;
    private ConnectionTestResult source;
    private ConnectionTestResult target;
    private DataSourceCdcPrecheckVo cdcPrecheck;
    private TargetCompatibilityVo targetCompatibility;
    private String message;
}
