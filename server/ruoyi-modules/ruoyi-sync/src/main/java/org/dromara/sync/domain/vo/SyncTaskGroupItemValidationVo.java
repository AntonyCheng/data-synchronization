package org.dromara.sync.domain.vo;

import lombok.Data;

@Data
public class SyncTaskGroupItemValidationVo {
    private Long itemId;
    private String sourceTable;
    private String targetTable;
    private boolean passed;
    private String message;
    private TargetCompatibilityVo targetCompatibility;
}
