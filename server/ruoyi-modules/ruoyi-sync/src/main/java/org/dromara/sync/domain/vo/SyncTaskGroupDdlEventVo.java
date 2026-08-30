package org.dromara.sync.domain.vo;

import lombok.Data;

import java.time.LocalDateTime;

/** DDL event safe to display in the task-group UI. */
@Data
public class SyncTaskGroupDdlEventVo {
    private Long eventId;
    private Long itemId;
    private String sourceTable;
    private String targetTable;
    private String changeType;
    private String riskLevel;
    private String status;
    private String details;
    private String remediation;
    private LocalDateTime detectedAt;
}
