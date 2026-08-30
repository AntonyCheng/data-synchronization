package org.dromara.sync.domain.vo;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class SyncTaskGroupItemVo {
    private Long itemId;
    private Long groupId;
    private String sourceDatabase;
    private String sourceTable;
    private String targetSchema;
    private String targetTable;
    private String primaryKeys;
    private String ddlPolicy;
    private String selectedColumns;
    private String syncKeyColumns;
    private String schemaHash;
    private String status;
    private String engineJobId;
    private String engineConfigHash;
    private String lastCheckpointId;
    private String lastCheckpointTime;
    private String lastCheckpointStatus;
    private Long lastCheckSourceRows;
    private Long lastCheckTargetRows;
    private Long lastCheckDifference;
    private String lastCheckMatched;
    private LocalDateTime lastCheckTime;
    private String lastCheckMessage;
    private String lastError;
}
