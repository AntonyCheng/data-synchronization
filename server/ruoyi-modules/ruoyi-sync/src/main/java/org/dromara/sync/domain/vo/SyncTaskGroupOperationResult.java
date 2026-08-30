package org.dromara.sync.domain.vo;

import lombok.Data;

@Data
public class SyncTaskGroupOperationResult {
    private Long groupId;
    private String status;
    private String engineJobIds;
    private String message;
}
