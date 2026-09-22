package org.dromara.sync.domain.vo;

import lombok.Data;
import org.dromara.sync.domain.SyncTaskGroup;

@Data
public class SyncTaskGroupOperationResult {
    private Long groupId;
    private String status;
    private String engineJobIds;
    private String message;

    public static SyncTaskGroupOperationResult of(SyncTaskGroup group, String message) {
        SyncTaskGroupOperationResult result = new SyncTaskGroupOperationResult();
        result.setGroupId(group.getGroupId());
        result.setStatus(group.getStatus());
        result.setEngineJobIds(group.getEngineJobId());
        result.setMessage(message);
        return result;
    }
}
