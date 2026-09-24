package org.dromara.sync.domain.vo;

import lombok.Data;
import org.dromara.sync.domain.SyncTaskGroup;

@Data
public class SyncTaskGroupOperationResult {
    private Long groupId;
    private String status;
    private String engineJobIds;
    private String message;
    /**
     * The operation went through, but not for every table (a savepoint request, resume or stop
     * the engine refused for some of them); {@code message} names them. The group status alone
     * cannot say this - a group whose pause half-failed is still PAUSING.
     */
    private boolean partial;

    public static SyncTaskGroupOperationResult of(SyncTaskGroup group, String message) {
        SyncTaskGroupOperationResult result = new SyncTaskGroupOperationResult();
        result.setGroupId(group.getGroupId());
        result.setStatus(group.getStatus());
        result.setEngineJobIds(group.getEngineJobId());
        result.setMessage(message);
        return result;
    }

    public static SyncTaskGroupOperationResult partial(SyncTaskGroup group, String message) {
        SyncTaskGroupOperationResult result = of(group, message);
        result.setPartial(true);
        return result;
    }
}
