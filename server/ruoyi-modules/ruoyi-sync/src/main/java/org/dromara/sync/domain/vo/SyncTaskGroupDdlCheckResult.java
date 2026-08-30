package org.dromara.sync.domain.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/** Result of checking all table schemas in a task group. */
@Data
public class SyncTaskGroupDdlCheckResult {
    private Long groupId;
    private String status;
    private String message;
    private List<SyncTaskGroupDdlEventVo> events = new ArrayList<>();
}
