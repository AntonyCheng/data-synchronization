package org.dromara.sync.domain.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SyncTaskGroupStatus {
    private Long groupId;
    private String status;
    private String message;
    private List<SyncTaskGroupItemStatus> items = new ArrayList<>();
}
