package org.dromara.sync.domain.vo;

import lombok.Data;

@Data
public class SyncTaskGroupConfigPreview {
    private Long groupId;
    private String groupName;
    private Integer configVersion;
    private String engineJobName;
    private int tableCount;
    private String config;
}
