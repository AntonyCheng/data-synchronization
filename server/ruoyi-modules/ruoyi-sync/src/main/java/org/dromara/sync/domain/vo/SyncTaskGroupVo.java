package org.dromara.sync.domain.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SyncTaskGroupVo {
    private Long groupId;
    private String groupName;
    private Long sourceId;
    private Long targetId;
    private String syncScope;
    private String sourceDatabase;
    private String autoDiscover;
    private String syncMode;
    private String ddlPolicy;
    private String kafkaOutputFormat;
    private Integer readLimitRowsPerSecond;
    private Long readLimitBytesPerSecond;
    private Integer snapshotParallelism;
    private Integer sourceConnectionLimit;
    private String status;
    private Integer configVersion;
    private String engineJobId;
    private String engineConfigHash;
    private String lastCheckpointId;
    private String lastCheckpointTime;
    private String lastCheckpointStatus;
    private String lastError;
    private String createTime;
    private String updateTime;
    private List<SyncTaskGroupItemVo> items = new ArrayList<>();
}
