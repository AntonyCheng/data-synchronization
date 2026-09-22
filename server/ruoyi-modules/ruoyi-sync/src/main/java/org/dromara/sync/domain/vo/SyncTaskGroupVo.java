package org.dromara.sync.domain.vo;

import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;
import org.dromara.sync.domain.SyncTaskGroup;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/** Task group response object with its table items attached. */
@Data
@AutoMapper(target = SyncTaskGroup.class)
public class SyncTaskGroupVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

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
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private List<SyncTaskGroupItemVo> items = new ArrayList<>();
}
