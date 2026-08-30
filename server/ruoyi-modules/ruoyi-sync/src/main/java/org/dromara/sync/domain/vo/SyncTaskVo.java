package org.dromara.sync.domain.vo;

import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;
import org.dromara.sync.domain.SyncTask;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Synchronization task response object.
 */
@Data
@AutoMapper(target = SyncTask.class)
public class SyncTaskVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long taskId;
    private String taskName;
    private Long sourceId;
    private Long targetId;
    private String sourceTable;
    private String targetSchema;
    private String targetTable;
    private String syncMode;
    private String incrementalStartupMode;
    private LocalDateTime incrementalStartupTimestamp;
    private String incrementalStartupBinlogFile;
    private Long incrementalStartupBinlogPosition;
    private String fullDataMode;
    private String ddlPolicy;
    private String scheduleMode;
    private String cronExpression;
    private LocalDateTime nextRunTime;
    private LocalDateTime lastTriggerTime;
    private String lastSkipReason;
    private Integer configVersion;
    private String overwriteStageTable;
    private String selectedColumns;
    private String syncKeyColumns;
    private Integer readLimitRowsPerSecond;
    private Long readLimitBytesPerSecond;
    private Integer snapshotParallelism;
    private Integer sourceConnectionLimit;
    private String status;
    private String engineJobId;
    private String engineConfigHash;
    private String lastCheckpointId;
    private LocalDateTime lastCheckpointTime;
    private String lastCheckpointStatus;
    private Long lastCheckSourceRows;
    private Long lastCheckTargetRows;
    private Long lastCheckDifference;
    private String lastCheckMatched;
    private LocalDateTime lastCheckTime;
    private String lastCheckMessage;
    private String lastError;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
