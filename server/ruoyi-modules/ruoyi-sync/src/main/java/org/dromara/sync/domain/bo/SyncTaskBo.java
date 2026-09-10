package org.dromara.sync.domain.bo;

import io.github.linpeilie.annotations.AutoMapper;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;
import org.dromara.sync.domain.SyncTask;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Synchronization task request object.
 */
@Data
@AutoMapper(target = SyncTask.class, reverseConvertGenerate = false)
public class SyncTaskBo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long taskId;

    @NotBlank(message = "任务名称不能为空")
    @Size(max = 100, message = "任务名称不能超过{max}个字符")
    private String taskName;

    @NotNull(message = "源数据源不能为空")
    private Long sourceId;

    @NotNull(message = "目标数据源不能为空")
    private Long targetId;

    @NotBlank(message = "源表不能为空")
    private String sourceTable;

    private String targetSchema;

    @NotBlank(message = "目标表不能为空")
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
    private Integer configVersion;
    private String selectedColumns;
    private String syncKeyColumns;
    private String kafkaOutputFormat;
    private Integer readLimitRowsPerSecond;
    private Long readLimitBytesPerSecond;
    private Integer snapshotParallelism;
    private Integer sourceConnectionLimit;
    private String status;
}
