package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.dromara.common.mybatis.core.domain.BaseEntity;

import java.time.LocalDateTime;

/**
 * Single-table synchronization task metadata.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("ds_sync_task")
public class SyncTask extends BaseEntity {

    @TableId(value = "task_id", type = IdType.ASSIGN_ID)
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
    @TableField("full_data_mode")
    private String fullDataMode;
    private String ddlPolicy;
    private String scheduleMode;
    private String cronExpression;
    // Global mybatis-plus field-strategy is not_null, which would silently drop a
    // setNextRunTime(null) from any updateById() UPDATE statement - the very thing
    // SyncTaskScheduler.advance() and a manual stop() rely on to make a ONCE task (or a
    // just-stopped task) stop being schedule-eligible. Force nulls to actually persist.
    @TableField(updateStrategy = FieldStrategy.ALWAYS)
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
    private Long kafkaPublishedCount;
    private Integer kafkaLastPartition;
    private Long kafkaLastOffset;
    private LocalDateTime kafkaLastSourceEventTime;
    private LocalDateTime kafkaLastBrokerAckTime;
    private Long kafkaLagSeconds;
    private Long lastCheckSourceRows;
    private Long lastCheckTargetRows;
    private Long lastCheckDifference;
    private String lastCheckMatched;
    private LocalDateTime lastCheckTime;
    private String lastCheckMessage;
    private String lastError;
}
