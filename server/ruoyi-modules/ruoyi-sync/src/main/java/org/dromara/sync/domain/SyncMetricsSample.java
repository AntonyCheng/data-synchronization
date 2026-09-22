package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * One engine metrics sample of a task or task-group item, appended on every successful
 * status poll. Counters are the engine's cumulative values at that instant; the QPS
 * figures are the engine's own rate metrics.
 */
@Data
@TableName("ds_sync_metrics_sample")
public class SyncMetricsSample {

    public static final String OWNER_TASK = "TASK";
    public static final String OWNER_GROUP_ITEM = "GROUP_ITEM";

    @TableId(value = "sample_id", type = IdType.ASSIGN_ID)
    private Long sampleId;
    private String ownerType;
    private Long ownerId;
    private Long groupId;
    private String engineJobId;
    private String engineStatus;
    private String phase;
    private Long sourceReceivedCount;
    private Long sinkCommittedCount;
    private Long sourceReceivedBytes;
    private Long sinkCommittedBytes;
    private Double sourceQps;
    private Double sinkQps;
    private Long backlogRows;
    private Long cdcLagSeconds;
    private LocalDateTime sampledAt;
}
