package org.dromara.sync.domain.vo;

import lombok.Data;

@Data
public class SyncTaskGroupItemStatus {
    private Long itemId;
    private String sourceTable;
    private String engineJobId;
    private String engineStatus;
    private String status;
    private String errorMessage;
    private String phase;
    private Long sourceReceivedCount;
    private Long sinkCommittedCount;
    private Long sourceReceivedBytes;
    private Long sinkCommittedBytes;
    private Double sourceQps;
    private Double sinkQps;
    private Long cdcLagSeconds;
    private String metricsMessage;
}
