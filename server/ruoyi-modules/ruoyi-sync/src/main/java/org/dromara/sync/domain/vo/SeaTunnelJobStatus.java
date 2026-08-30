package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Platform-safe projection of a SeaTunnel job and its latest checkpoint. */
@Data
public class SeaTunnelJobStatus implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long taskId;
    private String engineJobId;
    private String engineStatus;
    private String status;
    private String errorMessage;
    private String lastCheckpointId;
    private LocalDateTime lastCheckpointTime;
    private String lastCheckpointStatus;
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
