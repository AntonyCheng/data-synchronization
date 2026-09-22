package org.dromara.sync.domain.vo;

import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;
import org.dromara.sync.domain.SyncMetricsSample;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** One point of a metrics series. */
@Data
@AutoMapper(target = SyncMetricsSample.class)
public class SyncMetricsSampleVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private LocalDateTime sampledAt;
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
}
