package org.dromara.sync.domain.vo;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serial;

/** Engine status of one table item inside a task group. */
@Data
@EqualsAndHashCode(callSuper = true)
public class SyncTaskGroupItemStatus extends EngineJobMetrics {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long itemId;
    private String sourceTable;
    private String engineJobId;
    private String engineStatus;
    private String status;
    private String errorMessage;
}
