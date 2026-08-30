package org.dromara.sync.domain.vo;

import lombok.Data;

/** One source/target range result from a bounded consistency check. */
@Data
public class SyncTaskDataCheckBlockVo {

    private Integer index;
    private String lowerBound;
    private String upperBound;
    private Long sourceRows;
    private Long targetRows;
    private Long difference;
    private boolean matched;
    private boolean success;
    private String message;
}
