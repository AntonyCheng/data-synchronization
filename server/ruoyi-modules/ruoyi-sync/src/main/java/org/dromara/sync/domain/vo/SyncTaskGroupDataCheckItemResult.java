package org.dromara.sync.domain.vo;

import lombok.Data;

/** Latest row-count consistency result for one table in a task group. */
@Data
public class SyncTaskGroupDataCheckItemResult {

    private Long itemId;
    private String sourceTable;
    private String targetTable;
    private Long sourceRows;
    private Long targetRows;
    private Long difference;
    private boolean matched;
    private boolean success;
    private String message;
}
