package org.dromara.sync.domain.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/** Aggregated read-only row-count comparison for every table in a task group. */
@Data
public class SyncTaskGroupDataCheckResult {

    private Long groupId;
    private int tableCount;
    private int matchedTableCount;
    private int mismatchedTableCount;
    private int failedTableCount;
    private boolean matched;
    private boolean success;
    private String message;
    private String consistencyNote;
    private List<SyncTaskGroupDataCheckItemResult> items = new ArrayList<>();
}
