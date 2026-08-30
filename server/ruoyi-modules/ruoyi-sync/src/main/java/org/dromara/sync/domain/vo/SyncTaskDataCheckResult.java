package org.dromara.sync.domain.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/** Result of a source/target row-count comparison. */
@Data
public class SyncTaskDataCheckResult {

    private Long taskId;
    private String sourceTable;
    private String targetTable;
    private Long sourceRows;
    private Long targetRows;
    private Long difference;
    private boolean matched;
    private boolean success;
    private String message;
    private String checkMode;
    private Integer blockSize;
    private Integer totalBlocks;
    private Integer matchedBlocks;
    private Integer mismatchedBlocks;
    private Integer failedBlocks;
    private String watermarkMessage;
    private List<SyncTaskDataCheckBlockVo> blocks = new ArrayList<>();
}
