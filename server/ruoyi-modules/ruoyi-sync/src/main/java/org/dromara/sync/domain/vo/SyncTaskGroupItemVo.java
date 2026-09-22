package org.dromara.sync.domain.vo;

import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;
import org.dromara.sync.domain.SyncTaskGroupItem;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/** Table item of a task group. The persisted schema snapshot body is intentionally not exposed. */
@Data
@AutoMapper(target = SyncTaskGroupItem.class)
public class SyncTaskGroupItemVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long itemId;
    private Long groupId;
    private String sourceDatabase;
    private String sourceTable;
    private String targetSchema;
    private String targetTable;
    private String primaryKeys;
    private String ddlPolicy;
    private String selectedColumns;
    private String syncKeyColumns;
    private String schemaHash;
    private String status;
    private String engineJobId;
    private String engineConfigHash;
    private String lastCheckpointId;
    private String lastCheckpointTime;
    private String lastCheckpointStatus;
    private Long lastCheckSourceRows;
    private Long lastCheckTargetRows;
    private Long lastCheckDifference;
    private String lastCheckMatched;
    private LocalDateTime lastCheckTime;
    private String lastCheckMessage;
    private String lastError;
}
