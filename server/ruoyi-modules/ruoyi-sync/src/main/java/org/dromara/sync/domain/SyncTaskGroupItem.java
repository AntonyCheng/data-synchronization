package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** Table mapping inside a multi-table synchronization release. */
@Data
@TableName("ds_sync_task_group_item")
public class SyncTaskGroupItem {

    @TableId(value = "item_id", type = IdType.ASSIGN_ID)
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
    private String schemaSnapshot;
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
