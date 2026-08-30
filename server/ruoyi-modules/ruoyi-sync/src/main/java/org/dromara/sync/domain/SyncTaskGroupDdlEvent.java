package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** Persisted schema-change event for one isolated table item. */
@Data
@TableName("ds_sync_task_group_ddl_event")
public class SyncTaskGroupDdlEvent {

    @TableId(value = "event_id", type = IdType.ASSIGN_ID)
    private Long eventId;
    private Long groupId;
    private Long itemId;
    private String changeType;
    private String riskLevel;
    private String status;
    private String details;
    private String remediation;
    private String sourceSchemaHash;
    private LocalDateTime detectedAt;
    private LocalDateTime resolvedAt;
}
