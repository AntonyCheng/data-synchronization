package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.dromara.common.mybatis.core.domain.BaseEntity;

/** A versioned multi-table synchronization release. */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("ds_sync_task_group")
public class SyncTaskGroup extends BaseEntity {

    @TableId(value = "group_id", type = IdType.ASSIGN_ID)
    private Long groupId;
    private String groupName;
    private Long sourceId;
    private Long targetId;
    private String syncScope;
    private String sourceDatabase;
    private String autoDiscover;
    private String syncMode;
    private String ddlPolicy;
    private Integer readLimitRowsPerSecond;
    private Long readLimitBytesPerSecond;
    private Integer snapshotParallelism;
    private Integer sourceConnectionLimit;
    private String status;
    private Integer configVersion;
    private String engineJobId;
    private String engineConfigHash;
    private String lastCheckpointId;
    private String lastCheckpointTime;
    private String lastCheckpointStatus;
    private String lastError;
}
