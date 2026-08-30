package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** Immutable, credential-free snapshot of task configuration semantics. */
@Data
@TableName("ds_sync_task_config_version")
public class SyncTaskConfigVersion {

    @TableId(value = "version_id", type = IdType.ASSIGN_ID)
    private Long versionId;
    private Long taskId;
    private Integer configVersion;
    private String configSnapshot;
    private LocalDateTime createTime;
}
