package org.dromara.sync.domain.bo;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class SyncTaskGroupBo implements Serializable {

    private Long groupId;

    @NotBlank(message = "任务组名称不能为空")
    @Size(max = 100, message = "任务组名称不能超过{max}个字符")
    private String groupName;

    @NotNull(message = "源数据源不能为空")
    private Long sourceId;

    @NotNull(message = "目标数据源不能为空")
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

    @Valid
    private List<SyncTaskGroupItemBo> items = new ArrayList<>();
}
