package org.dromara.sync.domain.bo;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.io.Serializable;

@Data
public class SyncTaskGroupItemBo implements Serializable {

    private Long itemId;
    private String sourceDatabase;

    @NotBlank(message = "源表不能为空")
    @Size(max = 255, message = "源表名不能超过{max}个字符")
    private String sourceTable;

    private String targetSchema;

    @NotBlank(message = "目标表不能为空")
    @Size(max = 255, message = "目标表名不能超过{max}个字符")
    private String targetTable;

    private String primaryKeys;
    private String ddlPolicy;
    private String selectedColumns;
    private String syncKeyColumns;
}
