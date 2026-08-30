package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Target table compatibility result for an MVP single-table task. */
@Data
public class TargetCompatibilityVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long taskId;
    private String sourceTable;
    private String targetTable;
    private boolean targetExists;
    private boolean passed;
    private String message;
    private List<DataSourceCheckItemVo> checks = new ArrayList<>();
}
