package org.dromara.sync.domain.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SyncTaskGroupValidationResult {
    private Long groupId;
    private boolean valid;
    private String message;
    private ConnectionTestResult source;
    private ConnectionTestResult target;
    private DataSourceCdcPrecheckVo cdcPrecheck;
    private List<SyncTaskGroupItemValidationVo> items = new ArrayList<>();
}
