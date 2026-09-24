package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/** Server-side limits the task-group wizard mirrors (the server enforces them regardless). */
@Data
public class SyncTaskGroupLimitsVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** {@code sync.group.max-tables}, clamped: most tables one group may hold, explicit or discovered. */
    private int maxTables;
}
