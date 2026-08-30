package org.dromara.sync.domain.bo;

import lombok.Data;

/** Optional parameters for the read-only data consistency check. */
@Data
public class SyncTaskDataCheckRequest {

    /** COUNT keeps the original whole-table check; KEY_RANGE enables bounded checks. */
    private String mode = "COUNT";
    /** Numeric sync-key range width used by KEY_RANGE mode. */
    private Integer blockSize = 10000;
    /** Reject checks for a moving CDC task unless the caller has paused it. */
    private Boolean strictWatermark = true;
}
