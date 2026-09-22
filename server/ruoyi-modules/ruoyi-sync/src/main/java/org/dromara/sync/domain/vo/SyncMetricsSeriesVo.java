package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Metrics history of one task or task-group item over a trailing window. */
@Data
public class SyncMetricsSeriesVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String ownerType;
    private Long ownerId;
    /** Trailing window actually applied (after clamping). */
    private Integer windowMinutes;
    /** Newest sample on record regardless of the window; null when the owner was never polled. */
    private SyncMetricsSampleVo latest;
    /** Oldest first. Thinned evenly when the window holds more points than the configured cap. */
    private List<SyncMetricsSampleVo> samples = new ArrayList<>();
}
