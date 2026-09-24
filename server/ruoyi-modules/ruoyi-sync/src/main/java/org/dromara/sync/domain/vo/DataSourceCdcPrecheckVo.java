package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** MySQL binlog CDC prerequisite report. */
@Data
public class DataSourceCdcPrecheckVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long sourceId;
    private String sourceType;
    private String serverId;
    private String gtidMode;
    private String binlogRetention;
    /** The server's time zone against the data source's (the {@code timezone} check item is derived from it). */
    private DataSourceTimeZoneVo timeZone;
    private Boolean passed;
    private String message;
    private List<DataSourceCheckItemVo> checks = new ArrayList<>();
}
