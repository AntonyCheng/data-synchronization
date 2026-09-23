package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/**
 * What a picker needs to offer a data source: enough to label it and to decide whether it may
 * be a source or a target. Deliberately excludes host, port and credentials.
 */
@Data
public class DataSourceOptionVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long sourceId;
    private String sourceName;
    private String sourceType;
    private String databaseName;
    /** '0' = enabled, same vocabulary as the list view. */
    private String status;
}
