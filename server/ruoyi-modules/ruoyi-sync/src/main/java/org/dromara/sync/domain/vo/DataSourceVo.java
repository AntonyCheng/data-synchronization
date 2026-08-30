package org.dromara.sync.domain.vo;

import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;
import org.dromara.sync.domain.DataSource;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Data source response object. The password is intentionally omitted.
 */
@Data
@AutoMapper(target = DataSource.class)
public class DataSourceVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long sourceId;
    private String sourceName;
    private String sourceType;
    private String host;
    private Integer port;
    private String databaseName;
    private String schemaName;
    private String username;
    private String sslEnabled;
    private String status;
    private String remark;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
