package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/** Column metadata exposed to the task creation flow. */
@Data
public class DataSourceColumnVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String name;
    private String typeName;
    private Integer jdbcType;
    private Integer size;
    private Integer scale;
    private Boolean nullable;
    private Boolean autoIncrement;
    private String defaultValue;
}
