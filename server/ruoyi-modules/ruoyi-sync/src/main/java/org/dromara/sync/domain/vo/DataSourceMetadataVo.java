package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Table metadata used before a sync task is created. */
@Data
public class DataSourceMetadataVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long sourceId;
    private String sourceType;
    private String databaseName;
    private String tableName;
    private String charset;
    private String collation;
    private List<DataSourceColumnVo> columns = new ArrayList<>();
    private List<String> primaryKeys = new ArrayList<>();
    private List<DataSourceIndexVo> uniqueKeys = new ArrayList<>();
}
