package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/** Safe-to-display representation of a generated SeaTunnel job. */
@Data
public class SeaTunnelJobConfigPreview implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long taskId;
    private String jobName;
    private String sourceTable;
    private String targetTable;
    private List<String> primaryKeys;
    private String config;
}
