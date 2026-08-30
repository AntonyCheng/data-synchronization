package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;

/** Result returned after a SeaTunnel lifecycle operation. */
@Data
public class SeaTunnelJobOperationResult implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long taskId;
    private String engineJobId;
    private String status;
    private String message;
}
