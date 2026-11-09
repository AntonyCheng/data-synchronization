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
    /** MySQL only; blank = compatibility mode (Asia/Shanghai). */
    private String serverTimeZone;
    /** Kafka only; blank = PLAINTEXT. The client private key is never returned. */
    private String kafkaSecurityProtocol;
    private String kafkaSaslMechanism;
    private String kafkaSslCaCert;
    private String kafkaSslClientCert;
    private String kafkaSslVerifyHostname;
    private String status;
    private String remark;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
