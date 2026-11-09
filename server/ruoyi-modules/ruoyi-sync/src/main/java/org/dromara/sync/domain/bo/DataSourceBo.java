package org.dromara.sync.domain.bo;

import io.github.linpeilie.annotations.AutoMapper;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;
import org.dromara.sync.domain.DataSource;

import java.io.Serial;
import java.io.Serializable;

/**
 * Data source request object.
 */
@Data
@AutoMapper(target = DataSource.class, reverseConvertGenerate = false)
public class DataSourceBo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Long sourceId;

    @NotBlank(message = "数据源名称不能为空")
    @Size(max = 100, message = "数据源名称不能超过{max}个字符")
    private String sourceName;

    @NotBlank(message = "数据源类型不能为空")
    private String sourceType;

    @NotBlank(message = "主机地址不能为空")
    private String host;

    @NotNull(message = "端口不能为空")
    @Min(value = 1, message = "端口必须大于0")
    @Max(value = 65535, message = "端口不能超过65535")
    private Integer port;

    private String databaseName;

    private String schemaName;

    private String username;

    @Size(max = 512, message = "密码不能超过{max}个字符")
    private String password;

    private String sslEnabled;

    /** MySQL only: IANA id such as {@code UTC} or {@code Asia/Shanghai}; blank = compatibility mode (Asia/Shanghai). */
    @Size(max = 64, message = "服务器时区不能超过{max}个字符")
    private String serverTimeZone;

    /** Kafka only: PLAINTEXT (or blank), SASL_PLAINTEXT, SASL_SSL, SSL. */
    @Size(max = 20, message = "Kafka 安全协议不能超过{max}个字符")
    private String kafkaSecurityProtocol;

    /** Kafka SASL only: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. */
    @Size(max = 20, message = "SASL 机制不能超过{max}个字符")
    private String kafkaSaslMechanism;

    /** Kafka SSL only: CA certificate(s), PEM. */
    @Size(max = 32768, message = "CA 证书不能超过{max}个字符")
    private String kafkaSslCaCert;

    /** Kafka SSL only: client certificate chain, PEM. */
    @Size(max = 32768, message = "客户端证书不能超过{max}个字符")
    private String kafkaSslClientCert;

    /** Kafka SSL only: client private key, unencrypted PKCS#8 PEM; blank on edit keeps the stored one. */
    @Size(max = 16384, message = "客户端私钥不能超过{max}个字符")
    private String kafkaSslClientKey;

    /** Kafka SSL only: "0" turns the broker host-name check off. */
    private String kafkaSslVerifyHostname;

    private String status;
    private String remark;
}
