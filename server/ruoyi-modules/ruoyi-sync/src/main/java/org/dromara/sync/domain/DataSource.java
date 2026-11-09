package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.dromara.common.mybatis.core.domain.BaseEntity;
import org.dromara.common.encrypt.annotation.EncryptField;
import org.dromara.common.encrypt.enums.AlgorithmType;

/**
 * Data source metadata.
 */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("ds_data_source")
public class DataSource extends BaseEntity {

    @TableId(value = "source_id", type = IdType.ASSIGN_ID)
    private Long sourceId;

    private String sourceName;
    private String sourceType;
    private String host;
    private Integer port;
    private String databaseName;
    private String schemaName;
    private String username;

    /** Stored encrypted when SYNC_CREDENTIAL_ENCRYPTION_ENABLED is enabled. */
    @EncryptField(algorithm = AlgorithmType.AES)
    private String password;
    private String sslEnabled;

    /**
     * MySQL only: the IANA zone the server renders {@code TIMESTAMP} in, told to the MySQL-CDC
     * source as {@code server-time-zone}. Blank = compatibility mode ({@code Asia/Shanghai}),
     * see {@code SourceTimeZones}.
     */
    private String serverTimeZone;
    /** Kafka only: SASL_PLAINTEXT, SASL_SSL or SSL; blank = PLAINTEXT. See {@code KafkaSecurity}. */
    private String kafkaSecurityProtocol;
    /** Kafka SASL only: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512; it authenticates with the username and password. */
    private String kafkaSaslMechanism;
    /** Kafka SSL only: the CA certificate(s) the brokers are checked against (PEM); blank = the JVM's default trust store. */
    private String kafkaSslCaCert;
    /** Kafka SSL only: the client certificate chain (PEM) for brokers that require client authentication. */
    private String kafkaSslClientCert;
    /** Kafka SSL only: the client certificate's private key (unencrypted PKCS#8 PEM). Stored encrypted like the password, never returned. */
    @EncryptField(algorithm = AlgorithmType.AES)
    private String kafkaSslClientKey;
    /** Kafka SSL only: "0" turns the broker host-name check off; "1" (default) keeps it. */
    private String kafkaSslVerifyHostname;
    private String status;
    private String remark;
}
