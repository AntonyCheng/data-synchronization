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
    private String status;
    private String remark;
}
