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
    private String status;
    private String remark;
}
