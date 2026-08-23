package org.dromara.demo.domain;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.dromara.common.encrypt.annotation.EncryptField;
import org.dromara.common.encrypt.enums.AlgorithmType;

/**
 * 测试加密字段实体。
 *
 * @author Lion Li
 */
@Data
@EqualsAndHashCode(callSuper = true)
@TableName("test_demo")
public class TestDemoEncrypt extends TestDemo {

    /**
     * key键
     */
    // Encryption keys are resolved from environment-backed application configuration.
    @EncryptField(algorithm = AlgorithmType.RSA)
    private String testKey;

    /**
     * 值
     */
    // @EncryptField // 什么也不写走默认yml配置
    @EncryptField(algorithm = AlgorithmType.AES)
    private String value;

}
