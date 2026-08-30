package org.dromara.sync.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.dromara.common.encrypt.properties.EncryptorProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Fails fast when data-source credential encryption is enabled without a valid AES key. */
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(value = "mybatis-encryptor.enable", havingValue = "true")
public class CredentialEncryptionValidator {

    private final EncryptorProperties properties;

    @PostConstruct
    public void validate() {
        String password = properties.getPassword();
        if (password == null || password.isBlank()
            || (password.length() != 16 && password.length() != 24 && password.length() != 32)) {
            throw new IllegalStateException("SYNC_CREDENTIAL_ENCRYPTION_PASSWORD 必须是 16、24 或 32 个字符");
        }
    }
}
