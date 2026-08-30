package org.dromara.sync.domain.vo;

import lombok.Data;

/** Result of encrypting existing data-source credentials. */
@Data
public class DataSourceCredentialMigrationResult {

    private boolean enabled;
    private int total;
    private int migrated;
    private String message;
}
