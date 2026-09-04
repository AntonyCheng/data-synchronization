package org.dromara.sync.domain.bo;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("dev")
class SyncTaskGroupBoValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void wholeDatabaseGroupAllowsEmptyItems() {
        SyncTaskGroupBo request = baseRequest();
        request.setSyncScope("DATABASE");

        assertThat(validator.validate(request)).isEmpty();
    }

    @Test
    void multiTableGroupDefersEmptyItemValidationToService() {
        SyncTaskGroupBo request = baseRequest();
        request.setSyncScope("MULTI_TABLE");

        assertThat(validator.validate(request)).isEmpty();
    }

    private SyncTaskGroupBo baseRequest() {
        SyncTaskGroupBo request = new SyncTaskGroupBo();
        request.setGroupName("validation-test");
        request.setSourceId(1L);
        request.setTargetId(2L);
        return request;
    }
}
