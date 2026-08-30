package org.dromara.sync.domain.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

/** One CDC prerequisite result. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataSourceCheckItemVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String code;
    private String label;
    private Boolean required;
    private Boolean passed;
    private String actual;
    private String message;
    private String suggestion;

    public DataSourceCheckItemVo(String code, String label, Boolean required, Boolean passed, String actual, String message) {
        this.code = code;
        this.label = label;
        this.required = required;
        this.passed = passed;
        this.actual = actual;
        this.message = message;
    }
}
