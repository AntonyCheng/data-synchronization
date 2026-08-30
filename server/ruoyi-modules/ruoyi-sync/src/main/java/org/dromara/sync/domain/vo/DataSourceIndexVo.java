package org.dromara.sync.domain.vo;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** A unique index candidate used for CDC key validation. */
@Data
public class DataSourceIndexVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String name;
    private Boolean unique;
    private List<String> columns = new ArrayList<>();
    private Boolean allNotNull;
}
