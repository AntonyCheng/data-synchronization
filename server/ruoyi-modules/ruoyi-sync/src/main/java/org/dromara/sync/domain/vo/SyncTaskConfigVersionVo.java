package org.dromara.sync.domain.vo;

import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;
import org.dromara.sync.domain.SyncTaskConfigVersion;

import java.time.LocalDateTime;

@Data
@AutoMapper(target = SyncTaskConfigVersion.class)
public class SyncTaskConfigVersionVo {
    private Long versionId;
    private Long taskId;
    private Integer configVersion;
    private String configSnapshot;
    private LocalDateTime createTime;
}
