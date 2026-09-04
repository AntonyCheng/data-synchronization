package org.dromara.sync.domain.bo;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class KafkaTopicCreateBo {
    @NotBlank(message = "topic 名称不能为空")
    private String topic;

    @Min(value = 1, message = "分区数必须大于 0")
    private Integer partitions = 1;

    @Min(value = 1, message = "副本数必须大于 0")
    private Short replicationFactor = 1;
}
