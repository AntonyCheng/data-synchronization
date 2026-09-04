package org.dromara.sync.domain.vo;

import lombok.Data;

@Data
public class KafkaTopicVo {
    private String topic;
    private int partitions;
    private short replicationFactor;
}
