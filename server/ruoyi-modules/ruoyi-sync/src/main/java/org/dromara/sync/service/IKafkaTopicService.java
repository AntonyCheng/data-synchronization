package org.dromara.sync.service;

import org.dromara.sync.domain.bo.KafkaTopicCreateBo;
import org.dromara.sync.domain.vo.KafkaTopicVo;

import java.util.List;

/** Topic administration on a Kafka data source, used by the task wizard to pick or create the output topic. */
public interface IKafkaTopicService {

    /** User-visible topics (the platform's private {@code __ds_raw_*} buffers are hidden). */
    List<KafkaTopicVo> listTopics(Long sourceId);

    KafkaTopicVo createTopic(Long sourceId, KafkaTopicCreateBo bo);
}
