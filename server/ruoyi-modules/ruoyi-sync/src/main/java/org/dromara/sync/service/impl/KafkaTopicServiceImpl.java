package org.dromara.sync.service.impl;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.constant.DataSourceType;
import org.dromara.sync.domain.DataSource;
import org.dromara.sync.domain.bo.KafkaTopicCreateBo;
import org.dromara.sync.domain.vo.KafkaTopicVo;
import org.dromara.sync.kafka.KafkaAdminClients;
import org.dromara.sync.service.IDataSourceService;
import org.dromara.sync.service.IKafkaTopicService;
import org.dromara.sync.support.SyncText;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Kafka topic listing / creation for the wizard. Every admin call is bounded by an explicit timeout. */
@RequiredArgsConstructor
@Service
public class KafkaTopicServiceImpl implements IKafkaTopicService {

    private static final String RAW_TOPIC_PREFIX = "__ds_raw_";
    private static final long ADMIN_TIMEOUT_SECONDS = 10;

    private final IDataSourceService dataSourceService;

    @Override
    public List<KafkaTopicVo> listTopics(Long sourceId) {
        DataSource source = requireKafka(sourceId);
        try (AdminClient admin = KafkaAdminClients.open(source)) {
            List<String> names = new ArrayList<>(admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).stream()
                .filter(name -> !name.startsWith(RAW_TOPIC_PREFIX))
                .toList());
            names.sort(String::compareTo);
            Map<String, TopicDescription> descriptions = admin.describeTopics(names)
                .allTopicNames().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            List<KafkaTopicVo> result = new ArrayList<>();
            for (String name : names) {
                TopicDescription description = descriptions.get(name);
                if (description == null || description.partitions().isEmpty()) continue;
                result.add(toVo(name, description));
            }
            return result;
        } catch (Exception ex) {
            throw new ServiceException("读取 Kafka topic 列表失败：" + SyncText.safeMessage(ex, ex.getClass().getSimpleName()));
        }
    }

    @Override
    public KafkaTopicVo createTopic(Long sourceId, KafkaTopicCreateBo bo) {
        DataSource source = requireKafka(sourceId);
        String topicName = bo.getTopic().trim();
        if (!topicName.matches("[A-Za-z0-9._-]{1,249}")) {
            throw new ServiceException("topic 名称只能包含字母、数字、点、下划线和连字符，长度不超过 249");
        }
        int partitions = bo.getPartitions() == null ? 1 : bo.getPartitions();
        short replicationFactor = bo.getReplicationFactor() == null ? 1 : bo.getReplicationFactor();
        if (partitions < 1 || partitions > 1000) throw new ServiceException("分区数必须在 1 到 1000 之间");
        if (replicationFactor < 1 || replicationFactor > 100) throw new ServiceException("副本数必须在 1 到 100 之间");
        try (AdminClient admin = KafkaAdminClients.open(source)) {
            if (admin.listTopics().names().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).contains(topicName)) {
                throw new ServiceException("Kafka topic 已存在，请选择已有 topic 或更换名称");
            }
            admin.createTopics(List.of(new NewTopic(topicName, partitions, replicationFactor)))
                .all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            TopicDescription description = admin.describeTopics(List.of(topicName)).allTopicNames()
                .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topicName);
            if (description == null) throw new ServiceException("Kafka topic 创建后无法读取详情");
            return toVo(topicName, description);
        } catch (ServiceException ex) {
            throw ex;
        } catch (Exception ex) {
            if (ex.getCause() instanceof TopicExistsException) {
                throw new ServiceException("Kafka topic 已存在，请选择已有 topic 或更换名称");
            }
            throw new ServiceException("创建 Kafka topic 失败：" + SyncText.safeMessage(ex, ex.getClass().getSimpleName()));
        }
    }

    private DataSource requireKafka(Long sourceId) {
        DataSource source = dataSourceService.requireById(sourceId, "");
        if (!DataSourceType.isKafka(source)) throw new ServiceException("该数据源不是 Kafka");
        return source;
    }

    private static KafkaTopicVo toVo(String name, TopicDescription description) {
        KafkaTopicVo topic = new KafkaTopicVo();
        topic.setTopic(name);
        topic.setPartitions(description.partitions().size());
        topic.setReplicationFactor((short) description.partitions().getFirst().replicas().size());
        return topic;
    }
}
