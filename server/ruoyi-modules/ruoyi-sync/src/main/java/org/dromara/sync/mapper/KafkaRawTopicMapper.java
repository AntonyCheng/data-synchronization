package org.dromara.sync.mapper;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import org.dromara.common.mybatis.core.mapper.BaseMapperPlus;
import org.dromara.sync.domain.KafkaRawTopic;
import org.springframework.dao.DuplicateKeyException;

import java.time.LocalDateTime;

/** Registry of the raw topics the platform may delete; internal to the bridge and its janitor, so no VO. */
public interface KafkaRawTopicMapper extends BaseMapperPlus<KafkaRawTopic, KafkaRawTopic> {

    /**
     * Records the topic unless it is already registered on that data source. Two instances starting
     * the same owner's bridge may race; the loser hits the unique key, which is just as good.
     * Returns whether this call added the row.
     */
    default boolean registerIfAbsent(KafkaRawTopic topic) {
        if (exists(new LambdaQueryWrapper<KafkaRawTopic>()
            .eq(KafkaRawTopic::getDataSourceId, topic.getDataSourceId())
            .eq(KafkaRawTopic::getTopicName, topic.getTopicName()))) return false;
        try {
            return insert(topic) > 0;
        } catch (DuplicateKeyException ex) {
            return false;
        }
    }

    default int markRetired(Long rawTopicId, LocalDateTime retiredTime) {
        return update(null, new LambdaUpdateWrapper<KafkaRawTopic>()
            .eq(KafkaRawTopic::getRawTopicId, rawTopicId)
            .set(KafkaRawTopic::getRetiredTime, retiredTime));
    }

    default int markDeleted(Long rawTopicId, LocalDateTime deletedTime) {
        return update(null, new LambdaUpdateWrapper<KafkaRawTopic>()
            .eq(KafkaRawTopic::getRawTopicId, rawTopicId)
            .set(KafkaRawTopic::getDeletedTime, deletedTime));
    }

    /** Back in use: clears both marks explicitly ({@code updateById} would skip the nulls). */
    default int markInUse(Long rawTopicId) {
        return update(null, new LambdaUpdateWrapper<KafkaRawTopic>()
            .eq(KafkaRawTopic::getRawTopicId, rawTopicId)
            .set(KafkaRawTopic::getRetiredTime, null)
            .set(KafkaRawTopic::getDeletedTime, null));
    }

    default int deleteByDataSource(Long dataSourceId) {
        return delete(new LambdaQueryWrapper<KafkaRawTopic>().eq(KafkaRawTopic::getDataSourceId, dataSourceId));
    }
}
