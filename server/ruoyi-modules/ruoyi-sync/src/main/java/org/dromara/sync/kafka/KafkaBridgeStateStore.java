package org.dromara.sync.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dromara.sync.domain.KafkaRawTopic;
import org.dromara.sync.mapper.KafkaRawTopicMapper;
import org.dromara.sync.mapper.SyncTaskGroupItemMapper;
import org.dromara.sync.mapper.SyncTaskMapper;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * What the Kafka bridge keeps in the metadata database: the raw topic registry rows
 * ({@code ds_kafka_raw_topic}) with the raw offset it committed, and - on the task / table item row -
 * why its bridge stopped publishing, which the status refresh, the VOs and {@code SyncAlertNotifier}
 * read. Writes the bridge can live without are best effort: a metadata database hiccup must not stop
 * a bridge that publishes fine.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaBridgeStateStore {

    private final KafkaRawTopicMapper rawTopicMapper;
    private final SyncTaskMapper taskMapper;
    private final SyncTaskGroupItemMapper itemMapper;

    /** What the platform knows of a bridge's position on a raw topic; see {@link KafkaRawTopic#getBridgeOffset()}. */
    public record RawPosition(Long offset, boolean resetAllowed) {
    }

    /** Registers the raw topic for the janitor; returns whether this call added the row. Throws when the database does. */
    boolean register(KafkaRawTopic row) {
        return rawTopicMapper.registerIfAbsent(row);
    }

    /**
     * The registry's record of the bridge position on {@code topic}. Throws when the database cannot be
     * read: deciding "no record" on a hiccup could report a gap that is not there.
     */
    RawPosition rawPosition(Long dataSourceId, String topic) {
        KafkaRawTopic row = rawTopicMapper.selectByTopic(dataSourceId, topic);
        return row == null ? new RawPosition(null, false) : new RawPosition(row.getBridgeOffset(), "1".equals(row.getResetAllowed()));
    }

    void saveOffset(Long dataSourceId, String topic, long offset) {
        try {
            rawTopicMapper.updateBridgeOffset(dataSourceId, topic, offset);
        } catch (RuntimeException ex) {
            log.warn("bridge offset {} of raw topic {} not recorded: {}", offset, topic, ex.getMessage());
        }
    }

    /** A fresh run (start / reinitialize) is about to begin on {@code topic}; see {@link KafkaRawTopic#getResetAllowed()}. */
    void allowReset(Long dataSourceId, String topic) {
        try {
            rawTopicMapper.updateResetAllowed(dataSourceId, topic, true);
        } catch (RuntimeException ex) {
            log.warn("fresh-run mark of raw topic {} not recorded: {}", topic, ex.getMessage());
        }
    }

    /** The fresh run's bridge has positioned and committed; later positions are checked strictly again. */
    void consumeReset(Long dataSourceId, String topic) {
        try {
            rawTopicMapper.updateResetAllowed(dataSourceId, topic, false);
        } catch (RuntimeException ex) {
            log.warn("fresh-run mark of raw topic {} not cleared: {}", topic, ex.getMessage());
        }
    }

    /** {@code backlog}: raw records behind the stop at this attempt, null when Kafka could not say. */
    void reportBlocked(String ownerType, Long ownerId, String error, LocalDateTime since, Long backlog) {
        try {
            if (KafkaRawTopic.OWNER_GROUP_ITEM.equals(ownerType)) itemMapper.updateKafkaBridgeError(ownerId, error, since, backlog);
            else taskMapper.updateKafkaBridgeError(ownerId, error, since, backlog);
        } catch (RuntimeException ex) {
            log.warn("kafka bridge stop of {} {} not recorded: {}", ownerType, ownerId, ex.getMessage());
        }
    }

    void clearBlocked(String ownerType, Long ownerId) {
        try {
            if (KafkaRawTopic.OWNER_GROUP_ITEM.equals(ownerType)) itemMapper.clearKafkaBridgeError(ownerId);
            else taskMapper.clearKafkaBridgeError(ownerId);
        } catch (RuntimeException ex) {
            log.warn("kafka bridge stop of {} {} not cleared: {}", ownerType, ownerId, ex.getMessage());
        }
    }
}
