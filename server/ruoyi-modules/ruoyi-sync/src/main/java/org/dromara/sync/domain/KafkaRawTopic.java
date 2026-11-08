package org.dromara.sync.domain;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * A private raw topic ({@code __ds_raw_{ownerId}_v{configVersion}}) the bridge created or found on a
 * Kafka target. {@code KafkaRawTopicJanitor} deletes only topics recorded here: another platform
 * environment may share the cluster with the same naming scheme, so the name alone never proves a
 * topic is ours to delete.
 */
@Data
@TableName("ds_kafka_raw_topic")
public class KafkaRawTopic {

    public static final String OWNER_TASK = "TASK";
    public static final String OWNER_GROUP_ITEM = "GROUP_ITEM";

    @TableId(value = "raw_topic_id", type = IdType.ASSIGN_ID)
    private Long rawTopicId;
    /** The Kafka target data source the topic lives on. */
    private Long dataSourceId;
    private String topicName;
    private String ownerType;
    /** Task id, or table item id for {@link #OWNER_GROUP_ITEM}. */
    private Long ownerId;
    private Integer configVersion;
    /**
     * The raw offset the bridge last committed (the next one to read), recorded a few seconds behind
     * its Kafka consumer group: it outlives the group's offsets, which the broker drops once the group
     * has been empty for {@code offsets.retention.minutes} (a task paused for a week). Null until the
     * bridge commits on this topic.
     */
    private Long bridgeOffset;
    /**
     * {@code "1"} while a fresh run (start / reinitialize) has not positioned its bridge yet: its new
     * initial load supersedes whatever an unreadable old position held, so the bridge may begin at
     * the earliest retained record instead of reporting a gap.
     */
    private String resetAllowed;
    /** When the janitor first found no owner using the topic; null while it is in use. */
    private LocalDateTime retiredTime;
    /** When the janitor deleted the topic; the row stays a grace period to catch a re-creation. */
    private LocalDateTime deletedTime;
    private LocalDateTime createTime;
}
