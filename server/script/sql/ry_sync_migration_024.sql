-- Registry of the private Kafka raw topics (__ds_raw_{ownerId}_v{configVersion}) the bridge created
-- or found on a Kafka target. Every bridge (re)start registers its raw topic; KafkaRawTopicJanitor
-- deletes a registered topic once no task / table item uses it any more (owner deleted, config
-- version superseded, target switched), after sync.kafka-bridge.raw-topic-retire-grace. A topic that
-- is not registered here is never deleted: another platform environment may share the cluster.
-- Safe to run repeatedly.
create table if not exists ds_kafka_raw_topic
(
    raw_topic_id   bigint       not null comment '主键',
    data_source_id bigint       not null comment 'topic 所在的 Kafka 目标数据源ID',
    topic_name     varchar(249) not null comment 'raw topic 名称',
    owner_type     varchar(16)  not null comment 'TASK / GROUP_ITEM',
    owner_id       bigint       not null comment 'task_id 或 item_id',
    config_version int          not null comment '写入该 topic 的配置版本',
    retired_time   datetime     default null comment '首次发现不再被使用的时间（空为使用中）',
    deleted_time   datetime     default null comment '从 Kafka 删除的时间',
    create_time    datetime     default current_timestamp comment '登记时间',
    primary key (raw_topic_id),
    unique key uk_ds_kafka_raw_topic (data_source_id, topic_name),
    key idx_ds_kafka_raw_topic_owner (owner_id)
) engine=innodb comment='平台创建的 Kafka raw topic 登记（仅登记过的 topic 会被清理）';
