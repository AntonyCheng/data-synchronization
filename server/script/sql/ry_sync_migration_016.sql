-- Kafka producer runtime metrics. Safe to run repeatedly against ry-vue.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_published_count') = 0,
    'alter table ds_sync_task add column kafka_published_count bigint default 0 comment ''Kafka 已发布事件数'' after last_checkpoint_status', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_last_partition') = 0,
    'alter table ds_sync_task add column kafka_last_partition int default null comment ''Kafka 最近发布分区'' after kafka_published_count', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
-- Remaining columns are applied independently so reruns remain safe.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_last_offset') = 0,
    'alter table ds_sync_task add column kafka_last_offset bigint default null comment ''Kafka 最近发布位点'' after kafka_last_partition', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_last_source_event_time') = 0,
    'alter table ds_sync_task add column kafka_last_source_event_time datetime default null comment ''Kafka 最近源事件时间'' after kafka_last_offset', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_last_broker_ack_time') = 0,
    'alter table ds_sync_task add column kafka_last_broker_ack_time datetime default null comment ''Kafka 最近 broker 确认时间'' after kafka_last_source_event_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_lag_seconds') = 0,
    'alter table ds_sync_task add column kafka_lag_seconds bigint default null comment ''Kafka 源事件到 broker 确认延迟秒数'' after kafka_last_broker_ack_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
update ds_sync_task set kafka_published_count = 0 where kafka_published_count is null;
