-- Kafka bridge failure handling. A Kafka-target task / table item records why its bridge stopped
-- publishing (an event the target topic refuses, an unreadable raw record, a broker error): the
-- bridge holds the raw topic at the failed event instead of skipping it, retries with a growing
-- interval, clears the columns once it publishes again, and SyncAlertNotifier announces it.
-- kafka_bridge_backlog is how many raw records waited behind the stop at the bridge's last attempt;
-- the status refresh adds it to the owner's backlog, which otherwise only counts the engine.
-- The raw topic registry records the raw offset the bridge has committed, so a bridge whose consumer
-- group offsets expired (paused longer than the broker's offsets.retention.minutes) resumes where it
-- stopped and a position Kafka can no longer serve is reported instead of silently skipped;
-- reset_allowed marks a fresh run (start / reinitialize), whose new initial load may begin at the
-- earliest retained record. Safe to run repeatedly.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_bridge_error') = 0,
    'alter table ds_sync_task add column kafka_bridge_error varchar(2000) default null comment ''Kafka 桥接暂停发布的原因（空为正常）'' after kafka_lag_seconds', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_bridge_error_time') = 0,
    'alter table ds_sync_task add column kafka_bridge_error_time datetime default null comment ''Kafka 桥接本次暂停发布的开始时间'' after kafka_bridge_error', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_bridge_backlog') = 0,
    'alter table ds_sync_task add column kafka_bridge_backlog bigint default null comment ''Kafka 桥接暂停时 raw topic 中待发布的记录数（最近一次重试时统计）'' after kafka_bridge_error_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'kafka_bridge_error') = 0,
    'alter table ds_sync_task_group_item add column kafka_bridge_error varchar(2000) default null comment ''Kafka 桥接暂停发布的原因（空为正常）'' after last_error', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'kafka_bridge_error_time') = 0,
    'alter table ds_sync_task_group_item add column kafka_bridge_error_time datetime default null comment ''Kafka 桥接本次暂停发布的开始时间'' after kafka_bridge_error', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'kafka_bridge_backlog') = 0,
    'alter table ds_sync_task_group_item add column kafka_bridge_backlog bigint default null comment ''Kafka 桥接暂停时 raw topic 中待发布的记录数（最近一次重试时统计）'' after kafka_bridge_error_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_kafka_raw_topic' and column_name = 'bridge_offset') = 0,
    'alter table ds_kafka_raw_topic add column bridge_offset bigint default null comment ''桥接已提交的 raw 位点（下一条待读 offset，空为未记录）'' after config_version', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_kafka_raw_topic' and column_name = 'reset_allowed') = 0,
    'alter table ds_kafka_raw_topic add column reset_allowed char(1) default ''0'' comment ''1=新一轮运行（启动/重新初始化），读取位点失效时可从最早保留的记录开始'' after bridge_offset', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
