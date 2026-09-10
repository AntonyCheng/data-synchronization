-- Pluggable Kafka output format on the platform bridge serializer. ENVELOPE (the
-- historical PRD event envelope) stays the default so existing tasks are unaffected;
-- the other four are CDC-compatible JSON shapes re-consumable by a SeaTunnel Kafka
-- source with the matching format. Task groups carry one group-level value shared by
-- every item. Safe to run repeatedly against ry-vue.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'kafka_output_format') = 0,
    'alter table ds_sync_task add column kafka_output_format varchar(32) not null default ''ENVELOPE'' comment ''Kafka 输出格式 ENVELOPE/CANAL_JSON/COMPATIBLE_DEBEZIUM_JSON/MAXWELL_JSON/OGG_JSON'' after sync_key_columns', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'kafka_output_format') = 0,
    'alter table ds_sync_task_group add column kafka_output_format varchar(32) not null default ''ENVELOPE'' comment ''Kafka 输出格式（组内全部表项统一）ENVELOPE/CANAL_JSON/COMPATIBLE_DEBEZIUM_JSON/MAXWELL_JSON/OGG_JSON'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
-- Belt and braces: the column default already backfills existing rows.
update ds_sync_task set kafka_output_format = 'ENVELOPE' where kafka_output_format is null or kafka_output_format = '';
update ds_sync_task_group set kafka_output_format = 'ENVELOPE' where kafka_output_format is null or kafka_output_format = '';
