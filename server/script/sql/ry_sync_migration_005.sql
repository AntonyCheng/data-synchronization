-- Sync mode and full-load target semantics. Safe to run repeatedly.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'full_data_mode') = 0,
    'alter table ds_sync_task add column full_data_mode varchar(20) default ''UPSERT'' comment ''全量目标数据模式'' after sync_mode', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

update ds_sync_task set full_data_mode = 'UPSERT' where full_data_mode is null or full_data_mode = '';
