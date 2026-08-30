-- Persist the latest read-only consistency check for single-table tasks.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_check_source_rows') = 0,
    'alter table ds_sync_task add column last_check_source_rows bigint default null comment ''最近核对源端行数'' after last_checkpoint_status', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_check_target_rows') = 0,
    'alter table ds_sync_task add column last_check_target_rows bigint default null comment ''最近核对目标端行数'' after last_check_source_rows', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_check_difference') = 0,
    'alter table ds_sync_task add column last_check_difference bigint default null comment ''最近核对差异行数'' after last_check_target_rows', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_check_matched') = 0,
    'alter table ds_sync_task add column last_check_matched char(1) default null comment ''最近核对是否一致 0否 1是'' after last_check_difference', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_check_time') = 0,
    'alter table ds_sync_task add column last_check_time datetime default null comment ''最近核对时间'' after last_check_matched', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_check_message') = 0,
    'alter table ds_sync_task add column last_check_message varchar(500) default null comment ''最近核对结论'' after last_check_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
