-- Field selection and explicit CDC key selection. Run against ry-vue; safe to re-run.

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'selected_columns') = 0,
    'alter table ds_sync_task add column selected_columns varchar(2000) default null comment ''纳入同步的源字段，逗号分隔'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'sync_key_columns') = 0,
    'alter table ds_sync_task add column sync_key_columns varchar(1000) default null comment ''用户确认的同步键字段，逗号分隔'' after selected_columns', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'selected_columns') = 0,
    'alter table ds_sync_task_group_item add column selected_columns varchar(2000) default null comment ''纳入同步的源字段，逗号分隔'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'sync_key_columns') = 0,
    'alter table ds_sync_task_group_item add column sync_key_columns varchar(1000) default null comment ''用户确认的同步键字段，逗号分隔'' after selected_columns', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
