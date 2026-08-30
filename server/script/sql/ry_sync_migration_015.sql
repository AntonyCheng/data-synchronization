-- Pure CDC startup-position policy. Run against ry-vue; safe to re-run.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'incremental_startup_mode') = 0,
    'alter table ds_sync_task add column incremental_startup_mode varchar(20) not null default ''LATEST'' comment ''纯增量启动策略 LATEST/TIMESTAMP/SPECIFIC'' after sync_mode', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'incremental_startup_timestamp') = 0,
    'alter table ds_sync_task add column incremental_startup_timestamp datetime default null comment ''纯增量按时间启动时间'' after incremental_startup_mode', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'incremental_startup_binlog_file') = 0,
    'alter table ds_sync_task add column incremental_startup_binlog_file varchar(128) default null comment ''纯增量指定binlog文件'' after incremental_startup_timestamp', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'incremental_startup_binlog_position') = 0,
    'alter table ds_sync_task add column incremental_startup_binlog_position bigint default null comment ''纯增量指定binlog位置'' after incremental_startup_binlog_file', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

update ds_sync_task set incremental_startup_mode = 'LATEST' where incremental_startup_mode is null or incremental_startup_mode = '';
