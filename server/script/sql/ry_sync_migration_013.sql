-- Scheduling, immutable configuration version and safe overwrite metadata.
-- Run against ry-vue; safe to re-run.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'schedule_mode') = 0,
    'alter table ds_sync_task add column schedule_mode varchar(20) not null default ''MANUAL'' comment ''调度模式 ONCE/CRON/REALTIME'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'cron_expression') = 0,
    'alter table ds_sync_task add column cron_expression varchar(120) default null comment ''Cron表达式'' after schedule_mode', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'next_run_time') = 0,
    'alter table ds_sync_task add column next_run_time datetime default null comment ''下次调度时间'' after cron_expression', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_trigger_time') = 0,
    'alter table ds_sync_task add column last_trigger_time datetime default null comment ''最近触发时间'' after next_run_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_skip_reason') = 0,
    'alter table ds_sync_task add column last_skip_reason varchar(500) default null comment ''最近一次跳过原因'' after last_trigger_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'config_version') = 0,
    'alter table ds_sync_task add column config_version int not null default 1 comment ''任务配置版本'' after last_skip_reason', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'overwrite_stage_table') = 0,
    'alter table ds_sync_task add column overwrite_stage_table varchar(255) default null comment ''覆盖刷新临时表'' after config_version', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
-- Existing tasks remain manual after migration; newly saved tasks default to one-time.
update ds_sync_task set schedule_mode = 'MANUAL' where schedule_mode is null or schedule_mode = '';
update ds_sync_task set config_version = 1 where config_version is null or config_version < 1;
