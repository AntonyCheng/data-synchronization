-- Sync alerts: each task / task group / table item remembers the status the alert
-- notifier last raised a message-center notice for, so a FAILED / REINITIALIZE_REQUIRED /
-- DEGRADED / DDL_BLOCKED state is announced exactly once per transition, across restarts
-- and instances. Safe to run repeatedly.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'alerted_status') = 0,
    'alter table ds_sync_task add column alerted_status varchar(32) default '''' comment ''告警通知器最近通知过的状态（空为无未处理告警）'' after last_error', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'alerted_status') = 0,
    'alter table ds_sync_task_group add column alerted_status varchar(32) default '''' comment ''告警通知器最近通知过的状态（空为无未处理告警）'' after last_error', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'alerted_status') = 0,
    'alter table ds_sync_task_group_item add column alerted_status varchar(32) default '''' comment ''告警通知器最近通知过的状态（空为无未处理告警）'' after last_error', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
