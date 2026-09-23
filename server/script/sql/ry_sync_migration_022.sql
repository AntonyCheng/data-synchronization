-- Indexes for the background passes. Every one of these queries runs on a timer against the
-- whole table: the scheduler scans (schedule_mode, next_run_time) every 15 s, status reconcile
-- and the alert notifier scan `status` every 30 s, the DDL check scans group `status` every
-- 60 s. Without these they are full scans that grow with the task count. Safe to re-run.
set @sql = if((select count(*) from information_schema.statistics where table_schema = database() and table_name = 'ds_sync_task' and index_name = 'idx_ds_sync_task_status') = 0,
    'create index idx_ds_sync_task_status on ds_sync_task (status)', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.statistics where table_schema = database() and table_name = 'ds_sync_task' and index_name = 'idx_ds_sync_task_due') = 0,
    'create index idx_ds_sync_task_due on ds_sync_task (schedule_mode, next_run_time)', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.statistics where table_schema = database() and table_name = 'ds_sync_task_group' and index_name = 'idx_ds_sync_task_group_status') = 0,
    'create index idx_ds_sync_task_group_status on ds_sync_task_group (status)', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

-- The item table already has (group_id); the alert pass filters on status across all groups.
set @sql = if((select count(*) from information_schema.statistics where table_schema = database() and table_name = 'ds_sync_task_group_item' and index_name = 'idx_ds_sync_group_item_status') = 0,
    'create index idx_ds_sync_group_item_status on ds_sync_task_group_item (status)', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
