-- Task-level source protection. Defaults are deliberately conservative and all values are also checked by the service.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'read_limit_rows_per_second') = 0,
    'alter table ds_sync_task add column read_limit_rows_per_second int default 1000 comment ''源端读取最大行数每秒'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'read_limit_bytes_per_second') = 0,
    'alter table ds_sync_task add column read_limit_bytes_per_second bigint default 10485760 comment ''源端读取最大字节每秒'' after read_limit_rows_per_second', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'snapshot_parallelism') = 0,
    'alter table ds_sync_task add column snapshot_parallelism int default 1 comment ''快照阶段并行度'' after read_limit_bytes_per_second', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'source_connection_limit') = 0,
    'alter table ds_sync_task add column source_connection_limit int default 2 comment ''MySQL CDC源端连接池上限'' after snapshot_parallelism', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'read_limit_rows_per_second') = 0,
    'alter table ds_sync_task_group add column read_limit_rows_per_second int default 1000 comment ''每个表项的源端读取最大行数每秒'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'read_limit_bytes_per_second') = 0,
    'alter table ds_sync_task_group add column read_limit_bytes_per_second bigint default 10485760 comment ''每个表项的源端读取最大字节每秒'' after read_limit_rows_per_second', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'snapshot_parallelism') = 0,
    'alter table ds_sync_task_group add column snapshot_parallelism int default 1 comment ''每个表项的快照阶段并行度'' after read_limit_bytes_per_second', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'source_connection_limit') = 0,
    'alter table ds_sync_task_group add column source_connection_limit int default 2 comment ''每个表项的MySQL CDC源端连接池上限'' after snapshot_parallelism', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
