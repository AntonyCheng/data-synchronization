-- Database-scope synchronization: continuously discover new source tables.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'sync_scope') = 0,
    'alter table ds_sync_task_group add column sync_scope varchar(20) not null default ''MULTI_TABLE'' comment ''同步粒度 MULTI_TABLE/DATABASE'' after target_id', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'source_database') = 0,
    'alter table ds_sync_task_group add column source_database varchar(128) default null comment ''整库同步源库名'' after sync_scope', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group' and column_name = 'auto_discover') = 0,
    'alter table ds_sync_task_group add column auto_discover char(1) not null default ''0'' comment ''整库新增表自动发现 0否 1是'' after source_database', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

insert ignore into sys_menu values
(1761400000000002062, '整库表发现', 1761400000000002050, 12, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:discover', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '扫描整库同步的新表');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id = 1761400000000002062;
