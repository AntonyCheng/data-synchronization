-- SeaTunnel job lifecycle metadata migration for the platform database.
-- Run against ry-vue. Every ALTER is guarded so it is safe to re-run.

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'engine_config_hash') = 0,
    'alter table ds_sync_task add column engine_config_hash varchar(64) default null comment ''提交配置SHA-256指纹'' after engine_job_id', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_checkpoint_id') = 0,
    'alter table ds_sync_task add column last_checkpoint_id varchar(64) default null comment ''最近完成检查点ID'' after engine_config_hash', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_checkpoint_time') = 0,
    'alter table ds_sync_task add column last_checkpoint_time datetime default null comment ''最近完成检查点时间'' after last_checkpoint_id', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task' and column_name = 'last_checkpoint_status') = 0,
    'alter table ds_sync_task add column last_checkpoint_status varchar(32) default null comment ''最近检查点状态'' after last_checkpoint_time', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

insert ignore into sys_menu values
(1761400000000002030, '任务提交', 1761400000000002002, 7, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:start', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002031, '任务状态刷新', 1761400000000002002, 8, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:status', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002032, '任务暂停', 1761400000000002002, 9, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:pause', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002033, '任务恢复', 1761400000000002002, 10, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:resume', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002034, '任务停止', 1761400000000002002, 11, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:stop', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002035, '数据核对', 1761400000000002002, 12, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:check', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id between 1761400000000002030 and 1761400000000002035;
