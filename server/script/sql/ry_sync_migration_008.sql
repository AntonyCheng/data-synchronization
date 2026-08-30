-- Runtime DDL change tracking for isolated multi-table synchronization items.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'schema_snapshot') = 0,
    'alter table ds_sync_task_group_item add column schema_snapshot longtext default null comment ''启动时源表结构快照'' after ddl_policy', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_task_group_item' and column_name = 'schema_hash') = 0,
    'alter table ds_sync_task_group_item add column schema_hash varchar(64) default null comment ''启动时源表结构指纹'' after schema_snapshot', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

create table if not exists ds_sync_task_group_ddl_event
(
    event_id           bigint       not null,
    group_id           bigint       not null,
    item_id            bigint       not null,
    change_type        varchar(64)  not null comment 'ADD_COLUMN/DROP_COLUMN/ALTER_COLUMN/KEY_CHANGED',
    risk_level         varchar(16)  not null comment 'LOW/HIGH',
    status             varchar(32)  not null comment 'PENDING_FIX/READY_TO_RESUME/RESOLVED',
    details            varchar(2000) default null,
    remediation        varchar(2000) default null,
    source_schema_hash varchar(64)  default null,
    detected_at        datetime     not null,
    resolved_at        datetime     default null,
    primary key (event_id),
    key idx_ds_sync_group_ddl_item (item_id, status, detected_at),
    key idx_ds_sync_group_ddl_group (group_id, status, detected_at)
) engine=innodb comment='同步任务组表结构变更事件';

insert ignore into sys_menu values
(1761400000000002063, '多表结构检查', 1761400000000002050, 13, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:ddl-check', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '检查并隔离运行时表结构变更');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id = 1761400000000002063;
