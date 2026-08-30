-- Credential migration permission. Enable mybatis-encryptor before invoking the endpoint.
insert ignore into sys_menu values
(1761400000000002042, '凭证迁移', 1761400000000002001, 8, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:credential-migrate', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id = 1761400000000002042;

-- Multi-table release metadata. The first execution adapter uses one isolated job per table item.
create table if not exists ds_sync_task_group
(
    group_id bigint not null,
    group_name varchar(100) not null,
    source_id bigint not null,
    target_id bigint not null,
    sync_mode varchar(20) default 'FULL_CDC',
    ddl_policy varchar(20) default 'FAIL',
    status varchar(20) default 'DRAFT',
    config_version int default 1,
    engine_job_id varchar(2000) default null,
    engine_config_hash varchar(64) default null,
    last_checkpoint_id varchar(64) default null,
    last_checkpoint_time varchar(64) default null,
    last_checkpoint_status varchar(32) default null,
    last_error varchar(2000) default null,
    create_dept bigint default null,
    create_by bigint default null,
    create_time datetime default null,
    update_by bigint default null,
    update_time datetime default null,
    primary key (group_id),
    unique key uk_ds_sync_task_group_name (group_name)
) engine=innodb comment='多表同步任务组';

create table if not exists ds_sync_task_group_item
(
    item_id bigint not null,
    group_id bigint not null,
    source_database varchar(128) default null,
    source_table varchar(255) not null,
    target_schema varchar(128) default 'public',
    target_table varchar(255) not null,
    primary_keys varchar(1000) default null,
    ddl_policy varchar(20) default 'FAIL',
    selected_columns varchar(2000) default null comment '纳入同步的源字段，逗号分隔',
    sync_key_columns varchar(1000) default null comment '用户确认的同步键字段，逗号分隔',
    status varchar(20) default 'PENDING',
    engine_job_id varchar(128) default null,
    engine_config_hash varchar(64) default null,
    last_checkpoint_id varchar(64) default null,
    last_checkpoint_time varchar(64) default null,
    last_checkpoint_status varchar(32) default null,
    last_error varchar(2000) default null,
    primary key (item_id),
    unique key uk_ds_sync_group_item_table (group_id, source_table),
    key idx_ds_sync_group_item_group (group_id)
) engine=innodb comment='多表同步任务组表项';

insert ignore into sys_menu values
(1761400000000002050, '多表同步', 1761400000000002000, 3, 'group', 'sync/group/index', '', 'N', 'Y', 'C', '0', '0', 'sync:group:list', 'table', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '多表同步任务组');
insert ignore into sys_menu values
(1761400000000002051, '多表新增', 1761400000000002050, 1, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:add', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002052, '多表修改', 1761400000000002050, 2, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:edit', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002053, '多表删除', 1761400000000002050, 3, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:remove', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002054, '多表详情', 1761400000000002050, 4, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:query', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002055, '多表校验', 1761400000000002050, 5, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:validate', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002056, '多表配置预览', 1761400000000002050, 6, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:engine-config', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id between 1761400000000002050 and 1761400000000002056;
insert ignore into sys_menu values
(1761400000000002057, '多表启动', 1761400000000002050, 7, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:start', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, ''),
(1761400000000002058, '多表状态', 1761400000000002050, 8, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:status', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, ''),
(1761400000000002059, '多表暂停', 1761400000000002050, 9, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:pause', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, ''),
(1761400000000002060, '多表恢复', 1761400000000002050, 10, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:resume', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, ''),
(1761400000000002061, '多表停止', 1761400000000002050, 11, 'group', '', '', 'N', 'Y', 'F', '0', '0', 'sync:group:stop', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id between 1761400000000002057 and 1761400000000002061;
