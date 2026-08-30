-- Immutable task-configuration snapshots. Run against ry-vue; safe to re-run.
create table if not exists ds_sync_task_config_version
(
    version_id bigint not null comment '版本快照ID',
    task_id bigint not null comment '同步任务ID',
    config_version int not null comment '任务配置版本号',
    config_snapshot longtext not null comment '无凭证配置快照JSON',
    create_time datetime default current_timestamp comment '创建时间',
    primary key (version_id),
    unique key uk_ds_sync_task_config_version (task_id, config_version),
    key idx_ds_sync_task_config_version_task (task_id)
) engine = innodb comment = '同步任务不可变配置版本快照';
