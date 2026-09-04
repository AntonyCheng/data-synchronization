-- 数据同步平台元数据表与初始菜单。
-- 该脚本只作用于平台元数据库 ry-vue，不涉及 POC 数据库。

create table if not exists ds_data_source
(
    source_id     bigint         not null comment '数据源ID',
    source_name   varchar(100)   not null comment '数据源名称',
    source_type   varchar(20)    not null comment '数据源类型 MYSQL/POSTGRESQL/KAFKA',
    host          varchar(255)   not null comment '主机地址',
    port          int            not null comment '端口',
    database_name varchar(128)   not null comment '数据库名称',
    schema_name   varchar(128)   default null comment 'Schema名称',
    username      varchar(128)   not null comment '连接用户名',
    password      varchar(512)   not null comment '连接密码（后续接入密钥管理）',
    ssl_enabled   char(1)        default '0' comment '是否启用SSL',
    status        char(1)        default '0' comment '状态（0正常 1停用）',
    remark        varchar(500)   default '' comment '备注',
    create_dept   bigint         default null comment '创建部门',
    create_by     bigint         default null comment '创建者',
    create_time   datetime       default null comment '创建时间',
    update_by     bigint         default null comment '更新者',
    update_time   datetime       default null comment '更新时间',
    primary key (source_id),
    unique key uk_ds_data_source_name (source_name)
) engine = innodb comment = '数据同步数据源';

create table if not exists ds_sync_task
(
    task_id       bigint         not null comment '任务ID',
    task_name     varchar(100)   not null comment '任务名称',
    source_id     bigint         not null comment '源数据源ID',
    target_id     bigint         not null comment '目标数据源ID',
    source_table  varchar(255)   not null comment '源表名',
    target_schema varchar(128)   default 'public' comment '目标Schema',
    target_table  varchar(255)   not null comment '目标表名',
    sync_mode     varchar(20)    default 'FULL_CDC' comment '同步模式',
    incremental_startup_mode varchar(20) not null default 'LATEST' comment '纯增量启动策略 LATEST/TIMESTAMP/SPECIFIC',
    incremental_startup_timestamp datetime default null comment '纯增量按时间启动时间',
    incremental_startup_binlog_file varchar(128) default null comment '纯增量指定binlog文件',
    incremental_startup_binlog_position bigint default null comment '纯增量指定binlog位置',
    full_data_mode varchar(20)   default 'UPSERT' comment '全量目标数据模式',
    ddl_policy    varchar(20)    default 'FAIL' comment 'DDL策略',
    schedule_mode varchar(20)    default 'ONCE' comment '调度模式 ONCE/CRON/REALTIME',
    cron_expression varchar(120) default null comment 'Cron表达式',
    next_run_time datetime default null comment '下次调度时间',
    last_trigger_time datetime default null comment '最近触发时间',
    last_skip_reason varchar(500) default null comment '最近一次跳过原因',
    config_version int not null default 1 comment '任务配置版本',
    overwrite_stage_table varchar(255) default null comment '覆盖刷新临时表',
    selected_columns varchar(2000) default null comment '纳入同步的源字段，逗号分隔',
    sync_key_columns varchar(1000) default null comment '用户确认的同步键字段，逗号分隔',
    read_limit_rows_per_second int default 1000 comment '源端读取最大行数每秒',
    read_limit_bytes_per_second bigint default 10485760 comment '源端读取最大字节每秒',
    snapshot_parallelism int default 1 comment '快照阶段并行度',
    source_connection_limit int default 2 comment 'MySQL CDC源端连接池上限',
    status        varchar(20)    default 'DRAFT' comment '任务状态',
    engine_job_id varchar(128)   default null comment 'SeaTunnel作业ID',
    engine_config_hash varchar(64) default null comment '提交配置SHA-256指纹',
    last_checkpoint_id varchar(64) default null comment '最近完成检查点ID',
    last_checkpoint_time datetime default null comment '最近完成检查点时间',
    last_checkpoint_status varchar(32) default null comment '最近检查点状态',
    kafka_published_count bigint default 0 comment 'Kafka 已发布事件数',
    kafka_last_partition int default null comment 'Kafka 最近发布分区',
    kafka_last_offset bigint default null comment 'Kafka 最近发布位点',
    kafka_last_source_event_time datetime default null comment 'Kafka 最近源事件时间',
    kafka_last_broker_ack_time datetime default null comment 'Kafka 最近 broker 确认时间',
    kafka_lag_seconds bigint default null comment 'Kafka 源事件到 broker 确认延迟秒数',
    last_error    varchar(2000)  default null comment '最近错误',
    create_dept   bigint         default null comment '创建部门',
    create_by     bigint         default null comment '创建者',
    create_time   datetime       default null comment '创建时间',
    update_by     bigint         default null comment '更新者',
    update_time   datetime       default null comment '更新时间',
    primary key (task_id),
    unique key uk_ds_sync_task_name (task_name),
    key idx_ds_sync_task_source (source_id),
    key idx_ds_sync_task_target (target_id)
) engine = innodb comment = '数据同步任务';

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

insert ignore into sys_menu values
(1761400000000002000, '数据同步', 0, 2, 'sync', null, '', 'N', 'Y', 'M', '0', '0', '', 'server', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '数据同步平台目录');
insert ignore into sys_menu values
(1761400000000002001, '数据源管理', 1761400000000002000, 1, 'data-source', 'sync/data-source/index', '', 'N', 'Y', 'C', '0', '0', 'sync:data-source:list', 'database', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '数据源管理');
insert ignore into sys_menu values
(1761400000000002002, '同步任务', 1761400000000002000, 2, 'task', 'sync/task/index', '', 'N', 'Y', 'C', '0', '0', 'sync:task:list', 'job', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '同步任务');

insert ignore into sys_menu values
(1761400000000002010, '数据源新增', 1761400000000002001, 1, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:add', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002011, '数据源修改', 1761400000000002001, 2, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:edit', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002012, '数据源删除', 1761400000000002001, 3, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:remove', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002013, '数据源测试', 1761400000000002001, 4, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:test', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002014, '数据源详情', 1761400000000002001, 5, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:query', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002040, '数据源元数据探查', 1761400000000002001, 6, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:metadata', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002041, 'CDC 前置检查', 1761400000000002001, 7, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:cdc-precheck', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002020, '任务新增', 1761400000000002002, 1, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:add', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002021, '任务修改', 1761400000000002002, 2, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:edit', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002022, '任务删除', 1761400000000002002, 3, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:remove', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002023, '任务校验', 1761400000000002002, 4, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:validate', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002024, '任务详情', 1761400000000002002, 5, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:query', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002025, '引擎配置预览', 1761400000000002002, 6, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:engine-config', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
insert ignore into sys_menu values
(1761400000000002026, '数据核对', 1761400000000002002, 12, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:check', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');
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
(1761400000000002036, '任务重新初始化', 1761400000000002002, 13, 'task', '', '', 'N', 'Y', 'F', '0', '0', 'sync:task:reinitialize', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');

insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id between 1761400000000002000 and 1761400000000002041;
