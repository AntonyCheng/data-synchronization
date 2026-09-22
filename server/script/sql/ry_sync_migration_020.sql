-- Engine run-metrics history. Every successful engine status poll (the 30 s reconcile
-- pass and manual refreshes) appends one sample per task / task-group item, so throughput
-- and CDC lag can be charted and looked back on after the fact. Samples older than
-- sync.metrics.retention-days (default 7) are purged hourly. Safe to run repeatedly.
create table if not exists ds_sync_metrics_sample
(
    sample_id             bigint        not null,
    owner_type            varchar(16)   not null comment 'TASK / GROUP_ITEM',
    owner_id              bigint        not null comment 'task_id 或 item_id',
    group_id              bigint        default null comment '表项所属任务组',
    engine_job_id         varchar(64)   default null,
    engine_status         varchar(32)   default null comment 'SeaTunnel 作业状态',
    phase                 varchar(16)   default null comment 'SNAPSHOT / CDC',
    source_received_count bigint        default null,
    sink_committed_count  bigint        default null,
    source_received_bytes bigint        default null,
    sink_committed_bytes  bigint        default null,
    source_qps            decimal(18,3) default null,
    sink_qps              decimal(18,3) default null,
    backlog_rows          bigint        default null comment '源端已读取 - 目标已提交',
    cdc_lag_seconds       bigint        default null comment '端到端延迟秒数（Kafka 目标由桥接计算）',
    sampled_at            datetime      not null,
    primary key (sample_id),
    key idx_ds_sync_metrics_owner (owner_type, owner_id, sampled_at),
    key idx_ds_sync_metrics_sampled_at (sampled_at)
) engine=innodb comment='同步作业运行指标采样';

-- backlog_rows was added to the sample shape after the table first shipped to a dev database;
-- the guarded ALTER brings such a table in line (no-op once the column exists).
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_sync_metrics_sample' and column_name = 'backlog_rows') = 0,
    'alter table ds_sync_metrics_sample add column backlog_rows bigint default null comment ''源端已读取 - 目标已提交'' after sink_qps', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
