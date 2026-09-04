-- Recovery status values include REINITIALIZE_REQUIRED (21 characters).
-- Older installations created status as varchar(20), which caused the
-- recovery action to fail with MySQL data truncation after a restart.
alter table ds_sync_task modify column status varchar(32) default 'DRAFT' comment '任务状态';
alter table ds_sync_task_group modify column status varchar(32) default 'DRAFT' comment '任务组状态';
alter table ds_sync_task_group_item modify column status varchar(32) default 'PENDING' comment '表项状态';
