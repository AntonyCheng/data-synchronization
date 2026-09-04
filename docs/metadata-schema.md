# 元数据模型

## `ds_data_source`

保存可被同步引擎使用的连接信息。MVP 支持 `MYSQL` 和 `POSTGRESQL` 两种类型；密码通过 `ruoyi-common-encrypt` 按部署配置加密存储，主密钥不进入元数据库。未启用加密开关的旧 POC 数据可兼容读取，但生产部署必须启用加密并完成旧数据迁移。

关键字段：`source_id`、`source_name`、`source_type`、`host`、`port`、`database_name`、`schema_name`、`username`、`password`、`ssl_enabled`、`status`。运行时字段元数据还包括字段默认值，用于判断目标表额外必填列是否可安全写入。

## `ds_sync_task`

描述一条单表同步任务，并保存引擎运行标识、调度状态和最近错误。MVP 约束源端为 MySQL、目标端为 PostgreSQL，支持 `FULL`、`INCREMENTAL`、`FULL_CDC`，默认目标 Schema 为 `public`，默认 DDL 策略为 `FAIL`。

关键字段：`task_id`、`task_name`、`source_id`、`target_id`、`source_table`、`target_schema`、`target_table`、`sync_mode`、`full_data_mode`、`ddl_policy`、`schedule_mode`、`cron_expression`、`next_run_time`、`last_trigger_time`、`last_skip_reason`、`config_version`、`overwrite_stage_table`、`status`、`engine_job_id`、`last_error`。

`ds_sync_task_config_version` 保存每次创建或修改产生的不可变、无凭证配置快照；其唯一键为 `(task_id, config_version)`。快照包含同步范围、字段选择、同步键、目标策略、调度和源库保护参数，不保存数据源密码或完整引擎配置。

外键关系暂由应用服务校验，避免元数据库初始化时依赖具体数据库方言。删除和修改任务时由服务层依据任务状态执行约束。

## 探查结果（运行时）

数据库、表和字段元数据当前按需从源端 JDBC 读取，不落库。接口返回字段列表、主键、唯一键、字符集和排序规则，用于任务创建前的同步键检查与类型提示。MVP 任务使用数据源的 `database_name` 作为源数据库；后续整库/多表能力落地时，再增加持久化的表实例和库表选择模型。
