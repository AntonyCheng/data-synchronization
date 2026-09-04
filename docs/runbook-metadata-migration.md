# 平台元数据库迁移手册

## 适用范围

本手册用于将已有平台元数据库升级到当前 PRD MVP 所需的同步模块结构。迁移只针对平台元数据库 `ry-vue`，默认容器为 `dbs-mysql`；不会操作 `ds-poc-*`、`mysql`、`postgresql` 或其他业务容器。

## 执行方式

确保 `dbs-mysql` 为 healthy 后，在仓库根目录执行：

```powershell
.\deploy\migrate-platform-schema.ps1
```

如已通过环境变量设置元数据库密码，脚本会读取 `PLATFORM_MYSQL_ROOT_PASSWORD`；也可以显式传入 `-MetadataDbPassword`。脚本按 `ry_sync_migration_002.sql` 至最新编号顺序执行，SQL 本身按字段/表/菜单存在性设计为可重复执行。

## 验证口径

迁移完成后至少检查：

- `ds_sync_task` 的 CDC 启动策略、调度、配置版本、覆盖刷新、字段选择、源库保护和最近核对字段存在；
- `ds_sync_task_group`、`ds_sync_task_group_item`、`ds_sync_task_group_ddl_event` 和 `ds_sync_task_config_version` 存在；
- 同步菜单权限已存在，且既有任务和任务组数量不因迁移改变；
- 重复执行脚本返回成功，不产生重复数据或结构错误；
- 重启后端并通过登录、数据源列表和任务列表接口做冒烟检查。

## 回滚边界

这些迁移只增加同步模块字段、表和菜单权限，不删除既有业务表或任务。若升级后需要回退，应停止后端并使用元数据库备份恢复，禁止直接手工删除新增列；目标端业务数据和 `deploy/local-stack/` 下的本地引擎栈数据不属于本迁移操作范围。
