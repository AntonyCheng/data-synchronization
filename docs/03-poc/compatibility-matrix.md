# POC 兼容矩阵

| 组件/能力 | 候选范围 | 本次验证值 | 结果 | 备注 |
|---|---|---|---|---|
| SeaTunnel Zeta | 2.3.13 | 2.3.13 | 通过 | POC 定制镜像 `data-sync-poc/seatunnel:2.3.13` |
| MySQL | 8.0.x | Docker `mysql:8.0` | 通过 | ROW、FULL、GTID，`server_id=223344` |
| PostgreSQL | 17.x | 17.6 | 通过 | JDBC sink，驱动 42.7.5 |
| PostgreSQL JDBC 驱动隔离 | 42.7.5 | 42.7.5 | 已确认冲突并修正 | 官方镜像的 `opengauss-jdbc` 包含同名 `org.postgresql.Driver`，POC 镜像必须移除该 jar |
| 全量+增量 | MySQL CDC initial | 已验证 | 通过 | 一致性快照后持续 CDC |
| 联合主键 | 支持 | 已验证 | 通过 | `orders(tenant_id, order_no)` |
| 非空唯一键 | 支持候选 | `inventory_by_sku(sku, warehouse_code)` | 通过（显式 primary_keys） | 新表自动建表时不能依赖 `${primary_key}` 模板变量，适配器需显式传入键列 |
| checkpoint 恢复 | 持久化目录 | 已验证 | 通过 | 引擎重启后由适配器 restore |
| savepoint/restore | 官方任务控制能力 | 已验证 | 通过 | 同 jobId、同配置版本恢复 |
| 新增字段 | 有条件支持 | 已验证边界 | 不自动变更 | `schema-changes.enabled=false` 时需人工处理 |
