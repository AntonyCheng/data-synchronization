# 数据源凭证保护

## 实现

`ruoyi-sync` 的 `DataSource.password` 使用脚手架 `ruoyi-common-encrypt` 的 `@EncryptField(algorithm = AES)`。MyBatis 入参拦截器在写入前加密并添加框架密文前缀，出参拦截器在读取后解密；`DataSourceVo` 不包含密码，因此列表、详情和配置预览都不会返回密码。SeaTunnel 只接收业务层解密后的运行时值，生成的预览始终脱敏，元数据库和平台日志不保存引擎明文配置。

## 配置

```yaml
mybatis-encryptor:
  enable: ${SYNC_CREDENTIAL_ENCRYPTION_ENABLED:false}
  password: ${SYNC_CREDENTIAL_ENCRYPTION_PASSWORD:}
```

启用时 `SYNC_CREDENTIAL_ENCRYPTION_PASSWORD` 必须是 16、24 或 32 个字符。密钥只通过部署环境注入，不提交到仓库、Compose 文件或测试结果。生产环境应使用密钥管理系统并按环境轮换。

## 明文兼容与迁移

读取旧的明文密码不会尝试解密，连接测试和任务生成可以继续工作。对数据源执行一次编辑保存（密码字段留空会沿用当前值）即可触发加密写入；迁移前必须备份元数据库，迁移后通过连接测试和任务配置预览验证。切换密钥不支持直接覆盖，需先用旧密钥读出并在停机/迁移窗口用新密钥重写。

## 验收要求

- 未配置开关时现有环境行为不变。
- 启用后新增和更新记录的数据库值带加密前缀，API 响应不含 `password` 字段。
- 旧明文记录可读取、连接测试可用，并能通过保存迁移为密文。
- 错误、配置预览和作业审计中不得出现真实密码。
