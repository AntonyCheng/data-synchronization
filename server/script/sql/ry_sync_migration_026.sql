-- Kafka data source security: SASL (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512) with the data source's
-- username and password, and SSL with PEM certificates kept on the data source. Empty protocol =
-- PLAINTEXT, exactly what every Kafka data source used before, so existing tasks' engine configs and
-- config fingerprints do not change. The client private key is stored encrypted like the password.
-- Safe to run repeatedly.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'kafka_security_protocol') = 0,
    'alter table ds_data_source add column kafka_security_protocol varchar(20) default null comment ''Kafka 安全协议 SASL_PLAINTEXT/SASL_SSL/SSL（空为 PLAINTEXT）'' after server_time_zone', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'kafka_sasl_mechanism') = 0,
    'alter table ds_data_source add column kafka_sasl_mechanism varchar(20) default null comment ''Kafka SASL 机制 PLAIN/SCRAM-SHA-256/SCRAM-SHA-512'' after kafka_security_protocol', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'kafka_ssl_ca_cert') = 0,
    'alter table ds_data_source add column kafka_ssl_ca_cert text default null comment ''Kafka SSL 信任的 CA 证书（PEM，空为 JVM 默认信任库）'' after kafka_sasl_mechanism', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'kafka_ssl_client_cert') = 0,
    'alter table ds_data_source add column kafka_ssl_client_cert text default null comment ''Kafka SSL 客户端证书链（PEM，双向认证）'' after kafka_ssl_ca_cert', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'kafka_ssl_client_key') = 0,
    'alter table ds_data_source add column kafka_ssl_client_key text default null comment ''Kafka SSL 客户端私钥（PKCS#8 PEM，加密保存）'' after kafka_ssl_client_cert', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;

set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'kafka_ssl_verify_hostname') = 0,
    'alter table ds_data_source add column kafka_ssl_verify_hostname char(1) default null comment ''Kafka SSL 是否校验 broker 主机名（0 不校验，空或 1 校验）'' after kafka_ssl_client_key', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
