-- Per-data-source server time zone for MySQL sources. The MySQL-CDC source is told this zone
-- (server-time-zone and the connection's serverTimezone) to turn binlog TIMESTAMP instants back
-- into the wall clock the server shows. Empty/NULL = compatibility mode: Asia/Shanghai, exactly
-- what every CDC config carried before, so existing tasks' config fingerprints do not change until
-- an operator sets the zone. Safe to run repeatedly.
set @sql = if((select count(*) from information_schema.columns where table_schema = database() and table_name = 'ds_data_source' and column_name = 'server_time_zone') = 0,
    'alter table ds_data_source add column server_time_zone varchar(64) default null comment ''MySQL 源端服务器时区（IANA ID，空为兼容模式 Asia/Shanghai）'' after ssl_enabled', 'select 1');
prepare stmt from @sql; execute stmt; deallocate prepare stmt;
