. (Join-Path $PSScriptRoot 'common.ps1')

Invoke-PocDocker -Arguments @(
    'exec', '-e', 'MYSQL_PWD=poc_root_pw', 'ds-poc-mysql',
    'sh', '-c', 'mysql -uroot --default-character-set=utf8mb4 < /poc-cases/mysql/002-cdc-changes.sql'
)
