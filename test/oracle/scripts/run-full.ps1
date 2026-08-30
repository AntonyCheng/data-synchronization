. (Join-Path $PSScriptRoot 'common.ps1')

& docker exec ds-poc-oracle-seatunnel /opt/seatunnel/bin/seatunnel.sh --config /opt/seatunnel/config/oracle-jobs/mysql-to-oracle-full.conf
if ($LASTEXITCODE -ne 0) { throw 'MySQL to Oracle full POC job failed.' }

Invoke-OracleSql '/poc-cases/oracle/verify-full.sql'
