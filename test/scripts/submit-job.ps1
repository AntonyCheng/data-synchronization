. (Join-Path $PSScriptRoot 'common.ps1')

Invoke-PocDocker exec ds-poc-seatunnel bash /opt/seatunnel/bin/seatunnel.sh `
    --config /opt/seatunnel/config/poc-jobs/mysql-to-postgres-cdc.conf `
    --async `
    --name ds-poc-mysql-postgres-cdc
