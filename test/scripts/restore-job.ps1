param(
    [Parameter(Mandatory = $true)][string]$JobId
)

. (Join-Path $PSScriptRoot 'common.ps1')

Invoke-PocDocker -Arguments @(
    'exec', 'ds-poc-seatunnel', 'bash', '/opt/seatunnel/bin/seatunnel.sh',
    '-r', $JobId, '-c', '/opt/seatunnel/config/poc-jobs/mysql-to-postgres-cdc.conf', '--async'
)
