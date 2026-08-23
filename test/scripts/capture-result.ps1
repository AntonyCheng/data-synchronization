param(
    [Parameter(Mandatory = $true)][string]$ResultDirectory
)

. (Join-Path $PSScriptRoot 'common.ps1')

Invoke-PocCompose ps | Out-File -Encoding utf8 (Join-Path $ResultDirectory 'compose-ps.txt')
Invoke-PocCompose logs --no-color seatunnel | Out-File -Encoding utf8 (Join-Path $ResultDirectory 'seatunnel.log')

& docker exec -e MYSQL_PWD=poc_root_pw ds-poc-mysql mysql -uroot -N -e `
    "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image','gtid_mode','enforce_gtid_consistency');" `
    | Out-File -Encoding utf8 (Join-Path $ResultDirectory 'mysql-cdc-variables.tsv')

& docker exec -e PGPASSWORD=poc_password ds-poc-postgres psql -U poc -d sink_db -At -c `
    "SELECT 'customers', count(*) FROM public.customers UNION ALL SELECT 'orders', count(*) FROM public.orders ORDER BY 1;" `
    | Out-File -Encoding utf8 (Join-Path $ResultDirectory 'target-counts.txt')

try {
    Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=20' -TimeoutSec 10 `
        | ConvertTo-Json -Depth 20 `
        | Out-File -Encoding utf8 (Join-Path $ResultDirectory 'running-jobs.json')
} catch {
    $_ | Out-File -Encoding utf8 (Join-Path $ResultDirectory 'running-jobs-error.txt')
}
