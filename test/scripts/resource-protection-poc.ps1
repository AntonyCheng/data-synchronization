param(
    [int]$RowCount = 500,
    [int]$RowsPerSecond = 100,
    [long]$BytesPerSecond = 1048576,
    [long]$StartId = 2000000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

if ($RowCount -lt 100 -or $RowCount -gt 5000) { throw 'RowCount must be between 100 and 5000.' }
if ($RowsPerSecond -lt 1 -or $RowsPerSecond -gt 100000) { throw 'RowsPerSecond must be between 1 and 100000.' }
if ($BytesPerSecond -lt 1 -or $BytesPerSecond -gt 1073741824) { throw 'BytesPerSecond must be between 1 and 1073741824.' }

$resultDirectory = New-PocResultDirectory
$jobName = "ds-poc-resource-protection-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
$lastId = $StartId + $RowCount - 1
$jobId = $null

function Invoke-MySql([string]$Sql) {
    & docker exec -e 'MYSQL_PWD=poc_root_pw' ds-poc-mysql mysql -uroot --default-character-set=utf8mb4 -N -e $Sql
    if ($LASTEXITCODE -ne 0) { throw 'MySQL command failed.' }
}

function Invoke-Postgres([string]$Sql) {
    & docker exec -e 'PGPASSWORD=poc_password' ds-poc-postgres psql -U poc -d sink_db -At -c $Sql
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL command failed.' }
}

try {
    & (Join-Path $PSScriptRoot 'wait-ready.ps1')
    $running = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=100' -TimeoutSec 10
    if (@($running.data).Count -gt 0) { throw 'A SeaTunnel job is already running. Stop it before resource protection verification.' }

    Invoke-MySql "DELETE FROM source_db.customers WHERE id BETWEEN $StartId AND $lastId;" | Out-Null
    Invoke-Postgres "DELETE FROM public.customers WHERE id BETWEEN $StartId AND $lastId;" | Out-Null

    for ($offset = 0; $offset -lt $RowCount; $offset += 50) {
        $count = [Math]::Min(50, $RowCount - $offset)
        $values = for ($index = 0; $index -lt $count; $index++) {
            $id = $StartId + $offset + $index
            "($id, 'resource-$id@example.com', 'Resource $id', 1, 1.00, JSON_OBJECT('suite','resource-protection'), 'rate-limit', UTC_TIMESTAMP(6))"
        }
        Invoke-MySql "INSERT INTO source_db.customers (id,email,display_name,active,credit,profile,notes,registered_at) VALUES $($values -join ',');" | Out-Null
    }

    $config = Get-Content -Raw (Join-Path $PSScriptRoot '..\jobs\mysql-to-postgres-cdc.conf')
    $config = $config -replace 'parallelism = 1', 'parallelism = 1'
    $config = $config -replace 'read_limit\.rows_per_second = 1000', "read_limit.rows_per_second = $RowsPerSecond"
    $config = $config -replace 'read_limit\.bytes_per_second = 10485760', "read_limit.bytes_per_second = $BytesPerSecond"
    $config = $config -replace 'table-names = \[\s*"source_db\.customers",\s*"source_db\.orders"\s*\]', 'table-names = ["source_db.customers"]'
    $config = $config -replace 'table = "public\.\$\{table_name\}"', 'table = "public.customers"'
    $config = $config -replace 'primary_keys = \["\$\{primary_key\}"\]', 'primary_keys = ["id"]'
    $config = $config -replace 'server-time-zone = "UTC"', "server-time-zone = `"UTC`"`n    connection.pool.size = 2"
    $config | Out-File -Encoding utf8 (Join-Path $resultDirectory 'job.conf')

    $submittedAt = [DateTimeOffset]::UtcNow
    $submitted = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18080/submit-job?format=hocon&jobName=$jobName" -ContentType 'text/plain' -Body $config -TimeoutSec 30
    $jobId = [string]$submitted.jobId
    if ([string]::IsNullOrWhiteSpace($jobId)) { throw 'SeaTunnel did not return a jobId.' }

    $deadline = (Get-Date).AddMinutes(3)
    do {
        Start-Sleep -Milliseconds 500
        $targetCount = [int]((Invoke-Postgres "SELECT count(*) FROM public.customers WHERE id BETWEEN $StartId AND $lastId;").Trim())
    } while ($targetCount -lt $RowCount -and (Get-Date) -lt $deadline)
    if ($targetCount -ne $RowCount) { throw "Rate-limited snapshot timed out at $targetCount/$RowCount rows." }

    $elapsedSeconds = [Math]::Max(0.001, ([DateTimeOffset]::UtcNow - $submittedAt).TotalSeconds)
    $measuredRowsPerSecond = $RowCount / $elapsedSeconds
    $minimumExpectedSeconds = ($RowCount / [double]$RowsPerSecond) * 0.8
    if ($elapsedSeconds -lt $minimumExpectedSeconds) {
        throw "Observed $([Math]::Round($measuredRowsPerSecond, 2)) rows/s, faster than the configured $RowsPerSecond rows/s protection threshold."
    }

    [PSCustomObject]@{
        result = 'PASS'; jobId = $jobId; rowCount = $RowCount; rowsPerSecondLimit = $RowsPerSecond
        bytesPerSecondLimit = $BytesPerSecond; connectionPoolSize = 2; elapsedSeconds = [Math]::Round($elapsedSeconds, 3)
        measuredRowsPerSecond = [Math]::Round($measuredRowsPerSecond, 2); minimumExpectedSeconds = [Math]::Round($minimumExpectedSeconds, 3)
    } | ConvertTo-Json | Out-File -Encoding utf8 (Join-Path $resultDirectory 'metrics.json')
    Invoke-RestMethod -Uri "http://127.0.0.1:18080/job-info/$jobId" -TimeoutSec 10 | ConvertTo-Json -Depth 20 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'job-info.json')
    'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    Write-Host "Resource protection POC passed. Evidence: $resultDirectory"
} catch {
    $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'failure.txt')
    'FAIL' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    throw
} finally {
    try {
        if ($jobId) {
            $body = '{"jobId":' + $jobId + ',"isStopWithSavePoint":false,"force":true}'
            Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18080/stop-job' -ContentType 'application/json' -Body $body -TimeoutSec 30 | Out-Null
        }
    } catch { $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'stop-error.txt') }
    try { Invoke-MySql "DELETE FROM source_db.customers WHERE id BETWEEN $StartId AND $lastId;" | Out-Null } catch { $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'cleanup-source-error.txt') }
    try { Invoke-Postgres "DELETE FROM public.customers WHERE id BETWEEN $StartId AND $lastId;" | Out-Null } catch { $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'cleanup-target-error.txt') }
}
