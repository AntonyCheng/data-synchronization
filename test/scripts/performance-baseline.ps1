param(
    [int]$RowCount = 5000,
    [long]$StartId = 1000000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'common.ps1')

if ($RowCount -lt 1 -or $RowCount -gt 50000) {
    throw 'RowCount must be between 1 and 50000.'
}

$resultDirectory = New-PocResultDirectory
$jobName = "ds-poc-performance-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
$firstId = $StartId
$lastId = $StartId + $RowCount - 1
$cleanupLastId = $lastId + 1
$sourceSql = "DELETE FROM source_db.customers WHERE id BETWEEN $firstId AND $cleanupLastId;"
$targetSql = "DELETE FROM public.customers WHERE id BETWEEN $firstId AND $cleanupLastId;"
$jobId = $null

function Invoke-MySql([string]$Sql) {
    return Invoke-DockerCapture @('exec', '-e', 'MYSQL_PWD=poc_root_pw', 'ds-poc-mysql', 'mysql', '-uroot', '--default-character-set=utf8mb4', '-N', '-e', $Sql)
}

function Invoke-Postgres([string]$Sql) {
    return Invoke-DockerCapture @('exec', '-e', 'PGPASSWORD=poc_password', 'ds-poc-postgres', 'psql', '-U', 'poc', '-d', 'sink_db', '-At', '-c', $Sql)
}

function Invoke-DockerCapture {
    param([string[]]$Arguments)
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = 'docker.exe'
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) { [void]$startInfo.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) { throw 'Unable to start docker.exe.' }
    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) { throw "docker command failed: $stderr" }
    return $stdout.Trim()
}

function Get-TargetBenchmarkCount {
    $value = Invoke-Postgres "SELECT count(*) FROM public.customers WHERE id BETWEEN $firstId AND $lastId;"
    return [int](($value -join '').Trim())
}

try {
    & (Join-Path $PSScriptRoot 'wait-ready.ps1')
    $running = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=100' -TimeoutSec 10
    if (@($running.data).Count -gt 0) {
        throw 'A SeaTunnel job is already running. Stop it before running the performance baseline.'
    }

    Invoke-MySql $sourceSql | Out-Null
    Invoke-Postgres $targetSql | Out-Null

    # Keep each docker exec argument list below Windows command-line limits.
    $chunkSize = 50
    for ($offset = 0; $offset -lt $RowCount; $offset += $chunkSize) {
        $end = [Math]::Min($RowCount, $offset + $chunkSize)
        $values = [System.Collections.Generic.List[string]]::new()
        for ($index = $offset; $index -lt $end; $index++) {
            $id = $StartId + $index
            $values.Add("($id, 'benchmark-$id@example.com', 'Benchmark $id', 1, 1.00, JSON_OBJECT('suite','performance'), 'baseline', UTC_TIMESTAMP(6))")
        }
        Invoke-MySql "INSERT INTO source_db.customers (id,email,display_name,active,credit,profile,notes,registered_at) VALUES $($values -join ',');" | Out-Null
    }

    $config = Get-Content -Raw (Join-Path $PSScriptRoot '..\jobs\mysql-to-postgres-cdc.conf')
    $config = $config -replace 'table-names = \[\s*"source_db\.customers",\s*"source_db\.orders"\s*\]', 'table-names = ["source_db.customers"]'
    $config = $config -replace 'table = "public\.\$\{table_name\}"', 'table = "public.customers"'
    $config = $config -replace 'primary_keys = \["\$\{primary_key\}"\]', 'primary_keys = ["id"]'
    $submitAt = [DateTimeOffset]::UtcNow
    $submitted = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18080/submit-job?format=hocon&jobName=$jobName" -ContentType 'text/plain' -Body $config -TimeoutSec 30
    $jobId = [string]$submitted.jobId
    if ([string]::IsNullOrWhiteSpace($jobId)) { throw 'SeaTunnel did not return a jobId.' }

    $expected = $RowCount
    $deadline = (Get-Date).AddMinutes(5)
    do {
        Start-Sleep -Seconds 2
        $targetCount = Get-TargetBenchmarkCount
    } while ($targetCount -lt $expected -and (Get-Date) -lt $deadline)
    if ($targetCount -lt $expected) { throw "Full load timed out at $targetCount/$expected rows." }
    $fullLoadCompletedAt = [DateTimeOffset]::UtcNow

    $markerId = $lastId + 1
    $markerAt = [DateTimeOffset]::UtcNow
    Invoke-MySql "INSERT INTO source_db.customers (id,email,display_name,active,credit,profile,notes,registered_at) VALUES ($markerId, 'benchmark-marker-$markerId@example.com', 'CDC marker', 1, 1.00, JSON_OBJECT('suite','performance'), 'cdc-marker', UTC_TIMESTAMP(6));" | Out-Null
    $cdcDeadline = (Get-Date).AddMinutes(2)
    do {
        Start-Sleep -Seconds 1
        $markerCount = Invoke-Postgres "SELECT count(*) FROM public.customers WHERE id = $markerId;"
        $markerCount = [int](($markerCount -join '').Trim())
    } while ($markerCount -lt 1 -and (Get-Date) -lt $cdcDeadline)
    if ($markerCount -ne 1) { throw 'CDC marker was not observed in the target.' }
    $cdcObservedAt = [DateTimeOffset]::UtcNow

    $fullSeconds = [Math]::Max(0.001, ($fullLoadCompletedAt - $submitAt).TotalSeconds)
    $cdcSeconds = [Math]::Max(0, ($cdcObservedAt - $markerAt).TotalSeconds)
    [PSCustomObject]@{
        result = 'PASS'; jobId = $jobId; rowCount = $RowCount; sourceRows = $RowCount + 1
        targetRows = (Get-TargetBenchmarkCount) + 1; fullLoadSeconds = [Math]::Round($fullSeconds, 3)
        fullLoadRowsPerSecond = [Math]::Round($RowCount / $fullSeconds, 2)
        cdcMarkerLatencySeconds = [Math]::Round($cdcSeconds, 3); measuredAtUtc = $cdcObservedAt.ToString('o')
    } | ConvertTo-Json | Out-File -Encoding utf8 (Join-Path $resultDirectory 'metrics.json')
    Invoke-RestMethod -Uri "http://127.0.0.1:18080/job-info/$jobId" -TimeoutSec 10 | ConvertTo-Json -Depth 20 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'job-info.json')
    Invoke-RestMethod -Uri "http://127.0.0.1:18080/jobs/checkpoints/$jobId" -TimeoutSec 10 | ConvertTo-Json -Depth 20 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'checkpoints.json')
    Invoke-DockerCapture @('stats', '--no-stream', '--format', '{{.Name}} {{.CPUPerc}} {{.MemUsage}} {{.BlockIO}}', 'ds-poc-mysql', 'ds-poc-postgres', 'ds-poc-seatunnel') | Out-File -Encoding utf8 (Join-Path $resultDirectory 'docker-stats.txt')
    'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    Write-Host "Performance baseline passed. Evidence: $resultDirectory"
} catch {
    $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'failure.txt')
    'FAIL' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    throw
} finally {
    try {
        if ($jobId) {
            $stopBody = '{"jobId":' + $jobId + ',"isStopWithSavePoint":false,"force":true}'
            Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18080/stop-job' -ContentType 'application/json' -Body $stopBody -TimeoutSec 30 | Out-Null
        }
    } catch { $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'stop-error.txt') }
    try { Invoke-MySql $sourceSql | Out-Null } catch { $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'cleanup-source-error.txt') }
    try { Invoke-Postgres $targetSql | Out-Null } catch { $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'cleanup-target-error.txt') }
}
