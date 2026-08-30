param(
    [string]$TaskId
)

. (Join-Path $PSScriptRoot 'common.ps1')

$resultDirectory = New-PocResultDirectory
$containers = @('ds-poc-mysql', 'ds-poc-postgres', 'ds-poc-seatunnel')
$containerStates = @{}
foreach ($container in $containers) {
    $state = (& docker inspect --format '{{.State.Status}}' $container 2>$null | Out-String).Trim()
    $containerStates[$container] = $state
}
$containerStates | ConvertTo-Json | Out-File -Encoding utf8 (Join-Path $resultDirectory 'containers.json')
if ($containerStates.Values | Where-Object { $_ -ne 'running' }) {
    throw 'One or more POC containers are not running.'
}

$sourceRows = (& docker exec ds-poc-mysql mysql -useatunnel -pseatunnel_pw -N -B source_db -e 'SELECT COUNT(*) FROM source_db.customers;' | Out-String).Trim()
$targetRows = (& docker exec ds-poc-postgres psql -U poc -d sink_db -At -c 'SELECT COUNT(*) FROM public.customers;' | Out-String).Trim()
[ordered]@{ sourceRows = $sourceRows; targetRows = $targetRows; matched = $sourceRows -eq $targetRows } |
    ConvertTo-Json | Out-File -Encoding utf8 (Join-Path $resultDirectory 'row-counts.json')
if ($sourceRows -ne $targetRows) {
    throw "Source and target row counts differ: $sourceRows vs $targetRows."
}

$jobs = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=50' -TimeoutSec 10
$runningJobs = @($jobs.data)
$runningJobs | ConvertTo-Json -Depth 20 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'running-jobs.json')
if ($TaskId) {
    $matches = @($runningJobs | Where-Object { $_.jobName -eq "ds-task-$TaskId" })
    if ($matches.Count -ne 1) {
        throw "Expected exactly one running job for task $TaskId, found $($matches.Count)."
    }
}

'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
Write-Host "MVP stability check passed. Evidence: $resultDirectory"
