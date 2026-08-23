. (Join-Path $PSScriptRoot 'common.ps1')

$resultDirectory = New-PocResultDirectory

try {
    & (Join-Path $PSScriptRoot 'up.ps1')

    $runningJobs = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=20' -TimeoutSec 10
    $runningJobsText = $runningJobs | ConvertTo-Json -Depth 20
    if ($runningJobsText -notmatch 'ds-poc-mysql-postgres-cdc') {
        & (Join-Path $PSScriptRoot 'submit-job.ps1')
    }

    & (Join-Path $PSScriptRoot 'verify-initial.ps1')
    & (Join-Path $PSScriptRoot 'apply-cdc-changes.ps1')
    & (Join-Path $PSScriptRoot 'verify-cdc.ps1')

    'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    Write-Host "POC passed. Evidence: $resultDirectory"
} catch {
    $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'failure.txt')
    'FAIL' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    throw
} finally {
    & (Join-Path $PSScriptRoot 'capture-result.ps1') -ResultDirectory $resultDirectory
}
