param(
    [ValidateSet('checkpoint', 'binlog')]
    [string]$Scenario = 'checkpoint',
    [string]$JobId
)

. (Join-Path $PSScriptRoot 'common.ps1')

$resultDirectory = New-PocResultDirectory
$ErrorActionPreference = 'Stop'

function Invoke-SeatunnelCli {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    $output = & docker exec ds-poc-seatunnel bash /opt/seatunnel/bin/seatunnel.sh @Arguments 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    [pscustomobject]@{ Output = $output; ExitCode = $exitCode }
}

function Get-RunningJob {
    $jobs = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=50' -TimeoutSec 10
    if ($JobId) {
        return @($jobs.data | Where-Object { $_.jobId -eq $JobId } | Select-Object -First 1)
    }
    return @($jobs.data | Where-Object { $_.jobStatus -eq 'RUNNING' } | Select-Object -First 1)
}

try {
    $job = Get-RunningJob
    if (-not $job) {
        throw 'No running POC job was found. Start test/scripts/run-poc.ps1 first.'
    }
    $JobId = [string]$job.jobId
    $JobId | Out-File -Encoding ascii (Join-Path $resultDirectory 'job-id.txt')

    Invoke-RestMethod -Uri "http://127.0.0.1:18080/job-info/$JobId" -TimeoutSec 10 |
        ConvertTo-Json -Depth 30 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'job-info-before.json')
    Invoke-RestMethod -Uri "http://127.0.0.1:18080/jobs/checkpoints/$JobId" -TimeoutSec 10 |
        ConvertTo-Json -Depth 30 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'checkpoints-before.json')

    $savepoint = Invoke-SeatunnelCli '-s', $JobId
    $savepoint.Output | Out-File -Encoding utf8 (Join-Path $resultDirectory 'savepoint.txt')
    if ($savepoint.ExitCode -ne 0) {
        throw "Could not create savepoint for job $JobId."
    }

    $deadline = (Get-Date).AddMinutes(2)
    while ((Get-Date) -lt $deadline) {
        $running = Get-RunningJob
        if (-not $running) { break }
        Start-Sleep -Seconds 3
    }

    if ($Scenario -eq 'checkpoint') {
        $checkpointDirectory = Join-Path $script:TestRoot "runtime/seatunnel/checkpoint/$JobId"
        $checkpointFile = Get-ChildItem -LiteralPath $checkpointDirectory -File |
            Where-Object { $_.Extension -eq '.ser' } |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if (-not $checkpointFile) {
            throw "No checkpoint state file was found under $checkpointDirectory."
        }

        $backupFile = Join-Path $resultDirectory 'checkpoint-backup.ser'
        Copy-Item -LiteralPath $checkpointFile.FullName -Destination $backupFile -Force
        [IO.File]::WriteAllBytes($checkpointFile.FullName, [Text.Encoding]::ASCII.GetBytes('corrupted checkpoint state'))
        $checkpointFile.FullName | Out-File -Encoding utf8 (Join-Path $resultDirectory 'corrupted-file.txt')

        $failedRestore = Invoke-SeatunnelCli '-r', $JobId, '-c', '/opt/seatunnel/config/poc-jobs/mysql-to-postgres-cdc.conf', '--async'
        $failedRestore.Output | Out-File -Encoding utf8 (Join-Path $resultDirectory 'restore-corrupted.txt')
        if ($failedRestore.ExitCode -eq 0) {
            throw 'Corrupted checkpoint restore unexpectedly succeeded.'
        }
        if ($failedRestore.Output -notmatch '(?i)checkpoint|savepoint|restore|state') {
            throw 'Restore failed, but the engine output did not identify a checkpoint/savepoint/state error.'
        }

        Copy-Item -LiteralPath $backupFile -Destination $checkpointFile.FullName -Force
        $restored = Invoke-SeatunnelCli '-r', $JobId, '-c', '/opt/seatunnel/config/poc-jobs/mysql-to-postgres-cdc.conf', '--async'
        $restored.Output | Out-File -Encoding utf8 (Join-Path $resultDirectory 'restore-repaired.txt')
        if ($restored.ExitCode -ne 0) {
            throw 'Restore after repairing the checkpoint failed.'
        }
    } else {
        $purge = & docker exec -e MYSQL_PWD=poc_root_pw ds-poc-mysql mysql -uroot -N -B -e `
            "RESET MASTER;" 2>&1 | Out-String
        $purge | Out-File -Encoding utf8 (Join-Path $resultDirectory 'reset-master.txt')
        if ($LASTEXITCODE -ne 0) {
            throw 'Could not reset the isolated POC MySQL binlog.'
        }

        $failedRestore = Invoke-SeatunnelCli '-r', $JobId, '-c', '/opt/seatunnel/config/poc-jobs/mysql-to-postgres-cdc.conf', '--async'
        $failedRestore.Output | Out-File -Encoding utf8 (Join-Path $resultDirectory 'restore-expired-binlog.txt')
        $jobFailure = $null
        $deadline = (Get-Date).AddMinutes(2)
        while ((Get-Date) -lt $deadline) {
            try {
                $jobInfo = Invoke-RestMethod -Uri "http://127.0.0.1:18080/job-info/$JobId" -TimeoutSec 10
                if ($jobInfo.jobStatus -eq 'FAILED') {
                    $jobFailure = $jobInfo
                    break
                }
            } catch { }
            Start-Sleep -Seconds 3
        }
        if (-not $jobFailure) {
            throw 'Restore after RESET MASTER did not enter FAILED state within the verification window.'
        }
        $jobFailure | ConvertTo-Json -Depth 30 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'job-info-after.json')
        $failureText = [string]$jobFailure.errorMsg
        if ($failureText -notmatch '(?i)binlog|offset|checkpoint|restore|gtid|log|no longer available') {
            throw 'The failed job did not identify a binlog/offset/recovery error.'
        }
    }

    'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    Write-Host "Recovery drill ($Scenario) passed. Evidence: $resultDirectory"
} catch {
    $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'failure.txt')
    'FAIL' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
    throw
} finally {
    try {
        Invoke-RestMethod -Uri "http://127.0.0.1:18080/running-jobs?page=1&rows=50" -TimeoutSec 10 |
            ConvertTo-Json -Depth 30 | Out-File -Encoding utf8 (Join-Path $resultDirectory 'running-jobs.json')
    } catch {
        $_ | Out-File -Encoding utf8 (Join-Path $resultDirectory 'running-jobs-error.txt')
    }
}
