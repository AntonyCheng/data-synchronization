param(
    [string]$JobId
)

. (Join-Path $PSScriptRoot 'common.ps1')

if (-not $JobId) {
    $jobs = Invoke-RestMethod -Uri 'http://127.0.0.1:18080/running-jobs?page=1&rows=20' -TimeoutSec 10
    $job = @($jobs.data) | Select-Object -First 1
    if (-not $job) {
        throw 'No running POC job was found.'
    }
    $JobId = $job.jobId
}

$resultDirectory = New-PocResultDirectory
$pocId = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$marker = "pause-restore-$pocId"

Invoke-RestMethod -Uri "http://127.0.0.1:18080/jobs/checkpoints/$JobId" -TimeoutSec 10 |
    ConvertTo-Json -Depth 20 |
    Out-File -Encoding utf8 (Join-Path $resultDirectory 'checkpoints-before.json')

Invoke-PocDocker -Arguments @('exec', 'ds-poc-seatunnel', 'bash', '/opt/seatunnel/bin/seatunnel.sh', '-s', $JobId) |
    Out-File -Encoding utf8 (Join-Path $resultDirectory 'savepoint.txt')

Invoke-PocDocker -Arguments @(
    'exec', '-e', 'MYSQL_PWD=poc_root_pw', 'ds-poc-mysql', 'mysql', '-uroot', '--default-character-set=utf8mb4', '-e',
    "USE source_db; INSERT INTO customers (id,email,display_name,active,credit,profile,notes,registered_at) VALUES ($pocId,'poc-$pocId@example.com','Pause Restore',1,1.00,JSON_OBJECT('case','pause-restore'),'$marker',UTC_TIMESTAMP(6));"
)

$targetWhilePaused = & docker exec -e PGPASSWORD=poc_password ds-poc-postgres `
    psql -U poc -d sink_db -At -c "SELECT count(*) FROM public.customers WHERE id=$pocId;"
if (($targetWhilePaused -join '').Trim() -ne '0') {
    throw 'The target changed while the job was paused.'
}
$targetWhilePaused | Out-File -Encoding utf8 (Join-Path $resultDirectory 'target-while-paused.txt')

Invoke-PocDocker -Arguments @(
    'exec', 'ds-poc-seatunnel', 'bash', '/opt/seatunnel/bin/seatunnel.sh',
    '-r', $JobId, '-c', '/opt/seatunnel/config/poc-jobs/mysql-to-postgres-cdc.conf', '--async'
) |
    Out-File -Encoding utf8 (Join-Path $resultDirectory 'restore.txt')

$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
    $check = & docker exec -e PGPASSWORD=poc_password ds-poc-postgres psql -U poc -d sink_db -At -c `
        "SELECT count(*) FROM public.customers WHERE id = $pocId AND notes = '$marker';" 2>$null
    if (($check -join '').Trim() -eq '1') {
        Invoke-RestMethod -Uri "http://127.0.0.1:18080/jobs/checkpoints/$JobId" -TimeoutSec 10 |
            ConvertTo-Json -Depth 20 |
            Out-File -Encoding utf8 (Join-Path $resultDirectory 'checkpoints-after.json')
        'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
        Write-Host "Pause/restore verification passed. Evidence: $resultDirectory"
        exit 0
    }
    Start-Sleep -Seconds 3
}

throw 'Pause/restore verification failed or timed out.'
