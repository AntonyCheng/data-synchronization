. (Join-Path $PSScriptRoot 'common.ps1')

$resultDirectory = New-PocResultDirectory
$submitOutput = Invoke-PocDocker -Arguments @(
    'exec', 'ds-poc-seatunnel', 'bash', '/opt/seatunnel/bin/seatunnel.sh',
    '--config', '/opt/seatunnel/config/poc-jobs/mysql-to-postgres-unique-key.conf',
    '--async', '--name', 'ds-poc-unique-key'
) | Out-String
$submitOutput | Out-File -Encoding utf8 (Join-Path $resultDirectory 'submit.txt')

$jobIdMatch = [regex]::Match($submitOutput, 'job id:\s*(\d+)', 'IgnoreCase')
if (-not $jobIdMatch.Success) {
    throw 'Could not find the unique-key job id in SeaTunnel output.'
}
$jobId = $jobIdMatch.Groups[1].Value
$jobId | Out-File -Encoding ascii (Join-Path $resultDirectory 'job-id.txt')

$deadline = (Get-Date).AddMinutes(3)
$snapshotPassed = $false
while ((Get-Date) -lt $deadline) {
    $count = & docker exec -e PGPASSWORD=poc_password ds-poc-postgres psql -U poc -d sink_db -At -c `
        "SELECT count(*) FROM public.inventory_by_sku;" 2>$null
    if (($count -join '').Trim() -eq '2') {
        $snapshotPassed = $true
        break
    }
    Start-Sleep -Seconds 3
}
if (-not $snapshotPassed) {
    throw 'Unique-key snapshot verification failed or timed out.'
}

Invoke-PocDocker -Arguments @(
    'exec', '-e', 'MYSQL_PWD=poc_root_pw', 'ds-poc-mysql', 'mysql', '-uroot', '-e',
    "USE source_db; UPDATE inventory_by_sku SET quantity=77 WHERE sku='SKU-001' AND warehouse_code='WH-B'; DELETE FROM inventory_by_sku WHERE sku='SKU-001' AND warehouse_code='WH-A'; INSERT INTO inventory_by_sku(sku,warehouse_code,quantity) VALUES('SKU-002','WH-A',25) ON DUPLICATE KEY UPDATE quantity=VALUES(quantity);"
)

$cdcPassed = $false
while ((Get-Date) -lt $deadline) {
    $assertion = & docker exec -e PGPASSWORD=poc_password ds-poc-postgres psql -U poc -d sink_db -At -c `
        "SELECT (count(*)=2 AND count(*) FILTER (WHERE sku='SKU-001' AND warehouse_code='WH-B' AND quantity=77)=1 AND count(*) FILTER (WHERE sku='SKU-002' AND warehouse_code='WH-A' AND quantity=25)=1 AND count(*) FILTER (WHERE sku='SKU-001' AND warehouse_code='WH-A')=0)::int FROM public.inventory_by_sku;" 2>$null
    if (($assertion -join '').Trim() -eq '1') {
        $cdcPassed = $true
        break
    }
    Start-Sleep -Seconds 3
}
if (-not $cdcPassed) {
    throw 'Unique-key CDC verification failed or timed out.'
}

Invoke-RestMethod -Uri "http://127.0.0.1:18080/jobs/checkpoints/$jobId" -TimeoutSec 10 |
    ConvertTo-Json -Depth 20 |
    Out-File -Encoding utf8 (Join-Path $resultDirectory 'checkpoints.json')
& docker exec -e PGPASSWORD=poc_password ds-poc-postgres psql -U poc -d sink_db -c `
    'SELECT * FROM public.inventory_by_sku ORDER BY sku,warehouse_code;' |
    Out-File -Encoding utf8 (Join-Path $resultDirectory 'target.txt')

Invoke-PocDocker -Arguments @(
    'exec', 'ds-poc-seatunnel', 'bash', '/opt/seatunnel/bin/seatunnel.sh', '-s', $jobId
) | Out-File -Encoding utf8 (Join-Path $resultDirectory 'savepoint.txt')

'PASS' | Out-File -Encoding ascii (Join-Path $resultDirectory 'result.txt')
Write-Host "Unique-key verification passed. Evidence: $resultDirectory"
