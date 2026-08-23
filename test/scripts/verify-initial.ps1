. (Join-Path $PSScriptRoot 'common.ps1')

$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
    & docker exec -e PGPASSWORD=poc_password ds-poc-postgres `
        psql -v ON_ERROR_STOP=1 -U poc -d sink_db -f /poc-cases/postgres/verify-initial.sql *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host 'Initial snapshot verification passed.'
        exit 0
    }
    Start-Sleep -Seconds 3
}

throw 'Initial snapshot verification failed or timed out.'
