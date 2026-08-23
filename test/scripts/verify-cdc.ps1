. (Join-Path $PSScriptRoot 'common.ps1')

$deadline = (Get-Date).AddMinutes(3)
while ((Get-Date) -lt $deadline) {
    & docker exec -e PGPASSWORD=poc_password ds-poc-postgres `
        psql -v ON_ERROR_STOP=1 -U poc -d sink_db -f /poc-cases/postgres/verify-cdc.sql *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host 'CDC INSERT/UPDATE/DELETE verification passed.'
        exit 0
    }
    Start-Sleep -Seconds 3
}

throw 'CDC verification failed or timed out.'
