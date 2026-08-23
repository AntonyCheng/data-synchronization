. (Join-Path $PSScriptRoot 'common.ps1')

$deadline = (Get-Date).AddMinutes(5)

while ((Get-Date) -lt $deadline) {
    $mysqlHealth = & docker inspect --format '{{.State.Health.Status}}' ds-poc-mysql 2>$null
    $postgresHealth = & docker inspect --format '{{.State.Health.Status}}' ds-poc-postgres 2>$null
    $restReady = $false

    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:18080/overview' -TimeoutSec 3
        $restReady = $response.StatusCode -eq 200
    } catch {
        $restReady = $false
    }

    if ($mysqlHealth -eq 'healthy' -and $postgresHealth -eq 'healthy' -and $restReady) {
        Write-Host 'MySQL, PostgreSQL and SeaTunnel REST API are ready.'
        exit 0
    }

    Start-Sleep -Seconds 3
}

Invoke-PocCompose ps
Invoke-PocCompose logs --tail 100 seatunnel
throw 'POC environment did not become ready within 5 minutes.'
