. (Join-Path $PSScriptRoot 'common.ps1')

if (-not (Test-Path (Join-Path $script:OraclePocRoot '.env'))) {
    throw 'Missing test/oracle/.env. Copy .env.example and set isolated Oracle test credentials.'
}
if (-not (Test-Path (Join-Path $script:OraclePocRoot 'vendor/ojdbc11.jar'))) {
    throw 'Missing Oracle JDBC driver: test/oracle/vendor/ojdbc11.jar. Do not download it during the build.'
}
if (-not (Test-Path (Join-Path $script:OraclePocRoot 'vendor/ojdbc11.jar.sha256'))) {
    throw 'Missing Oracle JDBC checksum file: test/oracle/vendor/ojdbc11.jar.sha256.'
}

$expectedHash = (Get-Content (Join-Path $script:OraclePocRoot 'vendor/ojdbc11.jar.sha256') -Raw).Trim().Split()[0].ToUpperInvariant()
if ($expectedHash -notmatch '^[A-F0-9]{64}$') { throw 'Oracle JDBC checksum must be a SHA-256 value.' }
$actualHash = (Get-FileHash (Join-Path $script:OraclePocRoot 'vendor/ojdbc11.jar') -Algorithm SHA256).Hash.ToUpperInvariant()
if ($actualHash -ne $expectedHash) { throw 'Oracle JDBC checksum verification failed.' }

& docker inspect --format '{{.State.Running}}' ds-poc-mysql *> $null
if ($LASTEXITCODE -ne 0) { throw 'Required isolated source container ds-poc-mysql is not running. Start the existing MySQL POC first.' }
& docker image inspect 'gvenzl/oracle-free:23-slim-faststart' *> $null
if ($LASTEXITCODE -ne 0) { throw 'Oracle image is missing. Pull gvenzl/oracle-free:23-slim-faststart before running this POC.' }

foreach ($directory in 'runtime/oracle', 'runtime/seatunnel/checkpoint', 'runtime/seatunnel/logs') {
    New-Item -ItemType Directory -Force -Path (Join-Path $script:OraclePocRoot $directory) | Out-Null
}

Invoke-OraclePocCompose up --detach --build
$deadline = (Get-Date).AddMinutes(8)
while ((Get-Date) -lt $deadline) {
    $health = & docker inspect --format '{{.State.Health.Status}}' ds-poc-oracle 2>$null
    if ($health -eq 'healthy') {
        Invoke-OracleSql '/poc-cases/oracle/001-target.sql'
        Write-Host 'Oracle POC environment is ready.'
        exit 0
    }
    Start-Sleep -Seconds 5
}

Invoke-OraclePocCompose ps
Invoke-OraclePocCompose logs --tail 100 oracle
throw 'Oracle POC environment did not become ready within 8 minutes.'
