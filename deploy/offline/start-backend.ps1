[CmdletBinding()]
param(
    [switch]$Background,
    [switch]$SkipSchemaMigration,
    [ValidateRange(10, 300)]
    [int]$StartupTimeoutSeconds = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$packageRoot = $PSScriptRoot
$envFile = Join-Path $packageRoot '.env'
$jar = Join-Path $packageRoot 'backend\ruoyi-admin.jar'
$socketDirectory = Join-Path $packageRoot 'runtime\jdk-sockets'
$logDirectory = Join-Path $packageRoot 'runtime\backend'

if (-not (Test-Path $envFile)) { throw 'Missing .env. Copy .env.example and provide deployment credentials.' }
if (-not (Test-Path $jar)) { throw 'Missing backend/ruoyi-admin.jar.' }

if (!$SkipSchemaMigration) {
    & (Join-Path $packageRoot 'migrate-schema.ps1')
}

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if (!$line -or $line.StartsWith('#')) { return }
    $parts = $line.Split('=', 2)
    if ($parts.Count -eq 2) { Set-Item -Path "Env:$($parts[0].Trim())" -Value $parts[1].Trim() }
}

if ((& java -version 2>&1 | Out-String) -notmatch 'version "21\.') { throw 'Java 21 is required.' }

foreach ($required in 'MYSQL_ROOT_PASSWORD', 'REDIS_PASSWORD', 'SYNC_CREDENTIAL_ENCRYPTION_PASSWORD') {
    if ([string]::IsNullOrWhiteSpace((Get-Item "Env:$required" -ErrorAction SilentlyContinue).Value)) {
        throw "Required deployment setting '$required' is empty."
    }
}

$env:SERVER_PORT = $env:BACKEND_PORT
$env:SPRING_PROFILES_ACTIVE = 'dev'
$env:SPRING_DATASOURCE_DYNAMIC_DATASOURCE_MASTER_URL = "jdbc:mysql://localhost:$($env:MYSQL_PORT)/$($env:MYSQL_DATABASE)?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=GMT%2B8&autoReconnect=true&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true&nullCatalogMeansCurrent=true"
$env:SPRING_DATASOURCE_DYNAMIC_DATASOURCE_MASTER_USERNAME = 'root'
$env:SPRING_DATASOURCE_DYNAMIC_DATASOURCE_MASTER_PASSWORD = $env:MYSQL_ROOT_PASSWORD
$env:SPRING_DATA_REDIS_HOST = 'localhost'
$env:SPRING_DATA_REDIS_PORT = $env:REDIS_PORT
$env:SPRING_DATA_REDIS_PASSWORD = $env:REDIS_PASSWORD
$env:SEATUNNEL_ENDPOINT = "http://localhost:$($env:SEATUNNEL_PORT)"

New-Item -ItemType Directory -Force -Path $socketDirectory, $logDirectory | Out-Null
$arguments = @("-Djdk.net.unixdomain.tmpdir=$socketDirectory", '-jar', $jar)

if (!$Background) {
    & java @arguments
    exit $LASTEXITCODE
}

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$process = Start-Process -FilePath 'java' -ArgumentList $arguments -WorkingDirectory $packageRoot -RedirectStandardOutput (Join-Path $logDirectory "backend-$timestamp.out.log") -RedirectStandardError (Join-Path $logDirectory "backend-$timestamp.err.log") -PassThru
$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
while ((Get-Date) -lt $deadline) {
    try {
        if ((Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:$($env:BACKEND_PORT)/" -TimeoutSec 3).StatusCode -eq 200) {
            Write-Host "Backend ready on http://localhost:$($env:BACKEND_PORT) (PID $($process.Id))."
            exit 0
        }
    } catch { Start-Sleep -Seconds 2 }
}

Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
throw "Backend did not become ready. Review $logDirectory."
