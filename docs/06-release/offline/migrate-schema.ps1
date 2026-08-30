[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$packageRoot = $PSScriptRoot
$envFile = Join-Path $packageRoot '.env'
$compose = Join-Path $packageRoot 'compose.yml'
$migrationDirectory = Join-Path $packageRoot 'database\migrations'

if (-not (Test-Path $envFile)) { throw 'Missing .env. Copy .env.example and provide deployment credentials.' }
if (-not (Test-Path $migrationDirectory)) { throw 'Missing database/migrations.' }

Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if (!$line -or $line.StartsWith('#')) { return }
    $parts = $line.Split('=', 2)
    if ($parts.Count -eq 2) { Set-Item -Path "Env:$($parts[0].Trim())" -Value $parts[1].Trim() }
}

$containerId = (& docker compose --env-file $envFile --file $compose ps --quiet mysql).Trim()
if (!$containerId) { throw 'Offline MySQL container is not running.' }

$migrations = @(Get-ChildItem (Join-Path $migrationDirectory 'ry_sync_migration_*.sql') | Sort-Object Name)
if ($migrations.Count -eq 0) { throw 'No offline schema migrations were found.' }

foreach ($migration in $migrations) {
    Get-Content -Raw -Encoding UTF8 $migration.FullName |
        docker exec -i -e "MYSQL_PWD=$($env:MYSQL_ROOT_PASSWORD)" $containerId mysql --protocol=socket --default-character-set=utf8mb4 -uroot --show-warnings $env:MYSQL_DATABASE
    if ($LASTEXITCODE -ne 0) { throw "Migration failed: $($migration.Name)" }
}

Write-Host "Offline schema migration completed ($($migrations.Count) files)."
