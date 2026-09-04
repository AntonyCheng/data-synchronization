[CmdletBinding()]
param(
    [ValidatePattern('^dbs-[a-z0-9-]+$')]
    [string]$MetadataDbContainer = 'dbs-mysql',
    [string]$MetadataDatabase = 'ry-vue',
    [string]$MetadataDbPassword = $(if ($env:PLATFORM_MYSQL_ROOT_PASSWORD) { $env:PLATFORM_MYSQL_ROOT_PASSWORD } else { 'root' })
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'docker is required.'
}

$running = @(docker inspect --format '{{.State.Running}}' $MetadataDbContainer 2>$null)
if ($LASTEXITCODE -ne 0 -or $running.Count -eq 0 -or $running[0] -ne 'true') {
    throw "Metadata container '$MetadataDbContainer' is not running."
}

$databaseExists = docker exec -e "MYSQL_PWD=$MetadataDbPassword" $MetadataDbContainer mysql -uroot -N -B -e "select count(*) from information_schema.schemata where schema_name='$MetadataDatabase'" 2>$null
if ($LASTEXITCODE -ne 0 -or ([int]($databaseExists | Select-Object -First 1)) -ne 1) {
    throw "Metadata database '$MetadataDatabase' was not found in '$MetadataDbContainer'."
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$migrationDirectory = Join-Path $projectRoot 'server\script\sql'
$migrationFiles = @(Get-ChildItem (Join-Path $migrationDirectory 'ry_sync_migration_*.sql') | Sort-Object Name)
if ($migrationFiles.Count -eq 0) {
    throw "No migration files found in '$migrationDirectory'."
}

Write-Host "Migrating $MetadataDatabase on $MetadataDbContainer ($($migrationFiles.Count) files)"
foreach ($file in $migrationFiles) {
    Write-Host ("APPLY " + $file.Name)
    Get-Content -Raw -Encoding UTF8 $file.FullName |
        docker exec -i -e "MYSQL_PWD=$MetadataDbPassword" $MetadataDbContainer mysql --protocol=socket --default-character-set=utf8mb4 -uroot --show-warnings $MetadataDatabase
    if ($LASTEXITCODE -ne 0) {
        throw "Migration failed: $($file.Name)"
    }
}

Write-Host 'Migration completed successfully.'
