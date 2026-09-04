[CmdletBinding()]
param(
    [string]$OutputDirectory = 'deploy\release\mvp',
    [switch]$SkipImageExport,
    [string]$ImageArchivePath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$output = Join-Path $root $OutputDirectory
$template = Join-Path $root 'deploy\offline'

if (Test-Path $output) {
    $prohibited = @(@(
        Get-Item (Join-Path $output '.env') -ErrorAction SilentlyContinue
        Get-ChildItem (Join-Path $output 'runtime') -Recurse -File -ErrorAction SilentlyContinue
    ) | Where-Object { $_ })
    if ($prohibited.Count -gt 0) {
        throw "Offline package output contains deployment credentials or runtime data. Clear '$OutputDirectory' before rebuilding."
    }
}

$required = @(
    'server\ruoyi-admin\target\ruoyi-admin.jar',
    'web\dist\index.html',
    'server\script\sql\ry_vue.sql',
    'server\script\sql\ry_sync.sql',
    'platform\init\001-ry-vue.sh',
    'deploy\local-stack\seatunnel\config\seatunnel.yaml',
    'server\script\sql\ry_sync_migration_016.sql'
)
$missing = @($required | Where-Object { -not (Test-Path (Join-Path $root $_)) })
if ($missing.Count -gt 0) { throw "Offline package prerequisites are missing: $($missing -join ', ')" }

New-Item -ItemType Directory -Force -Path $output | Out-Null
foreach ($directory in 'backend', 'database', 'database\migrations', 'frontend', 'images', 'runtime\mysql', 'runtime\redis', 'runtime\seatunnel\checkpoint', 'runtime\seatunnel\logs', 'seatunnel') {
    New-Item -ItemType Directory -Force -Path (Join-Path $output $directory) | Out-Null
}

Copy-Item (Join-Path $template 'compose.yml') (Join-Path $output 'compose.yml') -Force
Copy-Item (Join-Path $template 'Caddyfile') (Join-Path $output 'Caddyfile') -Force
Copy-Item (Join-Path $template '.env.example') (Join-Path $output '.env.example') -Force
Copy-Item (Join-Path $template 'README.md') (Join-Path $output 'README.md') -Force
Copy-Item (Join-Path $template 'start-backend.ps1') (Join-Path $output 'start-backend.ps1') -Force
Copy-Item (Join-Path $template 'migrate-schema.ps1') (Join-Path $output 'migrate-schema.ps1') -Force
Copy-Item (Join-Path $root 'server\ruoyi-admin\target\ruoyi-admin.jar') (Join-Path $output 'backend\ruoyi-admin.jar') -Force
Copy-Item (Join-Path $root 'server\script\sql\ry_vue.sql') (Join-Path $output 'database\ry_vue.sql') -Force
Copy-Item (Join-Path $root 'server\script\sql\ry_sync.sql') (Join-Path $output 'database\ry_sync.sql') -Force
Copy-Item (Join-Path $root 'platform\init\001-ry-vue.sh') (Join-Path $output 'database\001-ry-vue.sh') -Force
Copy-Item (Join-Path $root 'deploy\local-stack\seatunnel\config\seatunnel.yaml') (Join-Path $output 'seatunnel\seatunnel.yaml') -Force
Copy-Item (Join-Path $root 'server\script\sql\ry_sync_migration_*.sql') (Join-Path $output 'database\migrations') -Force
Copy-Item (Join-Path $root 'web\dist\*') (Join-Path $output 'frontend') -Recurse -Force

if ($ImageArchivePath) {
    $archiveSource = Resolve-Path $ImageArchivePath -ErrorAction SilentlyContinue
    if (!$archiveSource) { throw "Specified image archive was not found: $ImageArchivePath" }
    Copy-Item $archiveSource (Join-Path $output 'images\data-sync-mvp-images.tar') -Force
}
elseif (!$SkipImageExport) {
    & docker image inspect mysql:8.0 redis:7-alpine caddy:2.8.4-alpine apache/kafka:3.8.0 data-sync-poc/seatunnel:2.3.13 *> $null
    if ($LASTEXITCODE -ne 0) { throw 'Required offline images are missing from the local Docker cache.' }
    & docker image save --output (Join-Path $output 'images\data-sync-mvp-images.tar') mysql:8.0 redis:7-alpine caddy:2.8.4-alpine apache/kafka:3.8.0 data-sync-poc/seatunnel:2.3.13
    if ($LASTEXITCODE -ne 0) { throw 'Docker image export failed.' }
}

$imageArchive = Join-Path $output 'images\data-sync-mvp-images.tar'
$files = @(Get-ChildItem $output -Recurse -File | Where-Object { $_.FullName -notmatch '\\runtime\\' })
$manifest = [ordered]@{
    generatedAt = (Get-Date).ToUniversalTime().ToString('o')
    product = 'data-synchronization-platform'
    baseline = 'MVP / PRD v0.4'
    imagesIncluded = Test-Path $imageArchive
    files = @($files | ForEach-Object { [ordered]@{ path = $_.FullName.Substring($output.Length + 1); bytes = $_.Length; sha256 = (Get-FileHash $_.FullName -Algorithm SHA256).Hash } })
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -Encoding utf8 (Join-Path $output 'manifest.json')
Write-Host "OFFLINE_PACKAGE_PASS $($files.Count) files"
Write-Host "Package: $output"
