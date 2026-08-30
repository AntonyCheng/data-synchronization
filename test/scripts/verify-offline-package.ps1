[CmdletBinding()]
param(
    [string]$OutputDirectory = 'test\release\mvp'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$output = Join-Path $root $OutputDirectory
$required = @(
    'docs\01-product\数据同步平台_PRD.md',
    'docs\05-testing\release-checklist.md',
    'server\script\bin\start-backend-dev.ps1',
    'server\script\sql\ry_sync.sql',
    'test\scripts\migrate-platform-schema.ps1',
    'platform\docker-compose.yml',
    'test\docker-compose.yml',
    'web\dist\index.html',
    'server\ruoyi-admin\target\ruoyi-admin.jar'
)

New-Item -ItemType Directory -Force -Path $output | Out-Null
$missing = @($required | Where-Object { -not (Test-Path (Join-Path $root $_)) })
if ($missing.Count -gt 0) {
    throw "Offline package prerequisites are missing: $($missing -join ', ')"
}

$manifest = [ordered]@{
    generatedAt = (Get-Date).ToUniversalTime().ToString('o')
    product = 'data-synchronization-platform'
    baseline = 'MVP / PRD v0.3'
    java = '21'
    frontend = 'plus-ui-react 6.0.0'
    engine = 'Apache SeaTunnel 2.3.13'
    included = $required
    excluded = @('platform\runtime', 'test\runtime', 'test\results', '**\node_modules', '**\.git', '*.env', '*secret*', '*password*')
    note = 'This manifest validates repository prerequisites; runtime data and credentials are never copied.'
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -Encoding UTF8 (Join-Path $output 'manifest.json')

$sensitive = @(Get-ChildItem $output -Recurse -File | Where-Object { $_.Name -match '(?i)(password|secret|token|aes|credential)' })
if ($sensitive.Count -gt 0) {
    throw "Sensitive-looking files were created in the package directory: $($sensitive.Name -join ', ')"
}

Write-Host "OFFLINE_MANIFEST_PASS $($required.Count) prerequisites"
Write-Host ("Manifest: " + (Join-Path $output 'manifest.json'))
