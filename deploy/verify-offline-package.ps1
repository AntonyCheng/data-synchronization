[CmdletBinding()]
param(
    [string]$OutputDirectory = 'deploy\release\mvp'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$output = Join-Path $root $OutputDirectory
$required = @(
    'server\script\bin\start-backend-dev.ps1',
    'server\script\sql\ry_sync.sql',
    'deploy\migrate-platform-schema.ps1',
    'platform\docker-compose.yml',
    'deploy\local-stack\compose.yml',
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
    baseline = 'MVP / PRD v0.4'
    java = '21'
    frontend = 'plus-ui-react 6.0.0'
    engine = 'Apache SeaTunnel 2.3.13'
    included = $required
    excluded = @('platform\runtime', '.dev-runtime', 'deploy\local-stack\runtime', '**\node_modules', '**\.git', '*.env', '*secret*', '*password*')
    note = 'This manifest validates repository prerequisites; runtime data and credentials are never copied.'
}
$verificationManifestPath = Join-Path $output 'verification-manifest.json'
$manifest | ConvertTo-Json -Depth 5 | Set-Content -Encoding UTF8 $verificationManifestPath

$sensitive = @(Get-ChildItem $output -Recurse -File | Where-Object {
    $_.Name -eq '.env' -or (
        $_.Extension -in '.env', '.key', '.pem', '.properties', '.yml', '.yaml', '.json', '.txt' -and
        $_.Name -match '(?i)(password|secret|token|aes|credential)'
    )
})
if ($sensitive.Count -gt 0) {
    throw "Sensitive-looking files were created in the package directory: $($sensitive.Name -join ', ')"
}

Write-Host "OFFLINE_MANIFEST_PASS $($required.Count) prerequisites"
Write-Host ("Verification manifest: " + $verificationManifestPath)
