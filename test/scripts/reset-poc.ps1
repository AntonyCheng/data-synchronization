param(
    [switch]$Force
)

if (-not $Force) {
    throw 'Reset is destructive within test/runtime. Re-run with -Force after confirming the POC data can be discarded.'
}

. (Join-Path $PSScriptRoot 'common.ps1')

Invoke-PocCompose down

$runtimeRoot = Join-Path $script:TestRoot 'runtime'
$targets = @(
    (Join-Path $runtimeRoot 'mysql'),
    (Join-Path $runtimeRoot 'postgres'),
    (Join-Path $runtimeRoot 'seatunnel/checkpoint'),
    (Join-Path $runtimeRoot 'seatunnel/logs')
)

foreach ($target in $targets) {
    $resolvedRoot = [IO.Path]::GetFullPath($runtimeRoot)
    $resolvedTarget = [IO.Path]::GetFullPath($target)
    if (-not $resolvedTarget.StartsWith($resolvedRoot + [IO.Path]::DirectorySeparatorChar)) {
        throw "Refusing to remove path outside test/runtime: $resolvedTarget"
    }
    if (Test-Path -LiteralPath $resolvedTarget) {
        Remove-Item -LiteralPath $resolvedTarget -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $resolvedTarget | Out-Null
}

Write-Host 'POC runtime reset. Results were preserved; run up.ps1 to initialize a clean environment.'
