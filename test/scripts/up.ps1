. (Join-Path $PSScriptRoot 'common.ps1')

$runtimeDirectories = @(
    (Join-Path $script:TestRoot 'runtime/mysql'),
    (Join-Path $script:TestRoot 'runtime/postgres'),
    (Join-Path $script:TestRoot 'runtime/seatunnel/checkpoint'),
    (Join-Path $script:TestRoot 'runtime/seatunnel/logs'),
    (Join-Path $script:TestRoot 'results')
)

foreach ($runtimeDirectory in $runtimeDirectories) {
    New-Item -ItemType Directory -Force -Path $runtimeDirectory | Out-Null
}

Invoke-PocCompose up --detach --build
& (Join-Path $PSScriptRoot 'wait-ready.ps1')
