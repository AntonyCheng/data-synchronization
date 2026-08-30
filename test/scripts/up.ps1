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

$vendorReady = (Test-Path (Join-Path $script:TestRoot 'docker/seatunnel/vendor/connectors')) -and
    (Test-Path (Join-Path $script:TestRoot 'docker/seatunnel/vendor/drivers'))
$imageReady = $false
try {
    & docker image inspect 'data-sync-poc/seatunnel:2.3.13' *> $null
    $imageReady = $LASTEXITCODE -eq 0
} catch {
    $imageReady = $false
}

if ($vendorReady) {
    Invoke-PocCompose up --detach --build
} elseif ($imageReady) {
    Write-Host 'SeaTunnel vendor JARs are not present; reusing data-sync-poc/seatunnel:2.3.13.'
    Invoke-PocCompose up --detach
} else {
    throw 'SeaTunnel vendor JARs are missing and data-sync-poc/seatunnel:2.3.13 is not available. Pull/build the POC image first.'
}
& (Join-Path $PSScriptRoot 'wait-ready.ps1')
