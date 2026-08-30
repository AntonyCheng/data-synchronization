Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Local POC endpoints must not be sent through a developer HTTP proxy.
$env:NO_PROXY = '127.0.0.1,localhost'
$env:no_proxy = $env:NO_PROXY

$script:TestRoot = Split-Path -Parent $PSScriptRoot
$script:ComposeFile = Join-Path $script:TestRoot 'docker-compose.yml'
$script:ProjectName = 'data-sync-poc'

function Invoke-PocCompose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    & docker compose --project-name $script:ProjectName --file $script:ComposeFile @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Invoke-PocDocker {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker failed with exit code $LASTEXITCODE"
    }
}

function New-PocResultDirectory {
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $resultDirectory = Join-Path $script:TestRoot "results/$timestamp"
    New-Item -ItemType Directory -Force -Path $resultDirectory | Out-Null
    return $resultDirectory
}
