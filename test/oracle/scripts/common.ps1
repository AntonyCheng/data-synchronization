Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$env:NO_PROXY = '127.0.0.1,localhost'
$env:no_proxy = $env:NO_PROXY

$script:OraclePocRoot = Split-Path -Parent $PSScriptRoot
$script:OracleComposeFile = Join-Path $script:OraclePocRoot 'docker-compose.yml'
$script:OracleProjectName = 'data-sync-oracle-poc'

function Invoke-OraclePocCompose {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    & docker compose --project-name $script:OracleProjectName --env-file (Join-Path $script:OraclePocRoot '.env') --file $script:OracleComposeFile @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Oracle POC docker compose failed with exit code $LASTEXITCODE" }
}

function Invoke-OracleSql {
    param([string]$SqlFile)

    & docker exec ds-poc-oracle sqlplus -sL 'poc/poc_password@//localhost:1521/FREEPDB1' "@$SqlFile"
    if ($LASTEXITCODE -ne 0) { throw "Oracle SQL failed: $SqlFile" }
}
