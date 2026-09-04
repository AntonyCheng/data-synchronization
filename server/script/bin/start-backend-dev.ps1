<#!
.SYNOPSIS
Starts the RuoYi backend locally as a packaged fat jar (production-like path).

.DESCRIPTION
Builds ruoyi-admin.jar from the current reactor sources and runs it. This is the
release / offline-package verification path and is intentionally close to how the
backend runs in delivery.

For the fast day-to-day development loop use run-backend-dev.ps1 (mvn spring-boot:run,
no fat jar, devtools hot restart) or the dev.ps1 orchestrator at the repo root.

The explicit ASCII socket directory avoids a JDK 21/Redisson compatibility issue
when the Windows user profile path contains non-ASCII characters.
#>
[CmdletBinding()]
param(
    [switch]$Background,
    [switch]$SkipInfrastructureCheck,
    [ValidateRange(10, 300)]
    [int]$StartupTimeoutSeconds = 180
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"

$projectRoot = Get-RepoRoot
$serverDirectory = Join-Path $projectRoot 'server'
$adminDirectory = Join-Path $projectRoot 'server\ruoyi-admin'
$socketDirectory = Join-Path $projectRoot '.dev-runtime\jdk-sockets'
$runtimeDirectory = Join-Path $projectRoot '.dev-runtime\backend'
$port = 18081

# Codex and long-lived terminals do not inherit user variables added after
# their process starts. Load the local credential-encryption settings so an
# existing ENC_ data-source password remains decryptable after a restart.
Import-BackendCredentialEnvironment

Assert-Java21
Test-RequiredCommand 'mvn'

if (-not $SkipInfrastructureCheck) {
    Test-RequiredCommand 'docker'
    Test-RequiredContainer 'dbs-mysql'
    Test-RequiredContainer 'dbs-redis'
}

$listeners = @(Get-PortListeners -Port $port)
if ($listeners.Count -gt 0) {
    throw "Port $port is already in use by process ID(s): $($listeners -join ', '). Stop the existing backend before starting another instance."
}

New-Item -ItemType Directory -Force -Path $socketDirectory, $runtimeDirectory | Out-Null

Write-Host "Starting backend with Java 21 on http://localhost:$port"
Write-Host "Socket temporary directory: $socketDirectory"

# Build from the reactor so ruoyi-admin always embeds current module sources.
Push-Location $serverDirectory
try {
    & mvn '-pl' 'ruoyi-admin' '-am' 'package' '-DskipTests'
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to package the backend from the current reactor sources."
    }
}
finally {
    Pop-Location
}

if ($Background) {
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $stdoutPath = Join-Path $runtimeDirectory "backend-$timestamp.out.log"
    $stderrPath = Join-Path $runtimeDirectory "backend-$timestamp.err.log"
    $jarPath = Join-Path $adminDirectory 'target\ruoyi-admin.jar'
    if (-not (Test-Path -LiteralPath $jarPath)) {
        throw "Packaged backend JAR was not found: $jarPath"
    }
    $javaArguments = @(
        "-Djdk.net.unixdomain.tmpdir=$socketDirectory",
        '-jar',
        $jarPath,
        '--spring.profiles.active=dev'
    )
    $process = Start-Process -FilePath 'java.exe' -ArgumentList $javaArguments -WorkingDirectory $adminDirectory `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru

    Write-Host "Backend process started with PID $($process.Id)."
    Write-Host "Standard output: $stdoutPath"
    Write-Host "Standard error: $stderrPath"
    Write-Host "Waiting for readiness check: http://localhost:$port/"
    if (-not (Wait-HttpOk -Uri "http://localhost:$port/" -TimeoutSeconds $StartupTimeoutSeconds -Headers @{})) {
        throw "Backend did not become healthy within $StartupTimeoutSeconds seconds. Check the runtime logs for details."
    }
    Write-Host "Backend is ready."
    exit 0
}

Push-Location $adminDirectory
try {
    $jarPath = Join-Path $adminDirectory 'target\ruoyi-admin.jar'
    & java "-Djdk.net.unixdomain.tmpdir=$socketDirectory" '-jar' $jarPath '--spring.profiles.active=dev'
}
finally {
    Pop-Location
}
