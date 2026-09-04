<#
.SYNOPSIS
Local development backend launcher: reactor build (no fat jar) + mvn spring-boot:run.

.DESCRIPTION
Two steps:
  1. `mvn -pl ruoyi-admin -am -DskipTests -Dspring-boot.repackage.skip=true install`
     from server/ - builds every module and installs thin jars to the local repo.
     This is the whole reactor but WITHOUT the ~2 min spring-boot:repackage fat-jar step
     (~35 s warm, vs ~4 min for the old package path).
  2. `mvn spring-boot:run` from server/ruoyi-admin/ - the short `spring-boot` prefix
     resolves against ruoyi-admin's own pom (plugin version ${spring-boot.version}) and
     the goal runs on ruoyi-admin, not the aggregator. Business-module deps come from the
     freshly installed local repo. `optimizedLaunch` (plugin default) adds
     -XX:TieredStopAtLevel=1 for a faster boot.

start-backend-dev.ps1 remains the fat-jar / offline-package verification path.

.PARAMETER Fast
Also activate the dev-fast profile (workflow / LiteFlow off, lazy-init on) for a faster boot.

.PARAMETER Background
Start detached and return once ready; otherwise run in the foreground (Ctrl+C to stop).

.PARAMETER SkipInfrastructureCheck
Skip the dbs-mysql / dbs-redis running check.

.PARAMETER SkipBuild
Skip step 1 (the reactor install). Use when the local repo is already up to date
(e.g. dev.ps1 restart-backend has just installed the changed modules).

.PARAMETER StartupTimeoutSeconds
Readiness wait timeout in Background mode.
#>
[CmdletBinding()]
param(
    [switch]$Fast,
    [switch]$Background,
    [switch]$SkipInfrastructureCheck,
    [switch]$SkipBuild,
    [ValidateRange(30, 600)]
    [int]$StartupTimeoutSeconds = 240
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\common.ps1"

$repoRoot = Get-RepoRoot
$serverDirectory = Join-Path $repoRoot 'server'
$adminDirectory = Join-Path $serverDirectory 'ruoyi-admin'
$socketDirectory = Join-Path $repoRoot '.dev-runtime\jdk-sockets'
$runtimeDirectory = Join-Path $repoRoot '.dev-runtime\backend'
$port = 18081
$profiles = if ($Fast) { 'dev,dev-fast' } else { 'dev' }

# Load credential-encryption settings so existing ENC_ data-source passwords stay
# decryptable across restarts.
Import-BackendCredentialEnvironment

Assert-Java21
Test-RequiredCommand 'mvn'

if (-not $SkipInfrastructureCheck) {
    Test-RequiredCommand 'docker'
    Test-RequiredContainer 'dbs-mysql'
    Test-RequiredContainer 'dbs-redis'
}

$existing = @(Get-PortListeners -Port $port)
if ($existing.Count -gt 0) {
    throw "Port $port is already in use by process ID(s): $($existing -join ', '). Stop the running backend first (dev.ps1 down)."
}

New-Item -ItemType Directory -Force -Path $socketDirectory, $runtimeDirectory | Out-Null

# --- Step 1: reactor build + install, no fat jar --------------------------- #
if (-not $SkipBuild) {
    Write-Host "Building reactor (no fat jar): mvn -pl ruoyi-admin -am -Dspring-boot.repackage.skip=true install"
    Push-Location $serverDirectory
    try {
        & mvn '-pl' 'ruoyi-admin' '-am' '-DskipTests' '-Dspring-boot.repackage.skip=true' 'install'
        if ($LASTEXITCODE -ne 0) { throw "reactor build/install failed." }
    }
    finally {
        Pop-Location
    }
}

# --- Step 2: run --------------------------------------------------------- #
# spring-boot:run forks a new JVM when jvmArguments is set, so this flag takes effect
# in the app JVM. The socket dir path has no spaces, so it passes as one token.
$jvmArguments = "-Djdk.net.unixdomain.tmpdir=$socketDirectory"
$runArgs = @(
    '-DskipTests'
    "-Dspring-boot.run.profiles=$profiles"
    "-Dspring-boot.run.jvmArguments=$jvmArguments"
    'spring-boot:run'
)

Write-Host "Backend (spring-boot:run) on http://localhost:$port  profiles=[$profiles]"
Write-Host "Working dir: $adminDirectory"

if ($Background) {
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $stdoutPath = Join-Path $runtimeDirectory "backend-$timestamp.out.log"
    $stderrPath = Join-Path $runtimeDirectory "backend-$timestamp.err.log"

    $process = Start-Process -FilePath 'cmd.exe' `
        -ArgumentList (@('/c', 'mvn') + $runArgs) `
        -WorkingDirectory $adminDirectory `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru -WindowStyle Hidden

    Write-Host "Backend process PID $($process.Id)"
    Write-Host "  stdout: $stdoutPath"
    Write-Host "  stderr: $stderrPath"
    Write-Host "Waiting for readiness: http://localhost:$port/ (<= $StartupTimeoutSeconds s)"

    if (Wait-HttpOk -Uri "http://localhost:$port/" -TimeoutSeconds $StartupTimeoutSeconds -Headers @{}) {
        Write-Host 'Backend is ready.'
        [pscustomobject]@{ pid = $process.Id; startedAt = (Get-Date).ToString('o'); stdout = $stdoutPath } |
            ConvertTo-Json | Set-Content -Encoding UTF8 (Join-Path $runtimeDirectory 'backend.pid.json')
        exit 0
    }

    Write-Warning "Backend did not become ready within $StartupTimeoutSeconds s. Tail the log: $stdoutPath"
    exit 1
}

Push-Location $adminDirectory
try {
    & mvn @runArgs
}
finally {
    Pop-Location
}
