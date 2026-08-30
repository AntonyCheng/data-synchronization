<#!
.SYNOPSIS
Starts the RuoYi backend locally with the development profile.

.DESCRIPTION
Uses the local Java 21 and Maven installation. The explicit ASCII socket
directory avoids a JDK 21/Redisson compatibility issue when the Windows user
profile path contains non-ASCII characters.
#>
[CmdletBinding()]
param(
    [switch]$Background,
    [switch]$SkipInfrastructureCheck,
    [ValidateRange(10, 300)]
    [int]$StartupTimeoutSeconds = 90
)

$ErrorActionPreference = 'Stop'

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$adminDirectory = Join-Path $projectRoot 'server\ruoyi-admin'
$socketDirectory = Join-Path $projectRoot 'test\runtime\jdk-sockets'
$runtimeDirectory = Join-Path $projectRoot 'test\runtime\backend'
$port = 18081

function Test-RequiredCommand {
    param([string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found in PATH."
    }
}

function Test-RequiredContainer {
    param([string]$Name)

    $running = @(& docker inspect --format '{{.State.Running}}' $Name 2>$null)
    $dockerExitCode = $LASTEXITCODE
    if ($dockerExitCode -ne 0 -or $running[0] -ne 'true') {
        throw "Required Docker container '$Name' is not running. Start the local infrastructure first."
    }
}

function Wait-BackendReady {
    param([int]$TimeoutSeconds)

    # Use the public root endpoint as the readiness probe. Authentication and
    # captcha endpoints may be filtered or rate-limited while the dispatcher
    # is still wiring, even after the embedded server has started listening.
    $healthUri = "http://localhost:$port/"
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $healthUri -TimeoutSec 3
            if ($response.StatusCode -eq 200) {
                return
            }
        }
        catch {
            # The embedded server may return a transient error before Spring
            # finishes wiring its request dispatcher.
        }
        Start-Sleep -Seconds 2
    }

    throw "Backend did not become healthy within $TimeoutSeconds seconds. Check the runtime logs for details."
}

Test-RequiredCommand 'java'
Test-RequiredCommand 'mvn'

# java -version writes its version banner to stderr. Run it through cmd so the
# banner is collected as ordinary output under PowerShell's strict error mode.
$javaVersion = (& cmd.exe /c 'java -version 2>&1' | Out-String)
if ($javaVersion -notmatch 'version "21\.') {
    throw "Java 21 is required. Current Java version output: $javaVersion"
}

if (-not $SkipInfrastructureCheck) {
    Test-RequiredCommand 'docker'
    Test-RequiredContainer 'dbs-mysql'
    Test-RequiredContainer 'dbs-redis'
}

$listeners = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
if ($listeners.Count -gt 0) {
    $pids = ($listeners | Select-Object -ExpandProperty OwningProcess -Unique) -join ', '
    throw "Port $port is already in use by process ID(s): $pids. Stop the existing backend before starting another instance."
}

New-Item -ItemType Directory -Force -Path $socketDirectory, $runtimeDirectory | Out-Null

$mavenArguments = @(
    'spring-boot:run',
    '-Dspring-boot.run.profiles=dev',
    "-Dspring-boot.run.jvmArguments=-Djdk.net.unixdomain.tmpdir=$socketDirectory"
)

Write-Host "Starting backend with Java 21 on http://localhost:$port"
Write-Host "Socket temporary directory: $socketDirectory"

if ($Background) {
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $stdoutPath = Join-Path $runtimeDirectory "backend-$timestamp.out.log"
    $stderrPath = Join-Path $runtimeDirectory "backend-$timestamp.err.log"
    $process = Start-Process -FilePath 'mvn.cmd' -ArgumentList $mavenArguments -WorkingDirectory $adminDirectory `
        -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru

    Write-Host "Backend process started with PID $($process.Id)."
    Write-Host "Standard output: $stdoutPath"
    Write-Host "Standard error: $stderrPath"
    Write-Host "Waiting for readiness check: http://localhost:$port/"
    Wait-BackendReady -TimeoutSeconds $StartupTimeoutSeconds
    Write-Host "Backend is ready."
    exit 0
}

Push-Location $adminDirectory
try {
    & mvn @mavenArguments
}
finally {
    Pop-Location
}
