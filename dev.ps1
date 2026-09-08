<#
.SYNOPSIS
Local dev orchestrator for the data-synchronization platform.

.DESCRIPTION
Division of labour is unchanged:
  docker  -> base services (dbs-mysql / dbs-redis, optionally the SeaTunnel POC stack)
  java21  -> backend (reactor install without the fat jar, then mvn spring-boot:run)
  pnpm    -> frontend (Umi / Vite dev server)

Usage:
  .\dev.ps1 up   [-Poc] [-Fast] [-NoBuild]   containers(health-gated) -> migrations -> backend || frontend
  .\dev.ps1 down [-All]                       stop backend + frontend; -All also stops containers
  .\dev.ps1 status                            containers / ports / PIDs
  .\dev.ps1 restart-backend                   incremental reinstall -> relaunch backend
  .\dev.ps1 logs <backend|frontend>           tail the latest log

Flags:
  -Poc      also bring up deploy/local-stack/compose.yml (ds-poc-* stack; only needed to run sync jobs)
  -Fast     backend adds the dev-fast profile (workflow/LiteFlow off + lazy-init; faster, workflow pages 500)
  -NoBuild  backend = java -jar the existing fat jar (fastest, no devtools; falls back to a build if absent)
  -All      down only: also `docker compose stop`

ASCII only, Windows PowerShell 5.1 compatible (there is no pwsh on this machine).
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('up', 'down', 'status', 'restart-backend', 'logs', 'help')]
    [string]$Command = 'help',

    [Parameter(Position = 1)]
    [ValidateSet('backend', 'frontend')]
    [string]$Target,

    [switch]$Poc,
    [switch]$Fast,
    [switch]$NoBuild,
    [switch]$All
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. "$PSScriptRoot\server\script\bin\common.ps1"

# --------------------------------------------------------------------------- #
# Paths and constants
# --------------------------------------------------------------------------- #
$RepoRoot = $PSScriptRoot
$WebDir = Join-Path $RepoRoot 'web'
$ServerDir = Join-Path $RepoRoot 'server'
$RuntimeDir = Join-Path $RepoRoot '.dev-runtime'
$BackendLogDir = Join-Path $RuntimeDir 'backend'
$FrontendLogDir = Join-Path $RuntimeDir 'frontend'
$SocketDir = Join-Path $RuntimeDir 'jdk-sockets'
$StateFile = Join-Path $RuntimeDir 'dev-state.json'
$MigrateHashFile = Join-Path $RuntimeDir '.migrate-hash'

$PlatformCompose = Join-Path $RepoRoot 'platform\docker-compose.yml'
$PlatformProject = 'data-sync-platform'
$PocCompose = Join-Path $RepoRoot 'deploy\local-stack\compose.yml'
$PocProject = 'data-sync-poc'
$MigrateScript = Join-Path $RepoRoot 'deploy\migrate-platform-schema.ps1'
$VendorScript = Join-Path $RepoRoot 'deploy\local-stack\seatunnel\fetch-vendor.ps1'
$ConnectorPatchScript = Join-Path $RepoRoot 'deploy\local-stack\seatunnel\build-patched-connector.ps1'
$SqlDir = Join-Path $RepoRoot 'server\script\sql'

$BackendPort = 18081
$PlatformContainers = @('dbs-mysql', 'dbs-redis')
$PocContainers = @('ds-poc-mysql', 'ds-poc-mysql-target', 'ds-poc-postgres', 'ds-poc-kafka', 'ds-poc-seatunnel')

function Get-FrontendPort {
    $envFile = Join-Path $WebDir '.env.development'
    if (Test-Path $envFile) {
        $line = Get-Content $envFile | Where-Object { $_ -match '^\s*VITE_APP_PORT\s*=' } | Select-Object -First 1
        if ($line) {
            $value = ($line -split '=', 2)[1].Trim().Trim('"').Trim("'")
            if ($value -match '^\d+$') { return [int]$value }
        }
    }
    return 8000
}
$FrontendPort = Get-FrontendPort

# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
function Write-Step { param([string]$Text) Write-Host "==> $Text" -ForegroundColor Cyan }
function Write-Ok { param([string]$Text) Write-Host "  ok  $Text" -ForegroundColor Green }
function Write-Skip { param([string]$Text) Write-Host " skip $Text" -ForegroundColor DarkGray }
function Write-Warn { param([string]$Text) Write-Host " warn $Text" -ForegroundColor Yellow }

function Read-State {
    if (Test-Path $StateFile) {
        try { return (Get-Content -Raw $StateFile | ConvertFrom-Json) } catch { return $null }
    }
    return $null
}

function Write-DevState {
    param([hashtable]$State)
    New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
    [pscustomobject]$State | ConvertTo-Json | Set-Content -Encoding UTF8 $StateFile
}

function Invoke-Compose {
    param([string]$Project, [string]$File, [string[]]$ComposeArgs)
    # No | Out-Host: docker compose writes its progress UI (e.g. "Container X Recreate")
    # to stderr, and piping a native command's output under WinPS 5.1 turns those lines
    # into terminating NativeCommandError records ($ErrorActionPreference = 'Stop').
    # Nothing captures this function's return value, so plain passthrough is safe.
    & docker compose --project-name $Project --file $File @ComposeArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose ($Project) failed with exit code $LASTEXITCODE" }
}

function Get-LatestLog {
    param([string]$Dir, [string]$Pattern)
    if (-not (Test-Path $Dir)) { return $null }
    return Get-ChildItem -Path $Dir -Filter $Pattern -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
}

# --------------------------------------------------------------------------- #
# Migrations: only run when the migration script set changed
# --------------------------------------------------------------------------- #
function Invoke-MigrationsIfChanged {
    $files = @(Get-ChildItem (Join-Path $SqlDir 'ry_sync_migration_*.sql') | Sort-Object Name)
    $fingerprint = ($files | ForEach-Object { "$($_.Name):$($_.Length)" }) -join '|'
    $current = ''
    if (Test-Path $MigrateHashFile) { $current = (Get-Content -Raw $MigrateHashFile).Trim() }

    if ($fingerprint -eq $current) {
        Write-Skip "metadata migrations ($($files.Count) scripts, unchanged)"
        return
    }
    Write-Step "applying metadata migrations ($($files.Count) scripts)"
    # No | Out-Host, same reason as Invoke-Compose: the migration script's own tool
    # calls may write to stderr, and this function's return value isn't captured either.
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $MigrateScript
    if ($LASTEXITCODE -ne 0) { throw "migration failed (see output above)" }
    New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
    Set-Content -Encoding UTF8 -Path $MigrateHashFile -Value $fingerprint
    Write-Ok "migrations applied"
}

# --------------------------------------------------------------------------- #
# Frontend
# --------------------------------------------------------------------------- #
# web/.env.development and web/node_modules are gitignored, so a fresh clone has
# neither. Seed both from what the repo does carry (.env.example, pnpm-lock.yaml).
function Initialize-Frontend {
    $envFile = Join-Path $WebDir '.env.development'
    if (-not (Test-Path $envFile)) {
        Copy-Item (Join-Path $WebDir '.env.example') $envFile
        Write-Ok "created web/.env.development from .env.example"
    }
    if (-not (Test-Path (Join-Path $WebDir 'node_modules'))) {
        Write-Step "installing frontend dependencies (pnpm install)"
        Push-Location $WebDir
        try {
            & pnpm install | Out-Host
            if ($LASTEXITCODE -ne 0) { throw "pnpm install failed" }
        }
        finally { Pop-Location }
    }
}

function Start-Frontend {
    $existing = @(Get-PortListeners -Port $FrontendPort)
    if ($existing.Count -gt 0) {
        Write-Skip "frontend already on :$FrontendPort (PID $($existing -join ', '))"
        return $existing[0]
    }
    New-Item -ItemType Directory -Force -Path $FrontendLogDir | Out-Null
    $ts = Get-Date -Format 'yyyyMMdd-HHmmss'
    $outPath = Join-Path $FrontendLogDir "frontend-$ts.out.log"
    $errPath = Join-Path $FrontendLogDir "frontend-$ts.err.log"

    Write-Step "starting frontend (pnpm dev) -> :$FrontendPort"
    $proc = Start-Process -FilePath 'cmd.exe' -ArgumentList @('/c', 'pnpm', 'dev') `
        -WorkingDirectory $WebDir -RedirectStandardOutput $outPath -RedirectStandardError $errPath `
        -PassThru -WindowStyle Hidden
    Write-Host "  PID $($proc.Id)  log: $outPath"
    return $proc.Id
}

# --------------------------------------------------------------------------- #
# Backend - inline (same shape as Start-Frontend, no nested powershell).
#   step 1: reactor install, no fat jar - runs in the foreground so its progress
#           is visible; skipped with -NoBuild.
#   step 2: `mvn spring-boot:run` from ruoyi-admin/, detached via Start-Process.
# Returns the launched process PID (the cmd wrapper; taskkill /T reaches mvn+java).
# The caller waits on http://localhost:18081/ afterwards.
# --------------------------------------------------------------------------- #
function Start-Backend {
    param(
        [bool]$UseFast = [bool]$Fast,
        [bool]$SkipReactorBuild = [bool]$NoBuild
    )
    Import-BackendCredentialEnvironment
    New-Item -ItemType Directory -Force -Path $SocketDir, $BackendLogDir | Out-Null
    $activeProfiles = if ($UseFast) { 'dev,dev-fast' } else { 'dev' }

    if (-not $SkipReactorBuild) {
        Write-Step "reactor build (no fat jar): mvn -pl ruoyi-admin -am -Dspring-boot.repackage.skip=true install"
        Write-Host "  (~40s-2min; output streams below)" -ForegroundColor DarkGray
        Push-Location $ServerDir
        try {
            # | Out-Host: stream mvn output to the console WITHOUT letting it land in this
            # function's return value (that would poison $bePid -> dev-state.json).
            & mvn '-pl' 'ruoyi-admin' '-am' '-DskipTests' '-Dspring-boot.repackage.skip=true' 'install' | Out-Host
            if ($LASTEXITCODE -ne 0) { throw "reactor build/install failed." }
        }
        finally {
            Pop-Location
        }
    }

    $ts = Get-Date -Format 'yyyyMMdd-HHmmss'
    $outPath = Join-Path $BackendLogDir "backend-$ts.out.log"
    $errPath = Join-Path $BackendLogDir "backend-$ts.err.log"
    $jvmArgs = "-Djdk.net.unixdomain.tmpdir=$SocketDir"

    Write-Step "starting backend (spring-boot:run, profiles=[$activeProfiles]) -> :$BackendPort"
    $proc = Start-Process -FilePath 'cmd.exe' -ArgumentList @(
        '/c', 'mvn', '-DskipTests',
        "-Dspring-boot.run.profiles=$activeProfiles",
        "-Dspring-boot.run.jvmArguments=$jvmArgs",
        'spring-boot:run'
    ) -WorkingDirectory (Join-Path $ServerDir 'ruoyi-admin') `
        -RedirectStandardOutput $outPath -RedirectStandardError $errPath `
        -PassThru -WindowStyle Hidden
    Write-Host "  PID $($proc.Id)  log: $outPath"
    return [int]$proc.Id
}

# --------------------------------------------------------------------------- #
# up
# --------------------------------------------------------------------------- #
function Invoke-Up {
    Assert-Java21
    Test-RequiredCommand 'mvn'
    Test-RequiredCommand 'docker'
    Test-RequiredCommand 'pnpm'

    Initialize-Frontend
    $script:FrontendPort = Get-FrontendPort

    Write-Step "starting base containers ($PlatformProject)"
    Invoke-Compose -Project $PlatformProject -File $PlatformCompose -ComposeArgs @('up', '--detach')
    if (-not (Wait-DockerHealthy -Name $PlatformContainers -TimeoutSeconds 150)) {
        throw "containers not healthy within 150s: $($PlatformContainers -join ', ')"
    }
    Write-Ok "$($PlatformContainers -join ' / ') healthy"

    if ($Poc) {
        Write-Step "ensuring SeaTunnel connector JARs (deploy/local-stack/seatunnel/vendor)"
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $VendorScript | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "SeaTunnel vendor JAR provisioning failed (see output above)" }

        Write-Step "building GoldenDB-patched MySQL-CDC connector"
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $ConnectorPatchScript | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "GoldenDB connector patch build failed (see output above)" }

        Write-Step "starting SeaTunnel POC stack ($PocProject)"
        Invoke-Compose -Project $PocProject -File $PocCompose -ComposeArgs @('up', '--detach')
        Wait-DockerHealthy -Name $PocContainers -TimeoutSeconds 180 | Out-Null
        Write-Ok "POC stack up"
    }

    Invoke-MigrationsIfChanged

    # Frontend launches first (fully detached), then the backend build runs in the
    # foreground, so the two overlap. Both readiness checks happen at the end.
    $fePid = Start-Frontend

    $bePid = $null
    $backendWasRunning = $false
    $existingBe = @(Get-PortListeners -Port $BackendPort)
    if ($existingBe.Count -gt 0) {
        Write-Skip "backend already on :$BackendPort (PID $($existingBe -join ', '))"
        $bePid = $existingBe[0]
        $backendWasRunning = $true
    }
    else {
        $bePid = Start-Backend
    }

    if (-not $backendWasRunning) {
        Write-Step "waiting for backend http://localhost:$BackendPort/"
        if (Wait-HttpOk -Uri "http://localhost:$BackendPort/" -TimeoutSeconds 240 -Headers @{}) {
            Write-Ok "backend ready"
        }
        else {
            Write-Warn "backend not ready within 240s; check: .\dev.ps1 logs backend"
        }
    }

    Write-Step "waiting for frontend http://localhost:$FrontendPort/"
    if (Wait-HttpOk -Uri "http://localhost:$FrontendPort/" -TimeoutSeconds 120) {
        Write-Ok "frontend ready"
    }
    else {
        Write-Warn "frontend not ready within 120s; check: .\dev.ps1 logs frontend"
    }

    Write-DevState @{
        backendPid   = $bePid
        frontendPid  = $fePid
        backendPort  = $BackendPort
        frontendPort = $FrontendPort
        fast         = [bool]$Fast
        poc          = [bool]$Poc
        startedAt    = (Get-Date).ToString('o')
    }

    Write-Host ''
    Write-Host '----------------------------------------------' -ForegroundColor Green
    Write-Host "  frontend  http://localhost:$FrontendPort" -ForegroundColor Green
    Write-Host "  backend   http://localhost:$BackendPort" -ForegroundColor Green
    Write-Host "  login     admin / admin123  (captcha on)" -ForegroundColor Green
    if ($Fast) { Write-Host "  mode      -Fast (workflow / LiteFlow pages unavailable)" -ForegroundColor Yellow }
    Write-Host '----------------------------------------------' -ForegroundColor Green
    Write-Host "  stop   .\dev.ps1 down        logs  .\dev.ps1 logs backend|frontend"
}

# --------------------------------------------------------------------------- #
# down
# --------------------------------------------------------------------------- #
# Coerce a value that might be an int, a numeric string, an array, or junk into the
# integer PIDs it contains. Tolerates a poisoned dev-state.json.
function ConvertTo-PidList {
    param($Value)
    $out = @()
    foreach ($v in @($Value)) {
        if ($null -ne $v -and "$v" -match '^\d+$') { $out += [int]$v }
    }
    return $out
}

function Invoke-Down {
    $state = Read-State
    $killed = @()
    $statedBackendPid = if ($state) { $state.backendPid } else { $null }
    $statedFrontendPid = if ($state) { $state.frontendPid } else { $null }

    foreach ($entry in @(
            @{ name = 'backend'; procId = $statedBackendPid; port = $BackendPort },
            @{ name = 'frontend'; procId = $statedFrontendPid; port = $FrontendPort }
        )) {
        $pids = @()
        $pids += ConvertTo-PidList $entry.procId
        $pids += ConvertTo-PidList (Get-PortListeners -Port $entry.port)
        foreach ($p in @($pids | Sort-Object -Unique)) {
            Stop-ProcessTree -ProcessId $p
            $killed += "$($entry.name):$p"
        }
    }

    if ($killed.Count -gt 0) { Write-Ok "stopped $($killed -join ', ')" }
    else { Write-Skip "no backend / frontend processes running" }

    if (Test-Path $StateFile) { Remove-Item $StateFile -Force }

    if ($All) {
        Write-Step "stopping base containers"
        Invoke-Compose -Project $PlatformProject -File $PlatformCompose -ComposeArgs @('stop')
        if ($state -and $state.poc) {
            Invoke-Compose -Project $PocProject -File $PocCompose -ComposeArgs @('stop')
        }
        Write-Ok "containers stopped (data kept)"
    }
    else {
        Write-Host "  base containers still running (.\dev.ps1 down -All stops them too)" -ForegroundColor DarkGray
    }
}

# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #
function Invoke-Status {
    Write-Step "containers"
    foreach ($c in ($PlatformContainers + $PocContainers)) {
        $state = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $c 2>$null | Select-Object -First 1)
        if (-not $state) { $state = '(absent)' }
        Write-Host ("  {0,-24} {1}" -f $c, $state)
    }

    Write-Step "processes / ports"
    foreach ($svc in @(@{ n = 'backend '; p = $BackendPort }, @{ n = 'frontend'; p = $FrontendPort })) {
        $pids = @(Get-PortListeners -Port $svc.p)
        $desc = if ($pids.Count -gt 0) { "listening  PID $($pids -join ', ')" } else { 'not running' }
        Write-Host ("  {0}  :{1,-6} {2}" -f $svc.n, $svc.p, $desc)
    }

    $state = Read-State
    if ($state) {
        Write-Step "dev-state.json"
        $state | Format-List | Out-String | Write-Host
    }
}

# --------------------------------------------------------------------------- #
# restart-backend
# --------------------------------------------------------------------------- #
function Invoke-RestartBackend {
    $running = @(Get-PortListeners -Port $BackendPort)
    if ($running.Count -eq 0) {
        throw "backend is not running. Run: .\dev.ps1 up"
    }
    # Reinstall only the actively-developed modules (fast, no -am, no fat jar) so the
    # local repo picks up the source change, then stop + relaunch the app skipping the
    # full reactor build.
    Write-Step "reinstalling ruoyi-sync + ruoyi-admin (no fat jar)"
    Push-Location $ServerDir
    try {
        # Every arg must be quoted: PowerShell mangles an unquoted "-Dspring-boot.repackage.skip=true"
        # when calling mvn.cmd (a batch file) bare - it gets split into "-Dspring-boot" and
        # ".repackage.skip=true" as separate argv entries, and Maven then reads the latter as a
        # lifecycle phase. Quoting each token keeps it as one argv entry, as done in Start-Backend.
        & mvn '-pl' 'ruoyi-modules/ruoyi-sync,ruoyi-admin' '-DskipTests' '-Dspring-boot.repackage.skip=true' 'install' | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "incremental install failed" }
    }
    finally {
        Pop-Location
    }

    Write-Step "restarting backend process"
    foreach ($p in @(ConvertTo-PidList ($running + (Get-PortListeners -Port $BackendPort)) | Sort-Object -Unique)) {
        Stop-ProcessTree -ProcessId $p
    }
    Start-Sleep -Seconds 2

    # carry the -Fast choice from the last `up`, and relaunch inline (skip the reactor build)
    $state = Read-State
    $wasFast = [bool]($state -and $state.fast)
    $bePid = Start-Backend -UseFast $wasFast -SkipReactorBuild $true

    Write-Step "waiting for backend http://localhost:$BackendPort/"
    if (Wait-HttpOk -Uri "http://localhost:$BackendPort/" -TimeoutSeconds 240 -Headers @{}) {
        Write-Ok "backend restarted on :$BackendPort (PID $bePid)"
    }
    else {
        Write-Warn "backend not ready within 240s; check: .\dev.ps1 logs backend"
    }
}

# --------------------------------------------------------------------------- #
# logs
# --------------------------------------------------------------------------- #
function Invoke-Logs {
    if (-not $Target) { throw "usage: .\dev.ps1 logs backend|frontend" }
    if ($Target -eq 'backend') {
        $log = Get-LatestLog -Dir $BackendLogDir -Pattern 'backend-*.out.log'
    }
    else {
        $log = Get-LatestLog -Dir $FrontendLogDir -Pattern 'frontend-*.out.log'
    }
    if (-not $log) { throw "$Target has no log file yet" }
    Write-Host "tail -f $($log.FullName)  (Ctrl+C to exit)" -ForegroundColor Cyan
    Get-Content -Path $log.FullName -Tail 60 -Wait
}

function Show-Usage {
    Write-Host @"
dev.ps1 - local dev orchestrator

  .\dev.ps1 up   [-Poc] [-Fast] [-NoBuild]
  .\dev.ps1 down [-All]
  .\dev.ps1 status
  .\dev.ps1 restart-backend
  .\dev.ps1 logs <backend|frontend>
"@
}

# --------------------------------------------------------------------------- #
# dispatch
# --------------------------------------------------------------------------- #
switch ($Command) {
    'up' { Invoke-Up }
    'down' { Invoke-Down }
    'status' { Invoke-Status }
    'restart-backend' { Invoke-RestartBackend }
    'logs' { Invoke-Logs }
    default { Show-Usage }
}

# Reached only when the command above didn't throw. Without this, the process exit
# code falls back to whatever $LASTEXITCODE a native call (e.g. a transient
# `docker inspect` during Wait-DockerHealthy) last left behind, which can be
# nonzero even though everything above succeeded.
exit 0
