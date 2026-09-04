<#
.SYNOPSIS
Shared helpers for the data-synchronization local dev scripts.

.DESCRIPTION
Dot-sourced by server/script/bin/*.ps1 and the repo-root dev.ps1:

    . "$PSScriptRoot\common.ps1"

Do not run this file directly. ASCII only, Windows PowerShell 5.1 compatible.
#>

Set-StrictMode -Version Latest

# Repo root. This file lives at server/script/bin/, three levels below the root.
# $PSScriptRoot always points at this file's own directory, even when dot-sourced elsewhere.
function Get-RepoRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
}

# Import a User-scope environment variable into the current process if not already set.
# Long-lived terminals / agents do not inherit User variables added after they started.
function Import-UserEnvironmentSetting {
    param([Parameter(Mandatory)][string]$Name)

    if (-not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($Name, 'Process'))) {
        return
    }
    $userValue = [Environment]::GetEnvironmentVariable($Name, 'User')
    if (-not [string]::IsNullOrWhiteSpace($userValue)) {
        [Environment]::SetEnvironmentVariable($Name, $userValue, 'Process')
    }
}

# Credential-encryption settings must be present or existing ENC_ data-source passwords
# cannot be decrypted after a restart.
function Import-BackendCredentialEnvironment {
    Import-UserEnvironmentSetting 'SYNC_CREDENTIAL_ENCRYPTION_ENABLED'
    Import-UserEnvironmentSetting 'SYNC_CREDENTIAL_ENCRYPTION_PASSWORD'
}

function Test-RequiredCommand {
    param([Parameter(Mandatory)][string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found in PATH."
    }
}

# java -version writes its banner to stderr; run it through cmd so it is captured as output.
function Assert-Java21 {
    Test-RequiredCommand 'java'
    $javaVersion = (& cmd.exe /c 'java -version 2>&1' | Out-String)
    if ($javaVersion -notmatch 'version "21\.') {
        throw "Java 21 is required. Current java -version output:`n$javaVersion"
    }
}

# True when the named Docker container is running. Never throws.
function Test-DockerContainerRunning {
    param([Parameter(Mandatory)][string]$Name)

    $running = @(& docker inspect --format '{{.State.Running}}' $Name 2>$null)
    return ($LASTEXITCODE -eq 0 -and $running.Count -gt 0 -and $running[0] -eq 'true')
}

function Test-RequiredContainer {
    param([Parameter(Mandatory)][string]$Name)

    if (-not (Test-DockerContainerRunning -Name $Name)) {
        throw "Required Docker container '$Name' is not running. Start the infrastructure first (dev.ps1 up)."
    }
}

# Wait until each container reports health=healthy. Containers with no healthcheck
# count as ready once status=running.
function Wait-DockerHealthy {
    param(
        [Parameter(Mandatory)][string[]]$Name,
        [int]$TimeoutSeconds = 120
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $pending = @()
        foreach ($container in $Name) {
            $state = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $container 2>$null | Select-Object -First 1)
            if ($state -ne 'healthy' -and $state -ne 'running') {
                $pending += "$container=$state"
            }
        }
        if ($pending.Count -eq 0) { return $true }
        Start-Sleep -Seconds 2
    }
    return $false
}

# Poll an HTTP endpoint until it returns 200. Returns $true/$false.
# Default Accept: text/html - Umi's Vite dev server only serves the HTML shell to
# requests that accept HTML.
function Wait-HttpOk {
    param(
        [Parameter(Mandatory)][string]$Uri,
        [int]$TimeoutSeconds = 180,
        [hashtable]$Headers = @{ Accept = 'text/html' }
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 5 -Headers $Headers
            if ($response.StatusCode -eq 200) { return $true }
        }
        catch {
            # not ready yet
        }
        Start-Sleep -Seconds 2
    }
    return $false
}

# PIDs listening on a TCP port. May return 0, 1 (scalar) or many; callers should
# wrap the result in @( ) before using .Count / indexing.
function Get-PortListeners {
    param([Parameter(Mandatory)][int]$Port)

    $conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    $ids = @()
    foreach ($c in @($conns)) {
        if ($c -and $null -ne $c.OwningProcess) { $ids += [int]$c.OwningProcess }
    }
    $ids | Sort-Object -Unique
}

# Kill a whole process tree by PID (taskkill /T /F). Silent when the PID is already gone.
# Redirection happens INSIDE cmd (>nul 2>nul) so PowerShell never sees taskkill's stderr -
# `2>&1` on a native command wraps stderr lines in NativeCommandError under WinPS 5.1.
function Stop-ProcessTree {
    param([Parameter(Mandatory)][int]$ProcessId)

    if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) { return }
    & cmd.exe /c "taskkill /PID $ProcessId /T /F >nul 2>nul"
}
