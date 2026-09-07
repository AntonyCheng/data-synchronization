<#
.SYNOPSIS
Provision the SeaTunnel connector / JDBC driver JARs the local engine image needs.

.DESCRIPTION
deploy/local-stack/seatunnel/Dockerfile COPYs vendor/connectors/ and vendor/drivers/
into the image, but those JARs are gitignored (*.jar) and not carried in the repo.
This script downloads them from Maven Central and verifies every file against
vendor/checksums.sha1. It is idempotent: a file already present with the right
SHA-1 is left alone. `dev.ps1 up -Poc` runs this before building the image.

Exit code 0 = every checksum entry is present and valid.
#>
[CmdletBinding()]
param(
    [string]$MavenBase = 'https://repo1.maven.org/maven2'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$vendorDir = Join-Path $PSScriptRoot 'vendor'
$checksumFile = Join-Path $vendorDir 'checksums.sha1'
if (-not (Test-Path $checksumFile)) { throw "missing $checksumFile" }

# checksum-relative path -> Maven Central path (groupId/artifactId/version/file)
$mavenPath = @{
    'connectors/connector-cdc-mysql-2.3.13.jar' = 'org/apache/seatunnel/connector-cdc-mysql/2.3.13/connector-cdc-mysql-2.3.13.jar'
    'connectors/connector-jdbc-2.3.13.jar'      = 'org/apache/seatunnel/connector-jdbc/2.3.13/connector-jdbc-2.3.13.jar'
    'drivers/mysql-connector-j-8.0.33.jar'      = 'com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar'
    'drivers/postgresql-42.7.5.jar'             = 'org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar'
}

function Get-Sha1Lower { param([string]$Path) (Get-FileHash -Algorithm SHA1 -Path $Path).Hash.ToLower() }

$entries = Get-Content $checksumFile | Where-Object { $_ -match '\S' }
$failed = @()

foreach ($line in $entries) {
    $parts = $line -split '\s+', 2
    $expected = $parts[0].ToLower()
    $relPath = $parts[1].Trim()
    $target = Join-Path $vendorDir $relPath

    if ((Test-Path $target) -and (Get-Sha1Lower $target) -eq $expected) {
        Write-Host ("  ok    {0}" -f $relPath) -ForegroundColor DarkGray
        continue
    }

    if (-not $mavenPath.ContainsKey($relPath)) {
        $failed += "$relPath (no download URL mapped; place it manually)"
        continue
    }

    $url = "$MavenBase/$($mavenPath[$relPath])"
    New-Item -ItemType Directory -Force -Path (Split-Path $target) | Out-Null
    Write-Host ("  fetch {0}" -f $relPath) -ForegroundColor Cyan
    try {
        $previous = $ProgressPreference; $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $url -OutFile $target -UseBasicParsing
        $ProgressPreference = $previous
    }
    catch {
        $failed += "$relPath (download failed: $($_.Exception.Message))"
        continue
    }

    $actual = Get-Sha1Lower $target
    if ($actual -ne $expected) {
        Remove-Item $target -Force
        $failed += "$relPath (SHA-1 mismatch: got $actual, want $expected)"
    }
    else {
        Write-Host ("  ok    {0}" -f $relPath) -ForegroundColor Green
    }
}

if ($failed.Count -gt 0) {
    Write-Host ''
    Write-Host 'SeaTunnel vendor JAR provisioning failed:' -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host ''
    Write-Host 'Fallback: take the connector JARs from the Apache SeaTunnel 2.3.13' -ForegroundColor Yellow
    Write-Host 'binary distribution (connectors/ dir) and drop them into' -ForegroundColor Yellow
    Write-Host "  $vendorDir\{connectors,drivers}\" -ForegroundColor Yellow
    exit 1
}

Write-Host 'SeaTunnel vendor JARs present and verified.' -ForegroundColor Green
exit 0
