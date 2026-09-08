<#
.SYNOPSIS
Build the GoldenDB-compatible SeaTunnel MySQL-CDC connector jar.

.DESCRIPTION
ZTE GoldenDB writes a non-standard Table_map_event: 8 extra bytes between the
post-header and the standard payload, plus flags 0x1f01 instead of 0x0001. That
shifts every downstream field, so the stock binlog parser reads an empty schema,
an empty table and an illegal column type, and CDC produces nothing. See
patch/README.md for the captured bytes and the full analysis.

The jar patched here is the one the engine actually loads: SeaTunnel 2.3.13 scans
/opt/seatunnel/connectors/ (that is where plugin-mapping.properties lives), and the
apache/seatunnel base image ships its own connector build there - a different
artifact from the Maven Central one in vendor/connectors/. So the base image's jar
is extracted, patched, and copied back over the original by the Dockerfile.

Steps: pull the base jar out of the image (cached in vendor/base/), compile patch/
against it, run a self-test covering both the GoldenDB events and a standard MySQL
event so the plain MySQL path cannot regress, then write vendor/patched/.

`dev.ps1 up -Poc` runs this before `docker compose up --build`. Requires a JDK
(javac) and docker on PATH.

Exit code 0 = vendor/patched/ holds a current, self-tested jar.
#>
[CmdletBinding()]
param(
    [switch]$Force,
    [string]$BaseImage = 'apache/seatunnel:2.3.13'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$connectorJarName = 'connector-cdc-mysql-2.3.13.jar'
$connectorPathInImage = "/opt/seatunnel/connectors/$connectorJarName"
$patchedEntries = @(
    'com/github/shyiko/mysql/binlog/event/deserialization/TableMapEventDataDeserializer.class',
    'com/github/shyiko/mysql/binlog/event/deserialization/TableMapEventDataDeserializer$1.class'
)

$patchDir = Join-Path $PSScriptRoot 'patch'
$baseDir = Join-Path $PSScriptRoot 'vendor\base'
$baseJar = Join-Path $baseDir $connectorJarName
$patchedDir = Join-Path $PSScriptRoot 'vendor\patched'
$patchedJar = Join-Path $patchedDir $connectorJarName
$classesDir = Join-Path $PSScriptRoot 'vendor\.patch-classes'

foreach ($tool in 'javac', 'java', 'docker') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool is required to build the GoldenDB connector patch"
    }
}

# The engine loads the base image's connector build, not the Maven Central one, so
# that is the jar the patched class has to be injected into.
if (-not (Test-Path $baseJar)) {
    Write-Host "  extract $connectorJarName from $BaseImage" -ForegroundColor Cyan
    New-Item -ItemType Directory -Force -Path $baseDir | Out-Null
    $containerId = (& docker create $BaseImage | Select-Object -First 1)
    if ($LASTEXITCODE -ne 0 -or -not $containerId) { throw "docker create $BaseImage failed" }
    try {
        & docker cp "${containerId}:$connectorPathInImage" $baseJar
        if ($LASTEXITCODE -ne 0) { throw "docker cp $connectorPathInImage failed" }
    }
    finally {
        & docker rm $containerId | Out-Null
    }
}

$sources = @(Get-ChildItem -Path $patchDir -Filter '*.java' -Recurse)
if ($sources.Count -eq 0) { throw "no .java sources under $patchDir" }

# Idempotent: rebuild only when an input is newer than the output.
if (-not $Force -and (Test-Path $patchedJar)) {
    $outputTime = (Get-Item $patchedJar).LastWriteTimeUtc
    $newestInput = ($sources + (Get-Item $baseJar) |
        Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1).LastWriteTimeUtc
    if ($outputTime -ge $newestInput) {
        Write-Host "  ok    $connectorJarName (patched, up to date)" -ForegroundColor DarkGray
        exit 0
    }
}

Write-Host "  build GoldenDB Table_map patch -> vendor\patched\$connectorJarName" -ForegroundColor Cyan

if (Test-Path $classesDir) { Remove-Item $classesDir -Recurse -Force }
New-Item -ItemType Directory -Force -Path $classesDir, $patchedDir | Out-Null

# --release 8 keeps the bytecode level of the jar we are patching into.
& javac '--release' '8' '-nowarn' '-cp' $baseJar '-d' $classesDir ($sources | ForEach-Object { $_.FullName })
if ($LASTEXITCODE -ne 0) { throw 'javac failed' }

Write-Host '  self-test (GoldenDB events + standard MySQL regression)' -ForegroundColor Cyan
& java '-cp' "$classesDir;$baseJar" 'GoldenDbTableMapSelfTest'
if ($LASTEXITCODE -ne 0) { throw 'GoldenDbTableMapSelfTest failed - refusing to publish the patched jar' }

Copy-Item $baseJar $patchedJar -Force

Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::Open($patchedJar, 'Update')
try {
    foreach ($entryName in $patchedEntries) {
        $classFile = Join-Path $classesDir ($entryName -replace '/', '\')
        if (-not (Test-Path $classFile)) { throw "compiler did not produce $entryName" }
        $existing = $zip.GetEntry($entryName)
        if ($existing) { $existing.Delete() }
        [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, $classFile, $entryName) | Out-Null
        Write-Host "  patch $entryName" -ForegroundColor DarkGray
    }
}
finally {
    $zip.Dispose()
}

Remove-Item $classesDir -Recurse -Force
Write-Host "GoldenDB-patched connector ready: vendor\patched\$connectorJarName" -ForegroundColor Green
exit 0
