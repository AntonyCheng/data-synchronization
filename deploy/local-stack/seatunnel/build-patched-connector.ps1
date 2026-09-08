<#
.SYNOPSIS
Build the GoldenDB-compatible SeaTunnel MySQL-CDC connector jar.

.DESCRIPTION
ZTE GoldenDB writes a non-standard Table_map_event: 8 extra bytes between the
post-header and the standard payload, plus flags 0x1f01 instead of 0x0001. That
shifts every downstream field, so the stock binlog parser reads an empty schema,
an empty table and an illegal column type, and CDC produces nothing. See
patch/README.md for the captured bytes and the full analysis.

This script compiles patch/ against the pristine
vendor/connectors/connector-cdc-mysql-2.3.13.jar, runs a self-test that covers both
the GoldenDB events and a standard MySQL event (so the plain MySQL path cannot
regress), and writes the patched jar to vendor/patched/.

vendor/connectors/ is left untouched, so it keeps matching vendor/checksums.sha1 and
fetch-vendor.ps1 stays idempotent. The Dockerfile copies vendor/patched/ after
vendor/connectors/, so the patched jar overwrites the pristine one in the image.

`dev.ps1 up -Poc` runs this after fetch-vendor.ps1. Requires a JDK (javac) on PATH.

Exit code 0 = vendor/patched/ holds a current, self-tested jar.
#>
[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$connectorJarName = 'connector-cdc-mysql-2.3.13.jar'
$patchedEntries = @(
    'com/github/shyiko/mysql/binlog/event/deserialization/TableMapEventDataDeserializer.class',
    'com/github/shyiko/mysql/binlog/event/deserialization/TableMapEventDataDeserializer$1.class'
)

$patchDir = Join-Path $PSScriptRoot 'patch'
$pristineJar = Join-Path $PSScriptRoot "vendor\connectors\$connectorJarName"
$patchedDir = Join-Path $PSScriptRoot 'vendor\patched'
$patchedJar = Join-Path $patchedDir $connectorJarName
$classesDir = Join-Path $PSScriptRoot 'vendor\.patch-classes'

if (-not (Test-Path $pristineJar)) {
    throw "missing $pristineJar - run fetch-vendor.ps1 first"
}
if (-not (Get-Command javac -ErrorAction SilentlyContinue)) {
    throw 'javac is required to build the GoldenDB connector patch (install a JDK 21 and put it on PATH)'
}

$sources = @(Get-ChildItem -Path $patchDir -Filter '*.java' -Recurse)
if ($sources.Count -eq 0) { throw "no .java sources under $patchDir" }

# Idempotent: rebuild only when an input is newer than the output.
if (-not $Force -and (Test-Path $patchedJar)) {
    $outputTime = (Get-Item $patchedJar).LastWriteTimeUtc
    $newestInput = ($sources + (Get-Item $pristineJar) |
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
& javac '--release' '8' '-nowarn' '-cp' $pristineJar '-d' $classesDir ($sources | ForEach-Object { $_.FullName })
if ($LASTEXITCODE -ne 0) { throw 'javac failed' }

Write-Host '  self-test (GoldenDB events + standard MySQL regression)' -ForegroundColor Cyan
& java '-cp' "$classesDir;$pristineJar" 'GoldenDbTableMapSelfTest'
if ($LASTEXITCODE -ne 0) { throw 'GoldenDbTableMapSelfTest failed - refusing to publish the patched jar' }

Copy-Item $pristineJar $patchedJar -Force

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
