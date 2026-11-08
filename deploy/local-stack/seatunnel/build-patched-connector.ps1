<#
.SYNOPSIS
Build the patched SeaTunnel connector jars the engine image ships.

.DESCRIPTION
Two connectors of SeaTunnel 2.3.13 are patched; the sources, the evidence and the reasoning are in
patch/README.md:

  connector-cdc-mysql  ZTE GoldenDB writes a non-standard Table_map_event (8 extra bytes, flags
                       0x1f01); the stock binlog parser reads an empty schema, an empty table and an
                       illegal column type, and CDC produces nothing.
  connector-kafka      the AT_LEAST_ONCE sender drops a record the producer refuses (larger than
                       max.request.size, ...) without a trace: the job keeps running and its checkpoint
                       moves past the row. The patched sender fails the job instead.

The jars patched are the ones the engine actually loads: SeaTunnel 2.3.13 scans /opt/seatunnel/connectors/
(that is where plugin-mapping.properties lives), and the apache/seatunnel base image ships its own
connector builds there - different artifacts from the Maven Central ones in vendor/connectors/. So each
base jar is taken from the base image - or, when that is not pulled, from the local engine image, accepted
only with the original jar's SHA-1 - and cached in vendor/base/; patch/<connector>/ is compiled against it,
the connector's self-test runs, and a copy of the base jar with the recompiled classes replaced is written
to vendor/patched/. The Dockerfile copies vendor/patched/ over the base image's jars.

`dev.ps1 up -Poc` runs this before `docker compose up --build`. Requires a JDK (javac) and docker on PATH,
and slf4j-api 1.7.36 in the local Maven repository (the Kafka self-test runs a KafkaProducer).

Exit code 0 = vendor/patched/ holds current, self-tested jars.
#>
[CmdletBinding()]
param(
    [switch]$Force,
    [string]$BaseImage = 'apache/seatunnel:2.3.13',
    [string]$EngineImage = 'data-sync-poc/seatunnel:2.3.13'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Sha1 = the original jar in the base image's /opt/seatunnel/connectors/ (not the Maven Central build).
$patches = @(
    [pscustomobject]@{ Name = 'connector-cdc-mysql'; Jar = 'connector-cdc-mysql-2.3.13.jar'; SelfTest = 'GoldenDbTableMapSelfTest'
        Sha1 = 'cff47abf7dcbca546cd1d4de0bed32f2237964ed'; NeedsSlf4j = $false },
    [pscustomobject]@{ Name = 'connector-kafka'; Jar = 'connector-kafka-2.3.13.jar'; SelfTest = 'KafkaSendFailureSelfTest'
        Sha1 = '572796adc19ec192d4faadc905e2bac8a0f81b74'; NeedsSlf4j = $true }
)

$patchRoot = Join-Path $PSScriptRoot 'patch'
$baseDir = Join-Path $PSScriptRoot 'vendor\base'
$patchedDir = Join-Path $PSScriptRoot 'vendor\patched'
$classesRoot = Join-Path $PSScriptRoot 'vendor\.patch-classes'
# kafka-clients 3.4.0, bundled in connector-kafka, logs through slf4j, which the engine's own classpath provides.
$slf4jApi = Join-Path $env:USERPROFILE '.m2\repository\org\slf4j\slf4j-api\1.7.36\slf4j-api-1.7.36.jar'

foreach ($tool in 'javac', 'java', 'docker') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        throw "$tool is required to build the connector patches"
    }
}

function Get-Sha1([string]$Path) {
    return (Get-FileHash $Path -Algorithm SHA1).Hash.ToLowerInvariant()
}

# Under 'Stop', redirecting a native command's stderr in PS 5.1 makes every stderr line fatal.
function Test-ImagePresent([string]$Image) {
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & docker image inspect $Image *> $null
        return $LASTEXITCODE -eq 0
    }
    finally {
        $ErrorActionPreference = $saved
    }
}

# The original connector jar, cached in vendor/base/ and checked against its SHA-1 either way.
function Get-BaseJar($Patch) {
    $baseJar = Join-Path $baseDir $Patch.Jar
    if (Test-Path $baseJar) {
        if ((Get-Sha1 $baseJar) -ne $Patch.Sha1) {
            throw "$baseJar is not the original $($Patch.Jar) (SHA-1 mismatch) - delete it and rerun"
        }
        return $baseJar
    }
    New-Item -ItemType Directory -Force -Path $baseDir | Out-Null
    # The engine image keeps the base image's connector jars except the ones patched before; the SHA-1
    # check below refuses a jar that already is a patched one. Neither image local: docker create pulls.
    $source = $BaseImage
    if (-not (Test-ImagePresent $BaseImage) -and (Test-ImagePresent $EngineImage)) { $source = $EngineImage }
    Write-Host "  extract $($Patch.Jar) from $source" -ForegroundColor Cyan
    $extracted = "$baseJar.tmp"
    $containerId = (& docker create $source | Select-Object -First 1)
    if ($LASTEXITCODE -ne 0 -or -not $containerId) { throw "docker create $source failed" }
    try {
        & docker cp "${containerId}:/opt/seatunnel/connectors/$($Patch.Jar)" $extracted
        if ($LASTEXITCODE -ne 0) { throw "docker cp $($Patch.Jar) failed" }
    }
    finally {
        & docker rm $containerId | Out-Null
    }
    if ((Get-Sha1 $extracted) -ne $Patch.Sha1) {
        Remove-Item $extracted -Force
        throw "$($Patch.Jar) in $source is not the original (SHA-1 mismatch) - it is a patched build; pull $BaseImage and rerun"
    }
    Move-Item $extracted $baseJar -Force
    return $baseJar
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
New-Item -ItemType Directory -Force -Path $patchedDir | Out-Null

foreach ($patch in $patches) {
    $sourceDir = Join-Path $patchRoot $patch.Name
    $sources = @(Get-ChildItem -Path $sourceDir -Filter '*.java' -Recurse)
    if ($sources.Count -eq 0) { throw "no .java sources under $sourceDir" }
    $baseJar = Get-BaseJar $patch
    $patchedJar = Join-Path $patchedDir $patch.Jar

    # Idempotent: rebuild only when an input (sources, base jar, this script) is newer than the output.
    if (-not $Force -and (Test-Path $patchedJar)) {
        $outputTime = (Get-Item $patchedJar).LastWriteTimeUtc
        $newestInput = ($sources + (Get-Item $baseJar) + (Get-Item $PSCommandPath) |
            Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1).LastWriteTimeUtc
        if ($outputTime -ge $newestInput) {
            Write-Host "  ok    $($patch.Jar) (patched, up to date)" -ForegroundColor DarkGray
            continue
        }
    }

    Write-Host "  build $($patch.Name) patch -> vendor\patched\$($patch.Jar)" -ForegroundColor Cyan
    $classesDir = Join-Path $classesRoot $patch.Name
    if (Test-Path $classesDir) { Remove-Item $classesDir -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $classesDir | Out-Null

    # --release 8 keeps the bytecode level of the jar we are patching into (the engine runs Java 8).
    & javac '--release' '8' '-nowarn' '-cp' $baseJar '-d' $classesDir ($sources | ForEach-Object { $_.FullName })
    if ($LASTEXITCODE -ne 0) { throw "javac failed for $($patch.Name)" }

    $classpath = @($classesDir, $baseJar)
    if ($patch.NeedsSlf4j) {
        if (-not (Test-Path $slf4jApi)) {
            throw "$slf4jApi is missing - run: mvn dependency:get -Dartifact=org.slf4j:slf4j-api:1.7.36"
        }
        $classpath += $slf4jApi
    }
    Write-Host "  self-test $($patch.SelfTest)" -ForegroundColor Cyan
    & java '-cp' ($classpath -join ';') $patch.SelfTest
    if ($LASTEXITCODE -ne 0) { throw "$($patch.SelfTest) failed - refusing to publish the patched $($patch.Jar)" }

    # Every compiled class in a package replaces the base jar's own; the self-test lives in the default package.
    $entries = @(Get-ChildItem $classesDir -Recurse -Filter '*.class' |
        ForEach-Object { $_.FullName.Substring($classesDir.Length + 1) -replace '\\', '/' } |
        Where-Object { $_.Contains('/') })
    if ($entries.Count -eq 0) { throw "the compiler produced no patched class for $($patch.Name)" }

    Copy-Item $baseJar $patchedJar -Force
    $zip = [System.IO.Compression.ZipFile]::Open($patchedJar, 'Update')
    try {
        foreach ($entryName in $entries) {
            $existing = $zip.GetEntry($entryName)
            if ($existing) { $existing.Delete() }
            $classFile = Join-Path $classesDir ($entryName -replace '/', '\')
            [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($zip, $classFile, $entryName) | Out-Null
            Write-Host "  patch $entryName" -ForegroundColor DarkGray
        }
    }
    finally {
        $zip.Dispose()
    }
    Write-Host "  patched $($patch.Jar) ready" -ForegroundColor Green
}

if (Test-Path $classesRoot) { Remove-Item $classesRoot -Recurse -Force }
Write-Host "Patched connectors ready: vendor\patched\" -ForegroundColor Green
exit 0
