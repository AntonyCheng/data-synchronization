<#
.SYNOPSIS
Build the production deployment package: Docker images, compose files, scripts and Word manuals.

.DESCRIPTION
Runs on the Windows build host (Docker Desktop, JDK 21 + Maven, Node + pnpm). Produces
deploy\release\data-sync-<Version>\ and deploy\release\data-sync-<Version>.tar - everything a Linux
x86_64 host with Docker needs, with no registry access (see deploy\prod\README.md):

  1. patched SeaTunnel connectors (deploy\local-stack\seatunnel\build-patched-connector.ps1, self-tested);
  2. backend jar with the Maven prod profile;
  3. frontend production build (deploy\prod\web.env.production);
  4. images data-sync/backend, data-sync/seatunnel (slimmed), data-sync/web - built from images already
     on this host, so nothing is pulled - and checked: the engine image must carry exactly the patched jars;
  5. the Word manuals from deploy\prod\docs\*.md;
  6. the package directory, the image archive (docker save | gzip), manifest.json and SHA256SUMS.

-DocsImages: the screenshots the operations manual embeds (deploy\prod\docs\images, named in user-guide.md).

-WebDir: the frontend checkout to build. An elevated remote session cannot read web\node_modules
(pnpm junctions made by another session), so the remote build passes a copy with its own install
(.dev-runtime\web-remote, see docs/dev-workflow.md).
#>
[CmdletBinding()]
param(
    [string]$Version = '1.0.0',
    [string]$WebDir = 'web',
    [string]$BackendRuntimeImage = 'data-sync-mvp/backend:latest',
    [string]$EngineSourceImage = 'data-sync-poc/seatunnel:2.3.13-before-kafka-patch',
    [string]$CaddyImage = 'caddy:2.8.4-alpine',
    [string]$MysqlImage = 'mysql:8.0',
    [string]$RedisImage = 'redis:7-alpine',
    [string]$DocsNodeModules = '.dev-runtime\docgen\node_modules',
    [string]$DocsImages = 'deploy\prod\docs\images',
    [switch]$SkipBackendBuild,
    [switch]$SkipWebBuild,
    [switch]$SkipImageExport
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $root
$prod = Join-Path $root 'deploy\prod'
$releaseRoot = Join-Path $root 'deploy\release'
$pkgName = "data-sync-$Version"
$pkg = Join-Path $releaseRoot $pkgName
$stage = Join-Path $root ".dev-runtime\prod-build"
$utf8 = New-Object Text.UTF8Encoding $false

function Step([string]$Text) { Write-Host "`n==> $Text" -ForegroundColor Cyan }
function Invoke-Native([string]$What, [scriptblock]$Command) {
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit code $LASTEXITCODE)" }
}
# Under 'Stop', a native command's redirected stderr is fatal in PS 5.1; probes run under 'Continue'.
function Test-Native([scriptblock]$Command) {
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & $Command *> $null; return $LASTEXITCODE -eq 0 } finally { $ErrorActionPreference = $saved }
}
function Get-Sha1([string]$Path) { (Get-FileHash $Path -Algorithm SHA1).Hash.ToLowerInvariant() }
function Get-Sha256([string]$Path) { (Get-FileHash $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
# Text files for the Linux host: UTF-8 without BOM, LF line endings.
function Copy-Text([string]$From, [string]$To) {
    $text = [IO.File]::ReadAllText($From, [Text.Encoding]::UTF8) -replace "`r`n", "`n"
    New-Item -ItemType Directory -Force (Split-Path $To) | Out-Null
    [IO.File]::WriteAllText($To, $text, $utf8)
}
function Write-Text([string]$Path, [string]$Text) {
    New-Item -ItemType Directory -Force (Split-Path $Path) | Out-Null
    [IO.File]::WriteAllText($Path, ($Text -replace "`r`n", "`n"), $utf8)
}
# ENV lines of a local image's config, to carry its runtime environment into a flattened image.
function Get-ImageEnvLines([string]$Image) {
    $json = & docker image inspect --format '{{json .Config.Env}}' $Image
    if ($LASTEXITCODE -ne 0) { throw "image $Image is not on this host" }
    $lines = @()
    foreach ($entry in ($json | ConvertFrom-Json)) {
        $name, $value = $entry -split '=', 2
        if ($name -in 'HOSTNAME', 'HOME') { continue }
        # Inside a double-quoted Dockerfile ENV value, \ " and $ are special: escape them.
        $escaped = $value.Replace('\', '\\').Replace('"', '\"').Replace('$', '\$')
        $lines += "ENV $name=`"$escaped`""
    }
    return ($lines -join "`n")
}

$commit = (& git rev-parse HEAD).Trim()
$dirty = [bool](& git status --porcelain --untracked-files=no)
$built = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
Write-Host "data-sync $Version from $commit$(if ($dirty) { ' (working tree has uncommitted changes)' })"

foreach ($tool in 'docker', 'java', 'node') {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool is required" }
}
foreach ($image in $BackendRuntimeImage, $EngineSourceImage, $CaddyImage, $MysqlImage, $RedisImage) {
    if (-not (Test-Native { docker image inspect $image })) { throw "image $image is not on this host" }
}
if (Test-Path $pkg) {
    if ((Test-Path (Join-Path $pkg '.env')) -or (Test-Path (Join-Path $pkg 'data'))) {
        throw "$pkg holds a .env or data/ - it has been used for an installation; remove it by hand"
    }
    Remove-Item -Recurse -Force $pkg
}
if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
New-Item -ItemType Directory -Force $pkg, $stage | Out-Null

# ---------------------------------------------------------------- 1. patched connectors
Step 'patched SeaTunnel connectors'
& (Join-Path $root 'deploy\local-stack\seatunnel\build-patched-connector.ps1')
$vendor = Join-Path $root 'deploy\local-stack\seatunnel\vendor'
$stockSha1 = @{ 'connector-cdc-mysql-2.3.13.jar' = 'cff47abf7dcbca546cd1d4de0bed32f2237964ed'; 'connector-kafka-2.3.13.jar' = '572796adc19ec192d4faadc905e2bac8a0f81b74' }
$patchedSha1 = @{}
foreach ($jar in $stockSha1.Keys) {
    $path = Join-Path $vendor "patched\$jar"
    if (-not (Test-Path $path)) { throw "missing patched connector $path" }
    $patchedSha1[$jar] = Get-Sha1 $path
    if ($patchedSha1[$jar] -eq $stockSha1[$jar]) { throw "$jar in vendor\patched is the stock jar, not the patched one" }
    Write-Host "  $jar $($patchedSha1[$jar])"
}

# ---------------------------------------------------------------- 2. backend jar
$jar = Join-Path $root 'server\ruoyi-admin\target\ruoyi-admin.jar'
if (-not $SkipBackendBuild) {
    Step 'backend jar (mvn -Pprod package)'
    Push-Location (Join-Path $root 'server')
    try {
        & mvn -B -ntp -o -Pprod -pl ruoyi-admin -am package -DskipTests
        if ($LASTEXITCODE -ne 0) {
            Write-Host 'offline build failed - retrying with the remote repositories'
            Invoke-Native 'maven package' { mvn -B -ntp -Pprod -pl ruoyi-admin -am package -DskipTests }
        }
    } finally { Pop-Location }
}
if (-not (Test-Path $jar)) { throw "missing $jar" }

# ---------------------------------------------------------------- 3. frontend
$web = (Resolve-Path $WebDir).Path
if (-not $SkipWebBuild) {
    Step "frontend production build ($web)"
    Copy-Text (Join-Path $prod 'web.env.production') (Join-Path $web '.env.production')
    Push-Location $web
    try { Invoke-Native 'pnpm build' { pnpm build } } finally { Pop-Location }
}
$dist = Join-Path $web 'dist'
if (-not (Test-Path (Join-Path $dist 'index.html'))) { throw "missing $dist\index.html" }
$prodApi = Get-ChildItem $dist -Recurse -Filter '*.js' | Select-String -SimpleMatch '/prod-api' -List | Select-Object -First 1
if (-not $prodApi) { throw 'the frontend build does not call /prod-api - .env.production was not applied' }

# ---------------------------------------------------------------- 4. images
Step 'images'
$labels = @('--label', "org.opencontainers.image.version=$Version", '--label', "org.opencontainers.image.revision=$commit",
    '--label', "org.opencontainers.image.created=$built", '--label', 'org.opencontainers.image.vendor=data-synchronization')

# backend: flattened runtime of $BackendRuntimeImage + this jar + the BCrypt helper
$ctx = Join-Path $stage 'backend'
New-Item -ItemType Directory -Force (Join-Path $ctx 'tools') | Out-Null
Copy-Item $jar (Join-Path $ctx 'app.jar')
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [IO.Compression.ZipFile]::OpenRead($jar)
try {
    $hutool = @($zip.Entries | Where-Object { $_.FullName -match '^BOOT-INF/lib/hutool-(core|crypto)-[^/]+\.jar$' })
    if ($hutool.Count -ne 2) { throw "expected hutool-core and hutool-crypto in the jar, found: $($hutool.FullName -join ', ')" }
    foreach ($entry in $hutool) { [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, (Join-Path $ctx "tools\$($entry.Name)"), $true) }
} finally { $zip.Dispose() }
Copy-Text (Join-Path $prod 'images\backend\BCryptHash.java') (Join-Path $ctx 'tools\BCryptHash.java')
$dockerfile = [IO.File]::ReadAllText((Join-Path $prod 'images\backend\Dockerfile')).Replace('# @RUNTIME_ENV@', (Get-ImageEnvLines $BackendRuntimeImage))
Write-Text (Join-Path $ctx 'Dockerfile') $dockerfile
Invoke-Native 'backend image' { docker build --build-arg "RUNTIME_IMAGE=$BackendRuntimeImage" @labels -t "data-sync/backend:$Version" $ctx }

# engine: slimmed $EngineSourceImage + the patched connectors
$ctx = Join-Path $stage 'seatunnel'
New-Item -ItemType Directory -Force (Join-Path $ctx 'patched'), (Join-Path $ctx 'drivers') | Out-Null
foreach ($j in $stockSha1.Keys) { Copy-Item (Join-Path $vendor "patched\$j") (Join-Path $ctx 'patched') }
Copy-Item (Join-Path $vendor 'drivers\*.jar') (Join-Path $ctx 'drivers')
Copy-Text (Join-Path $prod 'images\seatunnel\prune.sh') (Join-Path $ctx 'prune.sh')
Copy-Text (Join-Path $prod 'images\seatunnel\entrypoint.sh') (Join-Path $ctx 'entrypoint.sh')
$dockerfile = [IO.File]::ReadAllText((Join-Path $prod 'images\seatunnel\Dockerfile')).Replace('# @RUNTIME_ENV@', (Get-ImageEnvLines $EngineSourceImage))
Write-Text (Join-Path $ctx 'Dockerfile') $dockerfile
Invoke-Native 'engine image' { docker build --build-arg "ENGINE_SOURCE_IMAGE=$EngineSourceImage" @labels -t "data-sync/seatunnel:$Version" $ctx }

# web: Caddy + the frontend build
$ctx = Join-Path $stage 'web'
New-Item -ItemType Directory -Force $ctx | Out-Null
Copy-Item -Recurse $dist (Join-Path $ctx 'dist')
Copy-Text (Join-Path $prod 'caddy\Caddyfile') (Join-Path $ctx 'Caddyfile')
Copy-Text (Join-Path $prod 'images\web\Dockerfile') (Join-Path $ctx 'Dockerfile')
Invoke-Native 'web image' { docker build --build-arg "CADDY_IMAGE=$CaddyImage" @labels -t "data-sync/web:$Version" $ctx }

Step 'image checks'
$check = & docker run --rm --entrypoint sh "data-sync/seatunnel:$Version" -c 'cd /opt/seatunnel/connectors && sha1sum connector-cdc-mysql-2.3.13.jar connector-kafka-2.3.13.jar && ls connector-*.jar | wc -l && java -version 2>&1 | head -1 && command -v curl'
if ($LASTEXITCODE -ne 0) { throw 'engine image check failed' }
$check | ForEach-Object { Write-Host "  engine: $_" }
foreach ($j in $stockSha1.Keys) {
    if (-not ($check -match "^$($patchedSha1[$j])\s+$([regex]::Escape($j))$")) { throw "engine image does not carry the patched $j" }
}
$check = & docker run --rm --entrypoint java "data-sync/backend:$Version" -cp '/opt/datasync/tools/*' /opt/datasync/tools/BCryptHash.java 'check-1234'
if ($LASTEXITCODE -ne 0 -or -not ("$check" -like '$2a$*')) { throw "backend BCrypt helper failed: $check" }
Write-Host '  backend: BCrypt helper ok'
Invoke-Native 'backend curl check' { docker run --rm --entrypoint sh "data-sync/backend:$Version" -c 'java -version 2>&1 | head -1; command -v curl' }
Invoke-Native 'Caddyfile check' { docker run --rm "data-sync/web:$Version" caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile }

# ---------------------------------------------------------------- 5. package directory
Step "package $pkg"
foreach ($f in 'compose.yml', 'compose.https.yml', 'caddy\Caddyfile', 'caddy\Caddyfile.https', 'seatunnel\seatunnel.yaml',
    'database\migrate.sh', 'database\prod-hardening.sql', 'database\prod-fresh-install.sql') {
    Copy-Text (Join-Path $prod $f) (Join-Path $pkg $f)
}
Copy-Text (Join-Path $prod 'env.example') (Join-Path $pkg '.env.example')
Get-ChildItem (Join-Path $prod 'scripts') -Filter '*.sh' | ForEach-Object { Copy-Text $_.FullName (Join-Path $pkg "scripts\$($_.Name)") }
$sql = Join-Path $root 'server\script\sql'
Copy-Text (Join-Path $sql 'ry_vue.sql') (Join-Path $pkg 'database\ry_vue.sql')
Copy-Text (Join-Path $sql 'ry_sync.sql') (Join-Path $pkg 'database\ry_sync.sql')
Get-ChildItem $sql -Filter 'ry_sync_migration_*.sql' | ForEach-Object { Copy-Text $_.FullName (Join-Path $pkg "database\migrations\$($_.Name)") }
Write-Text (Join-Path $pkg 'certs\README.txt') "启用 HTTPS 时把 PEM 格式的证书与私钥放在本目录：server.crt（含中间证书链）、server.key，并在 .env 中设置 ENABLE_HTTPS=true。`n"
Write-Text (Join-Path $pkg 'VERSION') "$Version`n"
Copy-Text (Join-Path $prod 'README.txt') (Join-Path $pkg 'README.txt')

Step 'Word manuals'
$nodeModules = Join-Path $root $DocsNodeModules
if (-not (Test-Path (Join-Path $nodeModules 'docx'))) {
    $docgen = Split-Path $nodeModules
    New-Item -ItemType Directory -Force $docgen | Out-Null
    Invoke-Native 'npm install docx' { npm.cmd install --prefix $docgen --no-audit --no-fund docx@9.15.3 }
}
$env:NODE_PATH = $nodeModules
Invoke-Native 'manuals' { node (Join-Path $prod 'docs\build-docx.cjs') --version $Version --commit $commit.Substring(0, 8) --out (Join-Path $pkg 'docs') --images (Join-Path $root $DocsImages) }

if (-not $SkipImageExport) {
    Step 'image archive'
    $images = @("data-sync/backend:$Version", "data-sync/seatunnel:$Version", "data-sync/web:$Version", $MysqlImage, $RedisImage)
    New-Item -ItemType Directory -Force (Join-Path $pkg 'images') | Out-Null
    $tar = Join-Path $stage "data-sync-images-$Version.tar"
    Invoke-Native 'docker save' { docker image save --output $tar @images }
    $gz = Join-Path $pkg "images\data-sync-images-$Version.tar.gz"
    $in = [IO.File]::OpenRead($tar)
    $out = [IO.File]::Create($gz)
    $gzip = New-Object IO.Compression.GZipStream($out, [IO.Compression.CompressionLevel]::Optimal)
    try { $in.CopyTo($gzip, 4MB) } finally { $gzip.Dispose(); $out.Dispose(); $in.Dispose() }
    Remove-Item $tar
}

Step 'manifest'
$imageInfo = foreach ($image in "data-sync/backend:$Version", "data-sync/seatunnel:$Version", "data-sync/web:$Version", $MysqlImage, $RedisImage) {
    $inspect = (& docker image inspect $image | ConvertFrom-Json)[0]
    [ordered]@{ image = $image; id = $inspect.Id; sizeBytes = $inspect.Size }
}
$files = Get-ChildItem $pkg -Recurse -File | Sort-Object FullName
$manifest = [ordered]@{
    product = 'data-synchronization-platform'
    version = $Version
    commit = $commit
    workingTreeDirty = $dirty
    builtAt = $built
    platform = 'linux/amd64'
    engine = 'Apache SeaTunnel 2.3.13 (Zeta), patched connector-cdc-mysql + connector-kafka'
    patchedConnectors = $patchedSha1
    images = @($imageInfo)
    files = @($files | ForEach-Object { [ordered]@{ path = $_.FullName.Substring($pkg.Length + 1).Replace('\', '/'); bytes = $_.Length; sha256 = Get-Sha256 $_.FullName } })
}
Write-Text (Join-Path $pkg 'manifest.json') ($manifest | ConvertTo-Json -Depth 6)
$sums = ($manifest.files | ForEach-Object { "$($_.sha256)  $($_.path)" }) -join "`n"
Write-Text (Join-Path $pkg 'SHA256SUMS') "$sums`n"

Step 'outer archive'
$outer = Join-Path $releaseRoot "$pkgName.tar"
if (Test-Path $outer) { Remove-Item $outer }
Invoke-Native 'tar' { tar -cf $outer -C $releaseRoot $pkgName }
Write-Text (Join-Path $releaseRoot "$pkgName.tar.sha256") "$(Get-Sha256 $outer)  $pkgName.tar`n"
Write-Host ("PROD_PACKAGE_PASS {0} ({1:N0} MB) commit {2}" -f $outer, ((Get-Item $outer).Length / 1MB), $commit)
