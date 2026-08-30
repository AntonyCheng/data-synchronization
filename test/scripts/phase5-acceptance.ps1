[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][ValidateNotNullOrEmpty()][string]$Token,
    [string]$ApiBase = 'http://localhost:18081',
    [string]$MetadataDbContainer = 'dbs-mysql',
    [string]$MetadataDbPassword = 'root'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$env:NO_PROXY = '127.0.0.1,localhost'
$env:no_proxy = $env:NO_PROXY
. (Join-Path $PSScriptRoot 'common.ps1')

$headers = @{ Authorization = "Bearer $Token"; clientid = 'e5cd7e4891bf95d1d19206ce24a7b32e' }
$resultDirectory = New-PocResultDirectory
$summary = [ordered]@{ startedAt = (Get-Date).ToUniversalTime().ToString('o'); apiBase = $ApiBase; checks = @(); skipped = @() }
$createdTaskId = $null

function Add-Check([string]$Name, [bool]$Passed, [string]$Message) {
    $summary.checks += [ordered]@{ name = $Name; passed = $Passed; message = $Message }
    $label = 'FAIL'; if ($Passed) { $label = 'PASS' }
    Write-Host ("{0} {1} - {2}" -f $label, $Name, $Message)
}

function Invoke-Api([ValidateSet('GET','POST','PUT','DELETE')][string]$Method, [string]$Path, [object]$Body) {
    $p = @{ Method = $Method; Uri = "$ApiBase$Path"; Headers = $headers; ContentType = 'application/json'; UseBasicParsing = $true }
    if ($null -ne $Body) { $p.Body = ($Body | ConvertTo-Json -Depth 12 -Compress) }
    try { return Invoke-RestMethod @p }
    catch {
        $response = $_.Exception.Response
        if ($null -ne $response) {
            $reader = New-Object IO.StreamReader($response.GetResponseStream())
            $text = $reader.ReadToEnd(); $reader.Dispose()
            try { return ($text | ConvertFrom-Json) } catch { return [pscustomobject]@{ code = [int]$response.StatusCode; msg = $text; data = $null } }
        }
        throw
    }
}

function Rows($Response) {
    if ($null -eq $Response -or [int]$Response.code -ne 200) { return @() }
    if ($null -ne $Response.data.rows) { return @($Response.data.rows) }
    return @($Response.data)
}

function Assert-Code($Response, [string]$Name) {
    $ok = $null -ne $Response -and [int]$Response.code -eq 200
    $message = "API code $($Response.code): $($Response.msg)"; if ($ok) { $message = 'API code 200' }
    Add-Check $Name $ok $message
    return $ok
}

function MetadataSql([string]$Sql) {
    $output = & docker exec -e "MYSQL_PWD=$MetadataDbPassword" $MetadataDbContainer mysql -uroot -N -B -e $Sql 2>&1
    if ($LASTEXITCODE -ne 0) { throw "metadata SQL failed: $output" }
    return ($output -join [Environment]::NewLine).Trim()
}

function New-Payload($task, [string]$taskName, [string]$targetTable, [string]$mode, [string]$selected, [string]$keys) {
    return [ordered]@{
        taskName = $taskName; sourceId = [long]$task.sourceId; targetId = [long]$task.targetId
        sourceTable = $task.sourceTable; targetSchema = $task.targetSchema; targetTable = $targetTable
        syncMode = $mode; incrementalStartupMode = 'LATEST'; fullDataMode = 'UPSERT'; ddlPolicy = 'FAIL'
        scheduleMode = 'MANUAL'; selectedColumns = $selected; syncKeyColumns = $keys
        readLimitRowsPerSecond = 1000; readLimitBytesPerSecond = 10485760; snapshotParallelism = 1; sourceConnectionLimit = 2
    }
}

try {
    $list = Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null
    if (-not (Assert-Code $list '任务列表接口')) { throw '任务列表接口不可用' }
    $baseline = Rows $list | Where-Object { $_.sourceTable -eq 'customers' } | Select-Object -First 1
    if ($null -eq $baseline) { throw '没有 customers 基线任务，无法安全推导 POC 数据源 ID' }

    $suffix = [DateTime]::UtcNow.ToString('yyyyMMddHHmmss')
    $taskName = "phase5-acceptance-$suffix"
    $targetTable = "phase5_acceptance_$suffix"
    $selected = [string]$baseline.selectedColumns
    $keys = [string]$baseline.syncKeyColumns
    if ([string]::IsNullOrWhiteSpace($selected)) { $selected = 'id,display_name,updated_at' }
    if ([string]::IsNullOrWhiteSpace($keys)) { $keys = 'id' }

    $created = Invoke-Api POST '/sync/task' (New-Payload $baseline $taskName $targetTable 'FULL' $selected $keys)
    if (-not (Assert-Code $created '创建 FULL 草稿')) { throw "创建任务失败: $($created.msg)" }
    $createdTask = (Rows (Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null) | Where-Object { $_.taskName -eq $taskName } | Select-Object -First 1)
    if ($null -eq $createdTask) { throw '创建成功后未在任务列表找到临时任务' }
    $createdTaskId = [long]$createdTask.taskId
    Add-Check '默认字段与同步键持久化' ([string]$createdTask.selectedColumns -eq $selected -and [string]$createdTask.syncKeyColumns -eq $keys) "selected=$($createdTask.selectedColumns), key=$($createdTask.syncKeyColumns)"
    Add-Check '初始配置版本为 1' ([int]$createdTask.configVersion -eq 1) "configVersion=$($createdTask.configVersion)"

    $snapshot = MetadataSql "select config_snapshot from ``ry-vue``.ds_sync_task_config_version where task_id=$createdTaskId and config_version=1"
    Add-Check '配置快照存在且无密码' (-not [string]::IsNullOrWhiteSpace($snapshot) -and $snapshot -notmatch '(?i)password|secret|poc_password') '快照为无凭证 JSON'

    $selectedParts = @($selected.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $keyParts = @($keys.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    $nonKey = $selectedParts | Where-Object { $keyParts -notcontains $_ } | Select-Object -First 1
    if ($null -eq $nonKey) { throw '没有可排除的非同步键字段' }
    $editedSelected = (($selectedParts | Where-Object { $_ -ne $nonKey }) -join ',')
    $editPayload = New-Payload $createdTask $taskName $targetTable 'FULL' $editedSelected $keys
    $editPayload.taskId = $createdTaskId
    $edited = Invoke-Api PUT '/sync/task' $editPayload
    if (-not (Assert-Code $edited '编辑草稿并排除字段')) { throw "编辑任务失败: $($edited.msg)" }
    $afterEdit = Rows (Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null) | Where-Object { [long]$_.taskId -eq $createdTaskId } | Select-Object -First 1
    Add-Check '编辑递增配置版本' ($null -ne $afterEdit -and [int]$afterEdit.configVersion -eq 2) "configVersion=$($afterEdit.configVersion)"
    $versionCount = [int](MetadataSql "select count(*) from ``ry-vue``.ds_sync_task_config_version where task_id=$createdTaskId")
    Add-Check '两版配置快照均存在' ($versionCount -eq 2) "versionCount=$versionCount"

    $cases = @(
        @{ name = '排除同步键被拒绝'; payload = (New-Payload $createdTask $taskName $targetTable 'FULL' $editedSelected $nonKey) },
        @{ name = '不存在字段被拒绝'; payload = (New-Payload $createdTask $taskName $targetTable 'FULL' "$editedSelected,missing_phase5" $keys) },
        @{ name = 'INCREMENTAL 非法同步键被拒绝'; payload = (New-Payload $createdTask $taskName $targetTable 'INCREMENTAL' $editedSelected 'missing_key') },
        @{ name = 'TIMESTAMP 未来时间被拒绝'; payload = (New-Payload $createdTask $taskName $targetTable 'INCREMENTAL' $editedSelected $keys) },
        @{ name = 'SPECIFIC 非法位点被拒绝'; payload = (New-Payload $createdTask $taskName $targetTable 'INCREMENTAL' $editedSelected $keys) },
        @{ name = 'FULL_CDC 覆盖刷新被拒绝'; payload = (New-Payload $createdTask $taskName $targetTable 'FULL_CDC' $editedSelected $keys) }
    )
    $cases[3].payload.incrementalStartupMode = 'TIMESTAMP'; $cases[3].payload.incrementalStartupTimestamp = (Get-Date).AddHours(1).ToString('s')
    $cases[4].payload.incrementalStartupMode = 'SPECIFIC'; $cases[4].payload.incrementalStartupBinlogFile = 'bad file'; $cases[4].payload.incrementalStartupBinlogPosition = 3
    $cases[5].payload.fullDataMode = 'OVERWRITE'
    foreach ($case in $cases) {
        $response = Invoke-Api POST '/sync/task' $case.payload
        Add-Check $case.name ([int]$response.code -ne 200) "API code=$($response.code), message=$($response.msg)"
    }
    $summary.finishedAt = (Get-Date).ToUniversalTime().ToString('o'); $summary.passed = @($summary.checks | Where-Object { -not $_.passed }).Count -eq 0
}
catch {
    $summary.finishedAt = (Get-Date).ToUniversalTime().ToString('o'); $summary.passed = $false; $summary.error = $_.Exception.Message
}
finally {
    if ($null -ne $createdTaskId) {
        try { [void](Invoke-Api DELETE "/sync/task/$createdTaskId" $null) } catch { $summary.cleanupError = $_.Exception.Message }
    }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $resultDirectory 'phase5-acceptance.json') -Encoding utf8
}
if (-not $summary.passed) { exit 1 }
