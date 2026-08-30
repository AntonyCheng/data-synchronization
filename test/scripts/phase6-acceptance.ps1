[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][ValidateNotNullOrEmpty()][string]$Token,
    [string]$ApiBase = 'http://localhost:18081',
    [string]$MetadataDbContainer = 'dbs-mysql',
    [string]$MetadataDbPassword = 'root'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$env:NO_PROXY = '127.0.0.1,localhost'; $env:no_proxy = $env:NO_PROXY
. (Join-Path $PSScriptRoot 'common.ps1')
$headers = @{ Authorization = "Bearer $Token"; clientid = 'e5cd7e4891bf95d1d19206ce24a7b32e' }
$resultDirectory = New-PocResultDirectory
$summary = [ordered]@{ startedAt = (Get-Date).ToUniversalTime().ToString('o'); apiBase = $ApiBase; checks = @(); skipped = @() }
$taskIds = [System.Collections.Generic.List[long]]::new()

function Add-Check([string]$Name, [bool]$Passed, [string]$Message) {
    $summary.checks += [ordered]@{ name = $Name; passed = $Passed; message = $Message }
    $label = 'FAIL'; if ($Passed) { $label = 'PASS' }
    Write-Host ("{0} {1} - {2}" -f $label, $Name, $Message)
}
function Invoke-Api([ValidateSet('GET','POST','PUT','DELETE')][string]$Method, [string]$Path, [object]$Body) {
    $p = @{ Method = $Method; Uri = "$ApiBase$Path"; Headers = $headers; ContentType = 'application/json'; UseBasicParsing = $true }
    if ($null -ne $Body) { $p.Body = ($Body | ConvertTo-Json -Depth 12 -Compress) }
    try { return Invoke-RestMethod @p } catch {
        $response = $_.Exception.Response
        if ($null -eq $response) { throw }
        $reader = New-Object IO.StreamReader($response.GetResponseStream()); $text = $reader.ReadToEnd(); $reader.Dispose()
        try { return ($text | ConvertFrom-Json) } catch { return [pscustomobject]@{ code = [int]$response.StatusCode; msg = $text; data = $null } }
    }
}
function Rows($Response) { if ($null -eq $Response -or [int]$Response.code -ne 200) { return @() }; if ($null -ne $Response.data.rows) { return @($Response.data.rows) }; return @($Response.data) }
function Payload($task, [string]$name, [string]$mode, [string]$schedule, [string]$cron, [string]$fullMode) {
    [ordered]@{
        taskName = $name; sourceId = [long]$task.sourceId; targetId = [long]$task.targetId; sourceTable = $task.sourceTable
        targetSchema = $task.targetSchema; targetTable = "phase6_acceptance_$([DateTime]::UtcNow.ToString('yyyyMMddHHmmssfff'))"
        syncMode = $mode; incrementalStartupMode = 'LATEST'; fullDataMode = $fullMode; ddlPolicy = 'FAIL'; scheduleMode = $schedule; cronExpression = $cron
        selectedColumns = $task.selectedColumns; syncKeyColumns = $task.syncKeyColumns; readLimitRowsPerSecond = 1000; readLimitBytesPerSecond = 10485760
        snapshotParallelism = 1; sourceConnectionLimit = 2
    }
}
try {
    $list = Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null
    if ([int]$list.code -ne 200) { throw "任务列表接口失败: $($list.msg)" }
    $baseline = Rows $list | Where-Object { $_.sourceTable -eq 'customers' } | Select-Object -First 1
    if ($null -eq $baseline) { throw '没有 customers 基线任务，无法执行 Phase 6 API 验收' }
    $stamp = [DateTime]::UtcNow.ToString('yyyyMMddHHmmss')

    $cronName = "phase6-cron-$stamp"
    $cronPayload = Payload $baseline $cronName 'FULL' 'CRON' '0 0 2 * * ?' 'UPSERT'
    $created = Invoke-Api POST '/sync/task' $cronPayload
    if ([int]$created.code -ne 200) { throw "创建 CRON 任务失败: $($created.msg)" }
    $cronTask = Rows (Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null) | Where-Object { $_.taskName -eq $cronName } | Select-Object -First 1
    if ($null -eq $cronTask) { throw 'CRON 任务未出现在列表中' }
    $taskIds.Add([long]$cronTask.taskId)
    Add-Check '合法 Cron 计算下次执行时间' ($cronTask.scheduleMode -eq 'CRON' -and $null -ne $cronTask.nextRunTime) "nextRunTime=$($cronTask.nextRunTime)"

    $invalidPayload = Payload $baseline "phase6-invalid-cron-$stamp" 'FULL' 'CRON' 'not-a-cron' 'UPSERT'
    $invalid = Invoke-Api POST '/sync/task' $invalidPayload
    Add-Check '非法 Cron 被拒绝' ([int]$invalid.code -ne 200) "API code=$($invalid.code), message=$($invalid.msg)"

    $limitPayload = Payload $baseline "phase6-limit-$stamp" 'FULL' 'MANUAL' $null 'UPSERT'
    $limitPayload.readLimitRowsPerSecond = 100001
    $limit = Invoke-Api POST '/sync/task' $limitPayload
    Add-Check '超出源库保护上限被拒绝' ([int]$limit.code -ne 200) "API code=$($limit.code), message=$($limit.msg)"

    $overwriteName = "phase6-overwrite-$stamp"
    $overwritePayload = Payload $baseline $overwriteName 'FULL' 'MANUAL' $null 'OVERWRITE'
    $overwrite = Invoke-Api POST '/sync/task' $overwritePayload
    if ([int]$overwrite.code -ne 200) { throw "创建覆盖刷新任务失败: $($overwrite.msg)" }
    $overwriteTask = Rows (Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null) | Where-Object { $_.taskName -eq $overwriteName } | Select-Object -First 1
    if ($null -eq $overwriteTask) { throw '覆盖刷新任务未出现在列表中' }
    $taskIds.Add([long]$overwriteTask.taskId)
    $preview = Invoke-Api POST "/sync/task/$([long]$overwriteTask.taskId)/engine-config" $null
    $configText = [string]$preview.data.config
    # The preview intentionally returns quoted asterisks. Do not treat the
    # opening quote as a credential when checking for an unmasked password.
    $hasPasswordLeak = $configText -match '(?im)password\s*=\s*"(?!\*{6}")'
    Add-Check '覆盖刷新配置使用版本化临时表' ([int]$preview.code -eq 200 -and $configText -match '__ds_stage_' -and -not $hasPasswordLeak) '配置预览包含 stage 表且凭证已脱敏'
    $summary.skipped += '未提交 SeaTunnel 作业：真实替换成功/失败回滚属于需要运行引擎的集成演练，避免污染现有 POC 任务。'
    $summary.finishedAt = (Get-Date).ToUniversalTime().ToString('o'); $summary.passed = @($summary.checks | Where-Object { -not $_.passed }).Count -eq 0
}
catch {
    $summary.finishedAt = (Get-Date).ToUniversalTime().ToString('o'); $summary.passed = $false; $summary.error = $_.Exception.Message
}
finally {
    foreach ($taskId in $taskIds) { try { [void](Invoke-Api DELETE "/sync/task/$taskId" $null) } catch { $summary.cleanupError = $_.Exception.Message } }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $resultDirectory 'phase6-acceptance.json') -Encoding utf8
}
if (-not $summary.passed) { exit 1 }
