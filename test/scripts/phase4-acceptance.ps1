[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Token,
    [long]$TaskId,
    [string]$ApiBase = 'http://localhost:18081',
    [int]$BlockSize = 2,
    [long]$StrictTaskId,
    [switch]$SkipMutation
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$env:NO_PROXY = '127.0.0.1,localhost'
$env:no_proxy = $env:NO_PROXY

. (Join-Path $PSScriptRoot 'common.ps1')

$headers = @{
    Authorization = "Bearer $Token"
    clientid = 'e5cd7e4891bf95d1d19206ce24a7b32e'
}
$resultDirectory = New-PocResultDirectory
$summary = [ordered]@{
    startedAt = (Get-Date).ToUniversalTime().ToString('o')
    apiBase = $ApiBase
    taskId = $null
    checks = @()
    skipped = @()
}

function Add-Check {
    param(
        [string]$Name,
        [bool]$Passed,
        [string]$Message
    )
    $summary.checks += [ordered]@{name = $Name; passed = $Passed; message = $Message}
    if ($Passed) { Write-Host "PASS $Name - $Message" -ForegroundColor Green }
    else { Write-Host "FAIL $Name - $Message" -ForegroundColor Red }
}

function Invoke-Api {
    param(
        [ValidateSet('GET', 'POST')]
        [string]$Method,
        [string]$Path,
        [object]$Body
    )
    $params = @{
        Method = $Method
        Uri = "$ApiBase$Path"
        Headers = $headers
        ContentType = 'application/json'
        UseBasicParsing = $true
    }
    if ($null -ne $Body) { $params.Body = ($Body | ConvertTo-Json -Depth 10 -Compress) }
    return Invoke-RestMethod @params
}

function Assert-ApiSuccess {
    param([object]$Response, [string]$Name)
    $ok = $null -ne $Response -and [int]$Response.code -eq 200
    $message = "API code $($Response.code): $($Response.msg)"
    if ($ok) { $message = 'HTTP/API code 200' }
    Add-Check $Name $ok $message
    if (-not $ok) { throw "API request failed: $Name" }
}

function Invoke-PocSql {
    param(
        [ValidateSet('mysql', 'postgres')]
        [string]$Engine,
        [string]$Sql
    )
    if ($Engine -eq 'mysql') {
        $output = & docker exec -e MYSQL_PWD=poc_root_pw ds-poc-mysql mysql -uroot -N -B -e $Sql 2>&1
    } else {
        $output = & docker exec -e PGPASSWORD=poc_password ds-poc-postgres psql -v ON_ERROR_STOP=1 -U poc -d sink_db -At -c $Sql 2>&1
    }
    if ($LASTEXITCODE -ne 0) { throw "POC SQL failed: $output" }
    return ($output -join [Environment]::NewLine).Trim()
}

try {
    $list = Invoke-Api GET '/sync/task/list?pageNum=1&pageSize=100' $null
    Assert-ApiSuccess $list '任务列表接口'
    $records = @($list.data.rows)
    if ($TaskId -gt 0) {
        $task = $records | Where-Object { [long]$_.taskId -eq $TaskId } | Select-Object -First 1
    } else {
        $task = $records | Where-Object { $_.sourceTable -eq 'customers' } | Select-Object -First 1
    }
    if ($null -eq $task) { throw '没有找到可用于 Phase 4 验收的 customers 任务，请通过 -TaskId 指定。' }
    $TaskId = [long]$task.taskId
    $summary.taskId = $TaskId

    $countBody = @{mode = "COUNT"; strictWatermark = $false}
    $count = Invoke-Api POST "/sync/task/$TaskId/check" $countBody
    Assert-ApiSuccess $count "COUNT 核对"
    $countData = $count.data
    Add-Check "COUNT 结果一致" ([bool]$countData.success -and [bool]$countData.matched -and [long]$countData.difference -eq 0) "source=$($countData.sourceRows), target=$($countData.targetRows), difference=$($countData.difference)"

    $rangeBody = @{mode = "KEY_RANGE"; blockSize = $BlockSize; strictWatermark = $false}
    $range = Invoke-Api POST "/sync/task/$TaskId/check" $rangeBody
    Assert-ApiSuccess $range "KEY_RANGE 核对"
    $rangeData = $range.data
    $rangeOk = [bool]$rangeData.success -and [bool]$rangeData.matched -and [int]$rangeData.totalBlocks -gt 0
    Add-Check "KEY_RANGE 结果一致" $rangeOk "blocks=$($rangeData.totalBlocks), matched=$($rangeData.matchedBlocks), mismatched=$($rangeData.mismatchedBlocks)"

    if (-not $SkipMutation) {
        $backupTable = "phase4_acceptance_backup_$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))"
        Invoke-PocSql postgres "DROP TABLE IF EXISTS $backupTable; CREATE TABLE $backupTable AS SELECT * FROM public.customers WHERE id = 10; DELETE FROM public.customers WHERE id = 10;" | Out-Null
        try {
            $mismatch = Invoke-Api POST "/sync/task/$TaskId/check" $rangeBody
            Assert-ApiSuccess $mismatch "差异块核对接口"
            $mismatchData = $mismatch.data
            $hasMismatch = [int]$mismatchData.mismatchedBlocks -gt 0 -and [long]$mismatchData.difference -eq 1
            Add-Check "差异块被识别" $hasMismatch "mismatched=$($mismatchData.mismatchedBlocks), difference=$($mismatchData.difference)"
        }
        finally {
            Invoke-PocSql postgres "INSERT INTO public.customers SELECT * FROM $backupTable; DROP TABLE IF EXISTS $backupTable;" | Out-Null
        }
    }

    $invalid = $null
    try {
        $invalidBody = @{mode = "KEY_RANGE"; blockSize = 0; strictWatermark = $false}
        $invalid = Invoke-Api POST "/sync/task/$TaskId/check" $invalidBody
    }
    catch {
        Add-Check "非法分块步长被拒绝" $true "请求被服务端拒绝"
    }
    if ($null -ne $invalid) {
        Add-Check "非法分块步长被拒绝" ([int]$invalid.code -ne 200 -or [bool]$invalid.data.success -eq $false) "API code=$($invalid.code)"
    }

    $noKeyTask = $records | Where-Object { $_.sourceTable -eq 'no_key_platform' } | Select-Object -First 1
    if ($null -ne $noKeyTask) {
        $unsupported = Invoke-Api POST "/sync/task/$([long]$noKeyTask.taskId)/check" $rangeBody
        $unsupportedData = $unsupported.data
        $unsupportedOk = [bool]$unsupportedData.success -eq $false -and $unsupportedData.message -match '未配置同步键'
        Add-Check "无同步键任务明确提示" $unsupportedOk $unsupportedData.message
    }

    $strictTask = $null
    if ($StrictTaskId -gt 0) {
        $strictTask = $records | Where-Object { [long]$_.taskId -eq $StrictTaskId } | Select-Object -First 1
    } else {
        $strictTask = $records | Where-Object {
            $_.syncMode -eq 'FULL_CDC' -and $_.status -in @('RUNNING', 'PAUSING')
        } | Select-Object -First 1
    }
    if ($null -ne $strictTask) {
        $strictBody = @{mode = "COUNT"; strictWatermark = $true}
        $strict = Invoke-Api POST "/sync/task/$([long]$strictTask.taskId)/check" $strictBody
        $strictData = $strict.data
        $strictOk = [bool]$strictData.success -eq $false -and $strictData.watermarkMessage -match '先暂停任务'
        Add-Check "CDC 严格水位保护" $strictOk $strictData.message
    } else {
        $summary.skipped += '未发现 RUNNING/PAUSING 的 FULL_CDC 任务，未执行严格水位用例'
        Write-Host 'SKIP CDC 严格水位保护 - 当前没有运行中的 FULL_CDC 任务' -ForegroundColor Yellow
    }

    $summary.finishedAt = (Get-Date).ToUniversalTime().ToString('o')
    $summary.passed = @($summary.checks | Where-Object { -not $_.passed }).Count -eq 0
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $resultDirectory 'phase4-acceptance.json') -Encoding utf8
    if (-not $summary.passed) { exit 1 }
}
catch {
    $summary.finishedAt = (Get-Date).ToUniversalTime().ToString('o')
    $summary.passed = $false
    $summary.error = $_.Exception.Message
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $resultDirectory 'phase4-acceptance.json') -Encoding utf8
    throw
}
