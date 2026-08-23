. (Join-Path $PSScriptRoot 'common.ps1')

Invoke-PocCompose down
Write-Host 'POC containers stopped. Runtime data and results were preserved.'
