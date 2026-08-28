[CmdletBinding()]
param(
    [string]$BridgePath = "$env:LOCALAPPDATA\Programs\CopilotQQBridge\qq-bridge.exe",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml",

    [string]$TaskName = "CopilotQQBridge"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $BridgePath -PathType Leaf)) {
    throw "Bridge executable not found: $BridgePath"
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw "Bridge config not found: $ConfigPath"
}

$CurrentUser = if ($env:USERDOMAIN) {
    "$env:USERDOMAIN\$env:USERNAME"
}
else {
    $env:USERNAME
}

$Action = New-ScheduledTaskAction `
    -Execute $BridgePath `
    -Argument ('--config "' + $ConfigPath + '" run')
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser
$Principal = New-ScheduledTaskPrincipal `
    -UserId $CurrentUser `
    -LogonType Interactive `
    -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Principal $Principal `
    -Settings $Settings `
    -Description "QQ remote supervision bridge for VS Code Copilot" `
    -Force | Out-Null

Write-Host "Registered scheduled task $TaskName for $CurrentUser with LIMITED privileges."
