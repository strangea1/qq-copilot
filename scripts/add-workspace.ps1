[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$Workspace,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml"
)

$ErrorActionPreference = "Stop"
$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"
$SwitchScript = Join-Path $InstallDirectory "scripts\switch-vscode-integration.ps1"

if (-not (Test-Path -LiteralPath $Bridge -PathType Leaf)) {
    throw "Bridge executable not found: $Bridge"
}
if (-not (Test-Path -LiteralPath $SwitchScript -PathType Leaf)) {
    throw "Integration switch script not found: $SwitchScript"
}

$StatusOutput = & $Bridge --config $ConfigPath status
if ($LASTEXITCODE -ne 0) {
    throw "The Bridge must be running so its idle state can be verified before restart."
}
$Status = ($StatusOutput -join [Environment]::NewLine) | ConvertFrom-Json
if (-not $Status.ahp) {
    throw "AHP mode is not configured."
}

$Bindings = @()
if ($Status.ahp.bindings) {
    $Bindings = @($Status.ahp.bindings)
} elseif ($Status.ahp.binding) {
    $Bindings = @($Status.ahp.binding)
}
$BusyBindings = @($Bindings | Where-Object {
    $_.active_turn_id -or [int]($_.queued_message_count) -ne 0
})
if ($BusyBindings.Count -ne 0) {
    throw "Wait for all active Turns and queued messages to finish before adding a workspace."
}
if ([int]($Status.ahp.pending_commands) -ne 0) {
    throw "Wait for pending Adapter commands to finish before adding a workspace."
}
if (
    [int]($Status.ahp.pending_approvals) -ne 0 -or
    [int]($Status.ahp.pending_inputs) -ne 0
) {
    throw "Wait for pending approvals and clarification inputs to finish before adding a workspace."
}

$Arguments = @("--config", $ConfigPath, "add-workspace")
foreach ($Path in $Workspace) {
    $Resolved = (Resolve-Path -LiteralPath $Path).Path
    if (-not (Test-Path -LiteralPath $Resolved -PathType Container)) {
        throw "Workspace is not a directory: $Resolved"
    }
    $Arguments += @("--workspace", $Resolved)
}

& $Bridge @Arguments
if ($LASTEXITCODE -ne 0) {
    throw "qq-bridge add-workspace failed with exit code $LASTEXITCODE"
}

& $SwitchScript `
    -Mode Ahp `
    -InstallDirectory $InstallDirectory `
    -ConfigPath $ConfigPath
if ($LASTEXITCODE -ne 0) {
    throw "Bridge restart failed with exit code $LASTEXITCODE"
}

Write-Host "Target workspace configuration applied. Send /sessions in QQ to verify it."
