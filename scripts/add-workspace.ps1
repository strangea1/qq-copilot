[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$Workspace,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml"
)

$ErrorActionPreference = "Stop"
$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"

if (-not (Test-Path -LiteralPath $Bridge -PathType Leaf)) {
    throw "Bridge executable not found: $Bridge"
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw "Bridge config not found: $ConfigPath"
}

& $Bridge --config $ConfigPath reload-targets
if ($LASTEXITCODE -ne 0) {
    throw "Bridge/Adapter authorization hot reload is unavailable; no workspace was added."
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

& $Bridge --config $ConfigPath reload-targets
if ($LASTEXITCODE -ne 0) {
    throw "Workspaces were saved, but live authorization reload is unconfirmed. Check Bridge/Adapter status and retry reload-targets."
}

Write-Host "Target workspace configuration applied. Send /sessions in QQ to verify it."
