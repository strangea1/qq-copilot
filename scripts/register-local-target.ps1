[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Workspace,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml",

    [int]$TrustTimeoutSeconds = 300,

    [Alias("SkipRestart")]
    [switch]$SkipReloadConfirmation,

    [switch]$DoNotOpenCode
)

$ErrorActionPreference = "Stop"
$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"

if (-not (Test-Path -LiteralPath $Bridge -PathType Leaf)) {
    throw "Bridge executable not found: $Bridge"
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw "Bridge config not found: $ConfigPath"
}
if (-not $SkipReloadConfirmation) {
    & $Bridge --config $ConfigPath reload-targets
    if ($LASTEXITCODE -ne 0) {
        throw "Bridge/Adapter authorization hot reload is unavailable; no target was registered."
    }
}

$Resolved = (Resolve-Path -LiteralPath $Workspace).Path
if (-not (Test-Path -LiteralPath $Resolved -PathType Container)) {
    throw "Workspace is not a directory: $Resolved"
}

& $Bridge `
    --config $ConfigPath `
    register-local-target `
    --workspace $Resolved `
    --open-vscode $(if ($DoNotOpenCode) { "false" } else { "true" }) `
    --trust-timeout-seconds $TrustTimeoutSeconds
if ($LASTEXITCODE -ne 0) {
    throw "qq-bridge register-local-target failed with exit code $LASTEXITCODE"
}

if (-not $SkipReloadConfirmation) {
    & $Bridge --config $ConfigPath reload-targets
    if ($LASTEXITCODE -ne 0) {
        throw "The target was saved, but live authorization reload is unconfirmed. Check Bridge/Adapter status and retry reload-targets."
    }
}

Write-Host "Registered local target workspace: $Resolved"
