[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Workspace,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml",

    [int]$TrustTimeoutSeconds = 300,

    [switch]$SkipRestart,

    [switch]$DoNotOpenCode
)

$ErrorActionPreference = "Stop"
$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"
$SwitchScript = Join-Path $InstallDirectory "scripts\switch-vscode-integration.ps1"

if (-not (Test-Path -LiteralPath $Bridge -PathType Leaf)) {
    throw "Bridge executable not found: $Bridge"
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

if (-not $SkipRestart -and (Test-Path -LiteralPath $SwitchScript -PathType Leaf)) {
    & $SwitchScript `
        -Mode Ahp `
        -InstallDirectory $InstallDirectory `
        -ConfigPath $ConfigPath `
        -RequireIdle
    if ($LASTEXITCODE -ne 0) {
        throw "Bridge restart failed with exit code $LASTEXITCODE"
    }
}

Write-Host "Registered local target workspace: $Resolved"
