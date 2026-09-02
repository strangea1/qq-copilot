[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$SshAlias,

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

if (-not $Workspace.StartsWith("/")) {
    throw "Remote workspace must be an absolute POSIX path."
}

& $Bridge `
    --config $ConfigPath `
    register-remote-target `
    --ssh-alias $SshAlias `
    --workspace $Workspace `
    --open-vscode $(if ($DoNotOpenCode) { "false" } else { "true" }) `
    --trust-timeout-seconds $TrustTimeoutSeconds
if ($LASTEXITCODE -ne 0) {
    throw "qq-bridge register-remote-target failed with exit code $LASTEXITCODE"
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

Write-Host "Registered remote target workspace: ssh:$SshAlias $Workspace"
