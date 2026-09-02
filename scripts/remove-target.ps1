[CmdletBinding(DefaultParameterSetName = "Local")]
param(
    [Parameter(Mandatory = $true, ParameterSetName = "Local")]
    [string]$LocalWorkspace,

    [Parameter(Mandatory = $true, ParameterSetName = "Remote")]
    [string]$SshAlias,

    [Parameter(Mandatory = $true, ParameterSetName = "Remote")]
    [string]$RemoteWorkspace,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml",

    [switch]$SkipRestart
)

$ErrorActionPreference = "Stop"
$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"
$SwitchScript = Join-Path $InstallDirectory "scripts\switch-vscode-integration.ps1"

if (-not (Test-Path -LiteralPath $Bridge -PathType Leaf)) {
    throw "Bridge executable not found: $Bridge"
}

if ($PSCmdlet.ParameterSetName -eq "Local") {
    $Resolved = (Resolve-Path -LiteralPath $LocalWorkspace).Path
    & $Bridge --config $ConfigPath remove-local-target --workspace $Resolved
    if ($LASTEXITCODE -ne 0) {
        throw "qq-bridge remove-local-target failed with exit code $LASTEXITCODE"
    }
    Write-Host "Removed local target workspace: $Resolved"
}
else {
    if (-not $RemoteWorkspace.StartsWith("/")) {
        throw "Remote workspace must be an absolute POSIX path."
    }
    & $Bridge `
        --config $ConfigPath `
        remove-remote-target `
        --ssh-alias $SshAlias `
        --workspace $RemoteWorkspace
    if ($LASTEXITCODE -ne 0) {
        throw "qq-bridge remove-remote-target failed with exit code $LASTEXITCODE"
    }
    Write-Host "Removed remote target workspace: ssh:$SshAlias $RemoteWorkspace"
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
