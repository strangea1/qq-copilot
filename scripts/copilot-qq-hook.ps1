[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,

    [Parameter(Mandatory = $true)]
    [ValidateSet("prompt", "pre-tool", "post-tool", "stop")]
    [string]$Mode
)

$ErrorActionPreference = "Stop"
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom

$Hook = Join-Path $PSScriptRoot "copilot-qq-hook.exe"
if (-not (Test-Path -LiteralPath $Hook -PathType Leaf)) {
    Write-Error "Hook executable not found: $Hook"
    exit 2
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    Write-Error "Bridge config not found: $ConfigPath"
    exit 2
}

$HookInput = [Console]::In.ReadToEnd()
if ([string]::IsNullOrWhiteSpace($HookInput)) {
    Write-Error "Hook stdin was empty"
    exit 2
}
$HookInput | & $Hook --config $ConfigPath $Mode
exit $LASTEXITCODE
exit $LASTEXITCODE
