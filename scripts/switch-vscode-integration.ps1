[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("Ahp", "Legacy")]
    [string]$Mode,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml"
)

$ErrorActionPreference = "Stop"
$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"
$GeneratedDirectory = Split-Path -Parent $ConfigPath
$GeneratedMcp = Join-Path $GeneratedDirectory "mcp.json"
$GeneratedAgent = Join-Path $GeneratedDirectory "qq-remote.agent.md"
$UserMcp = Join-Path $env:APPDATA "Code\User\mcp.json"
$UserAgentDirectory = Join-Path $env:USERPROFILE ".copilot\agents"
$UserAgent = Join-Path $UserAgentDirectory "qq-remote.agent.md"
$BackupDirectory = Join-Path $GeneratedDirectory "legacy-vscode"
$StatusExtensionSource = Join-Path $InstallDirectory "vscode-extension\qq-copilot-ahp-status"
$StatusExtensionTarget = Join-Path `
    $env:USERPROFILE `
    ".vscode\extensions\guoyu-local.qq-copilot-ahp-status-0.1.0"
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

if (-not (Test-Path -LiteralPath $Bridge -PathType Leaf)) {
    throw "Bridge executable not found: $Bridge"
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw "Bridge config not found: $ConfigPath"
}

New-Item -ItemType Directory -Path $BackupDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $UserAgentDirectory -Force | Out-Null

if (Test-Path -LiteralPath $UserMcp -PathType Leaf) {
    $Mcp = Get-Content -LiteralPath $UserMcp -Raw | ConvertFrom-Json
}
else {
    $Mcp = [PSCustomObject]@{
        servers = [PSCustomObject]@{}
        inputs = @()
    }
}
if (-not $Mcp.servers) {
    $Mcp | Add-Member -MemberType NoteProperty -Name "servers" -Value ([PSCustomObject]@{})
}

if ($Mode -eq "Ahp") {
    $LegacyServer = $Mcp.servers.PSObject.Properties["qq-copilot"]
    if ($LegacyServer) {
        [System.IO.File]::WriteAllText(
            (Join-Path $BackupDirectory "qq-copilot-mcp-server.json"),
            ($LegacyServer.Value | ConvertTo-Json -Depth 20),
            $Utf8NoBom
        )
        $Mcp.servers.PSObject.Properties.Remove("qq-copilot")
    }
    if (Test-Path -LiteralPath $UserAgent -PathType Leaf) {
        Copy-Item `
            -LiteralPath $UserAgent `
            -Destination (Join-Path $BackupDirectory "qq-remote.agent.md") `
            -Force
        Remove-Item -LiteralPath $UserAgent -Force
    }
    & $Bridge --config $ConfigPath set-mode ahp
    if (-not (Test-Path -LiteralPath $StatusExtensionSource -PathType Container)) {
        throw "Managed VS Code status extension not found: $StatusExtensionSource"
    }
    New-Item -ItemType Directory -Path $StatusExtensionTarget -Force | Out-Null
    Copy-Item `
        -LiteralPath (Join-Path $StatusExtensionSource "package.json") `
        -Destination (Join-Path $StatusExtensionTarget "package.json") `
        -Force
    Copy-Item `
        -LiteralPath (Join-Path $StatusExtensionSource "dist") `
        -Destination $StatusExtensionTarget `
        -Recurse `
        -Force
}
else {
    if (-not (Test-Path -LiteralPath $GeneratedMcp -PathType Leaf)) {
        throw "Generated legacy MCP config not found: $GeneratedMcp"
    }
    if (-not (Test-Path -LiteralPath $GeneratedAgent -PathType Leaf)) {
        throw "Generated legacy Agent not found: $GeneratedAgent"
    }
    $Legacy = Get-Content -LiteralPath $GeneratedMcp -Raw | ConvertFrom-Json
    $LegacyServer = $Legacy.servers.PSObject.Properties["qq-copilot"]
    if (-not $LegacyServer) {
        throw "Generated legacy MCP config does not contain qq-copilot"
    }
    $Mcp.servers | Add-Member `
        -MemberType NoteProperty `
        -Name "qq-copilot" `
        -Value $LegacyServer.Value `
        -Force
    Copy-Item -LiteralPath $GeneratedAgent -Destination $UserAgent -Force
    if (Test-Path -LiteralPath $StatusExtensionTarget -PathType Container) {
        Get-ChildItem -LiteralPath $StatusExtensionTarget -Force -Recurse -File |
            ForEach-Object {
                $_.IsReadOnly = $false
            }
        [System.IO.Directory]::Delete($StatusExtensionTarget, $true)
    }
    & $Bridge --config $ConfigPath set-mode legacy
}
if ($LASTEXITCODE -ne 0) {
    throw "Bridge mode update failed with exit code $LASTEXITCODE"
}

[System.IO.File]::WriteAllText(
    $UserMcp,
    ($Mcp | ConvertTo-Json -Depth 100),
    $Utf8NoBom
)

$ManagedExecutables = @(
    (Join-Path $InstallDirectory "qq-bridge.exe"),
    (Join-Path $InstallDirectory "qq-mcp.exe")
)
$ManagedProcesses = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.ExecutablePath -in $ManagedExecutables -or
            (
                $_.Name -eq "node.exe" -and
                $_.CommandLine -match "CopilotQQBridge[\\/]ahp-adapter[\\/]dist[\\/]main\.js"
            )
        }
)
foreach ($Process in $ManagedProcesses) {
    Stop-Process -Id $Process.ProcessId
    Wait-Process -Id $Process.ProcessId -Timeout 15 -ErrorAction SilentlyContinue
}

$ConfigArgument = '"' + $ConfigPath + '"'
Start-Process `
    -FilePath $Bridge `
    -ArgumentList @("--config", $ConfigArgument, "run") `
    -WindowStyle Hidden
Start-Sleep -Seconds 2
& $Bridge --config $ConfigPath status | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Bridge did not become ready after integration switch"
}

Write-Host "VS Code integration switched to $Mode mode."
Write-Host "Run 'Developer: Reload Window' in VS Code so MCP and Agent discovery refresh immediately."
