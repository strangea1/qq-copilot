[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$Workspace,

    [string]$InstallDirectory = "$env:LOCALAPPDATA\Programs\CopilotQQBridge",

    [string]$ConfigPath = "$env:LOCALAPPDATA\CopilotQQBridge\config.toml",

    [string[]]$AhpWorkspace,

    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot

if (-not $SkipBuild) {
    Push-Location $ProjectRoot
    try {
        & cargo build --release --locked
        if ($LASTEXITCODE -ne 0) {
            throw "cargo build failed with exit code $LASTEXITCODE"
        }
        & npm ci --prefix adapter
        if ($LASTEXITCODE -ne 0) {
            throw "AHP Adapter npm ci failed with exit code $LASTEXITCODE"
        }
        & npm run build --prefix adapter
        if ($LASTEXITCODE -ne 0) {
            throw "AHP Adapter build failed with exit code $LASTEXITCODE"
        }
        & npm ci --prefix vscode-extension
        if ($LASTEXITCODE -ne 0) {
            throw "VS Code status extension npm ci failed with exit code $LASTEXITCODE"
        }
        & npm run build --prefix vscode-extension
        if ($LASTEXITCODE -ne 0) {
            throw "VS Code status extension build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}

$ResolvedWorkspaces = @()
foreach ($Path in $Workspace) {
    $Resolved = (Resolve-Path -LiteralPath $Path).Path
    if (-not (Test-Path -LiteralPath $Resolved -PathType Container)) {
        throw "Workspace is not a directory: $Resolved"
    }
    $ResolvedWorkspaces += $Resolved
}
$ResolvedAhpWorkspaces = @()
foreach ($Path in $AhpWorkspace) {
    $Resolved = (Resolve-Path -LiteralPath $Path).Path
    if (-not (Test-Path -LiteralPath $Resolved -PathType Container)) {
        throw "AHP target workspace is not a directory: $Resolved"
    }
    $ResolvedAhpWorkspaces += $Resolved
}

New-Item -ItemType Directory -Path $InstallDirectory -Force | Out-Null
$CurrentUser = if ($env:USERDOMAIN) {
    "$env:USERDOMAIN\$env:USERNAME"
}
else {
    $env:USERNAME
}

& icacls.exe $InstallDirectory /inheritance:r /grant:r "${CurrentUser}:(OI)(CI)(F)" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "Failed to harden the install directory ACL"
}

$Binaries = @("qq-bridge.exe", "copilot-qq-hook.exe", "qq-mcp.exe")
foreach ($Binary in $Binaries) {
    $Source = Join-Path $ProjectRoot "target\release\$Binary"
    if (-not (Test-Path -LiteralPath $Source -PathType Leaf)) {
        throw "Missing release binary: $Source"
    }
    $Destination = Join-Path $InstallDirectory $Binary
    $Copied = $false
    for ($Attempt = 1; $Attempt -le 20; $Attempt++) {
        try {
            Copy-Item -LiteralPath $Source -Destination $Destination -Force
            $Copied = $true
            break
        }
        catch [System.IO.IOException] {
            if ($Attempt -eq 20) {
                throw
            }
            Start-Sleep -Milliseconds 300
        }
    }
    if (-not $Copied) {
        throw "Failed to install binary: $Binary"
    }
}
Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "scripts\copilot-qq-hook.ps1") `
    -Destination (Join-Path $InstallDirectory "copilot-qq-hook.ps1") `
    -Force
$ManagedScriptsDirectory = Join-Path $InstallDirectory "scripts"
New-Item -ItemType Directory -Path $ManagedScriptsDirectory -Force | Out-Null
foreach ($ManagedScript in @(
    "add-workspace.ps1",
    "switch-vscode-integration.ps1",
    "register-startup.ps1"
)) {
    Copy-Item `
        -LiteralPath (Join-Path $ProjectRoot "scripts\$ManagedScript") `
        -Destination (Join-Path $ManagedScriptsDirectory $ManagedScript) `
        -Force
}

$NodeCommand = Get-Command node.exe -ErrorAction Stop
$NodeVersion = (& $NodeCommand.Source --version).TrimStart("v")
$NodeMajor = [int]($NodeVersion.Split(".")[0])
if ($NodeMajor -lt 24) {
    throw "AHP Adapter requires Node 24 or newer; found $NodeVersion"
}

$AdapterDirectory = Join-Path $InstallDirectory "ahp-adapter"
$AdapterVendorDirectory = Join-Path $InstallDirectory "vendor"
New-Item -ItemType Directory -Path $AdapterDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $AdapterVendorDirectory -Force | Out-Null
Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "adapter\package.json") `
    -Destination (Join-Path $AdapterDirectory "package.json") `
    -Force
Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "adapter\package-lock.json") `
    -Destination (Join-Path $AdapterDirectory "package-lock.json") `
    -Force
Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "adapter\dist") `
    -Destination $AdapterDirectory `
    -Recurse `
    -Force
Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "vendor\microsoft-agent-host-protocol-0.8.0.tgz") `
    -Destination (Join-Path $AdapterVendorDirectory "microsoft-agent-host-protocol-0.8.0.tgz") `
    -Force
Push-Location $AdapterDirectory
try {
    & npm ci --omit=dev --ignore-scripts
    if ($LASTEXITCODE -ne 0) {
        throw "AHP Adapter production dependency install failed with exit code $LASTEXITCODE"
    }

    $StatusExtensionSource = Join-Path $InstallDirectory "vscode-extension\qq-copilot-ahp-status"
    New-Item -ItemType Directory -Path $StatusExtensionSource -Force | Out-Null
    Copy-Item `
        -LiteralPath (Join-Path $ProjectRoot "vscode-extension\package.json") `
        -Destination (Join-Path $StatusExtensionSource "package.json") `
        -Force
    Copy-Item `
        -LiteralPath (Join-Path $ProjectRoot "vscode-extension\dist") `
        -Destination $StatusExtensionSource `
        -Recurse `
        -Force
}
finally {
    Pop-Location
}

$Bridge = Join-Path $InstallDirectory "qq-bridge.exe"
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    $InitArgs = @("--config", $ConfigPath, "init")
    foreach ($Path in $ResolvedWorkspaces) {
        $InitArgs += @("--workspace", $Path)
    }
    & $Bridge @InitArgs
    if ($LASTEXITCODE -ne 0) {
        throw "qq-bridge init failed with exit code $LASTEXITCODE"
    }
}
else {
    $AddWorkspaceArgs = @("--config", $ConfigPath, "add-workspace")
    foreach ($Path in $ResolvedWorkspaces) {
        $AddWorkspaceArgs += @("--workspace", $Path)
    }
    & $Bridge @AddWorkspaceArgs
    if ($LASTEXITCODE -ne 0) {
        throw "qq-bridge add-workspace failed with exit code $LASTEXITCODE"
    }
}

if ($ResolvedAhpWorkspaces.Count -gt 0) {
    $ConfigureAhpArgs = @("--config", $ConfigPath, "configure-ahp")
    foreach ($Path in $ResolvedAhpWorkspaces) {
        $ConfigureAhpArgs += @("--workspace", $Path)
    }
    $ConfigureAhpArgs += @(
        "--node", $NodeCommand.Source,
        "--adapter-script", (Join-Path $AdapterDirectory "dist\main.js")
    )
    & $Bridge @ConfigureAhpArgs
    if ($LASTEXITCODE -ne 0) {
        throw "qq-bridge configure-ahp failed with exit code $LASTEXITCODE"
    }
}

$ConfigDirectory = Split-Path -Parent $ConfigPath
$Hook = Join-Path $InstallDirectory "copilot-qq-hook.ps1"
$Mcp = Join-Path $InstallDirectory "qq-mcp.exe"
$HookPrefix = 'powershell.exe -NoLogo -NoProfile -NonInteractive ' +
    '-ExecutionPolicy RemoteSigned -File "' + $Hook +
    '" -ConfigPath "' + $ConfigPath + '" -Mode'

$Hooks = @{
    hooks = @{
        SessionStart = @(
            @{ type = "command"; windows = "$HookPrefix prompt"; timeout = 15 }
        )
        UserPromptSubmit = @(
            @{ type = "command"; windows = "$HookPrefix prompt"; timeout = 15 }
        )
        PreToolUse = @(
            @{ type = "command"; windows = "$HookPrefix pre-tool"; timeout = 660 }
        )
        PostToolUse = @(
            @{ type = "command"; windows = "$HookPrefix post-tool"; timeout = 15 }
        )
        Stop = @(
            @{ type = "command"; windows = "$HookPrefix stop"; timeout = 30 }
        )
    }
}

$McpConfig = @{
    servers = @{
        "qq-copilot" = @{
            type = "stdio"
            command = $Mcp
            args = @("--config", $ConfigPath)
        }
    }
}

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText(
    (Join-Path $ConfigDirectory "hooks.json"),
    ($Hooks | ConvertTo-Json -Depth 10),
    $Utf8NoBom
)
[System.IO.File]::WriteAllText(
    (Join-Path $ConfigDirectory "mcp.json"),
    ($McpConfig | ConvertTo-Json -Depth 10),
    $Utf8NoBom
)
$AgentTemplate = Get-Content `
    -LiteralPath (Join-Path $ProjectRoot "examples\qq-remote.agent.md") `
    -Raw
$AgentContent = $AgentTemplate.Replace(
    "{{INSTALL_DIR}}",
    $InstallDirectory
).Replace(
    "{{CONFIG_PATH}}",
    $ConfigPath
)
[System.IO.File]::WriteAllText(
    (Join-Path $ConfigDirectory "qq-remote.agent.md"),
    $AgentContent,
    $Utf8NoBom
)

Write-Host "Installed binaries to $InstallDirectory"
Write-Host "Config: $ConfigPath"
Write-Host "AHP Adapter: $AdapterDirectory"
Write-Host "VS Code status extension: $StatusExtensionSource"
Write-Host "Next: set qq.app_id, run qq-bridge store-secret, then start qq-bridge run."
if ($ResolvedAhpWorkspaces.Count -gt 0) {
    Write-Host "Next: run scripts\switch-vscode-integration.ps1 -Mode Ahp, reload VS Code,"
    Write-Host "then create/list/bind an Agents Window Session in a target workspace."
}
else {
    Write-Host "Legacy mode: register mcp.json, copy qq-remote.agent.md to ~/.copilot/agents,"
    Write-Host "and enable chat.useCustomAgentHooks."
}
