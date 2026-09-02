[CmdletBinding()]
param(
    [string]$Revision = "a0bc67f840788f816c9b44bb1325181cb4c4661d",

    [switch]$Force
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$VendorDirectory = Join-Path $ProjectRoot "vendor"
$PackageName = "microsoft-agent-host-protocol-0.8.0.tgz"
$HashName = "$PackageName.sha256"
$PackagePath = Join-Path $VendorDirectory $PackageName
$HashPath = Join-Path $VendorDirectory $HashName
$OverlayPath = Join-Path $VendorDirectory "ahp-vscode-1.136.patch"
$TempDirectory = Join-Path `
    ([Environment]::GetFolderPath("LocalApplicationData")) `
    "Temp"
$WorkDirectory = Join-Path $TempDirectory ("qq-copilot-ahp-" + [Guid]::NewGuid().ToString("N"))
$PackDirectory = Join-Path $WorkDirectory "packed"
$PrimaryError = $null

if (
    -not $Force -and
    (Test-Path -LiteralPath $PackagePath -PathType Leaf) -and
    (Test-Path -LiteralPath $HashPath -PathType Leaf)
) {
    Write-Host "AHP client package already exists: $PackagePath"
    exit 0
}
if (-not (Test-Path -LiteralPath $OverlayPath -PathType Leaf)) {
    throw "Missing VS Code AHP compatibility overlay: $OverlayPath"
}

New-Item -ItemType Directory -Path $VendorDirectory -Force | Out-Null
New-Item -ItemType Directory -Path $WorkDirectory -Force | Out-Null

try {
    & git clone `
        --filter=blob:none `
        --no-checkout `
        "https://github.com/microsoft/agent-host-protocol.git" `
        $WorkDirectory
    if ($LASTEXITCODE -ne 0) {
        throw "git clone failed with exit code $LASTEXITCODE"
    }

    & git -C $WorkDirectory checkout --detach $Revision
    if ($LASTEXITCODE -ne 0) {
        throw "git checkout failed with exit code $LASTEXITCODE"
    }

    & git -C $WorkDirectory apply --check $OverlayPath
    if ($LASTEXITCODE -ne 0) {
        throw "VS Code AHP compatibility overlay check failed with exit code $LASTEXITCODE"
    }
    & git -C $WorkDirectory apply $OverlayPath
    if ($LASTEXITCODE -ne 0) {
        throw "VS Code AHP compatibility overlay failed with exit code $LASTEXITCODE"
    }

    New-Item -ItemType Directory -Path $PackDirectory -Force | Out-Null
    Push-Location $WorkDirectory
    try {
        & npm ci
        if ($LASTEXITCODE -ne 0) {
            throw "AHP root npm ci failed with exit code $LASTEXITCODE"
        }
        & npm run generate:typescript
        if ($LASTEXITCODE -ne 0) {
            throw "AHP TypeScript generation failed with exit code $LASTEXITCODE"
        }
        & npm --prefix "clients\typescript" ci
        if ($LASTEXITCODE -ne 0) {
            throw "AHP client npm ci failed with exit code $LASTEXITCODE"
        }
        & npm --prefix "clients\typescript" run build
        if ($LASTEXITCODE -ne 0) {
            throw "AHP client build failed with exit code $LASTEXITCODE"
        }
        & npm pack ".\clients\typescript" --pack-destination $PackDirectory
        if ($LASTEXITCODE -ne 0) {
            throw "AHP client pack failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }

    $GeneratedPackage = Join-Path $PackDirectory $PackageName
    if (-not (Test-Path -LiteralPath $GeneratedPackage -PathType Leaf)) {
        throw "Expected AHP package was not generated: $GeneratedPackage"
    }
    $Hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $GeneratedPackage).Hash.ToLowerInvariant()
    $GeneratedHashPath = Join-Path $WorkDirectory $HashName
    [System.IO.File]::WriteAllText(
        $GeneratedHashPath,
        "$Hash  $PackageName`n",
        (New-Object System.Text.UTF8Encoding($false))
    )
    Copy-Item -LiteralPath $GeneratedPackage -Destination $PackagePath -Force
    Copy-Item -LiteralPath $GeneratedHashPath -Destination $HashPath -Force
    Write-Host "Generated pinned AHP client package: $PackagePath"
}
catch {
    $PrimaryError = $_
}
finally {
    if (Test-Path -LiteralPath $WorkDirectory) {
        try {
            Get-ChildItem -LiteralPath $WorkDirectory -Force -Recurse -File |
                ForEach-Object {
                    $_.IsReadOnly = $false
                }
            [System.IO.Directory]::Delete($WorkDirectory, $true)
        }
        catch {
            if ($null -eq $PrimaryError) {
                throw
            }
            Write-Warning "Failed to fully clean temporary AHP checkout: $WorkDirectory"
        }
    }
}

if ($null -ne $PrimaryError) {
    throw $PrimaryError
}
