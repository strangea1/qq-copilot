[CmdletBinding()]
param(
    [string]$Revision = "f770e26b8483de59050e8de71b65a20efdab62d4"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$VendorDirectory = Join-Path $ProjectRoot "vendor"
$PackagePath = Join-Path $VendorDirectory "microsoft-agent-host-protocol-0.8.0.tgz"
$TempDirectory = Join-Path `
    ([Environment]::GetFolderPath("LocalApplicationData")) `
    "Temp"
$WorkDirectory = Join-Path $TempDirectory ("qq-copilot-ahp-" + [Guid]::NewGuid().ToString("N"))
$PrimaryError = $null

if (Test-Path -LiteralPath $PackagePath -PathType Leaf) {
    Write-Host "AHP client package already exists: $PackagePath"
    exit 0
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
        & npm pack ".\clients\typescript" --pack-destination $VendorDirectory
        if ($LASTEXITCODE -ne 0) {
            throw "AHP client pack failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }

    if (-not (Test-Path -LiteralPath $PackagePath -PathType Leaf)) {
        throw "Expected AHP package was not generated: $PackagePath"
    }
    $Hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $PackagePath).Hash.ToLowerInvariant()
    [System.IO.File]::WriteAllText(
        (Join-Path $VendorDirectory "microsoft-agent-host-protocol-0.8.0.tgz.sha256"),
        "$Hash  microsoft-agent-host-protocol-0.8.0.tgz`n",
        (New-Object System.Text.UTF8Encoding($false))
    )
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
