[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $ChangedPathsJson,
    [Parameter(Mandatory = $true)]
    [string] $FormattedPathsJson,
    [switch] $FailOnOwned
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Read-PathArray([string] $Json, [string] $Label) {
    try {
        $parsed = @($Json | ConvertFrom-Json)
    } catch {
        throw "$Label is not valid JSON: $($_.Exception.Message)"
    }
    return @(
        foreach ($path in $parsed) {
            if ($null -eq $path) { continue }
            $normalized = ([string]$path).Trim().Replace('\', '/')
            if (-not [string]::IsNullOrWhiteSpace($normalized)) {
                $normalized
            }
        }
    )
}

$comparer = if ($IsWindows) {
    [StringComparer]::OrdinalIgnoreCase
} else {
    [StringComparer]::Ordinal
}
$changedRust = [Collections.Generic.HashSet[string]]::new($comparer)
foreach ($path in (Read-PathArray $ChangedPathsJson 'ChangedPathsJson')) {
    if ($path -match '\.rs$' -and $path -notmatch '^vendor/atlas/') {
        [void]$changedRust.Add($path)
    }
}

$owned = [Collections.Generic.List[string]]::new()
$historical = [Collections.Generic.List[string]]::new()
foreach ($path in (Read-PathArray $FormattedPathsJson 'FormattedPathsJson' | Sort-Object -Unique)) {
    if ($changedRust.Contains($path)) {
        $owned.Add($path)
    } else {
        $historical.Add($path)
    }
}

$result = [ordered]@{
    owned = @($owned)
    historical = @($historical)
}
$resultJson = ConvertTo-Json -InputObject $result -Depth 3 -Compress
Write-Output $resultJson

if ($FailOnOwned -and $owned.Count -gt 0) {
    throw "rustfmt would change owned diff paths: $($owned -join ', ')"
}
