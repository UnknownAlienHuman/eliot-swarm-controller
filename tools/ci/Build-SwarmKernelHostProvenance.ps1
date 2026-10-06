[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string] $TargetDir,
    [Parameter(Mandatory = $true)][string] $OutputDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'Kernel host provenance packaging emits Windows executables only.' }

$builder = Join-Path $PSScriptRoot 'build-module-package.ps1'
if (-not (Test-Path -LiteralPath $builder -PathType Leaf)) {
    throw 'The shared module package builder is missing.'
}
& $builder -Package 'swarm-kernel-host' -Profile 'release' `
    -TargetDir $TargetDir -OutputDir $OutputDir
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }