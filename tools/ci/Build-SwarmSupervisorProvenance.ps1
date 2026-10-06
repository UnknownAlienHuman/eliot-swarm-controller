[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string] $TargetDir,
    [Parameter(Mandatory = $true)][string] $OutputDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$builder = Join-Path $PSScriptRoot 'build-module-package.ps1'
& $builder -Package 'swarm-supervisor' -Profile 'release' -TargetDir $TargetDir -OutputDir $OutputDir
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }