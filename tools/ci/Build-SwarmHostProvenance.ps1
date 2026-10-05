[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string] $TargetDir,
    [Parameter(Mandatory = $true)][string] $OutputDir
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { throw 'Host provenance packaging emits Windows executables only.' }

function Get-CanonicalPath([string] $Path, [string] $Label) {
    if (-not [IO.Path]::IsPathFullyQualified($Path)) { throw "$Label must be an absolute path." }
    return [IO.Path]::GetFullPath($Path)
}

$targetPath = Get-CanonicalPath $TargetDir 'TargetDir'
$outputPath = Get-CanonicalPath $OutputDir 'OutputDir'
if (-not (Test-Path -LiteralPath $targetPath -PathType Container)) {
    throw 'TargetDir must be an existing shared Cargo target directory; this entrypoint never creates it.'
}
$targetItem = Get-Item -LiteralPath $targetPath
if (($targetItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw 'TargetDir must not itself be a reparse point.'
}
if (Test-Path -LiteralPath $outputPath) {
    throw 'OutputDir must not already exist; refusing to reuse or overwrite a provenance package.'
}

$builder = Join-Path $PSScriptRoot 'build-module-package.ps1'
if (-not (Test-Path -LiteralPath $builder -PathType Leaf)) {
    throw 'The shared module package builder is missing.'
}
& $builder -Package 'eliot-swarm-controller' -Profile 'release' `
    -TargetDir $targetPath -OutputDir $outputPath
