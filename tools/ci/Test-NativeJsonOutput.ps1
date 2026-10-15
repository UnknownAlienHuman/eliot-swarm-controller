#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$sourcePath = Join-Path $PSScriptRoot 'build-module-package.ps1'
$tokens = $null
$parseErrors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($sourcePath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count -ne 0) { throw 'Package-builder source does not parse.' }
foreach ($name in @('Invoke-NativeText', 'Invoke-NativeStdout')) {
    $definition = @($ast.FindAll({
        param($node)
        $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name
    }, $true))
    if ($definition.Count -ne 1) { throw "Expected one production function $name." }
    . ([scriptblock]::Create($definition[0].Extent.Text))
}

$shellPath = (Get-Process -Id $PID).Path
$warningThenJson = @'
[Console]::Error.WriteLine('Blocking waiting for file lock on package cache')
[Console]::Out.WriteLine('{"packages":[],"workspace_members":[],"format":"metadata-fixture"}')
'@
$arguments = @('-NoProfile', '-NonInteractive', '-Command', $warningThenJson)
$merged = Invoke-NativeText $shellPath $arguments
$oldRejected = $false
try { $null = $merged | ConvertFrom-Json -AsHashtable } catch { $oldRejected = $true }
if (-not $oldRejected) { throw 'Negative control accepted stderr-contaminated JSON.' }

$json = Invoke-NativeStdout $shellPath $arguments
$metadata = $json | ConvertFrom-Json -AsHashtable
if ($metadata.format -cne 'metadata-fixture' -or $metadata.packages.Count -ne 0 -or
    $metadata.workspace_members.Count -ne 0 -or $json.Contains('Blocking waiting')) {
    throw 'Successful stdout was contaminated or lost.'
}

$failingChild = @'
[Console]::Out.WriteLine('{"packages":[]}')
[Console]::Error.WriteLine('metadata-fixture-failure')
exit 7
'@
$failure = $null
try {
    $null = Invoke-NativeStdout $shellPath @('-NoProfile', '-NonInteractive', '-Command', $failingChild)
} catch { $failure = $_.Exception.Message }
if ($null -eq $failure -or -not $failure.Contains('exit code 7') -or
    -not $failure.Contains('metadata-fixture-failure') -or -not $failure.Contains('"packages"')) {
    throw 'Nonzero native exit was accepted or its diagnostics were lost.'
}
'Native JSON boundary: original negative control, separate stdout, and nonzero rejection passed.'
