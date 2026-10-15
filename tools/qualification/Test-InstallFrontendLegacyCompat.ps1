#Requires -Version 7.0
# Focused installer regression: legacy swarm-kernel-host inventory compatibility.
#
# Loads the REAL production functions from tools/modules/Install-SwarmFrontend.ps1
# via AST (function definitions only; the installer top-level flow never executes)
# and stages REAL fixtures on disk with REAL SHA-256 pins computed by the
# production Get-Sha256 / Get-DependencyTreeRows helpers.
#
# Covers the P2: a valid legacy host (no Antigravity additional group) stays
# accepted/idempotent and does not block peer installs, while new hosts still
# require the exact current Antigravity group and tampered/missing group files
# are still rejected. Tool-identity pins (node/npm) are synthetic but
# self-consistent; every file pin is a real hash of staged bytes.
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$installerPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..' 'modules' 'Install-SwarmFrontend.ps1'))
if (-not (Test-Path -LiteralPath $installerPath -PathType Leaf)) { throw "Production installer not found: $installerPath" }

$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($installerPath, [ref]$null, [ref]$parseErrors)
if ($parseErrors.Count -gt 0) { throw "Production installer has parse errors: $($parseErrors[0].Message)" }
foreach ($func in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true)) {
    $loader = [scriptblock]::Create($func.Extent.Text)
    . $loader
}
$mapAssign = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left -is [System.Management.Automation.Language.VariableExpressionAst] -and $n.Left.VariablePath.UserPath -eq 'packageMap' }, $true) | Select-Object -First 1
if ($null -eq $mapAssign) { throw 'Production $packageMap assignment not found.' }
$packageMap = & ([scriptblock]::Create($mapAssign.Right.Extent.Text))
$spec = $packageMap['swarm-kernel-host']
if ($null -eq $spec) { throw 'Production swarm-kernel-host spec not found.' }

$failures = [Collections.Generic.List[string]]::new()
function Assert-True([bool]$Condition, [string]$Name, [string]$Detail = '') {
    if ($Condition) { Write-Output "PASS: $Name" }
    else { $failures.Add("$Name :: $Detail"); Write-Output "FAIL: $Name :: $Detail" }
}
function Expect-Throw([scriptblock]$Code, [string]$Like, [string]$Name) {
    try {
        & $Code
        $failures.Add("$Name :: expected throw, got success")
        Write-Output "FAIL: $Name :: expected throw like '$Like', got success"
    } catch {
        if ($_.Exception.Message -like $Like) { Write-Output "PASS: $Name" }
        else { $failures.Add("$Name :: wrong error: $($_.Exception.Message)"); Write-Output "FAIL: $Name :: wrong error: $($_.Exception.Message)" }
    }
}
function Write-Ascii([string]$Path, [string]$Content) {
    [void][IO.Directory]::CreateDirectory((Split-Path -Parent $Path))
    [IO.File]::WriteAllText($Path, $Content, [Text.UTF8Encoding]::new($false))
}

$work = Join-Path ([IO.Path]::GetTempPath()) ('legacy-compat-' + [Guid]::NewGuid().ToString('N'))
$shimDir = Join-Path $work 'shims'
Write-Ascii (Join-Path $shimDir 'cargo-toml.txt') 'cargo-toml-shim'
Write-Ascii (Join-Path $shimDir 'cargo-lock.txt') 'cargo-lock-shim'
Write-Ascii (Join-Path $shimDir 'toolchain.txt') 'toolchain-shim'
Write-Ascii (Join-Path $shimDir 'checksum.txt') 'checksum-shim'
Write-Ascii (Join-Path $shimDir 'node.txt') 'node-shim'
Write-Ascii (Join-Path $shimDir 'npm.txt') 'npm-shim'
$cargoTomlHash = Get-Sha256 (Join-Path $shimDir 'cargo-toml.txt')
$cargoLockHash = Get-Sha256 (Join-Path $shimDir 'cargo-lock.txt')
$toolchainHash = Get-Sha256 (Join-Path $shimDir 'toolchain.txt')
$checksumHex = Get-Sha256 (Join-Path $shimDir 'checksum.txt')
$nodeExeHash = Get-Sha256 (Join-Path $shimDir 'node.txt')
$npmCliHash = Get-Sha256 (Join-Path $shimDir 'npm.txt')
$commitHex = 'ab12' * 10
$treeHex = 'cd34' * 10

function New-HostFixture([string]$Root, [switch]$WithAntigravity) {
    $policy = $spec.resource_dependency_install
    $kernelRoot = Join-Path $Root ([string]$spec.resource_installed_relative_root)
    $kernelRows = foreach ($entry in @($spec.resource_files)) {
        $p = Join-Path $kernelRoot ([string]$entry.path)
        Write-Ascii $p ("kernel::$($entry.path)::v1`n")
        $sha = Get-Sha256 $p
        [ordered]@{ path = [string]$entry.path; role = [string]$entry.role; file = ([string]$spec.resource_installed_relative_root + '/' + [string]$entry.path); bytes = [long](Get-Item -LiteralPath $p).Length; source_sha256 = $sha; artifact_sha256 = $sha }
    }
    $nmRoot = Join-Path $kernelRoot ([string]$policy.output_directory)
    Write-Ascii (Join-Path $nmRoot 'pkg/index.js') "'use strict';`n"
    $tree = Get-DependencyTreeRows $nmRoot $policy
    $treeHash = Get-DependencyTreeSha256 @($tree.files)
    $pkgJsonHash = Get-Sha256 (Join-Path $kernelRoot 'package.json')
    $pkgLockHash = Get-Sha256 (Join-Path $kernelRoot 'package-lock.json')
    $closureManifest = [ordered]@{
        schema_version = 1; format = 'eliot.opencode_dependency_closure.v1'; coordinate = [string]$spec.resource_coordinate
        package_json_sha256 = $pkgJsonHash; package_lock_sha256 = $pkgLockHash
        node = [ordered]@{ file = 'node.exe'; version = 'v22.0.0-test'; executable_sha256 = $nodeExeHash }
        npm = [ordered]@{ file = 'npm-cli.js'; version = '10.0.0-test'; cli_sha256 = $npmCliHash }
        tree_sha256 = $treeHash; entry_count = [long]$tree.entry_count; file_count = [long]$tree.files.Count; total_bytes = [long]$tree.total_bytes
        files = @($tree.files)
    }
    Write-Ascii (Join-Path $kernelRoot ([string]$policy.closure_manifest_file)) ((ConvertTo-Json -InputObject $closureManifest -Depth 10 -Compress) + "`n")
    $closurePin = [ordered]@{
        schema_version = 1; manifest_file = [string]$policy.closure_manifest_file; manifest_sha256 = (Get-Sha256 (Join-Path $kernelRoot ([string]$policy.closure_manifest_file)))
        coordinate = [string]$spec.resource_coordinate; package_json_sha256 = $pkgJsonHash; package_lock_sha256 = $pkgLockHash; tree_sha256 = $treeHash
        node_version = 'v22.0.0-test'; node_executable_sha256 = $nodeExeHash; node_file = 'node.exe'
        npm_version = '10.0.0-test'; npm_cli_sha256 = $npmCliHash; npm_cli_file = 'npm-cli.js'
        entry_count = [long]$tree.entry_count; file_count = [long]$tree.files.Count; total_bytes = [long]$tree.total_bytes
    }
    $groupObjs = @()
    if ($WithAntigravity) {
        $gspec = @($spec.additional_resource_groups)[0]
        $groot = Join-Path $Root ([string]$gspec.installed_relative_root)
        $grows = foreach ($entry in @($gspec.resource_files)) {
            $p = Join-Path $groot ([string]$entry.path)
            Write-Ascii $p ("agy::$($entry.path)::v1`n")
            $sha = Get-Sha256 $p
            [ordered]@{ path = [string]$entry.path; role = [string]$entry.role; file = ([string]$gspec.installed_relative_root + '/' + [string]$entry.path); bytes = [long](Get-Item -LiteralPath $p).Length; source_sha256 = $sha; artifact_sha256 = $sha }
        }
        $groupObjs = @([ordered]@{ coordinate = [string]$gspec.coordinate; module_id = [string]$gspec.module_id; artifact_id = [string]$gspec.artifact_id; version = [string]$gspec.version; repository_relative_root = [string]$gspec.repository_relative_root; installed_relative_root = [string]$gspec.installed_relative_root; files = @($grows) })
    }
    $depPolicy = [ordered]@{
        node_modules = 'installer_generated_locked_closure'; manager = [string]$policy.manager; install_command = 'npm ci --ignore-scripts'
        arguments = @(@($policy.arguments) | ForEach-Object { [string]$_ })
        output_directory = [string]$policy.output_directory; closure_manifest_file = [string]$policy.closure_manifest_file
        max_files = [long]$policy.max_files; max_entries = [long]$policy.max_entries; max_total_bytes = [long]$policy.max_total_bytes
        max_manifest_bytes = [long]$policy.max_manifest_bytes; timeout_seconds = [long]$policy.timeout_seconds
    }
    $resources = [ordered]@{ schema_version = 1; coordinate = [string]$spec.resource_coordinate; repository_relative_root = 'modules/opencode'; installed_relative_root = [string]$spec.resource_installed_relative_root; files = @($kernelRows); dependency_policy = $depPolicy }
    if ($WithAntigravity) { $resources.additional_groups = $groupObjs }
    $exePath = Join-Path $Root 'swarm-kernel-host.exe'
    Write-Ascii $exePath 'MZ-fixture-host-binary-v1'
    $exeHash = Get-Sha256 $exePath
    $exeBytes = [long](Get-Item -LiteralPath $exePath).Length
    $manifest = [ordered]@{
        schema_version = 1; format = 'eliot.module_build_manifest.v1'
        build = [ordered]@{ package_name = 'swarm-kernel-host'; package_manifest = 'crates/swarm-kernel-host/Cargo.toml'; binary_targets = @('swarm-kernel-host'); profile = 'release'; package_version = '1.0.0-fixture' }
        source = [ordered]@{ commit = $commitHex; tree = $treeHex; checkout_clean_before = $true; checkout_clean_after = $true; cargo_toml_sha256 = $cargoTomlHash; cargo_lock_sha256 = $cargoLockHash; rust_toolchain_toml_sha256 = $toolchainHash; package_manifest_sha256 = $checksumHex; workspace_cargo_toml_sha256 = $cargoTomlHash }
        dependency_pins = @([ordered]@{ name = 'serde'; version = '1.0.100'; source = 'registry+https://github.com/rust-lang/crates.io-index'; checksum = $checksumHex; features = @() })
        compatibility = [ordered]@{
            host_ipc = [ordered]@{ protocol_version = 1 }
            target = [ordered]@{ rustc_host_triple = 'x86_64-pc-windows-msvc' }
            host_runtime = [ordered]@{ package_name = 'swarm-kernel-host'; manifest = 'crates/swarm-kernel-host/Cargo.toml'; binary_target = 'swarm-kernel-host'; role = 'host_runtime'; resource_coordinate = [string]$spec.resource_coordinate; resource_installed_relative_root = [string]$spec.resource_installed_relative_root }
            host_supervisor = [ordered]@{ package_name = 'swarm-supervisor'; manifest = 'crates/swarm-supervisor/Cargo.toml'; binary_target = 'swarm-supervisor'; role = 'host_supervisor' }
            required_sibling_binaries = @('swarm-supervisor'); process_role = 'host_runtime'
        }
        resources = $resources
        artifacts = @([ordered]@{ target_name = 'swarm-kernel-host'; file = 'bin/swarm-kernel-host.exe'; source_sha256 = $exeHash; artifact_sha256 = $exeHash; bytes = $exeBytes })
        toolchain = [ordered]@{ pinned_channel = '1.90.0'; rustc_version_verbose = 'rustc 1.90.0 (fixture)'; cargo_version_verbose = 'cargo 1.90.0 (fixture)' }
    }
    Write-Ascii (Join-Path $Root 'swarm-kernel-host.build-manifest.json') ((ConvertTo-Json -InputObject $manifest -Depth 14) + "`n")
    $manifestHash = Get-Sha256 (Join-Path $Root 'swarm-kernel-host.build-manifest.json')
    $receiptRows = foreach ($r in $kernelRows) { [ordered]@{ path = $r.path; file = $r.file; artifact_sha256 = $r.artifact_sha256 } }
    $receipt = [ordered]@{
        schema_version = 1; format = 'eliot.frontend_install_receipt.v1'; package_name = 'swarm-kernel-host'; role = 'host_runtime'
        binary_target = 'swarm-kernel-host'; binary_file = 'swarm-kernel-host.exe'; build_manifest_file = 'swarm-kernel-host.build-manifest.json'
        package_version = '1.0.0-fixture'; required_sibling_binaries = @('swarm-supervisor')
        package_manifest_sha256 = $checksumHex; executable_sha256 = $exeHash; build_manifest_sha256 = $manifestHash
        source_commit = $commitHex; source_tree = $treeHex; cargo_lock_sha256 = $cargoLockHash; cargo_toml_sha256 = $cargoTomlHash
        workspace_cargo_toml_sha256 = $cargoTomlHash; rust_toolchain_toml_sha256 = $toolchainHash
        resources = [ordered]@{ coordinate = [string]$spec.resource_coordinate; installed_relative_root = [string]$spec.resource_installed_relative_root; files = @($receiptRows); dependency_closure = $closurePin }
    }
    Write-Ascii (Join-Path $Root 'swarm-kernel-host.install-receipt.json') ((ConvertTo-Json -InputObject $receipt -Depth 14) + "`n")
    return [ordered]@{ root = $Root; manifest = $manifest }
}

try {
    $legacy = New-HostFixture (Join-Path $work 'legacy')
    Expect-Throw { Assert-ResourceManifest $legacy.manifest $spec 'Build manifest' } '*incomplete additional resource group set*' 'new-package gate still rejects group-less manifest'
    $gotPeer = Get-InstalledFrontend 'swarm-kernel-host' $legacy.root $spec 'swarm-mcp'
    Assert-True ($null -ne $gotPeer -and $gotPeer.package_name -ceq 'swarm-kernel-host') 'legacy host accepted, peer install not blocked'
    $gotSelf = Get-InstalledFrontend 'swarm-kernel-host' $legacy.root $spec 'swarm-kernel-host'
    Assert-True ($null -ne $gotSelf) 'legacy host accepted for idempotent reinstall inventory'
    $current = New-HostFixture (Join-Path $work 'current') -WithAntigravity
    $gotNew = Get-InstalledFrontend 'swarm-kernel-host' $current.root $spec 'swarm-mcp'
    $wantCoordinate = [string]@($spec.additional_resource_groups)[0].coordinate
    Assert-True ($null -ne $gotNew -and @($gotNew.resources.additional_groups).Count -eq 1 -and [string]$gotNew.resources.additional_groups[0].coordinate -ceq $wantCoordinate) 'current host Antigravity group verified'
    Remove-Item -LiteralPath (Join-Path $current.root 'resources/modules/antigravity/bridge-v4.mjs') -Force -Confirm:$false
    Expect-Throw { Get-InstalledFrontend 'swarm-kernel-host' $current.root $spec 'swarm-mcp' } '*incomplete*missing*' 'missing group file rejected'
    $tampered = New-HostFixture (Join-Path $work 'tampered') -WithAntigravity
    [IO.File]::AppendAllText((Join-Path $tampered.root 'resources/modules/antigravity/codec.mjs'), 'tampered')
    Expect-Throw { Get-InstalledFrontend 'swarm-kernel-host' $tampered.root $spec 'swarm-mcp' } '*differs from its pinned digest*' 'tampered group file rejected'
    $bad = New-HostFixture (Join-Path $work 'badver') -WithAntigravity
    $bad.manifest.resources.additional_groups[0].version = '5'
    Expect-Throw { Assert-ResourceManifest $bad.manifest $spec 'Build manifest' } '*differs from the package policy*' 'wrong group version rejected on new-package gate'
} finally {
    if (Test-Path -LiteralPath $work) { Remove-Item -LiteralPath $work -Recurse -Force -Confirm:$false }
}
if ($failures.Count -gt 0) { Write-Output ("FAILURES: $($failures.Count)"); exit 1 }
Write-Output 'ALL PASS'
