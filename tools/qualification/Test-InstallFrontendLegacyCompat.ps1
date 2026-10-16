#Requires -Version 7.0
# Focused installer regression: retired OpenCode server removal.
#
# Loads the REAL production functions from tools/modules/Install-SwarmFrontend.ps1
# via AST (function definitions only; the installer top-level flow never executes)
# and stages REAL fixtures on disk with REAL SHA-256 pins computed by the
# production Get-Sha256 / Get-DependencyTreeRows helpers.
#
# Proves the connected removal: a NEW swarm-kernel-host package carries only
# the exact Antigravity Node bridge group, passes the new-package gate, and
# inventories with NO node_modules / dependency-closure.json / serve.mjs on
# disk (no npm, no OpenCode server). Historical manifests and receipts that
# still pin the retired swarm-kernel-host-opencode-resources coordinate remain
# accepted as fenced read-only inventory (peer + idempotent reinstall), while
# the new-package gate refuses to stage or install them anew. Tampered or
# missing bridge files are still rejected. Tool-identity pins (node/npm) in
# legacy fixtures are synthetic but self-consistent; every file pin is a real
# hash of staged bytes. Nothing here launches npm, Node, Cargo, or a host.
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

# Retired ex-policy literals, pinned here for historical fixtures only. They
# must never come from live production policy again.
$retiredCoordinate = 'swarm-kernel-host-opencode-resources'
$retiredRepositoryRoot = 'modules/opencode'
$retiredInstalledRoot = 'resources/modules/opencode'
$retiredFiles = @(
    [ordered]@{ path = 'serve.mjs'; role = 'server_program' },
    [ordered]@{ path = 'native-mcp-proof.mjs'; role = 'plugin_module' },
    [ordered]@{ path = 'index.mjs'; role = 'plugin_entry' },
    [ordered]@{ path = 'package.json'; role = 'dependency_manifest' },
    [ordered]@{ path = 'package-lock.json'; role = 'dependency_lock' }
)
$retiredPolicy = [ordered]@{
    manager = 'npm'
    arguments = @('ci', '--ignore-scripts', '--no-audit', '--no-fund', '--no-progress', '--loglevel=error')
    output_directory = 'node_modules'
    closure_manifest_file = 'dependency-closure.json'
    max_files = 100000
    max_entries = 200000
    max_total_bytes = 1073741824
    max_manifest_bytes = 33554432
    timeout_seconds = 1800
}

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

function New-BridgeGroup([string]$Root) {
    $gspec = @($spec.additional_resource_groups)[0]
    $groot = Join-Path $Root ([string]$gspec.installed_relative_root)
    $grows = foreach ($entry in @($gspec.resource_files)) {
        $p = Join-Path $groot ([string]$entry.path)
        Write-Ascii $p ("agy::$($entry.path)::v1`n")
        $sha = Get-Sha256 $p
        [ordered]@{ path = [string]$entry.path; role = [string]$entry.role; file = ([string]$gspec.installed_relative_root + '/' + [string]$entry.path); bytes = [long](Get-Item -LiteralPath $p).Length; source_sha256 = $sha; artifact_sha256 = $sha }
    }
    return [ordered]@{ coordinate = [string]$gspec.coordinate; module_id = [string]$gspec.module_id; artifact_id = [string]$gspec.artifact_id; version = [string]$gspec.version; repository_relative_root = [string]$gspec.repository_relative_root; installed_relative_root = [string]$gspec.installed_relative_root; files = @($grows) }
}

function New-BaseManifest([string]$Root) {
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
            host_runtime = [ordered]@{ package_name = 'swarm-kernel-host'; manifest = 'crates/swarm-kernel-host/Cargo.toml'; binary_target = 'swarm-kernel-host'; role = 'host_runtime' }
            host_supervisor = [ordered]@{ package_name = 'swarm-supervisor'; manifest = 'crates/swarm-supervisor/Cargo.toml'; binary_target = 'swarm-supervisor'; role = 'host_supervisor' }
            required_sibling_binaries = @('swarm-supervisor'); process_role = 'host_runtime'
        }
        artifacts = @([ordered]@{ target_name = 'swarm-kernel-host'; file = 'bin/swarm-kernel-host.exe'; source_sha256 = $exeHash; artifact_sha256 = $exeHash; bytes = $exeBytes })
        toolchain = [ordered]@{ pinned_channel = '1.90.0'; rustc_version_verbose = 'rustc 1.90.0 (fixture)'; cargo_version_verbose = 'cargo 1.90.0 (fixture)' }
    }
    return [ordered]@{ manifest = $manifest; exeHash = $exeHash }
}

function Write-Receipt([string]$Root, [object]$Manifest, [object]$Resources, [string]$ManifestHash) {
    $receipt = [ordered]@{
        schema_version = 1; format = 'eliot.frontend_install_receipt.v1'; package_name = 'swarm-kernel-host'; role = 'host_runtime'
        binary_target = 'swarm-kernel-host'; binary_file = 'swarm-kernel-host.exe'; build_manifest_file = 'swarm-kernel-host.build-manifest.json'
        package_version = '1.0.0-fixture'; required_sibling_binaries = @('swarm-supervisor')
        package_manifest_sha256 = $checksumHex; executable_sha256 = $Manifest.artifacts[0].artifact_sha256; build_manifest_sha256 = $ManifestHash
        source_commit = $commitHex; source_tree = $treeHex; cargo_lock_sha256 = $cargoLockHash; cargo_toml_sha256 = $cargoTomlHash
        workspace_cargo_toml_sha256 = $cargoTomlHash; rust_toolchain_toml_sha256 = $toolchainHash
        resources = $Resources
    }
    Write-Ascii (Join-Path $Root 'swarm-kernel-host.install-receipt.json') ((ConvertTo-Json -InputObject $receipt -Depth 14) + "`n")
}

function New-HostFixture([string]$Root) {
    $base = New-BaseManifest $Root
    $group = New-BridgeGroup $Root
    # NEW shape: Antigravity group only. No OpenCode coordinate, no files, no
    # dependency policy, no node_modules, no dependency closure on disk.
    $base.manifest.resources = [ordered]@{ schema_version = 1; additional_groups = @($group) }
    Write-Ascii (Join-Path $Root 'swarm-kernel-host.build-manifest.json') ((ConvertTo-Json -InputObject $base.manifest -Depth 14) + "`n")
    $manifestHash = Get-Sha256 (Join-Path $Root 'swarm-kernel-host.build-manifest.json')
    $receiptGroups = @([ordered]@{
        coordinate = [string]$group.coordinate; module_id = [string]$group.module_id; artifact_id = [string]$group.artifact_id; version = [string]$group.version
        repository_relative_root = [string]$group.repository_relative_root; installed_relative_root = [string]$group.installed_relative_root
        files = @(foreach ($r in @($group.files)) { [ordered]@{ path = $r.path; file = $r.file; artifact_sha256 = $r.artifact_sha256 } })
    })
    $receiptResources = [ordered]@{ additional_groups = @($receiptGroups) }
    Write-Receipt $Root $base.manifest $receiptResources $manifestHash
    return [ordered]@{ root = $Root; manifest = $base.manifest }
}

function New-LegacyHostFixture([string]$Root, [switch]$WithAntigravity) {
    $base = New-BaseManifest $Root
    $kernelRoot = Join-Path $Root $retiredInstalledRoot
    $kernelRows = foreach ($entry in $retiredFiles) {
        $p = Join-Path $kernelRoot ([string]$entry.path)
        Write-Ascii $p ("kernel::$($entry.path)::v1`n")
        $sha = Get-Sha256 $p
        [ordered]@{ path = [string]$entry.path; role = [string]$entry.role; file = ($retiredInstalledRoot + '/' + [string]$entry.path); bytes = [long](Get-Item -LiteralPath $p).Length; source_sha256 = $sha; artifact_sha256 = $sha }
    }
    $nmRoot = Join-Path $kernelRoot ([string]$retiredPolicy.output_directory)
    Write-Ascii (Join-Path $nmRoot 'pkg/index.js') "'use strict';`n"
    $tree = Get-DependencyTreeRows $nmRoot $retiredPolicy
    $treeHash = Get-DependencyTreeSha256 @($tree.files)
    $pkgJsonHash = Get-Sha256 (Join-Path $kernelRoot 'package.json')
    $pkgLockHash = Get-Sha256 (Join-Path $kernelRoot 'package-lock.json')
    $closureManifest = [ordered]@{
        schema_version = 1; format = 'eliot.opencode_dependency_closure.v1'; coordinate = $retiredCoordinate
        package_json_sha256 = $pkgJsonHash; package_lock_sha256 = $pkgLockHash
        node = [ordered]@{ file = 'node.exe'; version = 'v22.0.0-test'; executable_sha256 = $nodeExeHash }
        npm = [ordered]@{ file = 'npm-cli.js'; version = '10.0.0-test'; cli_sha256 = $npmCliHash }
        tree_sha256 = $treeHash; entry_count = [long]$tree.entry_count; file_count = [long]$tree.files.Count; total_bytes = [long]$tree.total_bytes
        files = @($tree.files)
    }
    Write-Ascii (Join-Path $kernelRoot ([string]$retiredPolicy.closure_manifest_file)) ((ConvertTo-Json -InputObject $closureManifest -Depth 10 -Compress) + "`n")
    $closurePin = [ordered]@{
        schema_version = 1; manifest_file = [string]$retiredPolicy.closure_manifest_file; manifest_sha256 = (Get-Sha256 (Join-Path $kernelRoot ([string]$retiredPolicy.closure_manifest_file)))
        coordinate = $retiredCoordinate; package_json_sha256 = $pkgJsonHash; package_lock_sha256 = $pkgLockHash; tree_sha256 = $treeHash
        node_version = 'v22.0.0-test'; node_executable_sha256 = $nodeExeHash; node_file = 'node.exe'
        npm_version = '10.0.0-test'; npm_cli_sha256 = $npmCliHash; npm_cli_file = 'npm-cli.js'
        entry_count = [long]$tree.entry_count; file_count = [long]$tree.files.Count; total_bytes = [long]$tree.total_bytes
    }
    $groupObjs = @()
    if ($WithAntigravity) {
        $groupObjs = @(New-BridgeGroup $Root)
    }
    $depPolicy = [ordered]@{
        node_modules = 'installer_generated_locked_closure'; manager = [string]$retiredPolicy.manager; install_command = 'npm ci --ignore-scripts'
        arguments = @(@($retiredPolicy.arguments) | ForEach-Object { [string]$_ })
        output_directory = [string]$retiredPolicy.output_directory; closure_manifest_file = [string]$retiredPolicy.closure_manifest_file
        max_files = [long]$retiredPolicy.max_files; max_entries = [long]$retiredPolicy.max_entries; max_total_bytes = [long]$retiredPolicy.max_total_bytes
        max_manifest_bytes = [long]$retiredPolicy.max_manifest_bytes; timeout_seconds = [long]$retiredPolicy.timeout_seconds
    }
    $base.manifest.resources = [ordered]@{ schema_version = 1; coordinate = $retiredCoordinate; repository_relative_root = $retiredRepositoryRoot; installed_relative_root = $retiredInstalledRoot; files = @($kernelRows); dependency_policy = $depPolicy }
    if ($WithAntigravity) { $base.manifest.resources.additional_groups = $groupObjs }
    # Historical compatibility record still pins the retired server requirement.
    $base.manifest.compatibility.host_runtime = [ordered]@{ package_name = 'swarm-kernel-host'; manifest = 'crates/swarm-kernel-host/Cargo.toml'; binary_target = 'swarm-kernel-host'; role = 'host_runtime'; resource_coordinate = $retiredCoordinate; resource_installed_relative_root = $retiredInstalledRoot }
    Write-Ascii (Join-Path $Root 'swarm-kernel-host.build-manifest.json') ((ConvertTo-Json -InputObject $base.manifest -Depth 14) + "`n")
    $manifestHash = Get-Sha256 (Join-Path $Root 'swarm-kernel-host.build-manifest.json')
    $receiptRows = foreach ($r in $kernelRows) { [ordered]@{ path = $r.path; file = $r.file; artifact_sha256 = $r.artifact_sha256 } }
    $receiptResources = [ordered]@{ coordinate = $retiredCoordinate; installed_relative_root = $retiredInstalledRoot; files = @($receiptRows); dependency_closure = $closurePin }
    Write-Receipt $Root $base.manifest $receiptResources $manifestHash
    return [ordered]@{ root = $Root; manifest = $base.manifest }
}

try {
    # 1. New production spec carries no server/npm requirement, only the bridge.
    $noServerFields = -not $spec.Contains('resource_coordinate') -and -not $spec.Contains('resource_installed_relative_root') -and -not $spec.Contains('resource_repository_relative_root') -and -not $spec.Contains('resource_files') -and -not $spec.Contains('resource_dependency_install')
    Assert-True $noServerFields 'new spec carries no OpenCode server/npm requirement'
    $bridgeGroups = @($spec.additional_resource_groups)
    Assert-True ($bridgeGroups.Count -eq 1 -and [string]$bridgeGroups[0].coordinate -ceq 'swarm-kernel-host-antigravity-node-bridge') 'new spec still pins the exact Antigravity bridge group'

    # 2. New-package gate accepts the Antigravity-only manifest.
    $new = New-HostFixture (Join-Path $work 'new')
    $gated = Assert-ResourceManifest $new.manifest $spec 'Build manifest'
    Assert-True ($null -ne $gated -and @($gated.additional_groups).Count -eq 1 -and -not $gated.Contains('coordinate') -and -not $gated.Contains('dependency_policy')) 'new-package gate accepts Antigravity-only manifest'
    $newCompat = Get-HostRuntimeCoordinate $new.manifest 'Build manifest'
    Assert-True ((-not $newCompat.Contains('resource_coordinate')) -and $newCompat.role -ceq 'host_runtime') 'new host runtime coordinate needs no server resources'

    # 3. New inventory needs no npm/OpenCode server and keeps receipt readback.
    $gotNew = Get-InstalledFrontend 'swarm-kernel-host' $new.root $spec 'swarm-mcp'
    $wantCoordinate = [string]@($spec.additional_resource_groups)[0].coordinate
    Assert-True ($null -ne $gotNew -and @($gotNew.resources.additional_groups).Count -eq 1 -and [string]$gotNew.resources.additional_groups[0].coordinate -ceq $wantCoordinate) 'new host inventories with its Antigravity group'
    $serverRootGone = -not (Test-Path -LiteralPath (Join-Path $new.root $retiredInstalledRoot))
    $nodeModulesGone = -not (Test-Path -LiteralPath (Join-Path $new.root (Join-Path $retiredInstalledRoot 'node_modules')))
    $closureGone = -not (Test-Path -LiteralPath (Join-Path $new.root (Join-Path $retiredInstalledRoot 'dependency-closure.json')))
    $serveGone = -not (Test-Path -LiteralPath (Join-Path $new.root (Join-Path $retiredInstalledRoot 'serve.mjs')))
    Assert-True ($serverRootGone -and $nodeModulesGone -and $closureGone -and $serveGone) 'new install needs no npm/OpenCode server files on disk'
    $newReceipt = Read-JsonMap (Join-Path $new.root 'swarm-kernel-host.install-receipt.json') 'New install receipt'
    Assert-True ((-not $newReceipt.resources.Contains('dependency_closure')) -and @($newReceipt.resources.additional_groups).Count -eq 1) 'new receipt pins the bridge group without a dependency closure'
    Assert-ReceiptResourceContract $newReceipt $new.manifest.resources 'New install receipt'
    Write-Output 'PASS: new receipt readback matches its build manifest'

    # 4. New-package gate refuses the retired server manifest anew.
    $legacy = New-LegacyHostFixture (Join-Path $work 'legacy') -WithAntigravity
    Expect-Throw { Assert-ResourceManifest $legacy.manifest $spec 'Build manifest' } '*retired*' 'new-package gate rejects retired server manifest'
    Expect-Throw { Get-HostRuntimeCoordinate $legacy.manifest 'Build manifest' } '*retired*' 'new compatibility gate rejects retired server requirement'

    # 5. Retained receipt readback: grouped legacy inventory stays valid/fenced.
    $gotPeer = Get-InstalledFrontend 'swarm-kernel-host' $legacy.root $spec 'swarm-mcp'
    Assert-True ($null -ne $gotPeer -and $gotPeer.package_name -ceq 'swarm-kernel-host') 'grouped legacy host accepted, peer install not blocked'
    $gotSelf = Get-InstalledFrontend 'swarm-kernel-host' $legacy.root $spec 'swarm-kernel-host'
    Assert-True ($null -ne $gotSelf) 'grouped legacy host accepted for idempotent reinstall inventory'
    $legacyRuntime = Get-HostRuntimeCoordinate $legacy.manifest 'Installed build manifest' -AllowLegacyServerResources
    Assert-True ([string]$legacyRuntime.resource_coordinate -ceq $retiredCoordinate) 'grouped legacy runtime coordinate still reads back fenced'

    # 6. Oldest legacy (no bridge group) inventory stays accepted, never upgraded.
    $oldest = New-LegacyHostFixture (Join-Path $work 'oldest')
    Expect-Throw { Assert-ResourceManifest $oldest.manifest $spec 'Build manifest' } '*retired*' 'new-package gate rejects group-less legacy manifest too'
    $gotOldest = Get-InstalledFrontend 'swarm-kernel-host' $oldest.root $spec 'swarm-mcp'
    Assert-True ($null -ne $gotOldest) 'group-less legacy host accepted as read-only inventory'

    # 7-8. Bridge custody still fenced on new installs.
    $current = New-HostFixture (Join-Path $work 'current')
    Remove-Item -LiteralPath (Join-Path $current.root 'resources/modules/antigravity/bridge-v4.mjs') -Force -Confirm:$false
    Expect-Throw { Get-InstalledFrontend 'swarm-kernel-host' $current.root $spec 'swarm-mcp' } '*incomplete*missing*' 'missing bridge file rejected'
    $tampered = New-HostFixture (Join-Path $work 'tampered')
    [IO.File]::AppendAllText((Join-Path $tampered.root 'resources/modules/antigravity/codec.mjs'), 'tampered')
    Expect-Throw { Get-InstalledFrontend 'swarm-kernel-host' $tampered.root $spec 'swarm-mcp' } '*differs from its pinned digest*' 'tampered bridge file rejected'

    # 9. Wrong bridge version still rejected on the new-package gate.
    $bad = New-HostFixture (Join-Path $work 'badver')
    $bad.manifest.resources.additional_groups[0].version = '5'
    Expect-Throw { Assert-ResourceManifest $bad.manifest $spec 'Build manifest' } '*differs from the package policy*' 'wrong bridge version rejected on new-package gate'

    # 10. Mixed receipt/manifest shapes never validate.
    Expect-Throw { Assert-ReceiptResourceContract $newReceipt $legacy.manifest.resources 'Mixed new receipt vs legacy manifest' } '*does not match*' 'new receipt vs legacy manifest rejected'
    $legacyReceipt = Read-JsonMap (Join-Path $legacy.root 'swarm-kernel-host.install-receipt.json') 'Legacy install receipt'
    Expect-Throw { Assert-ReceiptResourceContract $legacyReceipt $new.manifest.resources 'Mixed legacy receipt vs new manifest' } '*does not match*' 'legacy receipt vs new manifest rejected'

    # 11. Absent vs orphaned installs unchanged.
    $emptyRoot = Join-Path $work 'empty'
    [void][IO.Directory]::CreateDirectory($emptyRoot)
    Assert-True ($null -eq (Get-InstalledFrontend 'swarm-kernel-host' $emptyRoot $spec 'swarm-mcp')) 'empty root still inventories as absent'
    $orphanRoot = Join-Path $work 'orphan'
    Write-Ascii (Join-Path $orphanRoot 'resources/modules/antigravity/bridge.mjs') 'orphan'
    Expect-Throw { Get-InstalledFrontend 'swarm-kernel-host' $orphanRoot $spec 'swarm-mcp' } '*orphaned resource files*' 'orphaned bridge files without identity rejected'
} finally {
    $cleanupPath = [IO.Path]::GetFullPath($work)
    $tempPrefix = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
    if (-not $cleanupPath.StartsWith($tempPrefix, [StringComparison]::OrdinalIgnoreCase) -or
        [IO.Path]::GetFileName($cleanupPath) -notmatch '^legacy-compat-[0-9a-f]{32}$') {
        throw 'Fixture cleanup target escaped its generated temporary directory.'
    }
    if (Test-Path -LiteralPath $cleanupPath) {
        $cleanupItem = Get-Item -LiteralPath $cleanupPath -Force
        if (($cleanupItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Fixture cleanup target became a reparse point.'
        }
        Remove-Item -LiteralPath $cleanupPath -Recurse -Force -Confirm:$false
    }
}
if ($failures.Count -gt 0) { Write-Output ("FAILURES: $($failures.Count)"); exit 1 }
Write-Output 'ALL PASS'
