[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Classify', 'Resolve', 'ValidateDocs', 'ValidateTooling', 'Verify', 'FullRust')]
    [string] $Stage,
    [string] $BaseSha,
    [string] $HeadSha
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$script:PathComparison = if ($IsWindows) {
    [StringComparison]::OrdinalIgnoreCase
} else {
    [StringComparison]::Ordinal
}

function Write-Result([string] $Name, [string] $Value) {
    if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_OUTPUT)) {
        $line = "$Name=$Value`n"
        [IO.File]::AppendAllText(
            $env:GITHUB_OUTPUT,
            $line,
            [Text.UTF8Encoding]::new($false)
        )
    } else {
        Write-Output "$Name=$Value"
    }
}

function Write-JsonResult([string] $Name, [object[]] $Value) {
    $items = @($Value)
    $json = if ($items.Count -eq 0 -or ($items.Count -eq 1 -and $null -eq $items[0])) {
        '[]'
    } else {
        ConvertTo-Json -InputObject $items -Depth 8 -Compress
    }
    Write-Result $Name $json
}

function Resolve-Commit([string] $Value, [string] $Label) {
    if ($Value -notmatch '^[0-9a-fA-F]{40,64}$') {
        throw "$Label must be a full commit SHA; refusing an ambiguous Git range."
    }
    $resolved = (& git -C $script:RepoRoot rev-parse --verify "$($Value)^{commit}" | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($resolved)) {
        throw "Could not resolve $Label commit SHA."
    }
    return $resolved
}

function Get-ChangedPaths([string] $Base, [string] $Head) {
    $baseCommit = Resolve-Commit $Base 'Base'
    $headCommit = Resolve-Commit $Head 'Head'
    $checkoutCommit = (& git -C $script:RepoRoot rev-parse --verify 'HEAD^{commit}' | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $checkoutCommit -ne $headCommit) {
        throw 'HeadSha must match the checked-out commit so metadata and changed paths describe the same source.'
    }
    $raw = @(& git -C $script:RepoRoot diff --no-renames --name-only $baseCommit $headCommit --)
    if ($LASTEXITCODE -ne 0) {
        throw 'git diff could not classify the requested commits.'
    }
    $normalized = @(
        foreach ($path in $raw) {
            if (-not [string]::IsNullOrWhiteSpace($path)) {
                $path.TrimEnd("`r", "`n").Replace('\', '/')
            }
        }
    )
    return [pscustomobject]@{
        Base = $baseCommit
        Head = $headCommit
        Paths = @($normalized | Sort-Object -Unique)
    }
}

function Get-Classification([string[]] $Paths) {
    $docsChanged = @($Paths | Where-Object { $_ -match '(^docs/|\.md$)' }).Count -gt 0
    $productPaths = @($Paths | Where-Object { $_ -notmatch '\.md$' })
    $docsOnly = $Paths.Count -gt 0 -and $productPaths.Count -eq 0

    $rustChanged = @($Paths | Where-Object {
        $_ -match '\.rs$' -or
        $_ -match '(^|/)Cargo\.(toml|lock)$' -or
        $_ -match '(^|/)rust-toolchain(\.toml)?$' -or
        $_ -match '^\.cargo/' -or
        $_ -match '^migrations/.*\.(sql|toml)$'
    }).Count -gt 0
    $testsChanged = @($productPaths | Where-Object { $_ -match '(^|/)tests/' }).Count -gt 0
    $toolingChanged = @($Paths | Where-Object {
        $_ -eq 'Justfile' -or
        $_ -match '^tools/' -or
        $_ -match '^\.github/(workflows|actions)/' -or
        $_ -match '(^|/)build\.rs$' -or
        $_ -match '^modules/[^/]+/(package\.json|package-lock\.json|npm-shrinkwrap\.json)$'
    }).Count -gt 0
    $moduleJs = @($Paths | Where-Object {
        $_ -match '^modules/.+\.(mjs|cjs|js)$'
    } | Where-Object {
        Test-Path -LiteralPath (Get-FullPath $_) -PathType Leaf
    })
    $atlasChanged = @($Paths | Where-Object { $_ -match '^vendor/atlas/' }).Count -gt 0
    $globalCargoChange = @($Paths | Where-Object {
        $_ -match '^Cargo\.(toml|lock)$' -or
        $_ -match '^\.cargo/' -or
        $_ -match '^rust-toolchain(\.toml)?$'
    }).Count -gt 0
    $productChanged = @($productPaths | Where-Object {
        $_ -match '\.rs$' -or
        $_ -match '(^|/)Cargo\.(toml|lock)$' -or
        $_ -match '(^|/)rust-toolchain(\.toml)?$' -or
        $_ -match '^\.cargo/' -or
        $_ -match '^(src|tests|migrations|modules|vendor|config)/'
    }).Count -gt 0

    return [pscustomobject]@{
        DocsChanged = [bool]$docsChanged
        DocsOnly = [bool]$docsOnly
        RustChanged = [bool]$rustChanged
        MetadataNeeded = [bool]($rustChanged -or $testsChanged)
        TestsChanged = [bool]$testsChanged
        ToolingChanged = [bool]$toolingChanged
        ModuleJs = @($moduleJs)
        AtlasChanged = [bool]$atlasChanged
        GlobalCargoChange = [bool]$globalCargoChange
        ProductChanged = [bool]$productChanged
    }
}

function Publish-Classification($Changes, $Classification) {
    Write-Result 'docs_changed' ([string]$Classification.DocsChanged).ToLowerInvariant()
    Write-Result 'docs_only' ([string]$Classification.DocsOnly).ToLowerInvariant()
    Write-Result 'rust_changed' ([string]$Classification.RustChanged).ToLowerInvariant()
    Write-Result 'metadata_needed' ([string]$Classification.MetadataNeeded).ToLowerInvariant()
    Write-Result 'tests_changed' ([string]$Classification.TestsChanged).ToLowerInvariant()
    Write-Result 'tooling_changed' ([string]$Classification.ToolingChanged).ToLowerInvariant()
    Write-Result 'atlas_changed' ([string]$Classification.AtlasChanged).ToLowerInvariant()
    Write-Result 'product_changed' ([string]$Classification.ProductChanged).ToLowerInvariant()
    Write-JsonResult 'changed_paths' $Changes.Paths
    Write-JsonResult 'module_js_files' $Classification.ModuleJs
}

function Get-ChangeContext {
    if ([string]::IsNullOrWhiteSpace($BaseSha)) {
        if ($Stage -eq 'Classify') {
            # A manual full workflow has no diff range; preserve its explicit full gate.
            $empty = [pscustomobject]@{ Base = ''; Head = ''; Paths = @() }
            $classification = [pscustomobject]@{
                DocsChanged = $false; DocsOnly = $false; RustChanged = $false
                MetadataNeeded = $false
                TestsChanged = $false; ToolingChanged = $false; ModuleJs = @()
                AtlasChanged = $false; GlobalCargoChange = $false; ProductChanged = $true
            }
            return [pscustomobject]@{ Changes = $empty; Classification = $classification }
        }
        throw 'BaseSha is required for this stage.'
    }
    if ([string]::IsNullOrWhiteSpace($HeadSha)) {
        throw 'HeadSha is required for this stage.'
    }
    $changes = Get-ChangedPaths $BaseSha $HeadSha
    $classification = Get-Classification $changes.Paths
    return [pscustomobject]@{ Changes = $changes; Classification = $classification }
}

function Get-FullPath([string] $Path) {
    if ([IO.Path]::IsPathRooted($Path)) {
        return [IO.Path]::GetFullPath($Path).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
    }
    return [IO.Path]::GetFullPath((Join-Path $script:RepoRoot $Path)).TrimEnd([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
}

function Test-PathWithin([string] $Path, [string] $Directory) {
    $fullPath = Get-FullPath $Path
    $fullDirectory = Get-FullPath $Directory
    $relative = [IO.Path]::GetRelativePath($fullDirectory, $fullPath)
    if ($relative -eq '.') { return $true }
    if ([IO.Path]::IsPathRooted($relative) -or $relative -eq '..') { return $false }
    return -not $relative.StartsWith("..$([IO.Path]::DirectorySeparatorChar)", $script:PathComparison)
}

function Get-LocalPackageGraph {
    Push-Location $script:RepoRoot
    try {
        $metadataText = (& cargo metadata --locked --no-deps --format-version 1 | Out-String)
        $metadataExit = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    if ($metadataExit -ne 0) {
        throw 'cargo metadata --locked --no-deps failed; package scope cannot be guessed.'
    }
    try {
        $metadata = $metadataText | ConvertFrom-Json
    } catch {
        throw 'Cargo metadata output was not valid JSON; package scope cannot be guessed.'
    }

    $memberIds = @($metadata.workspace_members)
    $vendorRoot = Get-FullPath 'vendor'
    $entries = @(
        foreach ($package in @($metadata.packages)) {
            if ($memberIds -contains [string]$package.id) {
                $manifest = Get-FullPath ([string]$package.manifest_path)
                $vendorTargetCount = @($package.targets | Where-Object {
                    Test-PathWithin (Get-FullPath ([string]$_.src_path)) $vendorRoot
                }).Count
                [pscustomobject]@{
                    Id = [string]$package.id
                    Name = [string]$package.name
                    Version = [string]$package.version
                    Spec = "$($package.name)@$($package.version)"
                    Manifest = $manifest
                    Directory = [IO.Path]::GetDirectoryName($manifest)
                    VendorTargetCount = $vendorTargetCount
                    OwnedTargetCount = @($package.targets).Count - $vendorTargetCount
                    Package = $package
                }
            }
        }
    )
    if ($entries.Count -eq 0) {
        throw 'Cargo metadata reported no workspace packages; refusing a guessed scope.'
    }

    $entriesById = @{}
    foreach ($entry in $entries) { $entriesById[$entry.Id] = $entry }
    $reverseDependents = @{}
    $externalRoots = [Collections.Generic.List[object]]::new()

    foreach ($entry in $entries) {
        foreach ($dependency in @($entry.Package.dependencies)) {
            $targetKey = $null
            $dependencyPath = $dependency.PSObject.Properties['path']
            if ($null -ne $dependencyPath -and -not [string]::IsNullOrWhiteSpace([string]$dependencyPath.Value)) {
                $rawPath = [string]$dependencyPath.Value
                if ([IO.Path]::GetFileName($rawPath) -eq 'Cargo.toml') {
                    if ([IO.Path]::IsPathRooted($rawPath)) {
                        $manifestCandidate = $rawPath
                    } else {
                        $manifestCandidate = Join-Path $entry.Directory $rawPath
                    }
                    $dependencyManifest = Get-FullPath $manifestCandidate
                    $dependencyRoot = [IO.Path]::GetDirectoryName($dependencyManifest)
                } else {
                    if ([IO.Path]::IsPathRooted($rawPath)) {
                        $rootCandidate = $rawPath
                    } else {
                        $rootCandidate = Join-Path $entry.Directory $rawPath
                    }
                    $dependencyRoot = Get-FullPath $rootCandidate
                    $dependencyManifest = Get-FullPath (Join-Path $dependencyRoot 'Cargo.toml')
                }
                $workspaceTarget = @($entries | Where-Object {
                    [string]::Equals($_.Manifest, $dependencyManifest, $script:PathComparison)
                })
                if ($workspaceTarget.Count -eq 1) {
                    $targetKey = $workspaceTarget[0].Id
                } elseif ($workspaceTarget.Count -gt 1) {
                    throw "Path dependency resolves to multiple workspace packages: $dependencyManifest"
                } else {
                    $targetKey = "external:$dependencyRoot"
                    if (-not @($externalRoots | Where-Object { $_.Key -eq $targetKey }).Count) {
                        $externalRoots.Add([pscustomobject]@{ Key = $targetKey; Directory = $dependencyRoot })
                    }
                }
            } elseif ([string]::IsNullOrWhiteSpace([string]$dependency.source)) {
                # Some Cargo metadata versions omit `path` for a workspace edge.
                # A unique source-less workspace package name is still an actual edge.
                $localTargets = @($entries | Where-Object { $_.Name -eq [string]$dependency.name })
                if ($localTargets.Count -eq 1) {
                    $targetKey = $localTargets[0].Id
                } elseif ($localTargets.Count -gt 1) {
                    throw "Ambiguous source-less dependency '$($dependency.name)'; exact package graph unavailable."
                } else {
                    throw "Cargo metadata omitted the path for local dependency '$($dependency.name)'; exact package graph unavailable."
                }
            }

            if ($null -ne $targetKey) {
                if (-not $reverseDependents.ContainsKey($targetKey)) {
                    $reverseDependents[$targetKey] = [Collections.Generic.List[string]]::new()
                }
                $reverseDependents[$targetKey].Add($entry.Id)
            }
        }
    }

    return [pscustomobject]@{
        Entries = @($entries)
        EntriesById = $entriesById
        ReverseDependents = $reverseDependents
        ExternalRoots = @($externalRoots)
    }
}

function Resolve-PackageScope($Changes, $Classification) {
    $graph = Get-LocalPackageGraph
    $seeds = [Collections.Generic.HashSet[string]]::new()
    $testPackageIds = [Collections.Generic.HashSet[string]]::new()

    if ($Classification.GlobalCargoChange) {
        foreach ($entry in $graph.Entries) { [void]$seeds.Add($entry.Id) }
    } else {
        if ($Classification.RustChanged) {
            $rustPaths = @($Changes.Paths | Where-Object {
                $_ -match '\.rs$' -or
                $_ -match '(^|/)Cargo\.toml$' -or
                $_ -match '^migrations/.*\.(sql|toml)$'
            })
            foreach ($relativePath in $rustPaths) {
                $absolutePath = Get-FullPath $relativePath
                $workspaceMatches = @($graph.Entries | Where-Object {
                    Test-PathWithin $absolutePath $_.Directory
                } | Sort-Object { $_.Directory.Length } -Descending)
                if ($workspaceMatches.Count -gt 0) {
                    [void]$seeds.Add($workspaceMatches[0].Id)
                    continue
                }
                $externalMatches = @($graph.ExternalRoots | Where-Object {
                    Test-PathWithin $absolutePath $_.Directory
                } | Sort-Object { $_.Directory.Length } -Descending)
                if ($externalMatches.Count -gt 0) {
                    [void]$seeds.Add($externalMatches[0].Key)
                    continue
                }
                throw "Rust source path '$relativePath' is outside metadata package roots; refusing a guessed scope."
            }
        }

    }

    # Test assets can change without a Rust source diff. Resolve their owning
    # workspace package from metadata so the exact package integration targets
    # still run; never silently skip fixture-only tests/** changes. This also
    # runs for workspace-wide Cargo changes so changed test targets are retained.
    foreach ($relativePath in @($Changes.Paths | Where-Object {
        $_ -match '(^|/)tests/' -and $_ -notmatch '\.md$'
    })) {
        $absolutePath = Get-FullPath $relativePath
        $workspaceMatches = @($graph.Entries | Where-Object {
            Test-PathWithin $absolutePath $_.Directory
        } | Sort-Object { $_.Directory.Length } -Descending)
        if ($workspaceMatches.Count -eq 0) {
            throw "Test path '$relativePath' is outside metadata workspace packages; refusing a guessed scope."
        }
        $testPackageId = [string]$workspaceMatches[0].Id
        [void]$testPackageIds.Add($testPackageId)
        [void]$seeds.Add($testPackageId)
    }
    if ($seeds.Count -eq 0) {
        throw 'Rust-relevant changes produced no package seed; refusing a guessed scope.'
    }

    $selectedIds = [Collections.Generic.HashSet[string]]::new()
    $visited = [Collections.Generic.HashSet[string]]::new()
    $queue = [Collections.Generic.Queue[string]]::new()
    foreach ($seed in $seeds) { $queue.Enqueue($seed) }
    while ($queue.Count -gt 0) {
        $current = $queue.Dequeue()
        if (-not $visited.Add($current)) { continue }
        if ($graph.EntriesById.ContainsKey($current)) { [void]$selectedIds.Add($current) }
        if ($graph.ReverseDependents.ContainsKey($current)) {
            foreach ($dependent in $graph.ReverseDependents[$current]) {
                [void]$selectedIds.Add($dependent)
                $queue.Enqueue($dependent)
            }
        }
    }

    $selectedEntries = @($graph.Entries | Where-Object { $selectedIds.Contains($_.Id) })
    $mixedTargets = @($selectedEntries | Where-Object { $_.VendorTargetCount -gt 0 -and $_.OwnedTargetCount -gt 0 })
    if ($mixedTargets.Count -gt 0) {
        $names = @($mixedTargets | ForEach-Object { $_.Spec }) -join ', '
        throw "Selected workspace packages mix owned and vendor-backed Rust targets ($names); exact target-level handling is required."
    }
    $ownedSelectedEntries = @($selectedEntries | Where-Object { $_.VendorTargetCount -eq 0 })
    $fmtSpecs = @()
    $clippySpecs = @()
    if ($Classification.RustChanged) {
        $duplicateFormatNames = @($ownedSelectedEntries | Group-Object Name | Where-Object { $_.Count -gt 1 })
        if ($duplicateFormatNames.Count -gt 0) {
            throw 'Selected format packages have duplicate names; cargo fmt requires an unambiguous package selector.'
        }
        $fmtSpecs = @($ownedSelectedEntries | Sort-Object Name, Version | ForEach-Object { $_.Name })
        $clippySpecs = @(
            foreach ($entry in $ownedSelectedEntries | Sort-Object Name, Version) {
                $hasRustCheckTarget = $false
                foreach ($target in @($entry.Package.targets)) {
                    if (@($target.kind) -contains 'lib' -or @($target.kind) -contains 'bin') {
                        $hasRustCheckTarget = $true
                        break
                    }
                }
                if ($hasRustCheckTarget) {
                    $entry.Spec
                }
            }
        )
    }

    $testTargetMap = @{}
    foreach ($entry in $selectedEntries) {
        if (-not $testPackageIds.Contains($entry.Id)) { continue }
        foreach ($target in @($entry.Package.targets)) {
            if ($target.test -eq $true -and @($target.kind) -contains 'test') {
                if (Test-PathWithin (Get-FullPath ([string]$target.src_path)) (Get-FullPath 'vendor')) { continue }
                $key = "$($entry.Spec)|$($target.name)"
                $testTargetMap[$key] = [pscustomobject]@{
                    package = $entry.Spec
                    target = [string]$target.name
                }
            }
        }
        if (-not @($testTargetMap.Values | Where-Object { $_.package -eq $entry.Spec }).Count) {
            throw "Test files changed for '$($entry.Spec)' but Cargo metadata has no integration-test target; refusing to skip tests."
        }
    }
    $testTargets = @($testTargetMap.Values | Sort-Object package, target)

    return [pscustomobject]@{
        FormatPackages = @($fmtSpecs | Sort-Object -Unique)
        ClippyPackages = @($clippySpecs | Sort-Object -Unique)
        TestTargets = @($testTargets)
        WorkspaceWide = [bool]$Classification.GlobalCargoChange
    }
}

function Invoke-ScopedCargoChecks($Scope) {
    if ($Scope.FormatPackages.Count -gt 0) {
        $formatArgs = @('fmt', '--check')
        foreach ($package in $Scope.FormatPackages) { $formatArgs += @('--package', $package) }
        $formatArgs += '--'
        Write-Host "cargo $($formatArgs -join ' ')"
        & cargo @formatArgs
        if ($LASTEXITCODE -ne 0) { throw "cargo fmt failed with exit code $LASTEXITCODE." }
    }
    if ($Scope.ClippyPackages.Count -gt 0) {
        $clippyArgs = @('clippy', '--locked', '--lib', '--bins', '--no-deps')
        foreach ($package in $Scope.ClippyPackages) { $clippyArgs += @('--package', $package) }
        $clippyArgs += @('--', '-D', 'warnings')
        Write-Host "cargo $($clippyArgs -join ' ')"
        & cargo @clippyArgs
        if ($LASTEXITCODE -ne 0) { throw "cargo clippy failed with exit code $LASTEXITCODE." }
    }
}

function Invoke-FullOwnedRustChecks {
    $graph = Get-LocalPackageGraph
    $mixedTargets = @($graph.Entries | Where-Object { $_.VendorTargetCount -gt 0 -and $_.OwnedTargetCount -gt 0 })
    if ($mixedTargets.Count -gt 0) {
        $names = @($mixedTargets | ForEach-Object { $_.Spec }) -join ', '
        throw "Full qualification found packages mixing owned and vendor-backed targets ($names); exact target-level handling is required."
    }
    $owned = @($graph.Entries | Where-Object { $_.VendorTargetCount -eq 0 } | Sort-Object Name, Version)
    if ($owned.Count -eq 0) {
        throw 'Cargo metadata reported no owned workspace packages; refusing a guessed full scope.'
    }
    $duplicateFormatNames = @($owned | Group-Object Name | Where-Object { $_.Count -gt 1 })
    if ($duplicateFormatNames.Count -gt 0) {
        throw 'Owned workspace packages have duplicate names; cargo fmt requires an unambiguous package selector.'
    }
    $formatNames = @($owned | ForEach-Object { $_.Name })
    $clippySpecs = @(
        foreach ($entry in $owned) {
            if (@($entry.Package.targets | Where-Object {
                @($_.kind) -contains 'lib' -or @($_.kind) -contains 'bin'
            }).Count -gt 0) {
                $entry.Spec
            }
        }
    )
    $testSpecs = @($owned | ForEach-Object { $_.Spec })

    $formatArgs = @('fmt', '--check')
    foreach ($package in $formatNames) { $formatArgs += @('--package', $package) }
    $formatArgs += '--'
    Write-Host "cargo $($formatArgs -join ' ')"
    & cargo @formatArgs
    if ($LASTEXITCODE -ne 0) { throw "Full owned-package cargo fmt failed with exit code $LASTEXITCODE." }

    if ($clippySpecs.Count -gt 0) {
        $clippyArgs = @('clippy', '--locked', '--lib', '--bins', '--no-deps')
        foreach ($package in $clippySpecs) { $clippyArgs += @('--package', $package) }
        $clippyArgs += @('--', '-D', 'warnings')
        Write-Host "cargo $($clippyArgs -join ' ')"
        & cargo @clippyArgs
        if ($LASTEXITCODE -ne 0) { throw "Full owned-package cargo clippy failed with exit code $LASTEXITCODE." }
    }

    $testArgs = @('test', '--locked')
    foreach ($package in $testSpecs) { $testArgs += @('--package', $package) }
    Write-Host "cargo $($testArgs -join ' ')"
    & cargo @testArgs
    if ($LASTEXITCODE -ne 0) { throw "Full owned-package cargo test failed with exit code $LASTEXITCODE." }
}

function Invoke-DiffCheck($Changes) {
    & git -C $script:RepoRoot diff --check $Changes.Base $Changes.Head --
    if ($LASTEXITCODE -ne 0) {
        throw 'git diff --check rejected whitespace errors.'
    }
}

function Invoke-MarkdownValidation($Changes) {
    $strictUtf8 = [Text.UTF8Encoding]::new($false, $true)
    foreach ($relativePath in @($Changes.Paths | Where-Object { $_ -match '\.md$' })) {
        $fullPath = Get-FullPath $relativePath
        if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) { continue }
        try {
            $text = $strictUtf8.GetString([IO.File]::ReadAllBytes($fullPath))
        } catch {
            throw "Markdown file '$relativePath' is not valid UTF-8."
        }
        $openChar = $null
        $openLength = 0
        $openLine = 0
        $lineNumber = 0
        foreach ($line in ($text -split "`r?`n")) {
            $lineNumber++
            if ($null -eq $openChar) {
                if ($line -match '^ {0,3}(`{3,}|~{3,})') {
                    $marker = $Matches[1]
                    $openChar = $marker.Substring(0, 1)
                    $openLength = $marker.Length
                    $openLine = $lineNumber
                }
            } else {
                $markerPattern = if ($openChar -eq '~') { '~' } else { '`' }
                $closePattern = '^ {0,3}' + $markerPattern + '{' + $openLength + ',}\s*$'
                if ($line -match $closePattern) {
                    $openChar = $null
                    $openLength = 0
                }
            }
        }
        if ($null -ne $openChar) {
            throw "Markdown file '$relativePath' has an unclosed fenced block opened at line $openLine."
        }
    }
}

function Invoke-ToolingValidation($Changes) {
    $parseErrors = [Collections.Generic.List[string]]::new()
    foreach ($relativePath in @($Changes.Paths | Where-Object { $_ -match '\.ps1$' })) {
        $fullPath = Get-FullPath $relativePath
        if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) { continue }
        $tokens = $null
        $errors = $null
        [Management.Automation.Language.Parser]::ParseFile($fullPath, [ref]$tokens, [ref]$errors) | Out-Null
        foreach ($parseError in @($errors)) {
            $parseErrors.Add("${relativePath}:$($parseError.Extent.StartLineNumber): $($parseError.Message)")
        }
    }
    if ($parseErrors.Count -gt 0) {
        throw ($parseErrors -join [Environment]::NewLine)
    }

    foreach ($relativePath in @($Changes.Paths | Where-Object {
        $_ -match '^modules/[^/]+/(package\.json|package-lock\.json|npm-shrinkwrap\.json)$'
    })) {
        $fullPath = Get-FullPath $relativePath
        if (-not (Test-Path -LiteralPath $fullPath -PathType Leaf)) { continue }
        try {
            [void](Get-Content -LiteralPath $fullPath -Raw | ConvertFrom-Json)
        } catch {
            throw "Module package metadata '$relativePath' is not valid JSON: $($_.Exception.Message)"
        }
    }

    $pythonFiles = @($Changes.Paths | Where-Object { $_ -match '^tools/.+\.py$' } | Where-Object {
        Test-Path -LiteralPath (Get-FullPath $_) -PathType Leaf
    })
    if ($pythonFiles.Count -gt 0) {
        $python = Get-Command python -ErrorAction SilentlyContinue
        if ($null -eq $python) {
            throw 'Python is required to syntax-check changed tools/*.py files.'
        }
        $pythonAst = 'import ast,pathlib,sys; [ast.parse(pathlib.Path(p).read_text(encoding="utf-8"), filename=p) for p in sys.argv[1:]]'
        $pythonPaths = @($pythonFiles | ForEach-Object { Get-FullPath $_ })
        & $python.Source -c $pythonAst @pythonPaths
        if ($LASTEXITCODE -ne 0) {
            throw 'Python AST parsing failed for a changed tools/*.py file.'
        }
    }
}

if ($Stage -eq 'FullRust') {
    Invoke-FullOwnedRustChecks
    return
}

$context = Get-ChangeContext
$changes = $context.Changes
$classification = $context.Classification

switch ($Stage) {
    'Classify' {
        Publish-Classification $changes $classification
        Write-Result 'packages_json' '[]'
        Write-Result 'clippy_packages_json' '[]'
        Write-Result 'test_targets_json' '[]'
    }
    'Resolve' {
        if (-not $classification.MetadataNeeded) {
            throw 'Resolve requires Rust source/manifest or tests/** changes.'
        }
        $scope = Resolve-PackageScope $changes $classification
        Write-JsonResult 'packages_json' $scope.FormatPackages
        Write-JsonResult 'clippy_packages_json' $scope.ClippyPackages
        Write-JsonResult 'test_targets_json' $scope.TestTargets
        Write-Result 'workspace_wide' ([string]$scope.WorkspaceWide).ToLowerInvariant()
    }
    'ValidateDocs' {
        Invoke-DiffCheck $changes
        Invoke-MarkdownValidation $changes
        Write-Host 'Documentation diff, UTF-8, and fenced-block checks passed.'
    }
    'ValidateTooling' {
        Invoke-DiffCheck $changes
        Invoke-ToolingValidation $changes
        Write-Host 'Changed PowerShell/Python tooling and module package metadata validated.'
    }
    'Verify' {
        $worktreeStatus = @(& git -C $script:RepoRoot status --porcelain=v1 --untracked-files=all)
        if ($LASTEXITCODE -ne 0) { throw 'git status could not verify the local checkout.' }
        if ($worktreeStatus.Count -gt 0) {
            throw 'Local Verify requires a clean checkout at HeadSha; commit the range or use the explicit per-package recipes for work in progress.'
        }
        Invoke-DiffCheck $changes
        if ($classification.DocsChanged) { Invoke-MarkdownValidation $changes }
        if ($classification.ToolingChanged) { Invoke-ToolingValidation $changes }
        if ($classification.RustChanged) {
            $scope = Resolve-PackageScope $changes $classification
            Invoke-ScopedCargoChecks $scope
        }
        Write-Host 'Scoped verification completed; full/native/release gates remain explicit.'
    }
}
