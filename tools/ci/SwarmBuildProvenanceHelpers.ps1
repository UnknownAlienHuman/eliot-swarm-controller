function Get-BytesSha256([byte[]] $Bytes) {
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return [Convert]::ToHexString($sha.ComputeHash($Bytes)).ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Get-PathPackageSourceSha256([string] $Root, [string] $ManifestPath) {
    $manifestFull = [IO.Path]::GetFullPath($ManifestPath)
    if (-not (Test-PathWithin $manifestFull $Root)) {
        throw "Path dependency manifest is outside the selected checkout: $manifestFull"
    }
    $packageRoot = [IO.Path]::GetDirectoryName($manifestFull)
    $rootItem = Get-Item -LiteralPath $packageRoot -Force
    if (($rootItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Reparse-point package root is not accepted in a path dependency: $packageRoot"
    }
    $records = [Collections.Generic.List[string]]::new()
    foreach ($entry in Get-ChildItem -LiteralPath $packageRoot -Recurse -Force) {
        if (($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "Reparse-point source is not accepted in a path dependency: $($entry.FullName)"
        }
        if ($entry.PSIsContainer) { continue }
        $relative = [IO.Path]::GetRelativePath($packageRoot, $entry.FullName).Replace('\', '/')
        if (@($relative.Split('/')) -contains '.git' -or @($relative.Split('/')) -contains 'target') { continue }
        $records.Add($relative + "`0" + (Get-Sha256 $entry.FullName))
    }
    if ($records.Count -eq 0) { throw "Path dependency contains no source files: $manifestFull" }
    $records.Sort([StringComparer]::Ordinal)
    $sourceHash = Get-BytesSha256 ([Text.UTF8Encoding]::new($false).GetBytes(($records -join "`n") + "`n"))
    return $sourceHash
}

function Get-PackageManifestPin([object] $Package, [string] $Root) {
    $manifestPath = [IO.Path]::GetFullPath([string]$Package.manifest_path)
    if (-not (Test-PathWithin $manifestPath $Root) -or
        -not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
        throw 'Selected package manifest is missing or outside the source checkout.'
    }
    $manifestItem = Get-Item -LiteralPath $manifestPath -Force
    if (($manifestItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or
        $manifestItem.Length -le 0) {
        throw 'Selected package manifest must be a non-empty regular file.'
    }
    return [ordered]@{
        path = [IO.Path]::GetRelativePath($Root, $manifestPath).Replace('\', '/')
        sha256 = Get-Sha256 $manifestPath
    }
}

function Get-CargoLockPackageRecords([string] $LockfilePath) {
    if (-not (Test-Path -LiteralPath $LockfilePath -PathType Leaf)) { throw 'Cargo.lock is missing.' }
    $lockText = Get-Content -LiteralPath $LockfilePath -Raw
    $blockPattern = '(?ms)^\[\[package\]\]\s*\r?\n(?<body>.*?)(?=^\[\[package\]\]|\z)'
    $records = [Collections.Generic.List[object]]::new()
    foreach ($match in [regex]::Matches($lockText, $blockPattern)) {
        $body = $match.Groups['body'].Value
        $name = [regex]::Match($body, '(?m)^name\s*=\s*"(?<value>[^"]+)"\s*$')
        $version = [regex]::Match($body, '(?m)^version\s*=\s*"(?<value>[^"]+)"\s*$')
        if (-not $name.Success -or -not $version.Success) { throw 'Cargo.lock contains a malformed package record.' }
        $source = [regex]::Match($body, '(?m)^source\s*=\s*"(?<value>[^"]+)"\s*$')
        $checksum = [regex]::Match($body, '(?m)^checksum\s*=\s*"(?<value>[^"]+)"\s*$')
        $records.Add([pscustomobject]@{
            name = $name.Groups['value'].Value
            version = $version.Groups['value'].Value
            source = if ($source.Success) { $source.Groups['value'].Value } else { $null }
            checksum = if ($checksum.Success) { $checksum.Groups['value'].Value } else { $null }
        })
    }
    if ($records.Count -eq 0) { throw 'Cargo.lock contains no package records.' }
    return $records.ToArray()
}

function Get-ResolvedDependencyPins([object] $Metadata, [object] $Selected, [string] $Root, [string] $LockfilePath) {
    if ($null -eq $Metadata.resolve -or $null -eq $Metadata.resolve.nodes) {
        throw 'Cargo metadata did not include a resolved dependency graph; package pins cannot be recorded.'
    }
    $packageById = @{}
    foreach ($package in $Metadata.packages) { $packageById[[string]$package.id] = $package }
    $nodeById = @{}
    foreach ($node in $Metadata.resolve.nodes) { $nodeById[[string]$node.id] = $node }
    $lockRecords = @(Get-CargoLockPackageRecords $LockfilePath)
    $pending = [Collections.Generic.Queue[string]]::new()
    $visited = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $pending.Enqueue([string]$Selected.id)
    $pins = [Collections.Generic.List[object]]::new()
    while ($pending.Count -gt 0) {
        $packageId = $pending.Dequeue()
        if (-not $visited.Add($packageId)) { continue }
        if (-not $packageById.ContainsKey($packageId) -or -not $nodeById.ContainsKey($packageId)) {
            throw "Cargo resolved dependency '$packageId' without its package or graph record."
        }
        $package = $packageById[$packageId]
        $node = $nodeById[$packageId]
        if ($packageId -cne [string]$Selected.id) {
            $source = if ($null -eq $package.source) { 'path' } else { [string]$package.source }
            $pin = [ordered]@{
                name = [string]$package.name
                version = [string]$package.version
                source = $source
                features = @($node.features | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)
                feature_scope = 'workspace_resolved_union_not_package_specific_build_features'
            }
            if ($source -ceq 'path') {
                $manifestPath = [IO.Path]::GetFullPath([string]$package.manifest_path)
                if (-not (Test-PathWithin $manifestPath $Root)) {
                    throw "Path dependency '$($package.name)' is outside the selected checkout."
                }
                $pin.manifest = [IO.Path]::GetRelativePath($Root, $manifestPath).Replace('\', '/')
                $pin.manifest_sha256 = Get-Sha256 $manifestPath
                $pin.source_tree_sha256 = Get-PathPackageSourceSha256 $Root $manifestPath
            } else {
                $locked = @($lockRecords | Where-Object {
                    [string]$_.name -ceq [string]$package.name -and
                    [string]$_.version -ceq [string]$package.version -and
                    [string]$_.source -ceq $source
                })
                if ($locked.Count -ne 1) {
                    throw "Cargo.lock must contain exactly one record for external package '$($package.name)@$($package.version)'."
                }
                $pin.checksum = if ($null -eq $locked[0].checksum) { $null } else { [string]$locked[0].checksum }
                if ($source -match '\A(registry|sparse)\+' -and [string]$pin.checksum -cnotmatch '\A[0-9a-f]{64}\z') {
                    throw "Cargo.lock has no SHA-256 checksum for registry package '$($package.name)'."
                }
                if ($null -ne $pin.checksum -and [string]$pin.checksum -cnotmatch '\A[0-9a-f]{64}\z') {
                    throw "Cargo.lock has an invalid checksum for package '$($package.name)'."
                }
                if ($null -eq $pin.checksum -and $source -notmatch '#[0-9a-f]{40,64}\z') {
                    throw "Cargo.lock does not pin an exact content checksum or Git revision for '$($package.name)'."
                }
            }
            $pins.Add($pin)
        }
        $dependencies = @()
        if ($null -ne $node.dependencies) { $dependencies = @($node.dependencies) }
        elseif ($null -ne $node.deps) { $dependencies = @($node.deps | ForEach-Object { $_.pkg }) }
        foreach ($dependencyId in $dependencies) { $pending.Enqueue([string]$dependencyId) }
    }
    $sortedPins = @($pins | Sort-Object -Property name, version, source -CaseSensitive)
    if ($sortedPins.Count -eq 0) { throw "Selected package '$($Selected.name)' has no resolved dependencies to pin." }
    return $sortedPins
}

function Get-HostIpcProtocolVersion([string] $Root) {
    $path = Join-Path $Root 'src/ipc.rs'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw 'The host IPC implementation is missing.' }
    $matches = [regex]::Matches((Get-Content -LiteralPath $path -Raw), '"protocol_version"\s*:\s*(?<version>[0-9]+)')
    if ($matches.Count -ne 1) { throw 'Expected one explicit client.hello protocol_version in src/ipc.rs.' }
    $version = 0
    if (-not [int]::TryParse($matches[0].Groups['version'].Value, [ref]$version) -or $version -le 0) {
        throw 'The host client.hello protocol_version must be a positive integer.'
    }
    return $version
}

function Get-RustcHostTriple([string] $RustcVersionVerbose) {
    $matches = [regex]::Matches($RustcVersionVerbose, '(?m)^host:\s*(?<triple>[A-Za-z0-9_-]+)\s*$')
    if ($matches.Count -ne 1) { throw 'rustc --version --verbose must identify one host target triple.' }
    return $matches[0].Groups['triple'].Value
}
