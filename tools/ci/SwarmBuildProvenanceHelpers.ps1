function Get-BytesSha256([byte[]] $Bytes) {
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return [Convert]::ToHexString($sha.ComputeHash($Bytes)).ToLowerInvariant() }
    finally { $sha.Dispose() }
}

function Get-MetadataField([object] $Object, [string] $Name) {
    $present = $false
    $value = $null
    if ($null -ne $Object) {
        if ($Object -is [Collections.IDictionary]) {
            if ($Object.Contains($Name)) {
                $present = $true
                $value = $Object[$Name]
            }
        } else {
            $property = $Object.PSObject.Properties[$Name]
            if ($null -ne $property) {
                $present = $true
                $value = $property.Value
            }
        }
    }
    return [pscustomobject]@{ present = $present; value = $value }
}

function Get-RequiredMetadataString([object] $Object, [string] $Name, [string] $Context) {
    $field = Get-MetadataField $Object $Name
    if (-not $field.present) {
        throw "Cargo metadata is missing required field '$Name' in $Context."
    }
    if ($field.value -isnot [string] -or [string]::IsNullOrWhiteSpace($field.value)) {
        throw "Cargo metadata field '$Name' in $Context must be a non-empty string."
    }
    return [string]$field.value
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
    $manifestValue = Get-RequiredMetadataString $Package 'manifest_path' 'selected Cargo package'
    $manifestPath = [IO.Path]::GetFullPath($manifestValue)
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
    $resolveField = Get-MetadataField $Metadata 'resolve'
    if (-not $resolveField.present -or $null -eq $resolveField.value) {
        throw 'Cargo metadata must include a non-null resolve graph.'
    }
    $nodesField = Get-MetadataField $resolveField.value 'nodes'
    if (-not $nodesField.present) {
        throw 'Cargo metadata resolve graph is missing its required nodes field.'
    }
    if ($nodesField.value -isnot [array] -or $nodesField.value.Count -eq 0) {
        throw 'Cargo metadata resolve.nodes must be a non-empty array.'
    }
    $packagesField = Get-MetadataField $Metadata 'packages'
    if (-not $packagesField.present) {
        throw 'Cargo metadata is missing its required packages field.'
    }
    if ($packagesField.value -isnot [array] -or $packagesField.value.Count -eq 0) {
        throw 'Cargo metadata packages must be a non-empty array.'
    }

    $selectedId = Get-RequiredMetadataString $Selected 'id' 'selected Cargo package'
    $selectedName = Get-RequiredMetadataString $Selected 'name' 'selected Cargo package'
    $packageById = @{}
    $packageNameById = @{}
    $packageVersionById = @{}
    $packageSourceById = @{}
    foreach ($package in $packagesField.value) {
        $packageId = Get-RequiredMetadataString $package 'id' 'Cargo package'
        $packageName = Get-RequiredMetadataString $package 'name' "Cargo package '$packageId'"
        $packageVersion = Get-RequiredMetadataString $package 'version' "Cargo package '$packageId'"
        $sourceField = Get-MetadataField $package 'source'
        if (-not $sourceField.present) {
            throw "Cargo package '$packageName@$packageVersion' is missing its required nullable source field."
        }
        if ($null -ne $sourceField.value -and
            ($sourceField.value -isnot [string] -or [string]::IsNullOrWhiteSpace($sourceField.value))) {
            throw "Cargo package '$packageName@$packageVersion' has an invalid source field."
        }
        $packageById[$packageId] = $package
        $packageNameById[$packageId] = $packageName
        $packageVersionById[$packageId] = $packageVersion
        $packageSourceById[$packageId] = $sourceField.value
    }
    $nodeById = @{}
    $nodeFeaturesById = @{}
    $nodeDependenciesById = @{}
    foreach ($node in $nodesField.value) {
        $nodeId = Get-RequiredMetadataString $node 'id' 'Cargo resolve node'
        $featuresField = Get-MetadataField $node 'features'
        if (-not $featuresField.present) {
            throw "Cargo resolve node '$nodeId' is missing its required features field."
        }
        if ($featuresField.value -isnot [array]) {
            throw "Cargo resolve node '$nodeId' features must be an array."
        }
        foreach ($feature in $featuresField.value) {
            if ($feature -isnot [string] -or [string]::IsNullOrWhiteSpace($feature)) {
                throw "Cargo resolve node '$nodeId' contains an invalid feature value."
            }
        }
        $featureValues = @($featuresField.value | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)

        $dependenciesField = Get-MetadataField $node 'dependencies'
        $dependencyDetailsField = Get-MetadataField $node 'deps'
        if ($dependenciesField.present -and $dependenciesField.value -is [array]) {
            $dependencies = $dependenciesField.value
        } elseif ($dependencyDetailsField.present -and $dependencyDetailsField.value -is [array]) {
            $dependencies = @(
                foreach ($dependency in $dependencyDetailsField.value) {
                    Get-RequiredMetadataString $dependency 'pkg' "dependency edge in Cargo resolve node '$nodeId'"
                }
            )
        } else {
            throw "Cargo resolve node '$nodeId' must include a dependencies or deps array."
        }
        foreach ($dependencyId in $dependencies) {
            if ($dependencyId -isnot [string] -or [string]::IsNullOrWhiteSpace($dependencyId)) {
                throw "Cargo resolve node '$nodeId' contains an invalid dependency package ID."
            }
        }
        $nodeById[$nodeId] = $node
        $nodeFeaturesById[$nodeId] = $featureValues
        $nodeDependenciesById[$nodeId] = $dependencies
    }
    $lockRecords = @(Get-CargoLockPackageRecords $LockfilePath)
    $pending = [Collections.Generic.Queue[string]]::new()
    $visited = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $pending.Enqueue($selectedId)
    $pins = [Collections.Generic.List[object]]::new()
    while ($pending.Count -gt 0) {
        $packageId = $pending.Dequeue()
        if (-not $visited.Add($packageId)) { continue }
        if (-not $packageById.ContainsKey($packageId) -or -not $nodeById.ContainsKey($packageId)) {
            throw "Cargo resolved dependency '$packageId' without its package or graph record."
        }
        $package = $packageById[$packageId]
        $packageName = $packageNameById[$packageId]
        $packageVersion = $packageVersionById[$packageId]
        $sourceValue = $packageSourceById[$packageId]
        $source = if ($null -eq $sourceValue) { 'path' } else { [string]$sourceValue }
        $features = $nodeFeaturesById[$packageId]
        if ($packageId -cne $selectedId) {
            $pin = [ordered]@{
                name = $packageName
                version = $packageVersion
                source = $source
                features = @($features)
                feature_scope = 'workspace_resolved_union_not_package_specific_build_features'
            }
            if ($source -ceq 'path') {
                $manifestValue = Get-RequiredMetadataString $package 'manifest_path' "Cargo path package '$packageName@$packageVersion'"
                $manifestPath = [IO.Path]::GetFullPath($manifestValue)
                if (-not (Test-PathWithin $manifestPath $Root)) {
                    throw "Path dependency '$packageName' is outside the selected checkout."
                }
                $pin.manifest = [IO.Path]::GetRelativePath($Root, $manifestPath).Replace('\', '/')
                $pin.manifest_sha256 = Get-Sha256 $manifestPath
                $pin.source_tree_sha256 = Get-PathPackageSourceSha256 $Root $manifestPath
            } else {
                $locked = @($lockRecords | Where-Object {
                    [string]$_.name -ceq $packageName -and
                    [string]$_.version -ceq $packageVersion -and
                    [string]$_.source -ceq $source
                })
                if ($locked.Count -ne 1) {
                    throw "Cargo.lock must contain exactly one record for external package '$packageName@$packageVersion'."
                }
                $pin.checksum = if ($null -eq $locked[0].checksum) { $null } else { [string]$locked[0].checksum }
                if ($source -match '\A(registry|sparse)\+' -and [string]$pin.checksum -cnotmatch '\A[0-9a-f]{64}\z') {
                    throw "Cargo.lock has no SHA-256 checksum for registry package '$packageName'."
                }
                if ($null -ne $pin.checksum -and [string]$pin.checksum -cnotmatch '\A[0-9a-f]{64}\z') {
                    throw "Cargo.lock has an invalid checksum for package '$packageName'."
                }
                if ($null -eq $pin.checksum -and $source -notmatch '#[0-9a-f]{40,64}\z') {
                    throw "Cargo.lock does not pin an exact content checksum or Git revision for '$packageName'."
                }
            }
            $pins.Add($pin)
        }

        foreach ($dependencyId in $nodeDependenciesById[$packageId]) {
            $pending.Enqueue($dependencyId)
        }
    }
    $sortedPins = @($pins | Sort-Object -Property name, version, source -CaseSensitive)
    if ($sortedPins.Count -eq 0) { throw "Selected package '$selectedName' has no resolved dependencies to pin." }
    return $sortedPins
}

function Get-HostIpcProtocolVersion([string] $Root) {
    $path = Join-Path $Root 'crates/swarm-kernel-host/src/ipc.rs'
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw 'The kernel host IPC implementation is missing.' }
    $matches = [regex]::Matches((Get-Content -LiteralPath $path -Raw), '"protocol_version"\s*:\s*(?<version>[0-9]+)')
    if ($matches.Count -ne 1) { throw 'Expected one explicit client.hello protocol_version in crates/swarm-kernel-host/src/ipc.rs.' }
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
