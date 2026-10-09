[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $SourceExecutable,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $DescriptorTemplate,

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string] $InstallRoot
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-OnlyKeys {
    param(
        [Parameter(Mandatory = $true)] [object] $Value,
        [Parameter(Mandatory = $true)] [string[]] $Allowed,
        [Parameter(Mandatory = $true)] [string] $Name
    )
    if ($Value -isnot [System.Collections.IDictionary]) {
        throw "$Name must be a JSON object."
    }
    foreach ($key in $Value.Keys) {
        if ($Allowed -cnotcontains [string] $key) {
            throw "$Name contains an unsupported field: $key"
        }
    }
}

function Set-DefaultField {
    param([System.Collections.IDictionary] $Map, [string] $Name, [object] $Value)
    if (-not $Map.Contains($Name)) {
        $Map[$Name] = $Value
    }
}

function Assert-NoReparseTraversal {
    param([Parameter(Mandatory = $true)] [string] $Path)

    $full = [System.IO.Path]::GetFullPath($Path)
    $root = [System.IO.Path]::GetPathRoot($full)
    if ([string]::IsNullOrEmpty($root)) {
        throw "Path has no filesystem root: $Path"
    }

    $cursor = $root
    $tail = $full.Substring($root.Length)
    $segments = $tail.Split(
        [char[]] @([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar),
        [System.StringSplitOptions]::RemoveEmptyEntries
    )
    foreach ($segment in $segments) {
        $cursor = [System.IO.Path]::Combine($cursor, $segment)
        $item = Get-Item -LiteralPath $cursor -Force -ErrorAction SilentlyContinue
        if ($null -ne $item -and ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
            throw "Reparse points are not accepted in installation paths: $cursor"
        }
    }
}

function Assert-AbsoluteExistingFile {
    param([Parameter(Mandatory = $true)] [string] $Path, [string] $Label)
    if (-not [System.IO.Path]::IsPathFullyQualified($Path)) {
        throw "$Label must be an absolute path."
    }
    $full = [System.IO.Path]::GetFullPath($Path)
    Assert-NoReparseTraversal -Path $full
    $item = Get-Item -LiteralPath $full -Force
    if ($item.PSIsContainer -or $item.Length -le 0) {
        throw "$Label must be a non-empty regular file."
    }
    return $full
}

function Assert-WithinRoot {
    param([string] $Path, [string] $Root)
    $fullPath = [System.IO.Path]::GetFullPath($Path)
    $fullRoot = [System.IO.Path]::GetFullPath($Root).TrimEnd('\', '/')
    $prefix = $fullRoot + [System.IO.Path]::DirectorySeparatorChar
    if (-not $fullPath.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Generated install path escapes the explicit install root: $fullPath"
    }
}

function Get-Sha256ForBytes {
    param([Parameter(Mandatory = $true)] [byte[]] $Bytes)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([System.Convert]::ToHexString($sha.ComputeHash($Bytes))).ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Get-Sha256ForStream {
    param([Parameter(Mandatory = $true)] [System.IO.Stream] $Stream)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $Stream.Position = 0
        return ([System.Convert]::ToHexString($sha.ComputeHash($Stream))).ToLowerInvariant()
    }
    finally {
        $sha.Dispose()
    }
}

function Get-Sha256ForFile {
    param([Parameter(Mandatory = $true)] [string] $Path)
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
    try { return Get-Sha256ForStream -Stream $stream }
    finally { $stream.Dispose() }
}

function Test-ExactBytes {
    param(
        [Parameter(Mandatory = $true)] [byte[]] $Left,
        [Parameter(Mandatory = $true)] [byte[]] $Right
    )
    if ($Left.Length -ne $Right.Length) { return $false }
    for ($index = 0; $index -lt $Left.Length; $index++) {
        if ($Left[$index] -ne $Right[$index]) { return $false }
    }
    return $true
}

function Assert-TextValue {
    param([object] $Value, [string] $Pattern, [string] $Field, [int] $MaximumBytes = 128)
    if ($Value -isnot [string] -or
        [string]::IsNullOrEmpty($Value) -or
        [System.Text.Encoding]::UTF8.GetByteCount($Value) -gt $MaximumBytes -or
        $Value -cnotmatch $Pattern) {
        throw "Invalid descriptor field: $Field"
    }
}

function Get-ValidatedInteger {
    param([object] $Value, [string] $Field, [long] $Minimum, [long] $Maximum)
    $numericTypes = @(
        'System.Byte', 'System.SByte', 'System.Int16', 'System.UInt16',
        'System.Int32', 'System.UInt32', 'System.Int64', 'System.UInt64'
    )
    if ($null -eq $Value -or $Value.GetType().FullName -cnotin $numericTypes) {
        throw "$Field must be a JSON integer."
    }
    $number = [decimal] $Value
    if ($number -lt $Minimum -or $number -gt $Maximum) {
        throw "$Field is out of range."
    }
    return [long] $number
}

function Assert-NoInstallerPlaceholder {
    param([string] $Value, [string] $Field)
    if ($Value -match '(?i)<INSTALLER_[A-Z0-9_-]+>' -or
        $Value -match '(?i)(?:\A|:)REPLACE_AT_INSTALL(?::|\z)') {
        throw "Unresolved installer placeholder: $Field"
    }
}

function Assert-ProtectedReference {
    param([object] $Value, [string] $Field)
    if ($Value -isnot [string] -or $Value.Length -eq 0 -or
        [System.Text.Encoding]::UTF8.GetByteCount($Value) -gt 512 -or
        $Value -match '[\x00-\x1f\x7f]') {
        throw "Invalid protected reference: $Field"
    }
    Assert-NoInstallerPlaceholder -Value $Value -Field $Field
}

function Assert-LaunchValue {
    param(
        [object] $Value,
        [string] $Field,
        [bool] $AllowModuleHostConfigPath = $false
    )
    Assert-OnlyKeys -Value $Value -Allowed @('kind', 'value') -Name $Field
    if (-not $Value.Contains('kind') -or -not $Value.Contains('value')) {
        throw "$Field must contain kind and value."
    }
    switch -CaseSensitive ([string] $Value.kind) {
        'literal' {
            if ($Value.value -isnot [string] -or
                [System.Text.Encoding]::UTF8.GetByteCount([string] $Value.value) -gt 16384 -or
                ([string] $Value.value).Contains([char] 0)) {
                throw "Invalid literal launch value: $Field"
            }
            Assert-NoInstallerPlaceholder -Value ([string] $Value.value) -Field $Field
        }
        'protected' { Assert-ProtectedReference -Value $Value.value -Field $Field }
        'module_host_config_path' {
            if (-not $AllowModuleHostConfigPath) {
                throw "The supervisor-generated module host config path is accepted only in launch.argv: $Field"
            }
            Assert-OnlyKeys -Value $Value.value -Allowed @('schema_version') -Name "$Field.value"
            if (-not $Value.value.Contains('schema_version') -or
                (Get-ValidatedInteger -Value $Value.value.schema_version -Field "$Field.value.schema_version" -Minimum 1 -Maximum 1) -ne 1) {
                throw "Unsupported supervisor-generated module host config path schema: $Field"
            }
        }
        default { throw "Invalid launch value kind: $Field" }
    }
}

function Assert-SchemaDescriptor {
    param([object] $Schema, [string] $Field)
    Assert-OnlyKeys -Value $Schema -Allowed @('schema_id', 'version', 'sha256') -Name $Field
    Assert-TextValue -Value $Schema.schema_id -Pattern '\A[A-Za-z0-9._:/@-]{1,128}\z' -Field "$Field.schema_id"
    Assert-TextValue -Value $Schema.version -Pattern '\A[A-Za-z0-9.+_-]{1,128}\z' -Field "$Field.version"
    if ($Schema.Contains('sha256') -and $null -ne $Schema.sha256 -and
        ([string] $Schema.sha256) -cnotmatch '\A[0-9a-f]{64}\z') {
        throw "Invalid SHA-256 field: $Field.sha256"
    }
}

function Assert-WorkspaceOption {
    param([Parameter(Mandatory = $true)] [object] $Value)

    Assert-OnlyKeys -Value $Value -Allowed @(
        'schema_version', 'native_options_pointer', 'semantics'
    ) -Name 'workspace_option'
    foreach ($field in @('schema_version', 'native_options_pointer', 'semantics')) {
        if (-not $Value.Contains($field)) { throw "workspace_option.$field is required." }
    }
    [void] (Get-ValidatedInteger -Value $Value.schema_version -Field 'workspace_option.schema_version' -Minimum 1 -Maximum 1)
    if ($Value.semantics -isnot [string] -or
        $Value.semantics -cne 'replace_with_admitted_absolute_workspace') {
        throw 'Unsupported workspace_option semantics.'
    }

    $pointer = $Value.native_options_pointer
    if ($pointer -isnot [string] -or
        [System.Text.Encoding]::UTF8.GetByteCount($pointer) -gt 1024 -or
        -not $pointer.StartsWith('/', [StringComparison]::Ordinal)) {
        throw 'workspace_option.native_options_pointer must be a bounded absolute JSON Pointer.'
    }
    $segments = $pointer.Substring(1).Split([char] '/')
    if ($segments.Length -lt 1 -or $segments.Length -gt 8) {
        throw 'workspace_option.native_options_pointer must contain one to eight segments.'
    }
    foreach ($segment in $segments) {
        if ([string]::IsNullOrEmpty($segment) -or
            [System.Text.Encoding]::UTF8.GetByteCount($segment) -gt 128) {
            throw 'workspace_option.native_options_pointer contains an empty or oversized segment.'
        }
        $decoded = [System.Text.StringBuilder]::new()
        for ($index = 0; $index -lt $segment.Length; $index++) {
            $character = $segment[$index]
            if ($character -eq [char] '~') {
                if (($index + 1) -ge $segment.Length) {
                    throw 'workspace_option.native_options_pointer contains an invalid JSON Pointer escape.'
                }
                $index++
                switch ($segment[$index]) {
                    '0' { [void] $decoded.Append([char] '~') }
                    '1' { [void] $decoded.Append([char] '/') }
                    default { throw 'workspace_option.native_options_pointer contains an invalid JSON Pointer escape.' }
                }
            }
            else { [void] $decoded.Append($character) }
        }
        $decodedText = $decoded.ToString()
        if ([string]::IsNullOrEmpty($decodedText) -or
            [System.Text.Encoding]::UTF8.GetByteCount($decodedText) -gt 128 -or
            @($decodedText.ToCharArray() | Where-Object { [char]::IsControl($_) }).Count -gt 0) {
            throw 'workspace_option.native_options_pointer contains an invalid decoded object key.'
        }
    }
}

function Assert-PreInputOpen {
    param([Parameter(Mandatory = $true)] [object] $Value)

    Assert-OnlyKeys -Value $Value -Allowed @(
        'schema_version', 'kind', 'completion_condition', 'native_identity',
        'initial_identity_adoption'
    ) -Name 'pre_input_open'
    foreach ($field in @('schema_version', 'kind', 'completion_condition', 'native_identity', 'initial_identity_adoption')) {
        if (-not $Value.Contains($field)) { throw "pre_input_open.$field is required." }
    }
    [void] (Get-ValidatedInteger -Value $Value.schema_version -Field 'pre_input_open.schema_version' -Minimum 1 -Maximum 1)
    if ($Value.kind -isnot [string] -or $Value.kind -cne 'pre_input_executor_ready' -or
        $Value.completion_condition -isnot [string] -or $Value.completion_condition -cne 'native_executor_prepared' -or
        $Value.native_identity -isnot [string] -or $Value.native_identity -cne 'rootless' -or
        $Value.initial_identity_adoption -isnot [string] -or
        $Value.initial_identity_adoption -cne 'first_task_dispatch_exact_native_echo') {
        throw 'Unsupported pre_input_open semantics.'
    }
}

function Assert-AntigravityV5DescriptorContract {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary] $Descriptor)

    # The generic descriptor validator remains shared by every module. This
    # narrow check pins the exact Antigravity .1 v5 coordinate to the claim
    # consumed by the adapter and Store; it does not add a productive-body
    # promise to the descriptor's agent.result capability.
    if ($Descriptor.module_id -ne 'antigravity' -or
        $Descriptor.artifact.artifact_id -ne 'eliot-antigravity.rust-headless.1' -or
        $Descriptor.artifact.version -ne '5') {
        return
    }

    $commands = @($Descriptor.command_schemas |
        ForEach-Object { '{0}@{1}' -f $_.schema_id, $_.version } |
        Sort-Object)
    $events = @($Descriptor.event_schemas |
        ForEach-Object { '{0}@{1}' -f $_.schema_id, $_.version } |
        Sort-Object)
    $expectedCommands = @(
        'swarm.normalized_result_context@1'
        'swarm.runtime_command@1'
        'swarm.task_dispatch_context@1'
        'swarm.task_prompt@1'
    )
    $expectedEvents = @(
        'swarm.normalized_result_page@1'
        'swarm.runtime_outcome@1'
        'swarm.task_dispatch_admission@1'
    )
    if (($commands -join ',') -ne ($expectedCommands -join ',') -or
        ($events -join ',') -ne ($expectedEvents -join ',')) {
        throw 'Antigravity artifact version 5 requires the exact normalized result, dispatch, and TaskPrompt schema sets.'
    }
    $schemas = @($Descriptor.command_schemas) + @($Descriptor.event_schemas)
    if ($schemas | Where-Object { $_.Contains('sha256') -and $null -ne $_.sha256 }) {
        throw 'Antigravity artifact version 5 uses the unkeyed shared schema descriptors.'
    }

    $capabilities = @($Descriptor.capabilities | ForEach-Object { [string]$_ } | Sort-Object)
    $expectedCapabilities = @(
        'agent.open'
        'agent.reconcile'
        'agent.refresh'
        'agent.result'
        'agent.send/next_turn'
        'task.dispatch'
    )
    if (($capabilities -join ',') -ne ($expectedCapabilities -join ',')) {
        throw 'Antigravity artifact version 5 capability set is not aligned with its adapter claim.'
    }
}

function Assert-DescriptorTemplate {
    param([System.Collections.IDictionary] $Descriptor)

    Assert-OnlyKeys -Value $Descriptor -Allowed @(
        'schema_version', 'module_id', 'artifact', 'launch', 'config_schema',
        'command_schemas', 'event_schemas', 'protocol', 'capabilities',
        'lifecycle', 'activation', 'enabled', 'restart', 'workspace_option',
        'pre_input_open'
    ) -Name 'descriptor'

    foreach ($required in @('module_id', 'artifact', 'launch', 'protocol', 'lifecycle', 'activation', 'enabled')) {
        if (-not $Descriptor.Contains($required)) { throw "descriptor.$required is required." }
    }

    Set-DefaultField -Map $Descriptor -Name 'schema_version' -Value 1
    if ((Get-ValidatedInteger -Value $Descriptor.schema_version -Field 'schema_version' -Minimum 1 -Maximum 1) -ne 1) {
        throw 'Only module descriptor schema version 1 is supported.'
    }
    Assert-TextValue -Value $Descriptor.module_id -Pattern '\A[A-Za-z0-9._:-]{1,128}\z' -Field 'module_id'
    if ($Descriptor.module_id -in @('.', '..')) { throw 'module_id cannot be . or ..' }

    Assert-OnlyKeys -Value $Descriptor.artifact -Allowed @('artifact_id', 'version', 'build_id') -Name 'artifact'
    Set-DefaultField -Map $Descriptor.artifact -Name 'build_id' -Value $null
    Assert-TextValue -Value $Descriptor.artifact.artifact_id -Pattern '\A[A-Za-z0-9._:-]{1,128}\z' -Field 'artifact.artifact_id'
    if ($Descriptor.artifact.artifact_id -in @('.', '..')) { throw 'artifact_id cannot be . or ..' }
    Assert-TextValue -Value $Descriptor.artifact.version -Pattern '\A[A-Za-z0-9.+_-]{1,128}\z' -Field 'artifact.version'
    if ($Descriptor.artifact.version -in @('.', '..')) { throw 'artifact.version cannot be . or ..' }
    if ($Descriptor.artifact.Contains('build_id') -and $null -ne $Descriptor.artifact.build_id) {
        Assert-TextValue -Value $Descriptor.artifact.build_id -Pattern '\A[A-Za-z0-9._:+-]{1,128}\z' -Field 'artifact.build_id'
    }

    Assert-OnlyKeys -Value $Descriptor.launch -Allowed @(
        'executable', 'argv', 'environment', 'credential_ref', 'working_directory',
        'inherited_environment_allowlist', 'executable_sha256'
    ) -Name 'launch'
    foreach ($field in @('argv', 'environment', 'inherited_environment_allowlist')) {
        Set-DefaultField -Map $Descriptor.launch -Name $field -Value ([object[]]::new(0))
    }
    foreach ($field in @('credential_ref', 'working_directory', 'executable_sha256')) {
        Set-DefaultField -Map $Descriptor.launch -Name $field -Value $null
    }
    if ($Descriptor.launch.argv -isnot [array] -or $Descriptor.launch.argv.Count -gt 256) {
        throw 'launch.argv must be an array of at most 256 values.'
    }
    $moduleHostConfigPathCount = 0
    foreach ($argument in $Descriptor.launch.argv) {
        if ($argument -is [System.Collections.IDictionary] -and
            $argument.Contains('kind') -and
            [string] $argument.kind -ceq 'module_host_config_path') {
            $moduleHostConfigPathCount++
        }
    }
    if ($moduleHostConfigPathCount -gt 1) {
        throw 'launch.argv may contain at most one supervisor-generated module host config path.'
    }
    $launchBytes = 0
    for ($index = 0; $index -lt $Descriptor.launch.argv.Count; $index++) {
        $argument = $Descriptor.launch.argv[$index]
        Assert-LaunchValue -Value $argument -Field "launch.argv[$index]" -AllowModuleHostConfigPath $true
        if ($argument.kind -eq 'module_host_config_path') {
            $launchBytes += 4096
        }
        else {
            $launchBytes += [System.Text.Encoding]::UTF8.GetByteCount([string] $argument.value)
        }
        if ($argument.kind -eq 'literal' -and ([string] $argument.value) -match '(?i)\A--?(token|secret|password|credential|api[-_]?key)(=|\z)') {
            throw 'Secret-bearing argv literals must use a protected reference.'
        }
        if ($index -gt 0 -and $Descriptor.launch.argv[$index - 1].kind -eq 'literal' -and
            ([string] $Descriptor.launch.argv[$index - 1].value) -match '(?i)\A--?(token|secret|password|credential|api[-_]?key)\z' -and
            $argument.kind -eq 'literal') {
            throw 'A secret-bearing argv value must use a protected reference.'
        }
    }
    if ($Descriptor.launch.environment -isnot [array] -or $Descriptor.launch.environment.Count -gt 256) {
        throw 'launch.environment must be an array of at most 256 assignments.'
    }
    $environmentNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($variable in $Descriptor.launch.environment) {
        Assert-OnlyKeys -Value $variable -Allowed @('name', 'value') -Name 'launch.environment[]'
        if ($variable.name -isnot [string] -or $variable.name -cnotmatch '\A[A-Za-z_][A-Za-z0-9_]*\z' -or
            -not $environmentNames.Add([string] $variable.name)) {
            throw 'launch.environment contains an invalid or duplicate name.'
        }
        Assert-LaunchValue -Value $variable.value -Field "launch.environment.$($variable.name)"
        $launchBytes += [System.Text.Encoding]::UTF8.GetByteCount([string] $variable.name)
        $launchBytes += [System.Text.Encoding]::UTF8.GetByteCount([string] $variable.value.value)
        if ($variable.name -match '(?i)(TOKEN|SECRET|PASSWORD|CREDENTIAL|API[_-]?KEY)' -and $variable.value.kind -eq 'literal') {
            throw "Secret-bearing environment variable $($variable.name) must use a protected reference."
        }
    }
    if ($Descriptor.launch.inherited_environment_allowlist -isnot [array] -or
        $Descriptor.launch.inherited_environment_allowlist.Count -gt 256) {
        throw 'launch.inherited_environment_allowlist must be an array of at most 256 names.'
    }
    foreach ($name in $Descriptor.launch.inherited_environment_allowlist) {
        if ($name -isnot [string] -or $name -cnotmatch '\A[A-Za-z_][A-Za-z0-9_]*\z' -or
            -not $environmentNames.Add([string] $name)) {
            throw 'launch.inherited_environment_allowlist contains an invalid or duplicate name.'
        }
        $launchBytes += [System.Text.Encoding]::UTF8.GetByteCount([string] $name)
    }
    if ($launchBytes -gt 65536) { throw 'Combined argv and environment values exceed 64 KiB.' }
    if ($null -ne $Descriptor.launch.credential_ref) {
        Assert-ProtectedReference -Value $Descriptor.launch.credential_ref -Field 'launch.credential_ref'
    }
    if ($null -ne $Descriptor.launch.working_directory -and
        (-not [System.IO.Path]::IsPathFullyQualified([string] $Descriptor.launch.working_directory) -or
         ([string] $Descriptor.launch.working_directory).Length -gt 4096)) {
        throw 'launch.working_directory must be an absolute path.'
    }

    Set-DefaultField -Map $Descriptor -Name 'config_schema' -Value $null
    if ($null -ne $Descriptor.config_schema) { Assert-SchemaDescriptor -Schema $Descriptor.config_schema -Field 'config_schema' }
    foreach ($field in @('command_schemas', 'event_schemas', 'capabilities')) {
        Set-DefaultField -Map $Descriptor -Name $field -Value ([object[]]::new(0))
    }
    foreach ($field in @('command_schemas', 'event_schemas')) {
        if ($Descriptor[$field] -isnot [array] -or $Descriptor[$field].Count -gt 256) {
            throw "$field must be an array of at most 256 entries."
        }
        foreach ($schema in $Descriptor[$field]) { Assert-SchemaDescriptor -Schema $schema -Field "$field[]" }
    }
    if ($Descriptor.capabilities -isnot [array] -or $Descriptor.capabilities.Count -gt 256) {
        throw 'capabilities must be an array of at most 256 entries.'
    }
    if ($Descriptor.Contains('workspace_option') -and $null -ne $Descriptor.workspace_option) {
        Assert-WorkspaceOption -Value $Descriptor.workspace_option
    }
    if ($Descriptor.Contains('pre_input_open') -and $null -ne $Descriptor.pre_input_open) {
        Assert-PreInputOpen -Value $Descriptor.pre_input_open
    }

    foreach ($capability in $Descriptor.capabilities) {
        Assert-TextValue -Value $capability -Pattern '\A[A-Za-z0-9._:/@-]{1,128}\z' -Field 'capabilities[]'
    }

    Assert-OnlyKeys -Value $Descriptor.protocol -Allowed @('minimum', 'maximum') -Name 'protocol'
    foreach ($bound in @('minimum', 'maximum')) {
        if (-not $Descriptor.protocol.Contains($bound)) { throw "protocol.$bound is required." }
        Assert-OnlyKeys -Value $Descriptor.protocol[$bound] -Allowed @('major', 'minor') -Name "protocol.$bound"
        if (-not $Descriptor.protocol[$bound].Contains('major') -or -not $Descriptor.protocol[$bound].Contains('minor')) {
            throw "protocol.$bound requires major and minor."
        }
        [void] (Get-ValidatedInteger -Value $Descriptor.protocol[$bound].major -Field "protocol.$bound.major" -Minimum 0 -Maximum 65535)
        [void] (Get-ValidatedInteger -Value $Descriptor.protocol[$bound].minor -Field "protocol.$bound.minor" -Minimum 0 -Maximum 65535)
    }
    if ($Descriptor.protocol.minimum.major -ne $Descriptor.protocol.maximum.major -or
        $Descriptor.protocol.minimum.minor -gt $Descriptor.protocol.maximum.minor) {
        throw 'Protocol range must be ordered within one major version.'
    }

    if ($Descriptor.lifecycle -cnotin @('external_attach', 'owned_service', 'one_shot') -or
        $Descriptor.activation -cnotin @('on_demand', 'continuous') -or
        $Descriptor.enabled -isnot [bool]) {
        throw 'Invalid module lifecycle, activation, or enabled metadata.'
    }
    Set-DefaultField -Map $Descriptor -Name 'restart' -Value ([ordered] @{
        max_starts = 5; window_ms = 60000; initial_backoff_ms = 250;
        max_backoff_ms = 30000; reset_after_healthy_ms = 60000; jitter = $true
    })
    Assert-OnlyKeys -Value $Descriptor.restart -Allowed @(
        'max_starts', 'window_ms', 'initial_backoff_ms', 'max_backoff_ms',
        'reset_after_healthy_ms', 'jitter'
    ) -Name 'restart'
    $maxStarts = Get-ValidatedInteger -Value $Descriptor.restart.max_starts -Field 'restart.max_starts' -Minimum 1 -Maximum 64
    [void] (Get-ValidatedInteger -Value $Descriptor.restart.window_ms -Field 'restart.window_ms' -Minimum 1 -Maximum 1800000)
    $initialBackoff = Get-ValidatedInteger -Value $Descriptor.restart.initial_backoff_ms -Field 'restart.initial_backoff_ms' -Minimum 1 -Maximum 300000
    $maxBackoff = Get-ValidatedInteger -Value $Descriptor.restart.max_backoff_ms -Field 'restart.max_backoff_ms' -Minimum 1 -Maximum 300000
    [void] (Get-ValidatedInteger -Value $Descriptor.restart.reset_after_healthy_ms -Field 'restart.reset_after_healthy_ms' -Minimum 1 -Maximum 1800000)
    if ($initialBackoff -gt $maxBackoff -or $Descriptor.restart.jitter -isnot [bool]) {
        throw 'Invalid bounded restart policy.'
    }
}

function New-Utf8JsonBytes {
    param([Parameter(Mandatory = $true)] [object] $Value)
    $json = ConvertTo-Json -InputObject $Value -Depth 64 -Compress
    return [System.Text.UTF8Encoding]::new($false).GetBytes($json + "`n")
}

function Publish-ImmutableBytes {
    param([string] $Path, [byte[]] $Bytes)
    Assert-WithinRoot -Path $Path -Root $script:NormalizedInstallRoot
    Assert-NoReparseTraversal -Path ([System.IO.Path]::GetDirectoryName($Path))
    if ([System.IO.File]::Exists($Path)) {
        Assert-NoReparseTraversal -Path $Path
        if (-not (Test-ExactBytes -Left ([System.IO.File]::ReadAllBytes($Path)) -Right $Bytes)) {
            throw "Immutable install metadata already exists with different bytes: $Path"
        }
        return
    }

    $temporary = Join-Path ([System.IO.Path]::GetDirectoryName($Path)) ('.stage-' + [guid]::NewGuid().ToString('N'))
    try {
        $stream = [System.IO.File]::Open($temporary, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
        try {
            $stream.Write($Bytes, 0, $Bytes.Length)
            $stream.Flush($true)
            if ((Get-Sha256ForStream -Stream $stream) -ne (Get-Sha256ForBytes -Bytes $Bytes)) {
                throw "Staged install metadata digest mismatch: $Path"
            }
        }
        finally { $stream.Dispose() }
        Assert-NoReparseTraversal -Path $temporary
        try { [System.IO.File]::Move($temporary, $Path) }
        catch [System.IO.IOException] {
            if (-not [System.IO.File]::Exists($Path) -or
                -not (Test-ExactBytes -Left ([System.IO.File]::ReadAllBytes($Path)) -Right $Bytes)) { throw }
        }
    }
    finally {
        if ([System.IO.File]::Exists($temporary)) { [System.IO.File]::Delete($temporary) }
    }
}

$sourcePath = Assert-AbsoluteExistingFile -Path $SourceExecutable -Label 'SourceExecutable'
$templatePath = Assert-AbsoluteExistingFile -Path $DescriptorTemplate -Label 'DescriptorTemplate'
if (-not [System.IO.Path]::IsPathFullyQualified($InstallRoot)) { throw 'InstallRoot must be absolute.' }
$script:NormalizedInstallRoot = [System.IO.Path]::GetFullPath($InstallRoot)
Assert-NoReparseTraversal -Path $script:NormalizedInstallRoot
if (-not [System.IO.Directory]::Exists($script:NormalizedInstallRoot)) { throw 'InstallRoot must already exist as a directory.' }
$rootItem = Get-Item -LiteralPath $script:NormalizedInstallRoot -Force
if (-not $rootItem.PSIsContainer) { throw 'InstallRoot must be a directory.' }
$rootSegments = $script:NormalizedInstallRoot.Split(
    [char[]] @([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar),
    [System.StringSplitOptions]::RemoveEmptyEntries
)
if ($rootSegments -contains '.codex' -or $rootSegments -contains 'Codex' -or
    $rootSegments -contains 'OpenCode' -or $rootSegments -contains 'Open-Codex' -or
    $rootSegments -contains 'OpenCodex' -or
    $rootSegments -contains 'swarm' -or $rootSegments -contains 'eliot-swarm-controller' -or
    @($rootSegments | Where-Object { $_ -match '(?i)\A(?:openai\.)?(?:codex|opencodex|opencode)(?:[_.-].*)?\z' }).Count -gt 0) {
    throw 'InstallRoot cannot be inside a current Codex/OpenCode or installed swarm location.'
}

$sourceLeaf = [System.IO.Path]::GetFileName($sourcePath)
if ($sourceLeaf -cnotmatch '\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z') {
    throw 'Source executable must have a simple portable file name.'
}
if ([System.IO.Path]::GetExtension($sourceLeaf) -ine '.exe') {
    throw 'This Windows installer accepts only an already-built .exe artifact.'
}
$sourceBaseName = [System.IO.Path]::GetFileNameWithoutExtension($sourceLeaf)
if ($sourceBaseName -match '(?i)\A(swarm|eliot-swarm|eliot-swarm-controller|codex|opencode|open-codex|opencodex)\z') {
    throw 'The installer refuses core swarm, Codex, and OpenCode executable names.'
}

try {
    $descriptor = Get-Content -LiteralPath $templatePath -Raw | ConvertFrom-Json -AsHashtable
}
catch { throw "DescriptorTemplate is not valid JSON: $($_.Exception.Message)" }
Assert-DescriptorTemplate -Descriptor $descriptor
Assert-AntigravityV5DescriptorContract -Descriptor $descriptor

$coordinate = [ordered]@{
    module_id = [string] $descriptor.module_id
    artifact_id = [string] $descriptor.artifact.artifact_id
    version = [string] $descriptor.artifact.version
}
$coordinateJsonBytes = New-Utf8JsonBytes -Value $coordinate
$scopeToken = Get-Sha256ForBytes -Bytes $coordinateJsonBytes
$artifactDirectory = Join-Path $script:NormalizedInstallRoot "artifact-$scopeToken"
$coordinatePath = Join-Path $artifactDirectory 'install-coordinate.json'
$binaryPath = Join-Path $artifactDirectory 'module.exe'
$descriptorPath = Join-Path $artifactDirectory 'module-descriptor.json'
$receiptPath = Join-Path $artifactDirectory 'install-receipt.json'
$stagingPathSample = Join-Path $artifactDirectory ('.stage-' + ('0' * 32))
foreach ($path in @($artifactDirectory, $coordinatePath, $binaryPath, $descriptorPath, $receiptPath, $stagingPathSample)) {
    Assert-WithinRoot -Path $path -Root $script:NormalizedInstallRoot
}
if ($coordinatePath.Length -gt 240 -or $binaryPath.Length -gt 240 -or
    $descriptorPath.Length -gt 240 -or $receiptPath.Length -gt 240 -or
    $stagingPathSample.Length -gt 240) {
    throw 'Generated installation paths exceed the supported 240-character limit.'
}

$sourceStream = [System.IO.File]::Open($sourcePath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
try { $sourceHash = Get-Sha256ForStream -Stream $sourceStream }
finally { $sourceStream.Dispose() }

$descriptor.launch.executable = $binaryPath
$descriptor.launch.executable_sha256 = $sourceHash
$descriptorJsonBytes = New-Utf8JsonBytes -Value $descriptor
$descriptorHash = Get-Sha256ForBytes -Bytes $descriptorJsonBytes
$receipt = [ordered] @{
    schema_version = 1
    format = 'eliot.module_install_receipt.v1'
    module_id = [string] $descriptor.module_id
    artifact_id = [string] $descriptor.artifact.artifact_id
    version = [string] $descriptor.artifact.version
    build_id = $descriptor.artifact.build_id
    source_file = $sourcePath
    installed_file = $binaryPath
    source_sha256 = $sourceHash
    staged_sha256 = $sourceHash
    installed_sha256 = $sourceHash
    descriptor_file = $descriptorPath
    descriptor_sha256 = $descriptorHash
}
$receiptJsonBytes = New-Utf8JsonBytes -Value $receipt
$receiptHash = Get-Sha256ForBytes -Bytes $receiptJsonBytes
$installIdentity = [ordered]@{
    schema_version = 1
    format = 'eliot.module_install_coordinate.v1'
    module_id = [string] $descriptor.module_id
    artifact_id = [string] $descriptor.artifact.artifact_id
    version = [string] $descriptor.artifact.version
    build_id = $descriptor.artifact.build_id
    source_file = $sourcePath
    source_sha256 = $sourceHash
    descriptor_sha256 = $descriptorHash
    receipt_sha256 = $receiptHash
}
$installIdentityJsonBytes = New-Utf8JsonBytes -Value $installIdentity

# Fail before creating anything if an immutable identity already points at
# different bytes or metadata. Repeated installation of identical bytes is safe.
Assert-NoReparseTraversal -Path $artifactDirectory
$coordinateAlreadyPresent = [System.IO.File]::Exists($coordinatePath)
$binaryAlreadyPresent = [System.IO.File]::Exists($binaryPath)
$receiptAlreadyPresent = [System.IO.File]::Exists($receiptPath)
$descriptorAlreadyPresent = [System.IO.File]::Exists($descriptorPath)
if ($coordinateAlreadyPresent) {
    Assert-NoReparseTraversal -Path $coordinatePath
    if (-not (Test-ExactBytes -Left ([System.IO.File]::ReadAllBytes($coordinatePath)) -Right $installIdentityJsonBytes)) {
        throw "Artifact scope hash collides with a different exact module/artifact/version identity: $coordinatePath"
    }
}
elseif ($binaryAlreadyPresent -or $receiptAlreadyPresent -or $descriptorAlreadyPresent) {
    throw 'Existing artifact files have no exact identity marker; refusing to adopt the destination.'
}
if ($binaryAlreadyPresent) {
    Assert-NoReparseTraversal -Path $binaryPath
    if ((Get-Sha256ForFile -Path $binaryPath) -ne $sourceHash) {
        throw "Immutable artifact destination already contains different bytes: $binaryPath"
    }
}
if ($receiptAlreadyPresent) {
    Assert-NoReparseTraversal -Path $receiptPath
    if (-not (Test-ExactBytes -Left ([System.IO.File]::ReadAllBytes($receiptPath)) -Right $receiptJsonBytes)) {
        throw "Immutable install receipt already differs: $receiptPath"
    }
}
if ($descriptorAlreadyPresent) {
    Assert-NoReparseTraversal -Path $descriptorPath
    if (-not (Test-ExactBytes -Left ([System.IO.File]::ReadAllBytes($descriptorPath)) -Right $descriptorJsonBytes)) {
        throw "Immutable module descriptor already differs: $descriptorPath"
    }
}

if (-not $PSCmdlet.ShouldProcess($binaryPath, 'Install and hash an already-built module artifact')) {
    return [pscustomobject] @{
        status = 'what_if'
        module_id = [string] $descriptor.module_id
        artifact_id = [string] $descriptor.artifact.artifact_id
        version = [string] $descriptor.artifact.version
        artifact_scope_sha256 = $scopeToken
        installed_file = $binaryPath
        sha256 = $sourceHash
    }
}

foreach ($directory in @(
    $artifactDirectory
)) {
    Assert-WithinRoot -Path $directory -Root $script:NormalizedInstallRoot
    Assert-NoReparseTraversal -Path $directory
    if (-not [System.IO.Directory]::Exists($directory)) { [void] [System.IO.Directory]::CreateDirectory($directory) }
    Assert-NoReparseTraversal -Path $directory
}

# This exact coordinate marker is published before the executable so a partial
# install or a theoretical full-SHA scope collision cannot adopt another tuple.
Publish-ImmutableBytes -Path $coordinatePath -Bytes $installIdentityJsonBytes

if (-not $binaryAlreadyPresent) {
    $stagedPath = Join-Path $artifactDirectory ('.stage-' + [guid]::NewGuid().ToString('N'))
    try {
        Assert-NoReparseTraversal -Path $sourcePath
        $sourceStream = [System.IO.File]::Open($sourcePath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)
        try {
            $sourceHashBeforeCopy = Get-Sha256ForStream -Stream $sourceStream
            if ($sourceHashBeforeCopy -ne $sourceHash) { throw 'Source executable changed before staging.' }
            $sourceStream.Position = 0
            $stagedStream = [System.IO.File]::Open($stagedPath, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
            try {
                $sourceStream.CopyTo($stagedStream)
                $stagedStream.Flush($true)
                $stagedHash = Get-Sha256ForStream -Stream $stagedStream
            }
            finally { $stagedStream.Dispose() }
            $sourceHashAfterCopy = Get-Sha256ForStream -Stream $sourceStream
            if ($sourceHashAfterCopy -ne $sourceHash -or $stagedHash -ne $sourceHash) {
                throw 'Source and staged executable digests differ.'
            }
        }
        finally { $sourceStream.Dispose() }

        Assert-NoReparseTraversal -Path $stagedPath
        try { [System.IO.File]::Move($stagedPath, $binaryPath) }
        catch [System.IO.IOException] {
            if (-not [System.IO.File]::Exists($binaryPath) -or
                (Get-Sha256ForFile -Path $binaryPath) -ne $sourceHash) { throw }
        }
    }
    finally {
        if ([System.IO.File]::Exists($stagedPath)) { [System.IO.File]::Delete($stagedPath) }
    }
}

Assert-NoReparseTraversal -Path $binaryPath
if ((Get-Sha256ForFile -Path $binaryPath) -ne $sourceHash) {
    throw 'Final executable readback digest does not match the verified source digest.'
}

# The receipt appears before the descriptor. A descriptor is therefore the
# final commit marker; consumers must require both files and verify their hashes.
Publish-ImmutableBytes -Path $receiptPath -Bytes $receiptJsonBytes
Publish-ImmutableBytes -Path $descriptorPath -Bytes $descriptorJsonBytes

[pscustomobject] @{
    status = if ($coordinateAlreadyPresent -and $binaryAlreadyPresent -and $receiptAlreadyPresent -and $descriptorAlreadyPresent) { 'already_present' } else { 'installed' }
    module_id = [string] $descriptor.module_id
    artifact_id = [string] $descriptor.artifact.artifact_id
    version = [string] $descriptor.artifact.version
    artifact_scope_sha256 = $scopeToken
    installed_file = $binaryPath
    installed_sha256 = $sourceHash
    descriptor_file = $descriptorPath
    descriptor_sha256 = $descriptorHash
    receipt_file = $receiptPath
}
