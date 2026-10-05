#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $SwarmExecutable,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedSwarmSha256,
    [Parameter(Mandatory)][string] $SwarmBuildManifestPath,
    [Parameter(Mandatory)][ValidatePattern('^[A-Fa-f0-9]{64}$')][string] $ExpectedSwarmBuildManifestSha256,
    [Parameter(Mandatory)][string] $HostConfigPath,
    [Parameter(Mandatory)][string] $OutputRoot,
    [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9._:-]{1,128}$')][string] $ProjectId,
    [Parameter(Mandatory)][string] $TaskSpecPath,
    [ValidatePattern('^[A-Za-z0-9._:-]{1,128}$')][string] $HookProjectId,
    [ValidatePattern('^[A-Fa-f0-9]{40}([A-Fa-f0-9]{24})?$')][string] $HookCommitOid,
    [ValidateRange(15, 180)][int] $TimeoutSeconds = 45
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:ContractBaseCommit = '410c327dd6172856bd8808da5938b2be9e1e4f9a'
$script:MaxCliOutputCharacters = 1MB
$script:MaxFrameBytes = 1MB
$script:RunDirectory = $null
$script:SwarmPath = $null
$script:ConfigPath = $null
$script:PendingPids = [System.Collections.Generic.List[int]]::new()
$script:Scenarios = [System.Collections.Generic.List[object]]::new()
$script:Summary = [ordered]@{
    schema_version = 1
    harness = 'eliot.core_failure_qualification.v1'
    source_contract_base_commit = $script:ContractBaseCommit
    status = 'not_started'
    qualification = 'not_qualified'
    started_at_utc = [DateTime]::UtcNow.ToString('o')
    host = $null
    scenarios = @()
    limits = [ordered]@{
        native_calls = 0
        model_calls = 0
        mutation_replay = $false
        uncertain_native_effect_replay = $false
        current_codex_or_opencode_process_control = 'none'
        process_control = 'only this harness own-host stdin EOF; no forced termination'
        raw_contents_in_receipt = $false
        task_spec_retained_in_private_store = $true
        no_public_optional_worker_fault_trigger = $true
        source_effect_unknown_state = 'only claimed when exact Manager/HookSource readback proves the corresponding durable record'
    }
}

function Stop-Harness {
    param([Parameter(Mandatory)][string] $Code)
    throw [System.InvalidOperationException]::new($Code)
}

function Assert-SafeAbsolutePath {
    param([Parameter(Mandatory)][string] $Path, [switch] $MustExist, [switch] $Directory)
    if (-not [System.IO.Path]::IsPathFullyQualified($Path)) { Stop-Harness 'PATH_MUST_BE_ABSOLUTE' }
    $full = [System.IO.Path]::GetFullPath($Path)
    $root = [System.IO.Path]::GetPathRoot($full)
    $current = $root
    foreach ($part in ($full.Substring($root.Length) -split '[\\/]')) {
        if ([string]::IsNullOrEmpty($part)) { continue }
        $current = Join-Path $current $part
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) { Stop-Harness 'REPARSE_PATH_NOT_ALLOWED' }
        }
    }
    if ($MustExist) {
        $item = Get-Item -LiteralPath $full -Force -ErrorAction SilentlyContinue
        if ($null -eq $item) { Stop-Harness 'INPUT_PATH_NOT_FOUND' }
        if ($Directory -and -not $item.PSIsContainer) { Stop-Harness 'INPUT_DIRECTORY_REQUIRED' }
        if (-not $Directory -and $item.PSIsContainer) { Stop-Harness 'INPUT_FILE_REQUIRED' }
    }
    return $full
}

function Get-Sha256 {
    param([Parameter(Mandatory)][string] $Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Read-JsonFile {
    param([Parameter(Mandatory)][string] $Path)
    try { return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable -Depth 48) }
    catch { Stop-Harness 'JSON_INPUT_INVALID' }
}

function Write-PrivateJson {
    param([Parameter(Mandatory)][string] $Path, [Parameter(Mandatory)] $Value)
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(($Value | ConvertTo-Json -Depth 48 -Compress))
    if ($bytes.Length -gt $script:MaxFrameBytes) { Stop-Harness 'REQUEST_FRAME_TOO_LARGE' }
    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) }
    finally { $stream.Dispose() }
}

function New-PrivateDirectory {
    param([Parameter(Mandatory)][string] $Path)
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $security = [System.Security.AccessControl.DirectorySecurity]::new()
    $security.SetAccessRuleProtection($true, $false)
    $security.SetOwner($identity.User)
    $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
        $identity.User,
        [System.Security.AccessControl.FileSystemRights]::FullControl,
        ([System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [System.Security.AccessControl.InheritanceFlags]::ObjectInherit),
        [System.Security.AccessControl.PropagationFlags]::None,
        [System.Security.AccessControl.AccessControlType]::Allow
    )
    [void]$security.AddAccessRule($rule)
    [void][System.IO.Directory]::CreateDirectory($Path, $security)
    [void](Assert-SafeAbsolutePath -Path $Path -MustExist -Directory)
}

function Save-Summary {
    if ([string]::IsNullOrWhiteSpace($script:RunDirectory)) { return }
    $script:Summary.scenarios = @($script:Scenarios.ToArray())
    $target = Join-Path $script:RunDirectory 'qualification-receipt.json'
    $temporary = Join-Path $script:RunDirectory ('.receipt-' + [Guid]::NewGuid().ToString('N') + '.tmp')
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes(($script:Summary | ConvertTo-Json -Depth 32))
    $stream = [System.IO.File]::Open($temporary, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) }
    finally { $stream.Dispose() }
    [System.IO.File]::Move($temporary, $target, $true)
}

function Add-Scenario {
    param([Parameter(Mandatory)][System.Collections.IDictionary] $Scenario)
    $script:Scenarios.Add([pscustomobject]$Scenario)
    Save-Summary
}

function Get-SafeErrorCode {
    param([AllowNull()][string] $Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return 'CLI_EXIT' }
    try {
        $value = $Text | ConvertFrom-Json -AsHashtable -Depth 16
        $code = $value.error.data.code
        if ([string]::IsNullOrWhiteSpace([string]$code)) { $code = $value.error.code }
        if ($code -is [string] -and $code -match '\A[A-Z0-9_]{1,64}\z') { return $code }
    }
    catch { }
    return 'CLI_EXIT'
}

function New-CaptureState {
    return [pscustomobject]@{
        stdout = [System.Text.StringBuilder]::new()
        stderr = [System.Text.StringBuilder]::new()
        sync = [object]::new()
        overflow = $false
    }
}

function Add-CapturedLine {
    param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][string] $StreamName, [AllowNull()][string] $Line)
    if ($null -eq $Line) { return }
    [System.Threading.Monitor]::Enter($State.sync)
    try {
        $buffer = $State.$StreamName
        $remaining = $script:MaxCliOutputCharacters - $buffer.Length
        if ($remaining -le 0) { $State.overflow = $true; return }
        $take = [Math]::Min($remaining, $Line.Length + 1)
        if ($take -lt $Line.Length + 1) { $State.overflow = $true }
        if ($take -gt 1) { [void]$buffer.Append($Line.Substring(0, [Math]::Min($Line.Length, $take - 1))) }
        [void]$buffer.Append("`n")
    }
    finally { [System.Threading.Monitor]::Exit($State.sync) }
}

function Start-BoundedProcess {
    param([Parameter(Mandatory)][string[]] $Arguments, [Parameter(Mandatory)][int] $TimeoutMilliseconds, [switch] $LongLived)
    $start = [System.Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $script:SwarmPath
    $start.WorkingDirectory = Split-Path -LiteralPath $script:SwarmPath -Parent
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = [bool]$LongLived
    foreach ($argument in $Arguments) { [void]$start.ArgumentList.Add([string]$argument) }
    $capture = New-CaptureState
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $start
    $outHandler = [System.Diagnostics.DataReceivedEventHandler]{ param($sender, $eventArgs) Add-CapturedLine -State $capture -StreamName 'stdout' -Line $eventArgs.Data }.GetNewClosure()
    $errHandler = [System.Diagnostics.DataReceivedEventHandler]{ param($sender, $eventArgs) Add-CapturedLine -State $capture -StreamName 'stderr' -Line $eventArgs.Data }.GetNewClosure()
    $process.add_OutputDataReceived($outHandler)
    $process.add_ErrorDataReceived($errHandler)
    try {
        if (-not $process.Start()) { return [pscustomobject]@{ started = $false; completed = $true; error_code = 'PROCESS_START_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $null; overflow = $false } }
        $process.BeginOutputReadLine()
        $process.BeginErrorReadLine()
        if ($LongLived) { return [pscustomobject]@{ started = $true; completed = $false; error_code = $null; stdout = ''; stderr = ''; process = $process; process_id = $process.Id; overflow = $false } }
        $completed = $process.WaitForExit($TimeoutMilliseconds)
        if (-not $completed) {
            $script:PendingPids.Add([int]$process.Id)
            return [pscustomobject]@{ started = $true; completed = $false; error_code = 'CLI_TIMEOUT_OUTCOME_UNKNOWN'; stdout = ''; stderr = ''; process = $null; process_id = $process.Id; overflow = $false }
        }
        $process.WaitForExit()
        [System.Threading.Monitor]::Enter($capture.sync)
        try {
            $stdout = $capture.stdout.ToString()
            $stderr = $capture.stderr.ToString()
            $overflow = [bool]$capture.overflow
        }
        finally { [System.Threading.Monitor]::Exit($capture.sync) }
        return [pscustomobject]@{ started = $true; completed = $true; exit_code = $process.ExitCode; error_code = $(if ($process.ExitCode -eq 0) { $null } else { Get-SafeErrorCode $stderr }); stdout = $stdout; stderr = $stderr; process = $null; process_id = $process.Id; overflow = $overflow }
    }
    catch {
        return [pscustomobject]@{ started = $false; completed = $true; error_code = 'PROCESS_IO_FAILED'; stdout = ''; stderr = ''; process = $null; process_id = $null; overflow = $false }
    }
    finally {
        if (-not $LongLived -or $null -eq $process -or $process.HasExited) { $process.Dispose() }
    }
}

function Get-PipeName {
    param([Parameter(Mandatory)][string] $DataDirectory)
    $canonical = [System.IO.Path]::GetFullPath($DataDirectory).ToLowerInvariant()
    $bytes = [System.Text.UTF8Encoding]::new($false).GetBytes($canonical)
    $hash = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
    return 'eliot-swarm-' + $hash.Substring(0, 32)
}

function Read-RpcResponse {
    param([Parameter(Mandatory)][System.IO.StreamReader] $Reader, [Parameter(Mandatory)][string] $ExpectedId)
    $line = $Reader.ReadLine()
    if ([string]::IsNullOrWhiteSpace($line) -or [System.Text.Encoding]::UTF8.GetByteCount($line) -gt $script:MaxFrameBytes) {
        return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null }
    }
    try { $reply = $line | ConvertFrom-Json -AsHashtable -Depth 32 }
    catch { return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null } }
    if ($reply.jsonrpc -cne '2.0' -or $reply.id -cne $ExpectedId) { return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_MISMATCH'; value = $null } }
    if ($reply.Contains('error')) {
        $code = [string]$reply.error.data.code
        if ($code -notmatch '\A[A-Z0-9_]{1,64}\z') { $code = 'RPC_ERROR' }
        return [pscustomobject]@{ success = $false; code = $code; value = $null }
    }
    if (-not $reply.Contains('result')) { return [pscustomobject]@{ success = $false; code = 'RPC_RESPONSE_INVALID'; value = $null } }
    return [pscustomobject]@{ success = $true; code = $null; value = $reply.result }
}

function Invoke-IpcCall {
    param(
        [Parameter(Mandatory)][string] $DataDirectory,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Credential,
        [Parameter(Mandatory)][string] $Method,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Params,
        [switch] $DropApplicationReply
    )
    $pipe = $null
    $reader = $null
    $writer = $null
    $applicationSent = $false
    $helloId = [Guid]::NewGuid().ToString('D')
    $requestId = [Guid]::NewGuid().ToString('D')
    try {
        $pipe = [System.IO.Pipes.NamedPipeClientStream]::new('.', (Get-PipeName $DataDirectory), [System.IO.Pipes.PipeDirection]::InOut, [System.IO.Pipes.PipeOptions]::None)
        $pipe.Connect([Math]::Min(10000, $TimeoutSeconds * 1000))
        $encoding = [System.Text.UTF8Encoding]::new($false)
        $reader = [System.IO.StreamReader]::new($pipe, $encoding, $false, 4096, $true)
        $writer = [System.IO.StreamWriter]::new($pipe, $encoding, 4096, $true)
        $writer.AutoFlush = $true
        $hello = [ordered]@{ jsonrpc = '2.0'; id = $helloId; method = 'client.hello'; params = $Credential }
        $helloLine = $hello | ConvertTo-Json -Depth 24 -Compress
        if ([System.Text.Encoding]::UTF8.GetByteCount($helloLine) -gt $script:MaxFrameBytes) { return [pscustomobject]@{ sent = $false; response_known = $true; success = $false; code = 'RPC_FRAME_TOO_LARGE'; value = $null } }
        $writer.WriteLine($helloLine)
        $helloReply = Read-RpcResponse -Reader $reader -ExpectedId $helloId
        if (-not $helloReply.success -or $helloReply.value.client_id -cne [string]$Credential.client_id) {
            return [pscustomobject]@{ sent = $false; response_known = $true; success = $false; code = $(if ($helloReply.code) { $helloReply.code } else { 'RPC_AUTH_MISMATCH' }); value = $null }
        }
        $request = [ordered]@{ jsonrpc = '2.0'; id = $requestId; method = $Method; params = $Params }
        $requestLine = $request | ConvertTo-Json -Depth 48 -Compress
        if ([System.Text.Encoding]::UTF8.GetByteCount($requestLine) -gt $script:MaxFrameBytes) { return [pscustomobject]@{ sent = $false; response_known = $true; success = $false; code = 'RPC_FRAME_TOO_LARGE'; value = $null } }
        $writer.WriteLine($requestLine)
        $writer.Flush()
        $applicationSent = $true
        if ($DropApplicationReply) {
            $writer.Dispose(); $writer = $null
            $reader.Dispose(); $reader = $null
            $pipe.Dispose(); $pipe = $null
            return [pscustomobject]@{ sent = $true; response_known = $false; success = $false; code = 'ACK_DROPPED_BY_HARNESS'; value = $null }
        }
        $reply = Read-RpcResponse -Reader $reader -ExpectedId $requestId
        return [pscustomobject]@{ sent = $applicationSent; response_known = $true; success = $reply.success; code = $reply.code; value = $reply.value }
    }
    catch {
        return [pscustomobject]@{ sent = $applicationSent; response_known = $false; success = $false; code = $(if ($applicationSent) { 'RPC_OUTCOME_UNKNOWN' } else { 'RPC_CONNECT_FAILED' }); value = $null }
    }
    finally {
        if ($null -ne $writer) { $writer.Dispose() }
        if ($null -ne $reader) { $reader.Dispose() }
        if ($null -ne $pipe) { $pipe.Dispose() }
    }
}

function New-ScenarioDirectory {
    param([Parameter(Mandatory)][string] $Name)
    $path = Join-Path $script:RunDirectory ($Name + '-' + [Guid]::NewGuid().ToString('N'))
    New-PrivateDirectory -Path $path
    $state = Join-Path $path 'state'
    $requests = Join-Path $path 'requests'
    New-PrivateDirectory -Path $state
    New-PrivateDirectory -Path $requests
    return [pscustomobject]@{ name = $Name; path = $path; state = $state; requests = $requests; host = $null; operator_path = (Join-Path $state 'operator.json') }
}

function Start-IsolatedHost {
    param([Parameter(Mandatory)] $Scenario)
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, 'host', '--stop-on-stdin-eof')
    $result = Start-BoundedProcess -Arguments $arguments -TimeoutMilliseconds 0 -LongLived
    if (-not $result.started) { return [pscustomobject]@{ ready = $false; code = $result.error_code; pid = $null } }
    $Scenario.host = $result.process
    $deadline = [DateTime]::UtcNow.AddSeconds([Math]::Min(30, $TimeoutSeconds))
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($Scenario.host.HasExited) { return [pscustomobject]@{ ready = $false; code = 'HOST_EXITED_DURING_START'; pid = $Scenario.host.Id } }
        if (Test-Path -LiteralPath $Scenario.operator_path) {
            $credential = Read-JsonFile $Scenario.operator_path
            $status = Invoke-IpcCall -DataDirectory $Scenario.state -Credential $credential -Method 'host.status' -Params ([ordered]@{})
            if ($status.success -and $null -ne $status.value) { return [pscustomobject]@{ ready = $true; code = $null; pid = $Scenario.host.Id; epoch = [long]$status.value.host_epoch } }
        }
        Start-Sleep -Milliseconds 250
    }
    return [pscustomobject]@{ ready = $false; code = 'HOST_READY_TIMEOUT'; pid = $Scenario.host.Id }
}

function Stop-IsolatedHost {
    param([Parameter(Mandatory)] $Scenario)
    if ($null -eq $Scenario.host) { return [pscustomobject]@{ stopped = $true; pending_pid = $null } }
    $process = $Scenario.host
    try {
        if (-not $process.HasExited) {
            $process.StandardInput.Close()
            if (-not $process.WaitForExit([Math]::Min(30000, $TimeoutSeconds * 1000))) {
                $script:PendingPids.Add([int]$process.Id)
                return [pscustomobject]@{ stopped = $false; pending_pid = [int]$process.Id }
            }
        }
        return [pscustomobject]@{ stopped = $true; pending_pid = $null }
    }
    catch {
        if (-not $process.HasExited) { $script:PendingPids.Add([int]$process.Id); return [pscustomobject]@{ stopped = $false; pending_pid = [int]$process.Id } }
        return [pscustomobject]@{ stopped = $true; pending_pid = $null }
    }
    finally {
        if ($process.HasExited) { $process.Dispose(); $Scenario.host = $null }
    }
}

function Invoke-SwarmCall {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][string] $Method,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Params,
        [int] $TimeoutMilliseconds = 15000
    )
    $requestPath = Join-Path $Scenario.requests ([Guid]::NewGuid().ToString('N') + '.json')
    Write-PrivateJson -Path $requestPath -Value $Params
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, '--credential', $CredentialPath, 'call', $Method, '--file', $requestPath)
    $process = Start-BoundedProcess -Arguments $arguments -TimeoutMilliseconds $TimeoutMilliseconds
    if ($process.completed) { try { [System.IO.File]::Delete($requestPath) } catch { } }
    if (-not $process.completed) { return [pscustomobject]@{ completed = $false; success = $false; code = $process.error_code; value = $null; pid = $process.process_id } }
    if (-not $process.started -or $process.overflow) { return [pscustomobject]@{ completed = $true; success = $false; code = $(if ($process.overflow) { 'CLI_OUTPUT_LIMIT' } else { $process.error_code }); value = $null; pid = $process.process_id } }
    if ($process.exit_code -ne 0) { return [pscustomobject]@{ completed = $true; success = $false; code = $process.error_code; value = $null; pid = $process.process_id } }
    try { $value = $process.stdout | ConvertFrom-Json -AsHashtable -Depth 48 }
    catch { return [pscustomobject]@{ completed = $true; success = $false; code = 'CLI_RESULT_INVALID'; value = $null; pid = $process.process_id } }
    return [pscustomobject]@{ completed = $true; success = $true; code = $null; value = $value; pid = $process.process_id }
}

function New-Manager {
    param([Parameter(Mandatory)] $Scenario)
    $managerId = 'core-failure-' + [Guid]::NewGuid().ToString('N')
    $credentialPath = Join-Path $Scenario.path 'manager.json'
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, '--credential', $Scenario.operator_path,
        'client-create', $managerId, '--role', 'manager', '--out', $credentialPath)
    $result = Start-BoundedProcess -Arguments $arguments -TimeoutMilliseconds 15000
    if (-not $result.completed -or $result.exit_code -ne 0 -or -not (Test-Path -LiteralPath $credentialPath)) { return [pscustomobject]@{ success = $false; code = $(if ($result.error_code) { $result.error_code } else { 'MANAGER_CREATE_UNKNOWN' }); id = $managerId; path = $credentialPath; credential = $null } }
    $credential = Read-JsonFile $credentialPath
    if ($credential.client_id -cne $managerId -or [string]::IsNullOrWhiteSpace([string]$credential.token)) { return [pscustomobject]@{ success = $false; code = 'MANAGER_CREDENTIAL_INVALID'; id = $managerId; path = $credentialPath; credential = $null } }
    return [pscustomobject]@{ success = $true; code = $null; id = $managerId; path = $credentialPath; credential = $credential }
}

function Read-TaskCreatedByOrigin {
    param([Parameter(Mandatory)] $Scenario, [Parameter(Mandatory)] $Manager, [Parameter(Mandatory)][string] $OriginKey)
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $listed = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Manager.path -Method 'task.list' -Params ([ordered]@{ after = 0; limit = 50 }) -TimeoutMilliseconds 10000
        if ($listed.success) {
            $matches = @($listed.value.items | Where-Object { $_.origin_key -ceq $OriginKey })
            if ($matches.Count -gt 1) { return [pscustomobject]@{ success = $false; code = 'TASK_ORIGIN_NOT_UNIQUE'; task = $null; operation = $null } }
            if ($matches.Count -eq 1) {
                $task = $matches[0]
                $ops = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Manager.path -Method 'operation.list' -Params ([ordered]@{ after = 0; limit = 50 }) -TimeoutMilliseconds 10000
                if ($ops.success) {
                    $opMatches = @($ops.value.items | Where-Object { $_.method -ceq 'task.create' -and $_.caller_id -ceq $Manager.id -and $_.task_id -ceq $task.task_id })
                    if ($opMatches.Count -gt 1) { return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_OPERATION_NOT_UNIQUE'; task = $task; operation = $null } }
                    if ($opMatches.Count -eq 1) {
                        $read = Invoke-SwarmCall -Scenario $Scenario -CredentialPath $Manager.path -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$opMatches[0].operation_id }) -TimeoutMilliseconds 10000
                        if ($read.success -and $read.value.operation_id -ceq $opMatches[0].operation_id -and $read.value.task_id -ceq $task.task_id) {
                            return [pscustomobject]@{ success = $true; code = $null; task = $task; operation = $read.value }
                        }
                    }
                }
            }
        }
        Start-Sleep -Milliseconds 300
    }
    return [pscustomobject]@{ success = $false; code = 'DURABLE_READBACK_TIMEOUT'; task = $null; operation = $null }
}

function Assert-TaskSpecFixture {
    param([Parameter(Mandatory)][System.Collections.IDictionary] $Spec)
    if (-not ($Spec.objective -is [string]) -or [string]::IsNullOrWhiteSpace($Spec.objective) -or
        -not ($Spec.phase -is [string]) -or [string]::IsNullOrWhiteSpace($Spec.phase) -or
        -not ($Spec.requirements -is [System.Collections.IList]) -or $Spec.requirements.Count -lt 1 -or
        -not ($Spec.scope -is [System.Collections.IDictionary]) -or
        -not ($Spec.scope.initial_paths -is [System.Collections.IList]) -or $Spec.scope.initial_paths.Count -lt 1) {
        Stop-Harness 'TASK_SPEC_FIXTURE_INCOMPLETE'
    }
    foreach ($requirement in $Spec.requirements) {
        if (-not ($requirement -is [System.Collections.IDictionary]) -or
            [string]::IsNullOrWhiteSpace([string]$requirement.id) -or
            [string]::IsNullOrWhiteSpace([string]$requirement.statement)) { Stop-Harness 'TASK_SPEC_FIXTURE_INCOMPLETE' }
    }
    foreach ($path in $Spec.scope.initial_paths) {
        if (-not ($path -is [string]) -or [string]::IsNullOrWhiteSpace($path)) { Stop-Harness 'TASK_SPEC_FIXTURE_INCOMPLETE' }
    }
}

function Remove-OwnedCredentialFiles {
    param([Parameter(Mandatory)] $Scenario)
    foreach ($path in @((Join-Path $Scenario.path 'manager.json'), $Scenario.operator_path)) {
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            try { Remove-Item -LiteralPath $path -Force -ErrorAction Stop } catch { }
        }
    }
}

function Get-OptionalWorkerSummary {
    param([Parameter(Mandatory)] $HostStatus)
    $workers = $HostStatus.host_lifecycle.optional_workers
    if ($null -eq $workers -or $workers -isnot [System.Collections.IDictionary]) { return @{} }
    $safe = [ordered]@{}
    foreach ($name in $workers.Keys) {
        if ([string]$name -notmatch '\A[a-z0-9-]{1,48}\z') { continue }
        $value = $workers[$name]
        $state = [string]$value.state
        if ($state -notin @('dormant', 'running', 'retry_wait', 'isolated')) { $state = 'unknown' }
        $errorCode = [string]$value.last_error_code
        if ($errorCode -notmatch '\A[A-Z0-9_]{1,64}\z') { $errorCode = $null }
        $safe[$name] = [ordered]@{ state = $state; consecutive_failures = [long]$value.consecutive_failures; last_error_code = $errorCode }
    }
    return $safe
}

function Invoke-ManagerDroppedAckScenario {
    $scenario = New-ScenarioDirectory -Name 'manager-dropped-ack-restart'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) { $result.code = $hostStart.code; return $result }
        $result.facts.host_pid = [int]$hostStart.pid
        $result.facts.host_epoch_before = [long]$hostStart.epoch
        $manager = New-Manager $scenario
        if (-not $manager.success) { $result.code = $manager.code; return $result }
        $taskSpec = Read-JsonFile $script:TaskSpecPath
        Assert-TaskSpecFixture $taskSpec
        $origin = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $logicalId = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $params = [ordered]@{ client_request_id = $logicalId; project_id = $ProjectId; origin_key = $origin; spec = $taskSpec }
        $send = Invoke-IpcCall -DataDirectory $scenario.state -Credential $manager.credential -Method 'task.create' -Params $params -DropApplicationReply
        if (-not $send.sent) { $result.code = $send.code; return $result }
        $result.facts.manager_ack = 'intentionally_not_read'
        $result.facts.logical_request_id = $logicalId
        $admitted = Read-TaskCreatedByOrigin -Scenario $scenario -Manager $manager -OriginKey $origin
        if (-not $admitted.success -or $admitted.operation.state -cne 'settled' -or
            $admitted.operation.result.task_id -cne $admitted.task.task_id -or
            $admitted.operation.result.created -ne $true) {
            $result.status = 'unknown'
            $result.code = $(if ($admitted.code) { $admitted.code } else { 'PRE_RESTART_ADMISSION_READBACK_MISMATCH' })
            return $result
        }
        $result.facts.admitted_operation_before_restart = [string]$admitted.operation.operation_id
        $result.facts.admitted_task_before_restart = [string]$admitted.task.task_id
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.status = 'pending'; $result.code = 'OWN_HOST_STOP_PENDING'; $result.facts.pending_owned_pid = $stopped.pending_pid; return $result }
        $restarted = Start-IsolatedHost $scenario
        if (-not $restarted.ready) { $result.status = 'unknown'; $result.code = $restarted.code; return $result }
        $result.facts.host_epoch_after = [long]$restarted.epoch
        if ($restarted.epoch -le $hostStart.epoch) { $result.status = 'unknown'; $result.code = 'HOST_EPOCH_DID_NOT_ADVANCE'; return $result }
        $readback = Read-TaskCreatedByOrigin -Scenario $scenario -Manager $manager -OriginKey $origin
        if (-not $readback.success) { $result.status = 'unknown'; $result.code = $readback.code; $result.facts.replayed = $false; return $result }
        if ($readback.operation.operation_id -cne $admitted.operation.operation_id -or
            $readback.task.task_id -cne $admitted.task.task_id) {
            $result.status = 'unknown'; $result.code = 'POST_RESTART_IDENTITY_CHANGED'; return $result
        }
        $result.facts.operation_id = [string]$readback.operation.operation_id
        $result.facts.task_id = [string]$readback.task.task_id
        if ($readback.operation.state -cne 'settled' -or $readback.operation.result.task_id -cne $readback.task.task_id -or $readback.operation.result.created -ne $true) {
            $result.status = 'unknown'; $result.code = 'TASK_CREATE_DURABLE_RECEIPT_MISMATCH'; return $result
        }
        $after = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$readback.operation.operation_id })
        if (-not $after.success -or $after.value.operation_id -cne $readback.operation.operation_id -or $after.value.state -cne 'settled' -or $after.value.result.task_id -cne $readback.task.task_id) {
            $result.status = 'unknown'; $result.code = $(if ($after.code) { $after.code } else { 'POST_RESTART_READBACK_MISMATCH' }); return $result
        }
        $result.status = 'observed'
        $result.facts.manager_readback_after_restart = $true
        $result.facts.operation_state_after_restart = [string]$after.value.state
        $result.facts.replayed = $false
        $result.facts.effect = 'one local Task create; caller acknowledgement was unknown until exact post-restart Manager readback'
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_owned_pid = $stopped.pending_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

function Invoke-ManagerConflictScenario {
    $scenario = New-ScenarioDirectory -Name 'manager-durable-request-conflict'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) { $result.code = $hostStart.code; return $result }
        $result.facts.host_pid = [int]$hostStart.pid
        $manager = New-Manager $scenario
        if (-not $manager.success) { $result.code = $manager.code; return $result }
        $taskSpec = Read-JsonFile $script:TaskSpecPath
        Assert-TaskSpecFixture $taskSpec
        $requestId = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $origin = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $first = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.create' -Params ([ordered]@{ client_request_id = $requestId; project_id = $ProjectId; origin_key = $origin; spec = $taskSpec })
        if (-not $first.success -or [string]::IsNullOrWhiteSpace([string]$first.value.task_id)) { $result.code = $(if ($first.code) { $first.code } else { 'TASK_CREATE_READBACK_REQUIRED' }); return $result }
        $originalTaskId = [string]$first.value.task_id
        $operations = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$first.value.operation_id })
        if (-not $operations.success -or $operations.value.state -cne 'settled') { $result.status = 'unknown'; $result.code = 'ORIGINAL_OPERATION_READBACK_FAILED'; return $result }
        $conflictOrigin = 'core-failure:' + [Guid]::NewGuid().ToString('N')
        $second = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.create' -Params ([ordered]@{ client_request_id = $requestId; project_id = $ProjectId; origin_key = $conflictOrigin; spec = $taskSpec })
        if ($second.success -or $second.code -cne 'REQUEST_ID_CONFLICT') { $result.status = 'unknown'; $result.code = $(if ($second.code) { $second.code } else { 'REQUEST_CONFLICT_NOT_REJECTED' }); return $result }
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.status = 'pending'; $result.code = 'OWN_HOST_STOP_PENDING'; $result.facts.pending_owned_pid = $stopped.pending_pid; return $result }
        $restarted = Start-IsolatedHost $scenario
        if (-not $restarted.ready) { $result.status = 'unknown'; $result.code = $restarted.code; return $result }
        $result.facts.host_epoch_after = [long]$restarted.epoch
        $after = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'operation.get' -Params ([ordered]@{ operation_id = [string]$operations.value.operation_id })
        $tasks = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'task.list' -Params ([ordered]@{ after = 0; limit = 50 })
        $conflictingTasks = if ($tasks.success) { @($tasks.value.items | Where-Object { $_.origin_key -ceq $conflictOrigin }) } else { @() }
        if (-not $after.success -or $after.value.operation_id -cne $operations.value.operation_id -or $after.value.result.task_id -cne $originalTaskId -or $conflictingTasks.Count -ne 0) {
            $result.status = 'unknown'; $result.code = 'DURABLE_CONFLICT_READBACK_MISMATCH'; return $result
        }
        $result.status = 'observed'
        $result.facts.manager_error_code = 'REQUEST_ID_CONFLICT'
        $result.facts.original_operation_id = [string]$after.value.operation_id
        $result.facts.original_task_id = $originalTaskId
        $result.facts.original_operation_state = [string]$after.value.state
        $result.facts.conflicting_task_created = $false
        $result.facts.current_manager_handover = $false
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_owned_pid = $stopped.pending_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

function Invoke-HookRestartDedupScenario {
    $scenario = New-ScenarioDirectory -Name 'hook-callback-restart-dedup'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        if ([string]::IsNullOrWhiteSpace($HookProjectId) -or [string]::IsNullOrWhiteSpace($HookCommitOid)) { $result.code = 'HOOK_FIXTURE_INPUTS_REQUIRED'; return $result }
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) { $result.code = $hostStart.code; return $result }
        $operator = Read-JsonFile $scenario.operator_path
        $sourceId = [Guid]::NewGuid().ToString('D')
        $token = [Guid]::NewGuid().ToString('D') + [Guid]::NewGuid().ToString('D')
        $hookCredential = [ordered]@{ client_id = 'hook-source:' + $sourceId; token = $token }
        $setup = [ordered]@{ client_request_id = 'core-failure:' + [Guid]::NewGuid().ToString('N'); project_id = $HookProjectId; source_id = $sourceId; credential = $hookCredential }
        $setupResult = Invoke-IpcCall -DataDirectory $scenario.state -Credential $operator -Method 'hook.source.setup' -Params $setup
        if (-not $setupResult.success -or $setupResult.value.source.source_id -cne $sourceId) { $result.code = $(if ($setupResult.code) { $setupResult.code } else { 'HOOK_SETUP_READBACK_MISMATCH' }); return $result }
        $eventParams = [ordered]@{ source_id = $sourceId; commit_oid = $HookCommitOid.ToLowerInvariant() }
        $dropped = Invoke-IpcCall -DataDirectory $scenario.state -Credential $hookCredential -Method 'hook.emit' -Params $eventParams -DropApplicationReply
        if (-not $dropped.sent) { $result.code = $dropped.code; return $result }
        $result.facts.callback_ack = 'intentionally_not_read'
        $result.facts.source_id = $sourceId
        $result.facts.commit_oid = $HookCommitOid.ToLowerInvariant()
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.status = 'pending'; $result.code = 'OWN_HOST_STOP_PENDING'; $result.facts.pending_owned_pid = $stopped.pending_pid; return $result }
        $restarted = Start-IsolatedHost $scenario
        if (-not $restarted.ready) { $result.status = 'unknown'; $result.code = $restarted.code; return $result }
        $result.facts.host_epoch_after = [long]$restarted.epoch
        $read = Invoke-IpcCall -DataDirectory $scenario.state -Credential $hookCredential -Method 'hook.source.get' -Params ([ordered]@{ source_id = $sourceId; after = 0; limit = 10 })
        if (-not $read.success) { $result.status = 'unknown'; $result.code = $(if ($read.code) { $read.code } else { 'HOOK_READBACK_FAILED' }); return $result }
        $events = @($read.value.events | Where-Object { $_.fact.source_id -ceq $sourceId -and $_.fact.commit_oid -ceq $eventParams.commit_oid })
        if ($events.Count -ne 1 -or $events[0].fact.readback_verified -ne $true) { $result.status = 'unknown'; $result.code = 'HOOK_EVENT_NOT_UNIQUE_AFTER_RESTART'; return $result }
        $firstId = [long]$events[0].observation_id
        $duplicate = Invoke-IpcCall -DataDirectory $scenario.state -Credential $hookCredential -Method 'hook.emit' -Params $eventParams
        if (-not $duplicate.success -or $duplicate.value.duplicate -ne $true -or $duplicate.value.recorded -ne $false -or [long]$duplicate.value.observation_id -ne $firstId) {
            $result.status = 'unknown'; $result.code = $(if ($duplicate.code) { $duplicate.code } else { 'HOOK_DUPLICATE_ACK_MISMATCH' }); return $result
        }
        $finalRead = Invoke-IpcCall -DataDirectory $scenario.state -Credential $hookCredential -Method 'hook.source.get' -Params ([ordered]@{ source_id = $sourceId; after = 0; limit = 10 })
        $finalEvents = if ($finalRead.success) { @($finalRead.value.events | Where-Object { $_.fact.commit_oid -ceq $eventParams.commit_oid }) } else { @() }
        if (-not $finalRead.success -or $finalEvents.Count -ne 1 -or [long]$finalEvents[0].observation_id -ne $firstId) {
            $result.status = 'unknown'; $result.code = 'HOOK_DEDUP_READBACK_MISMATCH'; return $result
        }
        $result.status = 'observed'
        $result.facts.observation_id = $firstId
        $result.facts.duplicate_callback = $true
        $result.facts.durable_after_host_restart = $true
        $result.facts.callback_identity = '{source_id, commit_oid}'
        $result.facts.replayed_native_effect = $false
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_owned_pid = $stopped.pending_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

function Invoke-OptionalWorkerObservation {
    $scenario = New-ScenarioDirectory -Name 'optional-worker-observation'
    $result = [ordered]@{ name = $scenario.name; run_id = (Split-Path -Leaf $scenario.path); status = 'blocked'; code = $null; facts = [ordered]@{} }
    try {
        $hostStart = Start-IsolatedHost $scenario
        if (-not $hostStart.ready) { $result.code = $hostStart.code; return $result }
        $manager = New-Manager $scenario
        if (-not $manager.success) { $result.code = $manager.code; return $result }
        $status = Invoke-SwarmCall -Scenario $scenario -CredentialPath $manager.path -Method 'host.status' -Params ([ordered]@{})
        if (-not $status.success) { $result.code = $(if ($status.code) { $status.code } else { 'HOST_STATUS_READBACK_FAILED' }); return $result }
        $workers = Get-OptionalWorkerSummary $status.value
        $result.facts.manager_visible_workers = $workers
        $failed = @($workers.GetEnumerator() | Where-Object { $_.Value.state -in @('retry_wait', 'isolated') })
        if ($failed.Count -gt 0) {
            $result.status = 'observed'
            $result.facts.failure_was_injected_by_harness = $false
            $result.facts.interpretation = 'existing bounded health is Manager-visible; this harness has no public control that can induce a specific legacy worker failure'
            return $result
        }
        $result.status = 'blocked'
        $result.code = 'NO_PUBLIC_OPTIONAL_WORKER_FAULT_TRIGGER'
        $result.facts.healthy_or_dormant_status_observed = $true
        $result.facts.failure_was_injected_by_harness = $false
        return $result
    }
    finally {
        $stopped = Stop-IsolatedHost $scenario
        if (-not $stopped.stopped) { $result.facts.pending_owned_pid = $stopped.pending_pid } else { Remove-OwnedCredentialFiles $scenario }
        Add-Scenario $result
    }
}

try {
    if (-not $IsWindows -or $PSVersionTable.PSVersion.Major -lt 7) { Stop-Harness 'POWERSHELL_7_WINDOWS_REQUIRED' }
    $script:SwarmPath = Assert-SafeAbsolutePath -Path $SwarmExecutable -MustExist
    $actualSwarmHash = Get-Sha256 $script:SwarmPath
    if ($actualSwarmHash -cne $ExpectedSwarmSha256.ToLowerInvariant()) { Stop-Harness 'SWARM_BINARY_HASH_MISMATCH' }
    $manifestPath = Assert-SafeAbsolutePath -Path $SwarmBuildManifestPath -MustExist
    if ([System.IO.Path]::GetFileName($manifestPath) -cne 'build-manifest.json' -or (Get-Sha256 $manifestPath) -cne $ExpectedSwarmBuildManifestSha256.ToLowerInvariant()) { Stop-Harness 'BUILD_MANIFEST_PIN_MISMATCH' }
    $manifest = Read-JsonFile $manifestPath
    $buildBinary = Assert-SafeAbsolutePath -Path (Join-Path (Split-Path -LiteralPath $manifestPath -Parent) 'bin\swarm.exe') -MustExist
    if ($manifest.schema_version -ne 1 -or $manifest.format -cne 'eliot.module_build_manifest.v1' -or
        $manifest.source.checkout_clean_before -ne $true -or $manifest.source.checkout_clean_after -ne $true -or
        $manifest.source.commit -notmatch '\A[a-f0-9]{40}\z' -or $manifest.source.tree -notmatch '\A[a-f0-9]{40}\z' -or
        $manifest.build.profile -cne 'release' -or $manifest.build.package_name -cne 'eliot-swarm-controller' -or
        @($manifest.build.binary_targets).Count -ne 1 -or $manifest.build.binary_targets[0] -cne 'swarm' -or
        @($manifest.artifacts).Count -ne 1 -or $manifest.artifacts[0].target_name -cne 'swarm' -or
        $manifest.artifacts[0].file -cne 'bin/swarm.exe' -or
        [long]$manifest.artifacts[0].bytes -ne (Get-Item -LiteralPath $script:SwarmPath).Length -or
        $manifest.artifacts[0].artifact_sha256 -cne $actualSwarmHash -or
        $manifest.artifacts[0].source_sha256 -cne $actualSwarmHash -or
        (Get-Sha256 $buildBinary) -cne $actualSwarmHash -or
        -not [string]::Equals($script:SwarmPath, $buildBinary, [StringComparison]::OrdinalIgnoreCase)) {
        Stop-Harness 'BUILD_MANIFEST_CONTRACT_MISMATCH'
    }
    $script:ConfigPath = Assert-SafeAbsolutePath -Path $HostConfigPath -MustExist
    $output = Assert-SafeAbsolutePath -Path $OutputRoot -MustExist -Directory
    $script:TaskSpecPath = Assert-SafeAbsolutePath -Path $TaskSpecPath -MustExist
    $script:TaskSpecHash = Get-Sha256 $script:TaskSpecPath
    $script:RunDirectory = Join-Path $output ([Guid]::NewGuid().ToString('D'))
    New-PrivateDirectory -Path $script:RunDirectory
    $script:Summary.status = 'running'
    $script:Summary.host = [ordered]@{
        package = 'eliot-swarm-controller'
        target = 'swarm'
        profile = 'release'
        image_sha256 = $actualSwarmHash
        manifest_sha256 = Get-Sha256 $manifestPath
        source_commit = $manifest.source.commit
        source_tree = $manifest.source.tree
        contract_base_commit = $script:ContractBaseCommit
        host_config_sha256 = Get-Sha256 $script:ConfigPath
        task_spec_sha256 = $script:TaskSpecHash
        target_dir_sha256 = [Convert]::ToHexString([System.Security.Cryptography.SHA256]::HashData([System.Text.Encoding]::UTF8.GetBytes(([string]$manifest.build.target_dir).ToUpperInvariant()))).ToLowerInvariant()
        manifest_is_unsigned = $true
    }
    Save-Summary

    $null = Invoke-ManagerDroppedAckScenario
    $null = Invoke-ManagerConflictScenario
    $null = Invoke-HookRestartDedupScenario
    $null = Invoke-OptionalWorkerObservation

    $script:Summary.status = 'completed'
    $script:Summary.completed_at_utc = [DateTime]::UtcNow.ToString('o')
    if ($script:PendingPids.Count -gt 0) { $script:Summary.limits.pending_owned_process_ids = @($script:PendingPids.ToArray()) }
    Save-Summary
    Write-Output (Join-Path $script:RunDirectory 'qualification-receipt.json')
}
catch {
    $code = $_.Exception.Message
    if ($code -notmatch '\A[A-Z0-9_]{1,64}\z') { $code = 'HARNESS_PREFLIGHT_FAILED' }
    $script:Summary.status = 'blocked'
    $script:Summary.preflight_error_code = $code
    if ($script:RunDirectory) { Save-Summary }
    Write-Error $code
    exit 2
}
