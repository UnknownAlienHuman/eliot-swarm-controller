#requires -Version 7.0
<# ==========================================================================================
 Invoke-R48SchedulerQualification.ps1

 Bounded sidecar qualification harness for R48 installed qualification.

 Scope of this file
   Implements only the subgates that the R48 acceptance plan leaves to the installed
   Q run and that do not depend on the automation worker package decision:

     1. Scheduler current-owner readiness       (public host.status owner projection)
     2. One real, eligible one-shot CheckRun    (public host.status plus operation.get)
     3. Host remains alive across the bounded readback window
     4. Uncertain initial automation.scheduler.admit receipt with no replay
        (bounded read-only observation; see the companion design file)

 This harness is a SIDECAR, not a replacement:
   * It does not touch, mutate, or replace any existing script under tools/qualification.
   * It does not reuse any binary, package, or credential produced by an earlier N run.
   * It owns exactly one fresh run directory under its own OutputRoot and nothing else.
   * The parent decides when the Q candidate set gains the matching release
     swarm-automation-worker package. This harness consumes that package through an
     explicit parameter and refuses to proceed without it. The current eight-package
     Q set (host runtime, supervisor, host launcher, CLI, process helper, MCP, OpenCode,
     Antigravity) excludes the automation worker, so it is always supplied separately.

 Authority boundaries that are preserved, never weakened
   * The Store keeps exclusive service authority over automation.scheduler.admit.
   * The scheduler worker keeps exclusive authority over its own caller identity and
     over the page cut it submits. This harness never forges either one.
   * No admission, schedule-cursor, or database decision is fabricated here.
   * Q receipts are preserved verbatim, and Q is never replayed by this harness.
     Where an assertion cannot be met, the harness records the gap and stops.

 What this harness does NOT do
   * No source change, no build, no test, no native execution, no model call.
   * No git mutation of any kind.
   * No claim of overall R48 pass. Each subgate is reported separately.

 Verifier
   The exact parse and explain checks are printed by -Explain and recorded in the
   receipt. The single written artifact is qualification-receipt.json in the fresh
   R48 run directory.
========================================================================================== #>

[CmdletBinding(DefaultParameterSetName = 'Run')]
param(
    [Parameter(ParameterSetName = 'Explain')][switch] $Explain,
    # ---- Provenance of the exact Q candidate set this run is allowed to consume ----
    [Parameter(ParameterSetName = 'Run')][string] $QRoot = '',
    [Parameter(ParameterSetName = 'Run')][string] $HostExecutable = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedHostSha256 = '',
    [Parameter(ParameterSetName = 'Run')][string] $HostSupervisorExecutable = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedHostSupervisorSha256 = '',
    [Parameter(ParameterSetName = 'Run')][string] $HostLauncherExecutable = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedHostLauncherSha256 = '',
    [Parameter(ParameterSetName = 'Run')][string] $PublicCliExecutable = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedPublicCliSha256 = '',

    # The scheduler worker. Always supplied separately because the current eight-package
    # Q set excludes it. Must be a Q-candidate release build whose manifest source
    # commit and tree match the Q host build manifest. Earlier N binaries are refused.
    [Parameter(ParameterSetName = 'Run')][string] $AutomationWorkerExecutable = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedAutomationWorkerSha256 = '',
    [Parameter(ParameterSetName = 'Run')][string] $AutomationWorkerBuildManifestPath = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedAutomationWorkerBuildManifestSha256 = '',
    [Parameter(ParameterSetName = 'Run')][string] $HostBuildManifestPath = '',
    [Parameter(ParameterSetName = 'Run')][string] $ExpectedHostBuildManifestSha256 = '',

    # ---- Bounded static host configuration, supplied by the operator ----
    # The host configuration is TOMC-ready: it must already contain the one-shot schedule,
    # because no schedule mutation or reload surface exists at runtime.
    [Parameter(ParameterSetName = 'Run')][string] $HostConfigPath = '',
    [Parameter(ParameterSetName = 'Run')][string] $ProjectId = '',
    [Parameter(ParameterSetName = 'Run')][string] $TaskSpecPath = '',
    [Parameter(ParameterSetName = 'Run')][string] $GitExecutable = '',
    [Parameter(ParameterSetName = 'Run')][string] $SourceRepository = '',
    [Parameter(ParameterSetName = 'Run')][string] $SourceCommitOid = '',

    # ---- Fresh disjoint R48 output location ----
    [Parameter(ParameterSetName = 'Run')][string] $OutputRoot = '',
    [Parameter(ParameterSetName = 'Run')][string] $ScheduleId = '',

    # The check profile the pinned schedule names. It must already exist in the
    # operator template, because this harness adds no check profile of its own.
    [Parameter(ParameterSetName = 'Run')][string] $CheckProfileId = '',
    [Parameter(ParameterSetName = 'Run')][string] $CheckProfileRevision = '',

    # ---- Bounds ----
    [ValidateRange(30, 300)][int] $TimeoutSeconds = 180,
    [ValidateRange(1, 300)][int] $OwnerReadinessPollSeconds = 60,
    [ValidateRange(1, 300)][int] $DueOperationPollSeconds = 45,
    [ValidateRange(120, 86400)][int] $MinimumScheduleLeadSeconds = 120,
    [ValidateRange(1, 60)][int] $HostAliveSamples = 3,
    [ValidateRange(1, 60)][int] $HostAliveSampleIntervalSeconds = 10

)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

<# ==========================================================================================
 Static bounds. Every value here is a limit, not a policy.
========================================================================================== #>
$script:Schema = 'eliot.swarm.r48.sidecar-qualification.v1'
$script:MaxCliOutputCharacters = 1MB
$script:MaxFrameBytes = 1MB
$script:AutomationServiceId = 'automation-scheduler-v1'
$script:WorkerExecutableName = 'swarm-automation-worker.exe'
$script:ContractBaseCommit = '410c327dd6172856bd8808da5938b2be9e1e4f9a'
$script:AutomationPipePrefix = 'eliot-swarm-'

$script:RunDirectory = $null
# Run parameters already occupy script scope; initialize only derived state.
$script:HostPath = $null
$script:HostSupervisorPath = $null
$script:HostLauncherPath = $null
$script:PublicCliPath = $null
$script:AutomationWorkerPath = $null
$script:AutomationWorkerInstalledPath = $null
$script:ConfigPath = $null
$script:HostConfigTemplatePath = $null
$script:BootstrapScenario = $null
$script:FinalScenario = $null
$script:Manager = $null
$script:ReceiptPath = $null
$script:SubgateResults = [System.Collections.Generic.List[object]]::new()
$script:QReceipts = [System.Collections.Generic.List[object]]::new()

$script:Report = [ordered]@{
    schema = $script:Schema
    harness = 'Invoke-R48SchedulerQualification.ps1'
    purpose = 'bounded R48 sidecar qualification: scheduler owner readiness, one real eligible one-shot CheckRun, host liveness, and uncertain initial admit receipt with no replay'
    qualification = 'not_qualified'
    status = 'not_started'
    source_contract_base_commit = $script:ContractBaseCommit
    started_at_utc = [DateTime]::UtcNow.ToString('o')
    run_directory = $null
    q_root = $null
    disjoint_from_q = $null
    automation_worker = $null
    subgates = [ordered]@{
        scheduler_current_owner_ready = 'not_verified'
        enabled_one_shot_checkrun_admitted = 'not_verified'
        uncertain_initial_admit_receipt_no_replay = 'not_verified'
        host_remains_alive = 'not_verified'
    }
    subgate_evidence = @()
    q_receipts = @()
    limits = [ordered]@{
        source_changes = 'none; this harness edits no Rust, fixture, config, or tracked file'
        builds_tests_native_model_calls = 'none'
        git_mutations = 'none'
        q_run_root_writes = 'none; the retained Q run root is only read for provenance'
        worker_binary_reuse = 'refused unless the supplied worker matches its Q-candidate manifest and hash'
        eight_package_q_set = 'unchanged; the automation worker package is supplied separately'
        overall_claim = 'this harness reports each subgate separately and claims no overall R48 result'
    }
    verifier = [ordered]@{
        parse_check = 'pwsh -NoProfile -Command "$e=$null;$t=$null;[void][System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path ./tools/qualification/Invoke-R48SchedulerQualification.ps1),[ref]$t,[ref]$e);if($e.Count){$e|ForEach-Object{Write-Host $_.Message};exit 1}"'
        explain_check = 'pwsh -NoProfile -File ./tools/qualification/Invoke-R48SchedulerQualification.ps1 -Explain'
        receipt_check = 'the harness prints the single qualification-receipt.json path it wrote under the fresh R48 run directory'
    }
}


<# ==========================================================================================
 Utilities
========================================================================================== #>
function Stop-Harness {
    param([Parameter(Mandatory)][string] $Code)
    $script:Report.status = 'blocked'
    $script:Report.blocked_code = $Code
    Save-Receipt
    Write-Error $Code
    exit 2
}

function Get-Sha256 {
    param([Parameter(Mandatory)][string] $Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Assert-SafeAbsolutePath {
    param(
        [Parameter(Mandatory)][string] $Path,
        [switch] $MustExist
    )
    if ([string]::IsNullOrWhiteSpace($Path)) { Stop-Harness 'EMPTY_PATH' }
    try { $full = [System.IO.Path]::GetFullPath($Path) }
    catch { Stop-Harness 'INVALID_ABSOLUTE_PATH' }
    if ($MustExist -and -not (Test-Path -LiteralPath $full)) { Stop-Harness 'REQUIRED_PATH_MISSING' }
    return $full
}

function New-PrivateDirectory {
    param([Parameter(Mandatory)][string] $Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        if ($IsWindows) {
            $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
            $security = [Security.AccessControl.DirectorySecurity]::new()
            $security.SetAccessRuleProtection($true, $false)
            $security.SetOwner($identity.User)
            $rule = [Security.AccessControl.FileSystemAccessRule]::new(
                $identity.User, [Security.AccessControl.FileSystemRights]::FullControl,
                ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit),
                [Security.AccessControl.PropagationFlags]::None, [Security.AccessControl.AccessControlType]::Allow
            )
            [void]$security.AddAccessRule($rule)
            [void][IO.FileSystemAclExtensions]::CreateDirectory($security, $Path)
        } else {
            [void][IO.Directory]::CreateDirectory($Path, ([IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite -bor [IO.UnixFileMode]::UserExecute))
        }
    }
    return $Path
}

function Read-JsonFile {
    param([Parameter(Mandatory)][string] $Path)
    return (Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable -Depth 32)
}

function Write-PrivateJson {
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][AllowEmptyCollection()] $Value
    )
    $json = ConvertTo-Json -InputObject $Value -Depth 32 -Compress
    [IO.File]::WriteAllText($Path, $json, [Text.UTF8Encoding]::new($false))
}

function Get-StrictInt64 {
    param($Value)
    # Only a genuine integral value is valid. A nonintegral, fractional, composite or
    # non-numeric value is reported invalid rather than accepted as a valid null.
    if ($null -eq $Value -or $Value -is [bool] -or $Value -is [double] -or
        $Value -is [decimal] -or $Value -is [single] -or $Value -is [char]) {
        return [pscustomobject]@{ valid = $false; value = $null }
    }
    $typeCode = [System.Type]::GetTypeCode($Value.GetType())
    if ($typeCode -notin @(
        [System.TypeCode]::SByte, [System.TypeCode]::Byte,
        [System.TypeCode]::Int16, [System.TypeCode]::UInt16,
        [System.TypeCode]::Int32, [System.TypeCode]::UInt32,
        [System.TypeCode]::Int64, [System.TypeCode]::UInt64
    )) { return [pscustomobject]@{ valid = $false; value = $null } }
    try { return [pscustomobject]@{ valid = $true; value = [long]$Value } }
    catch { return [pscustomobject]@{ valid = $false; value = $null } }
}

function Get-ControllerId {
    param($Value)
    $parsed = [Guid]::Empty
    if ($Value -isnot [string] -or
        -not [Guid]::TryParseExact($Value, 'D', [ref]$parsed) -or
        $parsed -eq [Guid]::Empty -or $parsed.ToString('D') -cne $Value) {
        return [pscustomobject]@{ valid = $false; value = $null }
    }
    return [pscustomobject]@{ valid = $true; value = $Value }
}

function Add-SubgateResult {
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][ValidateSet('verified', 'not_verified', 'blocked', 'unknown')][string] $Status,
        [string] $Code = $null,
        [System.Collections.IDictionary] $Evidence = $null
    )
    if ($null -eq $Evidence) { $Evidence = [ordered]@{} }
    $script:SubgateResults.Add([ordered]@{
        name = $Name
        status = $Status
        code = $Code
        evidence = $Evidence
        recorded_at_utc = [DateTime]::UtcNow.ToString('o')
    })
    $script:Report.subgates[$Name] = $Status
    $script:Report.subgate_evidence = @($script:SubgateResults.ToArray())
    Save-Receipt
}

function Record-QReceipt {
    param(
        [Parameter(Mandatory)][string] $Kind,
        [Parameter(Mandatory)][string] $Sha256,
        [Parameter(Mandatory)][string] $Path
    )
    $script:QReceipts.Add([ordered]@{
        kind = $Kind
        sha256 = $Sha256
        path = $Path
        recorded_at_utc = [DateTime]::UtcNow.ToString('o')
    })
    $script:Report.q_receipts = @($script:QReceipts.ToArray())
    Save-Receipt
}

function Save-Receipt {
    if ($null -eq $script:RunDirectory) { return }
    $path = Join-Path $script:RunDirectory 'qualification-receipt.json'
    Write-PrivateJson -Path $path -Value $script:Report
    $script:ReceiptPath = $path
}



<# ==========================================================================================
 Bounded process management. A started process is either owned and tracked here, or
 explicitly reported as pending ownership. Nothing is silently leaked.
========================================================================================== #>
function Stop-OwnedPublicCliProcess {
    param(
        [Parameter(Mandatory)] $Process,
        [ValidateRange(0, 5000)][int] $WaitMilliseconds = 5000
    )
    $processId = $null
    try { $processId = [int]$Process.Id } catch { }
    $stopped = $false
    try { $stopped = $Process.WaitForExit(0) } catch { }
    if (-not $stopped) {
        # Kill() without the process-tree overload is restricted to this owned CLI.
        try { $Process.Kill() } catch { }
        try { $stopped = $Process.WaitForExit($WaitMilliseconds) } catch { $stopped = $false }
    }
    if ($stopped) {
        try { $Process.Dispose() } catch { }
        return [pscustomobject]@{ stopped = $true; process_id = $processId; pending_pid = $null; process = $null }
    }
    return [pscustomobject]@{ stopped = $false; process_id = $processId; pending_pid = $processId; process = $Process }
}

function Start-BoundedProcess {
    param(
        [Parameter(Mandatory)][object[]] $Arguments,
        [Parameter(Mandatory)][int] $TimeoutMilliseconds,
        [Parameter(Mandatory)][string] $ExecutablePath,
        [switch] $LongLived
    )
    $result = [pscustomobject]@{
        started = $false; completed = $false; exit_code = $null; error_code = $null
        stdout = ''; stderr = ''; process = $null; process_id = $null
        stdout_task = $null; stderr_task = $null
        cleanup_confirmed = $false; cleanup_pending = $false
    }
    $psi = [Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = $ExecutablePath
    foreach ($argument in @($Arguments)) {
        if ($null -eq $argument) { continue }
        $text = [string]$argument
        if ($text.Length -gt 4096) {
            $result.error_code = 'ARGUMENT_TOO_LONG'
            return $result
        }
        # No manual quoting. With UseShellExecute false, ProcessStartInfo.ArgumentList
        # encodes each argument itself, so pre-embedded double quotes reach the child
        # as literal characters and corrupt any path containing a space.
        $psi.ArgumentList.Add($text)
    }
    $psi.UseShellExecute = $false
    # The owned long-lived host is stopped by closing its stdin, so stdin is redirected
    # only for that case. CreateNoWindow meets the hidden-window requirement; this
    # helper starts processes through ProcessStartInfo and never calls Start-Process.
    $psi.RedirectStandardInput = [bool]$LongLived
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $psi
    try { $null = $process.Start() }
    catch {
        $result.error_code = 'PROCESS_START_FAILED'
        try { $process.Dispose() } catch { }
        return $result
    }
    try {
        $result.started = $true
        $result.process_id = [int]$process.Id
        $result.stdout_task = $process.StandardOutput.ReadToEndAsync()
        $result.stderr_task = $process.StandardError.ReadToEndAsync()
        if ($LongLived) {
            $result.process = $process
            return $result
        }
        $callClock = [Diagnostics.Stopwatch]::StartNew()
        $completed = $process.WaitForExit($TimeoutMilliseconds)
        if (-not $completed) {
            $result.error_code = 'CLI_TIMEOUT_OUTCOME_UNKNOWN'
            return $result
        }
        $drainMilliseconds = [int][Math]::Max(0, [Math]::Min(5000, $TimeoutMilliseconds - $callClock.Elapsed.TotalMilliseconds))
        if (-not [Threading.Tasks.Task]::WaitAll(@($result.stdout_task, $result.stderr_task), $drainMilliseconds)) {
            $result.error_code = 'CLI_OUTPUT_DRAIN_PENDING'
            return $result
        }
        $result.completed = $true
        $result.exit_code = $process.ExitCode
        $result.stdout = $result.stdout_task.Result
        $result.stderr = $result.stderr_task.Result
        if ($process.ExitCode -ne 0) { $result.error_code = 'CLI_NONZERO_EXIT' }
        return $result
    }
    catch {
        $result.error_code = 'PROCESS_IO_FAILED'
        return $result
    }
    finally {
        if (-not $LongLived) {
            $stop = $null
            try { $stop = Stop-OwnedPublicCliProcess -Process $process -WaitMilliseconds 5000 }
            catch {
                $pendingPid = $null
                try { $pendingPid = [int]$process.Id } catch { }
                $stop = [pscustomobject]@{ stopped = $false; process_id = $pendingPid; pending_pid = $pendingPid; process = $process }
            }
            $result.cleanup_confirmed = $stop.stopped
            if ($stop.stopped) {
                $result.process = $null
            }
            else {
                $result.cleanup_pending = $true
                $result.process = $stop.process
                if ($null -ne $stop.process_id) { $result.process_id = $stop.process_id }
            }
        }
    }
}

function Wait-OwnedProcessOutput {
    param($Process, $StdoutTask, $StderrTask)
    if ($null -eq $Process -or $null -eq $StdoutTask -or $null -eq $StderrTask) {
        return [pscustomobject]@{ stdout = ''; stderr = ''; complete = $false }
    }
    $done = $false
    try { $done = [Threading.Tasks.Task]::WaitAll(@($StdoutTask, $StderrTask), 0) } catch { $done = $false }
    if ($done) {
        return [pscustomobject]@{
            stdout = [string]$StdoutTask.Result
            stderr = [string]$StderrTask.Result
            complete = $true
        }
    }
    return [pscustomobject]@{ stdout = ''; stderr = ''; complete = $false }
}

function Stop-OwnedHost {
    param($Scenario)
    if ($null -eq $Scenario -or $null -eq $Scenario.host) { return [pscustomobject]@{ stopped = $true } }
    $process = $Scenario.host
    $stopped = $false
    $lastPid = $null
    try { $lastPid = [int]$process.Id } catch { }
    try {
        if (-not $process.HasExited) {
            $process.StandardInput.Close()
            $stopped = $process.WaitForExit([Math]::Min(30000, $TimeoutSeconds * 1000))
        }
        else { $stopped = $true }
    }
    catch {
        return [pscustomobject]@{ stopped = $false; pending_pid = $lastPid; note = 'host exit read failed' }
    }
    if (-not $stopped) { return [pscustomobject]@{ stopped = $false; pending_pid = $lastPid } }
    $Scenario.host = $null
    try { $process.Dispose() } catch { }
    return [pscustomobject]@{ stopped = $true; pending_pid = $null }
}


<# ==========================================================================================
 Public Manager surface. This harness only uses documented public methods and the
 public CLI. It never opens a private credential or issues a service call.
========================================================================================== #>
function Invoke-PublicCall {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][string] $CredentialPath,
        [Parameter(Mandatory)][string] $Method,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Params,
        [ValidateRange(1, 60000)][int] $TimeoutMilliseconds = 60000
    )
    $requestPath = Join-Path $Scenario.requests ([Guid]::NewGuid().ToString('N') + '.json')
    Write-PrivateJson -Path $requestPath -Value $Params
    $arguments = @(
        '--config', $script:ConfigPath,
        '--data-dir', $Scenario.state,
        '--credential', $CredentialPath,
        'call', $Method,
        '--file', $requestPath
    )
    $callTimeoutMilliseconds = [int][Math]::Min($TimeoutMilliseconds, [Math]::Min(60000, $TimeoutSeconds * 1000))
    $result = Start-BoundedProcess -Arguments $arguments -ExecutablePath $script:PublicCliPath -TimeoutMilliseconds $callTimeoutMilliseconds
    if ($result.cleanup_pending) { $Scenario.pending_public_cli_processes.Add($result) }
    if (-not $result.started) {
        return [pscustomobject]@{ success = $false; code = $(if ($result.error_code) { $result.error_code } else { 'CLI_START_FAILED' }) }
    }
    if (-not $result.completed) {
        $code = if ($result.error_code) { $result.error_code } else { 'CLI_TIMEOUT_OUTCOME_UNKNOWN' }
        return [pscustomobject]@{
            success = $false
            code = $code
            cleanup_pending = [bool]$result.cleanup_pending
            pending_pid = $(if ($result.cleanup_pending) { $result.process_id } else { $null })
        }
    }
    if ($result.exit_code -ne 0) {
        return [pscustomobject]@{ success = $false; code = 'CLI_NONZERO_EXIT'; stderr = $result.stderr }
    }
    if ([string]::IsNullOrWhiteSpace($result.stdout)) { return [pscustomobject]@{ success = $false; code = 'CLI_EMPTY_OUTPUT' } }
    if ($result.stdout.Length -gt $script:MaxCliOutputCharacters) {
        return [pscustomobject]@{ success = $false; code = 'CLI_OUTPUT_OVERFLOW' }
    }
    $parsed = $null
    try { $parsed = $result.stdout | ConvertFrom-Json -AsHashtable -Depth 32 } catch { return [pscustomobject]@{ success = $false; code = 'CLI_OUTPUT_NOT_JSON' } }
    if ($parsed -isnot [System.Collections.IDictionary]) { return [pscustomobject]@{ success = $false; code = 'CLI_OUTPUT_NOT_OBJECT' } }
    # CLI call prints the application value directly. Its nested result belongs
    # to the Operation and must remain attached to its method and identity.
    if ($parsed.Contains('code') -and $parsed['code'] -is [string] -and
        [string]$parsed['code'] -match '\A[A-Z0-9_]{1,64}\z' -and -not $parsed.Contains('operation_id')) {
        return [pscustomobject]@{ success = $false; code = [string]$parsed['code'] }
    }
    return [pscustomobject]@{ success = $true; code = $null; value = $parsed }
}

function New-R48ScenarioDirectory {
    param(
        [Parameter(Mandatory)][string] $Name,
        [string] $SharedState = $null
    )
    $path = Join-Path $script:RunDirectory ($Name + '-' + [Guid]::NewGuid().ToString('N'))
    # Both phases must run against one Store. A private per-phase state directory would
    # give the final host a different database and a different operator credential, so
    # the seed identities created in phase 1 would be invisible in phase 2.
    if ([string]::IsNullOrWhiteSpace($SharedState)) {
        $state = Join-Path $path 'state'
    }
    else {
        $state = $SharedState
    }
    $requests = Join-Path $path 'requests'
    $null = New-PrivateDirectory -Path $path
    $null = New-PrivateDirectory -Path $state
    $null = New-PrivateDirectory -Path $requests
    return [pscustomobject]@{
        name = $Name
        path = $path
        state = $state
        requests = $requests
        host = $null
        stdout_task = $null
        stderr_task = $null
        pending_public_cli_processes = [System.Collections.Generic.List[object]]::new()
        pipe_name = $null
        operator_path = (Join-Path $state 'operator.json')
        manager_path = (Join-Path $path 'manager.json')
    }
}


<#
 Derive the named pipe the host bound, mirroring swarm_client::endpoint::ipc_endpoint:
 SHA-256 over the canonicalized, lowercased data-directory path, first 16 bytes,
 hex, prefixed with eliot-swarm-. Used only to explain the transport boundary and
 detect an unexpected pipe; it never grants any access.
#>
function Get-PipeNameForDataDirectory {
    param([Parameter(Mandatory)][string] $DataDirectory)
    $full = [System.IO.Path]::GetFullPath($DataDirectory)
    if (-not (Test-Path -LiteralPath $full)) { return $null }
    $canonical = $full
    try {
        $item = Get-Item -LiteralPath $full -Force
        if ($null -ne $item.Target -and @($item.Target).Count -gt 0) { return $null }
        $canonical = $item.FullName
    }
    catch { return $null }
    $bytes = [Text.Encoding]::UTF8.GetBytes($canonical.ToLowerInvariant())
    $sha = [Security.Cryptography.SHA256]::HashData($bytes)
    $suffix = -join ($sha[0..15] | ForEach-Object { '{0:x2}' -f $_ })
    return "$script:AutomationPipePrefix$suffix"
}

function Start-R48Host {
    param([Parameter(Mandatory)] $Scenario, [switch] $QualificationFault)
    $arguments = @('--config', $script:ConfigPath, '--data-dir', $Scenario.state, 'host', '--stop-on-stdin-eof')
    if ($QualificationFault) { $arguments += '--qualification-drop-first-admit-reply' }
    $result = Start-BoundedProcess -Arguments $arguments -ExecutablePath $script:HostPath -TimeoutMilliseconds 0 -LongLived
    if (-not $result.started) { return [pscustomobject]@{ ready = $false; code = $result.error_code } }
    $Scenario.host = $result.process
    $Scenario.stdout_task = $result.stdout_task
    $Scenario.stderr_task = $result.stderr_task
    $deadline = [DateTime]::UtcNow.AddSeconds([Math]::Min(60, $TimeoutSeconds))
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($Scenario.host.HasExited) {
            return [pscustomobject]@{ ready = $false; code = 'HOST_EXITED_DURING_START' }
        }
        if (Test-Path -LiteralPath $Scenario.operator_path) {
            $status = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.operator_path -Method 'host.status' -Params ([ordered]@{})
            if ($status.success -and $null -ne $status.value) {
                $pipe = Get-PipeNameForDataDirectory -DataDirectory $Scenario.state
                $Scenario.pipe_name = $pipe
                $controllerId = Get-ControllerId -Value $status.value['controller_id']
                $epoch = Get-StrictInt64 -Value $status.value['host_lifecycle']['current']['host_epoch']
                if (-not $controllerId.valid -or -not $epoch.valid -or $epoch.value -le 0) { continue }
                return [pscustomobject]@{
                    ready = $true
                    code = $null
                    pid = [int]$Scenario.host.Id
                    controller_id = $(if ($controllerId.valid) { $controllerId.value } else { $null })
                    epoch = $(if ($epoch.valid) { $epoch.value } else { $null })
                    pipe_name = $pipe
                }
            }
        }
        Start-Sleep -Milliseconds 250
    }
    return [pscustomobject]@{ ready = $false; code = 'HOST_READY_TIMEOUT'; pid = [int]$Scenario.host.Id }
}

function New-R48Manager {
    param([Parameter(Mandatory)] $Scenario)
    $managerId = 'r48-' + [Guid]::NewGuid().ToString('N')
    $arguments = @(
        '--config', $script:ConfigPath,
        '--data-dir', $Scenario.state,
        '--credential', $Scenario.operator_path,
        'client-create', $managerId, '--role', 'manager', '--out', $Scenario.manager_path
    )
    $create = Start-BoundedProcess -Arguments $arguments -ExecutablePath $script:PublicCliPath -TimeoutMilliseconds 30000
    if ($create.cleanup_pending) { $Scenario.pending_public_cli_processes.Add($create) }
    if (-not $create.completed -or $create.exit_code -ne 0 -or -not (Test-Path -LiteralPath $Scenario.manager_path)) {
        return [pscustomobject]@{
            success = $false
            code = $(if ($create.error_code) { $create.error_code } else { 'MANAGER_CREATE_FAILED' })
            detail = $create.error_code
            cleanup_pending = [bool]$create.cleanup_pending
            pending_pid = $(if ($create.cleanup_pending) { $create.process_id } else { $null })
        }
    }
    $credential = Read-JsonFile -Path $Scenario.manager_path
    if ($credential['client_id'] -cne $managerId) { return [pscustomobject]@{ success = $false; code = 'MANAGER_CREDENTIAL_INVALID' } }
    $handover = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.operator_path -Method 'gm.handover' -Params ([ordered]@{
        client_request_id = 'r48-gm:' + [Guid]::NewGuid().ToString('N')
        client_id = $managerId
    })
    if (-not $handover.success) { return [pscustomobject]@{ success = $false; code = 'GM_DESIGNATION_FAILED'; detail = $handover.code } }
    $status = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'host.status' -Params ([ordered]@{})
    if (-not $status.success) { return [pscustomobject]@{ success = $false; code = 'HOST_STATUS_READBACK_FAILED'; detail = $status.code } }
    $gm = $status.value['gm']
    if ($gm -isnot [System.Collections.IDictionary] -or [string]$gm['state'] -cne 'current' -or
        [string]$gm['client_id'] -cne $managerId) {
        return [pscustomobject]@{ success = $false; code = 'GM_DESIGNATION_READBACK_MISMATCH' }
    }
    return [pscustomobject]@{
        success = $true
        code = $null
        id = $managerId
        path = $Scenario.manager_path
        credential = $credential
    }
}


<# ==========================================================================================
  Run-local controller configuration.

  The operator template supplies trusted project, checks and IPC settings. This harness
  never parses or re-serializes it: no TOML parser is available here, and hand-editing TOML
  would risk corrupting an operator-owned artifact. The template bytes are preserved
  verbatim and the canonical sections this run owns are appended to fresh run-local files
  under the run directory.

  A template that already declares an owned scheduler or schedule section would make an
  appended section ambiguous, so that is refused instead of guessed.
 ========================================================================================== #>
function Assert-TemplateIsSchedulerFree {
    param([Parameter(Mandatory)][string] $Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return [pscustomobject]@{ ok = $false; code = 'TEMPLATE_MISSING' }
    }
    $bytes = [IO.File]::ReadAllBytes($Path)
    if ($bytes.Length -gt 1MB) {
        return [pscustomobject]@{ ok = $false; code = 'TEMPLATE_TOO_LARGE' }
    }
    # The template is read once for the owned-section refusal only. Its bytes are never
    # rewritten and it is never used as a schedule or scheduler source of truth.
    $text = [Text.Encoding]::UTF8.GetString($bytes)
    if ($text -match '(?m)^[ \t]*\[[ \t]*automation_scheduler[ \t]*\][ \t]*$') {
        return [pscustomobject]@{ ok = $false; code = 'TEMPLATE_DECLARES_OWNED_SECTION'; section = 'automation_scheduler' }
    }
    if ($text -match '(?m)^[ \t]*\[\[[ \t]*schedules[ \t]*\]\][ \t]*$') {
        return [pscustomobject]@{ ok = $false; code = 'TEMPLATE_DECLARES_OWNED_SECTION'; section = 'schedules' }
    }
    return [pscustomobject]@{ ok = $true; code = $null; template_bytes = $bytes.Length }
}

function Write-BoundedTextFile {
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][AllowEmptyString()][string] $Content
    )
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($Content)
    if ($bytes.Length -gt 1MB) { throw 'RUN_CONFIG_TOO_LARGE' }
    $stream = [IO.File]::Open($Path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try { $stream.Write($bytes, 0, $bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
    return $path
}
function Get-TomlLiteralString {
    param([Parameter(Mandatory)][AllowEmptyString()][string] $Value)
    <#
      A TOML single-quoted literal string never interprets an escape. The only character
      it cannot carry verbatim is the single quote itself, so a value containing one is
      refused rather than re-quoted into a different literal or left ambiguous.
    #>
    if ($Value.Contains("'")) { return $null }
    return $Value
}

function Write-R48RunConfig {
    param(
        [Parameter(Mandatory)][string] $TemplatePath,
        [Parameter(Mandatory)][string] $DestinationPath,
        [Parameter(Mandatory)][string] $StateDirectory,
        [Parameter(Mandatory)][string] $ScheduleId,
        [Parameter(Mandatory)][bool] $SchedulerEnabled,
        [System.Nullable[long]] $AnchorMs = $null,
        [string] $AttemptId = $null,
        [System.Nullable[long]] $ExpectedTaskRevision = $null,
        [string] $CandidateRef = $null,
        [string] $ProfileId = $null,
        [string] $ProfileRevision = $null
    )
    $guard = Assert-TemplateIsSchedulerFree -Path $TemplatePath
    if (-not $guard.ok) {
        return [pscustomobject]@{ ok = $false; code = $guard.code; section = $guard.section }
    }
    # No template line is edited and no table is re-serialized. The Store location is
    # already redirected by the --data-dir flag the host is started with, so this
    # helper appends only the sections this run actually owns.
    $template = [Text.Encoding]::UTF8.GetString([IO.File]::ReadAllBytes($TemplatePath))
    if ($template.Length -gt 0 -and $template[-1] -ne [char]10) { $template += [char]10 }
    $separator = [string][char]10
    $owned = [System.Collections.Generic.List[string]]::new()
    $owned.Add('')
    if ($SchedulerEnabled) {
        $owned.Add('# ---- R48 run-local owned sections (final: seeded one-shot CheckRun) ----')
    } else {
        $owned.Add('# ---- R48 run-local owned sections (bootstrap: scheduler disabled) ----')
    }
    $owned.Add('')
    $owned.Add('[automation_scheduler]')
    if ($SchedulerEnabled) {
        $owned.Add('enabled = true')
    } else {
        $owned.Add('enabled = false')
    }
    if (-not $SchedulerEnabled) {
        $content = $template + (($owned -join $separator) + $separator)
        $null = Write-BoundedTextFile -Path $DestinationPath -Content $content
        return [pscustomobject]@{ ok = $true; code = $null; config_path = $DestinationPath; scheduler_enabled = $false; schedule_count = 0 }
    }
    if ($null -eq $AnchorMs -or $AnchorMs -lt 0) {
        return [pscustomobject]@{ ok = $false; code = 'ANCHOR_INVALID' }
    }
    $scheduleLiteral = Get-TomlLiteralString -Value ([string]$ScheduleId)
    $attemptLiteral = Get-TomlLiteralString -Value ([string]$AttemptId)
    $candidateLiteral = Get-TomlLiteralString -Value ([string]$CandidateRef)
    $profileLiteral = Get-TomlLiteralString -Value ([string]$ProfileId)
    $revisionLiteral = Get-TomlLiteralString -Value ([string]$ProfileRevision)
    if ($null -in @($scheduleLiteral, $attemptLiteral, $candidateLiteral, $profileLiteral, $revisionLiteral)) {
        return [pscustomobject]@{ ok = $false; code = 'SEED_IDENTITY_NOT_LITERAL_SAFE' }
    }
    if ($scheduleLiteral -cnotmatch '\A[a-z0-9_-]{1,64}\z') {
        return [pscustomobject]@{ ok = $false; code = 'SCHEDULE_ID_SHAPE_INVALID' }
    }
    if ($null -eq $ExpectedTaskRevision -or $ExpectedTaskRevision -lt 1) {
        return [pscustomobject]@{ ok = $false; code = 'TASK_REVISION_INVALID' }
    }
    # The pinned one-shot CheckRun. period_ms is absent, which is how the host decodes a
    # one-shot schedule, and identity fields come only from the real seed result.
    $owned.Add('')
    $owned.Add('[[schedules]]')
    $owned.Add("schedule_id = '$scheduleLiteral'")
    $owned.Add('enabled = true')
    $owned.Add(('anchor_ms = ' + [string][long]$AnchorMs))
    $owned.Add('[schedules.action]')
    $owned.Add("kind = 'check_run'")
    $owned.Add("attempt_id = '$attemptLiteral'")
    $owned.Add(('expected_task_revision = ' + [string][long]$ExpectedTaskRevision))
    $owned.Add("candidate_ref = '$candidateLiteral'")
    $owned.Add("profile_id = '$profileLiteral'")
    $owned.Add("profile_revision = '$revisionLiteral'")
    $content = $template + (($owned -join $separator) + $separator)
    $null = Write-BoundedTextFile -Path $DestinationPath -Content $content
    return [pscustomobject]@{
        ok = $true
        code = $null
        config_path = $DestinationPath
        scheduler_enabled = $true
        schedule_count = 1
        schedule_id = $ScheduleId
        anchor_ms = [long]$AnchorMs
        attempt_id = $AttemptId
        expected_task_revision = [long]$ExpectedTaskRevision
        candidate_ref = $CandidateRef
        profile_id = $ProfileId
        profile_revision = $ProfileRevision
    }
}

<# ==========================================================================================
 Subgate 1: scheduler current-owner readiness.
 This is a pure public readback. Readiness means an active owner at the exact current
 service and host scope for the current host epoch, with the supervisor running.
 A supervisor state of running alone is not readiness and is not OS host liveness.
========================================================================================== #>
function Assert-SchedulerOwnerReady {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][string] $ExpectedControllerId,
        [Parameter(Mandatory)][long] $ExpectedHostEpoch,
        [Parameter(Mandatory)][int] $PollSeconds
    )
    $deadline = [DateTime]::UtcNow.AddSeconds($PollSeconds)
    $lastCode = 'OWNER_READINESS_TIMEOUT'
    $observations = [System.Collections.Generic.List[object]]::new()
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($null -eq $Scenario.host -or $Scenario.host.HasExited) {
            return [pscustomobject]@{ success = $false; code = 'HOST_PROCESS_CHANGED_OR_EXITED' }
        }
        $call = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'host.status' -Params ([ordered]@{})
        if (-not $call.success) { $lastCode = 'HOST_STATUS_READBACK_FAILED'; Start-Sleep -Milliseconds 500; continue }
        $status = $call.value
        $worker = $status['host_lifecycle']['optional_workers']['automation-scheduler']
        $owner = if ($worker -is [System.Collections.IDictionary]) { $worker['owner'] } else { $null }
        if ($null -eq $owner -or $owner -isnot [System.Collections.IDictionary]) {
            $lastCode = 'OWNER_PROJECTION_MISSING'
            Start-Sleep -Milliseconds 500
            continue
        }
        $controllerId = Get-ControllerId -Value $status['controller_id']
        $currentEpoch = Get-StrictInt64 -Value $status['host_lifecycle']['current']['host_epoch']
        $ownerControllerId = Get-ControllerId -Value $owner['host_scope']['controller_id']
        $ownerEpoch = Get-StrictInt64 -Value $owner['host_scope']['host_epoch']
        $observations.Add([ordered]@{
            observed_at_utc = [DateTime]::UtcNow.ToString('o')
            worker_state = if ($worker -is [System.Collections.IDictionary]) { [string]$worker['state'] } else { 'unknown' }
            owner_status = [string]$owner['status']
            owner_phase = [string]$owner['phase']
            owner_ready = if ($owner['ready'] -is [bool]) { [bool]$owner['ready'] } else { $null }
            readiness_source = [string]$owner['readiness_source']
            group_identity_present = ($owner['group_identity'] -is [System.Collections.IDictionary])
            owner_host_scope = [ordered]@{
                controller_id = $(if ($ownerControllerId.valid) { $ownerControllerId.value } else { $null })
                host_epoch = $(if ($ownerEpoch.valid) { $ownerEpoch.value } else { $null })
            }
            current_controller_id = $(if ($controllerId.valid) { $controllerId.value } else { $null })
            current_host_epoch = $(if ($currentEpoch.valid) { $currentEpoch.value } else { $null })
        })
        $problems = @()
        if ($worker -isnot [System.Collections.IDictionary] -or [string]$worker['state'] -cne 'running') { $problems += 'worker_not_running' }
        if ([string]$owner['status'] -cne 'current') { $problems += 'owner_not_current' }
        if ([string]$owner['phase'] -cne 'active') { $problems += 'owner_not_active' }
        if ($owner['ready'] -isnot [bool] -or -not [bool]$owner['ready']) { $problems += 'owner_not_ready' }
        if ([string]$owner['readiness_source'] -cne 'validated_current_owner_receipt') { $problems += 'readiness_source_unexpected' }
        if ($owner['group_identity'] -isnot [System.Collections.IDictionary]) { $problems += 'group_identity_missing' }
        if (-not $controllerId.valid -or -not $currentEpoch.valid) { $problems += 'current_identity_unreadable' }
        if (-not $ownerControllerId.valid -or -not $ownerEpoch.valid) { $problems += 'owner_scope_unreadable' }
        if ($problems.Count -eq 0) {
            if ($ownerControllerId.value -cne $controllerId.value -or $ownerEpoch.value -ne $currentEpoch.value) {
                $problems += 'owner_scope_not_current_host'
            }
        }
        if ($problems.Count -eq 0) {
            if ($controllerId.value -cne $ExpectedControllerId -or $currentEpoch.value -ne $ExpectedHostEpoch) {
                return [pscustomobject]@{
                    success = $false
                    code = 'HOST_IDENTITY_CHANGED'
                    observations = @($observations.ToArray())
                }
            }
            return [pscustomobject]@{
                success = $true
                code = $null
                observations = @($observations.ToArray())
                ready_at_utc = [DateTime]::UtcNow.ToString('o')
            }
        }
        $lastCode = 'OWNER_READINESS_INCOMPLETE:' + ($problems -join ',')
        Start-Sleep -Milliseconds 500
    }
    return [pscustomobject]@{ success = $false; code = $lastCode; observations = @($observations.ToArray()) }
}

<# ==========================================================================================
 Subgate 2: one real, eligible, one-shot CheckRun.
 The static configuration already pins the live Attempt and candidate, so this reads
 the projected schedule, waits for the due slot, then reads the durable work receipt
 through operation.get. A far-future next_due_ms alone is not eligibility evidence.
========================================================================================== #>
function Get-ScheduleItem {
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $HostStatus,
        [Parameter(Mandatory)][string] $ScheduleId
    )
    $schedules = $HostStatus['schedules']
    if ($null -eq $schedules -or $schedules -isnot [System.Collections.IDictionary]) { return $null }
    $items = $schedules['items']
    if ($null -eq $items -or $items -isnot [System.Collections.IList]) { return $null }
    foreach ($item in $items) {
        if ($item -is [System.Collections.IDictionary] -and [string]$item['schedule_id'] -ceq $ScheduleId) { return $item }
    }
    return $null
}

function Assert-StaticScheduleShape {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][string] $ScheduleId,
        [Parameter(Mandatory)][string] $ExpectedControllerId,
        [Parameter(Mandatory)][long] $ExpectedHostEpoch
    )
    $call = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'host.status' -Params ([ordered]@{})
    if (-not $call.success) { return [pscustomobject]@{ success = $false; code = 'HOST_STATUS_READBACK_FAILED' } }
    $controllerId = Get-ControllerId -Value $call.value['controller_id']
    $epoch = Get-StrictInt64 -Value $call.value['host_epoch']
    if (-not $controllerId.valid -or $controllerId.value -cne $ExpectedControllerId) {
        return [pscustomobject]@{ success = $false; code = 'HOST_CONTROLLER_ID_CHANGED' }
    }
    if (-not $epoch.valid -or $epoch.value -ne $ExpectedHostEpoch) {
        return [pscustomobject]@{ success = $false; code = 'HOST_EPOCH_CHANGED' }
    }
    $item = Get-ScheduleItem -HostStatus $call.value -ScheduleId $ScheduleId
    if ($null -eq $item) { return [pscustomobject]@{ success = $false; code = 'SCHEDULE_NOT_PROJECTED' } }
    if ($item['enabled'] -isnot [bool] -or -not $item['enabled']) { return [pscustomobject]@{ success = $false; code = 'SCHEDULE_NOT_ENABLED' } }
    if ($item['identity_changed'] -is [bool] -and $item['identity_changed']) {
        return [pscustomobject]@{ success = $false; code = 'SCHEDULE_IDENTITY_CHANGED' }
    }
    # A one-shot schedule serializes with a null period. A present period means this
    # is not the one-shot contour this harness is qualified to assert.
    if ($null -ne $item['period_ms']) { return [pscustomobject]@{ success = $false; code = 'SCHEDULE_NOT_ONE_SHOT' } }
    $anchor = Get-StrictInt64 -Value $item['anchor_ms']
    if (-not $anchor.valid) { return [pscustomobject]@{ success = $false; code = 'SCHEDULE_ANCHOR_INVALID' } }
    if ($null -ne $item['last_operation'] -or $null -ne $item['last_considered_slot'] -or
        $null -ne $item['last_observed_due_slot']) {
        return [pscustomobject]@{ success = $false; code = 'SCHEDULE_ALREADY_ADMITTED' }
    }
    return [pscustomobject]@{ success = $true; code = $null; schedule = $item; anchor_ms = $anchor.value }
}

function Wait-ForDueCheckRunOperation {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][string] $ScheduleId,
        [Parameter(Mandatory)][long] $AnchorMs,
        [Parameter(Mandatory)][int] $PollSeconds,
        [Parameter(Mandatory)][int] $ExpectedHostPid,
        [Parameter(Mandatory)][long] $ExpectedHostEpoch
    )
    $anchorUtc = [DateTimeOffset]::FromUnixTimeMilliseconds($AnchorMs).UtcDateTime
    # The pinned due time is the anchor, so the wait window is measured from the
    # anchor. A budget measured from the start of the wait expires before the slot is
    # ever due and wrongly reports DUE_OPERATION_TIMEOUT.
    $deadline = $anchorUtc.AddSeconds($PollSeconds)
    $lastCode = 'DUE_OPERATION_TIMEOUT'
    $samples = [System.Collections.Generic.List[object]]::new()
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($null -eq $Scenario.host -or $Scenario.host.HasExited -or [int]$Scenario.host.Id -ne $ExpectedHostPid) {
            return [pscustomobject]@{ success = $false; code = 'HOST_PROCESS_CHANGED_OR_EXITED' }
        }
        $call = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'host.status' -Params ([ordered]@{})
        if (-not $call.success) { $lastCode = 'HOST_STATUS_READBACK_FAILED'; Start-Sleep -Milliseconds 500; continue }
        $epoch = Get-StrictInt64 -Value $call.value['host_lifecycle']['current']['host_epoch']
        if (-not $epoch.valid -or $epoch.value -ne $ExpectedHostEpoch) {
            return [pscustomobject]@{ success = $false; code = 'HOST_EPOCH_CHANGED' }
        }
        $item = Get-ScheduleItem -HostStatus $call.value -ScheduleId $ScheduleId
        if ($null -eq $item) { return [pscustomobject]@{ success = $false; code = 'SCHEDULE_NOT_PROJECTED' } }
        if ($null -ne $item['last_failure']) {
            return [pscustomobject]@{
                success = $false
                code = 'SCHEDULE_REPORTED_FAILURE'
                failure = $item['last_failure']
                samples = @($samples.ToArray())
            }
        }
        $lastOperation = $item['last_operation']
        if ($lastOperation -is [System.Collections.IDictionary]) {
            $workOperationId = [string]$lastOperation['work_operation_id']
            if (-not [string]::IsNullOrWhiteSpace($workOperationId)) {
                $operation = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'operation.get' -Params ([ordered]@{ operation_id = $workOperationId })
                if (-not $operation.success) {
                    return [pscustomobject]@{
                        success = $false
                        code = 'DURABLE_OPERATION_READBACK_FAILED'
                        detail = $operation.code
                    }
                }
                $checkId = [string]$operation.value['result']['check_id']
                if ([string]::IsNullOrWhiteSpace($checkId)) {
                    return [pscustomobject]@{ success = $false; code = 'CHECK_ID_MISSING' }
                }
                $check = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'check.get' -Params ([ordered]@{ check_id = $checkId })
                if (-not $check.success) {
                    return [pscustomobject]@{ success = $false; code = 'CHECK_RECEIPT_READBACK_FAILED'; detail = $check.code }
                }
                $samples.Add([ordered]@{
                    observed_at_utc = [DateTime]::UtcNow.ToString('o')
                    schedule_last_operation = $lastOperation
                    operation = $operation.value
                    check_receipt = $check.value
                })
                return [pscustomobject]@{
                    success = $true
                    code = $null
                    work_operation_id = $workOperationId
                    operation = $operation.value
                    check_receipt = $check.value
                    samples = @($samples.ToArray())
                }
            }
        }
        Start-Sleep -Milliseconds 1000
    }
    return [pscustomobject]@{ success = $false; code = $lastCode; samples = @($samples.ToArray()) }
}

function Assert-EligibleCheckRunReceipt {
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $Operation,
        [Parameter(Mandatory)][System.Collections.IDictionary] $CheckReceipt,
        [Parameter(Mandatory)][string] $AttemptId,
        [Parameter(Mandatory)][string] $CandidateRef,
        [Parameter(Mandatory)][string] $ProfileId,
        [Parameter(Mandatory)][string] $ProfileRevision
    )
    $problems = @()
    if ([string]$Operation['method'] -cne 'check.run') { $problems += 'method_not_check_run' }
    if ([string]::IsNullOrWhiteSpace([string]$Operation['operation_id']) -or
        [string]$CheckReceipt['operation_id'] -cne [string]$Operation['operation_id'] -or
        [string]::IsNullOrWhiteSpace([string]$CheckReceipt['check_id']) -or
        [string]$CheckReceipt['check_id'] -cne [string]$Operation['result']['check_id']) {
        $problems += 'check_operation_link_mismatch'
    }
    # Profile identity comes from check.get; operation.get intentionally omits it.
    $attemptValues = @()
    $candidateValues = @()
    $profileIdValues = @()
    $profileRevisionValues = @()
    foreach ($container in @($Operation, $Operation['result'], $CheckReceipt)) {
        if ($container -isnot [System.Collections.IDictionary]) { continue }
        if ($container.Contains('attempt_id')) { $attemptValues += [string]$container['attempt_id'] }
        if ($container.Contains('candidate_ref')) { $candidateValues += [string]$container['candidate_ref'] }
        if ($container.Contains('profile_id')) { $profileIdValues += [string]$container['profile_id'] }
        if ($container.Contains('profile_revision')) { $profileRevisionValues += [string]$container['profile_revision'] }
    }
    if ($attemptValues.Count -eq 0 -or ($attemptValues | Where-Object { $_ -cne $AttemptId })) { $problems += 'attempt_mismatch' }
    if ($candidateValues.Count -eq 0 -or ($candidateValues | Where-Object { $_ -cne $CandidateRef })) { $problems += 'candidate_mismatch' }
    if ($profileIdValues.Count -eq 0 -or ($profileIdValues | Where-Object { $_ -cne $ProfileId })) { $problems += 'profile_id_mismatch' }
    if ($profileRevisionValues.Count -eq 0 -or ($profileRevisionValues | Where-Object { $_ -cne $ProfileRevision })) { $problems += 'profile_revision_mismatch' }
    $state = [string]$Operation['state']
    if ([string]::IsNullOrWhiteSpace($state)) { $state = [string]$Operation['result']['state'] }
    # Check execution retains native_accepted after the real child launch; it is
    # still a durable admission, while unknown/rejected outcomes are not proof.
    if ($state -notin @('queued', 'sending', 'native_accepted', 'settled')) {
        $problems += 'operation_not_durably_queued_or_settled'
    }
    if ($problems.Count -gt 0) {
        return [pscustomobject]@{ success = $false; code = ('RECEIPT_MISMATCH:' + ($problems -join ',')) }
    }
    return [pscustomobject]@{ success = $true; code = $null; state = $state }
}
<#
 The scheduler worker is a host child created with piped stdout and a null stderr
 sink. There is currently no supported host surface that captures worker stderr
 into a readable file, so this helper reports that honestly instead of inventing a
 log. When a capture point exists, it is found here; otherwise the caller records
 the gap.
#>
<# ==========================================================================================
 Subgate 3: uncertain initial admit receipt with no replay.

 The evidence channel is a bounded create-only marker file written by the live
 Store-owned worker, because that launch pipes stdout only as far as the host
 readiness frame and sends stderr to a null sink. Worker logging is therefore
 unreachable by design, and a nonexistent host.status property is never claimed.

 The marker is created beside the exact private worker config the Store issued,
 in the same private service directory, and binds the live owner launch PID and
 the worker config digest. It carries no credential, no request parameters and
 no Store decision content: only the method whose reply was withheld and the
 length of the withheld frame.

 Validation requires the marker to agree with the independently observed live
 state: the recorded PID must be the automation-scheduler process of this run,
 and the recorded config digest must equal the digest of the worker config file
 on disk and the digest the Store projects for the current owner.
========================================================================================== #>
function Test-QualificationFaultWitness {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][int] $ExpectedHostPid,
        [Parameter(Mandatory)][long] $ExpectedHostEpoch,
        [Parameter(Mandatory)][string] $ExpectedControllerId,
        [ValidateRange(1, 60000)][int] $ReadbackTimeoutMilliseconds = 60000
    )
    $witnessPath = Join-Path $Scenario.state 'automation-scheduler-v1' 'qualification-fault-witness.json'
    if (-not (Test-Path -LiteralPath $witnessPath)) {
        return [pscustomobject]@{
            success = $false
            code = 'QUALIFICATION_WITNESS_ABSENT'
            note = 'no applied-fault witness was found beside the private worker config'
            witness_path = $witnessPath
        }
    }
    $raw = [IO.File]::ReadAllText($witnessPath)
    if ([Text.Encoding]::UTF8.GetByteCount($raw) -gt 512) {
        return [pscustomobject]@{
            success = $false
            code = 'QUALIFICATION_WITNESS_TOO_LARGE'
            note = 'a witness larger than its bound is refused rather than trusted'
        }
    }
    $witness = $null
    try { $witness = $raw | ConvertFrom-Json -AsHashtable -Depth 8 } catch {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_UNREADABLE' }
    }
    $keys = @('method','owner_config_sha256','owner_launch_pid','replay','schema','withheld_frame_bytes')
    if ($witness -isnot [System.Collections.IDictionary] -or
        (@($witness.Keys | Sort-Object) -join ',') -cne ($keys -join ',') -or
        $witness['schema'] -cne 'eliot.swarm.r48.qualification-fault-witness.v1' -or
        $witness['method'] -cne 'automation.scheduler.admit' -or
        $witness['replay'] -cne 'withheld_before_delivery') {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_SCHEMA_UNKNOWN' }
    }
    $frameLength = Get-StrictInt64 -Value $witness['withheld_frame_bytes']
    if (-not $frameLength.valid -or $frameLength.value -le 0 -or $frameLength.value -gt $script:MaxFrameBytes) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_FRAME_LENGTH_INVALID' }
    }

    # ---- Independent observation of the live Store-owned worker ----
    $status = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'host.status' -Params ([ordered]@{}) -TimeoutMilliseconds $ReadbackTimeoutMilliseconds
    if (-not $status.success) {
        return [pscustomobject]@{ success = $false; code = 'HOST_STATUS_READBACK_FAILED' }
    }
    $current = $status.value['host_lifecycle']['current']
    $epoch = Get-StrictInt64 -Value $current['host_epoch']
    if (-not $epoch.valid -or $epoch.value -ne $ExpectedHostEpoch) {
        return [pscustomobject]@{ success = $false; code = 'HOST_EPOCH_CHANGED' }
    }
    $controller = Get-ControllerId -Value $status.value['controller_id']
    $worker = $status.value['host_lifecycle']['optional_workers']['automation-scheduler']
    $owner = if ($worker -is [System.Collections.IDictionary]) { $worker['owner'] } else { $null }
    if (-not $controller.valid -or $controller.value -cne $ExpectedControllerId) {
        return [pscustomobject]@{ success = $false; code = 'HOST_IDENTITY_CHANGED' }
    }
    if ($worker -isnot [System.Collections.IDictionary] -or $worker['state'] -cne 'running' -or
        $owner -isnot [System.Collections.IDictionary] -or $owner['host_scope'] -isnot [System.Collections.IDictionary]) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_OWNER_SCOPE_INVALID' }
    }
    $ownerController = Get-ControllerId -Value $owner['host_scope']['controller_id']
    $ownerEpoch = Get-StrictInt64 -Value $owner['host_scope']['host_epoch']
    if (-not $ownerController.valid -or $ownerController.value -cne $ExpectedControllerId -or
        -not $ownerEpoch.valid -or $ownerEpoch.value -ne $ExpectedHostEpoch) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_OWNER_SCOPE_INVALID' }
    }

    # ---- Bind the witness to the observed live worker ----
    $recordedPid = Get-StrictInt64 -Value $witness['owner_launch_pid']
    if (-not $recordedPid.valid) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_PID_INVALID' }
    }
    # The authoritative worker PID is the one the Store validated for the current
    # owner, carried in the owner group identity. A host PID or a name match is
    # not accepted, because neither proves this is the Store-owned process.
    $ownerWorkerPid = [pscustomobject]@{ valid = $false; value = $null }
    if ($owner -is [System.Collections.IDictionary] -and $owner['group_identity'] -is [System.Collections.IDictionary]) {
        $ownerWorkerPid = Get-StrictInt64 -Value $owner['group_identity']['pid']
    }
    if (-not $ownerWorkerPid.valid -or $ownerWorkerPid.value -le 0) {
        return [pscustomobject]@{
            success = $false
            code = 'CURRENT_OWNER_WORKER_PID_UNAVAILABLE'
            note = 'the Store did not project a validated worker PID for the current owner'
        }
    }
    if ($recordedPid.value -ne $ownerWorkerPid.value) {
        return [pscustomobject]@{
            success = $false
            code = 'QUALIFICATION_WITNESS_PID_NOT_CURRENT_OWNER'
            recorded_pid = $recordedPid.value
            current_owner_worker_pid = $ownerWorkerPid.value
            observed_host_pid = $ExpectedHostPid
        }
    }
    if ($owner['status'] -cne 'current' -or $owner['phase'] -cne 'active' -or $owner['ready'] -isnot [bool] -or -not $owner['ready']) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_OWNER_NOT_READY' }
    }

    $recordedDigest = [string]$witness['owner_config_sha256']
    $configPath = Join-Path $Scenario.state 'automation-scheduler-v1' 'worker.json'
    if (-not (Test-Path -LiteralPath $configPath)) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_WITNESS_CONFIG_MISSING' }
    }
    $configDigestMatchesFile = $false
    $onDiskConfig = $null
    try {
        $onDiskConfig = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json -AsHashtable -Depth 8
    } catch { $onDiskConfig = $null }
    if ($onDiskConfig -is [System.Collections.IDictionary]) {
        $configDigestMatchesFile = ([string]$onDiskConfig['config_sha256'] -ceq $recordedDigest)
    }
    # A mismatch is a hard failure. The witness binds the exact private config
    # the Store issued, so a digest that does not agree with that file means the
    # witness belongs to a different launch and is not evidence for this one.
    if (-not $configDigestMatchesFile) {
        return [pscustomobject]@{
            success = $false
            code = 'QUALIFICATION_WITNESS_CONFIG_DIGEST_MISMATCH'
            recorded_digest = $recordedDigest
            on_disk_digest = $(if ($onDiskConfig -is [System.Collections.IDictionary]) { [string]$onDiskConfig['config_sha256'] } else { $null })
            config_path = $configPath
        }
    }

    return [pscustomobject]@{
        success = $true
        code = $null
        witness_schema = [string]$witness['schema']
        withheld_method = [string]$witness['method']
        withheld_replay = [string]$witness['replay']
        withheld_frame_bytes = $frameLength.value
        owner_launch_pid = $recordedPid.value
        pid_equals_current_owner_worker = $true
        current_owner_worker_pid = $ownerWorkerPid.value
        config_digest = $recordedDigest
        config_digest_matches_file = $configDigestMatchesFile
        owner_status = [string]$owner['status']
        owner_phase = [string]$owner['phase']
        owner_ready = if ($owner['ready'] -is [bool]) { [bool]$owner['ready'] } else { $null }
        epoch_unchanged = $true
        credential_or_payload_recorded = $false
    }
}

function Wait-ForQualificationFaultWitness {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][int] $ExpectedHostPid,
        [Parameter(Mandatory)][long] $ExpectedHostEpoch,
        [Parameter(Mandatory)][string] $ExpectedControllerId,
        [Parameter(Mandatory)][ValidateRange(1, 300)][int] $PollSeconds
    )
    $clock = [Diagnostics.Stopwatch]::StartNew()
    $lastAbsent = [pscustomobject]@{ success=$false; code='QUALIFICATION_WITNESS_ABSENT'; note='no applied-fault witness was observed within the bound' }
    while ($true) {
        $remaining = $PollSeconds * 1000 - $clock.Elapsed.TotalMilliseconds
        if ($remaining -lt 1) { return $lastAbsent }
        if ($null -eq $Scenario.host -or $Scenario.host.HasExited -or $Scenario.host.Id -ne $ExpectedHostPid) {
            return [pscustomobject]@{ success = $false; code = 'HOST_PROCESS_GONE'; note = 'the owned host exited before an applied-fault witness was observed' }
        }
        $readbackBudget = [int][Math]::Min(60000, [Math]::Floor($remaining))
        $result = Test-QualificationFaultWitness -Scenario $Scenario -ExpectedHostPid $ExpectedHostPid -ExpectedHostEpoch $ExpectedHostEpoch -ExpectedControllerId $ExpectedControllerId -ReadbackTimeoutMilliseconds $readbackBudget
        if ($clock.Elapsed.TotalMilliseconds -ge $PollSeconds * 1000 -and $result.success) {
            return [pscustomobject]@{ success=$false; code='QUALIFICATION_WITNESS_DEADLINE_EXCEEDED'; note='witness validation completed outside the observation window' }
        }
        # Retry only absence. A present but foreign or invalid witness fails.
        if ($result.success -or $result.code -cne 'QUALIFICATION_WITNESS_ABSENT') { return $result }
        $lastAbsent = $result
        $remaining = $PollSeconds * 1000 - $clock.Elapsed.TotalMilliseconds
        if ($remaining -lt 1) { return $lastAbsent }
        Start-Sleep -Milliseconds ([int][Math]::Min(200, [Math]::Floor($remaining)))
    }
}

function Test-QualificationAdmitAttempts {
    param([Parameter(Mandatory)] $Scenario, [Parameter(Mandatory)] $Injection)
    $directory = Join-Path $Scenario.state 'automation-scheduler-v1'
    if (Test-Path -LiteralPath (Join-Path $directory 'qualification-admit-attempt-2.json')) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_SECOND_ADMIT_ATTEMPT_OBSERVED' }
    }
    $path = Join-Path $directory 'qualification-admit-attempt-1.json'
    try {
        $bytes = [IO.File]::ReadAllBytes($path)
        if ($bytes.Length -gt 512) { throw 'oversize marker' }
        $marker = [Text.Encoding]::UTF8.GetString($bytes) | ConvertFrom-Json -AsHashtable -Depth 8
    } catch { return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_ADMIT_MARKER_UNREADABLE' } }
    $keys = @('method','owner_config_sha256','owner_launch_pid','schema','sequence')
    if ($marker -isnot [System.Collections.IDictionary] -or
        (@($marker.Keys | Sort-Object) -join ',') -cne ($keys -join ',') -or
        $marker['schema'] -cne 'eliot.swarm.r48.qualification-admit-attempt.v1' -or
        $marker['method'] -cne 'automation.scheduler.admit') {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_ADMIT_MARKER_INVALID' }
    }
    $sequence = Get-StrictInt64 -Value $marker['sequence']
    $pidValue = Get-StrictInt64 -Value $marker['owner_launch_pid']
    if (-not $sequence.valid -or $sequence.value -ne 1 -or -not $pidValue.valid -or
        $pidValue.value -ne $Injection.owner_launch_pid -or $marker['owner_config_sha256'] -cne $Injection.config_digest) {
        return [pscustomobject]@{ success = $false; code = 'QUALIFICATION_ADMIT_MARKER_SCOPE_MISMATCH' }
    }
    return [pscustomobject]@{ success = $true; code = $null; admission_intents = 1; second_intent_observed = $false; observation_end = 'confirmed_graceful_host_stop'; injection = $Injection }
}
<# ==========================================================================================
 Subgate 4: the host stays alive and keeps its identity.
========================================================================================== #>
function Assert-HostRemainsAlive {
    param(
        [Parameter(Mandatory)] $Scenario,
        [Parameter(Mandatory)][int] $ExpectedPid,
        [Parameter(Mandatory)][long] $ExpectedHostEpoch,
        [Parameter(Mandatory)][string] $ExpectedControllerId,
        [Parameter(Mandatory)][int] $Samples,
        [Parameter(Mandatory)][int] $IntervalSeconds
    )
    $observations = [System.Collections.Generic.List[object]]::new()
    $firstPid = $null
    $firstStart = $null
    $firstControllerId = $null
    $firstEpoch = $null
    for ($i = 0; $i -lt $Samples; $i++) {
        $observedPid = $null
        $startTime = $null
        try {
            $process = Get-Process -Id $ExpectedPid -ErrorAction Stop
            $observedPid = [int]$process.Id
            try { $startTime = $process.StartTime.ToUniversalTime() } catch { $startTime = $null }
        }
        catch { $observedPid = $null }
        $call = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'host.status' -Params ([ordered]@{})
        $lifecycleState = $null
        $controllerId = $null
        $epoch = $null
        if ($call.success -and $call.value -is [System.Collections.IDictionary]) {
            $current = $call.value['host_lifecycle']['current']
            $lifecycleState = [string]$current['state']
            $c = Get-ControllerId -Value $call.value['controller_id']
            $e = Get-StrictInt64 -Value $current['host_epoch']
            $controllerId = $(if ($c.valid) { $c.value } else { $null })
            $epoch = $(if ($e.valid) { $e.value } else { $null })
        }
        $observations.Add([ordered]@{
            sample_index = $i
            observed_at_utc = [DateTime]::UtcNow.ToString('o')
            pid = $observedPid
            process_start_time_utc = $(if ($null -ne $startTime) { $startTime.ToString('o') } else { $null })
            host_lifecycle_state = $lifecycleState
            controller_id = $controllerId
            host_epoch = $epoch
        })
        if ($null -eq $observedPid -or $Scenario.host.HasExited) {
            return [pscustomobject]@{ success = $false; code = 'HOST_PROCESS_GONE'; observations = @($observations.ToArray()) }
        }
        if ($null -eq $controllerId -or $null -eq $epoch -or $epoch -le 0) {
            return [pscustomobject]@{ success = $false; code = 'HOST_IDENTITY_UNREADABLE'; observations = @($observations.ToArray()) }
        }
        if ($null -eq $firstPid) {
            $firstPid = $observedPid
            $firstStart = $startTime
            $firstControllerId = $controllerId
            $firstEpoch = $epoch
        }
        else {
            if ($observedPid -ne $firstPid) { return [pscustomobject]@{ success = $false; code = 'HOST_PID_CHANGED'; observations = @($observations.ToArray()) } }
            if ($null -ne $startTime -and $null -ne $firstStart -and $startTime -ne $firstStart) {
                return [pscustomobject]@{ success = $false; code = 'HOST_START_TIME_CHANGED'; observations = @($observations.ToArray()) }
            }
            if ($controllerId -cne $firstControllerId) {
                return [pscustomobject]@{ success = $false; code = 'CONTROLLER_ID_CHANGED'; observations = @($observations.ToArray()) }
            }
            if ($null -ne $epoch -and $null -ne $firstEpoch -and $epoch -ne $firstEpoch) {
                return [pscustomobject]@{ success = $false; code = 'HOST_EPOCH_CHANGED'; observations = @($observations.ToArray()) }
            }
        }
        if ($lifecycleState -cne 'running') {
            return [pscustomobject]@{ success = $false; code = 'HOST_LIFECYCLE_NOT_RUNNING'; observations = @($observations.ToArray()) }
        }
        if ($controllerId -cne $ExpectedControllerId) {
            return [pscustomobject]@{ success = $false; code = 'CONTROLLER_ID_MISMATCH'; observations = @($observations.ToArray()) }
        }
        if ($null -ne $epoch -and $epoch -ne $ExpectedHostEpoch) {
            return [pscustomobject]@{ success = $false; code = 'HOST_EPOCH_MISMATCH'; observations = @($observations.ToArray()) }
        }
        if ($i -lt $Samples - 1) { Start-Sleep -Seconds $IntervalSeconds }
    }
    return [pscustomobject]@{ success = $true; code = $null; observations = @($observations.ToArray()) }
}


<# ==========================================================================================
 Automation worker installation. Q-candidate package only. A worker inherited from
 any earlier run is refused rather than reused.
========================================================================================== #>
function Install-QAutomationWorker {
    $destination = Join-Path ([System.IO.Path]::GetDirectoryName($script:HostPath)) $script:WorkerExecutableName
    $script:AutomationWorkerInstalledPath = $destination
    if (Test-Path -LiteralPath $destination) {
        $installedHash = $null
        try { $installedHash = Get-Sha256 -Path $destination } catch { $installedHash = $null }
        if ($installedHash -cne $ExpectedAutomationWorkerSha256.ToLowerInvariant()) {
            return [pscustomobject]@{
                success = $false
                code = 'AUTOMATION_WORKER_ALREADY_INSTALLED_FROM_ANOTHER_RUN'
                destination = $destination
            }
        }
    }
    $packageHash = Get-Sha256 -Path $script:AutomationWorkerPath
    if ($packageHash -cne $ExpectedAutomationWorkerSha256.ToLowerInvariant()) {
        return [pscustomobject]@{ success = $false; code = 'AUTOMATION_WORKER_HASH_MISMATCH' }
    }
    $manifestPath = Assert-SafeAbsolutePath -Path $AutomationWorkerBuildManifestPath -MustExist
    $manifestHash = Get-Sha256 -Path $manifestPath
    if ($manifestHash -cne $ExpectedAutomationWorkerBuildManifestSha256.ToLowerInvariant()) {
        return [pscustomobject]@{ success = $false; code = 'AUTOMATION_WORKER_MANIFEST_HASH_MISMATCH' }
    }
    $manifest = $null
    try { $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json -AsHashtable -Depth 8 } catch {
        return [pscustomobject]@{ success = $false; code = 'AUTOMATION_WORKER_MANIFEST_INVALID' }
    }
    $workerProof = Test-R48BuildManifest -Manifest $manifest -Package 'swarm-automation' -Target 'swarm-automation-worker' -ImagePath $script:AutomationWorkerPath -ExpectedSha256 $packageHash
    if (-not $workerProof.success) { return [pscustomobject]@{ success = $false; code = "AUTOMATION_WORKER_$($workerProof.code)" } }
    $hostManifestPath = Assert-SafeAbsolutePath -Path $HostBuildManifestPath -MustExist
    if ((Get-Sha256 -Path $hostManifestPath) -cne $ExpectedHostBuildManifestSha256.ToLowerInvariant()) {
        return [pscustomobject]@{ success = $false; code = 'HOST_MANIFEST_HASH_MISMATCH' }
    }
    try { $hostManifest = Read-JsonFile -Path $hostManifestPath } catch {
        return [pscustomobject]@{ success = $false; code = 'HOST_MANIFEST_INVALID' }
    }
    $hostProof = Test-R48BuildManifest -Manifest $hostManifest -Package 'swarm-kernel-host' -Target 'swarm-kernel-host' -ImagePath $script:HostPath -ExpectedSha256 $ExpectedHostSha256.ToLowerInvariant()
    if (-not $hostProof.success) { return [pscustomobject]@{ success = $false; code = "HOST_$($hostProof.code)" } }
    $manifestCommit = $workerProof.source_commit
    $manifestTree = $workerProof.source_tree
    if ($hostProof.source_commit -cne $manifestCommit -or $hostProof.source_tree -cne $manifestTree -or $manifestCommit -cne $script:SourceCommitOid) {
        return [pscustomobject]@{ success = $false; code = 'AUTOMATION_WORKER_SOURCE_MISMATCH_WITH_HOST' }
    }
    try { Copy-Item -LiteralPath $script:AutomationWorkerPath -Destination $destination -Force } catch {
        return [pscustomobject]@{ success = $false; code = 'AUTOMATION_WORKER_COPY_FAILED' }
    }
    $installedHash = Get-Sha256 -Path $destination
    if ($installedHash -cne $packageHash) { return [pscustomobject]@{ success = $false; code = 'INSTALLED_WORKER_HASH_MISMATCH' } }
    return [pscustomobject]@{
        success = $true
        code = $null
        destination = $destination
        build_manifest = $manifestPath
        host_build_manifest = $hostManifestPath
        qualification_faults = $true
        image_sha256 = $installedHash
        source_commit = $manifestCommit
        source_tree = $manifestTree
    }
}

function Test-R48BuildManifest {
    param(
        $Manifest,
        [Parameter(Mandatory)][string] $Package,
        [Parameter(Mandatory)][string] $Target,
        [Parameter(Mandatory)][string] $ImagePath,
        [Parameter(Mandatory)][string] $ExpectedSha256
    )
    if ($Manifest -isnot [System.Collections.IDictionary] -or $Manifest['format'] -cne 'eliot.module_build_manifest.v1' -or $Manifest['schema_version'] -ne 1) {
        return [pscustomobject]@{ success = $false; code = 'MANIFEST_INVALID' }
    }
    $source = $Manifest['source']
    $build = $Manifest['build']
    if ($source -isnot [System.Collections.IDictionary] -or $build -isnot [System.Collections.IDictionary] -or
        [string]$source['commit'] -cnotmatch '^[a-f0-9]{40}([a-f0-9]{24})?$' -or
        [string]$source['tree'] -cnotmatch '^[a-f0-9]{40}([a-f0-9]{24})?$' -or
        $source['checkout_clean_before'] -isnot [bool] -or -not $source['checkout_clean_before'] -or
        $source['checkout_clean_after'] -isnot [bool] -or -not $source['checkout_clean_after']) {
        return [pscustomobject]@{ success = $false; code = 'MANIFEST_PROVENANCE_INVALID' }
    }
    if ($build['package_name'] -cne $Package -or $build['profile'] -cne 'release') {
        return [pscustomobject]@{ success = $false; code = 'MANIFEST_BUILD_MISMATCH' }
    }
    $arguments = @($build['cargo_arguments'])
    $featureIndexes = @(for ($i = 0; $i -lt $arguments.Count; $i++) { if ($arguments[$i] -ceq '--features') { $i } })
    if ($featureIndexes.Count -ne 1 -or $featureIndexes[0] + 1 -ge $arguments.Count -or $arguments[$featureIndexes[0] + 1] -cne 'qualification-faults') {
        return [pscustomobject]@{ success = $false; code = 'MANIFEST_QUALIFICATION_FEATURE_MISSING' }
    }
    $artifacts = @($Manifest['artifacts'] | Where-Object { $_['target_name'] -ceq $Target })
    if ($artifacts.Count -ne 1 -or $artifacts[0]['file'] -cne "bin/$Target.exe" -or
        $artifacts[0]['source_sha256'] -cne $ExpectedSha256 -or $artifacts[0]['artifact_sha256'] -cne $ExpectedSha256 -or
        (Get-Sha256 -Path $ImagePath) -cne $ExpectedSha256 -or
        $artifacts[0]['bytes'] -ne (Get-Item -LiteralPath $ImagePath).Length) {
        return [pscustomobject]@{ success = $false; code = 'MANIFEST_IMAGE_MISMATCH' }
    }
    return [pscustomobject]@{ success = $true; code = $null; source_commit = [string]$source['commit']; source_tree = [string]$source['tree'] }
}

<# ==========================================================================================
 Bootstrap seed phase. Creates the real target identities the static schedule pins.
 The bootstrap host runs on the same configured storage.data_dir, is stopped
 gracefully before the final host starts, and its Store is never copied or edited.
========================================================================================== #>
function New-TaskSpecFixture {
    param([Parameter(Mandatory)][string] $Path)
    $spec = [ordered]@{
        objective = 'R48 sidecar qualification probe task.'
        phase = 'qualification'
        requirements = @(
            [ordered]@{ id = 'R1'; statement = 'Provide a real target identity for a one-shot scheduled CheckRun.' }
        )
        dependencies = @()
        owner_policy_id = 'owner-policy-v1'
        scope = [ordered]@{
            initial_paths = @('src/')
            forbidden_paths = @()
            prerequisite_policy = 'owner_module_prerequisites'
        }
        source_refs = @()
    }
    Write-PrivateJson -Path $Path -Value $spec
}

function Invoke-BootstrapSeed {
    param([Parameter(Mandatory)] $Scenario)
    $create = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'task.create' -Params ([ordered]@{
        client_request_id = 'r48-seed-task:' + [Guid]::NewGuid().ToString('N')
        project_id = $script:ProjectId
        spec = (Read-JsonFile -Path $script:TaskSpecPath)
    })
    if (-not $create.success) { return [pscustomobject]@{ success = $false; code = 'TASK_CREATE_FAILED'; detail = $create.code } }
    $taskId = [string]$create.value['task_id']
    if ([string]::IsNullOrWhiteSpace($taskId)) { $taskId = [string]$create.value['result']['task_id'] }
    if ([string]::IsNullOrWhiteSpace($taskId)) { return [pscustomobject]@{ success = $false; code = 'TASK_ID_MISSING' } }
    $createdRevision = Get-StrictInt64 -Value $create.value['revision']
    if (-not $createdRevision.valid -or $createdRevision.value -le 0) {
        return [pscustomobject]@{ success = $false; code = 'TASK_REVISION_MISSING' }
    }

    $claim = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'task.claim' -Params ([ordered]@{
        client_request_id = 'r48-seed-claim:' + [Guid]::NewGuid().ToString('N')
        task_id = $taskId
        expected_revision = $createdRevision.value
    })
    if (-not $claim.success) { return [pscustomobject]@{ success = $false; code = 'TASK_CLAIM_FAILED'; detail = $claim.code } }
    $attemptId = [string]$claim.value['attempt_id']
    if ([string]::IsNullOrWhiteSpace($attemptId)) { $attemptId = [string]$claim.value['result']['attempt_id'] }
    if ([string]::IsNullOrWhiteSpace($attemptId)) { return [pscustomobject]@{ success = $false; code = 'ATTEMPT_ID_MISSING' } }

    $capture = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'source.capture' -Params ([ordered]@{
        client_request_id = 'r48-seed-capture:' + [Guid]::NewGuid().ToString('N')
        attempt_id = $attemptId
        expected_revision = $createdRevision.value
        repository = $script:SourceRepository
        commit = $script:SourceCommitOid
    })
    if (-not $capture.success) { return [pscustomobject]@{ success = $false; code = 'SOURCE_CAPTURE_FAILED'; detail = $capture.code } }
    $captureOperationId = [string]$capture.value['operation_id']
    if ([string]::IsNullOrWhiteSpace($captureOperationId)) { $captureOperationId = [string]$capture.value['result']['operation_id'] }
    if ([string]::IsNullOrWhiteSpace($captureOperationId)) { return [pscustomobject]@{ success = $false; code = 'SOURCE_CAPTURE_OPERATION_MISSING' } }

    # Deterministic source snapshot identity derived from the capture operation ID,
    # using the same rule as swarm-kernel-host checks::source. No source bytes are
    # read or retained by this harness.
    $candidateBytes = [Text.Encoding]::UTF8.GetBytes($captureOperationId)
    $candidateHash = -join ([Security.Cryptography.SHA256]::HashData($candidateBytes) | ForEach-Object { '{0:x2}' -f $_ })
    $candidateRef = 'source-' + $candidateHash

    $artifact = $null
    $artifactDeadline = [DateTime]::UtcNow.AddSeconds([Math]::Min(120, $TimeoutSeconds + 60))
    $artifactCode = 'SOURCE_SNAPSHOT_READBACK_UNAVAILABLE'
    while ([DateTime]::UtcNow -lt $artifactDeadline) {
        if ($null -eq $Scenario.host -or $Scenario.host.HasExited) {
            return [pscustomobject]@{ success = $false; code = 'HOST_PROCESS_CHANGED_OR_EXITED' }
        }
        $artifact = Invoke-PublicCall -Scenario $Scenario -CredentialPath $Scenario.manager_path -Method 'artifact.get' -Params ([ordered]@{ artifact_id = $candidateRef })
        if ($artifact.success) { break }
        $artifactCode = $artifact.code
        Start-Sleep -Milliseconds 500
    }
    if (-not $artifact.success) {
        return [pscustomobject]@{
            success = $false
            code = 'SOURCE_SNAPSHOT_NOT_SETTLED'
            detail = $artifactCode
            candidate_ref = $candidateRef
            capture_operation_id = $captureOperationId
        }
    }
    $kind = [string]$artifact.value['kind']
    if ($kind -cne 'source_snapshot') { return [pscustomobject]@{ success = $false; code = 'ARTIFACT_NOT_SOURCE_SNAPSHOT'; kind = $kind } }
    $metaAttempt = [string]$artifact.value['metadata']['attempt_id']
    $metaRevision = Get-StrictInt64 -Value $artifact.value['metadata']['task_revision']
    if ($metaAttempt -cne $attemptId -or -not $metaRevision.valid -or $metaRevision.value -ne $createdRevision.value) {
        return [pscustomobject]@{ success = $false; code = 'SNAPSHOT_METADATA_MISMATCH' }
    }
    # The pinned Task revision the schedule must name, read from the durable snapshot.
    $taskRevision = $metaRevision.value

    return [pscustomobject]@{
        success = $true
        code = $null
        task_id = $taskId
        attempt_id = $attemptId
        task_revision = $taskRevision
        capture_operation_id = $captureOperationId
        candidate_ref = $candidateRef
    }
}


<# ==========================================================================================
 Explain mode. Prints the bounded plan and exits without running anything.
========================================================================================== #>
if ($Explain) {
    $plan = [ordered]@{
        schema = $script:Schema
        mode = 'explain'
        purpose = 'bounded R48 sidecar qualification using a fresh disjoint run root'
        implements = @(
            'scheduler current-owner readiness from the public host.status owner projection'
            'one real enabled one-shot CheckRun admission read from host.status plus operation.get'
            'host remains alive across a bounded post-readback window with unchanged identity'
            'uncertain initial automation.scheduler.admit receipt with no replay, as a bounded read-only observation'
        )
        does_not_implement = @(
            'any source, build, test, native, or model execution'
            'any git mutation'
            'any change to the eight-package Q set; the automation worker package is always separate'
            'any overall R48 qualification claim'
        )
        write_scope = [ordered]@{
            fresh_run_root = 'exactly one directory under -OutputRoot'
            worker_install = 'the worker executable beside the Q host executable'
            q_run_root = 'never opened for write'
            retained_q_or_r_state = 'never read for evidence and never mutated'
        }
        preconditions = [ordered]@{
            automation_worker = 'required as a separate Q-candidate release package with a matching manifest'
            host_config = 'operator-supplied TOMC that already contains the one-shot schedule'
            fresh_run_root = 'a new directory under -OutputRoot, disjoint from the Q run root'
        }
        subgates = @(
            'scheduler_current_owner_ready'
            'enabled_one_shot_checkrun_admitted'
            'uncertain_initial_admit_receipt_no_replay'
            'host_remains_alive'
        )
        verifier = $script:Report.verifier
    }
$plan | ConvertTo-Json -Depth 16 | Write-Host
    exit 0
}

# ---- Run parameter completeness ------------------------------------------------
# A run is refused with one explicit code rather than prompting interactively,
# so an incomplete invocation cannot hang an automated qualification.
$missingRunParameters = @()
foreach ($required in @('QRoot','HostExecutable','HostSupervisorExecutable','HostLauncherExecutable','PublicCliExecutable','AutomationWorkerExecutable','AutomationWorkerBuildManifestPath','HostBuildManifestPath','HostConfigPath','ProjectId','TaskSpecPath','GitExecutable','SourceRepository','OutputRoot','ScheduleId','CheckProfileId','CheckProfileRevision')) {
    if ([string]::IsNullOrWhiteSpace((Get-Variable -Name $required -ValueOnly -ErrorAction SilentlyContinue))) {
        $missingRunParameters += $required
    }
}
if ($missingRunParameters.Count -gt 0) {
    Write-Error ('MISSING_RUN_PARAMETERS: ' + ($missingRunParameters -join ','))
    exit 2
}

# The value shapes that were previously enforced by parameter attributes. They are
# checked here so the Explain set can bind without them while a real run is still
# refused on a malformed value.
$shapeRules = [ordered]@{
    ExpectedHostSha256 = '^[A-Fa-f0-9]{64}$'
    ExpectedHostSupervisorSha256 = '^[A-Fa-f0-9]{64}$'
    ExpectedHostLauncherSha256 = '^[A-Fa-f0-9]{64}$'
    ExpectedPublicCliSha256 = '^[A-Fa-f0-9]{64}$'
    ExpectedAutomationWorkerSha256 = '^[A-Fa-f0-9]{64}$'
    ExpectedAutomationWorkerBuildManifestSha256 = '^[A-Fa-f0-9]{64}$'
    ExpectedHostBuildManifestSha256 = '^[A-Fa-f0-9]{64}$'
    SourceCommitOid = '^[A-Fa-f0-9]{40}([A-Fa-f0-9]{24})?$'
    ScheduleId = '^[a-z0-9_-]{1,48}$'
    ProjectId = '^[A-Za-z0-9._:-]{1,128}$'
    CheckProfileId = '^[A-Za-z0-9_-]{1,100}$'
    CheckProfileRevision = '^[A-Za-z0-9._-]{1,128}$'
}
$malformedRunParameters = @()
foreach ($name in $shapeRules.Keys) {
    $value = [string](Get-Variable -Name $name -ValueOnly -ErrorAction SilentlyContinue)
    if ($value -cnotmatch $shapeRules[$name]) { $malformedRunParameters += $name }
}
if ($malformedRunParameters.Count -gt 0) {
    Write-Error ('MALFORMED_RUN_PARAMETERS: ' + ($malformedRunParameters -join ','))
    exit 2
}

<# ==========================================================================================
 Preflight. Nothing is written and no process is started before these checks pass.
========================================================================================== #>
try {
$script:QRoot = Assert-SafeAbsolutePath -Path $QRoot
if (-not (Test-Path -LiteralPath $script:QRoot -PathType Container)) { Stop-Harness 'Q_ROOT_NOT_A_DIRECTORY' }
$script:HostPath = Assert-SafeAbsolutePath -Path $HostExecutable -MustExist
$script:HostSupervisorPath = Assert-SafeAbsolutePath -Path $HostSupervisorExecutable -MustExist
$script:HostLauncherPath = Assert-SafeAbsolutePath -Path $HostLauncherExecutable -MustExist
$script:PublicCliPath = Assert-SafeAbsolutePath -Path $PublicCliExecutable -MustExist
$script:AutomationWorkerPath = Assert-SafeAbsolutePath -Path $AutomationWorkerExecutable -MustExist
$script:ConfigPath = Assert-SafeAbsolutePath -Path $HostConfigPath -MustExist
$templateGuard = Assert-TemplateIsSchedulerFree -Path $script:ConfigPath
if (-not $templateGuard.ok) { Stop-Harness $templateGuard.code }
$script:HostConfigTemplatePath = $script:ConfigPath
$script:TaskSpecPath = Assert-SafeAbsolutePath -Path $TaskSpecPath -MustExist
$script:GitExecutable = Assert-SafeAbsolutePath -Path $GitExecutable -MustExist
$script:ProjectId = $ProjectId
$script:ScheduleId = $ScheduleId
$script:SourceRepository = Assert-SafeAbsolutePath -Path $SourceRepository
if (-not (Test-Path -LiteralPath $script:SourceRepository -PathType Container)) { Stop-Harness 'SOURCE_REPOSITORY_NOT_A_DIRECTORY' }
$script:SourceCommitOid = $SourceCommitOid

# The output root must be disjoint from the Q run root. Both are resolved to full
# paths before comparison so a relative or nested path cannot alias the Q root.
$outputRootFull = Assert-SafeAbsolutePath -Path $OutputRoot
if ($outputRootFull.StartsWith($script:QRoot, [StringComparison]::OrdinalIgnoreCase)) {
    Stop-Harness 'OUTPUT_ROOT_NOT_DISJOINT_FROM_Q_ROOT'
}
if ($script:QRoot.StartsWith($outputRootFull, [StringComparison]::OrdinalIgnoreCase)) {
    Stop-Harness 'OUTPUT_ROOT_NOT_DISJOINT_FROM_Q_ROOT'
}
New-PrivateDirectory -Path $outputRootFull
$script:RunDirectory = Join-Path $outputRootFull ('r48-' + [Guid]::NewGuid().ToString('N'))
New-PrivateDirectory -Path $script:RunDirectory
$script:Report.run_directory = $script:RunDirectory
$script:Report.q_root = $script:QRoot
$script:Report.disjoint_from_q = $true
$script:Report.status = 'preflight_passed'
Save-Receipt

# Provenance receipts for every Q artifact this run consumes.
Record-QReceipt -Kind 'host' -Sha256 (Get-Sha256 -Path $script:HostPath) -Path $script:HostPath
Record-QReceipt -Kind 'host_supervisor' -Sha256 (Get-Sha256 -Path $script:HostSupervisorPath) -Path $script:HostSupervisorPath
Record-QReceipt -Kind 'host_launcher' -Sha256 (Get-Sha256 -Path $script:HostLauncherPath) -Path $script:HostLauncherPath
Record-QReceipt -Kind 'public_cli' -Sha256 (Get-Sha256 -Path $script:PublicCliPath) -Path $script:PublicCliPath
Record-QReceipt -Kind 'host_config' -Sha256 (Get-Sha256 -Path $script:ConfigPath) -Path $script:ConfigPath
Record-QReceipt -Kind 'automation_worker' -Sha256 (Get-Sha256 -Path $script:AutomationWorkerPath) -Path $script:AutomationWorkerPath

$hostHash = Get-Sha256 -Path $script:HostPath
if ($hostHash -cne $ExpectedHostSha256.ToLowerInvariant()) { Stop-Harness 'HOST_IMAGE_HASH_MISMATCH' }
$supervisorHash = Get-Sha256 -Path $script:HostSupervisorPath
if ($supervisorHash -cne $ExpectedHostSupervisorSha256.ToLowerInvariant()) { Stop-Harness 'HOST_SUPERVISOR_HASH_MISMATCH' }
$launcherHash = Get-Sha256 -Path $script:HostLauncherPath
if ($launcherHash -cne $ExpectedHostLauncherSha256.ToLowerInvariant()) { Stop-Harness 'HOST_LAUNCHER_HASH_MISMATCH' }
$cliHash = Get-Sha256 -Path $script:PublicCliPath
if ($cliHash -cne $ExpectedPublicCliSha256.ToLowerInvariant()) { Stop-Harness 'PUBLIC_CLI_HASH_MISMATCH' }

# The worker package is verified and installed before any host starts.
$workerInstall = Install-QAutomationWorker
if (-not $workerInstall.success) { Stop-Harness $workerInstall.code }
$script:Report.automation_worker = [ordered]@{
    package_executable = $script:AutomationWorkerPath
    image_sha256 = $workerInstall.image_sha256
    installed_path = $workerInstall.destination
    build_manifest = $workerInstall.build_manifest
    source_commit = $workerInstall.source_commit
    source_tree = $workerInstall.source_tree
    supplied_separately_from_eight_package_set = $true
}
Save-Receipt


<# ==========================================================================================
 Phase 1: bootstrap host. Scheduler disabled, no schedules. Creates the real target
 identities that the final static schedule pins. Stopped gracefully before phase 2.
========================================================================================== #>
$sharedStateDirectory = Join-Path $script:RunDirectory 'shared-state'
New-PrivateDirectory -Path $sharedStateDirectory
$script:BootstrapScenario = New-R48ScenarioDirectory -Name 'r48-bootstrap' -SharedState $sharedStateDirectory
$bootstrapConfig = Write-R48RunConfig -TemplatePath $script:HostConfigTemplatePath -DestinationPath (Join-Path $script:RunDirectory 'r48-bootstrap-config.toml') -StateDirectory $sharedStateDirectory -ScheduleId $ScheduleId -SchedulerEnabled $false
if (-not $bootstrapConfig.ok) { Stop-Harness $bootstrapConfig.code }
$script:ConfigPath = $bootstrapConfig.config_path
$bootstrapStart = Start-R48Host -Scenario $script:BootstrapScenario
if (-not $bootstrapStart.ready) {
    Stop-Harness $(if ($bootstrapStart.code) { $bootstrapStart.code } else { 'BOOTSTRAP_HOST_START_FAILED' })
}
$bootstrapManager = New-R48Manager -Scenario $script:BootstrapScenario
if (-not $bootstrapManager.success) {
    Stop-Harness $(if ($bootstrapManager.code) { $bootstrapManager.code } else { 'BOOTSTRAP_MANAGER_CREATE_FAILED' })
}
$script:BootstrapScenario.manager_path = $bootstrapManager.path
$script:Manager = $bootstrapManager
$script:Report.bootstrap_host = [ordered]@{
    pid = $bootstrapStart.pid
    controller_id = $bootstrapStart.controller_id
    host_epoch = $bootstrapStart.epoch
    started_at_utc = [DateTime]::UtcNow.ToString('o')
}
Save-Receipt

$seed = Invoke-BootstrapSeed -Scenario $script:BootstrapScenario
if (-not $seed.success) {
    Add-SubgateResult -Name 'bootstrap_seed' -Status 'blocked' -Code $seed.code -Evidence ([ordered]@{
        candidate_ref = $seed.candidate_ref
        capture_operation_id = $seed.capture_operation_id
        detail = $seed.detail
    })
    Stop-Harness $seed.code
}
$script:Report.seed = [ordered]@{
    task_id = $seed.task_id
    attempt_id = $seed.attempt_id
    task_revision = $seed.task_revision
    candidate_ref = $seed.candidate_ref
    capture_operation_id = $seed.capture_operation_id
}
Add-SubgateResult -Name 'bootstrap_seed' -Status 'verified' -Code $null -Evidence ([ordered]@{
    task_id = $seed.task_id
    attempt_id = $seed.attempt_id
    candidate_ref = $seed.candidate_ref
    capture_operation_id = $seed.capture_operation_id
})
Save-Receipt

# Graceful bootstrap shutdown before the final host starts on the same Store.
$bootstrapStop = Stop-OwnedHost -Scenario $script:BootstrapScenario
if (-not $bootstrapStop.stopped) {
    $script:Report.limits.pending_bootstrap_host_pid = $bootstrapStop.pending_pid
    Stop-Harness 'BOOTSTRAP_HOST_STOP_UNCONFIRMED'
}

<# ==========================================================================================
 Phase 2: final scheduler-enabled host. The static config pins the bootstrap
 Attempt and candidate and the one-shot schedule.
========================================================================================== #>
# The seeded one-shot schedule is generated here, after the confirmed graceful
# bootstrap stop, because its identity fields are the real seed results. The
# immutable operator template is never rewritten; a fresh run-local config is written.
$scheduleAnchorMs = [DateTimeOffset]::UtcNow.AddSeconds($MinimumScheduleLeadSeconds).ToUnixTimeMilliseconds()
$finalConfig = Write-R48RunConfig -TemplatePath $script:HostConfigTemplatePath -DestinationPath (Join-Path $script:RunDirectory 'r48-final-config.toml') -StateDirectory $sharedStateDirectory -ScheduleId $ScheduleId -SchedulerEnabled $true -AnchorMs $scheduleAnchorMs -AttemptId $seed.attempt_id -ExpectedTaskRevision $seed.task_revision -CandidateRef $seed.candidate_ref -ProfileId $CheckProfileId -ProfileRevision $CheckProfileRevision
if (-not $finalConfig.ok) { Stop-Harness $finalConfig.code }
$script:ConfigPath = $finalConfig.config_path
$script:Report.final_schedule = [ordered]@{
    schedule_id = $ScheduleId
    anchor_ms = $scheduleAnchorMs
    anchor_utc = [DateTimeOffset]::FromUnixTimeMilliseconds($scheduleAnchorMs).UtcDateTime.ToString('o')
    attempt_id = $seed.attempt_id
    expected_task_revision = $seed.task_revision
    candidate_ref = $seed.candidate_ref
    profile_id = $CheckProfileId
    profile_revision = $CheckProfileRevision
    generated_from = 'real seed identities after the confirmed graceful bootstrap stop'
}
Save-Receipt
# The final host opens the SAME Store the bootstrap host seeded, with the same
# operator credential the bootstrap host created. No Store is copied or edited.
$script:FinalScenario = New-R48ScenarioDirectory -Name 'r48-final' -SharedState $sharedStateDirectory
$finalStart = Start-R48Host -Scenario $script:FinalScenario -QualificationFault
if (-not $finalStart.ready) {
    Stop-Harness $(if ($finalStart.code) { $finalStart.code } else { 'FINAL_HOST_START_FAILED' })
}
$finalManager = New-R48Manager -Scenario $script:FinalScenario
if (-not $finalManager.success) {
    Stop-Harness $(if ($finalManager.code) { $finalManager.code } else { 'FINAL_MANAGER_CREATE_FAILED' })
}
$script:FinalScenario.manager_path = $finalManager.path
$script:Manager = $finalManager
$script:Report.final_host = [ordered]@{
    pid = $finalStart.pid
    controller_id = $finalStart.controller_id
    host_epoch = $finalStart.epoch
    pipe_name = $finalStart.pipe_name
    started_at_utc = [DateTime]::UtcNow.ToString('o')
}
Save-Receipt

# ---- Subgate 1: scheduler current-owner readiness --------------------------------
$ownerReady = Assert-SchedulerOwnerReady -Scenario $script:FinalScenario -ExpectedControllerId $finalStart.controller_id -ExpectedHostEpoch $finalStart.epoch -PollSeconds $OwnerReadinessPollSeconds
if (-not $ownerReady.success) {
    Add-SubgateResult -Name 'scheduler_current_owner_ready' -Status 'not_verified' -Code $ownerReady.code -Evidence ([ordered]@{ observations = $ownerReady.observations })
    Stop-Harness $ownerReady.code
}
Add-SubgateResult -Name 'scheduler_current_owner_ready' -Status 'verified' -Code $null -Evidence ([ordered]@{
    observations = $ownerReady.observations
    ready_at_utc = $ownerReady.ready_at_utc
})
Save-Receipt

# ---- Static schedule shape ------------------------------------------------------
$scheduleShape = Assert-StaticScheduleShape -Scenario $script:FinalScenario -ScheduleId $ScheduleId -ExpectedControllerId $finalStart.controller_id -ExpectedHostEpoch $finalStart.epoch
if (-not $scheduleShape.success) { Stop-Harness $scheduleShape.code }

# ---- Subgate 2: one real, eligible, one-shot CheckRun ---------------------------
$dueOperation = Wait-ForDueCheckRunOperation -Scenario $script:FinalScenario -ScheduleId $ScheduleId -AnchorMs $scheduleShape.anchor_ms -PollSeconds $DueOperationPollSeconds -ExpectedHostPid $finalStart.pid -ExpectedHostEpoch $finalStart.epoch
if (-not $dueOperation.success) {
    Add-SubgateResult -Name 'enabled_one_shot_checkrun_admitted' -Status 'not_verified' -Code $dueOperation.code -Evidence ([ordered]@{
        samples = $dueOperation.samples
        failure = $dueOperation.failure
        detail = $dueOperation.detail
    })
    Stop-Harness $dueOperation.code
}
$receipt = Assert-EligibleCheckRunReceipt -Operation $dueOperation.operation -CheckReceipt $dueOperation.check_receipt -AttemptId $seed.attempt_id -CandidateRef $seed.candidate_ref -ProfileId $CheckProfileId -ProfileRevision $CheckProfileRevision
if (-not $receipt.success) {
    Add-SubgateResult -Name 'enabled_one_shot_checkrun_admitted' -Status 'not_verified' -Code $receipt.code -Evidence ([ordered]@{
        work_operation_id = $dueOperation.work_operation_id
        operation = $dueOperation.operation
    })
    Stop-Harness $receipt.code
}
Add-SubgateResult -Name 'enabled_one_shot_checkrun_admitted' -Status 'verified' -Code $null -Evidence ([ordered]@{
    schedule_id = $ScheduleId
    anchor_ms = $scheduleShape.anchor_ms
    work_operation_id = $dueOperation.work_operation_id
    operation_state = $receipt.state
    attempt_id = $seed.attempt_id
    candidate_ref = $seed.candidate_ref
    samples = $dueOperation.samples
})
Save-Receipt

# ---- Subgate 3: uncertain initial admit receipt with no replay ------------------
# The evidence channel is the bounded create-only witness that the live, Store-owned
# worker writes beside its exact private config. The witness is bound to the observed
# live owner process and to the worker config digest on disk.
$noReplay = Wait-ForQualificationFaultWitness -Scenario $script:FinalScenario -ExpectedHostPid $finalStart.pid -ExpectedHostEpoch $finalStart.epoch -ExpectedControllerId $finalStart.controller_id -PollSeconds $TimeoutSeconds
if (-not $noReplay.success) {
    # When no witness exists the subgate is recorded as an explicit gap, never as a
    # verified claim and never hidden.
    $gapStatus = if ($noReplay.code -in @('QUALIFICATION_WITNESS_ABSENT')) { 'unknown' } else { 'not_verified' }
    Add-SubgateResult -Name 'uncertain_initial_admit_receipt_no_replay' -Status $gapStatus -Code $noReplay.code -Evidence ([ordered]@{ note = $noReplay.note })
    $script:Report.subgate_gap_note = 'No valid applied-fault witness was observed within the bound. Arming, the admit RPC outcome and witness-write outcome are separate facts; this gap proves none of them absent.'
}
else {
    # Injection is established here. The absence of a second admission intent
    # is checked only after the observation window and confirmed host shutdown.
    $script:Report.fault_injection = $noReplay
}
Save-Receipt

# ---- Subgate 4: host remains alive ----------------------------------------------
$hostAlive = Assert-HostRemainsAlive -Scenario $script:FinalScenario -ExpectedPid $finalStart.pid -ExpectedHostEpoch $finalStart.epoch -ExpectedControllerId $finalStart.controller_id -Samples $HostAliveSamples -IntervalSeconds $HostAliveSampleIntervalSeconds
if (-not $hostAlive.success) {
    Add-SubgateResult -Name 'host_remains_alive' -Status 'not_verified' -Code $hostAlive.code -Evidence ([ordered]@{ observations = $hostAlive.observations })
    Stop-Harness $hostAlive.code
}
Add-SubgateResult -Name 'host_remains_alive' -Status 'verified' -Code $null -Evidence ([ordered]@{
    observations = $hostAlive.observations
    pid = $finalStart.pid
    host_epoch = $finalStart.epoch
})
Save-Receipt

<# ==========================================================================================
 Completion. The final host is stopped gracefully and the receipt is written.
========================================================================================== #>
$finalStop = Stop-OwnedHost -Scenario $script:FinalScenario
if (-not $finalStop.stopped) {
    $script:Report.limits.pending_final_host_pid = $finalStop.pending_pid
    Stop-Harness 'FINAL_HOST_STOP_UNCONFIRMED'
}
if ($noReplay.success) {
    $attempts = Test-QualificationAdmitAttempts -Scenario $script:FinalScenario -Injection $noReplay
    if ($attempts.success) {
        Add-SubgateResult -Name 'uncertain_initial_admit_receipt_no_replay' -Status 'verified' -Code $null -Evidence $attempts
    } else {
        Add-SubgateResult -Name 'uncertain_initial_admit_receipt_no_replay' -Status 'not_verified' -Code $attempts.code
    }
}
$script:Report.status = 'completed'
$script:Report.completed_at_utc = [DateTime]::UtcNow.ToString('o')
$verified = @($script:SubgateResults | Where-Object { $_.status -eq 'verified' }).Count
$gaps = @($script:SubgateResults | Where-Object { $_.status -ne 'verified' }).Count
$script:Report.summary = [ordered]@{
    subgates_verified = $verified
    subgates_with_gaps = $gaps
    r48_qualification = 'not_qualified'
    reason = 'this sidecar harness reports bounded subgates only and never an overall R48 result'
}
Save-Receipt
Write-Output $script:ReceiptPath








} catch {
    if ($script:Report.status -ne 'blocked') {
        $script:Report.status = 'blocked'
        $script:Report.blocked_code = 'HARNESS_EXCEPTION'
        $script:Report.exception_type = $_.Exception.GetType().FullName
    }
    throw
} finally {
    $cleanup = [System.Collections.Generic.List[object]]::new()
    $pendingPublicCli = [System.Collections.Generic.List[object]]::new()
    foreach ($entry in @(@{ name='bootstrap'; scenario=$script:BootstrapScenario }, @{ name='final'; scenario=$script:FinalScenario })) {
        if ($null -eq $entry.scenario) { continue }
        if ($null -ne $entry.scenario.host) {
            $stopped = Stop-OwnedHost -Scenario $entry.scenario
            $cleanup.Add(@{phase=$entry.name;kind='host';stopped=$stopped.stopped})
            if (-not $stopped.stopped) {
                $script:Report.limits['pending_'+$entry.name+'_host_pid'] = $stopped.pending_pid
                $script:Report.status = 'blocked'
                if (-not $script:Report.Contains('blocked_code')) { $script:Report.blocked_code = 'OWNED_HOST_STOP_UNCONFIRMED' }
            }
        }
        foreach ($pendingCall in @($entry.scenario.pending_public_cli_processes.ToArray())) {
            $stopped = Stop-OwnedPublicCliProcess -Process $pendingCall.process -WaitMilliseconds 5000
            $cleanup.Add([ordered]@{
                phase = $entry.name
                kind = 'public_cli_process'
                process_id = $pendingCall.process_id
                initial_cleanup_pending = [bool]$pendingCall.cleanup_pending
                stopped = $stopped.stopped
                cli_error_code = $pendingCall.error_code
                pending_pid = $stopped.pending_pid
            })
            if ($stopped.stopped) {
                $pendingCall.cleanup_pending = $false
                $pendingCall.cleanup_confirmed = $true
                $pendingCall.process = $null
                $null = $entry.scenario.pending_public_cli_processes.Remove($pendingCall)
            }
            else {
                $pendingCall.cleanup_pending = $true
                $pendingCall.process = $stopped.process
                if ($null -ne $stopped.process_id) { $pendingCall.process_id = $stopped.process_id }
                $pendingPublicCli.Add([ordered]@{
                    phase = $entry.name
                    process_id = $pendingCall.process_id
                    cli_error_code = $pendingCall.error_code
                })
            }
        }
    }
    if ($pendingPublicCli.Count -gt 0) {
        $script:Report.limits['pending_public_cli_processes'] = @($pendingPublicCli.ToArray())
        $script:Report.status = 'blocked'
        if (-not $script:Report.Contains('blocked_code')) { $script:Report.blocked_code = 'PUBLIC_CLI_CLEANUP_PENDING' }
    }
    $script:Report.cleanup = @($cleanup.ToArray())
    if ($null -ne $script:RunDirectory) { Save-Receipt }
}
