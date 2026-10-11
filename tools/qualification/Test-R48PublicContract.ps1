#requires -Version 7.0
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$harness = Join-Path $PSScriptRoot 'Invoke-R48SchedulerQualification.ps1'
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($harness, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'R48_PARSE_FAILED' }
$functions = @('Get-ControllerId', 'Get-StrictInt64', 'Assert-EligibleCheckRunReceipt', 'Assert-HostRemainsAlive', 'New-PrivateDirectory', 'Write-PrivateJson', 'Invoke-PublicCall', 'Stop-OwnedHost', 'Stop-OwnedPublicCliProcess', 'Start-BoundedProcess', 'Test-QualificationFaultWitness', 'Wait-ForQualificationFaultWitness')
$definitions = @($ast.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $true) |
    Where-Object { $_.Name -in $functions })
if ($definitions.Count -ne $functions.Count) { throw 'R48_FUNCTION_MISSING' }
Invoke-Expression (($definitions | ForEach-Object { $_.Extent.Text }) -join "`n")
$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Output "PASS: $Name"
}

# Store model::new_id() publishes a canonical UUID string, not an integer.
$controllerId = '77d0e125-05f6-40e9-9369-715fbbeb2703'
Assert-Case 'canonical_controller_uuid' (Get-ControllerId $controllerId).valid
Assert-Case 'numeric_controller_rejected' (-not (Get-ControllerId 7).valid)
Assert-Case 'empty_controller_rejected' (-not (Get-ControllerId ([Guid]::Empty.ToString('D'))).valid)

# This is the public operation.get shape: profile fields occur only in check.get.
$operation = @{
    operation_id = 'operation-fixture'; method = 'check.run'; state = 'queued'; attempt_id = 'attempt-fixture'
    result = @{ check_id = 'check-fixture'; candidate_ref = 'source-fixture' }
}
$check = @{
    check_id = 'check-fixture'; operation_id = 'operation-fixture'; attempt_id = 'attempt-fixture'
    candidate_ref = 'source-fixture'; profile_id = 'fixture'; profile_revision = '1'
}
$receiptArgs = @{ Operation = $operation; CheckReceipt = $check; AttemptId = 'attempt-fixture'; CandidateRef = 'source-fixture'; ProfileId = 'fixture'; ProfileRevision = '1' }
Assert-Case 'separate_check_profile_readback' (Assert-EligibleCheckRunReceipt @receiptArgs).success
$operation.state = 'native_accepted'
Assert-Case 'launched_check_retains_durable_admission' (Assert-EligibleCheckRunReceipt @receiptArgs).success
$operation.state = 'outcome_unknown'
Assert-Case 'unknown_check_not_upgraded' (-not (Assert-EligibleCheckRunReceipt @receiptArgs).success)
$operation.state = 'rejected'
Assert-Case 'rejected_check_not_admitted' (-not (Assert-EligibleCheckRunReceipt @receiptArgs).success)
$operation.state = 'queued'
$check.operation_id = 'foreign-operation'
Assert-Case 'foreign_check_link_rejected' (-not (Assert-EligibleCheckRunReceipt @receiptArgs).success)
$check.operation_id = 'operation-fixture'
$check.profile_revision = '2'
Assert-Case 'different_check_profile_rejected' (-not (Assert-EligibleCheckRunReceipt @receiptArgs).success)

# Exercise the liveness reader with a real process handle and the public UUID wire type.
$script:fixtureStatus = @{ controller_id = $controllerId; host_lifecycle = @{ current = @{ state = 'running'; host_epoch = 1 } } }
function Invoke-PublicCall {
    param($Scenario, $CredentialPath, $Method, $Params, $TimeoutMilliseconds)
    $script:fixtureReadbackBudget = $TimeoutMilliseconds
    if ($script:fixtureReadbackDelay -gt 0) { Start-Sleep -Milliseconds $script:fixtureReadbackDelay }
    return [pscustomobject]@{ success = $true; value = $script:fixtureStatus }
}
$script:fixtureReadbackDelay = 0
$fixtureProcess = Get-Process -Id $PID
$timedChild = $null
try {
    $timedChild = Start-BoundedProcess -Arguments @('-NoProfile','-Command','Start-Sleep -Seconds 30') -ExecutablePath $fixtureProcess.Path -TimeoutMilliseconds 250
    Assert-Case 'timed_out_cli_child_is_stopped_and_exit_confirmed' (
        $timedChild.started -and -not $timedChild.completed -and
        $timedChild.error_code -ceq 'CLI_TIMEOUT_OUTCOME_UNKNOWN' -and
        $timedChild.cleanup_confirmed -and -not $timedChild.cleanup_pending -and
        $null -eq $timedChild.process)
} finally {
    if ($null -ne $timedChild -and $null -ne $timedChild.process) {
        $childCleanup = Stop-OwnedPublicCliProcess -Process $timedChild.process -WaitMilliseconds 5000
        if (-not $childCleanup.stopped) { throw 'FIXTURE_PUBLIC_CLI_CHILD_STOP_UNCONFIRMED' }
    }
}

$refusedProcess = [pscustomobject]@{
    Id = 424242
    waits = [System.Collections.Generic.List[int]]::new()
    kill_called = $false
    dispose_called = $false
}
$refusedProcess | Add-Member -MemberType ScriptMethod -Name WaitForExit -Value {
    param([int] $Milliseconds)
    $this.waits.Add($Milliseconds)
    return $false
}
$refusedProcess | Add-Member -MemberType ScriptMethod -Name Kill -Value { $this.kill_called = $true }
$refusedProcess | Add-Member -MemberType ScriptMethod -Name Dispose -Value { $this.dispose_called = $true }
$refusedStop = Stop-OwnedPublicCliProcess -Process $refusedProcess -WaitMilliseconds 1
Assert-Case 'unconfirmed_cli_stop_retains_handle_without_dispose' (
    -not $refusedStop.stopped -and $refusedStop.pending_pid -eq 424242 -and
    [object]::ReferenceEquals($refusedStop.process, $refusedProcess) -and
    $refusedProcess.kill_called -and -not $refusedProcess.dispose_called -and
    $refusedProcess.waits.Count -eq 2 -and $refusedProcess.waits[1] -eq 1)
$scenario = [pscustomobject]@{ host = $fixtureProcess; manager_path = 'unused-fixture' }
Assert-Case 'liveness_reads_uuid_without_pid_assignment' (Assert-HostRemainsAlive -Scenario $scenario -ExpectedPid $PID -ExpectedHostEpoch 1 -ExpectedControllerId $controllerId -Samples 1 -IntervalSeconds 1).success
$script:fixtureStatus.controller_id = 7
Assert-Case 'unreadable_live_identity_rejected' (-not (Assert-HostRemainsAlive -Scenario $scenario -ExpectedPid $PID -ExpectedHostEpoch 1 -ExpectedControllerId $controllerId -Samples 1 -IntervalSeconds 1).success)
$script:fixtureStatus.controller_id = [Guid]::NewGuid().ToString('D')
Assert-Case 'foreign_live_controller_rejected' (-not (Assert-HostRemainsAlive -Scenario $scenario -ExpectedPid $PID -ExpectedHostEpoch 1 -ExpectedControllerId $controllerId -Samples 1 -IntervalSeconds 1).success)

$privatePath = Join-Path ([IO.Path]::GetTempPath()) ('r48-private-contract-' + [Guid]::NewGuid().ToString('N'))
$null = New-PrivateDirectory $privatePath
if ($IsWindows) {
    $acl = Get-Acl -LiteralPath $privatePath
    $userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $rules = @($acl.Access)
    Assert-Case 'new_directory_private_at_creation' ($acl.AreAccessRulesProtected -and $rules.Count -eq 1 -and
        $rules[0].IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -ceq $userSid)
} else {
    $mode = [IO.File]::GetUnixFileMode($privatePath)
    Assert-Case 'new_directory_private_at_creation' ($mode -eq ([IO.UnixFileMode]::UserRead -bor [IO.UnixFileMode]::UserWrite -bor [IO.UnixFileMode]::UserExecute))
}

# A real child publishes a delayed witness; polling must retain the exact
# owner/config validation rather than accepting a marker's mere existence.
$script:MaxFrameBytes = 1MB
$witnessRoot = Join-Path $privatePath 'witness-state'
$servicePath = Join-Path $witnessRoot 'automation-scheduler-v1'
$null = New-PrivateDirectory $servicePath
$digest = 'a' * 64
Write-PrivateJson -Path (Join-Path $servicePath 'worker.json') -Value @{ config_sha256 = $digest }
$witnessPath = Join-Path $servicePath 'qualification-fault-witness.json'
$writerScript = Join-Path $privatePath 'delayed-witness.ps1'
[IO.File]::WriteAllText($writerScript, @'
param([string]$Path)
Start-Sleep -Milliseconds 300
$value = @{ schema='eliot.swarm.r48.qualification-fault-witness.v1'; owner_launch_pid=$PID; owner_config_sha256=('a'*64); method='automation.scheduler.admit'; replay='withheld_before_delivery'; withheld_frame_bytes=128 }
[IO.File]::WriteAllText($Path,($value|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
[Console]::In.ReadLine() | Out-Null
'@, [Text.UTF8Encoding]::new($false))
$writerInfo = [Diagnostics.ProcessStartInfo]::new()
$writerInfo.FileName = $fixtureProcess.Path
$writerInfo.UseShellExecute = $false
$writerInfo.CreateNoWindow = $true
$writerInfo.RedirectStandardInput = $true
foreach($argument in @('-NoProfile','-File',$writerScript,'-Path',$witnessPath)) { $writerInfo.ArgumentList.Add($argument) }
$writer = [Diagnostics.Process]::Start($writerInfo)
try {
    $script:fixtureStatus = @{ controller_id=$controllerId; host_lifecycle = @{ current = @{ host_epoch=1 }; optional_workers = @{ 'automation-scheduler' = @{ state='running'; owner = @{ status='current'; phase='active'; ready=$true; host_scope=@{controller_id=$controllerId;host_epoch=1}; group_identity=@{pid=$writer.Id} } } } } }
    $witnessScenario = [pscustomobject]@{ host=$fixtureProcess; state=$witnessRoot; manager_path='unused-fixture' }
    $waitArgs = @{Scenario=$witnessScenario;ExpectedHostPid=$PID;ExpectedHostEpoch=1;ExpectedControllerId=$controllerId;PollSeconds=5}
    $observed = Wait-ForQualificationFaultWitness @waitArgs
    Assert-Case 'delayed_witness_keeps_live_owner_binding' ($observed.success -and $observed.owner_launch_pid -eq $writer.Id -and $observed.config_digest_matches_file)
    $script:fixtureStatus.host_lifecycle.optional_workers['automation-scheduler'].owner.group_identity.pid = $PID
    Assert-Case 'foreign_delayed_witness_fails_closed' (-not (Wait-ForQualificationFaultWitness @waitArgs).success)
    $script:fixtureStatus.host_lifecycle.optional_workers['automation-scheduler'].owner.group_identity.pid = $writer.Id
    $script:fixtureStatus.controller_id = [Guid]::NewGuid().ToString('D')
    Assert-Case 'foreign_witness_controller_rejected' (-not (Wait-ForQualificationFaultWitness @waitArgs).success)
    $script:fixtureStatus.Remove('controller_id')
    Assert-Case 'missing_witness_controller_rejected' (-not (Wait-ForQualificationFaultWitness @waitArgs).success)
    $script:fixtureStatus.controller_id = $controllerId
    $owner = $script:fixtureStatus.host_lifecycle.optional_workers['automation-scheduler'].owner
    $owner.host_scope.controller_id = [Guid]::NewGuid().ToString('D')
    Assert-Case 'foreign_witness_owner_controller_rejected' (-not (Wait-ForQualificationFaultWitness @waitArgs).success)
    $owner.host_scope.controller_id = $controllerId
    $owner.host_scope.host_epoch = 2
    Assert-Case 'foreign_witness_owner_epoch_rejected' (-not (Wait-ForQualificationFaultWitness @waitArgs).success)
    $owner.host_scope.host_epoch = 1
    $script:fixtureStatus.host_lifecycle.optional_workers['automation-scheduler'].state = 'stopped'
    Assert-Case 'stopped_witness_worker_rejected' (-not (Wait-ForQualificationFaultWitness @waitArgs).success)
    $script:fixtureStatus.host_lifecycle.optional_workers['automation-scheduler'].state = 'running'
    $script:fixtureReadbackDelay = 1100
    $late = Wait-ForQualificationFaultWitness @waitArgs -PollSeconds 1
    Assert-Case 'late_validation_not_accepted' (-not $late.success -and $late.code -ceq 'QUALIFICATION_WITNESS_DEADLINE_EXCEEDED')
    Assert-Case 'witness_readback_gets_remaining_budget' ($script:fixtureReadbackBudget -gt 0 -and $script:fixtureReadbackBudget -le 1000)
    $script:fixtureReadbackDelay = 0
    $absentScenario = [pscustomobject]@{ host=$fixtureProcess; state=(Join-Path $privatePath 'absent-witness'); manager_path='unused-fixture' }
    $absent = Wait-ForQualificationFaultWitness -Scenario $absentScenario -ExpectedHostPid $PID -ExpectedHostEpoch 1 -ExpectedControllerId $controllerId -PollSeconds 1
    Assert-Case 'bounded_absence_stays_unknown' (-not $absent.success -and $absent.code -ceq 'QUALIFICATION_WITNESS_ABSENT')
} finally {
    $writer.StandardInput.Close()
    if (-not $writer.WaitForExit(5000)) { throw 'FIXTURE_WITNESS_WRITER_STOP_UNCONFIRMED' }
    $writer.Dispose()
}

# Re-load the real CLI helper after the liveness stub. Its transport accepts a
# params document and prints the complete application value, including result.
$publicCallDefinition = $definitions | Where-Object Name -eq 'Invoke-PublicCall'
Invoke-Expression $publicCallDefinition.Extent.Text
$script:ConfigPath = 'unused-fixture'
$script:PublicCliPath = 'unused-fixture'
$script:TimeoutSeconds = 60
$script:MaxCliOutputCharacters = 4096
$script:requestDocument = $null
function Start-BoundedProcess {
    param($Arguments, $ExecutablePath, $TimeoutMilliseconds)
    $fileIndex = [Array]::IndexOf($Arguments, '--file')
    $script:requestDocument = Get-Content -LiteralPath $Arguments[$fileIndex + 1] -Raw | ConvertFrom-Json -AsHashtable
    $script:transportReadbackBudget = $TimeoutMilliseconds
    return [pscustomobject]@{ started = $true; completed = $true; exit_code = 0; stdout = ($operation | ConvertTo-Json -Depth 5); error_code = $null; cleanup_pending = $false; cleanup_confirmed = $true; process_id = 1; process = $null }
}
$transportScenario = [pscustomobject]@{ state = 'unused-fixture'; requests = $privatePath; pending_public_cli_processes = [System.Collections.Generic.List[object]]::new() }
$transportCall = Invoke-PublicCall -Scenario $transportScenario -CredentialPath 'unused-fixture' -Method 'operation.get' -Params @{ operation_id = 'operation-fixture' }
Assert-Case 'cli_file_contains_only_params' ($script:requestDocument.Count -eq 1 -and $script:requestDocument.operation_id -ceq 'operation-fixture')
Assert-Case 'operation_result_keeps_outer_identity' ($transportCall.success -and $transportCall.value.method -ceq 'check.run' -and
    $transportCall.value.operation_id -ceq 'operation-fixture' -and $transportCall.value.result.check_id -ceq 'check-fixture')
$null = Invoke-PublicCall -Scenario $transportScenario -CredentialPath 'unused-fixture' -Method 'operation.get' -Params @{ operation_id='operation-fixture' } -TimeoutMilliseconds 123
Assert-Case 'cli_gets_supplied_readback_budget' ($script:transportReadbackBudget -eq 123)
$script:pendingStartResult = [pscustomobject]@{
    started = $true; completed = $false; exit_code = $null; error_code = 'CLI_TIMEOUT_OUTCOME_UNKNOWN'
    stdout = ''; stderr = ''; process = $refusedProcess; process_id = 424242
    cleanup_confirmed = $false; cleanup_pending = $true
}
function Start-BoundedProcess {
    param($Arguments, $ExecutablePath, $TimeoutMilliseconds)
    return $script:pendingStartResult
}
$pendingScenario = [pscustomobject]@{ state = 'unused-fixture'; requests = $privatePath; pending_public_cli_processes = [System.Collections.Generic.List[object]]::new() }
$pendingCall = Invoke-PublicCall -Scenario $pendingScenario -CredentialPath 'unused-fixture' -Method 'operation.get' -Params @{ operation_id = 'operation-fixture' }
Assert-Case 'public_cli_timeout_keeps_original_error_and_pending_handle' (
    -not $pendingCall.success -and $pendingCall.code -ceq 'CLI_TIMEOUT_OUTCOME_UNKNOWN' -and
    $pendingCall.cleanup_pending -and $pendingCall.pending_pid -eq 424242 -and
    $pendingScenario.pending_public_cli_processes.Count -eq 1 -and
    [object]::ReferenceEquals($pendingScenario.pending_public_cli_processes[0].process, $refusedProcess))

# Enter the complete public script in a separate process. An absent QRoot must be
# rejected before any host, Store, installation or model effect can occur.
$missingRoot = Join-Path ([IO.Path]::GetTempPath()) ('r48-entry-contract-' + [Guid]::NewGuid().ToString('N'))
$psi = [Diagnostics.ProcessStartInfo]::new()
$psi.FileName = $fixtureProcess.Path
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
foreach ($item in @('-NoProfile', '-File', $harness)) { $psi.ArgumentList.Add($item) }
foreach ($parameter in $ast.ParamBlock.Parameters) {
    $name = $parameter.Name.VariablePath.UserPath
    if ($name -eq 'Explain') { continue }
    $value = switch -Regex ($name) {
        '^QRoot$' { $missingRoot; break }
        '^Expected.*Sha256$' { '0' * 64; break }
        '^SourceCommitOid$' { '0' * 40; break }
        '^(ProjectId|ScheduleId|CheckProfileId|CheckProfileRevision)$' { 'fixture'; break }
        '^(TimeoutSeconds|OwnerReadinessPollSeconds|DueOperationPollSeconds)$' { '60'; break }
        '^MinimumScheduleLeadSeconds$' { '120'; break }
        '^HostAlive' { '1'; break }
        default { $harness }
    }
    $psi.ArgumentList.Add('-' + $name)
    $psi.ArgumentList.Add([string]$value)
}
$entryProcess = [Diagnostics.Process]::new()
$entryProcess.StartInfo = $psi
try {
    [void]$entryProcess.Start()
    $stdout = $entryProcess.StandardOutput.ReadToEndAsync()
    $stderr = $entryProcess.StandardError.ReadToEndAsync()
    if (-not $entryProcess.WaitForExit(10000)) {
        $entryProcess.Kill()
        throw 'R48_ENTRY_FIXTURE_TIMEOUT'
    }
    $output = $stdout.GetAwaiter().GetResult() + $stderr.GetAwaiter().GetResult()
    Assert-Case 'bound_arguments_reach_preflight' ($entryProcess.ExitCode -ne 0 -and $output.Contains('Q_ROOT_NOT_A_DIRECTORY') -and
        -not $output.Contains('MISSING_RUN_PARAMETERS') -and -not $output.Contains('MALFORMED_RUN_PARAMETERS') -and
        -not (Test-Path -LiteralPath $missingRoot))
} finally { $entryProcess.Dispose() }

# Confirm EOF shutdown and repeated cleanup with an actual owned child. A
# confirmed stop clears the handle before disposal; unknown handles stay owned.
$script:TimeoutSeconds = 5
$stopInfo = [Diagnostics.ProcessStartInfo]::new()
$stopInfo.FileName = $fixtureProcess.Path
$stopInfo.UseShellExecute = $false
$stopInfo.CreateNoWindow = $true
$stopInfo.RedirectStandardInput = $true
foreach ($item in @('-NoProfile', '-Command', '[void][Console]::In.ReadToEnd(); exit 0')) { $stopInfo.ArgumentList.Add($item) }
$stopProcess = [Diagnostics.Process]::new()
$stopProcess.StartInfo = $stopInfo
$stopScenario = [pscustomobject]@{ host = $stopProcess }
try {
    [void]$stopProcess.Start()
    Assert-Case 'owned_child_eof_stop_clears_handle' ((Stop-OwnedHost $stopScenario).stopped -and $null -eq $stopScenario.host)
    Assert-Case 'confirmed_child_stop_is_idempotent' (Stop-OwnedHost $stopScenario).stopped
} finally {
    if ($null -ne $stopScenario.host) {
        if (-not $stopProcess.HasExited) { $stopProcess.Kill(); [void]$stopProcess.WaitForExit(5000) }
        $stopProcess.Dispose()
    }
}
$unknownProcess = [Diagnostics.Process]::new()
$unknownScenario = [pscustomobject]@{ host = $unknownProcess }
try {
    $unknownStop = Stop-OwnedHost $unknownScenario
    Assert-Case 'unconfirmed_handle_remains_owned' (-not $unknownStop.stopped -and [object]::ReferenceEquals($unknownScenario.host, $unknownProcess))
} finally { $unknownProcess.Dispose() }
Write-Output "R48_PUBLIC_CONTRACT_RESULT passed=$passed failed=0"
