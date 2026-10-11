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
$functions = @('Get-ControllerId', 'Get-StrictInt64', 'Assert-EligibleCheckRunReceipt', 'Assert-HostRemainsAlive', 'New-PrivateDirectory', 'Write-PrivateJson', 'Invoke-PublicCall', 'Stop-OwnedHost')
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
$check.operation_id = 'foreign-operation'
Assert-Case 'foreign_check_link_rejected' (-not (Assert-EligibleCheckRunReceipt @receiptArgs).success)
$check.operation_id = 'operation-fixture'
$check.profile_revision = '2'
Assert-Case 'different_check_profile_rejected' (-not (Assert-EligibleCheckRunReceipt @receiptArgs).success)

# Exercise the liveness reader with a real process handle and the public UUID wire type.
$script:fixtureStatus = @{ controller_id = $controllerId; host_lifecycle = @{ current = @{ state = 'running'; host_epoch = 1 } } }
function Invoke-PublicCall { return [pscustomobject]@{ success = $true; value = $script:fixtureStatus } }
$fixtureProcess = Get-Process -Id $PID
$scenario = [pscustomobject]@{ host = $fixtureProcess; manager_path = 'unused-fixture' }
Assert-Case 'liveness_reads_uuid_without_pid_assignment' (Assert-HostRemainsAlive -Scenario $scenario -ExpectedPid $PID -ExpectedHostEpoch 1 -Samples 1 -IntervalSeconds 1).success
$script:fixtureStatus.controller_id = 7
Assert-Case 'unreadable_live_identity_rejected' (-not (Assert-HostRemainsAlive -Scenario $scenario -ExpectedPid $PID -ExpectedHostEpoch 1 -Samples 1 -IntervalSeconds 1).success)

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
    return [pscustomobject]@{ started = $true; completed = $true; exit_code = 0; stdout = ($operation | ConvertTo-Json -Depth 5) }
}
$transportScenario = [pscustomobject]@{ state = 'unused-fixture'; requests = $privatePath }
$transportCall = Invoke-PublicCall -Scenario $transportScenario -CredentialPath 'unused-fixture' -Method 'operation.get' -Params @{ operation_id = 'operation-fixture' }
Assert-Case 'cli_file_contains_only_params' ($script:requestDocument.Count -eq 1 -and $script:requestDocument.operation_id -ceq 'operation-fixture')
Assert-Case 'operation_result_keeps_outer_identity' ($transportCall.success -and $transportCall.value.method -ceq 'check.run' -and
    $transportCall.value.operation_id -ceq 'operation-fixture' -and $transportCall.value.result.check_id -ceq 'check-fixture')

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
