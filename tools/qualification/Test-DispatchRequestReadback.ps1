<#
Integrated regression coverage for the single-task-dispatch readback loop in
New-NativeQualification.ps1.

The dispatch call can fail without exposing an operation id, so the loop must recover
the id from operation.list by the exact client_request_id the harness sent, then confirm
it through operation.get. This drives that whole flow against a stub transport, including
the shape the O-run actually hit: an unscoped pre-admission rejection that an earlier
identity check refused because it demanded Task, Attempt and Binding scopes.

The production readback loop and the classification after it are extracted verbatim
from the harness by line range and evaluated inside this fixture, so the break that
ends the loop stays in the same scope as the classification. Only the transport and
the report sink are stubbed.

The Store projection surface itself is covered by the Rust regression
exact_caller_correlates_unscoped_rejections_by_retained_request_id.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$harnessPath = Join-Path $PSScriptRoot 'New-NativeQualification.ps1'
if (-not (Test-Path -LiteralPath $harnessPath)) { throw "harness not found: $harnessPath" }

$harnessLines = Get-Content -LiteralPath $harnessPath

function Get-FunctionBlock([string[]]$Lines, [string]$StartPrefix, [string]$EndPrefix) {
    $from = -1; $to = -1
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        if ($from -lt 0) {
            if ($Lines[$i].Trim().StartsWith($StartPrefix)) { $from = $i }
            continue
        }
        if ($i -gt $from + 1 -and $Lines[$i].Trim().StartsWith($EndPrefix)) { $to = $i; break }
    }
    if ($from -lt 0 -or $to -lt 0) { throw ("function block not found starting with " + $StartPrefix) }
    return ($Lines[$from..($to - 1)] -join [Environment]::NewLine) + [Environment]::NewLine
}

function Get-LoopBlock([string[]]$Lines) {
    $from = -1; $to = -1
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        if ($from -lt 0) {
            if ($Lines[$i].Trim().StartsWith('while ([DateTime]::UtcNow -lt $dispatchDeadline) {')) { $from = $i }
            continue
        }
        if ($Lines[$i].Trim().StartsWith('$codexStillAttached = Confirm-CodexStillAttached')) { $to = $i; break }
    }
    if ($from -lt 0 -or $to -lt 0) { throw 'readback loop not found in the harness' }
    return $Lines[$from..($to - 1)]
}

function Get-ClassificationBlock([string[]]$Lines) {
    $from = -1; $to = -1
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        if ($from -lt 0) {
            if ($Lines[$i].Trim().StartsWith('if ($null -eq $dispatchOperation) {')) { $from = $i }
            continue
        }
        if ($i -gt $from + 1 -and $Lines[$i].Trim().StartsWith('if (-not $codexStillAttached -and $Adapter -eq')) { $to = $i; break }
    }
    if ($from -lt 0 -or $to -lt 0) { throw 'classification block not found in the harness' }
    return $Lines[$from..($to - 1)]
}

# Field reader and the shared identity helpers, verbatim from the harness.
Invoke-Expression (Get-FunctionBlock $harnessLines 'function Get-OptionalField {' 'function Test-ReadinessRecordShape {')
Invoke-Expression (Get-FunctionBlock $harnessLines 'function Get-DispatchRequestId {' 'function New-NativeCaptureState {')

$taskId = 'f0d67568-584a-4103-bb4e-cd306d7a9d7d'
$attemptId = '5e7a8a4f-1f4f-410e-81a8-c8d9425d6db9'
$bindingId = '314cb683-fad4-431d-8244-08131ca1079b'
$generation = 1
$timeoutSeconds = 3

# The whole dispatch readback is evaluated in one function so the loop break and the
# post-loop classification share a scope exactly as they do in the harness.
$invokeLoop = {
    param($Transport)
    $script:Stages = New-Object System.Collections.ArrayList
    $script:Report = [ordered]@{ status = 'running'; qualification = 'unknown'; safe_facts = [ordered]@{}; limits = [ordered]@{} }

    function Add-Stage {
        param([string]$Name, [string]$Status, [string]$Code, [System.Collections.IDictionary]$Facts = @{})
        $stage = [ordered]@{ name = $Name; status = $Status }
        if ($Code) { $stage.code = $Code }
        if ($Facts.Count -gt 0) { $stage.facts = $Facts }
        [void]$script:Stages.Add([pscustomobject]$stage)
        $script:Report.stages = @($script:Stages.ToArray())
    }

    function Confirm-CodexStillAttached { return $true }

    function Get-Operations {
        param([Parameter(Mandatory)][int] $TimeoutMilliseconds)
        return [pscustomobject]@{ success = $true; code = $null; items = @($Transport.list) }
    }

    function Invoke-ManagerCall {
        param([string]$Method, [System.Collections.IDictionary]$Params, [int]$TimeoutMilliseconds = 30000)
        if ($Method -eq 'operation.get') {
            # The first entry answers every id, which is how a wrong-operation_id
            # response looks: the Store hands back a row whose operation_id field
            # differs from the id that was asked for.
            if ($Transport.rows.Count -gt 0) {
                return [pscustomobject]@{ completed = $true; exit_code = 0; error_code = $null; value = $Transport.rows[0] }
            }
            return [pscustomobject]@{ completed = $true; exit_code = 1; error_code = 'NOT_FOUND'; value = $null }
        }
        return [pscustomobject]@{
            completed = $true
            exit_code = $Transport.dispatchExitCode
            error_code = $Transport.dispatchErrorCode
            value = $Transport.dispatchValue
        }
    }

    $dispatchClientRequestId = [string]$Transport.requestId
    $dispatchParams = [ordered]@{ attempt_id = $attemptId; client_request_id = $dispatchClientRequestId }
    $dispatchOperationId = if ($Transport.dispatchExitCode -eq 0 -and $null -ne $Transport.dispatchValue) { [string]$Transport.dispatchValue.operation_id } else { $null }
    $dispatchDeadline = [DateTime]::UtcNow.AddSeconds($timeoutSeconds)
    $dispatchOperation = $null
    $dispatchIdentity = $null
    $dispatchReadbackCode = $null
    $dispatch = [pscustomobject]@{ error_code = $Transport.dispatchErrorCode }
    $terminalDispatchStates = @('settled', 'rejected', 'cancelled', 'outcome_unknown')

    # The production loop, verbatim.
    . ([scriptblock]::Create((Get-LoopBlock $harnessLines) -join [Environment]::NewLine))

    $codexStillAttached = Confirm-CodexStillAttached

    # The production classification, verbatim.
    . ([scriptblock]::Create((Get-ClassificationBlock $harnessLines) -join [Environment]::NewLine))

    return [pscustomobject]@{
        status = $script:Report.status
        qualification = $script:Report.qualification
        stages = @($script:Stages.ToArray())
        dispatchOperationId = $dispatchOperationId
        settled = ($null -ne $dispatchOperation)
        readbackCode = $dispatchReadbackCode
        safeFacts = $script:Report.safe_facts
    }
}

$script:failures = 0
$script:checks = 0

function Check([string]$Name, [bool]$Condition) {
    $script:checks++
    if ($Condition) { Write-Host ("PASS  " + $Name) }
    else { $script:failures++; Write-Host ("FAIL  " + $Name) }
}

$requestId = 'native-qualification-80e1d251d3cf42fe826be7f6f4632973'
$siblingRequestId = 'native-qualification-9fac584684dc4c199215c201785b9b0e'

function New-UnscopedRejected([string]$RequestId) {
    return [ordered]@{
        operation_id = [Guid]::NewGuid().ToString()
        method = 'task.dispatch'
        state = 'rejected'
        client_request_id = $RequestId
        result = [ordered]@{ code = 'STALE_LAUNCH_DISPATCH_SCOPE' }
    }
}

Write-Host '== integrated flow: fallback recovers the id, then operation.get settles =='
$own = New-UnscopedRejected $requestId
$transport = [pscustomobject]@{
    requestId = $requestId
    dispatchExitCode = 1
    dispatchErrorCode = 'STALE_LAUNCH_DISPATCH_SCOPE'
    dispatchValue = $null
    rows = @($own)
    list = @($own)
}
$result = & $invokeLoop $transport
Check "loop settles the pre-admission rejection" ($result.settled)
Check "recovered operation id is the retained row" ($result.dispatchOperationId -eq $own.operation_id)
Check "status is dispatch_rejected_before_admission" ($result.status -eq 'dispatch_rejected_before_admission')
Check "qualification stays not_qualified" ($result.qualification -eq 'not_qualified')
Check "exactly one stage recorded" (@($result.stages).Count -eq 1)
$stage = @($result.stages)[0]
Check "stage status is observed" ($stage.status -eq 'observed')
Check "stage names the Store code" ($stage.code -eq 'STALE_LAUNCH_DISPATCH_SCOPE')
Check "safe facts carry the operation id" ($result.safeFacts.dispatch.operation_id -eq $own.operation_id)
Check "safe facts carry the Store code" ($result.safeFacts.dispatch.error_code -eq 'STALE_LAUNCH_DISPATCH_SCOPE')

Write-Host ''
Write-Host '== wrong operation_id for the known-id path never settles =='
# operation.get answers with the right method, request id and full scope, but names a
# different operation_id. The independent operation-id check must refuse it.
$rightId = [Guid]::NewGuid().ToString()
$wrongId = [Guid]::NewGuid().ToString()
$wrongRow = [ordered]@{
    operation_id = $wrongId
    method = 'task.dispatch'
    state = 'settled'
    client_request_id = $requestId
    task_id = $taskId
    attempt_id = $attemptId
    binding_id = $bindingId
    binding_generation = $generation
    result = [ordered]@{ outcome = 'applied'; operation_id = $wrongId }
}
$transport = [pscustomobject]@{
    requestId = $requestId
    dispatchExitCode = 0
    dispatchErrorCode = $null
    dispatchValue = [ordered]@{ operation_id = $rightId }
    rows = @($wrongRow)
    list = @($wrongRow)
}
$result = & $invokeLoop $transport
Check "wrong operation_id does not settle" (-not $result.settled)
Check "known operation id is never replaced by a list row" ($result.dispatchOperationId -ceq $rightId)
Check "status stays dispatch_outcome_unknown" ($result.status -eq 'dispatch_outcome_unknown')
Check "readback records the identity mismatch" ($result.readbackCode -eq 'DISPATCH_OPERATION_IDENTITY_MISMATCH')

Write-Host ''
Write-Host '== wrong same-caller request id never settles =='
# A sibling by the same caller with a different request id must be invisible even
# when it carries the full expected scope.
$sibling = New-UnscopedRejected $siblingRequestId
$transport = [pscustomobject]@{
    requestId = $requestId
    dispatchExitCode = 1
    dispatchErrorCode = 'STALE_LAUNCH_DISPATCH_SCOPE'
    dispatchValue = $null
    rows = @()
    list = @($sibling)
}
$result = & $invokeLoop $transport
Check "wrong same-caller sibling does not settle" (-not $result.settled)
Check "wrong sibling stays dispatch_outcome_unknown" ($result.status -eq 'dispatch_outcome_unknown')

Write-Host ''
Write-Host '== no retained receipt stays unknown =='
$transport = [pscustomobject]@{
    requestId = $requestId
    dispatchExitCode = 1
    dispatchErrorCode = 'CLI_EXIT'
    dispatchValue = $null
    rows = @()
    list = @()
}
$result = & $invokeLoop $transport
Check "no retained receipt does not settle" (-not $result.settled)
Check "no retained receipt stays dispatch_outcome_unknown" ($result.status -eq 'dispatch_outcome_unknown')

Write-Host ''
Write-Host '== malformed list operation ids are skipped without a binding failure =='
$transport.list = @(
    [ordered]@{ method = 'task.dispatch'; client_request_id = $requestId; state = 'rejected' },
    [ordered]@{ operation_id = ''; method = 'task.dispatch'; client_request_id = $requestId; state = 'rejected' },
    [ordered]@{ operation_id = @(); method = 'task.dispatch'; client_request_id = $requestId; state = 'rejected' },
    [ordered]@{ operation_id = @($own.operation_id); method = 'task.dispatch'; client_request_id = $requestId; state = 'rejected' }
)
$result = & $invokeLoop $transport
Check 'malformed ids leave readback unknown' (-not $result.settled -and $result.status -eq 'dispatch_outcome_unknown')
Check 'malformed ids are never adopted' ([string]::IsNullOrWhiteSpace($result.dispatchOperationId))

Write-Host ''
Write-Host '== scope absence requires null or missing fields, never empty/coerced values =='
foreach ($field in @('task_id', 'attempt_id', 'binding_id', 'binding_generation')) {
    foreach ($value in @('', @(), @{}, @('foreign-scope'), 0)) {
        $malformed = New-UnscopedRejected $requestId
        $malformed[$field] = $value
        $identity = Assert-DispatchRequestIdentity -Operation $malformed -ExpectedRequestId $requestId -ExpectedMethod 'task.dispatch' -ExpectedTaskId $taskId -ExpectedAttemptId $attemptId -ExpectedBindingId $bindingId -ExpectedGeneration $generation -ExpectedOperationId $malformed.operation_id
        Check "malformed $field shape rejected: $($value.GetType().Name)" ($null -eq $identity)
    }
}
$nullScoped = New-UnscopedRejected $requestId
foreach ($field in @('task_id', 'attempt_id', 'binding_id', 'binding_generation')) { $nullScoped[$field] = $null }
$identity = Assert-DispatchRequestIdentity -Operation $nullScoped -ExpectedRequestId $requestId -ExpectedMethod 'task.dispatch' -ExpectedTaskId $taskId -ExpectedAttemptId $attemptId -ExpectedBindingId $bindingId -ExpectedGeneration $generation -ExpectedOperationId $nullScoped.operation_id
Check 'explicit null scopes preserve pre-admission rejection' ($null -ne $identity -and $identity.pre_admission_rejected)

Write-Host ''
Write-Host '== inconsistent public result cannot become an observed rejection =='
$inconsistent = New-UnscopedRejected $requestId
$inconsistent.result.operation_id = [Guid]::NewGuid().ToString()
$transport = [pscustomobject]@{
    requestId = $requestId; dispatchExitCode = 0; dispatchErrorCode = $null
    dispatchValue = [ordered]@{ operation_id = $inconsistent.operation_id }
    rows = @($inconsistent); list = @($inconsistent)
}
$result = & $invokeLoop $transport
Check 'foreign result id is inconsistent' ($result.status -eq 'dispatch_readback_inconsistent')
Check 'inconsistent rejection stage stays unknown' ($result.stages[0].status -eq 'unknown')
Check 'inconsistent rejection reports safe mismatch code' ($result.stages[0].code -eq 'DISPATCH_PUBLIC_RESULT_IDENTITY_MISMATCH')
Check 'unscoped inconsistent row does not invent a task scope' ($null -eq $result.safeFacts.dispatch.task_id -and $null -eq $result.safeFacts.dispatch.attempt_id)

Write-Host ''
Write-Host '== multiple exact request matches do not select an arbitrary operation =='
$transport.dispatchExitCode = 1
$transport.dispatchValue = $null
$transport.rows = @()
$transport.list = @((New-UnscopedRejected $requestId), (New-UnscopedRejected $requestId))
$result = & $invokeLoop $transport
Check 'ambiguous request remains unknown' (-not $result.settled -and $result.status -eq 'dispatch_outcome_unknown')
Check 'ambiguity is retained as a bounded code' ($result.readbackCode -eq 'MULTIPLE_DISPATCH_OPERATIONS')

Write-Host ''
Write-Host '== an exact admitted scope remains a normal observed terminal readback =='
$scoped = [ordered]@{}
foreach ($key in $wrongRow.Keys) { $scoped[$key] = $wrongRow[$key] }
$scoped.operation_id = $rightId
$scoped.result = [ordered]@{ outcome = 'applied'; operation_id = $rightId }
$transport.dispatchExitCode = 0
$transport.dispatchValue = [ordered]@{ operation_id = $rightId }
$transport.rows = @($scoped)
$transport.list = @($scoped)
$result = & $invokeLoop $transport
Check 'exact scoped dispatch remains complete' ($result.status -eq 'dispatch_readback_complete' -and $result.stages[0].status -eq 'observed')
Check 'exact scoped dispatch retains task identity' ($result.safeFacts.dispatch.task_id -ceq $taskId)

Write-Host ''
Write-Host ("checks: " + $script:checks + ", failures: " + $script:failures)
if ($script:failures -gt 0) { exit 1 }
Write-Host 'ALL PASS'
