#requires -Version 7.0
<#
Runs the production launch readiness loop and sole task.dispatch call against
public transport fixtures. Binding/hello is already ready in every case; the
launch phase, native MCP connection card and exact parent tuple vary. No native effects.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$harnessPath = Join-Path $PSScriptRoot 'New-NativeQualification.ps1'
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($harnessPath, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Harness parse failed.' }
$names = @('Get-OptionalField', 'Test-ReadinessRecordShape', 'Get-SafeReadinessErrorCode',
    'Get-SafeReadinessBindingState', 'Get-SafeReadinessSupervisorPhase', 'Get-SafeReadinessProjection',
    'Test-ReadinessStringEquals', 'Test-ReadinessLaunchParent', 'Test-ReadinessGenerationEquals',
    'Get-SafeReadinessLaunchPhase', 'Test-ReadinessDispatchPhase', 'Get-SafeReadinessDispatchCapabilityState', 'Get-SafeReadinessIdentifier',
    'New-ReadinessFailureFacts', 'Test-PublicAppliedOperationReadback', 'Get-TerminalOpenRejection')
$functions = @($ast.FindAll({ param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst]
}, $true) | Where-Object Name -In $names)
if (@($functions.Name | Sort-Object -Unique).Count -ne $names.Count) { throw 'Missing readiness helpers.' }
foreach ($function in $functions) { . ([scriptblock]::Create($function.Extent.Text)) }

$source = Get-Content -LiteralPath $harnessPath -Raw
$start = $source.IndexOf('    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)', [StringComparison]::Ordinal)
$end = $source.IndexOf("    `$script:CurrentStage = 'single_task_dispatch'", $start, [StringComparison]::Ordinal)
if ($start -lt 0 -or $end -le $start) { throw 'Production readiness block missing.' }
$readinessBlock = $source.Substring($start, $end - $start)
$dispatchCalls = @($ast.FindAll({ param($node)
    $node -is [Management.Automation.Language.AssignmentStatementAst] -and
    $node.Extent.Text.StartsWith('$dispatch = Invoke-ManagerCall -Method ''task.dispatch''')
}, $true))
if ($dispatchCalls.Count -ne 1) { throw 'Expected exactly one production task.dispatch call.' }
$dispatchStatement = $dispatchCalls[0].Extent.Text

function New-Parent {
    param([AllowNull()] $Phase = 'awaiting_native_mcp')
    return [ordered]@{
        operation_id = 'parent-fixture'; method = 'swarm.launch'; state = 'queued'
        task_id = 'task-fixture'; attempt_id = 'attempt-fixture'
        binding_id = 'binding-fixture'; binding_generation = 7
        result = [ordered]@{ operation_id = 'parent-fixture'; state = 'queued'; launch_state = $Phase }
        native_dispatch_capability_readback = [ordered]@{
            schema_version = 1; state = 'ready'; dispatch_permitted = $false; model_consumed = 'unknown'
        }
    }
}

function Invoke-ReadinessCase {
    param([object[]] $Samples, [string] $Adapter = 'OpenCode', [AllowNull()][string] $RefreshErrorCode = $null,
        [AllowNull()] $OpenFailure = $null, [string] $OpenScopeDamage = '')
    $taskId = 'task-fixture'; $attemptId = 'attempt-fixture'; $parentOperationId = 'parent-fixture'
    $TimeoutSeconds = 30
    $launch = [pscustomobject]@{ error_code = $null }
    $contract = [pscustomobject]@{ module_id = 'module-fixture'; artifact_id = 'artifact-fixture'; version = '1' }
    $open = [ordered]@{
        operation_id = 'open-fixture'; method = 'agent.open'; state = 'settled'
        prerequisite_operation_id = $parentOperationId; task_id = $taskId; attempt_id = $attemptId
        binding_id = 'binding-fixture'; binding_generation = 7
        result = [ordered]@{ operation_id = 'open-fixture'; outcome = 'applied' }
    }
    if ($null -ne $OpenFailure) {
        $open.state = 'rejected'; $open.result.outcome = 'rejected'; $open.result.failure = $OpenFailure
        if ($OpenScopeDamage) { $open[$OpenScopeDamage] = 'foreign-scope' }
    }
    $state = [ordered]@{
        binding_id = 'binding-fixture'; generation = 7; state = 'ready'
        observation = [ordered]@{ module_supervisor = [ordered]@{
            phase = 'ready'; module_id = $contract.module_id; artifact_id = $contract.artifact_id
            artifact_version = $contract.version; binding_id = 'binding-fixture'; generation = 7
        } }
    }
    $trace = [ordered]@{ polls = 0; dispatches = 0; refreshes = 0; refresh_scope = $null; dispatched_poll = $null; current_parent = $null }
    $script:Report = [ordered]@{ safe_facts = [ordered]@{} }
    $stages = [Collections.Generic.List[object]]::new()
    function Get-Operations {
        param([int] $TimeoutMilliseconds)
        $trace.polls++
        if ($trace.polls -gt $Samples.Count) { throw 'Unexpected extra readiness poll.' }
        $trace.current_parent = $Samples[$trace.polls - 1]
        # Exhaust the existing deadline after the last public sample, without
        # sleeping or replacing any production readiness predicate.
        if ($trace.polls -eq $Samples.Count) {
            Set-Variable -Name deadline -Value ([DateTime]::UtcNow.AddSeconds(-1)) -Scope 1
        }
        return [pscustomobject]@{ success = $true; code = $null; items = @($open) }
    }
    function Invoke-ManagerCall {
        param([string] $Method, $Params, [int] $TimeoutMilliseconds)
        if ($Method -eq 'agent.refresh') {
            $trace.refreshes++; $trace.refresh_scope = $Params
            return [pscustomobject]@{ completed = ($null -eq $RefreshErrorCode); exit_code = $(if ($RefreshErrorCode) { 2 } else { 0 }); value = $null; error_code = $RefreshErrorCode }
        }
        if ($Method -eq 'task.dispatch') {
            $trace.dispatches++; $trace.dispatched_poll = $trace.polls
            return [pscustomobject]@{ completed = $true; exit_code = 0; value = $null; error_code = $null }
        }
        if ($Method -ne 'operation.get') { throw "Unexpected method: $Method" }
        $value = if ($Params.operation_id -ceq 'parent-fixture') { $trace.current_parent } else { $open }
        return [pscustomobject]@{ completed = $true; exit_code = 0; value = $value; error_code = $null }
    }
    function Get-ExactAgentState {
        param([string] $BindingId, [long] $Generation, [int] $TimeoutMilliseconds)
        return [pscustomobject]@{ completed = $true; exit_code = 0; value = $state; error_code = $null }
    }
    function Start-Sleep { param([int] $Milliseconds) }
    function New-RequestId { return 'session-refresh-fixture' }
    function Add-Stage {
        param([string] $Name, [string] $Status, [string] $Code, $Facts)
        $stages.Add([ordered]@{ name = $Name; status = $Status; code = $Code; facts = $Facts })
    }
    function Stop-Qualification { param([string] $Code); throw "STOP:$Code" }
    $stop = $null
    try {
        Invoke-Expression $readinessBlock
        $dispatchParams = [ordered]@{ attempt_id = $attemptId; launch_operation_id = $parentOperationId }
        Invoke-Expression $dispatchStatement
    } catch {
        if (-not $_.Exception.Message.StartsWith('STOP:')) { throw }
        $stop = $_.Exception.Message.Substring(5)
    }
    return [pscustomobject]@{ trace = $trace; stop = $stop; stages = $stages.ToArray(); facts = $script:Report.safe_facts }
}

$checks = 0
function Assert-Case {
    param([string] $Name, [bool] $Condition)
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:checks++
    Write-Output "PASS: $Name"
}
$progression = Invoke-ReadinessCase @((New-Parent 'awaiting_binding'), (New-Parent 'awaiting_participant_credential'), (New-Parent))
Assert-Case 'ready hello waits for participant issuance' ($progression.trace.polls -eq 3 -and $progression.trace.dispatched_poll -eq 3)
Assert-Case 'one dispatch on the exact phase' ($progression.trace.dispatches -eq 1 -and $null -eq $progression.stop)
Assert-Case 'safe success phase retained' ($progression.facts.launch.launch_phase -ceq 'awaiting_native_mcp')
Assert-Case 'safe success connection state retained' ($progression.facts.launch.native_dispatch_capability_state -ceq 'ready')
Assert-Case 'OpenCode never requests AG session refresh' ($progression.trace.refreshes -eq 0)

$agProgression = Invoke-ReadinessCase -Adapter 'Antigravity' -Samples @((New-Parent), (New-Parent))
Assert-Case 'AG refresh precedes the sole dispatch' ($agProgression.trace.refreshes -eq 1 -and
    $agProgression.trace.dispatches -eq 1 -and $agProgression.trace.dispatched_poll -eq 2)
Assert-Case 'AG refresh uses only the exact binding generation' ($agProgression.trace.refresh_scope.binding_id -ceq 'binding-fixture' -and
    $agProgression.trace.refresh_scope.generation -eq 7 -and $agProgression.trace.refresh_scope.client_request_id -ceq 'session-refresh-fixture')
$agUnavailable = New-Parent
$agUnavailable.native_dispatch_capability_readback.state = 'unavailable'
$agUncertain = Invoke-ReadinessCase -Adapter 'Antigravity' -RefreshErrorCode 'NATIVE_OUTCOME_UNKNOWN' -Samples @($agUnavailable, $agUnavailable, (New-Parent))
Assert-Case 'uncertain AG refresh is never replayed' ($agUncertain.trace.refreshes -eq 1 -and $agUncertain.trace.dispatches -eq 1 -and
    $agUncertain.trace.dispatched_poll -eq 3 -and $agUncertain.facts.native_session_refresh.replayed -eq $false)
$agBlocked = Invoke-ReadinessCase -Adapter 'Antigravity' -RefreshErrorCode 'NATIVE_OUTCOME_UNKNOWN' -Samples @($agUnavailable, $agUnavailable)
Assert-Case 'unresolved AG session proof never dispatches' ($agBlocked.trace.refreshes -eq 1 -and $agBlocked.trace.dispatches -eq 0 -and
    $agBlocked.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT')

$legacyCard = New-Parent
$legacyCard.native_mcp_connection_readback = $legacyCard.native_dispatch_capability_readback
$legacyCard.Remove('native_dispatch_capability_readback')
$legacyOnly = Invoke-ReadinessCase @($legacyCard)
Assert-Case 'legacy MCP card cannot replace current route capability' ($legacyOnly.trace.dispatches -eq 0 -and
    $legacyOnly.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT')

# Reproduce Q: binding and launch phase are ready while the MCP link is missing.
$missingMcp = New-Parent
$missingMcp.Remove('native_dispatch_capability_readback')
$unavailableMcp = New-Parent
$unavailableMcp.native_dispatch_capability_readback.state = 'unavailable'
$connectionProgression = Invoke-ReadinessCase @($missingMcp, $unavailableMcp, (New-Parent))
Assert-Case 'ready phase waits for missing then unavailable MCP' ($connectionProgression.trace.polls -eq 3 -and
    $connectionProgression.trace.dispatched_poll -eq 3)
Assert-Case 'connection becoming ready permits only one dispatch' ($connectionProgression.trace.dispatches -eq 1 -and
    $null -eq $connectionProgression.stop)
Assert-Case 'connection success saves only closed state' ($connectionProgression.facts.launch.native_dispatch_capability_state -ceq 'ready' -and
    -not $connectionProgression.facts.launch.Contains('native_dispatch_capability_readback') -and
    -not $connectionProgression.facts.launch.Contains('schema_version'))

$unavailable = Invoke-ReadinessCase @($unavailableMcp, $unavailableMcp)
Assert-Case 'unavailable connection never dispatches' ($unavailable.trace.dispatches -eq 0 -and
    $unavailable.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT')
Assert-Case 'unavailable timeout retains closed reason and state' ($unavailable.stages[0].facts.readiness_failure.reason -ceq 'native_dispatch_capability' -and
    $unavailable.stages[0].facts.readiness_failure.observed_native_dispatch_capability_state -ceq 'unavailable' -and
    $unavailable.stages[0].facts.dispatch_started -eq $false)

foreach ($damage in @('missing_card', 'null_card', 'array_card', 'scalar_card', 'private_state', 'pending_state', 'upper_state', 'array_state',
        'missing_schema', 'future_schema', 'string_schema', 'boolean_schema', 'array_schema', 'floating_schema',
        'missing_permission', 'permission_true', 'permission_string', 'permission_number', 'permission_null', 'permission_array',
        'missing_consumption', 'consumed_model', 'array_consumption', 'null_consumption', 'extra_private_field', 'wrong_key_case')) {
    $sample = New-Parent
    $card = $sample.native_dispatch_capability_readback
    switch ($damage) {
        'missing_card' { $sample.Remove('native_dispatch_capability_readback') }
        'null_card' { $sample.native_dispatch_capability_readback = $null }
        'array_card' { $sample.native_dispatch_capability_readback = @($card) }
        'scalar_card' { $sample.native_dispatch_capability_readback = 'PRIVATE_MCP_CARD_MARKER' }
        'private_state' { $card.state = 'PRIVATE_MCP_CARD_MARKER' }
        'pending_state' { $card.state = 'pending' }
        'upper_state' { $card.state = 'READY' }
        'array_state' { $card.state = @('ready') }
        'missing_schema' { $card.Remove('schema_version') }
        'future_schema' { $card.schema_version = 2 }
        'string_schema' { $card.schema_version = '1' }
        'boolean_schema' { $card.schema_version = $true }
        'array_schema' { $card.schema_version = @(1) }
        'floating_schema' { $card.schema_version = 1.0 }
        'missing_permission' { $card.Remove('dispatch_permitted') }
        'permission_true' { $card.dispatch_permitted = $true }
        'permission_string' { $card.dispatch_permitted = 'false' }
        'permission_number' { $card.dispatch_permitted = 0 }
        'permission_null' { $card.dispatch_permitted = $null }
        'permission_array' { $card.dispatch_permitted = @($false) }
        'missing_consumption' { $card.Remove('model_consumed') }
        'consumed_model' { $card.model_consumed = 'true' }
        'array_consumption' { $card.model_consumed = @('unknown') }
        'null_consumption' { $card.model_consumed = $null }
        'extra_private_field' { $card.private_detail = 'PRIVATE_MCP_CARD_MARKER' }
        'wrong_key_case' { $card.Remove('schema_version'); $card['Schema_Version'] = 1 }
    }
    $result = Invoke-ReadinessCase @($sample)
    Assert-Case ('connection ' + $damage + ' cannot dispatch') ($result.trace.dispatches -eq 0 -and
        $result.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT')
    Assert-Case ('connection ' + $damage + ' has closed diagnostics') ($result.stages[0].facts.readiness_failure.reason -ceq 'native_dispatch_capability' -and
        $result.stages[0].facts.readiness_failure.observed_native_dispatch_capability_state -ceq 'unknown' -and
        -not (($result.stages | ConvertTo-Json -Depth 12) -match 'PRIVATE_MCP_CARD_MARKER|schema_version|dispatch_permitted|model_consumed'))
}

$pending = Invoke-ReadinessCase @((New-Parent 'awaiting_participant_credential'))
Assert-Case 'pending launch expires without dispatch' ($pending.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT' -and $pending.trace.dispatches -eq 0)
Assert-Case 'closed pending phase explains timeout' ($pending.stages[0].facts.readiness_failure.reason -ceq 'launch_phase' -and
    $pending.stages[0].facts.readiness_failure.observed_launch_phase -ceq 'awaiting_participant_credential')

foreach ($phase in @($null, '', 'AWAITING_NATIVE_MCP', 'PRIVATE_PHASE_MARKER', @('awaiting_native_mcp'), $true, 7)) {
    $sample = New-Parent
    $sample.result.launch_state = $phase
    $result = Invoke-ReadinessCase @($sample)
    Assert-Case ('damaged phase cannot dispatch ' + $checks) ($result.trace.dispatches -eq 0 -and $result.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT')
    Assert-Case ('unsafe phase not retained ' + $checks) ($null -eq $result.stages[0].facts.readiness_failure.observed_launch_phase)
}
foreach ($damage in @('missing_phase', 'array_result', 'foreign_result_id', 'foreign_binding', 'string_generation', 'settled_parent', 'settled_result', 'foreign_task')) {
    $sample = New-Parent
    switch ($damage) {
        'missing_phase' { $sample.result.Remove('launch_state') }
        'array_result' { $sample.result = @($sample.result) }
        'foreign_result_id' { $sample.result.operation_id = 'other-parent' }
        'foreign_binding' { $sample.binding_id = 'other-binding' }
        'string_generation' { $sample.binding_generation = '7' }
        'settled_parent' { $sample.state = 'settled' }
        'settled_result' { $sample.result.state = 'settled' }
        'foreign_task' { $sample.task_id = 'other-task' }
    }
    $result = Invoke-ReadinessCase @($sample)
    Assert-Case $damage ($result.trace.dispatches -eq 0 -and $result.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT')
}
foreach ($category in @('catalog_unavailable','model_absent','model_disabled','variant_absent')) {
    $failure = @{ code='NATIVE_MODEL_UNAVAILABLE'; catalog_failure_category=$category }
    $rejected = Invoke-ReadinessCase -Samples @((New-Parent)) -OpenFailure $failure
    Assert-Case ('terminal rejection stops without dispatch '+$category) ($rejected.stop -ceq 'NATIVE_MODEL_UNAVAILABLE' -and $rejected.trace.dispatches -eq 0 -and $rejected.trace.polls -eq 1)
    Assert-Case ('closed rejection category retained '+$category) ($rejected.stages[0].facts.readiness_failure.catalog_failure_category -ceq $category)
}
$unsafe = Invoke-ReadinessCase -Samples @((New-Parent)) -OpenFailure @{ code='NATIVE_MODEL_UNAVAILABLE'; catalog_failure_category='PRIVATE_CATALOG_MARKER' }
Assert-Case 'unrecognized catalog category not projected' ($unsafe.stop -ceq 'NATIVE_MODEL_UNAVAILABLE' -and $null -eq $unsafe.stages[0].facts.readiness_failure.catalog_failure_category)
$foreign = Invoke-ReadinessCase -Samples @((New-Parent)) -OpenFailure @{code='NATIVE_MODEL_UNAVAILABLE';catalog_failure_category='model_absent'} -OpenScopeDamage 'task_id'
Assert-Case 'foreign rejected open not attributed' ($foreign.stop -ceq 'LAUNCH_READY_READBACK_TIMEOUT' -and $foreign.trace.dispatches -eq 0)
Write-Output "PASS: $checks integrated readiness assertions; native/model calls=0"
