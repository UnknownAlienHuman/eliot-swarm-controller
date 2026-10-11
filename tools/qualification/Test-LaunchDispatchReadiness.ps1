#requires -Version 7.0
<#
Runs the production launch readiness loop and sole task.dispatch call against
public transport fixtures. Binding/hello is already ready in every case; only
the retained launch phase and exact parent tuple change. No native effects.
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
    'Get-SafeReadinessLaunchPhase', 'Test-ReadinessDispatchPhase', 'Get-SafeReadinessIdentifier',
    'New-ReadinessFailureFacts', 'Test-PublicAppliedOperationReadback')
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
    }
}

function Invoke-ReadinessCase {
    param([object[]] $Samples)
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
    $state = [ordered]@{
        binding_id = 'binding-fixture'; generation = 7; state = 'ready'
        observation = [ordered]@{ module_supervisor = [ordered]@{
            phase = 'ready'; module_id = $contract.module_id; artifact_id = $contract.artifact_id
            artifact_version = $contract.version; binding_id = 'binding-fixture'; generation = 7
        } }
    }
    $trace = [ordered]@{ polls = 0; dispatches = 0; dispatched_poll = $null; current_parent = $null }
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
Write-Output "PASS: $checks integrated readiness assertions; native/model calls=0"
