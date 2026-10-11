#requires -Version 7.0
[CmdletBinding()]
param()
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$tokens=$null; $errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $PSScriptRoot 'New-NativeQualification.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'NATIVE_QUALIFICATION_PARSE_FAILED'}
$parameter=@($ast.ParamBlock.Parameters | Where-Object {$_.Name.VariablePath.UserPath -ceq 'OpenCodeCommandTestModelRef'})
if($parameter.Count -ne 1){throw 'MODEL_PARAMETER_NOT_UNIQUE'}
$pattern=@($parameter[0].Attributes | Where-Object {$_.TypeName.Name -eq 'ValidatePattern'})[0].PositionalArguments[0].Value
$default=$parameter[0].DefaultValue.Value
$definition=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-AdapterContract'},$true))
if($definition.Count -ne 1){throw 'ADAPTER_CONTRACT_NOT_UNIQUE'}
Invoke-Expression $definition[0].Extent.Text
$passed=0
function Assert-Case([string]$Name,[bool]$Condition){
 if(-not $Condition){throw "FAIL: $Name"};$script:passed++;Write-Output "PASS: $Name"
}
Assert-Case 'default_preserves_native_step5_free_coordinate' ($default -ceq 'kilo/stepfun/step-5-preview-free')
$Adapter='OpenCode'
foreach($case in @(
 @{ref='kilo/stepfun/step-5-preview-free';provider='kilo';model='stepfun/step-5-preview-free'},
 @{ref='openrouter/deepseek/r1:free';provider='openrouter';model='deepseek/r1:free'},
 @{ref='openai/gpt-6.1-sol';provider='openai';model='gpt-6.1-sol'},
 @{ref='kilo/stepfun-step-5-preview-free';provider='kilo';model='stepfun-step-5-preview-free'},
 @{ref='custom.provider/model/family/version';provider='custom.provider';model='model/family/version'}
)){
 $OpenCodeCommandTestModelRef=$case.ref
 Assert-Case "parameter_accepts_$($case.ref)" ([regex]::IsMatch($case.ref,$pattern))
 $contract=Get-AdapterContract
 Assert-Case "contract_preserves_$($case.ref)" ($contract.expected_provider -ceq $case.provider -and $contract.expected_model -ceq $case.model -and $contract.expected_model_ref -ceq $case.ref)
}
foreach($invalid in @('kilo/','/stepfun/step-5-preview-free','kilo//step-5','kilo/stepfun/','kilo/stepfun/step 5','kilo\stepfun\step-5',"kilo/stepfun/step`n5","kilo/stepfun/step`t5","kilo/stepfun/step-5-preview-free`n","kilo/stepfun/step-5-preview-free`r`n")){
 Assert-Case 'malformed_native_coordinate_rejected' (-not [regex]::IsMatch($invalid,$pattern))
}
Write-Output "OPENCODE_NATIVE_MODEL_COORDINATE_RESULT passed=$passed failed=0 native_calls=0"
