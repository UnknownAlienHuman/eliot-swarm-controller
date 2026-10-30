param(
    [string]$Mode,
    [string]$Cli,
    [string]$DataDir,
    [string]$Agent,
    [string]$Generation,
    [string]$OtherCommandId,
    [string]$ContextPath,
    [string]$OwnCommandPath
)

$PSNativeCommandUseErrorActionPreference = $false

if ($env:CI -ne "script-approved") { exit 40 }

$inputStream = [Console]::OpenStandardInput()
$memory = [IO.MemoryStream]::new()
$buffer = [byte[]]::new(8192)
while (($read = $inputStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
    if ($memory.Length + $read -gt 1048576) { exit 41 }
    $memory.Write($buffer, 0, $read)
}
$contextBytes = $memory.ToArray()
[IO.File]::WriteAllBytes($ContextPath, $contextBytes)

# This is deliberately the Run principal's first RPC. Keep its JSON in a variable
# so only action NDJSON is written to this script's stdout.
$ownOutput = @(& $Cli --data-dir $DataDir --json cmd show $env:ELIOT_RUN_COMMAND_ID 2>&1)
$ownExit = $LASTEXITCODE
if ($ownExit -ne 0) {
    [Console]::Error.WriteLine("ELIOT_RUN_OWN_COMMAND_EXIT=$ownExit")
    $ownOutputText = $ownOutput -join "`n"
    $ownOutputBytes = [Text.Encoding]::UTF8.GetBytes($ownOutputText)
    $diagnosticLength = [Math]::Min(4096, $ownOutputBytes.Length)
    $strictUtf8 = [Text.UTF8Encoding]::new($false, $true)
    while ($diagnosticLength -gt 0) {
        try {
            $ownDiagnostic = $strictUtf8.GetString($ownOutputBytes, 0, $diagnosticLength)
            break
        } catch [Text.DecoderFallbackException] {
            $diagnosticLength--
        }
    }
    if ($diagnosticLength -gt 0) { [Console]::Error.WriteLine($ownDiagnostic) }
    exit 42
}
$ownText = $ownOutput -join "`n"
try {
    $ownCommand = ConvertFrom-Json -InputObject $ownText -AsHashtable -ErrorAction Stop
} catch {
    exit 43
}
if ($ownCommand["id"] -ne $env:ELIOT_RUN_COMMAND_ID -or $ownCommand["verb"] -ne "run") { exit 43 }
[IO.File]::WriteAllText($OwnCommandPath, $ownText, [Text.UTF8Encoding]::new($false))

# A real trigger command is readable by Owner; the Run principal must be denied.
$otherOutput = @(& $Cli --data-dir $DataDir --json cmd show $OtherCommandId 2>&1)
$otherExit = $LASTEXITCODE
$otherText = $otherOutput -join "`n"
if ($otherExit -ne 1 -or $otherText -notmatch "not_allowed") { exit 44 }

$sendText = "script-event-send"
if ($Mode -eq "invalid") { $sendText = "must-not-dispatch" }
if ($Mode -ne "success" -and $Mode -ne "invalid") { exit 45 }

$record = [ordered]@{
    action = "send"
    target = $Agent
    expected_generation = [long]$Generation
    text = $sendText
}
[Console]::Out.WriteLine(($record | ConvertTo-Json -Compress))
if ($Mode -eq "invalid") {
    $invalid = [ordered]@{}
    foreach ($name in $record.Keys) { $invalid[$name] = $record[$name] }
    $invalid["authority"] = "owner"
    [Console]::Out.WriteLine(($invalid | ConvertTo-Json -Compress))
}
