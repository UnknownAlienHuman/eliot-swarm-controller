[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$harnessPath = Join-Path $PSScriptRoot 'New-NativeQualification.ps1'
$tokens = $null
$parseErrors = $null
$harnessAst = [System.Management.Automation.Language.Parser]::ParseFile($harnessPath, [ref]$tokens, [ref]$parseErrors)
if ($parseErrors.Count -gt 0) { throw "Harness parse failed: $($parseErrors[0].Message)" }
$functionNames = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
foreach ($name in @(
    'Get-OptionalField',
    'Read-OpenCodeSelectedConnectionRecord',
    'Invoke-OpenCodeSelectedJsonGet',
    'Test-OpenCodeSelectedWorkspacePathMatch',
    'Get-OpenCodeSelectedCatalogEvidence',
    'Test-OpenCodeSelectedCatalogEvidence',
    'Get-OpenCodeSelectedCatalogStageFacts'
)) { [void]$functionNames.Add($name) }
$functionAsts = @($harnessAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $functionNames.Contains($node.Name)
}, $true))
if ($functionAsts.Count -ne $functionNames.Count) { throw 'Harness selected-catalog function set is incomplete.' }
foreach ($functionAst in $functionAsts) { Invoke-Expression $functionAst.Extent.Text }

function Assert-Fixture {
    param([Parameter(Mandatory)][bool] $Condition, [Parameter(Mandatory)][string] $Message)
    if (-not $Condition) { throw $Message }
}

Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

public sealed class OpenCodeSelectedCatalogFixtureServer : IDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new CancellationTokenSource();
    private readonly Thread _worker;
    private readonly object _sync = new object();
    private readonly List<string> _requestLines = new List<string>();
    private readonly string _workspace;
    public int Port { get; private set; }
    public bool CatalogMismatch { get; set; }

    public OpenCodeSelectedCatalogFixtureServer(string workspace)
    {
        _workspace = workspace;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _worker = new Thread(Serve) { IsBackground = true, Name = "opencode-selected-catalog-fixture" };
        _worker.Start();
    }

    public string[] GetRequestLines()
    {
        lock (_sync) { return _requestLines.ToArray(); }
    }

    private void Serve()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client = null;
            try { client = _listener.AcceptTcpClient(); }
            catch (SocketException) { if (_stop.IsCancellationRequested) return; else continue; }
            catch (ObjectDisposedException) { return; }
            try { ServeClient(client); }
            catch { if (client != null) client.Dispose(); }
        }
    }

    private void ServeClient(TcpClient client)
    {
        using (client)
        using (NetworkStream stream = client.GetStream())
        using (StreamReader reader = new StreamReader(stream, Encoding.ASCII, false, 1024, true))
        {
            string requestLine = reader.ReadLine();
            if (String.IsNullOrEmpty(requestLine)) return;
            string header;
            while (!String.IsNullOrEmpty(header = reader.ReadLine())) { }
            lock (_sync) { _requestLines.Add(requestLine); }
            string[] parts = requestLine.Split(' ');
            string target = parts.Length > 1 ? parts[1] : String.Empty;
            if (parts.Length == 0 || parts[0] != "GET")
            {
                WriteResponse(stream, 405, "{\"error\":\"method\"}");
                return;
            }
            if (target.StartsWith("/stall", StringComparison.Ordinal))
            {
                byte[] headers = Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\nConnection: close\r\n\r\n");
                stream.Write(headers, 0, headers.Length);
                _stop.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(10));
                return;
            }

            string body;
            if (target.StartsWith("/api/info", StringComparison.Ordinal))
            {
                body = JsonSerializer.Serialize(new {
                    pid = 42424,
                    version = "fixture-version",
                    urls = new[] { "http://127.0.0.1:" + Port.ToString() },
                    paths = new { tmp = Path.GetTempPath() }
                });
            }
            else if (target.StartsWith("/api/location?", StringComparison.Ordinal))
            {
                body = JsonSerializer.Serialize(new { directory = _workspace, project = new { id = "fixture-project" } });
            }
            else if (target.StartsWith("/api/model?", StringComparison.Ordinal))
            {
                string modelId = CatalogMismatch ? "stepfun/step-5-preview-free-other" : "stepfun/step-5-preview-free";
                body = JsonSerializer.Serialize(new {
                    data = new[] { new { providerID = "kilo", id = modelId, enabled = true, variants = new[] { new { id = "high" } } } },
                    location = new { directory = _workspace }
                });
            }
            else
            {
                WriteResponse(stream, 404, "{\"error\":\"path\"}");
                return;
            }
            WriteResponse(stream, 200, body);
        }
    }

    private static void WriteResponse(NetworkStream stream, int status, string body)
    {
        byte[] content = Encoding.UTF8.GetBytes(body);
        string reason = status == 200 ? "OK" : status == 404 ? "Not Found" : "Method Not Allowed";
        byte[] headers = Encoding.ASCII.GetBytes("HTTP/1.1 " + status.ToString() + " " + reason + "\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: " + content.Length.ToString() + "\r\nConnection: close\r\n\r\n");
        stream.Write(headers, 0, headers.Length);
        stream.Write(content, 0, content.Length);
        stream.Flush();
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        if (!_worker.Join(TimeSpan.FromSeconds(3))) throw new TimeoutException("Fixture server did not stop within its bound.");
        _stop.Dispose();
    }
}
'@ -ErrorAction Stop

$fixtureParent = [System.IO.Path]::TrimEndingDirectorySeparator([System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()))
$fixtureLeaf = 'opencode-selected-catalog-' + [Guid]::NewGuid().ToString('N')
$fixtureRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::Combine($fixtureParent, $fixtureLeaf))
$workspace = Join-Path $fixtureRoot 'workspace with spaces'
$connectionFile = Join-Path $fixtureRoot 'selected-connection.json'
$server = $null
$handler = $null
$client = $null
$fixturePassed = $false
try {
    [void][System.IO.Directory]::CreateDirectory($workspace)
    $server = [OpenCodeSelectedCatalogFixtureServer]::new($workspace)
    $endpoint = [Uri]::new("http://127.0.0.1:$($server.Port)/", [UriKind]::Absolute)
    $credentialUser = 'catalog-fixture-user'
    $credentialPassword = 'catalog-fixture-password'
    [ordered]@{
        schema_version = 1
        endpoint = $endpoint.AbsoluteUri
        pid = 42424
        username = $credentialUser
        password = $credentialPassword
    } | ConvertTo-Json -Compress | Set-Content -LiteralPath $connectionFile -Encoding utf8 -NoNewline

    $contract = [pscustomobject]@{
        module_id = 'eliot.opencode.v2'
        artifact_id = 'eliot-opencode-v2.rust-http.1'
        expected_provider = 'kilo'
        expected_model = 'stepfun/step-5-preview-free'
        expected_effort = 'high'
    }
    $routeModel = [pscustomobject]@{ provider_id = 'kilo'; model_id = 'stepfun/step-5-preview-free'; variant = 'high' }
    $attachedRoute = [ordered]@{
        alias = 'selected-catalog-fixture'
        enabled = $true
        runtime = 'module'
        module_artifact_id = $contract.artifact_id
        owned_service = $null
        native_options = [ordered]@{
            service_id = 'catalog-fixture-service'
            connection_file = $connectionFile
            directory = $workspace
        }
    }

    $capture = Get-OpenCodeSelectedCatalogEvidence -Route $attachedRoute -RouteModel $routeModel -Contract $contract -Mode 'pre_dispatch' -OperationScope ([ordered]@{ task_id = 'fixture-task'; attempt_id = 'fixture-attempt'; binding_id = 'fixture-binding'; generation = 1 })
    $decision = Test-OpenCodeSelectedCatalogEvidence -Evidence $capture -Contract $contract
    $captureJson = ConvertTo-Json -InputObject $capture -Depth 20 -Compress
    Assert-Fixture ($capture.capture_status -ceq 'captured') "Actual loopback catalog capture did not complete: $captureJson"
    Assert-Fixture ($decision.dispatch_allowed -and $decision.qualified -and $decision.stage_status -ceq 'passed') 'Matching captured catalog did not pass its consumer gate.'
    Assert-Fixture ($capture.scope.requested_model.provider_id -ceq 'kilo' -and $capture.scope.requested_model.model_id -ceq 'stepfun/step-5-preview-free' -and $capture.scope.requested_model.variant -ceq 'high') 'Requested native model coordinate or high variant changed.'
    Assert-Fixture ($capture.predicates.catalog_location_matches_route -and $capture.predicates.selected_model_enabled_is_true -and $capture.predicates.matching_model_count -eq 1 -and $capture.predicates.matching_variant_count -eq 1) 'Native catalog location, enabled, or exact-one predicates failed.'
    Assert-Fixture ($capture.selected_pid -eq 42424 -and $capture.responses.model_catalog.status -eq 200 -and $capture.responses.model_catalog.body_sha256 -match '^[0-9a-f]{64}$') 'Sanitized selected-server response evidence is incomplete.'
    foreach ($secret in @($credentialUser, $credentialPassword, $endpoint.AbsoluteUri, $connectionFile)) {
        Assert-Fixture (-not $captureJson.Contains($secret, [StringComparison]::Ordinal)) 'Evidence serialized a credential, endpoint, or connection-record path.'
    }
    Assert-Fixture (-not $captureJson.Contains('providerID', [StringComparison]::Ordinal) -and -not $captureJson.Contains('stepfun/step-5-preview-free-other', [StringComparison]::Ordinal)) 'Evidence retained raw catalog content.'

    $requestLines = $server.GetRequestLines()
    $locationLine = @($requestLines | Where-Object { $_.StartsWith('GET /api/location?', [StringComparison]::Ordinal) })
    $catalogLine = @($requestLines | Where-Object { $_.StartsWith('GET /api/model?', [StringComparison]::Ordinal) })
    Assert-Fixture ($locationLine.Count -eq 1 -and $catalogLine.Count -eq 1) 'Expected actual GET requests to selected location and model catalog endpoints.'
    $decodedCatalogTarget = [Uri]::UnescapeDataString(($catalogLine[0].Split(' ')[1]).Replace('+', ' '))
    Assert-Fixture ($decodedCatalogTarget.Contains('location[directory]=' + $workspace, [StringComparison]::Ordinal)) 'Catalog request omitted the exact selected workspace query.'

    $server.CatalogMismatch = $true
    $mismatch = Get-OpenCodeSelectedCatalogEvidence -Route $attachedRoute -RouteModel $routeModel -Contract $contract -Mode 'pre_dispatch'
    $mismatchDecision = Test-OpenCodeSelectedCatalogEvidence -Evidence $mismatch -Contract $contract
    Assert-Fixture ($mismatch.capture_status -ceq 'captured' -and -not $mismatchDecision.dispatch_allowed -and $mismatchDecision.stage_status -ceq 'blocked') 'A proved attached-route catalog mismatch did not block dispatch.'
    $server.CatalogMismatch = $false

    $ownedRoute = [ordered]@{
        alias = 'fresh-owned-fixture'
        enabled = $true
        runtime = 'module'
        module_artifact_id = $contract.artifact_id
        owned_service = [ordered]@{ origin = 'fresh_owned_service' }
        native_options = [ordered]@{ directory = $workspace }
    }
    $requestCountBeforeOwned = $server.GetRequestLines().Count
    $ownedEvidence = Get-OpenCodeSelectedCatalogEvidence -Route $ownedRoute -RouteModel $routeModel -Contract $contract -Mode 'pre_dispatch'
    $ownedDecision = Test-OpenCodeSelectedCatalogEvidence -Evidence $ownedEvidence -Contract $contract
    $ownedStageFacts = Get-OpenCodeSelectedCatalogStageFacts -Evidence $ownedEvidence
    Assert-Fixture ($ownedEvidence.capture_status -ceq 'unknown' -and $ownedEvidence.reason_code -ceq 'OPENCODE_SELECTED_CATALOG_IDENTITY_UNKNOWN') 'Fresh-owned missing public identity was not represented as unknown.'
    Assert-Fixture ($ownedDecision.dispatch_allowed -and -not $ownedDecision.qualified -and $ownedDecision.stage_status -ceq 'unknown') 'Unavailable supplement blocked dispatch or qualified the run.'
    Assert-Fixture ($ownedStageFacts.service_id -eq $null -and $ownedStageFacts.selected_pid -eq $null -and $ownedStageFacts.identity_limitation -is [string]) 'Fresh-owned unknown stage facts were not safe under StrictMode.'
    Assert-Fixture ($server.GetRequestLines().Count -eq $requestCountBeforeOwned) 'Fresh-owned unknown path guessed an endpoint or attempted catalog I/O.'

    $minimalFailure = [ordered]@{
        capture_status = 'unavailable'
        reason_code = 'OPENCODE_SELECTED_CATALOG_CAPTURE_UNAVAILABLE'
        scope = [ordered]@{ mode = 'pre_dispatch' }
    }
    $minimalDecision = Test-OpenCodeSelectedCatalogEvidence -Evidence $minimalFailure -Contract $contract
    $minimalStageFacts = Get-OpenCodeSelectedCatalogStageFacts -Evidence $minimalFailure
    Assert-Fixture (-not $minimalDecision.dispatch_allowed -and $minimalDecision.stage_status -ceq 'blocked' -and $minimalStageFacts.service_id -eq $null) 'Early unavailable evidence was not safely rejected under StrictMode.'

    $handler = [System.Net.Http.HttpClientHandler]::new()
    $handler.AllowAutoRedirect = $false
    $handler.UseProxy = $false
    $client = [System.Net.Http.HttpClient]::new($handler)
    $client.Timeout = [System.Threading.Timeout]::InfiniteTimeSpan
    $authToken = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($credentialUser + ':' + $credentialPassword))
    $clock = [System.Diagnostics.Stopwatch]::StartNew()
    $stalled = Invoke-OpenCodeSelectedJsonGet -Client $client -Endpoint $endpoint -Path '/stall' -AuthorizationToken $authToken -TimeoutSeconds 1
    $clock.Stop()
    Assert-Fixture ($stalled.status -eq 200 -and $stalled.parse_status -ceq 'timeout') "Headers-only stalled response did not terminate through the explicit cancellation token: $($stalled.status)/$($stalled.parse_status) after $($clock.Elapsed.TotalSeconds) seconds."
    Assert-Fixture ($clock.Elapsed.TotalSeconds -lt 4) 'Stalled response-body read exceeded its bounded fixture deadline.'
    $authToken = $null

    $requestLines = $server.GetRequestLines()
    Assert-Fixture (@($requestLines | Where-Object { -not $_.StartsWith('GET ', [StringComparison]::Ordinal) }).Count -eq 0) 'Fixture observed a non-GET or mutation request.'
    $harnessText = [System.IO.File]::ReadAllText($harnessPath)
    $readyIndex = $harnessText.IndexOf("Add-Stage -Name 'module_open_and_hello' -Status 'passed'", [StringComparison]::Ordinal)
    $catalogIndex = $harnessText.IndexOf("-Mode 'pre_dispatch'", [StringComparison]::Ordinal)
    $c8Index = $harnessText.IndexOf("Confirm-CodexStillAttached)) { Stop-Qualification 'CODEX_APP_SERVER_CHANGED_BEFORE_DISPATCH'", [StringComparison]::Ordinal)
    $dispatchIndex = $harnessText.IndexOf('$dispatch = Invoke-ManagerCall -Method ''task.dispatch''', [StringComparison]::Ordinal)
    $dispatchCalls = [regex]::Matches($harnessText, 'Invoke-ManagerCall\s+-Method\s+''task\.dispatch''').Count
    Assert-Fixture ($readyIndex -ge 0 -and $readyIndex -lt $catalogIndex -and $catalogIndex -lt $c8Index -and $c8Index -lt $dispatchIndex) 'Supplemental unknown handling bypassed or replaced the existing readiness/C8 dispatch gate.'
    Assert-Fixture ($dispatchCalls -eq 1 -and $harnessText.Contains('task_dispatch_calls_max = 1', [StringComparison]::Ordinal) -and $harnessText.Contains('mutation_replay = $false', [StringComparison]::Ordinal)) 'Harness no-replay or max-one-dispatch boundary changed.'

    $fixturePassed = $true
    [pscustomobject]@{
        status = 'PASS'
        loopback_capture = $capture.capture_status
        catalog_model = 'kilo/stepfun/step-5-preview-free'
        variant = 'high'
        attached_mismatch = $mismatchDecision.stage_status
        fresh_owned_supplement = $ownedDecision.stage_status
        fresh_owned_dispatch_allowed = $ownedDecision.dispatch_allowed
        stalled_body = $stalled.parse_status
        stalled_elapsed_seconds = [Math]::Round($clock.Elapsed.TotalSeconds, 3)
        http_requests = $requestLines.Count
        task_dispatch_calls_in_harness = $dispatchCalls
        model_or_native_calls = 0
    } | ConvertTo-Json -Compress
}
finally {
    if ($null -ne $client) { $client.Dispose() }
    if ($null -ne $handler) { $handler.Dispose() }
    if ($null -ne $server) { $server.Dispose() }
    if (-not $fixturePassed) {
        Write-Warning "Failed fixture retained at $fixtureRoot"
    }
    elseif ([System.IO.Directory]::Exists($fixtureRoot)) {
        $resolvedFixtureRoot = [System.IO.Path]::GetFullPath($fixtureRoot)
        $resolvedFixtureParent = [System.IO.Path]::GetDirectoryName($resolvedFixtureRoot)
        $resolvedFixtureLeaf = [System.IO.Path]::GetFileName($resolvedFixtureRoot)
        if (-not [string]::Equals($resolvedFixtureParent, $fixtureParent, [StringComparison]::OrdinalIgnoreCase) -or
            $resolvedFixtureLeaf -cne $fixtureLeaf -or
            -not $resolvedFixtureLeaf.StartsWith('opencode-selected-catalog-', [StringComparison]::Ordinal)) {
            throw 'Refusing recursive fixture cleanup outside its expected temporary directory.'
        }
        $fixtureAttributes = [System.IO.File]::GetAttributes($resolvedFixtureRoot)
        if (($fixtureAttributes -band [System.IO.FileAttributes]::Directory) -eq 0 -or
            ($fixtureAttributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'Refusing recursive fixture cleanup for a non-directory or reparse-point target.'
        }
        [System.IO.Directory]::Delete($resolvedFixtureRoot, $true)
    }
}
