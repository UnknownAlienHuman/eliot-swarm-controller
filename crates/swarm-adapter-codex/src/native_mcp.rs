//! Read-only Codex app-server MCP proof for one exact launch-issued Participant.
//!
//! This observes the current thread's native MCP inventory and makes two
//! harmless calls through the same thread-scoped server. It never starts a
//! model turn, writes Codex configuration, or receives a credential token.

use super::{
    AdapterError, Checkpoint, Journal, NativeClient, NativeRootPhase, OperationRecord,
    RuntimeCommand, outcome,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};
use swarm_contracts::{
    module_contract::ModuleContractClaim,
    native_mcp::{
        CODEX_NATIVE_MCP_READBACK_KIND, CODEX_NATIVE_MCP_SERVER_INFO_NAME,
        CodexNativeMcpCommand, CodexNativeMcpReadback, codex_participant_toolset,
    },
    runtime::EffectOutcome,
};

const MAX_STATUS_PAGES: usize = 16;
const STATUS_PAGE_LIMIT: u64 = 64;
const MAX_STATUS_BYTES: usize = 1024 * 1024;

fn now_ms() -> Result<i64, ReadFailure> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ReadFailure::Rejected("CODEX_CLOCK_INVALID"))?;
    i64::try_from(duration.as_millis()).map_err(|_| ReadFailure::Rejected("CODEX_CLOCK_INVALID"))
}

enum ReadFailure {
    Rejected(&'static str),
    Unknown(&'static str),
}

pub(super) async fn observe_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    native: &NativeClient,
    claim: &ModuleContractClaim,
    boot_id: &str,
) -> Result<super::RuntimeOutcome, AdapterError> {
    if !super::module_contract::codex_native_mcp_enabled(claim) {
        return Ok(super::rejected(
            command,
            "CODEX_NATIVE_MCP_CONTRACT_UNAVAILABLE",
            &journal.state,
        ));
    }
    let mut raw = command.input.clone();
    let effect = raw
        .as_object_mut()
        .and_then(|object| object.remove("effect"))
        .ok_or(AdapterError::HostProtocol)?;
    let request: CodexNativeMcpCommand =
        serde_json::from_value(raw).map_err(|_| AdapterError::HostProtocol)?;
    request
        .validate_against(command)
        .map_err(|_| AdapterError::HostProtocol)?;
    if effect != request.effect_identity()
        || request.expected_adapter_boot_id != boot_id
        || !current_thread_matches(&journal.state, native, &request)
    {
        return Ok(super::rejected(
            command,
            "CODEX_NATIVE_MCP_SCOPE_MISMATCH",
            &journal.state,
        ));
    }

    if let Some(previous) = journal.state.operations.get(&command.operation_id) {
        if previous.method != command.method
            || previous.kind != "native_mcp_observe"
            || previous.input_sha256.as_deref()
                != command.input_sha256.as_ref().map(|digest| digest.as_str())
        {
            return Err(AdapterError::HostProtocol);
        }
        // The prior intent may have reached the native MCP server. Preserve
        // the exact operation identity and never issue another native call.
        let mut result = outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            Some(&request.native_root_id),
            Some(&request.native_scope_key),
            None,
            None,
            json!({
                "diagnostic_code":"CODEX_NATIVE_MCP_READBACK_OUTCOME_UNKNOWN",
                "native_replay":false,
                "dispatch_permitted":false,
                "model_consumed":"unknown",
            }),
        );
        if previous.outcome.is_none()
            && let Some(record) = journal.state.operations.get_mut(&command.operation_id)
        {
            record.outcome = Some(
                serde_json::to_value(&result).map_err(|_| AdapterError::Checkpoint)?,
            );
            record.state = "outcome_unknown".into();
            journal.save()?;
        }
        result.details["native_mcp_action"] = json!(command.method);
        return Ok(result);
    }

    let mut intent = OperationRecord::intent("native.mcp.observe", "native_mcp_observe");
    intent.input_sha256 = command
        .input_sha256
        .as_ref()
        .map(|digest| digest.as_str().to_owned());
    intent.native_root_id = Some(request.native_root_id.clone());
    intent.native_scope_key = Some(request.native_scope_key.clone());
    intent.requested_model_provider = journal.state.requested_model_provider.clone();
    intent.requested_model = journal.state.requested_model.clone();
    intent.workspace_root = journal.state.workspace_root.clone();
    journal
        .state
        .operations
        .insert(command.operation_id.clone(), intent);
    journal.save()?;

    let readback = match read_current_thread(native, &journal.state, &request).await {
        Ok(readback) => readback,
        Err(ReadFailure::Rejected(code)) => {
            let mut result = outcome(
                &command.operation_id,
                EffectOutcome::Rejected,
                Some(&request.native_root_id),
                Some(&request.native_scope_key),
                None,
                None,
                json!({
                    "diagnostic_code":code,
                    "native_replay":false,
                    "dispatch_permitted":false,
                    "model_consumed":"unknown",
                }),
            );
            result.details["native_mcp_action"] = json!(command.method);
            return Ok(result);
        }
        Err(ReadFailure::Unknown(code)) => {
            let mut result = outcome(
                &command.operation_id,
                EffectOutcome::Unknown,
                Some(&request.native_root_id),
                Some(&request.native_scope_key),
                None,
                None,
                json!({
                    "diagnostic_code":code,
                    "native_replay":false,
                    "dispatch_permitted":false,
                    "model_consumed":"unknown",
                }),
            );
            result.details["native_mcp_action"] = json!(command.method);
            return Ok(result);
        }
    };
    if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
        record.thread_configuration_readback = Some("verified".into());
    }
    let mut result = outcome(
        &command.operation_id,
        EffectOutcome::Applied,
        Some(&request.native_root_id),
        Some(&request.native_scope_key),
        None,
        None,
        json!({
            "completion_condition":"exact_codex_thread_participant_mcp_readback",
            "native_replay":false,
            "dispatch_permitted":false,
            "model_consumed":"unknown",
            "codex_native_mcp_readback":readback,
        }),
    );
    result.details["native_mcp_action"] = json!(command.method);
    Ok(result)
}

fn current_thread_matches(
    state: &Checkpoint,
    native: &NativeClient,
    request: &CodexNativeMcpCommand,
) -> bool {
    let (Some(root), Some(scope), Some(workspace)) = (
        state.active_root_id(),
        state.native_scope_key.as_deref(),
        state.workspace_root.as_deref(),
    ) else {
        return false;
    };
    state.native_root_phase == Some(NativeRootPhase::Active)
        && state.root_operation_id.as_deref() == Some(request.open_operation_id.as_str())
        && root == request.native_root_id
        && scope == request.native_scope_key
        && native.identity_is_known()
        && native.scope_key() == Some(request.native_scope_key.as_str())
        && state.is_current_root(
            root,
            scope,
            state.requested_model_provider.as_deref(),
            state.requested_model.as_deref(),
            workspace,
        )
}

async fn read_current_thread(
    native: &NativeClient,
    state: &Checkpoint,
    request: &CodexNativeMcpCommand,
) -> Result<CodexNativeMcpReadback, ReadFailure> {
    let identity = native
        .server_identity
        .as_ref()
        .ok_or(ReadFailure::Rejected("CODEX_NATIVE_IDENTITY_UNAVAILABLE"))?;
    let thread = native
        .read_thread(&request.native_root_id)
        .await
        .map_err(|_| ReadFailure::Unknown("CODEX_THREAD_READ_UNKNOWN"))?;
    if thread["thread"]["id"].as_str() != Some(request.native_root_id.as_str())
        || thread["thread"]["cwd"].as_str().and_then(super::normalize_path)
            != state.workspace_root.as_deref().and_then(super::normalize_path)
        || !matches_native_model(&thread["thread"], state)
    {
        return Err(ReadFailure::Rejected("CODEX_THREAD_READBACK_MISMATCH"));
    }

    let statuses =
        list_thread_mcp_status(native, &request.native_root_id, &request.server_name).await?;
    let mut matching = statuses.iter().filter(|status| {
        status["name"].as_str() == Some(request.server_name.as_str())
            && status["serverInfo"]["name"].as_str()
                == Some(CODEX_NATIVE_MCP_SERVER_INFO_NAME)
    });
    let status = matching
        .next()
        .ok_or(ReadFailure::Rejected("CODEX_PARTICIPANT_MCP_SERVER_MISSING"))?;
    if matching.next().is_some()
        || status["runtimeStatus"] != "connected"
        || !status["toolsError"].is_null()
        || status["serverInfo"]["version"].as_str().is_none_or(str::is_empty)
    {
        return Err(ReadFailure::Rejected("CODEX_PARTICIPANT_MCP_SERVER_NOT_READY"));
    }
    let (expected_tools, expected_digest) = codex_participant_toolset()
        .map_err(|_| ReadFailure::Rejected("CODEX_PARTICIPANT_CATALOG_INVALID"))?;
    let actual_tools = actual_tools(status)?;
    if actual_tools != expected_tools
        || expected_digest.as_str() != request.expected_toolset_sha256.as_str()
    {
        return Err(ReadFailure::Rejected("CODEX_PARTICIPANT_TOOLSET_MISMATCH"));
    }

    let server_id = status["name"]
        .as_str()
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or(ReadFailure::Rejected("CODEX_PARTICIPANT_MCP_SERVER_ID_INVALID"))?;
    let tool_search = crate::swarm_contracts::mcp_catalog::tool_name("swarm.tools.search");
    let catalog = call_public_tool(
        native,
        &request.native_root_id,
        server_id,
        &tool_search,
        json!({
            "query":"context",
            "exact_method":"swarm.context.get",
            "task_id":request.task_id,
        }),
    )
    .await?;
    if catalog["authorization_revision"].as_str()
        != Some(request.expected_authorization_revision.as_str())
        || catalog["authorization_basis"] != "authenticated_store_scope"
        || !catalog["matches"].as_array().is_some_and(|matches| {
            matches.iter().any(|entry| entry["method"] == "swarm.context.get")
        })
    {
        return Err(ReadFailure::Rejected("CODEX_PARTICIPANT_AUTHORIZATION_MISMATCH"));
    }

    let context_tool = crate::swarm_contracts::mcp_catalog::tool_name("swarm.context.get");
    let context = call_public_tool(
        native,
        &request.native_root_id,
        server_id,
        &context_tool,
        json!({
            "task_id":request.task_id,
            "task_revision":request.task_revision,
            "attempt_id":request.attempt_id,
        }),
    )
    .await?;
    let assignment = &context["runtime_capability"]["authenticated_assignment"];
    if context["scope"]["participant"]["role"] != "participant"
        || context["scope"]["participant"]["client_id"] != request.participant_id
        || context["scope"]["participant"]["grant_revision"] != request.grant_revision
        || context["scope"]["task"]["task_id"] != request.task_id
        || context["scope"]["task"]["revision"] != request.task_revision
        || context["scope"]["attempt"]["attempt_id"] != request.attempt_id
        || context["scope"]["attempt"]["binding_id"] != request.binding_id
        || context["scope"]["attempt"]["binding_generation"] != request.binding_generation
        || assignment["participant_id"] != request.participant_id
        || assignment["grant_revision"] != request.grant_revision
        || assignment["task_id"] != request.task_id
        || assignment["task_revision"] != request.task_revision
        || assignment["attempt_id"] != request.attempt_id
        || assignment["binding_id"] != request.binding_id
        || assignment["binding_generation"] != request.binding_generation
        || assignment["native_session_id"].as_str()
            != request.participant_native_session_id.as_deref()
        || context["scope"]["task"]["state"] != "open"
        || context["scope"]["attempt"]["state"] != "reserved"
        || context["runtime_capability"]["authenticated_assignment"]["basis_kind"]
            != "attempt_owner"
        || context["runtime_capability"]["profile"] != "participant"
        || context["runtime_capability"]["surface"] != request.surface
    {
        return Err(ReadFailure::Rejected("CODEX_PARTICIPANT_CONTEXT_MISMATCH"));
    }
    let observed_at_ms = now_ms()?;
    let mut readback = CodexNativeMcpReadback {
        schema_version: 2,
        kind: CODEX_NATIVE_MCP_READBACK_KIND.to_owned(),
        operation_id: request.operation_id.clone(),
        launch_operation_id: request.launch_operation_id.clone(),
        assignment_sha256: request.assignment_sha256.clone(),
        binding_id: request.binding_id.clone(),
        binding_generation: request.binding_generation,
        open_operation_id: request.open_operation_id.clone(),
        native_root_id: request.native_root_id.clone(),
        native_scope_key: request.native_scope_key.clone(),
        adapter_boot_id: request.expected_adapter_boot_id.clone(),
        reported_product: identity.reported_product.clone(),
        reported_version: identity.reported_version.clone(),
        effective_model_provider: thread["thread"]["modelProvider"].as_str().ok_or(ReadFailure::Rejected("CODEX_THREAD_MODEL_PROVIDER_MISSING"))?.to_owned(),
        effective_model: thread["thread"]["model"].as_str().ok_or(ReadFailure::Rejected("CODEX_THREAD_MODEL_MISSING"))?.to_owned(),
        server_info_name: CODEX_NATIVE_MCP_SERVER_INFO_NAME.to_owned(),
        server_info_version: status["serverInfo"]["version"]
            .as_str()
            .ok_or(ReadFailure::Rejected("CODEX_MCP_SERVER_VERSION_MISSING"))?
            .to_owned(),
        server_id_sha256: Sha256Digest::new(super::digest_hex(server_id.as_bytes()))
            .map_err(|_| ReadFailure::Rejected("CODEX_MCP_SERVER_ID_DIGEST_INVALID"))?,
        runtime_status: "connected".to_owned(),
        participant_id: request.participant_id.clone(),
        task_id: request.task_id.clone(),
        task_revision: request.task_revision,
        attempt_id: request.attempt_id.clone(),
        grant_revision: request.grant_revision,
        participant_native_session_id: request.participant_native_session_id.clone(),
        surface: request.surface.clone(),
        authorization_revision: request.expected_authorization_revision.clone(),
        toolset_sha256: expected_digest,
        tool_count: u32::try_from(actual_tools.len())
            .map_err(|_| ReadFailure::Rejected("CODEX_TOOL_COUNT_INVALID"))?,
        observed_at_ms,
        evidence_sha256: Sha256Digest::new("0".repeat(64))
            .map_err(|_| ReadFailure::Rejected("CODEX_READBACK_DIGEST_INVALID"))?,
    };
    readback.evidence_sha256 = readback
        .computed_evidence_sha256()
        .map_err(|_| ReadFailure::Rejected("CODEX_READBACK_DIGEST_INVALID"))?;
    readback
        .validate()
        .map_err(|_| ReadFailure::Rejected("CODEX_READBACK_INVALID"))?;
    Ok(readback)
}

fn matches_native_model(thread: &Value, state: &Checkpoint) -> bool {
    let observed_provider = thread["modelProvider"].as_str();
    let observed_model = thread["model"].as_str();
    let requested_pair = state.requested_model_provider.is_some()
        && state.requested_model.is_some();
    if state.requested_model_provider.is_some() != state.requested_model.is_some() {
        return false;
    }
    if requested_pair
        && (observed_provider != state.requested_model_provider.as_deref()
            || observed_model != state.requested_model.as_deref())
    {
        return false;
    }
    if !requested_pair
        && (observed_provider.is_none_or(str::is_empty)
            || observed_model.is_none_or(str::is_empty))
    {
        return false;
    }
    if observed_provider != state.effective_model_provider.as_deref()
        || observed_model != state.effective_model.as_deref()
    {
        return false;
    }
    true
}

async fn list_thread_mcp_status(
    native: &NativeClient,
    thread_id: &str,
    server_name: &str,
) -> Result<Vec<Value>, ReadFailure> {
    let mut cursor: Option<String> = None;
    let mut seen = BTreeSet::new();
    let mut statuses = Vec::new();
    for _ in 0..MAX_STATUS_PAGES {
        let mut params = json!({
            "threadId":thread_id,
            "serverName":server_name,
            "detail":"toolsAndAuthOnly",
            "limit":STATUS_PAGE_LIMIT,
        });
        if let Some(value) = &cursor {
            params["cursor"] = json!(value);
        }
        let response = native
            .request("mcpServerStatus/list", params)
            .await
            .map_err(|_| ReadFailure::Unknown("CODEX_MCP_STATUS_OUTCOME_UNKNOWN"))?;
        let page = response["data"]
            .as_array()
            .ok_or(ReadFailure::Rejected("CODEX_MCP_STATUS_INVALID"))?;
        statuses.extend(page.iter().cloned());
        if serde_json::to_vec(&statuses)
            .map_err(|_| ReadFailure::Rejected("CODEX_MCP_STATUS_INVALID"))?
            .len()
            > MAX_STATUS_BYTES
        {
            return Err(ReadFailure::Rejected("CODEX_MCP_STATUS_TOO_LARGE"));
        }
        cursor = match response.get("nextCursor") {
            Some(Value::String(next)) if !next.is_empty() => {
                if !seen.insert(next.clone()) {
                    return Err(ReadFailure::Rejected("CODEX_MCP_STATUS_CURSOR_REPEATED"));
                }
                Some(next.clone())
            }
            Some(Value::Null) | None => return Ok(statuses),
            _ => return Err(ReadFailure::Rejected("CODEX_MCP_STATUS_INVALID")),
        };
    }
    Err(ReadFailure::Rejected("CODEX_MCP_STATUS_PAGE_LIMIT"))
}

fn actual_tools(status: &Value) -> Result<Vec<Value>, ReadFailure> {
    let tools = status["tools"]
        .as_object()
        .ok_or(ReadFailure::Rejected("CODEX_MCP_TOOLS_MISSING"))?;
    let mut output = Vec::with_capacity(tools.len());
    for (name, tool) in tools {
        let schema = tool["inputSchema"]
            .as_object()
            .map(|_| javascript_schema(&tool["inputSchema"]))
            .transpose()?
            .ok_or(ReadFailure::Rejected("CODEX_MCP_TOOL_SCHEMA_MISSING"))?;
        output.push(json!({"name":name,"input_schema":schema}));
    }
    output.sort_by(|left, right| {
        left["name"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["name"].as_str().unwrap_or_default())
    });
    Ok(output)
}

fn javascript_schema(value: &Value) -> Result<Value, ReadFailure> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or(ReadFailure::Rejected("CODEX_MCP_SCHEMA_INVALID")),
        Value::Array(values) => values
            .iter()
            .map(javascript_schema)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| Ok((key.clone(), javascript_schema(value)?)))
            .collect::<Result<serde_json::Map<String, Value>, ReadFailure>>()
            .map(Value::Object),
        other => Ok(other.clone()),
    }
}

async fn call_public_tool(
    native: &NativeClient,
    thread_id: &str,
    server_id: &str,
    tool: &str,
    arguments: Value,
) -> Result<Value, ReadFailure> {
    let response = native
        .request(
            "mcpServer/tool/call",
            json!({
                "threadId":thread_id,
                "server":server_id,
                "tool":tool,
                "arguments":arguments,
            }),
        )
        .await
        .map_err(|_| ReadFailure::Unknown("CODEX_MCP_TOOL_CALL_OUTCOME_UNKNOWN"))?;
    if response["isError"] == true {
        return Err(ReadFailure::Rejected("CODEX_MCP_TOOL_CALL_REJECTED"));
    }
    response
        .get("structuredContent")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or(ReadFailure::Rejected("CODEX_MCP_TOOL_RESULT_INVALID"))
}
