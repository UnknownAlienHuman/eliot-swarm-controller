//! MCP facade over the same application API used by the CLI.
//!
//! The General Manager's MCP client launches `swarm-mcp` as a child process
//! and speaks MCP over stdio. This process is only a client of the running
//! host: it forwards each tool call over the existing local IPC with the
//! configured credential, exactly like a CLI invocation. It never opens the
//! database, never opens a TCP listener, and closing it does not stop the
//! host or cancel admitted work (donor qualification points for `api.mcp`).
//!
//! Tool surface: one tool per registered frontend method, named after the
//! method with `.` replaced by `_`. There is deliberately no universal
//! passthrough or shell tool: every tool forwards to exactly one typed
//! application method, and the application layer keeps validating the
//! original request (field allow-lists, idempotency, role checks).
//!
//! Mutations follow the CLI's request-ID discipline. Restricted profiles
//! require a caller-known `client_request_id`, chosen and retained before
//! dispatch, so the caller can reconcile a lost reply without a hidden
//! retry. The explicit local `full` compatibility profile may omit it; in
//! that case one is generated and echoed only when a result arrives. A
//! failed transport is dropped, never silently retried: the next tool call
//! reconnects first.
//!
//! Tasks projection (R20, Documentation Program §17.1): the server
//! advertises the `io.modelcontextprotocol/tasks` extension. When the
//! client declares the same extension, a mutation whose Operation is
//! still in flight returns an MCP task seed whose `taskId` is the
//! existing Operation's ID; the Operation stays the single authority
//! for state, acceptance and completion, and this facade stores no task
//! state of its own. `tasks/get` projects the Operation read model
//! (`operation.get`, plus the Operation's exact pending native-input
//! attention items from `report.attention`, mapped to elicitation input
//! requests). `tasks/cancel` submits the existing addressed
//! `operation.cancel` — which only cancels queued Operations — so an
//! in-flight Operation refuses with the store's own reason instead of
//! inventing a parallel cancel path. RMCP's `TaskManager` is never
//! used: nothing in this facade executes work a second time. Clients
//! that do not declare the extension receive exactly the pre-Tasks
//! responses (the durable Operation handle in the structured result).
//! `tasks/update` is deliberately not implemented: a pending native
//! input is answered by the addressed `agent_reply` tool call, which is
//! a durable Operation of its own, not by a free-form elicitation
//! response.
//!
//! Subscriptions (R20, §17.3): the facade advertises the
//! `eliot/subscriptions` extension and answers two protocol methods of
//! its own, `eliot/subscribe` / `eliot/unsubscribe` (custom requests,
//! not tools). A subscription is a bounded, read-only freshness hint
//! over committed facts only — committed report transitions, mailbox
//! deliveries, Operation state transitions and Concilium transitions,
//! all filtered from the one committed observation stream `report.delta` reads, never from
//! a volatile or native live stream. Each subscription's queue holds
//! at most a fixed number of undelivered notifications; on overflow
//! the subscriber receives exactly one explicit `lagged` marker for
//! the skipped range and resyncs through the exact reads
//! (`report.delta` / `message.read` / `operation.get`, plus
//! `concilium.get` / `concilium.list` for Concilium facts) from the last
//! delivered cursor. Concilium notification items carry IDs and cursor
//! only. Notifications carry the S2 projection frame of
//! the page they were read from, so a subscriber can detect gaps
//! itself. Subscriptions die with the session: a reconnect is not
//! replay continuity, and state is re-established by cursor + resync.
//! The pollers share one dedicated IPC connection, separate from the
//! tool-call connection, under the same link discipline (see
//! `subscriptions.rs` for the full contract and the RMCP seam).
//! Fact: the facade keeps no cache; authoritative reads are forwarded
//! to the host with no stale-on-error caching.

use crate::config::{Config, Ipc, McpToolProfile};
use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, CancelTaskParams, ContentBlock,
        CreateTaskResult, CustomRequest, CustomResult, DetailedTask, ElicitRequest,
        ElicitRequestParams, ElicitationSchema, ExtensionCapabilities, GetTaskParams,
        GetTaskResult, Implementation, InputRequest, InputRequests, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig, Task as McpTask, TaskPayload,
        Tool, ToolAnnotations,
    },
    service::{Peer, RequestContext, RoleServer},
    transport::stdio,
};
use serde_json::{Map, Value, json};
use std::{collections::BTreeSet, path::PathBuf, sync::Arc};
use swarm_client::Client;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    mcp_catalog::{
        TOOLS, ToolSpec, effect_requires_preknown_request_id, input_schema,
        mutation_requires_caller_request_id, output_schema, tool_name,
    },
};
use tokio::sync::Mutex;
use uuid::Uuid;

type JsonObject = Map<String, Value>;

const PREKNOWN_REQUEST_ID_MESSAGE: &str =
    "caller-owned client_request_id is required before dispatch";

fn caller_request_id_present(params: &Value) -> bool {
    params
        .get("client_request_id")
        .and_then(Value::as_str)
        .is_some_and(|request_id| !request_id.trim().is_empty())
}

pub(crate) fn registered_application_methods() -> Vec<&'static str> {
    swarm_contracts::mcp_catalog::registered_application_methods()
}

pub fn application_method_read_only(method: &str) -> Option<bool> {
    swarm_contracts::mcp_catalog::application_method_read_only(method)
}
fn tool_from_spec(read_only: bool, spec: &ToolSpec, require_request_id: bool) -> Tool {
    let mut tool = Tool::new(
        tool_name(spec.method),
        spec.description,
        input_schema(spec, read_only, require_request_id),
    );
    if let Some(schema) = output_schema(spec.method) {
        tool = tool.with_raw_output_schema(schema);
    }
    if read_only {
        tool = tool.with_annotations(ToolAnnotations::new().read_only(true));
    }
    tool
}

fn find_tool(name: &str) -> Option<&'static (bool, ToolSpec)> {
    TOOLS
        .iter()
        .find(|(_, spec)| tool_name(spec.method) == name)
}

pub(crate) struct McpFacade {
    root: PathBuf,
    credential: Credential,
    ipc_config: Arc<Ipc>,
    client: Mutex<Option<Client>>,
    /// The subscription pollers' own IPC connection (§17.3): lazily
    /// connected, shared by every poller of this session, and never
    /// used for tool calls — a dead pump link cannot wedge a tool
    /// call, and a dropped tool link cannot stall a poller.
    pump_client: Arc<Mutex<Option<Client>>>,
    subscriptions: Arc<subscriptions::SubscriptionHub>,
}

impl McpFacade {
    pub fn new(root: PathBuf, credential: Credential, ipc_config: Arc<Ipc>) -> Self {
        Self {
            root,
            credential,
            ipc_config,
            client: Mutex::new(None),
            pump_client: Arc::new(Mutex::new(None)),
            subscriptions: Arc::new(subscriptions::SubscriptionHub::new(
                subscriptions::MAX_QUEUE_DEPTH,
                subscriptions::POLL_INTERVAL,
            )),
        }
    }

    /// One forwarded request over the lazily connected IPC link. A failed
    /// link is never reused or silently retried under a possibly
    /// different outcome; the next request reconnects first.
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        request_on(
            &self.client,
            &self.root,
            &self.credential,
            &self.ipc_config,
            method,
            params,
        )
        .await
    }

    /// `eliot/subscribe` (§17.3): open a bounded subscription over
    /// committed facts. The request's peer is captured for the
    /// subscription's forwarder; the acknowledgement names the
    /// starting cursor and the resync contract.
    async fn subscribe(
        &self,
        params: Option<Value>,
        peer: Peer<RoleServer>,
        profile: McpToolProfile,
    ) -> std::result::Result<CustomResult, McpError> {
        let params = params.unwrap_or_else(|| json!({}));
        let (categories, after) =
            subscriptions::parse_subscribe(&params).map_err(protocol_error)?;
        let authorization = self
            .request("mcp.authorization", json!({}))
            .await
            .map_err(|_| catalog_authorization_error())?;
        let authorization = parse_catalog_authorization(&authorization, None)
            .ok_or_else(catalog_authorization_error)?;
        let recovery_reads = [
            "report.delta",
            "message.read",
            "operation.get",
            "concilium.get",
            "concilium.list",
            "coordination.thread.get",
            "coordination.thread.list",
            "coordination.contract.get",
            "coordination.contract.list",
        ]
        .into_iter()
        .filter(|method| {
            profiles::exposes_method(profile, method)
                && authorization.allowed_methods.contains(*method)
        })
        .collect();
        let source = Arc::new(
            subscriptions::PumpSource::new(
                self.root.clone(),
                self.credential.clone(),
                self.ipc_config.clone(),
                self.pump_client.clone(),
            )
            .with_recovery_reads(recovery_reads),
        );
        let ack = self
            .subscriptions
            .subscribe(source, peer, categories, after)
            .await
            .map_err(protocol_error)?;
        Ok(CustomResult(ack))
    }

    /// `eliot/unsubscribe`: stop one subscription of this session.
    fn unsubscribe(&self, params: Option<Value>) -> std::result::Result<CustomResult, McpError> {
        let params = params.unwrap_or_else(|| json!({}));
        let id = subscriptions::parse_unsubscribe(&params).map_err(protocol_error)?;
        let ack = self
            .subscriptions
            .unsubscribe(&id)
            .map_err(protocol_error)?;
        Ok(CustomResult(ack))
    }

    async fn call(&self, method: &str, mut params: Value, read_only: bool) -> CallToolResult {
        let mut generated_request_id = None;
        if !read_only {
            if !params.is_object() {
                return tool_error(Error::invalid("tool arguments must be an object"));
            }
            if effect_requires_preknown_request_id(method) && !caller_request_id_present(&params) {
                return tool_error(Error::invalid(PREKNOWN_REQUEST_ID_MESSAGE));
            }
            if params.get("client_request_id").is_none() {
                let id = Uuid::new_v4().to_string();
                params["client_request_id"] = json!(id);
                generated_request_id = Some(id);
            }
        }
        match self.request(method, params).await {
            Ok(mut result) => {
                if let Some(id) = generated_request_id
                    && let Value::Object(map) = &mut result
                {
                    map.entry("client_request_id").or_insert_with(|| json!(id));
                }
                CallToolResult::structured(result)
            }
            Err(e) => tool_error(e),
        }
    }

    /// The full task projection of one Operation, read fresh from the
    /// host. This is a read model only: it stores nothing and executes
    /// nothing.
    async fn project_task(&self, task_id: &str) -> Result<DetailedTask> {
        let operation = self
            .request("operation.get", json!({"operation_id": task_id}))
            .await?;
        let inputs = if is_terminal_state(&operation) {
            InputRequests::new()
        } else {
            self.pending_inputs(&operation).await?
        };
        Ok(project_operation(&operation, inputs))
    }

    /// The Operation's pending native-input items, taken from the same
    /// `report.attention` projection managers read (§8.3) so there is
    /// exactly one definition of a pending attention item. Every page is
    /// read: a page cut could silently drop a pending request, which
    /// would understate what the task is waiting for.
    async fn pending_inputs(&self, operation: &Value) -> Result<InputRequests> {
        if operation["binding_id"].as_str().is_none() {
            return Ok(InputRequests::new());
        }
        let mut items = Vec::new();
        let mut after = 0_i64;
        loop {
            let page = self
                .request("report.attention", json!({"after": after, "limit": 200}))
                .await?;
            if let Some(batch) = page["items"].as_array() {
                items.extend(batch.iter().cloned());
            }
            let total = page["total_items"].as_i64().unwrap_or(0);
            let next = page["next_after"].as_i64().unwrap_or(after);
            if next <= after || next >= total {
                break;
            }
            after = next;
        }
        Ok(pending_input_requests(operation, &items))
    }

    /// Convert a finished mutation reply into the MCP task seed for its
    /// Operation — only for Tasks-negotiated clients, and only while
    /// the Operation is still in flight. Anything else (terminal
    /// Operations, error replies, replies without an Operation handle,
    /// a failed readback) keeps the plain result the caller would have
    /// received before the Tasks projection existed; a mutation that
    /// already succeeded never fails retroactively here.
    async fn task_seed_response(&self, result: CallToolResult) -> CallToolResponse {
        let Some(operation_id) = result
            .structured_content
            .as_ref()
            .filter(|_| result.is_error != Some(true))
            .and_then(|value| value["operation_id"].as_str().map(str::to_owned))
        else {
            return result.into();
        };
        match self.project_task(&operation_id).await {
            Ok(detailed) if !detailed.status().is_terminal() => {
                CallToolResponse::Task(CreateTaskResult::new(detailed.task))
            }
            _ => result.into(),
        }
    }
}

/// One forwarded request over one lazily connected IPC link slot.
/// The tool-call path and the subscription pump path each own a slot
/// and share exactly this discipline: connect on first use; on a
/// transport-class failure (the codes below) drop the link so the
/// next request reconnects first; never silently retry a request
/// whose outcome may differ on replay.
async fn request_on(
    slot: &Mutex<Option<Client>>,
    root: &std::path::Path,
    credential: &Credential,
    ipc_config: &Ipc,
    method: &str,
    params: Value,
) -> Result<Value> {
    let mut client = slot.lock().await;
    if client.is_none() {
        *client = Some(Client::connect(root, credential, ipc_config).await?);
    }
    let connected = client.as_mut().expect("client connected above");
    match connected.request(method, params).await {
        Ok(result) => Ok(result),
        Err(e) => {
            if matches!(
                e.code.as_str(),
                "DISCONNECTED"
                    | "OUTCOME_UNKNOWN"
                    | "PROTOCOL_ERROR"
                    | "IO_ERROR"
                    | "WRITE_TIMEOUT"
            ) {
                *client = None;
            }
            Err(e)
        }
    }
}

fn tool_error(error: Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(
        json!({"error": {"code": error.code, "message": error.message}}).to_string(),
    )])
}

/// Map an application error onto the JSON-RPC surface of `tasks/*`,
/// preserving the ELIOT code and message verbatim in `data`.
fn protocol_error(error: Error) -> McpError {
    let data = json!({"code": error.code, "message": error.message});
    match error.code.as_str() {
        "NOT_FOUND" | "INVALID_PARAMS" => {
            McpError::invalid_params(error.message.clone(), Some(data))
        }
        _ => McpError::internal_error(error.message.clone(), Some(data)),
    }
}

fn is_terminal_state(operation: &Value) -> bool {
    matches!(
        operation["state"].as_str(),
        Some("settled" | "rejected" | "cancelled")
    )
}

/// How `tasks/cancel` treats an Operation in this state. This mirrors
/// `operation.cancel` exactly: only a queued Operation can be cancelled
/// through it, a terminal one needs no cancellation, and an in-flight
/// one is refused — already-sent work requires native
/// cancellation/reconciliation, never local deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelAction {
    Ack,
    Submit,
    Refuse,
}

fn cancel_action(state: &str) -> CancelAction {
    match state {
        "settled" | "rejected" | "cancelled" => CancelAction::Ack,
        "queued" => CancelAction::Submit,
        _ => CancelAction::Refuse,
    }
}

/// Suggested `tasks/get` poll interval. Operations settle on native
/// evidence, which is slow relative to a local read; one second keeps
/// polling cheap without busy-looping the host.
const TASK_POLL_INTERVAL_MS: u64 = 1000;

/// Project one Operation record (the `operation.get` shape) into the
/// MCP `DetailedTask` shape. Pure: the caller supplies the Operation
/// and its pending input requests.
///
/// State mapping: `settled` → completed with the Operation's own
/// recorded result as the tool result; `rejected` → failed with the
/// recorded admission/native error preserved in `data`; `cancelled` →
/// cancelled; every in-flight state (`queued`, `sending`,
/// `native_accepted`, `outcome_unknown`, …) → working, or
/// input_required while exact native input requests are pending.
/// `outcome_unknown` stays working on purpose: the Operation has not
/// failed, its outcome is simply not yet proven, and the status
/// message names the exact ELIOT state.
fn project_operation(operation: &Value, input_requests: InputRequests) -> DetailedTask {
    let state = operation["state"].as_str().unwrap_or("unknown");
    let payload = match state {
        "settled" => TaskPayload::Completed {
            result: completed_result(operation),
        },
        "rejected" => TaskPayload::Failed {
            error: failure_error(operation),
        },
        "cancelled" => TaskPayload::Cancelled,
        _ if !input_requests.is_empty() => TaskPayload::InputRequired { input_requests },
        _ => TaskPayload::Working,
    };
    let task = McpTask::new(
        operation["operation_id"].as_str().unwrap_or_default(),
        payload.status(),
        iso8601_utc(operation["created_at_ms"].as_i64().unwrap_or(0)),
        iso8601_utc(operation["updated_at_ms"].as_i64().unwrap_or(0)),
    )
    .with_status_message(format!(
        "{} operation is {}",
        operation["method"].as_str().unwrap_or("unknown"),
        state
    ))
    .with_poll_interval_ms(TASK_POLL_INTERVAL_MS);
    DetailedTask::new(task, payload)
}

/// The terminal result of a settled Operation, in the same
/// `CallToolResult` shape a non-Tasks client receives in the mutation
/// reply: structured content is the Operation's recorded result.
fn completed_result(operation: &Value) -> Map<String, Value> {
    let result = operation["result"].clone();
    let structured = if result.is_object() {
        result
    } else {
        json!({"result": result})
    };
    match serde_json::to_value(CallToolResult::structured(structured)) {
        Ok(Value::Object(map)) => map,
        _ => Map::new(),
    }
}

/// The failure of a rejected Operation. The wire `code` is the generic
/// JSON-RPC internal-error code; the exact ELIOT error object is
/// preserved verbatim in `data`.
fn failure_error(operation: &Value) -> Map<String, Value> {
    let stored = operation["result"].clone();
    let message = stored["message"]
        .as_str()
        .unwrap_or("operation rejected")
        .to_string();
    let mut error = Map::new();
    error.insert("code".to_string(), json!(-32603));
    error.insert("message".to_string(), json!(message));
    error.insert("data".to_string(), stored);
    error
}

/// Filter the unified attention projection down to the Operation's own
/// pending native-input requests and map each to an elicitation input
/// request keyed by its native request ID. An item qualifies only by an
/// exact recorded address: kind `waiting_for_native_request` and the
/// Operation's own binding/generation. Nothing is paraphrased into a
/// form schema: the native request's own shape stays with the native
/// runtime, and the answer is the addressed `agent_reply` tool call.
fn pending_input_requests(operation: &Value, attention_items: &[Value]) -> InputRequests {
    let (Some(binding_id), Some(generation)) = (
        operation["binding_id"].as_str(),
        operation["binding_generation"].as_i64(),
    ) else {
        return InputRequests::new();
    };
    let mut requests = InputRequests::new();
    for item in attention_items {
        if item["kind"].as_str() != Some("waiting_for_native_request")
            || item["binding_id"].as_str() != Some(binding_id)
            || item["generation"].as_i64() != Some(generation)
        {
            continue;
        }
        let Some(request_id) = item["address"]["request_id"].as_str() else {
            continue;
        };
        requests.insert(
            request_id.to_string(),
            InputRequest::Elicitation(ElicitRequest::new(
                ElicitRequestParams::FormElicitationParams {
                    meta: None,
                    message: input_request_message(item),
                    requested_schema: ElicitationSchema::new(Default::default()),
                },
            )),
        );
    }
    requests
}

fn input_request_message(item: &Value) -> String {
    let address = &item["address"];
    let text = |key: &str| address[key].as_str().unwrap_or("unknown");
    let mut message = format!(
        "Native {} request {} in session {} is waiting for a decision",
        text("request_kind"),
        text("request_id"),
        text("session_id"),
    );
    if let Some(fingerprint) = address["fingerprint"].as_str() {
        message.push_str(&format!(" (fingerprint {fingerprint})"));
    }
    message.push_str(&format!(
        ". Answer it with the agent_reply tool for binding {} generation {}: \
         a reply is a durable addressed operation of its own, so \
         tasks/update is not supported for it.",
        text("binding_id"),
        address["generation"],
    ));
    message
}

/// Format epoch milliseconds as an ISO 8601 UTC timestamp (the wire
/// shape MCP task timestamps use). The crate carries no date library;
/// operations store epoch ms, so convert directly.
fn iso8601_utc(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let day_ms = ms.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        day_ms / 3_600_000,
        day_ms / 60_000 % 60,
        day_ms / 1_000 % 60,
        day_ms % 1_000
    )
}

/// Days since the Unix epoch → (year, month, day), proleptic Gregorian
/// (Howard Hinnant's civil-from-days algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d as u32)
}

impl ServerHandler for McpFacade {
    fn get_info(&self) -> ServerConfig {
        let mut extensions = ExtensionCapabilities::new();
        extensions.insert(subscriptions::EXTENSION_ID.to_string(), JsonObject::new());
        let mut capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_tasks()
            .build();
        match &mut capabilities.extensions {
            Some(map) => {
                map.extend(extensions);
            }
            None => capabilities.extensions = Some(extensions),
        }
        ServerConfig::new(capabilities)
            .with_server_info(Implementation::new(
                "eliot-swarm-controller",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Tools map one-to-one onto the swarm controller's application API and are \
             executed against the running host over local IPC with the configured \
             credential. Restricted profiles require a caller-owned stable \
             client_request_id before every mutation dispatch, so a lost reply can \
             be reconciled without a hidden retry. Only the explicit local full \
             compatibility profile permits omission; there, a generated ID is \
             echoed only in a received result and cannot make a retry safe if that \
             response itself was lost. Restricted-profile tasks/cancel likewise \
             requires client_request_id in request _meta; full generates one if \
             omitted. Operations returned by mutations are durable handles \
             to poll with operation_get. If the client declares the \
             io.modelcontextprotocol/tasks extension, a mutation whose operation is \
             still in flight instead returns an MCP task whose taskId is that \
             operation's ID: poll it with tasks/get, and cancel a still-queued \
             operation with tasks/cancel. Pending native inputs surfaced by \
             tasks/get are answered with the agent_reply tool, not tasks/update. \
             The eliot/subscribe and eliot/unsubscribe protocol methods open and \
             close a bounded subscription over committed facts (categories: \
             reports, mailbox, operations, concilium): notifications/eliot/committed carries \
             each committed transition with its projection frame, and an explicit \
             notifications/eliot/lagged marks any range the bounded queue \
             skipped. Notifications are a freshness hint, never complete history: \
             Concilium notification items contain IDs and cursor only. Resync \
             through report_delta, message_read or operation_get, and through \
             concilium_get or concilium_list for Concilium facts, from the \
             last delivered cursor, and after a reconnect re-subscribe with that \
             cursor — subscriptions do not survive the session.",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        let tools = TOOLS
            .iter()
            .map(|(read_only, spec)| {
                tool_from_spec(
                    *read_only,
                    spec,
                    mutation_requires_caller_request_id(
                        McpToolProfile::Full,
                        spec.method,
                        *read_only,
                    ),
                )
            })
            .collect();
        Ok(ListToolsResult {
            tools,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        let Some((read_only, spec)) = find_tool(&request.name) else {
            return Err(McpError::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >());
        };
        let params = request
            .arguments
            .map(Value::Object)
            .unwrap_or_else(|| json!({}));
        let result = self.call(spec.method, params, *read_only).await;
        // SEP-2663: a task handle is returned only to a client that
        // declared the tasks extension, and only for mutations; every
        // other response is byte-identical to the pre-Tasks facade.
        if *read_only || !client_tasks_negotiated(&context) {
            return Ok(result.into());
        }
        Ok(self.task_seed_response(result).await)
    }

    /// SEP-2663 `tasks/get`: project the Operation the taskId names.
    /// The Operation store is the only state read; an unknown taskId is
    /// the store's own NOT_FOUND.
    async fn get_task(
        &self,
        request: GetTaskParams,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<GetTaskResult, McpError> {
        let detailed = self
            .project_task(&request.task_id)
            .await
            .map_err(protocol_error)?;
        Ok(GetTaskResult::new(detailed))
    }

    /// SEP-2663 `tasks/cancel`: submit the existing addressed
    /// `operation.cancel` for a queued Operation. A terminal Operation
    /// is acknowledged (there is nothing to cancel); an in-flight one
    /// is refused with the store's reason, because already-sent work
    /// requires native cancellation/reconciliation. The ack is
    /// cooperative: the resulting state is observed via `tasks/get`.
    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<(), McpError> {
        let operation = self
            .request("operation.get", json!({"operation_id": request.task_id}))
            .await
            .map_err(protocol_error)?;
        let state = operation["state"].as_str().unwrap_or_default();
        match cancel_action(state) {
            CancelAction::Ack => Ok(()),
            CancelAction::Refuse => Err(McpError::invalid_request(
                format!(
                    "operation {} is {state}: tasks/cancel submits operation_cancel, \
                     which cancels only queued operations",
                    request.task_id
                ),
                Some(json!({
                    "code": "NOT_QUEUED",
                    "message": "already-sent operations require native cancellation/reconciliation, not local deletion",
                })),
            )),
            CancelAction::Submit => {
                let cancel = json!({
                    "operation_id": request.task_id,
                    "reason": "mcp tasks/cancel",
                    "client_request_id": context
                        .meta
                        .get("client_request_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                .unwrap_or_else(|| Uuid::new_v4().to_string()),
                });
                match self.request("operation.cancel", cancel).await {
                    Ok(_) => Ok(()),
                    Err(e) if matches!(e.code.as_str(), "NOT_QUEUED" | "CONFLICT") => {
                        // The operation left the queued state while the
                        // cancel was in flight; if it is terminal now,
                        // there is nothing left to cancel.
                        let current = self
                            .request("operation.get", json!({"operation_id": request.task_id}))
                            .await
                            .map_err(protocol_error)?;
                        match cancel_action(current["state"].as_str().unwrap_or_default()) {
                            CancelAction::Ack => Ok(()),
                            _ => Err(protocol_error(e)),
                        }
                    }
                    Err(e) => Err(protocol_error(e)),
                }
            }
        }
    }
    /// Facade-protocol methods (§17.3): `eliot/subscribe` and
    /// `eliot/unsubscribe`. Everything else keeps the router's
    /// default answer for a custom method: method not found.
    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CustomResult, McpError> {
        match request.method.as_str() {
            subscriptions::SUBSCRIBE_METHOD => {
                self.subscribe(request.params, context.peer, McpToolProfile::Full)
                    .await
            }
            subscriptions::UNSUBSCRIBE_METHOD => self.unsubscribe(request.params),
            _ => Err(McpError::new(
                rmcp::model::ErrorCode::METHOD_NOT_FOUND,
                request.method.clone(),
                None,
            )),
        }
    }
}

/// The production MCP boundary is a session-fixed frontend surface over the
/// local facade. Discovery intersects that presentation with live Store
/// authorization; requests outside the selected frontend surface are not
/// forwarded. Host authorization remains authoritative for application calls.
pub struct ProfiledFacade {
    inner: McpFacade,
    profile: McpToolProfile,
    surface: catalog::Surface,
}

impl ProfiledFacade {
    #[cfg(test)]
    fn new(inner: McpFacade, profile: McpToolProfile) -> Self {
        Self::with_surface(inner, profile, catalog::Surface::role_default(profile))
    }

    fn with_surface(inner: McpFacade, profile: McpToolProfile, surface: catalog::Surface) -> Self {
        Self {
            inner,
            profile,
            surface,
        }
    }

    /// Ask the authenticated Store for current method membership on every
    /// catalog operation. Discovery is not cached across grant/scope changes.
    async fn catalog_authorization(
        &self,
        task_id: Option<&str>,
    ) -> std::result::Result<CatalogAuthorization, McpError> {
        let mut params = json!({});
        if let Some(task_id) = task_id {
            params["task_id"] = json!(task_id);
        }
        let value = self
            .inner
            .request("mcp.authorization", params)
            .await
            .map_err(|_| catalog_authorization_error())?;
        parse_catalog_authorization(&value, task_id).ok_or_else(catalog_authorization_error)
    }

    async fn require_live_methods(
        &self,
        required: &[&str],
        exposed_method: &str,
    ) -> std::result::Result<(), McpError> {
        let authorization = self.catalog_authorization(None).await?;
        if required
            .iter()
            .all(|method| authorization.allowed_methods.contains(*method))
        {
            Ok(())
        } else {
            Err(method_not_found(exposed_method))
        }
    }

    async fn require_live_subscription_categories(
        &self,
        categories: &[subscriptions::Category],
    ) -> std::result::Result<(), McpError> {
        let authorization = self.catalog_authorization(None).await?;
        let allowed = categories.iter().all(|category| {
            let requirement = profiles::subscription_method_requirement(*category);
            requirement
                .all
                .iter()
                .all(|method| authorization.allowed_methods.contains(*method))
                && (requirement.any.is_empty()
                    || requirement
                        .any
                        .iter()
                        .any(|method| authorization.allowed_methods.contains(*method)))
        });
        if allowed {
            Ok(())
        } else {
            Err(method_not_found(subscriptions::SUBSCRIBE_METHOD))
        }
    }
}

struct CatalogAuthorization {
    revision: String,
    task_id: Option<String>,
    allowed_methods: BTreeSet<String>,
}

fn catalog_authorization_error() -> McpError {
    McpError::internal_error(
        "current application authorization could not be confirmed",
        None,
    )
}

fn parse_catalog_authorization(
    value: &Value,
    requested_task_id: Option<&str>,
) -> Option<CatalogAuthorization> {
    let object = value.as_object()?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "authorization_revision" | "basis" | "allowed_methods" | "role" | "task_id"
        )
    }) {
        return None;
    }
    let revision = object.get("authorization_revision")?.as_str()?.trim();
    if revision.is_empty() || revision.len() > 128 {
        return None;
    }
    if object.get("basis")?.as_str()? != "authenticated_store_scope" {
        return None;
    }
    if !matches!(
        object.get("role")?.as_str()?,
        "participant" | "manager" | "operator" | "observer"
    ) {
        return None;
    }
    let task_id = match object.get("task_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(task_id)) if !task_id.trim().is_empty() => Some(task_id.clone()),
        _ => return None,
    };
    if task_id.as_deref() != requested_task_id {
        return None;
    }
    let application_methods: BTreeSet<&str> =
        registered_application_methods().into_iter().collect();
    let mut allowed_methods = BTreeSet::new();
    for method in object.get("allowed_methods")?.as_array()? {
        let method = method.as_str()?;
        if method != "swarm.tools.search" && !application_methods.contains(method) {
            return None;
        }
        if !allowed_methods.insert(method.to_owned()) {
            return None;
        }
    }
    if !allowed_methods.contains("swarm.tools.search") {
        return None;
    }
    Some(CatalogAuthorization {
        revision: revision.to_owned(),
        task_id,
        allowed_methods,
    })
}

/// Build the same session-fixed, profile-enforced facade used by stdio MCP.
/// The caller supplies the configured principal and profile; neither is
/// derived from transport metadata.
pub fn profiled_facade(
    config: &Config,
    credential: Credential,
    profile_name: Option<&str>,
) -> Result<ProfiledFacade> {
    config.mcp.validate()?;
    let selected_name = profile_name.unwrap_or(&config.mcp.default_profile);
    let named_profile = config.mcp.profiles.get(selected_name).ok_or_else(|| {
        Error::new(
            "CONFIG_ERROR",
            format!("unknown MCP profile {selected_name:?}"),
        )
    })?;
    let profile = config
        .mcp
        .selected_tool_profile(Some(selected_name), &credential.client_id)?;
    let surface = catalog::Surface::configured(
        profile,
        named_profile.surface.as_deref(),
        &named_profile.deferred_groups,
        &named_profile.manual_tools,
    )
    .map_err(|error| Error::new("CONFIG_ERROR", error.to_string()))?;
    let facade = McpFacade::new(
        config.storage.data_dir.clone(),
        credential,
        Arc::new(config.ipc.clone()),
    );
    Ok(ProfiledFacade::with_surface(facade, profile, surface))
}

/// Canonical restricted Participant core schemas for comparison with an
/// independently observed native inventory. This grants no method authority.
pub fn participant_core_tool_contracts() -> Result<Vec<Value>> {
    swarm_contracts::mcp_catalog::participant_core_tool_contracts()
}

/// Describe the configured facade without claiming native tool loading.
pub fn launch_profile_surface(
    profile: McpToolProfile,
    surface_name: &str,
    groups: &[String],
    manual_tools: &[String],
) -> Result<Value> {
    swarm_contracts::mcp_catalog::launch_profile_surface(
        profile,
        surface_name,
        groups,
        manual_tools,
    )
}

fn method_not_found(method: &str) -> McpError {
    McpError::new(
        rmcp::model::ErrorCode::METHOD_NOT_FOUND,
        method.to_string(),
        None,
    )
}

fn require_caller_request_id(params: &Value) -> std::result::Result<(), McpError> {
    if !caller_request_id_present(params) {
        return Err(McpError::invalid_params(PREKNOWN_REQUEST_ID_MESSAGE, None));
    }
    Ok(())
}

fn parse_catalog_search<'a>(
    params: &'a Value,
) -> std::result::Result<catalog::SearchRequest<'a>, McpError> {
    let object = params.as_object().ok_or_else(|| {
        McpError::invalid_params("catalog search arguments must be an object", None)
    })?;
    const FIELDS: &[&str] = &[
        "query",
        "purpose",
        "task_id",
        "exact_method",
        "loaded_catalog_revision",
        "max_results",
    ];
    if object.keys().any(|key| !FIELDS.contains(&key.as_str())) {
        return Err(McpError::invalid_params(
            "catalog search contains an unsupported argument",
            None,
        ));
    }
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .filter(|query| query.len() <= 512)
        .ok_or_else(|| {
            McpError::invalid_params("query must be a string of at most 512 bytes", None)
        })?;
    let optional_text =
        |name: &str, limit: usize| -> std::result::Result<Option<&'a str>, McpError> {
            match object.get(name) {
                None | Some(Value::Null) => Ok(None),
                Some(Value::String(value)) if value.len() <= limit => Ok(Some(value)),
                _ => Err(McpError::invalid_params(
                    format!("{name} must be a string of at most {limit} bytes"),
                    None,
                )),
            }
        };
    let purpose = optional_text("purpose", 128)?;
    let task_id = optional_text("task_id", 128)?;
    let exact_method = optional_text("exact_method", 128)?;
    let loaded_catalog_revision = optional_text("loaded_catalog_revision", 64)?;
    if query.trim().is_empty() && exact_method.is_none() {
        return Err(McpError::invalid_params(
            "query must be non-empty unless exact_method is supplied",
            None,
        ));
    }
    if loaded_catalog_revision.is_some_and(|revision| {
        revision.len() != 64
            || !revision
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err(McpError::invalid_params(
            "loaded_catalog_revision must be a lowercase SHA-256 digest",
            None,
        ));
    }
    let max_results = match object.get("max_results") {
        None => 5,
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=catalog::MAX_SEARCH_RESULTS as u64).contains(value))
            .map(|value| value as usize)
            .ok_or_else(|| {
                McpError::invalid_params(
                    format!(
                        "max_results must be between 1 and {}",
                        catalog::MAX_SEARCH_RESULTS
                    ),
                    None,
                )
            })?,
    };
    Ok(catalog::SearchRequest {
        query,
        purpose,
        task_id,
        exact_method,
        loaded_catalog_revision,
        max_results,
    })
}

fn catalog_protocol_error(error: catalog::CatalogError) -> McpError {
    match error {
        catalog::CatalogError::InvalidCursor
        | catalog::CatalogError::InvalidSurface
        | catalog::CatalogError::StaleCursor
        | catalog::CatalogError::CursorOutOfRange
        | catalog::CatalogError::StaleCatalogRevision => {
            McpError::invalid_params(error.to_string(), None)
        }
        catalog::CatalogError::IncompleteRegistry
        | catalog::CatalogError::ToolSchemaTooLarge
        | catalog::CatalogError::Serialization(_) => {
            McpError::internal_error(error.to_string(), None)
        }
    }
}

impl ServerHandler for ProfiledFacade {
    fn get_info(&self) -> ServerConfig {
        self.inner.get_info()
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> std::result::Result<ListToolsResult, McpError> {
        let cursor = request.as_ref().and_then(|params| params.cursor.as_deref());
        let authorization = self.catalog_authorization(None).await?;
        let page = catalog::list_tools_page(
            self.profile,
            &self.surface,
            cursor,
            catalog::AuthorizationRevision {
                value: &authorization.revision,
                basis: catalog::AuthorizationBasis::AuthenticatedStoreScope,
            },
            |method, task_id| {
                authorization.allowed_methods.contains(method)
                    && task_id
                        .is_none_or(|task_id| authorization.task_id.as_deref() == Some(task_id))
            },
        )
        .map_err(catalog_protocol_error)?;
        Ok(ListToolsResult {
            tools: page.tools,
            next_cursor: page.next_cursor,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CallToolResponse, McpError> {
        let Some((read_only, spec)) = find_tool(&request.name) else {
            return Err(McpError::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >());
        };
        if !profiles::exposes_method(self.profile, spec.method) {
            return Err(McpError::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >());
        }
        if spec.method == "swarm.tools.search" {
            let arguments = Value::Object(request.arguments.clone().unwrap_or_default());
            let search = parse_catalog_search(&arguments)?;
            let authorization = self.catalog_authorization(search.task_id).await?;
            let result = catalog::search_catalog(
                self.profile,
                &self.surface,
                search,
                catalog::AuthorizationRevision {
                    value: &authorization.revision,
                    basis: catalog::AuthorizationBasis::AuthenticatedStoreScope,
                },
                |method, task_id| {
                    authorization.allowed_methods.contains(method)
                        && task_id
                            .is_none_or(|task_id| authorization.task_id.as_deref() == Some(task_id))
                },
            )
            .map_err(catalog_protocol_error)?;
            let value = serde_json::to_value(result)
                .map_err(|error| McpError::internal_error(error.to_string(), None))?;
            let Value::Object(result) = value else {
                return Err(McpError::internal_error(
                    "MCP catalog search produced a non-object result",
                    None,
                ));
            };
            return Ok(CallToolResult::structured(Value::Object(result)).into());
        }
        let required_methods = [spec.method];
        self.require_live_methods(&required_methods, spec.method)
            .await?;
        if mutation_requires_caller_request_id(self.profile, spec.method, *read_only) {
            let arguments = request
                .arguments
                .as_ref()
                .map(|arguments| Value::Object(arguments.clone()))
                .unwrap_or_else(|| json!({}));
            require_caller_request_id(&arguments)?;
        }
        self.inner.call_tool(request, context).await
    }

    async fn get_task(
        &self,
        request: GetTaskParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<GetTaskResult, McpError> {
        if !profiles::allows_task_get(self.profile) {
            return Err(method_not_found("tasks/get"));
        }
        self.require_live_methods(profiles::task_get_required_methods(), "tasks/get")
            .await?;
        self.inner.get_task(request, context).await
    }

    async fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<(), McpError> {
        if !profiles::allows_task_cancel(self.profile) {
            return Err(method_not_found("tasks/cancel"));
        }
        self.require_live_methods(profiles::task_cancel_required_methods(), "tasks/cancel")
            .await?;
        if self.profile != McpToolProfile::Full
            && context
                .meta
                .get("client_request_id")
                .and_then(Value::as_str)
                .is_none_or(|request_id| request_id.trim().is_empty())
        {
            return Err(McpError::invalid_params(
                "restricted-profile tasks/cancel requires a caller-owned client_request_id in _meta",
                None,
            ));
        }
        self.inner.cancel_task(request, context).await
    }

    async fn on_custom_request(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> std::result::Result<CustomResult, McpError> {
        if request.method == subscriptions::SUBSCRIBE_METHOD {
            let params = request
                .params
                .as_ref()
                .cloned()
                .unwrap_or_else(|| json!({}));
            let (categories, _) =
                subscriptions::parse_subscribe(&params).map_err(protocol_error)?;
            if !categories
                .iter()
                .all(|category| profiles::allows_subscription_category(self.profile, *category))
            {
                return Err(method_not_found(subscriptions::SUBSCRIBE_METHOD));
            }
            self.require_live_subscription_categories(&categories)
                .await?;
            return self
                .inner
                .subscribe(request.params, context.peer, self.profile)
                .await;
        }
        self.inner.on_custom_request(request, context).await
    }
}

/// Whether the connected client declared the tasks extension during
/// initialize — the gate for returning task handles from `tools/call`
/// and the same check the RMCP router applies to `tasks/*` methods.
fn client_tasks_negotiated(context: &RequestContext<RoleServer>) -> bool {
    context
        .client_capabilities()
        .is_some_and(|caps| caps.supports_tasks())
}

/// Serve MCP over stdio until the client disconnects. The host connection is
/// established lazily on the first catalog discovery, search, or tool call.
/// Discovery fails closed while the host cannot confirm current authorization.
pub async fn run(config: Config, credential: Credential) -> Result<()> {
    run_profiled(config, credential, None).await
}

/// Serve one MCP session after resolving its configured profile against the
/// credential identity. The wrapper remains fixed for the entire session.
pub async fn run_profiled(
    config: Config,
    credential: Credential,
    profile_name: Option<&str>,
) -> Result<()> {
    let facade = profiled_facade(&config, credential, profile_name)?;
    let service = facade
        .serve(stdio())
        .await
        .map_err(|e| Error::new("MCP_ERROR", e.to_string()))?;
    service
        .waiting()
        .await
        .map_err(|e| Error::new("MCP_ERROR", e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn tool_table_is_unique_and_complete() {
        let mut names = BTreeSet::new();
        let mut methods = BTreeSet::new();
        for (_, spec) in TOOLS {
            assert!(names.insert(tool_name(spec.method)), "duplicate tool");
            assert!(methods.insert(spec.method), "duplicate method");
            for required in spec.required {
                assert!(
                    spec.fields.iter().any(|field| &field.name == required),
                    "{} requires undeclared field {required}",
                    spec.method
                );
            }
        }
        let expected: BTreeSet<&str> = swarm_contracts::method_policy::METHOD_REGISTRY
            .iter()
            .filter(|entry| entry.mcp)
            .map(|entry| entry.method)
            .collect();
        assert_eq!(methods, expected);
        assert_eq!(TOOLS.len(), expected.len());
        assert_eq!(
            TOOLS.iter().filter(|(read_only, _)| *read_only).count(),
            swarm_contracts::method_policy::METHOD_REGISTRY
                .iter()
                .filter(|entry| {
                    entry.mcp
                        && matches!(
                            entry.class,
                            swarm_contracts::method_policy::MethodClass::ReadOnly
                                | swarm_contracts::method_policy::MethodClass::FacadeOnly
                        )
                })
                .count()
        );
        assert_eq!(
            TOOLS.iter().filter(|(read_only, _)| !*read_only).count(),
            swarm_contracts::method_policy::METHOD_REGISTRY
                .iter()
                .filter(|entry| {
                    entry.mcp
                        && entry.class == swarm_contracts::method_policy::MethodClass::Mutation
                })
                .count()
        );
    }

    #[test]
    fn schemas_are_closed_objects() {
        for (read_only, spec) in TOOLS {
            let schema = input_schema(
                spec,
                *read_only,
                mutation_requires_caller_request_id(McpToolProfile::Full, spec.method, *read_only),
            );
            assert_eq!(schema["type"], json!("object"));
            assert_eq!(schema["additionalProperties"], json!(false));
            if !read_only {
                assert!(schema["properties"].get("client_request_id").is_some());
            }
        }
        let run_now = find_tool("schedule_run_now").unwrap();
        let schema = input_schema(
            &run_now.1,
            run_now.0,
            mutation_requires_caller_request_id(McpToolProfile::Full, run_now.1.method, run_now.0),
        );
        assert_eq!(
            schema["required"],
            json!(["project_id", "automation_id", "client_request_id"])
        );
        assert_eq!(schema["properties"]["client_request_id"]["minLength"], 1);
        assert_eq!(schema["properties"]["client_request_id"]["maxLength"], 128);
        let send = find_tool("message_send").unwrap();
        let schema = input_schema(
            &send.1,
            send.0,
            mutation_requires_caller_request_id(McpToolProfile::Full, send.1.method, send.0),
        );
        for field in [
            "in_reply_to",
            "in_reply_to_digest",
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
        ] {
            assert!(schema["properties"].get(field).is_some(), "{field}");
        }
        let output = output_schema("message.send").unwrap();
        for field in [
            "delivery_id",
            "payload_digest",
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
            "in_reply_to",
            "reply_to",
            "cancellation",
        ] {
            assert!(output["properties"].get(field).is_some(), "{field}");
        }
        let reconcile = find_tool("github_pull_request_reconcile_description").unwrap();
        let schema = input_schema(&reconcile.1, reconcile.0, true);
        assert_eq!(schema["additionalProperties"], json!(false));
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("operation_id"))
        );
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("client_request_id"))
        );
        assert_eq!(schema["properties"]["operation_id"]["maxLength"], 128);
        assert!(schema["properties"].get("title").is_none());
        assert!(schema["properties"].get("body").is_none());
        let register = find_tool("script_register").unwrap();
        let schema = input_schema(&register.1, register.0, false);
        let grants = &schema["properties"]["bundle"]["properties"]["controller_effects"];
        assert_eq!(grants["maxItems"], json!(1));
        assert_eq!(grants["uniqueItems"], json!(true));
        assert_eq!(
            grants["items"]["enum"],
            json!(["task_owner_message", "manager_notification", "task_create"])
        );
        assert!(
            !schema["properties"]["bundle"]["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field == "controller_effects")
        );
    }
}

mod catalog;
mod profiles;
mod subscriptions;

#[cfg(test)]
#[path = "frontend_contract_tests.rs"]
mod frontend_contract_tests;
#[cfg(test)]
#[path = "subscription_contract_tests.rs"]
mod subscription_contract_tests;
#[cfg(test)]
#[path = "task_projection_tests.rs"]
mod task_projection_tests;
