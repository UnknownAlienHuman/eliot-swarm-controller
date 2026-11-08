//! Thin MCP stdio frontend over the existing authenticated daemon client.
//!
//! Official Rust MCP SDK (`rmcp 3.9.0`) over stdio; the handler implements
//! `ServerHandler` manually (`initialize` via `get_info`, plus `tools/list`
//! and `tools/call`), so no macro feature is required.
//! No embedded Router/Store/second daemon; the only daemon path is the
//! existing `eliot_daemon::client_call`, which resolves caller identity from
//! the inherited process environment and fails closed on partial/conflicting
//! Agent/Run/Owner credentials (never an owner fallback).
//!
//! Auth boundary: the `_auth`/`_payload` envelope is built server-side by
//! `eliot_daemon::client_call` from the inherited environment. Tool arguments
//! (`method`, `params`) can never inject or override credentials, and no
//! credential, token, or raw environment value is echoed to tool output or
//! logs. The tool is intentionally generic: `{ method, params }` maps 1:1 to
//! the existing management client; no verb catalogue or DTOs are copied.
//! Mutations keep their existing caller-known durable id — this frontend
//! never auto-retries, never pretends rollback on cancel, and never replays.
//! Incoming stdio frames are bounded by the SDK's own transport limit; the
//! peer-facing `params` size reuses the daemon 4 MiB frame limit.
//! stdout is MCP protocol only, diagnostics go to stderr. The optional
//! loopback StreamableHTTP frontend in [`http`] reuses the same handler.
//! HTTP sessions additionally receive committed owner events
//! (`x.user.notify`, `x.user.agent.attention`) as `notifications/message`
//! (one long-poll task per initialized session, normalized payloads only);
//! stdio advertises no logging capability and is unchanged.

mod bounded_io;
mod http;

pub use http::run_http;

use std::path::{Path, PathBuf};

use eliot_core::Event;
use eliot_core::api::{
    EventsBoundsRequest, EventsBoundsResponse, EventsFilter, EventsSubscribeRequest,
    EventsSubscribeResponse,
};
use rmcp::ErrorData as McpError;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig, ServerNotification,
    SetLevelRequestMethod, Tool, ToolAnnotations,
};
#[allow(deprecated)] // SEP-2577: logging is deprecated but functional in rmcp 3.9.
use rmcp::model::{
    LoggingLevel, LoggingMessageNotification, LoggingMessageNotificationParam,
    SetLevelRequestParams,
};
use rmcp::service::{MaybeSendFuture, NotificationContext, Peer, RequestContext, RoleServer};
use serde_json::{Map, Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;

const TOOL_NAME: &str = "swarm";
const TOOL_DESCRIPTION: &str =
    "Generic Eliot management call: { method, params } -> authenticated daemon RPC result";

const INVALID_ARGS_MESSAGE: &str = "tool arguments must be an object with method and params";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolArgs {
    method: String,
    params: Map<String, Value>,
}

/// MCP stdio entry point. The `data_dir` already comes from `--data-dir` /
/// `ELIOT_HOME` resolution in `main.rs`; no daemon is started here.
pub async fn run_stdio(data_dir: PathBuf) -> Result<(), String> {
    #[allow(deprecated)] // SEP-2577: LoggingLevel default is deprecated but functional in rmcp 3.9.
    let handler = EliotMcpHandler {
        data_dir,
        notify_owner_events: false,
        shutdown: CancellationToken::new(),
        notify_spawned: Arc::new(AtomicBool::new(false)),
        log_level: Arc::new(RwLock::new(LoggingLevel::Info)),
    };
    let (stdin, stdout) = rmcp::transport::io::stdio();
    let stdout = bounded_io::BoundedLineWriter::new(stdout, eliot_daemon::MAX_RPC_FRAME_BYTES);
    let transport = rmcp::transport::async_rw::AsyncRwTransport::new(stdin, stdout)
        .with_max_line_length(eliot_daemon::MAX_RPC_FRAME_BYTES);
    let service = rmcp::service::serve_server(handler, transport)
        .await
        .map_err(|_| "mcp stdio initialization failed".to_owned())?;
    let _reason = service
        .waiting()
        .await
        .map_err(|_| "mcp stdio transport closed".to_owned())?;
    Ok(())
}

#[derive(Clone)]
struct EliotMcpHandler {
    data_dir: PathBuf,
    /// True only for HTTP sessions: advertise `logging` and spawn the one
    /// owner-event delivery task on initialize. Stdio stays `false`.
    notify_owner_events: bool,
    /// Server shutdown token; the session task ends when it fires.
    shutdown: CancellationToken,
    /// Guards the one-task-per-session invariant against repeated
    /// `notifications/initialized` deliveries.
    notify_spawned: Arc<AtomicBool>,
    /// Per-session `logging/setLevel` minimum severity (default `Info`).
    /// Written by `set_level`, read by the session notify task.
    #[allow(deprecated)] // SEP-2577: LoggingLevel is deprecated but functional in rmcp 3.9.
    log_level: Arc<RwLock<LoggingLevel>>,
}

impl ServerHandler for EliotMcpHandler {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(server_capabilities(self.notify_owner_events))
            .with_server_info(Implementation::new("eliot", env!("CARGO_PKG_VERSION")))
    }

    fn on_initialized(
        &self,
        context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + MaybeSendFuture + '_ {
        if self.notify_owner_events && !self.notify_spawned.swap(true, Ordering::SeqCst) {
            spawn_owner_notify_task(
                self.data_dir.clone(),
                context.peer,
                self.shutdown.clone(),
                self.log_level.clone(),
            );
        }
        std::future::ready(())
    }

    #[allow(deprecated)] // SEP-2577: setLevel is deprecated but functional in rmcp 3.9.
    fn set_level(
        &self,
        request: SetLevelRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        if !self.notify_owner_events {
            return std::future::ready(Err(McpError::method_not_found::<SetLevelRequestMethod>()));
        }
        match self.log_level.write() {
            Ok(mut level) => {
                *level = request.level;
                std::future::ready(Ok(()))
            }
            Err(_) => std::future::ready(Err(McpError::internal_error(
                "logging level unavailable",
                None,
            ))),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, McpError>> + MaybeSendFuture + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(vec![tool_definition()])))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResponse, McpError>> + MaybeSendFuture + '_ {
        let data_dir = self.data_dir.clone();
        async move {
            if request.name.as_ref() != TOOL_NAME {
                return Err(McpError::invalid_params("tool not found", None));
            }
            let args = request
                .arguments
                .map(Value::Object)
                .ok_or_else(|| McpError::invalid_params(INVALID_ARGS_MESSAGE, None))?;
            let parsed: ToolArgs = serde_json::from_value(args)
                .map_err(|_| McpError::invalid_params(INVALID_ARGS_MESSAGE, None))?;
            if parsed.method.trim().is_empty() {
                return Err(McpError::invalid_params(INVALID_ARGS_MESSAGE, None));
            }
            let params = Value::Object(parsed.params);
            if let Ok(encoded) = serde_json::to_string(&params)
                && encoded.len().saturating_add(1) > eliot_daemon::MAX_RPC_FRAME_BYTES
            {
                return Ok(CallToolResult::error(vec![rmcp::model::ContentBlock::text(
                    "frame_too_large",
                )])
                .into());
            }
            match eliot_daemon::client_call(Path::new(&data_dir), &parsed.method, params).await {
                Ok(value) => Ok(CallToolResult::structured(
                    json!({ "method": parsed.method, "result": value }),
                )
                .into()),
                Err(error) => Ok(CallToolResult::structured_error(json!({
                    "code": error.code,
                    "message": error.message,
                }))
                .into()),
            }
        }
    }
}

fn tool_definition() -> Tool {
    let schema = Arc::new(
        serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "method": { "type": "string", "description": "Eliot management method, e.g. daemon.status, agent.get, command.get" },
                "params": { "type": "object", "description": "Method params object; auth is inherited server-side and cannot be overridden here" }
            },
            "required": ["method", "params"],
            "additionalProperties": false
        }))
        .unwrap_or_default(),
    );
    Tool::new(TOOL_NAME, TOOL_DESCRIPTION, schema)
        .with_annotations(ToolAnnotations::new().read_only(false).open_world(false))
}

/// Committed event kinds delivered to HTTP sessions. Anything else is
/// skipped after advancing the cursor, so one task observes the whole
/// committed log without fanning out.
const NOTIFY_EVENT_KINDS: [&str; 2] = ["x.user.notify", "x.user.agent.attention"];
const NOTIFY_PAGE_SIZE: usize = 100;
const NOTIFY_WAIT_MS: u64 = 1_000;
const NOTIFY_LOGGER: &str = "eliot";

/// Tools always; `logging` only for HTTP sessions.
///
/// The `allow` is SEP-2577 scope: logging is deprecated but functional and
/// is the only server-to-client message channel in rmcp 3.9.
#[allow(deprecated)]
fn server_capabilities(notify_owner_events: bool) -> ServerCapabilities {
    let builder = ServerCapabilities::builder().enable_tools();
    if notify_owner_events {
        builder.enable_logging().build()
    } else {
        builder.build()
    }
}

/// Spawn the single owner-event delivery task for one initialized HTTP
/// session. The task long-polls `events.subscribe` from the current tip
/// (live events only, no history replay) and ends on server shutdown,
/// peer/transport closure, or daemon error. Peer closure is probed every
/// poll cycle through the SDK channel, so an idle session with no matching
/// event still ends promptly (at most one in-flight long-poll past the
/// disconnect). It never retries: a dead task stays dead until the client
/// opens a new session.
#[allow(deprecated)] // SEP-2577: LoggingLevel is deprecated but functional in rmcp 3.9.
fn spawn_owner_notify_task(
    data_dir: PathBuf,
    peer: Peer<RoleServer>,
    shutdown: CancellationToken,
    log_level: Arc<RwLock<LoggingLevel>>,
) {
    tokio::spawn(async move {
        let Some(mut after) = current_tip(&data_dir).await else {
            return;
        };
        loop {
            // Native close signal: the SDK drops the session peer channel
            // when the session ends, so `is_transport_closed` observes a
            // dead session here even when no send ever fails.
            if peer.is_transport_closed() {
                return;
            }
            let params = EventsSubscribeRequest {
                after,
                limit: NOTIFY_PAGE_SIZE,
                filter: EventsFilter::default(),
                wait_ms: NOTIFY_WAIT_MS,
            };
            if params.validate().is_err() {
                return;
            }
            let response: EventsSubscribeResponse = tokio::select! {
                result = eliot_daemon::client_call_typed(&data_dir, "events.subscribe", &params) => {
                    match result {
                        Ok(response) => response,
                        Err(_) => return,
                    }
                }
                () = shutdown.cancelled() => return,
            };
            if response.validate(after).is_err() {
                return;
            }
            let minimum = log_level
                .read()
                .map(|level| *level)
                .unwrap_or(LoggingLevel::Info);
            for event in &response.events {
                if let Some((level, data)) = normalize_owner_event(event)
                    && severity(&level) >= severity(&minimum)
                    && peer
                        .send_notification(owner_notification(level, data))
                        .await
                        .is_err()
                {
                    return;
                }
            }
            after = response.next;
        }
    });
}

/// Read the allocated journal tip once so a new session delivers live events
/// only. `None` (daemon error or invalid response) aborts the task.
async fn current_tip(data_dir: &Path) -> Option<i64> {
    let params = EventsBoundsRequest::default();
    if params.validate().is_err() {
        return None;
    }
    let response: EventsBoundsResponse =
        eliot_daemon::client_call_typed(data_dir, "events.bounds", &params)
            .await
            .ok()?;
    response.validate().ok()?;
    Some(response.hi)
}

/// Map one committed event to its owner-facing logging level and payload,
/// or `None` when the kind is not delivered. Only normalized scalar fields
/// cross this boundary: never raw tool args, environment, credentials, or
/// log bodies. Malformed data is skipped, never forwarded raw.
#[allow(deprecated)] // SEP-2577: LoggingLevel is deprecated but functional in rmcp 3.9.
fn normalize_owner_event(event: &Event) -> Option<(LoggingLevel, Value)> {
    if !NOTIFY_EVENT_KINDS.contains(&event.kind.as_str()) {
        return None;
    }
    match event.kind.as_str() {
        "x.user.notify" => {
            let message = event.data.get("message")?.as_str()?;
            let mut payload = json!({
                "kind": event.kind,
                "seq": event.seq,
                "message": message,
            });
            if let Some(title) = event.data.get("title").and_then(Value::as_str) {
                payload["title"] = Value::String(title.to_owned());
            }
            Some((LoggingLevel::Info, payload))
        }
        "x.user.agent.attention" => {
            let report = event.data.get("data")?;
            let incident_id = report.get("incident_id")?.as_str()?;
            let phase = report.get("phase")?.as_str()?;
            // Attention is never routine: incidents warn, recoveries inform,
            // and unknown future phases warn rather than vanish.
            let level = match phase {
                "incident" => LoggingLevel::Warning,
                "recovery" => LoggingLevel::Info,
                _ => LoggingLevel::Warning,
            };
            let mut payload = json!({
                "kind": event.kind,
                "seq": event.seq,
                "incident_id": incident_id,
                "phase": phase,
            });
            if let Some(reason) = report.get("reason").and_then(Value::as_str) {
                payload["reason"] = Value::String(reason.to_owned());
            }
            if let Some(agent) = report.get("agent_id").and_then(Value::as_str) {
                payload["agent_id"] = Value::String(agent.to_owned());
            }
            if let Some(generation) = report.get("generation").and_then(Value::as_i64) {
                payload["generation"] = Value::from(generation);
            }
            if let Some(observed_at) = report.get("observed_at").and_then(Value::as_i64) {
                payload["observed_at"] = Value::from(observed_at);
            }
            if let Some(activity) = report.get("activity").and_then(Value::as_str) {
                payload["activity"] = Value::String(activity.to_owned());
            }
            if let Some(agent_state) = report.get("agent_state").and_then(Value::as_str) {
                payload["agent_state"] = Value::String(agent_state.to_owned());
            }
            if let Some(source_health) = report.get("source_health").and_then(Value::as_bool) {
                payload["source_health"] = Value::from(source_health);
            }
            if let Some(progress_age_ms) = report.get("progress_age_ms").and_then(Value::as_i64) {
                payload["progress_age_ms"] = Value::from(progress_age_ms);
            }
            if let Some(tool_coverage) = report.get("tool_coverage").and_then(Value::as_str) {
                payload["tool_coverage"] = Value::String(tool_coverage.to_owned());
            }
            if let Some(native) = report.get("native_status").and_then(Value::as_object) {
                if let Some(freshness) = native.get("freshness").and_then(Value::as_str) {
                    payload["native_freshness"] = Value::String(freshness.to_owned());
                }
                if let Some(basis) = native.get("basis").and_then(Value::as_str) {
                    payload["native_basis"] = Value::String(basis.to_owned());
                }
                if let Some(state) = native.get("state").and_then(Value::as_str) {
                    payload["native_state"] = Value::String(state.to_owned());
                }
                if let Some(error) = native.get("error_code").and_then(Value::as_str) {
                    payload["native_error"] = Value::String(error.to_owned());
                }
            }
            Some((level, payload))
        }
        _ => None,
    }
}

/// Syslog severity rank for [`LoggingLevel`]: higher is more severe. Only
/// `Info`/`Warning` are ever emitted; the full rank keeps client minimums
/// (e.g. `warning`) filtering exactly.
#[allow(deprecated)] // SEP-2577: LoggingLevel is deprecated but functional in rmcp 3.9.
fn severity(level: &LoggingLevel) -> u8 {
    match level {
        LoggingLevel::Debug => 0,
        LoggingLevel::Info => 1,
        LoggingLevel::Notice => 2,
        LoggingLevel::Warning => 3,
        LoggingLevel::Error => 4,
        LoggingLevel::Critical => 5,
        LoggingLevel::Alert => 6,
        LoggingLevel::Emergency => 7,
    }
}

/// Build one `notifications/message` envelope for the session peer.
/// `clientInfo` is never read: the peer is the owner service principal by
/// deployment design (Cloudflare Access plus cloudflared JWT at the edge),
/// and the logger name is fixed.
#[allow(deprecated)] // SEP-2577: deprecated but functional in rmcp 3.9.
fn owner_notification(level: LoggingLevel, data: Value) -> ServerNotification {
    ServerNotification::LoggingMessageNotification(LoggingMessageNotification::new(
        LoggingMessageNotificationParam::new(level, data).with_logger(NOTIFY_LOGGER),
    ))
}
