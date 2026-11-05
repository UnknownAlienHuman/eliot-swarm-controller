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
//! stdout is MCP protocol only, diagnostics go to stderr.

use std::path::{Path, PathBuf};

use rmcp::ErrorData as McpError;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool, ToolAnnotations,
};
use rmcp::service::{MaybeSendFuture, RequestContext, RoleServer};
use serde_json::{Map, Value, json};
use std::sync::Arc;

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
    let handler = EliotMcpHandler { data_dir };
    let (stdin, stdout) = rmcp::transport::io::stdio();
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
}

impl ServerHandler for EliotMcpHandler {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("eliot", env!("CARGO_PKG_VERSION")))
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
