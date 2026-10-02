//! MCP facade over the same application API used by the CLI.
//!
//! The General Manager's MCP client launches `swarm mcp` as a child process
//! and speaks MCP over stdio. This process is only a client of the running
//! host: it forwards each tool call over the existing local IPC with the
//! configured credential, exactly like a CLI invocation. It never opens the
//! database, never opens a TCP listener, and closing it does not stop the
//! host or cancel admitted work (donor qualification points for `api.mcp`).
//!
//! Tool surface: one tool per public application method, named after the
//! method with `.` replaced by `_`. There is deliberately no universal
//! passthrough or shell tool: every tool forwards to exactly one typed
//! application method, and the application layer keeps validating the
//! original request (field allow-lists, idempotency, role checks).
//!
//! Mutations follow the CLI's request-ID discipline. Only a caller-known
//! `client_request_id`, chosen and retained by the caller **before**
//! dispatch, makes a mutation safely retryable after a lost reply: the
//! retry reuses that same ID and the host returns the retained receipt.
//! When the caller omits it, one is generated for that call and echoed
//! back in the result object — but that generated ID is correlation only
//! for a response the caller actually received. It cannot rescue a call
//! whose response itself was lost, because the caller never learns the
//! generated ID in that case, so such a mutation cannot be safely
//! retried. A failed transport is dropped, never silently retried: the
//! next tool call reconnects first.
//!
//! Proposed, **not implemented** (R20): an MCP Tasks projection of
//! Operations — this facade returns Operation handles for polling with
//! `operation_get` instead — and RMCP subscriptions; bounded-lag and
//! resync semantics for subscriptions are a future contract, not current
//! behavior. Fact: the facade keeps no cache; authoritative reads are
//! forwarded to the host with no stale-on-error caching.

use crate::{
    config::{Config, Ipc},
    error::{Error, Result},
    ipc,
    model::{self, Credential},
};
use rmcp::{
    ErrorData as McpError, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
        ToolAnnotations,
    },
    service::{RequestContext, RoleServer},
    transport::stdio,
};
use serde_json::{Map, Value, json};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

type JsonObject = Map<String, Value>;

#[derive(Clone, Copy)]
struct Field {
    name: &'static str,
    kind: &'static str,
}

const fn f(name: &'static str, kind: &'static str) -> Field {
    Field { name, kind }
}
const S: &str = "string";
const SN: &str = "string_or_null";
const I: &str = "integer";
const B: &str = "boolean";
const O: &str = "object";
const A: &str = "array";

struct ToolSpec {
    method: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
}

const fn read(
    method: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
) -> (bool, ToolSpec) {
    (
        true,
        ToolSpec {
            method,
            description,
            fields,
            required,
        },
    )
}
const fn mutation(
    method: &'static str,
    description: &'static str,
    fields: &'static [Field],
    required: &'static [&'static str],
) -> (bool, ToolSpec) {
    (
        false,
        ToolSpec {
            method,
            description,
            fields,
            required,
        },
    )
}

/// (read_only, spec). Mirrors the public method list in the README; the
/// read/write split matches `Store`'s read classification plus the CLI's
/// request-ID treatment (`agent.result` and `host.mode` are mutations).
static TOOLS: &[(bool, ToolSpec)] = &[
    // Read-only methods.
    read("host.status", "Controller status snapshot.", &[], &[]),
    read(
        "route.list",
        "Configured routes; not live qualification.",
        &[],
        &[],
    ),
    read(
        "client.list",
        "Registered clients (operator only).",
        &[],
        &[],
    ),
    read(
        "task.get",
        "One task with its current attempt state.",
        &[f("task_id", S)],
        &["task_id"],
    ),
    read(
        "task.list",
        "Page tasks by offset.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "task.submission",
        "One immutable submission with paged requirement claims.",
        &[f("submission_ref", S), f("after", I), f("limit", I)],
        &["submission_ref"],
    ),
    read(
        "task.acceptance",
        "One acceptance decision and its current/revoked status.",
        &[f("acceptance_operation_id", S)],
        &["acceptance_operation_id"],
    ),
    read(
        "attempt.get",
        "One attempt and its disposition.",
        &[f("attempt_id", S)],
        &["attempt_id"],
    ),
    read(
        "operation.get",
        "One operation: the durable handle for async work.",
        &[f("operation_id", S)],
        &["operation_id"],
    ),
    read(
        "operation.list",
        "Page operations, optionally filtered by state.",
        &[f("after", I), f("limit", I), f("state", S)],
        &[],
    ),
    read(
        "agent.state",
        "Observed state of one binding generation.",
        &[f("binding_id", S), f("generation", I)],
        &["binding_id", "generation"],
    ),
    read(
        "agent.list",
        "Page known bindings with their observed state.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "agent.family",
        "A retained family observation; not a live query or a complete inventory claim.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("observation_id", I),
            f("after", I),
            f("limit", I),
        ],
        &["binding_id", "generation"],
    ),
    read(
        "check.get",
        "One check run and its evidence state.",
        &[f("check_id", S)],
        &["check_id"],
    ),
    read("check.profiles", "Configured check profiles.", &[], &[]),
    read(
        "artifact.get",
        "Artifact metadata.",
        &[f("artifact_id", S)],
        &["artifact_id"],
    ),
    read(
        "artifact.read",
        "One byte range of an immutable artifact.",
        &[
            f("artifact_id", S),
            f("offset_bytes", I),
            f("length_bytes", I),
        ],
        &["artifact_id"],
    ),
    read(
        "artifact.parts",
        "Page the provenance manifest of a whole assembled result.",
        &[f("artifact_id", S), f("after", I), f("limit", I)],
        &["artifact_id"],
    ),
    read(
        "report.delta",
        "Incremental report entries after a cursor.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    read(
        "message.read",
        "Read directed mailbox messages after a cursor; reading does not delete.",
        &[f("after", I), f("limit", I)],
        &[],
    ),
    // Mutations. All accept an optional stable client_request_id.
    mutation(
        "host.mode",
        "Enable or disable admission of new work on the host: new_work is the string \"enabled\" or \"disabled\".",
        &[f("new_work", S)],
        &["new_work"],
    ),
    mutation(
        "client.register",
        "Register a scoped client by token hash (operator only). The caller generates the token and keeps the credential; only its SHA-256 hash is sent.",
        &[
            f("client_id", S),
            f("role", S),
            f("token_hash", S),
            f("binding_id", S),
            f("binding_generation", I),
        ],
        &["client_id", "role", "token_hash"],
    ),
    mutation(
        "source.capture",
        "Capture an exact Git commit of a local repository as a fixed-source candidate.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("repository", S),
            f("commit", S),
        ],
        &["attempt_id", "expected_revision", "repository", "commit"],
    ),
    mutation(
        "check.run",
        "Run a configured check profile against a captured candidate.",
        &[
            f("attempt_id", S),
            f("candidate_ref", S),
            f("profile_id", S),
            f("profile_revision", S),
        ],
        &[
            "attempt_id",
            "candidate_ref",
            "profile_id",
            "profile_revision",
        ],
    ),
    mutation(
        "check.cancel",
        "Request cancellation of one check run; the reply confirms the durable request, not termination.",
        &[f("check_id", S), f("reason", S)],
        &["check_id", "reason"],
    ),
    mutation(
        "task.create",
        "Create a task from a specification; no native effects.",
        &[f("project_id", S), f("spec", O), f("origin_key", S)],
        &["project_id", "spec"],
    ),
    mutation(
        "task.revise",
        "Replace a task specification by CAS on its revision; resets current acceptance.",
        &[f("task_id", S), f("expected_revision", I), f("spec", O)],
        &["task_id", "expected_revision", "spec"],
    ),
    mutation(
        "task.claim",
        "Reserve an attempt for a task; start_owner defaults to native_manager.",
        &[
            f("task_id", S),
            f("expected_revision", I),
            f("owner_id", S),
            f("start_owner", S),
            f("binding_id", S),
            f("binding_generation", I),
        ],
        &["task_id", "expected_revision"],
    ),
    mutation(
        "task.dispatch",
        "Start a claimed controller-start attempt, or reuse its existing start operation.",
        &[
            f("attempt_id", S),
            f("text", S),
            f("prerequisite_operation_id", S),
        ],
        &["attempt_id"],
    ),
    mutation(
        "task.submit",
        "Seal an immutable submission and requirement report for an attempt; not acceptance.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("expected_submission_ref", SN),
            f("candidate_ref", S),
            f("summary", S),
            f("claims", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "expected_submission_ref",
            "candidate_ref",
            "summary",
        ],
    ),
    mutation(
        "task.request_changes",
        "Return one anchored finding about the exact current submission/candidate.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("submission_ref", S),
            f("candidate_ref", S),
            f("finding_id", S),
            f("reason", S),
            f("requirement_ids", A),
            f("evidence", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "finding_id",
            "reason",
            "evidence",
        ],
    ),
    mutation(
        "task.accept",
        "Accept the exact sealed proposal as a separate decision owner after review.",
        &[
            f("attempt_id", S),
            f("expected_revision", I),
            f("submission_ref", S),
            f("candidate_ref", S),
            f("expected_feedback_observation_id", I),
            f("reason", S),
            f("reviews", A),
            f("check_ids", A),
        ],
        &[
            "attempt_id",
            "expected_revision",
            "submission_ref",
            "candidate_ref",
            "expected_feedback_observation_id",
            "reason",
            "reviews",
        ],
    ),
    mutation(
        "task.invalidate_acceptance",
        "Revoke one named acceptance decision; never restarts its producer.",
        &[
            f("acceptance_operation_id", S),
            f("reason", S),
            f("evidence", A),
        ],
        &["acceptance_operation_id", "reason", "evidence"],
    ),
    mutation(
        "attempt.release",
        "End task-specific ownership after the producer's disposition is known.",
        &[
            f("attempt_id", S),
            f("outcome", S),
            f("reason", S),
            f("assignment_closed", B),
        ],
        &["attempt_id", "outcome"],
    ),
    mutation(
        "attempt.bind_producer",
        "Associate an already observed native run with an attempt; never spawns a worker.",
        &[
            f("attempt_id", S),
            f("assignment_id", S),
            f("native_session_id", S),
            f("native_run_id", S),
            f("observation_id", I),
        ],
        &[
            "attempt_id",
            "assignment_id",
            "native_session_id",
            "native_run_id",
            "observation_id",
        ],
    ),
    mutation(
        "agent.open",
        "Reserve a lane binding and prepare its native root through an operation.",
        &[f("lane_id", S), f("route", S)],
        &["lane_id", "route"],
    ),
    mutation(
        "agent.send",
        "Deliver next-turn input or an exact-turn steer to one binding.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("text", S),
            f("delivery", S),
            f("expected_turn_id", S),
            f("prerequisite_operation_id", S),
        ],
        &["binding_id", "generation", "text"],
    ),
    mutation(
        "agent.reply",
        "Submit native decisions/answers for a binding's current requests.",
        &[f("binding_id", S), f("generation", I), f("reply", O)],
        &["binding_id", "generation", "reply"],
    ),
    mutation(
        "agent.configure",
        "Apply settings to one binding through its native configuration path.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("settings", O),
            f("prerequisite_operation_id", S),
        ],
        &["binding_id", "generation", "settings"],
    ),
    mutation(
        "agent.goal",
        "Set, edit, pause, resume or clear the goal of one binding.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("action", S),
            f("objective", S),
        ],
        &["binding_id", "generation", "action"],
    ),
    mutation(
        "agent.refresh",
        "Refresh one binding's observed state from its native runtime.",
        &[f("binding_id", S), f("generation", I), f("session_id", S)],
        &["binding_id", "generation"],
    ),
    mutation(
        "agent.reconcile",
        "Reconcile one operation's unknown outcome by readback only.",
        &[f("binding_id", S), f("generation", I), f("operation_id", S)],
        &["binding_id", "generation", "operation_id"],
    ),
    mutation(
        "agent.recover",
        "Explicitly recover a recorded native session for one binding.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("expected_boot_id", S),
            f("reason", S),
        ],
        &["binding_id", "generation", "expected_boot_id", "reason"],
    ),
    mutation(
        "agent.result",
        "Request one native result page into a retained artifact.",
        &[
            f("binding_id", S),
            f("generation", I),
            f("selector", O),
            f("offset_bytes", I),
            f("length_bytes", I),
        ],
        &["binding_id", "generation", "selector"],
    ),
    mutation(
        "artifact.assemble",
        "Assemble ordered retained result pages into one verified whole artifact.",
        &[f("page_refs", A), f("expected_sha256", S)],
        &["page_refs"],
    ),
    mutation(
        "operation.cancel",
        "Cancel one queued operation by its handle.",
        &[f("operation_id", S), f("reason", S)],
        &["operation_id", "reason"],
    ),
    mutation(
        "message.send",
        "Send a durable directed mailbox message.",
        &[f("recipient", S), f("text", S), f("in_reply_to", S)],
        &["recipient", "text"],
    ),
];

fn tool_name(method: &str) -> String {
    method.replace('.', "_")
}

fn input_schema(spec: &ToolSpec, read_only: bool) -> Arc<JsonObject> {
    let mut properties = JsonObject::new();
    for field in spec.fields {
        properties.insert(field.name.to_string(), field_schema(field.kind));
    }
    if !read_only {
        properties.insert(
            "client_request_id".to_string(),
            json!({
                "type": "string",
                "description": "Stable logical request ID. Reuse it when retrying after a lost reply; when omitted, one is generated and echoed in the result."
            }),
        );
    }
    let mut schema = json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": false,
    });
    if !spec.required.is_empty() {
        schema["required"] = json!(spec.required);
    }
    match schema {
        Value::Object(map) => Arc::new(map),
        _ => unreachable!("schema literal is an object"),
    }
}

fn field_schema(kind: &str) -> Value {
    match kind {
        "string" => json!({"type": "string"}),
        "string_or_null" => json!({"type": ["string", "null"]}),
        "integer" => json!({"type": "integer"}),
        "boolean" => json!({"type": "boolean"}),
        "object" => json!({"type": "object"}),
        "array" => json!({"type": "array"}),
        _ => json!({}),
    }
}

fn find_tool(name: &str) -> Option<&'static (bool, ToolSpec)> {
    TOOLS
        .iter()
        .find(|(_, spec)| tool_name(spec.method) == name)
}

pub struct McpFacade {
    root: PathBuf,
    credential: Credential,
    ipc_config: Arc<Ipc>,
    client: Mutex<Option<ipc::Client>>,
}

impl McpFacade {
    async fn call(&self, method: &str, mut params: Value, read_only: bool) -> CallToolResult {
        let mut generated_request_id = None;
        if !read_only {
            if !params.is_object() {
                return tool_error(Error::invalid("tool arguments must be an object"));
            }
            if params.get("client_request_id").is_none() {
                let id = model::new_id();
                params["client_request_id"] = json!(id);
                generated_request_id = Some(id);
            }
        }
        let mut client = self.client.lock().await;
        if client.is_none() {
            match ipc::Client::connect(&self.root, &self.credential, &self.ipc_config).await {
                Ok(connected) => *client = Some(connected),
                Err(e) => return tool_error(e),
            }
        }
        let connected = client.as_mut().expect("client connected above");
        match connected.request(method, params).await {
            Ok(mut result) => {
                if let Some(id) = generated_request_id
                    && let Value::Object(map) = &mut result
                {
                    map.entry("client_request_id").or_insert_with(|| json!(id));
                }
                CallToolResult::structured(result)
            }
            Err(e) => {
                // A failed link is never reused or silently retried under a
                // possibly different outcome; the next call reconnects.
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
                tool_error(e)
            }
        }
    }
}

fn tool_error(error: Error) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(
        json!({"error": {"code": error.code, "message": error.message}}).to_string(),
    )])
}

impl ServerHandler for McpFacade {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "eliot-swarm-controller",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Tools map one-to-one onto the swarm controller's application API and are \
                 executed against the running host over local IPC with the configured \
                 credential. Mutations accept a stable client_request_id: supply and \
                 retain your own before dispatch if a mutation must be safely retryable \
                 after a lost reply. When it is omitted, a generated ID is echoed only \
                 in a received result and cannot make a retry safe if that response \
                 itself was lost. Operations returned by mutations are durable handles \
                 to poll with operation_get.",
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
                let mut tool = Tool::new(
                    tool_name(spec.method),
                    spec.description,
                    input_schema(spec, *read_only),
                );
                if *read_only {
                    tool = tool.with_annotations(ToolAnnotations::new().read_only(true));
                }
                tool
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
        _context: RequestContext<RoleServer>,
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
        Ok(self.call(spec.method, params, *read_only).await.into())
    }
}

/// Serve MCP over stdio until the client disconnects. The host connection is
/// established lazily on the first tool call, so discovery works before (and
/// independently of) host availability.
pub async fn run(config: Config, credential: Credential) -> Result<()> {
    let facade = McpFacade {
        root: config.storage.data_dir.clone(),
        credential,
        ipc_config: Arc::new(config.ipc.clone()),
        client: Mutex::new(None),
    };
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
        // The public method list from the README: 20 reads + 27 mutations.
        assert_eq!(methods.len(), 47);
        assert_eq!(TOOLS.iter().filter(|(read_only, _)| *read_only).count(), 20);
    }

    #[test]
    fn schemas_are_closed_objects() {
        for (read_only, spec) in TOOLS {
            let schema = input_schema(spec, *read_only);
            assert_eq!(schema["type"], json!("object"));
            assert_eq!(schema["additionalProperties"], json!(false));
            if !read_only {
                assert!(schema["properties"].get("client_request_id").is_some());
            }
        }
    }
}
