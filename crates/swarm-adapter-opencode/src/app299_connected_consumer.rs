use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
use swarm_contracts::Credential;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};

#[tokio::test]
async fn app299_public_tasks_duplicate_creates_one_exact_native_root() {
    crate::app320_connected_consumer::run_app299_case()
        .await
        .unwrap();
}

// This client speaks the actual public ProfiledFacade JSON-RPC surface. The
// fixture runner owns the production host/supervisor/adapter and pauses the
// adapter after module.next so the Tasks calls can be checked before effect.
pub(super) struct PublicTasksClient {
    reader: BufReader<tokio::io::ReadHalf<DuplexStream>>,
    writer: tokio::io::WriteHalf<DuplexStream>,
    next_id: i64,
    server: tokio::task::JoinHandle<()>,
}

impl PublicTasksClient {
    pub(super) async fn connect(data_dir: PathBuf, credential: Credential) -> Result<Self, String> {
        let profile_name = "app299-connected-test";
        let frontend = swarm_mcp::Config {
            storage: swarm_mcp::config::Storage { data_dir },
            ipc: swarm_mcp::config::Ipc::default(),
            mcp: swarm_mcp::config::McpConfig {
                default_profile: profile_name.to_owned(),
                profiles: BTreeMap::from([(
                    profile_name.to_owned(),
                    swarm_mcp::config::McpProfileConfig {
                        tool_profile: swarm_mcp::config::McpToolProfile::Full,
                        expected_client_id: credential.client_id.clone(),
                        surface: None,
                        deferred_groups: Vec::new(),
                        manual_tools: Vec::new(),
                    },
                )]),
            },
        };
        let facade = swarm_mcp::profiled_facade(&frontend, credential, Some(profile_name))
            .map_err(|error| error.to_string())?;
        let (server_io, client_io) = tokio::io::duplex(256 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server = tokio::spawn(async move {
            if let Ok(running) = facade.serve((server_read, server_write)).await {
                let _ = running.waiting().await;
            }
        });
        let (read, writer) = tokio::io::split(client_io);
        let mut client = Self {
            reader: BufReader::new(read),
            writer,
            next_id: 0,
            server,
        };
        let initialized = async {
            let initialized = client
                .request(
                    "initialize",
                    json!({
                        "protocolVersion":"2025-11-25",
                        "capabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}},
                        "clientInfo":{"name":"app299-connected-consumer","version":"1"}
                    }),
                )
                .await?;
            if initialized["capabilities"]["extensions"]["io.modelcontextprotocol/tasks"]
                != json!({})
            {
                return Err(
                    "public facade did not advertise the negotiated Tasks extension".to_owned(),
                );
            }
            client.notify("notifications/initialized").await?;
            Ok::<(), String>(())
        }
        .await;
        if let Err(error) = initialized {
            client.close().await;
            return Err(error);
        }
        Ok(client)
    }

    pub(super) async fn agent_open_duplicate(
        &mut self,
        original_request: &Value,
        expected_operation_id: &str,
    ) -> Result<String, String> {
        let first = self
            .call_tool("agent_open", original_request.clone())
            .await?;
        let duplicate = self
            .call_tool("agent_open", original_request.clone())
            .await?;
        if first["isError"] == json!(true)
            || duplicate["isError"] == json!(true)
            || first["resultType"] != json!("task")
            || duplicate["resultType"] != json!("task")
            || first["status"] != json!("working")
            || duplicate["status"] != json!("working")
            || first["taskId"] != json!(expected_operation_id)
            || duplicate["taskId"] != json!(expected_operation_id)
        {
            return Err(
                "public agent_open retries did not replay the exact in-flight launcher child"
                    .to_owned(),
            );
        }
        Ok(expected_operation_id.to_owned())
    }

    pub(super) async fn task_get(&mut self, task_id: &str) -> Result<Value, String> {
        self.request("tasks/get", json!({"taskId":task_id})).await
    }

    async fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value, String> {
        self.request("tools/call", json!({"name":name,"arguments":arguments}))
            .await
    }

    async fn notify(&mut self, method: &str) -> Result<(), String> {
        let mut line = serde_json::to_string(&json!({"jsonrpc":"2.0","method":method}))
            .map_err(|error| error.to_string())?;
        line.push('\n');
        tokio::time::timeout(
            Duration::from_secs(30),
            self.writer.write_all(line.as_bytes()),
        )
        .await
        .map_err(|_| "MCP notification write timed out".to_owned())?
        .map_err(|error| error.to_string())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
        self.next_id += 1;
        let id = self.next_id;
        let mut line = serde_json::to_string(&json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":method,
            "params":params
        }))
        .map_err(|error| error.to_string())?;
        line.push('\n');
        tokio::time::timeout(
            Duration::from_secs(30),
            self.writer.write_all(line.as_bytes()),
        )
        .await
        .map_err(|_| format!("MCP {method} write timed out"))?
        .map_err(|error| error.to_string())?;
        let mut response = String::new();
        loop {
            response.clear();
            let count = tokio::time::timeout(
                Duration::from_secs(90),
                self.reader.read_line(&mut response),
            )
            .await
            .map_err(|_| format!("MCP {method} response timed out"))?
            .map_err(|error| error.to_string())?;
            if count == 0 {
                return Err("public MCP facade closed the connected transport".to_owned());
            }
            let message: Value = serde_json::from_str(response.trim())
                .map_err(|error| format!("invalid MCP JSON-RPC response: {error}"))?;
            if message["id"] != json!(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(format!("public MCP {method} returned an error: {error}"));
            }
            return message
                .get("result")
                .cloned()
                .ok_or_else(|| format!("public MCP {method} response omitted result"));
        }
    }

    pub(super) async fn close(mut self) {
        self.server.abort();
        let _ = (&mut self.server).await;
    }
}

impl Drop for PublicTasksClient {
    fn drop(&mut self) {
        self.server.abort();
    }
}

type RetainedAgentOpenRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    String,
    String,
);

type RetainedLaunchParentRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
);

pub(super) fn assert_retained_agent_open(
    data_dir: &Path,
    operation_id: &str,
    launch_operation_id: &str,
    route_alias: &str,
    allowed_workspace_root: &Path,
) -> Result<Value, String> {
    let database = data_dir.join("swarm.db");
    let connection = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("read-only Store database open failed: {error}"))?;
    let (
        method,
        caller_id,
        client_request_id,
        prerequisite_operation_id,
        task_id,
        attempt_id,
        binding_id,
        binding_generation,
        state,
        original_json,
        effective_json,
    ): RetainedAgentOpenRow = connection
        .query_row(
            "SELECT method,caller_id,client_request_id,prerequisite_operation_id,task_id,attempt_id,                    binding_id,binding_generation,state,original_request_json,effective_request_json              FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .map_err(|error| format!("retained Store child read failed: {error}"))?;
    let (
        parent_method,
        parent_caller_id,
        parent_state,
        parent_task_id,
        parent_attempt_id,
        parent_binding_id,
        parent_binding_generation,
        parent_effective_json,
    ): RetainedLaunchParentRow = connection
        .query_row(
            "SELECT method,caller_id,state,task_id,attempt_id,binding_id,binding_generation,effective_request_json              FROM operations WHERE operation_id=?1",
            [launch_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .map_err(|error| format!("retained Store launch parent read failed: {error}"))?;
    if method != "agent.open"
        || parent_method != "swarm.launch"
        || parent_state != "queued"
        || caller_id != parent_caller_id
        || client_request_id.as_deref()
            != Some(format!("launch:{launch_operation_id}:open").as_str())
        || prerequisite_operation_id.as_deref() != Some(launch_operation_id)
        || task_id != parent_task_id
        || attempt_id != parent_attempt_id
        || binding_id != parent_binding_id
        || binding_generation != parent_binding_generation
        || !["queued", "sending"].contains(&state.as_str())
    {
        return Err("retained agent.open is not the exact launcher child under the same actor/Task/Attempt/binding".to_owned());
    }
    let task_id = task_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "launcher child omitted its Task identity".to_owned())?;
    let attempt_id = attempt_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "launcher child omitted its Attempt identity".to_owned())?;
    let binding_id = binding_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "launcher child omitted its binding identity".to_owned())?;
    let binding_generation = binding_generation
        .filter(|generation| *generation > 0)
        .ok_or_else(|| "launcher child omitted its binding generation".to_owned())?;
    let original: Value = serde_json::from_str(&original_json)
        .map_err(|error| format!("retained original request is invalid: {error}"))?;
    let effective: Value = serde_json::from_str(&effective_json)
        .map_err(|error| format!("retained effective request is invalid: {error}"))?;
    let parent_effective: Value = serde_json::from_str(&parent_effective_json)
        .map_err(|error| format!("retained launch manifest is invalid: {error}"))?;
    let manifest = &parent_effective["launch_manifest"];
    let lease = &manifest["workspace"]["lease_authority"];
    let lease_id = lease["lease_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "launcher manifest omitted held lease identity".to_owned())?;
    let retained_lease: Value = connection
        .query_row(
            "SELECT task_id,attempt_id,operation_id,workspace_path,state FROM workspace_leases WHERE lease_id=?1",
            [lease_id],
            |row| {
                Ok(json!({
                    "task_id":row.get::<_, String>(0)?,
                    "attempt_id":row.get::<_, Option<String>>(1)?,
                    "operation_id":row.get::<_, Option<String>>(2)?,
                    "workspace_path":row.get::<_, String>(3)?,
                    "state":row.get::<_, String>(4)?,
                }))
            },
        )
        .map_err(|error| format!("retained held workspace lease read failed: {error}"))?;
    let lease_directory = retained_lease["workspace_path"]
        .as_str()
        .filter(|path| !path.is_empty())
        .ok_or_else(|| "held lease omitted its workspace directory".to_owned())?;
    let canonical_lease_directory = std::fs::canonicalize(lease_directory)
        .map_err(|error| format!("held lease workspace cannot be resolved: {error}"))?;
    let canonical_workspace_root =
        std::fs::canonicalize(allowed_workspace_root).map_err(|error| {
            format!("configured project workspace root cannot be resolved: {error}")
        })?;
    if !canonical_lease_directory.starts_with(&canonical_workspace_root)
        || canonical_lease_directory == canonical_workspace_root
    {
        return Err(
            "held lease workspace is not a strict descendant of the configured project root"
                .to_owned(),
        );
    }
    let expected_request_id = format!("launch:{launch_operation_id}:open");
    let expected_lane_id = format!("launch-{lease_id}");
    let exact_request_fields = original.as_object().is_some_and(|fields| {
        fields.len() == 3
            && fields.contains_key("client_request_id")
            && fields.contains_key("lane_id")
            && fields.contains_key("route")
    });
    if !exact_request_fields
        || original["client_request_id"] != json!(expected_request_id)
        || original["lane_id"] != json!(expected_lane_id)
        || original["route"] != json!(route_alias)
        || manifest["task"]["task_id"] != json!(task_id)
        || manifest["task"]["attempt_id"] != json!(attempt_id)
        || manifest["binding"]["operation_id"] != json!(operation_id)
        || manifest["binding"]["binding_id"] != json!(binding_id)
        || manifest["binding"]["generation"] != json!(binding_generation)
        || lease["state"] != json!("held")
        || lease["operation_id"] != json!(launch_operation_id)
        || lease["task_id"] != json!(task_id)
        || lease["attempt_id"] != json!(attempt_id)
        || effective["route"]["alias"] != json!(route_alias)
        || effective["route"]["workspace_option"] != json!("directory")
        || retained_lease["state"] != json!("held")
        || retained_lease["operation_id"] != json!(launch_operation_id)
        || retained_lease["task_id"] != json!(task_id)
        || retained_lease["attempt_id"] != json!(attempt_id)
        || effective["route"]["native_options"]["directory"] != json!(lease_directory)
    {
        return Err("retained agent.open differs from its exact launcher request, lease, route, or workspace".to_owned());
    }
    Ok(original)
}
