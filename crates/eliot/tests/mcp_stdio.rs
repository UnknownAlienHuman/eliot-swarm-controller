//! Real SDK MCP client -> new stdio child -> real daemon/router -> fake.
//!
//! Exercises the actual `rmcp` stdio service path (`initialize`,
//! `tools/list`, `tools/call` over `transport-io` / `transport-child-process`)
//! rather than a hand-rolled JSON-RPC stub: the client spawns the real
//! `swarm mcp` binary via `rmcp::transport::child_process::TokioChildProcess`.
//! Fixture daemon/router + fake are the existing test support (`Fixture`).
//! Dispatch goes through `eliot_daemon::client_call`, so identity is the
//! inherited process environment of the stdio child: partial or conflicting
//! Agent/Run/Owner credentials fail closed with `not_allowed` (no owner
//! fallback), and the same Router scope/errors (`unknown`, `unsupported`,
//! `id_conflict`, `frame_too_large`) are surfaced through the tool result.

use std::path::Path;
use std::time::Duration;

use rmcp::model::{
    CallToolRequestParams, ClientCapabilities, ClientConfig, Implementation, JsonObject,
};
use rmcp::transport::child_process::TokioChildProcess;
use serde_json::{Value, json};

mod support {
    include!("support/mod.rs");
}

use support::{CLI, Fixture};

const TIMEOUT: Duration = Duration::from_secs(20);

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    rt.block_on(async {
        tokio::time::timeout(TIMEOUT, single_connected_scenario())
            .await
            .expect("MCP stdio scenario deadline");
    });
}

type ClientService = rmcp::service::RunningService<rmcp::RoleClient, ClientConfig>;

async fn connect(data_dir: &Path, envs: &[(&str, String)]) -> std::io::Result<ClientService> {
    let data_dir_str = data_dir.to_string_lossy().into_owned();
    let mut cmd = tokio::process::Command::new(CLI);
    cmd.arg("--data-dir")
        .arg(&data_dir_str)
        .arg("mcp")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    for (key, val) in envs {
        cmd.env(key, val);
    }
    // Scrub identity unless explicitly set by the caller.
    for key in support::AMBIENT_IDENTITY_ENV_KEYS {
        if !envs.iter().any(|(name, _)| name == key) {
            cmd.env_remove(key);
        }
    }
    let transport =
        TokioChildProcess::new(cmd).map_err(|e| std::io::Error::other(e.to_string()))?;
    let client = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("mcp-stdio-test", "0.1"),
    );
    // Real handshake via the SDK's client lifecycle entry point.
    use rmcp::service::ClientServiceExt;
    client
        .serve_with_lifecycle(transport, rmcp::service::ClientLifecycleMode::Initialize)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))
}

async fn single_connected_scenario() {
    let mut fixture = Fixture::new("mcp-stdio");
    fixture.start();
    let mut client = connect(&fixture.data, &[])
        .await
        .expect("MCP client connect");

    let tools = client.peer().list_tools(None).await.expect("tools/list");
    assert_eq!(tools.tools.len(), 1);
    assert_eq!(tools.tools[0].name.as_ref(), "swarm");

    let key = format!("mcp-stdio-{}", ulid::Ulid::new());
    let value = json!({"origin": "rmcp-stdio"});
    let set = client
        .call_tool(tool_call(
            "var.set",
            json!({
                "id": ulid::Ulid::new().to_string(),
                "scope": "global",
                "key": key,
                "value": value,
            }),
        ))
        .await
        .expect("var.set over MCP");
    let set = set.structured_content.expect("structured var.set result");
    assert_eq!(set["method"], "var.set");
    assert_eq!(set["result"]["key"], key);
    assert_eq!(set["result"]["version"], 1);

    let readback = client
        .call_tool(tool_call("var.get", json!({"scope": "global", "key": key})))
        .await
        .expect("var.get over MCP");
    let readback = readback
        .structured_content
        .expect("structured var.get result");
    assert_eq!(readback["method"], "var.get");
    assert_eq!(readback["result"]["key"], key);
    assert_eq!(readback["result"]["value"], value);
    assert_eq!(readback["result"]["version"], 1);

    client.close().await.expect("close MCP client");
    fixture.stop(false);
}

fn tool_call(method: &str, params: Value) -> CallToolRequestParams {
    let arguments: JsonObject = serde_json::from_value(json!({
        "method": method,
        "params": params,
    }))
    .expect("MCP tool arguments");
    CallToolRequestParams::new("swarm").with_arguments(arguments)
}
