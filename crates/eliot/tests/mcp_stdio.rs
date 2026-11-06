//! Real RMCP client -> stdio child -> daemon/router/store -> Fake.
//!
//! Covers tools/list, var.set/get, and caller-bound command.submit for Fake
//! launch/send/stop. It checks caller-known IDs, retained same-ID replay,
//! changed-payload id_conflict, and one returned fake turn_id.

use std::path::Path;
use std::time::Duration;

use eliot_core::{
    Command as CoreCommand, CommandState,
    api::{
        AgentResultRequest, AgentResultResponse, CommandGetRequest, CommandSubmitRequest,
        CommandSubmitResponse,
    },
};
use rmcp::model::{
    CallToolRequestParams, ClientCapabilities, ClientConfig, Implementation, JsonObject,
};
use rmcp::transport::child_process::TokioChildProcess;
use serde_json::{Value, json};

#[allow(dead_code)]
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
    std::fs::create_dir_all(&fixture.data).expect("fixture data directory");
    std::fs::write(
        fixture.data.join("config.toml"),
        "[profiles.mcp_fake]\nharness = \"fake\"\n",
    )
    .expect("write Fake profile");
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

    let workspace = fixture.workspace.to_string_lossy().into_owned();
    let canonical_workspace = std::fs::canonicalize(&fixture.workspace)
        .expect("canonical fixture workspace")
        .to_string_lossy()
        .into_owned();
    let launch_id = ulid::Ulid::new().to_string();
    let mut launch = CommandSubmitRequest::new(launch_id.clone(), "launch");
    launch.expected_generation = Some(0);
    launch.args = json!({"profile": "mcp_fake", "workspace": workspace});
    let launch_response = submit_mcp(&client, &launch).await;
    assert_eq!(launch_response.command_id, launch_id);
    let launched = wait_mcp_applied(&client, &launch_id).await;
    assert_eq!(launched.id, launch_id);
    assert_eq!(launched.verb, "launch");
    assert_eq!(launched.expected_generation, Some(0));
    assert_eq!(launched.generation, Some(1));
    assert_eq!(launched.args["profile"], "mcp_fake");
    assert_eq!(launched.args["workspace"], canonical_workspace);
    let agent_id = launched.target.clone().expect("Fake launch Agent");

    let send_id = ulid::Ulid::new().to_string();
    let mut send = CommandSubmitRequest::new(send_id.clone(), "send");
    send.target = Some(agent_id.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text": "one MCP Fake turn"});
    let first_send = submit_mcp(&client, &send).await;
    assert_eq!(first_send.command_id, send_id);
    assert_eq!(first_send.agent.as_deref(), Some(agent_id.as_str()));
    assert_eq!(first_send.generation, Some(1));
    let sent = wait_mcp_applied(&client, &send_id).await;
    assert_eq!(sent.id, send_id);
    assert_eq!(sent.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(sent.expected_generation, Some(1));
    assert_eq!(sent.generation, Some(1));
    let turn_id = sent.turn_id.clone().expect("send turn binding");

    let replay = submit_mcp(&client, &send).await;
    assert_eq!(replay.command_id, send_id);
    assert_eq!(replay.agent.as_deref(), Some(agent_id.as_str()));
    assert_eq!(replay.generation, Some(1));
    assert_eq!(replay.state, sent.state);
    assert_eq!(replay.outcome, sent.outcome);
    let replayed = wait_mcp_applied(&client, &send_id).await;
    assert_eq!(replayed.turn_id.as_deref(), Some(turn_id.as_str()));

    let mut changed = send.clone();
    changed.args = json!({"text": "changed payload"});
    let conflict = client
        .call_tool(tool_call(
            "command.submit",
            serde_json::to_value(changed).expect("conflicting submit DTO"),
        ))
        .await
        .expect("id_conflict is a structured tool result");
    assert_eq!(conflict.is_error, Some(true));
    let conflict = conflict
        .structured_content
        .expect("structured id_conflict result");
    assert_eq!(conflict["code"], "id_conflict");

    let mut result_request = AgentResultRequest::new(agent_id.clone());
    result_request.generation = Some(1);
    let result: AgentResultResponse = serde_json::from_value(
        mcp_result(
            &client,
            "agent.result",
            serde_json::to_value(result_request).expect("AgentResultRequest DTO"),
        )
        .await,
    )
    .expect("typed Fake result readback");
    assert_eq!(result.agent, agent_id);
    assert_eq!(result.generation, 1);
    assert_eq!(result.turns.len(), 1, "replay/conflict added a NativeTurn");
    assert_eq!(result.turns[0].turn_id, turn_id);

    let stop_id = ulid::Ulid::new().to_string();
    let mut stop = CommandSubmitRequest::new(stop_id.clone(), "stop");
    stop.target = Some(agent_id.clone());
    stop.expected_generation = Some(1);
    stop.args = json!({"force": false});
    let stop_response = submit_mcp(&client, &stop).await;
    assert_eq!(stop_response.command_id, stop_id);
    let stopped = wait_mcp_applied(&client, &stop_id).await;
    assert_eq!(stopped.id, stop_id);
    assert_eq!(stopped.verb, "stop");
    assert_eq!(stopped.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(stopped.expected_generation, Some(1));

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

async fn submit_mcp(
    client: &ClientService,
    request: &CommandSubmitRequest,
) -> CommandSubmitResponse {
    let response: CommandSubmitResponse = serde_json::from_value(
        mcp_result(
            client,
            "command.submit",
            serde_json::to_value(request).expect("CommandSubmitRequest DTO"),
        )
        .await,
    )
    .expect("typed CommandSubmitResponse");
    request
        .validate_response(&response)
        .expect("submit response matches caller binding");
    response
}

async fn wait_mcp_applied(client: &ClientService, id: &str) -> CoreCommand {
    loop {
        let command: CoreCommand = serde_json::from_value(
            mcp_result(
                client,
                "command.get",
                serde_json::to_value(CommandGetRequest::new(id)).expect("CommandGetRequest DTO"),
            )
            .await,
        )
        .expect("typed command readback");
        assert_eq!(command.id, id);
        if command.state == CommandState::Applied {
            return command;
        }
        assert_ne!(command.state, CommandState::Unknown);
        assert!(
            !command.state.is_terminal(),
            "command did not apply: {command:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn mcp_result(client: &ClientService, method: &str, params: Value) -> Value {
    let response = client
        .call_tool(tool_call(method, params))
        .await
        .unwrap_or_else(|error| panic!("MCP {method} call failed: {error}"));
    assert_ne!(response.is_error, Some(true), "MCP {method} returned error");
    let response = response.structured_content.expect("structured MCP result");
    assert_eq!(response["method"], method);
    response["result"].clone()
}
