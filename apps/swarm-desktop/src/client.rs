//! Typed RPC caller for the desktop client.
//!
//! Every call goes through the existing authenticated daemon client
//! (`eliot_daemon::client_call_typed` and the `eliot_core::api` DTOs) with
//! the same request/response validators the CLI runs. There is no second
//! executor, no UI-owned database and no new endpoint: `project.list` is the
//! one exception to the typed boundary because it returns a bare JSON array,
//! so it is decoded from the raw client result with the same field checks.

use std::path::{Path, PathBuf};

use eliot_core::api::{
    AgentGetRequest, AgentGetResponse, AgentListRequest, AgentListResponse, AgentResultRequest,
    AgentResultResponse, AgentSummary, CommandGetRequest, CommandSubmitRequest,
    CommandSubmitResponse, ProjectRegistrationView, TurnGetRequest, discovery::ProjectListRequest,
};
use eliot_core::{Command, CommandState, Error, Result, Turn, TurnState};
use serde_json::Value;

const MAX_TEXT_BYTES: usize = 512 * 1024;
const POLL_INTERVAL_MS: u64 = 500;
const POLL_ROUNDS: usize = 240;

/// Resolve the daemon data directory exactly like the CLI: explicit override,
/// then `ELIOT_HOME`, then `%LOCALAPPDATA%\Eliot`, then the home share.
pub fn data_dir() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("ELIOT_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = std::env::var_os("LOCALAPPDATA").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path).join("Eliot"));
    }
    if let Some(path) = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path)
            .join(".local")
            .join("share")
            .join("eliot"));
    }
    Err(Error::new(
        "invalid",
        "cannot determine the data directory; set --data-dir or ELIOT_HOME",
    ))
}

/// One fresh mutation id per user action. Callers retain it and pass it back
/// on retry instead of minting a second id.
pub fn new_mutation_id() -> String {
    ulid::Ulid::new().to_string()
}

/// Drive one async daemon call from a non-async UI worker thread.
pub fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("desktop tokio runtime")
        .block_on(future)
}

fn check_text(text: &str) -> Result<()> {
    if text.trim().is_empty() {
        return Err(Error::new("invalid", "input text must not be blank"));
    }
    if text.len() > MAX_TEXT_BYTES {
        return Err(Error::new(
            "invalid",
            "input text exceeds the 512 KiB limit",
        ));
    }
    Ok(())
}

/// Project registrations for the launch picker. `project.list` returns a bare
/// array (not an object), so it cannot pass the typed object boundary; the
/// rows get the same field validation instead.
pub async fn list_projects(data_dir: &Path) -> Result<Vec<ProjectRegistrationView>> {
    let params = ProjectListRequest {};
    params.validate()?;
    let value =
        eliot_daemon::client_call(data_dir, "project.list", Value::Object(Default::default()))
            .await?;
    let registrations: Vec<ProjectRegistrationView> = serde_json::from_value(value)
        .map_err(|_| Error::new("unknown", "project.list returned a malformed result"))?;
    for registration in &registrations {
        registration.validate()?;
    }
    Ok(registrations)
}

/// Current Agents, optionally narrowed to one project. The UI always passes
/// an explicit project filter; the unfiltered fleet view is not needed here.
pub async fn list_agents(data_dir: &Path, project: Option<&str>) -> Result<Vec<AgentSummary>> {
    let params = AgentListRequest {
        project: project.map(str::to_owned),
        limit: 100,
        ..Default::default()
    };
    params.validate()?;
    let reply: AgentListResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.list", &params).await?;
    for row in &reply.agents {
        row.validate()?;
    }
    Ok(reply.agents.into_iter().map(|row| row.summary).collect())
}

/// Full readback for one Agent: generation, active turn, pending requests,
/// profile snapshot and effective settings.
pub async fn get_agent(data_dir: &Path, id: &str) -> Result<AgentGetResponse> {
    let params = AgentGetRequest::new(id);
    params.validate()?;
    let reply: AgentGetResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.get", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Latest retained final text for one Agent, with its turn binding.
pub async fn latest_final_text(data_dir: &Path, agent: &str) -> Result<Option<(String, String)>> {
    let params = AgentResultRequest::new(agent);
    params.validate()?;
    let reply: AgentResultResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.result", &params).await?;
    let mut latest: Option<(String, String)> = None;
    for turn in &reply.turns {
        if let Some(text) = turn.final_text.as_deref() {
            latest = Some((turn.turn_id.clone(), text.to_owned()));
        }
    }
    Ok(latest)
}

fn submit_request(
    mutation_id: String,
    target: Option<String>,
    verb: &str,
    args: Value,
) -> Result<CommandSubmitRequest> {
    let mut request = CommandSubmitRequest::new(mutation_id, verb);
    request.target = target;
    request.args = args;
    Ok(request)
}

async fn submit(data_dir: &Path, request: &CommandSubmitRequest) -> Result<CommandSubmitResponse> {
    request.validate()?;
    let reply: CommandSubmitResponse =
        eliot_daemon::client_call_typed(data_dir, "command.submit", request).await?;
    request.validate_response(&reply)?;
    Ok(reply)
}

async fn read_command(data_dir: &Path, id: &str) -> Result<Command> {
    let params = CommandGetRequest::new(id);
    params.validate()?;
    let command: Command =
        eliot_daemon::client_call_typed(data_dir, "command.get", &params).await?;
    params.validate_response(&command)?;
    Ok(command)
}

async fn read_turn(data_dir: &Path, agent: &str, generation: i64, turn_id: &str) -> Result<Turn> {
    let params = TurnGetRequest::new(agent, generation, turn_id);
    params.validate()?;
    let turn: Turn = eliot_daemon::client_call_typed(data_dir, "turn.get", &params).await?;
    params.validate_response(&turn)?;
    Ok(turn)
}

/// First prompt as one submission: nonblank text rides the launch admission
/// and the linked `<id>/send` command must appear in the reply.
pub async fn submit_launch(
    data_dir: &Path,
    mutation_id: String,
    profile: Option<String>,
    workspace: &str,
    text: Option<String>,
) -> Result<CommandSubmitResponse> {
    if profile.as_deref().is_none_or(|profile| profile.is_empty()) {
        return Err(Error::new(
            "invalid",
            "a profile is required for a parentless launch",
        ));
    }
    if let Some(text) = text.as_deref() {
        check_text(text)?;
    }
    let mut args = serde_json::json!({ "workspace": workspace });
    if let Some(profile) = profile {
        args["profile"] = Value::String(profile);
    }
    if let Some(text) = text {
        args["text"] = Value::String(text);
    }
    submit(
        data_dir,
        &submit_request(mutation_id, None, "launch", args)?,
    )
    .await
}

/// Follow-up input to an existing Agent: plain SEND admission, no turn
/// capture. Generation/turn binding is only added by turn-control verbs.
pub async fn submit_send(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    text: &str,
) -> Result<CommandSubmitResponse> {
    check_text(text)?;
    submit(
        data_dir,
        &submit_request(
            mutation_id,
            Some(agent.to_owned()),
            "send",
            serde_json::json!({ "text": text }),
        )?,
    )
    .await
}

/// Interrupt exactly the bound turn. A missing turn binding is captured once
/// from `agent.get` (the Agent's current active turn), like the CLI's exact
/// target resolution; explicit flags are never re-read after capture.
pub async fn submit_interrupt(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: Option<i64>,
    turn_id: Option<String>,
) -> Result<CommandSubmitResponse> {
    let (generation, turn_id) = match (generation, turn_id) {
        (Some(generation), Some(turn_id)) => (generation, turn_id),
        (generation, turn_id) => {
            let reply = get_agent(data_dir, agent).await?;
            let active = reply.agent.active_turn.clone().ok_or_else(|| {
                Error::new("invalid", "the agent has no active turn to interrupt")
            })?;
            (
                generation.unwrap_or(reply.agent.generation),
                turn_id.unwrap_or(active),
            )
        }
    };
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "interrupt",
        serde_json::json!({}),
    )?;
    request.expected_generation = Some(generation);
    request.expected_turn_id = Some(turn_id);
    submit(data_dir, &request).await
}

fn approval_args(choice: Option<String>, feedback: Option<String>) -> Value {
    let mut args = serde_json::Map::new();
    if let Some(choice) = choice {
        args.insert("choice".to_owned(), Value::String(choice));
    }
    if let Some(feedback) = feedback {
        args.insert("feedback".to_owned(), Value::String(feedback));
    }
    Value::Object(args)
}

/// Exact pending-request reply inputs, captured together at click time.
pub struct ApprovalInput {
    /// Target Agent id.
    pub agent: String,
    /// Agent generation observed when the control was armed.
    pub generation: i64,
    /// Pending request id being answered.
    pub request: String,
    /// True for approve, false for deny.
    pub approve: bool,
    /// Optional choice payload for choice-bearing requests.
    pub choice: Option<String>,
    /// Optional operator feedback.
    pub feedback: Option<String>,
}

/// Approve or deny one exact pending request: the request id captured at
/// click time, never re-read after a selection change.
pub async fn submit_approval(
    data_dir: &Path,
    mutation_id: String,
    input: ApprovalInput,
) -> Result<CommandSubmitResponse> {
    let verb = if input.approve { "approve" } else { "deny" };
    let mut request = submit_request(
        mutation_id,
        Some(input.agent),
        verb,
        approval_args(input.choice, input.feedback),
    )?;
    request.expected_generation = Some(input.generation);
    request.request_id = Some(input.request);
    submit(data_dir, &request).await
}

/// Answer a pending question that is not a binary approval. The payload must
/// already be a JSON object keyed by question id; it is never built by
/// guessing from the pending details.
pub async fn submit_answer(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
    request_id: &str,
    answers: Value,
) -> Result<CommandSubmitResponse> {
    if !answers.is_object() {
        return Err(Error::new(
            "invalid",
            "answers must be a JSON object keyed by question id",
        ));
    }
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "answer",
        serde_json::json!({ "answers": answers }),
    )?;
    request.expected_generation = Some(generation);
    request.request_id = Some(request_id.to_owned());
    submit(data_dir, &request).await
}

/// Settle one submitted command: applied, or a terminal outcome that keeps
/// its contract meaning. Unknown stays unknown; cancellation is reported,
/// never mistaken for success. Bounded so a UI worker always comes home.
pub async fn wait_command_settled(data_dir: &Path, command_id: &str) -> Result<Command> {
    for _ in 0..POLL_ROUNDS {
        let command = read_command(data_dir, command_id).await?;
        match command.state {
            CommandState::Applied => return Ok(command),
            CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => {
                tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
            CommandState::Rejected => {
                return Err(Error::new(
                    "rejected",
                    "command was rejected before native I/O",
                ));
            }
            CommandState::Failed => return Err(Error::new("failed", "command failed")),
            CommandState::Unknown => {
                return Err(Error::new("unknown", "command outcome is unknown"));
            }
            CommandState::Cancelled => {
                return Err(Error::new("cancelled", "command was cancelled"));
            }
        }
    }
    Err(Error::new(
        "unknown",
        "command did not settle in the readback window",
    ))
}

/// Read back the final text behind one settled command: the command's own
/// turn when it carries one, otherwise the linked `<id>/send` input turn
/// created by a launch-with-text admission.
pub async fn command_final_text(
    data_dir: &Path,
    command: &Command,
    linked_command: Option<&str>,
) -> Result<Option<(String, String)>> {
    if let Some(turn_id) = command.turn_id.as_deref() {
        let agent = command
            .target
            .as_deref()
            .ok_or_else(|| Error::new("unknown", "settled command has no agent binding"))?;
        let generation = command
            .generation
            .ok_or_else(|| Error::new("unknown", "settled command has no generation binding"))?;
        for _ in 0..POLL_ROUNDS {
            let turn = read_turn(data_dir, agent, generation, turn_id).await?;
            match turn.state {
                TurnState::Completed => {
                    return Ok(turn
                        .final_text
                        .clone()
                        .map(|text| (turn.turn_id.clone(), text)));
                }
                TurnState::Unknown => {
                    return Err(Error::new("unknown", "turn outcome is unknown"));
                }
                TurnState::Running => {
                    tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
                }
            }
        }
        return Err(Error::new(
            "unknown",
            "turn did not settle in the readback window",
        ));
    }
    if let Some(linked_id) = linked_command {
        let linked = wait_command_settled(data_dir, linked_id).await?;
        return Box::pin(command_final_text(data_dir, &linked, None)).await;
    }
    Ok(None)
}
