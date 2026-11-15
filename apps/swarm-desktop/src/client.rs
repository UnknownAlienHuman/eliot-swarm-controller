//! Typed RPC caller for the desktop client.
//!
//! Every call goes through the existing authenticated daemon client
//! (`eliot_daemon::client_call_typed` and the `eliot_core::api` DTOs) and runs
//! the same request/response validators the CLI runs; deserialization alone
//! is not validation. There is no second executor, no UI-owned database and
//! no new endpoint: `project.list` is the one exception to the typed boundary
//! because it returns a bare JSON array, so it is decoded from the raw client
//! result with the same field checks.

use std::path::{Path, PathBuf};

use eliot_core::api::{
    AgentGetRequest, AgentGetResponse, AgentListRequest, AgentListResponse, AgentResultRequest,
    AgentResultResponse, CommandChildrenRequest, CommandChildrenResponse, CommandGetRequest,
    CommandSubmitRequest, CommandSubmitResponse, EventsBoundsRequest, EventsBoundsResponse,
    EventsFilter, EventsReadRequest, EventsReadResponse, EventsSubscribeRequest,
    EventsSubscribeResponse, HarnessModelsRequest, HarnessModelsResponse, ProfileListRequest,
    ProfileListResponse, ProfilesReadRequest, ProfilesReadResponse, ProfilesSaveRequest,
    ProfilesSaveResponse, ProjectRegistrationView, RuleListEntry, RuleListRequest,
    RuleListResponse, ScriptListRequest, ScriptListResponse, ScriptReadRequest, ScriptReadResponse,
    ScriptSaveRequest, ScriptSaveResponse, TimerRunRequest, UsageSummaryRequest,
    UsageSummaryResponse, discovery::ProjectListRequest,
};
use eliot_core::{Command, Error, Result};
use futures_util::StreamExt;
use serde_json::Value;

use crate::chat::{CHAT_KINDS, InputText, bound_text};
use crate::live::WAKE_WAIT_MS;

const MAX_TEXT_BYTES: usize = 512 * 1024;
/// Concurrent ordinary reads issued by one worker (the issue's initial cap).
pub const MAX_CONCURRENT_READS: usize = 4;
/// Server-side scan bound per `events.read` call.
const EVENT_SCAN_ROWS: usize = 1_000;

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

/// One validated `agent.list` page. The response passes the canonical
/// `validate_response` (row filters, cursor, totals), not only row checks.
pub async fn list_agents(data_dir: &Path, params: &AgentListRequest) -> Result<AgentListResponse> {
    params.validate()?;
    let reply: AgentListResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.list", params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
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

/// Newest `limit` retained turns of the current generation of the Agent.
/// `agent.result` has no cursor: older pages are a larger limit.
pub async fn agent_results(
    data_dir: &Path,
    agent: &str,
    limit: usize,
) -> Result<AgentResultResponse> {
    let mut params = AgentResultRequest::new(agent);
    params.limit = limit;
    params.validate()?;
    let reply: AgentResultResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.result", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Filter for one conversation: its Agent and the kinds the view consumes,
/// so the feed never carries the tool/token traffic of the whole fleet.
pub fn chat_filter(agent: &str) -> EventsFilter {
    EventsFilter {
        agents: vec![agent.to_owned()],
        kinds: CHAT_KINDS.iter().map(|kind| (*kind).to_owned()).collect(),
        sources: Vec::new(),
    }
}

/// Retention floor and committed high-water: the snapshot cursor.
pub async fn events_bounds(data_dir: &Path, filter: &EventsFilter) -> Result<EventsBoundsResponse> {
    let params = EventsBoundsRequest {
        filter: filter.clone(),
    };
    params.validate()?;
    let reply: EventsBoundsResponse =
        eliot_daemon::client_call_typed(data_dir, "events.bounds", &params).await?;
    reply.validate()?;
    Ok(reply)
}

/// One validated `events.read` page strictly after `after`.
pub async fn events_read(
    data_dir: &Path,
    after: i64,
    limit: usize,
    filter: &EventsFilter,
) -> Result<EventsReadResponse> {
    let params = EventsReadRequest {
        after,
        limit,
        filter: filter.clone(),
    };
    params.validate()?;
    let reply: EventsReadResponse =
        eliot_daemon::client_call_typed(data_dir, "events.read", &params).await?;
    reply.validate(after)?;
    Ok(reply)
}

/// Long-poll wake: limit 1 so the call returns on the first matching event
/// instead of holding until the deadline; `wait_ms` is always explicit.
pub async fn events_wake(
    data_dir: &Path,
    after: i64,
    filter: &EventsFilter,
) -> Result<EventsSubscribeResponse> {
    let params = EventsSubscribeRequest {
        after,
        limit: 1,
        wait_ms: WAKE_WAIT_MS,
        filter: filter.clone(),
    };
    params.validate()?;
    let reply: EventsSubscribeResponse =
        eliot_daemon::client_call_typed(data_dir, "events.subscribe", &params).await?;
    reply.validate(after)?;
    Ok(reply)
}

/// Scan the journal window `(from, through]` for the filter. Events above
/// `through` are dropped: live delivery continues from `through` and will
/// carry them, so nothing is applied twice or skipped.
pub async fn scan_events(
    data_dir: &Path,
    from: i64,
    through: i64,
    filter: &EventsFilter,
) -> Result<Vec<eliot_core::Event>> {
    let mut after = from;
    let mut events = Vec::new();
    while after < through {
        let page = events_read(data_dir, after, EVENT_SCAN_ROWS, filter).await?;
        events.extend(page.events.into_iter().filter(|event| event.seq <= through));
        if page.next <= after || !page.truncated {
            break;
        }
        after = page.next;
    }
    Ok(events)
}

/// Exclusive journal cursor just before the first retained event committed
/// at or after `at_ms`, by binary search over single-row unfiltered reads
/// (about log2 of the journal length). Commit timestamps follow sequence
/// order closely; callers keep a margin below the result.
pub async fn seq_before(data_dir: &Path, at_ms: i64, floor: i64, high: i64) -> Result<i64> {
    let everything = EventsFilter::default();
    let (mut low, mut high) = (floor, high);
    while low < high {
        let mid = low + (high - low) / 2;
        let page = events_read(data_dir, mid, 1, &everything).await?;
        match page.events.first() {
            Some(event) if event.ts < at_ms => low = event.seq.max(mid + 1),
            Some(_) => high = mid,
            None => high = mid,
        }
    }
    Ok(low)
}

/// Input texts of retained commands, at most `MAX_CONCURRENT_READS` at once.
/// A failed read is reported per command, never retried as a new action.
pub async fn input_texts(data_dir: &Path, commands: Vec<String>) -> Vec<(String, InputText)> {
    futures_util::stream::iter(commands)
        .map(|command| async move {
            let text = match read_command(data_dir, &command).await {
                Ok(read) => match read.args.get("text").and_then(Value::as_str) {
                    Some(text) => InputText::Loaded(bound_text(text)),
                    None => InputText::Unavailable("the command carries no text".to_owned()),
                },
                Err(error) => InputText::Unavailable(error.to_string()),
            };
            (command, text)
        })
        .buffer_unordered(MAX_CONCURRENT_READS)
        .collect()
        .await
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

/// One retained command with its admitted args and state.
pub async fn read_command(data_dir: &Path, id: &str) -> Result<Command> {
    let params = CommandGetRequest::new(id);
    params.validate()?;
    let command: Command =
        eliot_daemon::client_call_typed(data_dir, "command.get", &params).await?;
    params.validate_response(&command)?;
    Ok(command)
}

/// What a launch freezes: a stored profile, or the Owner ad-hoc snapshot
/// of one harness with optional model/effort (absent = native default).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchTarget {
    Profile(String),
    Adhoc {
        harness: String,
        model: Option<String>,
        effort: Option<String>,
    },
}

/// First prompt as one submission: nonblank text rides the launch admission
/// and the linked `<id>/send` command must appear in the reply. Overrides
/// exist only on the ad-hoc path, which the daemon validates; a profile
/// launch freezes the profile as configured.
pub async fn submit_launch(
    data_dir: &Path,
    mutation_id: String,
    target: LaunchTarget,
    workspace: &str,
    text: Option<String>,
) -> Result<CommandSubmitResponse> {
    if let Some(text) = text.as_deref() {
        check_text(text)?;
    }
    let mut args = serde_json::json!({ "workspace": workspace });
    match target {
        LaunchTarget::Profile(profile) if !profile.trim().is_empty() => {
            args["profile"] = Value::String(profile);
        }
        LaunchTarget::Profile(_) => {
            return Err(Error::new(
                "invalid",
                "a profile is required for a profile launch",
            ));
        }
        LaunchTarget::Adhoc {
            harness,
            model,
            effort,
        } => {
            args["harness"] = Value::String(harness);
            if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
                args["model"] = Value::String(model);
            }
            if let Some(effort) = effort.filter(|effort| !effort.trim().is_empty()) {
                args["effort"] = Value::String(effort);
            }
        }
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

/// Mid-run model/effort overlay for the next turn of exactly the generation
/// shown at click time; the daemon refuses harnesses without a verified
/// per-turn mechanism.
pub async fn submit_configure(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
    model: Option<String>,
    effort: Option<String>,
) -> Result<CommandSubmitResponse> {
    let mut args = serde_json::Map::new();
    if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
        args.insert("model".to_owned(), Value::String(model));
    }
    if let Some(effort) = effort.filter(|effort| !effort.trim().is_empty()) {
        args.insert("effort".to_owned(), Value::String(effort));
    }
    if args.is_empty() {
        return Err(Error::new(
            "invalid",
            "configure needs a model or an effort",
        ));
    }
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "configure",
        Value::Object(args),
    )?;
    request.expected_generation = Some(generation);
    submit(data_dir, &request).await
}

/// Sanitized configured-profile summaries for the launch picker.
pub async fn list_profiles(data_dir: &Path) -> Result<ProfileListResponse> {
    let params = ProfileListRequest {};
    params.validate()?;
    let reply: ProfileListResponse =
        eliot_daemon::client_call_typed(data_dir, "profile.list", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// The `[profiles.*]` source text and the file revision.
pub async fn read_profiles_source(data_dir: &Path) -> Result<ProfilesReadResponse> {
    let params = ProfilesReadRequest {};
    params.validate()?;
    let reply: ProfilesReadResponse =
        eliot_daemon::client_call_typed(data_dir, "config.profiles.read", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Validate (dry run) or save the profile draft from `expected_revision`.
pub async fn save_profiles(
    data_dir: &Path,
    expected_revision: Option<String>,
    source: String,
    dry_run: bool,
) -> Result<ProfilesSaveResponse> {
    let params = ProfilesSaveRequest {
        expected_revision,
        source,
        dry_run,
    };
    params.validate()?;
    let reply: ProfilesSaveResponse =
        eliot_daemon::client_call_typed(data_dir, "config.profiles.save", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Rules of one namespace (global when `project` is absent), paging until
/// an empty page, at most `RULE_PAGES` pages; `true` when more remain.
pub async fn list_rules(
    data_dir: &Path,
    project: Option<String>,
) -> Result<(Vec<RuleListEntry>, bool)> {
    const RULE_PAGES: usize = 5;
    let mut rules = Vec::new();
    let mut after = None;
    for _ in 0..RULE_PAGES {
        let params = RuleListRequest {
            project: project.clone(),
            after: after.clone(),
            limit: 200,
        };
        params.validate()?;
        let reply: RuleListResponse =
            eliot_daemon::client_call_typed(data_dir, "rule.list", &params).await?;
        params.validate_response(&reply)?;
        if reply.rules.is_empty() {
            return Ok((rules, false));
        }
        after = reply.next.clone();
        rules.extend(reply.rules);
        if after.is_none() {
            return Ok((rules, false));
        }
    }
    Ok((rules, true))
}

/// Existing script entries under the approved Run roots.
pub async fn list_scripts(data_dir: &Path) -> Result<ScriptListResponse> {
    let mut params = ScriptListRequest::default();
    let mut roots: Vec<eliot_core::api::ScriptRootView> = Vec::new();
    loop {
        params.validate()?;
        let reply: ScriptListResponse =
            eliot_daemon::client_call_typed(data_dir, "script.list", &params).await?;
        params.validate_response(&reply)?;
        for root in reply.roots {
            if let Some(existing) = roots.iter_mut().find(|r| r.project == root.project) {
                existing.entries.extend(root.entries);
                existing.truncated |= root.truncated;
            } else {
                roots.push(root);
            }
        }
        match reply.next_cursor {
            Some(next) => params.cursor = Some(next),
            None => break,
        }
    }
    Ok(ScriptListResponse {
        roots,
        next_cursor: None,
    })
}

/// One script entry's text and revision.
pub async fn read_script(
    data_dir: &Path,
    project: Option<String>,
    path: String,
) -> Result<ScriptReadResponse> {
    let params = ScriptReadRequest { project, path };
    params.validate()?;
    let reply: ScriptReadResponse =
        eliot_daemon::client_call_typed(data_dir, "script.read", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Validate (dry run) or save one script entry from `expected_revision`.
pub async fn save_script(
    data_dir: &Path,
    project: Option<String>,
    path: String,
    expected_revision: String,
    content: String,
    dry_run: bool,
) -> Result<ScriptSaveResponse> {
    let params = ScriptSaveRequest {
        project,
        path,
        expected_revision,
        content,
        dry_run,
    };
    params.validate()?;
    let reply: ScriptSaveResponse =
        eliot_daemon::client_call_typed(data_dir, "script.save", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Run one explicit occurrence of a timer (the existing Owner Run path).
pub async fn run_timer(
    data_dir: &Path,
    mutation_id: String,
    project: Option<String>,
    rule: &str,
) -> Result<Command> {
    let request = TimerRunRequest::new(mutation_id, rule);
    let request = match project {
        Some(project) => request.in_project(project),
        None => request,
    };
    request.validate()?;
    let reply: Command = eliot_daemon::client_call_typed(data_dir, "timer.run", &request).await?;
    request.validate_response(&reply)?;
    Ok(reply)
}

/// The first page of a command's retained children.
pub async fn command_children(data_dir: &Path, id: &str) -> Result<Vec<Command>> {
    let params = CommandChildrenRequest::new(id);
    params.validate()?;
    let reply: CommandChildrenResponse =
        eliot_daemon::client_call_typed(data_dir, "command.children", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply.children)
}

/// The harness catalogue with its default model/effort and `configure`
/// support: under ad-hoc defaults, or under one Agent's frozen route.
pub async fn harness_models(
    data_dir: &Path,
    harness: &str,
    agent: Option<&str>,
) -> Result<HarnessModelsResponse> {
    let params = HarnessModelsRequest {
        harness: harness.to_owned(),
        profile: None,
        agent: agent.map(str::to_owned),
    };
    params.validate()?;
    let reply: HarnessModelsResponse =
        eliot_daemon::client_call_typed(data_dir, "harness.models", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
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

/// Interrupt exactly the turn and generation captured at click time; the
/// binding is never re-read, so a later turn cannot be interrupted instead.
pub async fn submit_interrupt(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
    turn_id: String,
) -> Result<CommandSubmitResponse> {
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

/// Bounded usage totals with explicit coverage for one Agent.
pub async fn usage_summary(data_dir: &Path, agent: &str) -> Result<UsageSummaryResponse> {
    let params = UsageSummaryRequest {
        agent: Some(agent.to_owned()),
        generation: None,
        project: None,
        root: None,
    };
    params.validate()?;
    let reply: UsageSummaryResponse =
        eliot_daemon::client_call_typed(data_dir, "usage.summary", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Lifecycle stop of exactly the generation observed at click time. This is
/// the explicit Agent action, never a substitute for interrupting a turn.
pub async fn submit_stop(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
    force: bool,
) -> Result<CommandSubmitResponse> {
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "stop",
        serde_json::json!({ "force": force }),
    )?;
    request.expected_generation = Some(generation);
    submit(data_dir, &request).await
}
