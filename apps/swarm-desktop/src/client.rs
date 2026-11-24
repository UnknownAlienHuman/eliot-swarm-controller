//! Typed RPC caller for the desktop client.
//!
//! Every call goes through the existing authenticated daemon client
//! (`eliot_daemon::client_call_typed` and the `eliot_core::api` DTOs) and runs
//! the same request/response validators the CLI runs; deserialization alone
//! is not validation. There is no second executor, no UI-owned database and
//! no new endpoint. Configuration reads and writes (profiles, projects,
//! rules, scripts) live in [`crate::config_client`], the Settings page's one
//! client module.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Duration,
};

use eliot_core::api::{
    AgentArchiveRequest, AgentDeleteRequest, AgentGetRequest, AgentGetResponse, AgentListRequest,
    AgentListResponse, AgentResultRequest, AgentResultResponse, AgentTreeResponse, ArchivedFilter,
    ChatAttachRequest, ChatDetachRequest, ChatOrganizationMutationResponse,
    ChatOrganizationRequest, ChatOrganizationResponse, ChatTagRequest, CleanupApplyRequest,
    CleanupApplyResponse, CleanupJobState, CleanupJobStatus, CleanupPlan, CleanupPlanRequest,
    CleanupStatusRequest, CommandCancelRequest, CommandCancelResponse, CommandChildrenRequest,
    CommandChildrenResponse, CommandGetRequest, CommandResolution, CommandResolveRequest,
    CommandResolveResponse, CommandSubmitRequest, CommandSubmitResponse, DaemonStatusRequest,
    DaemonStatusResponse, DiagnoseRequest, DiagnoseResponse, EventsBoundsRequest,
    EventsBoundsResponse, EventsFilter, EventsReadRequest, EventsReadResponse,
    EventsSubscribeRequest, EventsSubscribeResponse, GenerationBoundary, HandoffRequest,
    HandoffResponse, HarnessModelsRequest, HarnessModelsResponse, LaunchIntentGetRequest,
    LaunchIntentView, LaunchQueueListRequest, LaunchQueueRequest, LaunchQueueView,
    ORGANIZATION_CHANGED_KIND, OrgGroupView, OrgRowChange, OrganizationChangedEvent,
    ReparentRequest, ReparentResponse, SwitchRequest, SwitchResponse, UsageSummaryRequest,
    UsageSummaryResponse,
    auth_login::{
        AuthDesktopClaimRequest, AuthDesktopClaimResponse, AuthDesktopPollRequest,
        AuthDesktopPollResponse, AuthDesktopReportRequest, AuthDesktopReportResponse,
        AuthLoginRequest, AuthLoginResponse,
    },
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
const CLEANUP_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Resolve the daemon data directory with the same environment, saved-selection,
/// and platform-default precedence as the CLI.
pub fn data_dir() -> Result<PathBuf> {
    eliot_daemon::resolve_data_dir(None)
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

/// One validated `agent.list` page. The response passes the canonical
/// `validate_response` (row filters, cursor, totals), not only row checks.
pub async fn list_agents(data_dir: &Path, params: &AgentListRequest) -> Result<AgentListResponse> {
    params.validate()?;
    let reply: AgentListResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.list", params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Per-row chat and archived flags of one `agent.list` page.
pub type ListFlags = std::collections::HashMap<String, (bool, bool)>;

/// One validated `agent.list` page plus each row's `chat` and `archived`
/// flags, under the `archived` filter (`exclude` default, `include`, `only`).
pub async fn list_agents_flagged(
    data_dir: &Path,
    params: &AgentListRequest,
    archived: Option<&str>,
) -> Result<(AgentListResponse, ListFlags)> {
    let mut params = params.clone();
    params.archived = match archived {
        Some("only") => ArchivedFilter::Only,
        Some("include") => ArchivedFilter::Include,
        _ => ArchivedFilter::Exclude,
    };
    let reply = list_agents(data_dir, &params).await?;
    let flags = reply
        .agents
        .iter()
        .map(|row| {
            (
                row.summary.id.clone(),
                (row.summary.chat, row.summary.archived),
            )
        })
        .collect();
    Ok((reply, flags))
}

/// The `agent.list` error, worded for the sidebar's Show archived toggle.
pub fn archived_filter_error(error: &Error) -> String {
    error.user_message()
}

/// `agent.archive` (Owner only): hide or show an idle or ended Agent tree
/// in the default list (archiving an ended one needs its custody resolved);
/// durable, idempotent by mutation id. Returns the number of Agents it
/// changed.
pub async fn archive_agent(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    archived: bool,
) -> Result<usize> {
    let request = AgentArchiveRequest {
        id: mutation_id,
        agent: agent.to_owned(),
        archived,
    };
    request.validate()?;
    let reply: AgentTreeResponse = typed_call(data_dir, "agent.archive", &request).await?;
    Ok(reply.affected)
}

/// `agent.delete` cleans up eligible task worktrees and native chats before
/// removing the stopped or failed tree's records.
pub async fn delete_agent(data_dir: &Path, mutation_id: String, agent: &str) -> Result<usize> {
    let request = AgentDeleteRequest {
        id: mutation_id,
        agent: agent.to_owned(),
    };
    request.validate()?;
    let reply: AgentTreeResponse = typed_call(data_dir, "agent.delete", &request).await?;
    Ok(reply.affected)
}

/// Follow progress pages until there is a review the caller can show or use.
pub async fn cleanup_plan_page(
    data_dir: &Path,
    mut request: CleanupPlanRequest,
    accept: impl Fn(&CleanupPlan) -> bool,
) -> Result<CleanupPlan> {
    let mut cursors: HashSet<_> = request.continuation.iter().cloned().collect();
    loop {
        let page: CleanupPlan =
            eliot_daemon::client_call_typed(data_dir, "cleanup.plan", &request).await?;
        let Some(next) = page.continuation.as_ref().filter(|_| !accept(&page)) else {
            return Ok(page);
        };
        if !cursors.insert(next.clone()) {
            return Err(Error::new(
                "source_integrity",
                "Cleanup review did not advance",
            ));
        }
        request.continuation = Some(next.clone());
    }
}

/// Apply one reviewed cleanup page and, when the daemon reports an async job,
/// wait for its terminal status under the original mutation ID.
pub async fn cleanup_apply_and_wait(
    data_dir: &Path,
    request: &CleanupApplyRequest,
) -> Result<(CleanupApplyResponse, Option<String>)> {
    let initial: CleanupApplyResponse =
        eliot_daemon::client_call_typed(data_dir, "cleanup.apply", request).await?;
    let Some(initial_state) = initial.state.as_ref() else {
        // An older daemon returned only the settled response fields.
        return Ok((initial, None));
    };

    let status_request = CleanupStatusRequest {
        id: request.id.clone(),
    };
    let mut wait = matches!(initial_state, CleanupJobState::Running);
    loop {
        if wait {
            tokio::time::sleep_until(tokio::time::Instant::now() + CLEANUP_STATUS_POLL_INTERVAL)
                .await;
        }
        let status: CleanupJobStatus =
            eliot_daemon::client_call_typed(data_dir, "cleanup.status", &status_request)
                .await
                .map_err(|error| {
                    Error::new(
                        "unknown",
                        format!("cleanup status unavailable: {}", error.user_message()),
                    )
                })?;
        if status.id != request.id {
            return Err(Error::new(
                "source_integrity",
                "cleanup status returned a different job ID",
            ));
        }
        if status.state == CleanupJobState::Running {
            wait = true;
            continue;
        }
        let mut apply = status
            .apply
            .filter(|apply| apply.plan_id == request.plan_id)
            .ok_or_else(|| {
                Error::new(
                    "unknown",
                    "terminal cleanup job has no matching plan result",
                )
            })?;
        apply.state = Some(status.state);
        let notice = match status.state {
            CleanupJobState::Failed => Some("Cleanup failed"),
            CleanupJobState::Interrupted => Some("Cleanup interrupted"),
            _ => None,
        };
        return Ok((
            apply,
            notice.map(|notice| match status.reason {
                Some(reason) => format!("{notice} ({reason})"),
                None => notice.to_owned(),
            }),
        ));
    }
}

/// A validated daemon status snapshot for the Worktrees cleanup surface.
pub async fn daemon_status(data_dir: &Path) -> Result<DaemonStatusResponse> {
    let request = DaemonStatusRequest::default();
    request.validate()?;
    let response = eliot_daemon::client_call_typed(data_dir, "daemon.status", &request).await?;
    request.validate_response(&response)?;
    Ok(response)
}

async fn typed_call<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
    data_dir: &Path,
    method: &str,
    request: &Req,
) -> Result<Resp> {
    let params = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", format!("{method} request could not be encoded")))?;
    let raw = eliot_daemon::client_call(data_dir, method, params).await?;
    serde_json::from_value(raw)
        .map_err(|_| Error::new("unknown", format!("{method} returned a malformed result")))
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

/// Owner-scoped failure diagnosis for one command, Agent or agent:turn.
/// The desktop shows the one-line summary in Agent details; the full chain
/// lives behind `swarm why`.
pub async fn diagnose(data_dir: &Path, target: &str) -> Result<DiagnoseResponse> {
    let params = DiagnoseRequest {
        target: target.to_owned(),
    };
    params.validate()?;
    let reply: DiagnoseResponse =
        eliot_daemon::client_call_typed(data_dir, "diagnose", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Why a submitted input did not start, or `None` while its command is
/// still pending or went through. A rejected, failed or cancelled command
/// is definite (Retry may submit again); `unknown` is not, and neither is a
/// pending input of an Agent that has ended. A cancelled input fell with
/// its Agent's start, so the Agent's `why` explains it.
pub async fn input_failure(
    data_dir: &Path,
    agent: &str,
    command: &str,
    agent_ended: bool,
) -> Result<Option<crate::chat::InputFailure>> {
    use eliot_core::CommandState;
    let read = read_command(data_dir, command).await?;
    let (definite, target) = match read.state {
        CommandState::Rejected | CommandState::Failed => (true, command),
        CommandState::Cancelled => (true, agent),
        CommandState::Unknown => (false, command),
        CommandState::Accepted | CommandState::Queued if agent_ended => (false, agent),
        _ => return Ok(None),
    };
    let field = |name: &str| {
        read.outcome
            .as_ref()
            .and_then(|outcome| outcome.get(name))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    let mut failure = crate::chat::InputFailure {
        code: field("reason")
            .or_else(|| field("code"))
            .unwrap_or_else(|| {
                match read.state {
                    CommandState::Rejected => "rejected",
                    CommandState::Failed => "failed",
                    CommandState::Cancelled => "cancelled",
                    _ => "unknown",
                }
                .to_owned()
            }),
        message: field("message").unwrap_or_default(),
        origin: field("origin").unwrap_or_else(|| "unknown".to_owned()),
        definite,
    };
    if let Ok(reply) = diagnose(data_dir, target).await
        && let Some(step) = eliot_core::api::diagnosis_reason(&reply.steps)
    {
        if let Some(code) = &step.code {
            failure.code.clone_from(code);
        }
        if let Some(message) = &step.message {
            failure.message.clone_from(message);
        }
        if let Some(origin) = &step.origin {
            failure.origin.clone_from(origin);
        }
    }
    Ok(Some(failure))
}

#[derive(Clone, Debug)]
pub struct TranscriptResults {
    pub generation: i64,
    pub turns: Vec<eliot_core::Turn>,
    pub has_older: bool,
    /// Where each generation in the window came from (`generation_boundaries`
    /// of every page read).
    pub boundaries: Vec<GenerationBoundary>,
}

/// Read a bounded window of the Agent's retained turns across generations.
pub async fn agent_results(
    data_dir: &Path,
    agent: &str,
    limit: usize,
) -> Result<TranscriptResults> {
    let mut params = AgentResultRequest::new(agent);
    params.limit = limit;
    let first = agent_result_page(data_dir, &params).await?;
    let mut turns = first.turns;
    let mut next = first.next;
    let mut boundaries = first.generation_boundaries;
    while turns.len() < limit {
        let Some(before) = next.take() else { break };
        params.before = Some(before);
        params.limit = limit - turns.len();
        let page = agent_result_page(data_dir, &params).await?;
        if page.generation != first.generation || (page.turns.is_empty() && page.next.is_some()) {
            return Err(Error::new("unknown", "result window changed while paging"));
        }
        turns.extend(page.turns);
        boundaries.extend(page.generation_boundaries);
        next = page.next;
    }
    Ok(TranscriptResults {
        generation: first.generation,
        turns,
        has_older: next.is_some(),
        boundaries,
    })
}

async fn agent_result_page(
    data_dir: &Path,
    params: &AgentResultRequest,
) -> Result<AgentResultResponse> {
    params.validate()?;
    let page = eliot_daemon::client_call_typed(data_dir, "agent.result", params).await?;
    params.validate_response(&page)?;
    Ok(page)
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

/// Read `(from, through]` for the filter to the end. Events above `through`
/// are dropped: live delivery continues from `through` and will carry them,
/// so nothing is applied twice or skipped. An agent filter keeps the work to
/// that Agent's own rows (the daemon reads them through its agent index).
pub async fn scan_events(
    data_dir: &Path,
    from: i64,
    through: i64,
    filter: &EventsFilter,
) -> Result<Vec<eliot_core::Event>> {
    let scan = scan_pages(from, through, usize::MAX, &|| false, |after| {
        events_read(data_dir, after, EVENT_SCAN_ROWS, filter)
    })
    .await?;
    Ok(scan.events)
}

/// What a budgeted scan read and where it stopped.
#[derive(Debug)]
pub struct EventScan {
    pub events: Vec<eliot_core::Event>,
    /// The cursor to resume from: `through` once the window is complete,
    /// otherwise the last page's `next`.
    pub next: i64,
}

/// Scan `(from, through]` with at most `max_pages` `events.read` calls
/// (each returns at most `EVENT_SCAN_ROWS` rows), checking `cancelled`
/// before every call. A scan that runs out of budget or is cancelled stops
/// at the last page read; the caller resumes from `next`.
pub async fn scan_events_budgeted(
    data_dir: &Path,
    from: i64,
    through: i64,
    filter: &EventsFilter,
    max_pages: usize,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<EventScan> {
    scan_pages(from, through, max_pages, cancelled, |after| {
        events_read(data_dir, after, EVENT_SCAN_ROWS, filter)
    })
    .await
}

async fn scan_pages<F, Fut>(
    from: i64,
    through: i64,
    max_pages: usize,
    cancelled: &(dyn Fn() -> bool + Sync),
    mut read: F,
) -> Result<EventScan>
where
    F: FnMut(i64) -> Fut,
    Fut: std::future::Future<Output = Result<EventsReadResponse>>,
{
    let mut after = from;
    let mut events = Vec::new();
    let mut pages = 0;
    while after < through && pages < max_pages && !cancelled() {
        let page = read(after).await?;
        pages += 1;
        events.extend(page.events.into_iter().filter(|event| event.seq <= through));
        if page.next <= after || !page.truncated {
            after = through;
            break;
        }
        after = page.next;
    }
    Ok(EventScan {
        events,
        next: after.min(through),
    })
}

/// Exclusive journal cursor just before the first retained event committed
/// at or after `at_ms`, by binary search over single-row unfiltered reads
/// (about log2 of the journal length). Commit timestamps follow sequence
/// order closely; callers keep a margin below the result.
pub async fn seq_before(
    data_dir: &Path,
    at_ms: i64,
    floor: i64,
    high: i64,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<i64> {
    let everything = EventsFilter::default();
    let (mut low, mut high) = (floor, high);
    while low < high && !cancelled() {
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
/// An inline mail notice reads back redacted (`command.get`), so its mails
/// come from `agent`'s own `deliveries` (read-only `mail.read`): those whose
/// `presentation_command` is that notice.
pub async fn input_texts(
    data_dir: &Path,
    agent: &str,
    commands: Vec<String>,
    deliveries: &[String],
) -> Vec<(String, InputText)> {
    let reads: Vec<(String, Result<Command>)> = futures_util::stream::iter(commands)
        .map(|command| async move {
            let read = read_command(data_dir, &command).await;
            (command, read)
        })
        .buffer_unordered(MAX_CONCURRENT_READS)
        .collect()
        .await;
    let redacted = reads
        .iter()
        .any(|(_, read)| read.as_ref().is_ok_and(redacted_mail));
    let mail: Vec<eliot_core::api::MailDelivery> = if redacted {
        futures_util::stream::iter(deliveries)
            .map(|delivery| read_mail(data_dir, agent, delivery))
            .buffer_unordered(MAX_CONCURRENT_READS)
            .filter_map(|read| async move { read.ok() })
            .collect()
            .await
    } else {
        Vec::new()
    };
    reads
        .into_iter()
        .map(|(command, read)| {
            let text = match read {
                Ok(read) if redacted_mail(&read) => {
                    InputText::Inserted(crate::chat::presented_mail(&mail, &read.id))
                }
                Ok(read) => input_text(&read),
                Err(error) => InputText::Unavailable(error.to_string()),
            };
            (command, text)
        })
        .collect()
}

/// A mail notice whose text the daemon redacts on read (`redacted: true`).
fn redacted_mail(read: &Command) -> bool {
    read.args.get("notice_kind").and_then(Value::as_str) == Some("mail")
        && read.args.get("redacted").and_then(Value::as_bool) == Some(true)
}

/// One delivery of `agent`'s mailbox (`mail.read`, read-only; the Owner
/// names the mailbox).
pub async fn read_mail(
    data_dir: &Path,
    agent: &str,
    delivery: &str,
) -> Result<eliot_core::api::MailDelivery> {
    let request = eliot_core::api::MailReadRequest {
        agent: Some(agent.to_owned()),
        delivery_id: delivery.to_owned(),
    };
    request.validate()?;
    let read: eliot_core::api::MailDelivery =
        eliot_daemon::client_call_typed(data_dir, "mail.read", &request).await?;
    request.validate_response(&read)?;
    Ok(read)
}

/// What a retained command put in front of the model: a Swarm note
/// (`inject`), Swarm's mail notice (`send` with `notice_kind = "mail"`) or
/// wake delivery, both split into Swarm notes and owner mail, else the
/// owner's own text.
fn input_text(read: &Command) -> InputText {
    let Some(text) = read.args.get("text").and_then(Value::as_str) else {
        return InputText::Unavailable("the command carries no text".to_owned());
    };
    let text = bound_text(text);
    if read.verb == "inject" {
        return InputText::Note {
            principal: read.principal.clone(),
            text,
            turn: read.turn_id.clone(),
        };
    }
    if read.args.get("notice_kind").and_then(Value::as_str) == Some("mail") {
        return InputText::Inserted(crate::chat::mail_blocks(&text));
    }
    // A wake delivery is its exact frame line and a quoted-data fence.
    if read.principal == "owner"
        && let Some(fenced) = text.strip_prefix(crate::chat::WAKE_FRAME)
        && crate::chat::unfence(fenced) != fenced
    {
        return InputText::Inserted(vec![crate::chat::framed(&text)]);
    }
    if read.principal == eliot_core::BACKGROUND_WAKE_PRINCIPAL
        && let Some((_, rest)) = text.split_once('\n')
        && crate::chat::unfence(rest) != rest
    {
        return InputText::Inserted(vec![crate::chat::framed(&text)]);
    }
    InputText::Loaded(text)
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
    request.validate_response(&reply).map_err(unverified)?;
    Ok(reply)
}

/// A mutation reply that arrived but fails its binding check proves
/// nothing about what the daemon did: report it as an unknown outcome, so
/// the action keeps its mutation id and a retry reads back instead of
/// acting again.
fn unverified(error: Error) -> Error {
    Error::new(
        "unknown",
        format!("the daemon replied, but the reply failed its binding check ({error})"),
    )
}

/// One retained command with its admitted args and state.
/// Swarm autonomy (`autonomy.get`, Owner only): the daemon's last 24 hours
/// when `since_ms` is `None`, else from `since_ms` to now.
pub async fn autonomy(
    data_dir: &Path,
    since_ms: Option<i64>,
) -> Result<eliot_core::api::AutonomyResponse> {
    let request = eliot_core::api::AutonomyRequest { since_ms };
    eliot_daemon::client_call_typed(data_dir, "autonomy.get", &request).await
}

/// One page of the Owner decision queue (`owner.inbox`).
pub async fn owner_inbox(
    data_dir: &Path,
    after: Option<String>,
) -> Result<eliot_core::api::OwnerInboxResponse> {
    let request = eliot_core::api::OwnerInboxRequest {
        after,
        limit: Some(eliot_core::api::OwnerInboxRequest::MAX_ROWS),
    };
    request.effective_limit()?;
    eliot_daemon::client_call_typed(data_dir, "owner.inbox", &request).await
}

#[derive(Clone, Debug)]
pub struct SidebarStatus {
    pub owner_inbox: std::collections::BTreeMap<eliot_core::api::OwnerInboxKind, usize>,
    pub profile_command_issues: Vec<eliot_core::api::ProfileCommandIssue>,
}

pub async fn sidebar_status(data_dir: &Path) -> Result<SidebarStatus> {
    let request = eliot_core::api::DaemonStatusRequest::default();
    request.validate()?;
    let status: eliot_core::api::DaemonStatusResponse =
        eliot_daemon::client_call_typed(data_dir, "daemon.status", &request).await?;
    request.validate_response(&status)?;
    Ok(SidebarStatus {
        owner_inbox: status.owner_inbox,
        profile_command_issues: status.profile_command_issues,
    })
}

/// Decide one inbox item (`owner.decide`): an offered choice, `recommended`
/// or `snooze`, bound to the revision shown. The caller keeps `id` and
/// sends it again on retry; a stale item comes back as `stale_item`.
pub async fn owner_decide(
    data_dir: &Path,
    request: &eliot_core::api::OwnerDecideRequest,
) -> Result<eliot_core::api::OwnerDecideResponse> {
    request.validate()?;
    eliot_daemon::client_call_typed(data_dir, "owner.decide", request).await
}

/// Queue native sign-in for one harness, optionally bound to the inbox
/// revision shown by the desktop. This is a direct Owner action, not an
/// `owner.decide` choice.
pub async fn auth_login(
    data_dir: &Path,
    harness: &str,
    revision: Option<String>,
) -> Result<AuthLoginResponse> {
    let request = AuthLoginRequest {
        harness: harness.to_owned(),
        revision,
    };
    eliot_daemon::client_call_typed(data_dir, "auth.login", &request).await
}

/// Read native login work for the interactive Owner desktop.
pub async fn auth_desktop_poll(data_dir: &Path) -> Result<AuthDesktopPollResponse> {
    eliot_daemon::client_call_typed(data_dir, "auth.desktop.poll", &AuthDesktopPollRequest {}).await
}

/// Claim one exact native login task after the desktop is ready to launch it.
pub async fn auth_desktop_claim(
    data_dir: &Path,
    harness: &str,
    revision: &str,
) -> Result<AuthDesktopClaimResponse> {
    let request = AuthDesktopClaimRequest {
        harness: harness.to_owned(),
        revision: revision.to_owned(),
    };
    eliot_daemon::client_call_typed(data_dir, "auth.desktop.claim", &request).await
}

/// Report one native login probe or the outcome of its exact console attempt.
pub async fn auth_desktop_report(
    data_dir: &Path,
    request: &AuthDesktopReportRequest,
) -> Result<AuthDesktopReportResponse> {
    eliot_daemon::client_call_typed(data_dir, "auth.desktop.report", request).await
}

pub async fn read_command(data_dir: &Path, id: &str) -> Result<Command> {
    let params = CommandGetRequest::new(id);
    params.validate()?;
    let command: Command =
        eliot_daemon::client_call_typed(data_dir, "command.get", &params).await?;
    params.validate_response(&command)?;
    Ok(command)
}

/// Server-side wait until one retained command is terminal; its record, or
/// `None` when `deadline` passes first.
pub async fn wait_command(
    data_dir: &Path,
    id: &str,
    deadline: tokio::time::Instant,
) -> Result<Option<Command>> {
    eliot_daemon::wait_command(data_dir, &CommandGetRequest::new(id), deadline).await
}

/// Server-side wait (`swarm.wait`) until every listed command is terminal;
/// false when `deadline` passes first.
pub async fn wait_commands(
    data_dir: &Path,
    ids: Vec<String>,
    deadline: tokio::time::Instant,
) -> Result<bool> {
    eliot_daemon::wait_terminal(data_dir, ids, Vec::new(), deadline).await
}

/// What a launch freezes: a stored profile, or the Owner ad-hoc snapshot
/// of one harness with optional model/effort (absent = native default) and
/// access level (`ask`/`auto`/`full`; absent = the harness's safe defaults).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchTarget {
    /// No profile: the daemon's routing chooses at admission (role default).
    Routed,
    Profile(String),
    Adhoc {
        harness: String,
        model: Option<String>,
        effort: Option<String>,
        access: Option<String>,
    },
}

/// Where a new task runs: a project checkout or folder, or a chat without a
/// project (`chat: true` and no workspace: the daemon creates the chat's
/// private directory under `chats_root`, per the k92 launch contract).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchPlace {
    Workspace(String),
    /// The workspace, in a new worktree prepared for this task (`worktree`
    /// with an empty value: the daemon names the standard branch).
    Worktree(String),
    Chat,
}

/// First prompt as one submission: nonblank text rides the launch admission
/// and the linked `<id>/send` command must appear in the reply. Overrides
/// exist only on the ad-hoc path, which the daemon validates; a profile
/// launch freezes the profile as configured.
pub async fn submit_launch(
    data_dir: &Path,
    mutation_id: String,
    target: LaunchTarget,
    place: &LaunchPlace,
    text: Option<String>,
) -> Result<CommandSubmitResponse> {
    let args = launch_args(target, place, text)?;
    submit(
        data_dir,
        &submit_request(mutation_id, None, "launch", args)?,
    )
    .await
}

/// `launch.enqueue`: the same launch arguments as a durable intent the
/// daemon's fair arbiter admits when capacity frees (`docs/spec/api.md`,
/// Command admission). The intent has no Agent or command until admitted;
/// an exact retry with the same id reads the current intent back.
pub async fn enqueue_launch(
    data_dir: &Path,
    mutation_id: String,
    target: LaunchTarget,
    place: &LaunchPlace,
    text: Option<String>,
) -> Result<LaunchIntentView> {
    let request = LaunchQueueRequest {
        id: mutation_id,
        args: launch_args(target, place, text)?,
    };
    request.validate()?;
    let reply: LaunchIntentView =
        eliot_daemon::client_call_typed(data_dir, "launch.enqueue", &request).await?;
    if reply.id != request.id {
        return Err(unverified(Error::new(
            "unknown",
            "queued launch reply names another intent",
        )));
    }
    Ok(reply)
}

/// Most `chat.organization` group pages, `launch.queue` pages and
/// `organization.changed` event pages one sidebar refresh reads; the daemon
/// pages at 1,000 groups.
const ORGANIZATION_PAGES: usize = 8;

/// Every `chat.organization` group (projects first, the no-project bucket
/// last) with its header and exact totals, page by page, under the
/// sidebar's archived filter, and the lowest event watermark the pages
/// observed: one bounded read for the whole sidebar.
pub async fn chat_organization(
    data_dir: &Path,
    archived: ArchivedFilter,
) -> Result<(Vec<OrgGroupView>, i64)> {
    let mut groups = Vec::new();
    let mut watermark = i64::MAX;
    let mut after = None;
    for _ in 0..ORGANIZATION_PAGES {
        let request = ChatOrganizationRequest {
            project: None,
            after: after.take(),
            archived,
            // The headers count from the listed rows (k205), so a search
            // reads every group as it is.
            query: None,
        };
        request.validate()?;
        let reply: ChatOrganizationResponse =
            eliot_daemon::client_call_typed(data_dir, "chat.organization", &request).await?;
        request.validate_response(&reply)?;
        groups.extend(reply.groups);
        watermark = watermark.min(reply.event_watermark);
        if !reply.groups_truncated {
            return Ok((groups, watermark));
        }
        after = reply.next_after;
    }
    Err(Error::new(
        "unavailable",
        "more project groups than one sidebar read pages",
    ))
}

/// The `organization.changed` rows committed after `after`, page by page,
/// and the exclusive cursor to read on from: each names an Agent, the group
/// its row now reads in, its tags and link revision, so loaded rows refresh
/// by id.
pub async fn organization_changes(data_dir: &Path, after: i64) -> Result<(Vec<OrgRowChange>, i64)> {
    let filter = EventsFilter {
        kinds: vec![ORGANIZATION_CHANGED_KIND.to_owned()],
        ..EventsFilter::default()
    };
    let mut rows = Vec::new();
    let mut cursor = after;
    for _ in 0..ORGANIZATION_PAGES {
        let page = events_read(data_dir, cursor, EVENT_SCAN_ROWS, &filter).await?;
        for event in page.events {
            let change: OrganizationChangedEvent = serde_json::from_value(event.data)
                .map_err(|_| Error::new("unknown", "organization change event is malformed"))?;
            change.validate()?;
            rows.extend(change.rows);
        }
        let more = page.truncated && page.next > cursor;
        cursor = page.next;
        if !more {
            break;
        }
    }
    Ok((rows, cursor))
}

/// `launch.intent.get`: one queued launch intent's current state, also
/// after the arbiter admitted or refused it.
pub async fn launch_intent(data_dir: &Path, id: &str) -> Result<LaunchIntentView> {
    let request = LaunchIntentGetRequest { id: id.to_owned() };
    request.validate()?;
    let reply: LaunchIntentView =
        eliot_daemon::client_call_typed(data_dir, "launch.intent.get", &request).await?;
    if reply.id != request.id {
        return Err(Error::new(
            "unknown",
            "launch intent reply names another intent",
        ));
    }
    Ok(reply)
}

/// The whole `launch.queue`: queued interactive intents and automation
/// assignment waits, each with its wait reason, page by page. A watched
/// intent's reason is read even beyond the first queue page.
pub async fn launch_queue(data_dir: &Path) -> Result<LaunchQueueView> {
    let mut interactive = Vec::new();
    let mut assignments = Vec::new();
    let mut after = None;
    for _ in 0..ORGANIZATION_PAGES {
        let request = LaunchQueueListRequest {
            project: None,
            after: after.take(),
        };
        request.validate()?;
        let reply: LaunchQueueView =
            eliot_daemon::client_call_typed(data_dir, "launch.queue", &request).await?;
        interactive.extend(reply.interactive);
        assignments.extend(reply.assignments);
        match reply.next_after {
            Some(cursor) => after = Some(cursor),
            None => {
                return Ok(LaunchQueueView {
                    interactive,
                    assignments,
                    next_after: None,
                });
            }
        }
    }
    Ok(LaunchQueueView {
        interactive,
        assignments,
        next_after: after,
    })
}

/// One organization mutation: placement or tags, never authority. The
/// revision is the link revision the menu observed (0 for no overlay row);
/// the Store refuses a write whose revision moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Organize {
    Attach {
        agent: String,
        project: String,
        revision: i64,
    },
    Detach {
        agent: String,
        revision: i64,
    },
    Tag {
        agent: String,
        tags: Vec<String>,
        revision: i64,
    },
}

/// `chat.attach`, `chat.detach` or `chat.tag` for one Agent, with the
/// committed link read back and bound to the request.
pub async fn organize(
    data_dir: &Path,
    mutation_id: String,
    change: Organize,
) -> Result<ChatOrganizationMutationResponse> {
    match change {
        Organize::Attach {
            agent,
            project,
            revision,
        } => {
            let request = ChatAttachRequest {
                id: mutation_id,
                agents: vec![agent],
                project,
                expected_revision: Some(vec![revision]),
            };
            request.validate()?;
            let reply = eliot_daemon::client_call_typed(data_dir, "chat.attach", &request).await?;
            request.validate_response(&reply).map_err(unverified)?;
            Ok(reply)
        }
        Organize::Detach { agent, revision } => {
            let request = ChatDetachRequest {
                id: mutation_id,
                agents: vec![agent],
                expected_revision: Some(vec![revision]),
            };
            request.validate()?;
            let reply = eliot_daemon::client_call_typed(data_dir, "chat.detach", &request).await?;
            request.validate_response(&reply).map_err(unverified)?;
            Ok(reply)
        }
        Organize::Tag {
            agent,
            tags,
            revision,
        } => {
            let request = ChatTagRequest {
                id: mutation_id,
                agents: vec![agent],
                tags,
                expected_revision: Some(vec![revision]),
            };
            request.validate()?;
            let reply = eliot_daemon::client_call_typed(data_dir, "chat.tag", &request).await?;
            request.validate_response(&reply).map_err(unverified)?;
            Ok(reply)
        }
    }
}

/// The ordinary launch arguments of one launch or queued intent.
fn launch_args(target: LaunchTarget, place: &LaunchPlace, text: Option<String>) -> Result<Value> {
    if let Some(text) = text.as_deref() {
        check_text(text)?;
    }
    let mut args = match place {
        LaunchPlace::Workspace(workspace) => serde_json::json!({ "workspace": workspace }),
        LaunchPlace::Worktree(workspace) => {
            serde_json::json!({ "workspace": workspace, "worktree": "" })
        }
        LaunchPlace::Chat => serde_json::json!({ "chat": true }),
    };
    match target {
        LaunchTarget::Routed => {}
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
            access,
        } => {
            args["harness"] = Value::String(harness);
            if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
                args["model"] = Value::String(model);
            }
            if let Some(effort) = effort.filter(|effort| !effort.trim().is_empty()) {
                args["effort"] = Value::String(effort);
            }
            if let Some(access) = access.filter(|access| !access.trim().is_empty()) {
                args["access"] = Value::String(access);
            }
        }
    }
    if let Some(text) = text {
        args["text"] = Value::String(text);
    }
    Ok(args)
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

/// The first page of a command's retained children.
pub async fn command_children(data_dir: &Path, id: &str) -> Result<Vec<Command>> {
    let params = CommandChildrenRequest::new(id);
    params.validate()?;
    let reply: CommandChildrenResponse =
        eliot_daemon::client_call_typed(data_dir, "command.children", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply.children)
}

/// Most SENDs one handoff dialog reads for an Agent (newest first kept).
pub const MAX_LISTED_SENDS: usize = 200;

/// SENDs admitted for `agent`, oldest first, newest `MAX_LISTED_SENDS`,
/// read from its retained admission events and then `command.get`, plus how
/// many older ones were not read; the caller keeps the plain ones of its
/// bound generation.
pub async fn agent_sends(data_dir: &Path, agent: &str) -> Result<(Vec<Command>, usize)> {
    let filter = EventsFilter {
        agents: vec![agent.to_owned()],
        kinds: crate::handoff::ADMISSION_KINDS.map(str::to_owned).to_vec(),
        sources: Vec::new(),
    };
    let bounds = events_bounds(data_dir, &filter).await?;
    let events = scan_events(data_dir, bounds.pruned_through, bounds.hi, &filter).await?;
    let mut ids = crate::handoff::send_ids(&events, agent);
    let omitted = ids.len().saturating_sub(MAX_LISTED_SENDS);
    ids.drain(..omitted);
    let reads: Vec<Result<Command>> = futures_util::stream::iter(ids)
        .map(|id| async move { read_command(data_dir, &id).await })
        .buffered(MAX_CONCURRENT_READS)
        .collect()
        .await;
    Ok((reads.into_iter().collect::<Result<_>>()?, omitted))
}

/// Owner task transfer to an existing Agent. The same request (id
/// included) is also the readback: the daemon returns the committed result
/// and the current brief gate, never a second transfer.
pub async fn handoff(data_dir: &Path, request: &HandoffRequest) -> Result<HandoffResponse> {
    request.validate()?;
    let reply: HandoffResponse =
        eliot_daemon::client_call_typed(data_dir, "swarm.handoff", request).await?;
    request.validate_response(&reply).map_err(unverified)?;
    Ok(reply)
}

/// Owner launch-plus-handoff. The first reply is the reservation; the same
/// request again reads the components back and never launches another Agent.
pub async fn switch(data_dir: &Path, request: &SwitchRequest) -> Result<SwitchResponse> {
    request.validate()?;
    let reply: SwitchResponse =
        eliot_daemon::client_call_typed(data_dir, "swarm.switch", request).await?;
    request.validate_response(&reply).map_err(unverified)?;
    Ok(reply)
}

/// Owner assertion over a held handoff brief (`unknown`, or a failed brief
/// asserted applied). It records the owner as the source and resends nothing.
pub async fn resolve_command(
    data_dir: &Path,
    id: String,
    command: &str,
    resolution: CommandResolution,
    reason: String,
) -> Result<CommandResolveResponse> {
    let request = CommandResolveRequest {
        id,
        command_id: command.to_owned(),
        resolution,
        reason,
    };
    request.validate()?;
    let reply: CommandResolveResponse =
        eliot_daemon::client_call_typed(data_dir, "command.resolve", &request).await?;
    request.validate_response(&reply).map_err(unverified)?;
    Ok(reply)
}

/// Owner withdrawal of one accepted/queued handoff input (no effect).
pub async fn cancel_command(
    data_dir: &Path,
    id: String,
    command: &str,
) -> Result<CommandCancelResponse> {
    let request = CommandCancelRequest {
        id,
        command_id: command.to_owned(),
    };
    request.validate()?;
    let reply: CommandCancelResponse =
        eliot_daemon::client_call_typed(data_dir, "command.cancel", &request).await?;
    if reply.command_id != request.command_id {
        return Err(unverified(Error::new(
            "source_integrity",
            "cancel reply names another command",
        )));
    }
    Ok(reply)
}

/// Owner manager change bound to both exact generations; the daemon
/// validates topology, limits and state inside one Store transaction.
pub async fn reparent(data_dir: &Path, request: &ReparentRequest) -> Result<ReparentResponse> {
    request.validate()?;
    let reply: ReparentResponse =
        eliot_daemon::client_call_typed(data_dir, "swarm.reparent", request).await?;
    if reply.id != request.id {
        return Err(unverified(Error::new(
            "source_integrity",
            "reparent reply names another mutation",
        )));
    }
    Ok(reply)
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
        workspace: None,
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

/// Steer exactly the active turn and generation captured at click time with
/// `text`: supported steering of that turn, not a renamed SEND. A harness
/// without native steering refuses it (`unsupported`).
pub async fn submit_steer(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
    turn_id: String,
    text: &str,
) -> Result<CommandSubmitResponse> {
    check_text(text)?;
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "steer",
        serde_json::json!({ "text": text }),
    )?;
    request.expected_generation = Some(generation);
    request.expected_turn_id = Some(turn_id);
    submit(data_dir, &request).await
}

/// Resume the retained Agent at the generation captured at click time. With
/// nonblank `text` the daemon admits the linked `<id>/send` in the same
/// transaction; the prompt is never sent again after admission.
pub async fn submit_resume(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
    text: Option<&str>,
) -> Result<CommandSubmitResponse> {
    let mut args = serde_json::Map::new();
    if let Some(text) = text {
        check_text(text)?;
        args.insert("text".to_owned(), Value::String(text.to_owned()));
    }
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "resume",
        Value::Object(args),
    )?;
    request.expected_generation = Some(generation);
    submit(data_dir, &request).await
}

/// Detach an observed Agent at the generation captured at click time: the
/// observation ends; nobody's process is stopped.
pub async fn submit_detach(
    data_dir: &Path,
    mutation_id: String,
    agent: &str,
    generation: i64,
) -> Result<CommandSubmitResponse> {
    let mut request = submit_request(
        mutation_id,
        Some(agent.to_owned()),
        "detach",
        serde_json::json!({}),
    )?;
    request.expected_generation = Some(generation);
    submit(data_dir, &request).await
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
