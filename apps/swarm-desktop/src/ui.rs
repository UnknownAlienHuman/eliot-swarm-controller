//! Desktop screen: one native WinUI 3 window through Reactor.
//!
//! Donor: `windows-reactor` 0.100.0 (Microsoft windows-rs,
//! https://github.com/microsoft/windows-rs), MIT OR Apache-2.0, rust-version
//! 1.95. Used as a direct dependency; no donor source is copied.
//!
//! Data path: every read and mutation runs on a Reactor background worker
//! and calls [`crate::client`] / [`crate::feed`], which use the existing
//! typed daemon RPC and its validators. The UI thread never performs RPC.
//! Each control binds its Agent, generation and turn/request at click time
//! (see [`crate::state::BoundAction`]); later selection changes cannot
//! retarget an in-flight submission. Results carry the selection epoch and
//! are dropped when the conversation they were read for is no longer shown.
//!
//! Reactor 0.100.0 integration gaps, stated rather than papered over:
//! `ScrollViewer` exposes no offset, `ViewChanged`, `ChangeView` or anchor
//! ratio, so the transcript cannot detect "at the bottom" or auto-follow;
//! "Jump to latest" moves focus to the end anchor, which WinUI brings into
//! view. There is no font-family property (code is not monospace), no
//! clipboard (see [`crate::clipboard`]) and no key events on `TextBox`
//! (Enter cannot send; Ctrl+Enter is a Send accelerator).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use eliot_core::api::{AgentGetResponse, AgentListRequest, CommandSubmitResponse};
use windows_reactor::*;

use crate::chat::{Conversation, Invalidation};
use crate::client;
use crate::feed::{self, Delivery, Depth, Reread, Snapshot};
use crate::live::{self, LiveStatus, Next};
use crate::markdown::Doc;
use crate::state::{BoundAction, DraftStore, MutationLedger, Selection};
use crate::theme;
use crate::views::{self, RenderCache, RenderedItem, RowCallbacks};

/// Agent rows per `agent.list` page in the navigation list.
const AGENT_PAGE: usize = 100;
/// Index 0 of the project picker lists every Agent in scope.
const ALL_PROJECTS: &str = "All projects (every Agent in scope)";

/// UI copy of one project registration.
#[derive(Clone, Debug)]
struct UiProject {
    id: String,
    profiles: Vec<String>,
    workspace: String,
}

/// UI copy of one Agent row: identity, generation and liveness only.
#[derive(Clone, Debug)]
struct UiAgent {
    id: String,
    generation: i64,
    harness: String,
    state: String,
}

/// One page of the navigation list with its honest counters.
#[derive(Clone, Debug)]
struct AgentPage {
    rows: Vec<UiAgent>,
    next: Option<String>,
    total: usize,
}

/// UI copy of one pending request with its exact control binding.
#[derive(Clone, Debug)]
struct UiPending {
    binding: BoundAction,
    turn_id: String,
    kind: String,
    summary: String,
}

/// Readback for the selected Agent.
#[derive(Clone, Debug)]
struct UiDetail {
    generation: i64,
    active_turn: Option<String>,
    pending: Vec<UiPending>,
    profile: String,
    harness: String,
    effective: String,
    state: String,
}

/// Admission of one send: bound Agent, draft revision and submitted text.
#[derive(Clone, Debug)]
struct SendOutcome {
    agent: String,
    command_id: String,
    draft_rev: u64,
    text: String,
    state: String,
    recovered: bool,
}

/// Admission of one launch-with-text: the new Agent and its linked input.
#[derive(Clone, Debug)]
struct LaunchOutcome {
    agent: String,
    generation: i64,
    command_id: String,
    linked_command: Option<String>,
    text: String,
    launch_rev: u64,
}

type Fallible<T> = Result<T, String>;

/// Messages are plain owned data so background workers can return them.
#[derive(Clone, Debug)]
enum Message {
    ComposerChanged(String),
    WorkspaceChanged(String),
    ProfileTextChanged(String),
    AnswerChanged(String, String),
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    AgentsLoaded(u64, bool, Fallible<AgentPage>),
    ProjectSelected(Option<usize>),
    AgentSelected(Option<usize>),
    ProfileSelected(Option<usize>),
    RefreshRequested,
    MoreAgentsRequested,
    NewConversation,
    SnapshotLoaded(u64, Fallible<Box<Snapshot>>),
    Delivered(u64, Fallible<Option<Box<Delivery>>>),
    RereadLoaded(u64, Result<Box<Reread>, (String, Invalidation)>),
    RereadRetry(u64),
    BackoffDone(u64, bool),
    LoadOlderRequested,
    JumpToLatest,
    ToolsToggled(String, bool),
    CopyPart(String),
    CopyCode(String, usize),
    LaunchRequested,
    LaunchSubmitted(Fallible<LaunchOutcome>),
    SendRequested,
    SendSubmitted(Fallible<SendOutcome>),
    InterruptRequested,
    ApprovalRequested(UiPending, bool),
    AnswerRequested(UiPending),
    ControlSubmitted(String, Fallible<String>),
}

struct DesktopApp {
    data_dir: Fallible<PathBuf>,
    status: String,
    dark_accent: bool,
    selection: Selection,
    projects: Vec<UiProject>,
    project_idx: Option<usize>,
    agents: Vec<UiAgent>,
    agent_idx: Option<usize>,
    agents_next: Option<String>,
    agents_total: usize,
    agents_loading: bool,
    /// Tags agent-list loads; a reply for an older request is dropped.
    list_epoch: u64,
    pending_select: Option<UiAgent>,
    profiles: Vec<String>,
    profile_idx: Option<usize>,
    profile_text: String,
    workspace: String,
    launch_text: String,
    launch_rev: u64,
    // Conversation of the selected Agent.
    convo: Option<Conversation>,
    detail: Option<UiDetail>,
    epoch: u64,
    depth: Depth,
    live: LiveStatus,
    live_task: Option<ComponentTask>,
    failures: u32,
    reread_inflight: bool,
    invalidated: Invalidation,
    docs: HashMap<u64, Doc>,
    render_cache: RenderCache,
    items: Rc<Vec<Rc<RenderedItem>>>,
    items_revision: u64,
    expanded: HashSet<String>,
    unseen: usize,
    end_anchor: ElementRef<TextBox>,
    pending_local: Option<(String, String, String)>,
    // Mutations.
    drafts: DraftStore,
    ledger: MutationLedger,
    /// Ledger key of the single unresolved submission; blocks duplicates.
    in_flight_key: Option<String>,
    answers: HashMap<String, String>,
}

impl DesktopApp {
    fn selected_project(&self) -> Option<&UiProject> {
        self.project_idx
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| self.projects.get(index))
    }

    fn selected_agent(&self) -> Option<&UiAgent> {
        self.agent_idx.and_then(|index| self.agents.get(index))
    }

    fn selected_agent_id(&self) -> Option<String> {
        self.selection.agent.clone()
    }

    fn composer_text(&self) -> String {
        match self.selected_agent_id() {
            Some(agent) => self.drafts.get(&agent).to_owned(),
            None => self.launch_text.clone(),
        }
    }

    fn launch_profile(&self) -> Option<String> {
        if self.profiles.is_empty() {
            Some(self.profile_text.trim().to_owned()).filter(|profile| !profile.is_empty())
        } else {
            self.profile_idx
                .and_then(|index| self.profiles.get(index))
                .cloned()
        }
    }

    fn submitting(&self) -> bool {
        self.in_flight_key.is_some()
    }

    fn set_status(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    /// End the in-flight submission. A definite admission also releases the
    /// ledger entry; an uncertain outcome keeps the entry, so the next click
    /// of the same action reuses the same mutation id.
    fn settle_mutation(&mut self, admitted: bool) {
        if let Some(key) = self.in_flight_key.take()
            && admitted
        {
            self.ledger.complete(&key);
        }
    }

    fn rebuild_items(&mut self) {
        let items = self
            .convo
            .as_ref()
            .map(Conversation::items)
            .unwrap_or_default();
        let rendered = self.render_cache.render(&items, &self.docs);
        let previous: HashSet<&str> = self.items.iter().map(|item| item.key.as_str()).collect();
        let added = rendered
            .iter()
            .filter(|item| !previous.contains(item.key.as_str()))
            .count();
        if !self.items.is_empty() {
            self.unseen += added;
        }
        self.items_revision = views::key_revision(&rendered);
        self.items = Rc::new(rendered);
        let live: HashSet<u64> = items
            .iter()
            .flat_map(|item| item.parts.iter())
            .map(|part| crate::chat::text_hash(&part.text))
            .collect();
        self.docs.retain(|hash, _| live.contains(hash));
    }

    /// Start (or restart) the conversation feed: new epoch, cancel the old
    /// live task, capture a cursor and reload the current reads.
    fn start_snapshot(&mut self, context: &ComponentContext<Self>) {
        let Some(agent) = self.convo.as_ref().map(|convo| convo.agent().to_owned()) else {
            return;
        };
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.epoch += 1;
        if let Some(task) = self.live_task.take() {
            task.cancel();
        }
        self.reread_inflight = false;
        self.invalidated = Invalidation::default();
        if !matches!(self.live, LiveStatus::Stale(_)) {
            self.live = LiveStatus::Loading;
        }
        let epoch = self.epoch;
        let depth = self.depth;
        let _task = context.spawn_background(move |_| {
            Message::SnapshotLoaded(
                epoch,
                feed::snapshot(&data_dir, &agent, depth).map(Box::new),
            )
        });
    }

    fn spawn_delivery(&mut self, context: &ComponentContext<Self>, after: i64, wake: bool) {
        let Some(agent) = self.convo.as_ref().map(|convo| convo.agent().to_owned()) else {
            return;
        };
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let epoch = self.epoch;
        self.live_task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            Message::Delivered(
                epoch,
                feed::deliver(&data_dir, &agent, after, wake, &cancelled)
                    .map(|delivery| delivery.map(Box::new)),
            )
        }));
    }

    fn spawn_backoff(&mut self, context: &ComponentContext<Self>, delay_ms: u64) {
        let epoch = self.epoch;
        self.live_task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            Message::BackoffDone(epoch, feed::backoff(delay_ms, &cancelled))
        }));
    }

    /// Coalesce authoritative rereads: at most one in flight; invalidations
    /// that arrive meanwhile merge into the next one.
    fn queue_reread(&mut self, invalidation: Invalidation, context: &ComponentContext<Self>) {
        self.invalidated.merge(invalidation);
        if self.reread_inflight || !self.invalidated.any() {
            return;
        }
        let (Some(agent), Ok(data_dir)) = (
            self.convo.as_ref().map(|convo| convo.agent().to_owned()),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let work = std::mem::take(&mut self.invalidated);
        let limit = self.depth.result_limit;
        let epoch = self.epoch;
        self.reread_inflight = true;
        let _task = context.spawn_background(move |_| {
            let result = feed::reread(
                &data_dir,
                &agent,
                limit,
                work.turns,
                work.detail,
                work.inputs.clone(),
            );
            // A failed reread hands its work back so it is retried, not lost.
            Message::RereadLoaded(epoch, result.map(Box::new).map_err(|error| (error, work)))
        });
    }

    fn follow_next(&mut self, next: Next, context: &ComponentContext<Self>) {
        match next {
            Next::Wake { after } => {
                self.live = LiveStatus::Live;
                self.spawn_delivery(context, after, true);
            }
            Next::Drain { after } => self.spawn_delivery(context, after, false),
            Next::Resnapshot { reason } => {
                if let Some(convo) = self.convo.as_mut() {
                    convo.set_stale(Some(reason.clone()));
                }
                self.live = LiveStatus::Stale(reason);
                self.start_snapshot(context);
            }
            Next::Backoff {
                delay_ms,
                attempt,
                reason,
            } => {
                self.failures = attempt;
                self.live = LiveStatus::Reconnecting { attempt, reason };
                self.spawn_backoff(context, delay_ms);
            }
            Next::Stop { reason } => {
                self.live = LiveStatus::Stopped(reason);
                self.live_task = None;
            }
        }
    }

    fn open_conversation(&mut self, agent: &UiAgent, context: &ComponentContext<Self>) {
        self.selection.select_agent(Some(agent.id.clone()));
        self.convo = Some(Conversation::new(&agent.id));
        self.detail = None;
        self.depth = Depth::default();
        self.failures = 0;
        self.unseen = 0;
        self.items = Rc::new(Vec::new());
        self.live = LiveStatus::Loading;
        if let Some((target, command, text)) = self.pending_local.take() {
            if target == agent.id
                && let Some(convo) = self.convo.as_mut()
            {
                convo.note_local_input(&command, &text);
            } else {
                self.pending_local = Some((target, command, text));
            }
        }
        self.rebuild_items();
        self.set_status(format!("Reading retained history of {}…", agent.id));
        self.start_snapshot(context);
    }

    fn load_agents(&mut self, context: &ComponentContext<Self>, after: Option<String>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let append = after.is_some();
        self.list_epoch += 1;
        let list_epoch = self.list_epoch;
        let request = AgentListRequest {
            after,
            limit: AGENT_PAGE,
            project: self.selected_project().map(|project| project.id.clone()),
            ..Default::default()
        };
        self.agents_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::AgentsLoaded(
                list_epoch,
                append,
                load_agents_blocking(&data_dir, &request),
            )
        });
    }
}

/// Load project registrations on a worker; decode to picker rows.
fn load_projects_blocking(data_dir: &Path) -> Fallible<Vec<UiProject>> {
    client::block_on(async {
        let registrations = client::list_projects(data_dir)
            .await
            .map_err(|error| error.to_string())?;
        Ok(registrations
            .into_iter()
            .map(|registration| UiProject {
                workspace: registration.checkout.clone(),
                profiles: registration.profiles.clone(),
                id: registration.id.clone(),
            })
            .collect())
    })
}

/// One validated page of Agent rows on a worker.
fn load_agents_blocking(data_dir: &Path, request: &AgentListRequest) -> Fallible<AgentPage> {
    client::block_on(async {
        let reply = client::list_agents(data_dir, request)
            .await
            .map_err(|error| error.to_string())?;
        Ok(AgentPage {
            rows: reply
                .agents
                .into_iter()
                .map(|row| UiAgent {
                    id: row.summary.id,
                    generation: row.summary.generation,
                    harness: row.summary.harness,
                    state: format!("{:?}", row.summary.state).to_lowercase(),
                })
                .collect(),
            next: reply.next,
            total: reply.total,
        })
    })
}

fn detail_from(reply: &AgentGetResponse) -> UiDetail {
    let pending = reply
        .agent
        .pending
        .iter()
        .map(|request| UiPending {
            binding: BoundAction::request_control(
                &reply.agent.id,
                reply.agent.generation,
                &request.id,
            ),
            turn_id: request.turn_id.clone(),
            kind: request.kind.clone(),
            summary: request_summary(&request.details),
        })
        .collect();
    UiDetail {
        generation: reply.agent.generation,
        active_turn: reply.agent.active_turn.clone(),
        pending,
        profile: reply.agent.profile.clone(),
        harness: reply.agent.harness.clone(),
        effective: effective_summary(reply),
        state: format!("{:?}", reply.agent.state).to_lowercase(),
    }
}

fn request_summary(details: &serde_json::Value) -> String {
    let text = details.to_string();
    if text.chars().count() <= 160 {
        text
    } else {
        format!("{}…", text.chars().take(160).collect::<String>())
    }
}

/// Requested configuration revision and the effective native readback, kept
/// apart: the profile is not proof of what the harness runs.
fn effective_summary(reply: &AgentGetResponse) -> String {
    let effective = reply.agent.effective.as_ref();
    let model = effective
        .and_then(|value| value.get("model"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("not read back");
    let effort = effective
        .and_then(|value| value.get("effort"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("not read back");
    match reply.config.as_ref() {
        Some(config) => format!(
            "effective {model}/{effort} · requested rev {} {}/{}",
            config.revision,
            config.model.as_deref().unwrap_or("profile default"),
            config.effort.as_deref().unwrap_or("profile default"),
        ),
        None => format!("effective {model}/{effort}"),
    }
}

fn submit_note(reply: &CommandSubmitResponse) -> String {
    format!("{:?}", reply.state).to_lowercase()
}

impl Component for DesktopApp {
    type Input = ();
    type Message = Message;

    fn create(_input: &(), context: &ComponentContext<Self>) -> Self {
        let data_dir = client::data_dir().map_err(|error| error.to_string());
        let mut app = Self {
            data_dir,
            status: "Connecting to the daemon…".to_owned(),
            dark_accent: false,
            selection: Selection::default(),
            projects: Vec::new(),
            project_idx: Some(0),
            agents: Vec::new(),
            agent_idx: None,
            agents_next: None,
            agents_total: 0,
            agents_loading: false,
            list_epoch: 0,
            pending_select: None,
            profiles: Vec::new(),
            profile_idx: None,
            profile_text: String::new(),
            workspace: String::new(),
            launch_text: String::new(),
            launch_rev: 0,
            convo: None,
            detail: None,
            epoch: 0,
            depth: Depth::default(),
            live: LiveStatus::Idle,
            live_task: None,
            failures: 0,
            reread_inflight: false,
            invalidated: Invalidation::default(),
            docs: HashMap::new(),
            render_cache: RenderCache::default(),
            items: Rc::new(Vec::new()),
            items_revision: 0,
            expanded: HashSet::new(),
            unseen: 0,
            end_anchor: ElementRef::new(),
            pending_local: None,
            drafts: DraftStore::default(),
            ledger: MutationLedger::default(),
            in_flight_key: None,
            answers: HashMap::new(),
        };
        if let Ok(data_dir) = app.data_dir.clone() {
            let _task = context.spawn_background(move |_| {
                Message::ProjectsLoaded(load_projects_blocking(&data_dir))
            });
            app.load_agents(context, None);
        }
        app
    }

    fn update(&mut self, message: Message, context: &ComponentContext<Self>) {
        match message {
            Message::ComposerChanged(text) => {
                if let Some(agent) = self.selected_agent_id() {
                    self.drafts.set(&agent, text);
                } else {
                    self.launch_text = text;
                    self.launch_rev = self.launch_rev.wrapping_add(1);
                }
            }
            Message::WorkspaceChanged(text) => self.workspace = text,
            Message::ProfileTextChanged(text) => self.profile_text = text,
            Message::AnswerChanged(request, text) => {
                self.answers.insert(request, text);
            }
            Message::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
                    let count = projects.len();
                    self.projects = projects;
                    self.set_status(format!(
                        "{count} registered project(s); listing every Agent in scope."
                    ));
                }
                Err(error) => self.set_status(format!("project.list failed: {error}")),
            },
            Message::AgentsLoaded(list_epoch, append, page) => {
                if list_epoch != self.list_epoch {
                    return;
                }
                self.agents_loading = false;
                match page {
                    Ok(page) => {
                        if append {
                            self.agents.extend(page.rows);
                        } else {
                            self.agents = page.rows;
                        }
                        self.agents_next = page.next;
                        self.agents_total = page.total;
                        if let Some(wanted) = self.pending_select.take()
                            && !self.agents.iter().any(|agent| agent.id == wanted.id)
                        {
                            self.agents.push(wanted.clone());
                            self.pending_select = Some(wanted);
                        }
                        let selected = self
                            .pending_select
                            .take()
                            .map(|agent| agent.id)
                            .or_else(|| self.selection.agent.clone());
                        self.agent_idx = selected
                            .and_then(|id| self.agents.iter().position(|agent| agent.id == id));
                        if let Some(agent) = self.selected_agent().cloned()
                            && self.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                        {
                            self.open_conversation(&agent, context);
                        }
                    }
                    Err(error) => self.set_status(format!("agent.list failed: {error}")),
                }
            }
            Message::ProjectSelected(index) => {
                self.project_idx = index;
                self.agents.clear();
                self.agent_idx = None;
                self.agents_next = None;
                self.agents_total = 0;
                match self.selected_project().cloned() {
                    Some(project) => {
                        self.profiles = project.profiles.clone();
                        self.workspace = project.workspace.clone();
                        self.selection.project = Some(project.id.clone());
                    }
                    None => {
                        self.profiles.clear();
                        self.selection.project = None;
                    }
                }
                self.profile_idx = None;
                self.selection.profile = None;
                self.load_agents(context, None);
            }
            Message::AgentSelected(index) => {
                self.agent_idx = index;
                // A list reload clears the native selection (None); the open
                // conversation and its feed stay as they are.
                if let Some(agent) = self.selected_agent().cloned()
                    && self.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                {
                    self.open_conversation(&agent, context);
                }
            }
            Message::ProfileSelected(index) => {
                self.profile_idx = index;
                self.selection.profile = index.and_then(|index| self.profiles.get(index)).cloned();
            }
            Message::RefreshRequested => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let _task = context.spawn_background(move |_| {
                        Message::ProjectsLoaded(load_projects_blocking(&data_dir))
                    });
                }
                self.load_agents(context, None);
                if self.convo.is_some() {
                    self.failures = 0;
                    self.start_snapshot(context);
                }
            }
            Message::NewConversation => {
                // Leave the conversation locally; the Agent keeps running.
                self.epoch += 1;
                if let Some(task) = self.live_task.take() {
                    task.cancel();
                }
                self.selection.select_agent(None);
                self.agent_idx = None;
                self.convo = None;
                self.detail = None;
                self.live = LiveStatus::Idle;
                self.items = Rc::new(Vec::new());
                self.items_revision = 0;
                self.unseen = 0;
                self.set_status("New conversation: launch an Agent with its first message.");
            }
            Message::MoreAgentsRequested => {
                if let Some(after) = self.agents_next.clone() {
                    self.load_agents(context, Some(after));
                }
            }
            Message::SnapshotLoaded(epoch, result) => {
                if epoch != self.epoch {
                    return;
                }
                match result {
                    Ok(snapshot) => {
                        let snapshot = *snapshot;
                        self.failures = 0;
                        self.docs.extend(snapshot.docs);
                        self.detail = Some(detail_from(&snapshot.detail));
                        let cursor = snapshot.coverage.cursor;
                        if let Some(convo) = self.convo.as_mut() {
                            let missing =
                                convo.reset(snapshot.coverage, &snapshot.turns, &snapshot.events);
                            convo.apply_inputs(snapshot.inputs);
                            convo.set_stale(None);
                            self.invalidated.merge(Invalidation {
                                inputs: missing
                                    .inputs
                                    .into_iter()
                                    .filter(|command| convo.missing_inputs().contains(command))
                                    .collect(),
                                ..Invalidation::default()
                            });
                        }
                        self.rebuild_items();
                        self.set_status("Retained history loaded; following live events.");
                        self.follow_next(Next::Wake { after: cursor }, context);
                        self.queue_reread(Invalidation::default(), context);
                    }
                    Err(error) => {
                        let (code, text) = live::split_error(&error);
                        let next = live::after_error(code, text, self.failures);
                        self.set_status(format!("History read failed: {error}"));
                        match next {
                            // A snapshot that cannot read its own window retries later.
                            Next::Resnapshot { reason } => self.follow_next(
                                Next::Backoff {
                                    delay_ms: live::backoff_ms(self.failures),
                                    attempt: self.failures + 1,
                                    reason,
                                },
                                context,
                            ),
                            other => self.follow_next(other, context),
                        }
                    }
                }
            }
            Message::Delivered(epoch, result) => {
                if epoch != self.epoch {
                    return;
                }
                match result {
                    Ok(None) => {}
                    Ok(Some(delivery)) => {
                        let delivery = *delivery;
                        self.failures = 0;
                        self.docs.extend(delivery.docs);
                        let mut resnapshot = false;
                        if let Some(convo) = self.convo.as_mut() {
                            let invalidation = convo.apply_events(&delivery.events);
                            convo.advance_cursor(delivery.next);
                            resnapshot = invalidation.generation_changed;
                            if !delivery.events.is_empty() {
                                self.rebuild_items();
                                self.queue_reread(invalidation, context);
                            }
                        }
                        let next = if resnapshot {
                            Next::Resnapshot {
                                reason: "a newer generation started".to_owned(),
                            }
                        } else {
                            live::after_page(
                                delivery.after,
                                delivery.next,
                                delivery.truncated,
                                delivery.lagged,
                            )
                        };
                        self.follow_next(next, context);
                    }
                    Err(error) => {
                        let (code, text) = live::split_error(&error);
                        let next = live::after_error(code, text, self.failures);
                        self.follow_next(next, context);
                    }
                }
            }
            Message::BackoffDone(epoch, proceed) => {
                if epoch == self.epoch && proceed {
                    // Reconnect: capture a new cursor and reload the reads.
                    self.start_snapshot(context);
                }
            }
            Message::RereadLoaded(epoch, result) => {
                if epoch != self.epoch {
                    return;
                }
                self.reread_inflight = false;
                match result {
                    Ok(reread) => {
                        let reread = *reread;
                        self.docs.extend(reread.docs);
                        if let Some(detail) = reread.detail.as_ref() {
                            self.detail = Some(detail_from(detail));
                        }
                        if let Some(convo) = self.convo.as_mut() {
                            if let Some(turns) = reread.turns.as_deref() {
                                convo.apply_turns(turns);
                            }
                            convo.apply_inputs(reread.inputs);
                        }
                        self.rebuild_items();
                        self.queue_reread(Invalidation::default(), context);
                    }
                    Err((error, work)) => {
                        self.set_status(format!("Readback failed, retrying: {error}"));
                        self.invalidated.merge(work);
                        let delay = live::backoff_ms(self.failures.min(4));
                        let _task = context.spawn_background(move |cancel| {
                            let cancelled = || cancel.is_cancelled();
                            let _ = feed::backoff(delay, &cancelled);
                            Message::RereadRetry(epoch)
                        });
                    }
                }
            }
            Message::RereadRetry(epoch) => {
                if epoch == self.epoch {
                    self.queue_reread(Invalidation::default(), context);
                }
            }
            Message::LoadOlderRequested => {
                self.depth = self.depth.older();
                self.set_status("Loading older retained history…");
                self.start_snapshot(context);
            }
            Message::JumpToLatest => {
                self.unseen = 0;
                if !self.end_anchor.request_focus() {
                    self.set_status("Jump to latest: the transcript end is not realized yet.");
                }
            }
            Message::ToolsToggled(key, expanded) => {
                if expanded {
                    self.expanded.insert(key);
                } else {
                    self.expanded.remove(&key);
                }
            }
            Message::CopyPart(key) => {
                let text = self.convo.as_ref().and_then(|convo| convo.part_text(&key));
                match text.map(|text| (text.chars().count(), crate::clipboard::set_text(&text))) {
                    Some((count, Ok(()))) => self.set_status(format!("Copied {count} characters.")),
                    Some((_, Err(error))) => self.set_status(format!("Copy failed: {error}")),
                    None => self.set_status("That message is no longer resident."),
                }
            }
            Message::CopyCode(key, index) => {
                let code = self
                    .convo
                    .as_ref()
                    .and_then(|convo| convo.part_text(&key))
                    .map(|text| crate::markdown::parse(&text))
                    .and_then(|doc| doc.code_blocks().get(index).map(|code| (*code).to_owned()));
                match code.map(|code| crate::clipboard::set_text(&code)) {
                    Some(Ok(())) => self.set_status("Copied code block."),
                    Some(Err(error)) => self.set_status(format!("Copy failed: {error}")),
                    None => self.set_status("That code block is no longer resident."),
                }
            }
            Message::LaunchRequested => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let Some(profile) = self.launch_profile() else {
                    self.set_status("Choose or type a configured profile first.");
                    return;
                };
                let text = self.launch_text.clone();
                if text.trim().is_empty() || self.workspace.trim().is_empty() {
                    self.set_status("Type the workspace and the first message before launching.");
                    return;
                }
                let key = format!("launch:{profile}:{}:{}", self.workspace, self.launch_rev);
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                let workspace = self.workspace.clone();
                let launch_rev = self.launch_rev;
                self.set_status(format!("Launching ({mutation_id})…"));
                let _task = context.spawn_background(move |_| {
                    Message::LaunchSubmitted(client::block_on(async {
                        let reply = client::submit_launch(
                            &data_dir,
                            mutation_id,
                            Some(profile),
                            &workspace,
                            Some(text.clone()),
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                        Ok(LaunchOutcome {
                            agent: reply
                                .agent
                                .clone()
                                .ok_or_else(|| "launch admitted no agent".to_owned())?,
                            generation: reply.generation.unwrap_or_default(),
                            command_id: reply.command_id.clone(),
                            linked_command: reply.linked_command.clone(),
                            text,
                            launch_rev,
                        })
                    }))
                });
            }
            Message::LaunchSubmitted(outcome) => {
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(outcome) => {
                        // The first prompt rode the launch admission; it is
                        // never sent again. Clear only the submitted draft.
                        if self.launch_rev == outcome.launch_rev {
                            self.launch_text.clear();
                        }
                        if let Some(linked) = outcome.linked_command.clone() {
                            self.pending_local =
                                Some((outcome.agent.clone(), linked, outcome.text.clone()));
                        }
                        self.set_status(format!(
                            "Launched {} ({}); its first input is {}.",
                            outcome.agent,
                            outcome.command_id,
                            outcome.linked_command.as_deref().unwrap_or("not linked")
                        ));
                        self.pending_select = Some(UiAgent {
                            id: outcome.agent.clone(),
                            generation: outcome.generation,
                            harness: String::new(),
                            state: "created".to_owned(),
                        });
                        self.selection.select_agent(Some(outcome.agent));
                        self.load_agents(context, None);
                    }
                    Err(error) => {
                        self.set_status(format!(
                            "Launch not confirmed (Launch again reuses its id): {error}"
                        ));
                    }
                }
            }
            Message::SendRequested => {
                if self.submitting() {
                    return;
                }
                let (Ok(data_dir), Some(agent)) =
                    (self.data_dir.clone(), self.selected_agent().cloned())
                else {
                    self.set_status("Select an Agent first.");
                    return;
                };
                let text = self.drafts.get(&agent.id).to_owned();
                if text.trim().is_empty() {
                    self.set_status("Type a message before sending.");
                    return;
                }
                let revision = self.drafts.revision(&agent.id);
                let key = format!("send:{}:{revision}", agent.id);
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status(format!("Sending ({mutation_id})…"));
                let agent_id = agent.id.clone();
                let _task = context.spawn_background(move |_| {
                    Message::SendSubmitted(client::block_on(async {
                        let submitted =
                            client::submit_send(&data_dir, mutation_id.clone(), &agent_id, &text)
                                .await;
                        let (command_id, state, recovered) = match submitted {
                            Ok(reply) => (reply.command_id.clone(), submit_note(&reply), false),
                            Err(error)
                                if matches!(error.code.as_str(), "unknown" | "unavailable") =>
                            {
                                // Uncertain reply: read the retained command
                                // instead of sending again under a new id.
                                let command = client::read_command(&data_dir, &mutation_id)
                                    .await
                                    .map_err(|_| error.to_string())?;
                                (
                                    command.id,
                                    format!("{:?}", command.state).to_lowercase(),
                                    true,
                                )
                            }
                            Err(error) => return Err(error.to_string()),
                        };
                        Ok(SendOutcome {
                            agent: agent_id,
                            command_id,
                            draft_rev: revision,
                            text,
                            state,
                            recovered,
                        })
                    }))
                });
            }
            Message::SendSubmitted(outcome) => {
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(outcome) => {
                        // Bound to the submitting Agent and draft revision, not
                        // to whatever conversation is open now.
                        self.drafts.ack(&outcome.agent, outcome.draft_rev);
                        if let Some(convo) = self.convo.as_mut()
                            && convo.agent() == outcome.agent
                        {
                            convo.note_local_input(&outcome.command_id, &outcome.text);
                        }
                        self.rebuild_items();
                        self.set_status(format!(
                            "Send {} ({}){}.",
                            outcome.state,
                            outcome.command_id,
                            if outcome.recovered {
                                ", confirmed by reading the retained command"
                            } else {
                                ""
                            }
                        ));
                    }
                    Err(error) => {
                        self.set_status(format!(
                            "Send not admitted (Send again reuses its id): {error}"
                        ));
                    }
                }
            }
            Message::InterruptRequested => {
                if self.submitting() {
                    return;
                }
                let (Ok(data_dir), Some(agent), Some(detail)) = (
                    self.data_dir.clone(),
                    self.selected_agent_id(),
                    self.detail.clone(),
                ) else {
                    self.set_status("Select an Agent first.");
                    return;
                };
                let Some(turn) = detail.active_turn.clone() else {
                    self.set_status("No active turn to interrupt.");
                    return;
                };
                let bound = BoundAction::turn_control(&agent, detail.generation, &turn);
                let key = format!("interrupt:{agent}:{}:{turn}", detail.generation);
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status(format!("Interrupting turn {turn} ({mutation_id})…"));
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(async {
                        client::submit_interrupt(
                            &data_dir,
                            mutation_id,
                            &bound.agent,
                            bound.generation,
                            bound.turn.clone().unwrap_or_default(),
                        )
                        .await
                        .map(|reply| submit_note(&reply))
                        .map_err(|error| error.to_string())
                    });
                    Message::ControlSubmitted("Interrupt".to_owned(), result)
                });
            }
            Message::ApprovalRequested(request, approve) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let verb = if approve { "approve" } else { "deny" };
                let key = format!(
                    "{verb}:{}:{}",
                    request.binding.agent,
                    request.binding.request.clone().unwrap_or_default()
                );
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status("Answering that exact request…");
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(async {
                        client::submit_approval(
                            &data_dir,
                            mutation_id,
                            client::ApprovalInput {
                                agent: request.binding.agent.clone(),
                                generation: request.binding.generation,
                                request: request.binding.request.clone().unwrap_or_default(),
                                approve,
                                choice: None,
                                feedback: None,
                            },
                        )
                        .await
                        .map(|reply| submit_note(&reply))
                        .map_err(|error| error.to_string())
                    });
                    Message::ControlSubmitted(verb.to_owned(), result)
                });
            }
            Message::AnswerRequested(request) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let answer = self
                    .answers
                    .get(request.binding.request.as_deref().unwrap_or_default())
                    .cloned()
                    .unwrap_or_default();
                let answers = match serde_json::from_str::<serde_json::Value>(&answer) {
                    Ok(answers) if answers.is_object() => answers,
                    _ => {
                        self.set_status("Answers must be a JSON object keyed by question id.");
                        return;
                    }
                };
                let key = format!(
                    "answer:{}:{}",
                    request.binding.agent,
                    request.binding.request.clone().unwrap_or_default()
                );
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status("Answering that exact request…");
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(async {
                        client::submit_answer(
                            &data_dir,
                            mutation_id,
                            &request.binding.agent,
                            request.binding.generation,
                            request.binding.request.as_deref().unwrap_or_default(),
                            answers,
                        )
                        .await
                        .map(|reply| submit_note(&reply))
                        .map_err(|error| error.to_string())
                    });
                    Message::ControlSubmitted("Answer".to_owned(), result)
                });
            }
            Message::ControlSubmitted(what, outcome) => {
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(state) => self.set_status(format!("{what} admitted: {state}.")),
                    Err(error) => self.set_status(format!(
                        "{what} not admitted (retry reuses its id): {error}"
                    )),
                }
            }
        }
    }

    fn view(&self, _input: &(), context: &mut ViewContext<Self>) -> View {
        context.window_title("Swarm Desktop");
        let header = StackPanel::new()
            .grid_row(0)
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::LG)
            .margin(Thickness::new(
                theme::spacing::LG,
                theme::spacing::MD,
                theme::spacing::LG,
                theme::spacing::SM,
            ))
            .children((
                TextBlock::new()
                    .text("Swarm Desktop")
                    .font_size(theme::type_scale::TITLE)
                    .font_weight(FontWeight::BOLD),
                TextBlock::new()
                    .text(self.live.label())
                    .vertical_alignment(VerticalAlignment::Center)
                    .foreground(theme::winui::text_accent()),
                TextBlock::new()
                    .text(self.status.clone())
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .opacity(theme::MUTED_OPACITY),
            ));

        let main = Grid::new()
            .grid_row(1)
            .columns([
                GridLength::Pixel(theme::layout::NAV_WIDTH),
                GridLength::STAR,
            ])
            .column_spacing(theme::spacing::LG)
            .margin(Thickness::new(
                theme::spacing::LG,
                0.0,
                theme::spacing::LG,
                theme::spacing::LG,
            ))
            .children((self.nav_view(context), self.chat_view(context)));

        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((header, main))
    }
}

impl DesktopApp {
    fn nav_view(&self, context: &mut ViewContext<Self>) -> View {
        let mut project_names = vec![ALL_PROJECTS.to_owned()];
        project_names.extend(self.projects.iter().map(|project| project.id.clone()));
        let project_picker = ComboBox::new()
            .items_source(project_names)
            .selected_index(self.project_idx)
            .placeholder_text("Project")
            .automation_name("Project")
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .on_selection_changed(context.callback(Message::ProjectSelected));

        let agent_items: Vec<KeyedView> = self
            .agents
            .iter()
            .map(|agent| {
                KeyedView::new(
                    agent.id.clone(),
                    ListViewItem::new().content(
                        StackPanel::new().spacing(2.0).children((
                            TextBlock::new()
                                .text(format!("{} · {}", agent.id, agent.state))
                                .font_weight(FontWeight::SEMI_BOLD),
                            TextBlock::new()
                                .text(format!("{} · gen {}", agent.harness, agent.generation))
                                .font_size(theme::type_scale::LABEL)
                                .opacity(theme::MUTED_OPACITY),
                        )),
                    ),
                )
            })
            .collect();
        let agent_list = ListView::new()
            .grid_row(3)
            .selected_index(self.agent_idx)
            .automation_name("Agents")
            .on_selection_changed(context.callback(Message::AgentSelected))
            .collection_slot(ListViewSlot::Items, agent_items);

        let counter = TextBlock::new()
            .text(if self.agents_loading {
                "Loading Agents…".to_owned()
            } else {
                format!(
                    "Showing {} of {} Agent(s){}",
                    self.agents.len(),
                    self.agents_total,
                    if self.agents_next.is_some() && self.agents.len() < self.agents_total {
                        "; more exist"
                    } else {
                        ""
                    }
                )
            })
            .font_size(theme::type_scale::LABEL)
            .opacity(theme::MUTED_OPACITY);
        let more = Button::new()
            .grid_row(5)
            .is_enabled(!self.agents_loading && self.agents.len() < self.agents_total)
            .on_click(context.message(Message::MoreAgentsRequested))
            .content(TextBlock::new().text("More Agents"));

        let profile_input: View = if self.profiles.is_empty() {
            TextBox::new()
                .text(self.profile_text.clone())
                .placeholder_text("Configured profile name")
                .automation_name("Profile")
                .on_text_changed(context.callback(Message::ProfileTextChanged))
                .into()
        } else {
            ComboBox::new()
                .items_source(self.profiles.clone())
                .selected_index(self.profile_idx)
                .placeholder_text("Profile")
                .automation_name("Profile")
                .on_selection_changed(context.callback(Message::ProfileSelected))
                .into()
        };
        let workspace_box = TextBox::new()
            .text(self.workspace.clone())
            .placeholder_text("Workspace path")
            .automation_name("Workspace")
            .on_text_changed(context.callback(Message::WorkspaceChanged));
        let refresh = Button::new()
            .on_click(context.message(Message::RefreshRequested))
            .content(TextBlock::new().text("Refresh"));
        let new_chat = Button::new()
            .automation_name("New conversation")
            .on_click(context.message(Message::NewConversation))
            .content(TextBlock::new().text("New"));

        let section = |text: &str| {
            TextBlock::new()
                .text(text.to_owned())
                .font_size(theme::type_scale::LABEL)
                .font_weight(FontWeight::SEMI_BOLD)
        };
        Grid::new()
            .grid_column(0)
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .row_spacing(theme::spacing::SM)
            .children((
                section("Project").grid_row(0),
                project_picker.grid_row(1),
                section("Agents").grid_row(2),
                agent_list,
                counter.grid_row(4),
                more,
                section("Launch profile").grid_row(6),
                Border::new().grid_row(7).content(profile_input),
                section("Workspace").grid_row(8),
                workspace_box.grid_row(9),
                StackPanel::new()
                    .grid_row(10)
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((refresh, new_chat)),
            ))
    }

    fn chat_view(&self, context: &mut ViewContext<Self>) -> View {
        let detail_text = match (&self.detail, self.selected_agent_id()) {
            (Some(detail), Some(agent)) => format!(
                "{agent} · gen {} · {} · {} · profile {} · {} · turn {}",
                detail.generation,
                detail.state,
                detail.harness,
                detail.profile,
                detail.effective,
                detail.active_turn.as_deref().unwrap_or("none"),
            ),
            (None, Some(agent)) => format!("{agent} · reading…"),
            _ => "New conversation: choose a profile and workspace, then type the first message."
                .to_owned(),
        };
        let coverage_lines = self
            .convo
            .as_ref()
            .map(|convo| convo.coverage().lines(convo.inputs_unavailable()))
            .unwrap_or_default();
        let can_load_older = self
            .convo
            .as_ref()
            .is_some_and(|convo| convo.coverage().can_load_older())
            && !matches!(self.live, LiveStatus::Loading);
        let coverage = StackPanel::new()
            .grid_row(1)
            .spacing(theme::spacing::SM)
            .orientation(Orientation::Horizontal)
            .children((
                TextBlock::new()
                    .text(coverage_lines.join("\n"))
                    .font_size(theme::type_scale::LABEL)
                    .text_wrapping(TextWrapping::Wrap)
                    .max_width(theme::layout::READING_WIDTH - 140.0)
                    .opacity(theme::MUTED_OPACITY),
                Button::new()
                    .is_enabled(can_load_older)
                    .on_click(context.message(Message::LoadOlderRequested))
                    .content(TextBlock::new().text("Load older")),
            ));

        let callbacks = RowCallbacks {
            copy_part: context.callback(Message::CopyPart),
            copy_code: context
                .callback(|(key, index): (String, usize)| Message::CopyCode(key, index)),
            toggle_tools: context
                .callback(|(key, expanded): (String, bool)| Message::ToolsToggled(key, expanded)),
        };
        let items = Rc::clone(&self.items);
        let keys = Rc::clone(&self.items);
        let expanded = Rc::new(self.expanded.clone());
        let source = VirtualSource::new(
            self.items_revision,
            items.len(),
            move |index| keys[index].key.clone(),
            move |index| views::item_view(&items[index], &expanded, &callbacks),
        );
        // The end anchor is an inline reply box bound to the same draft as the
        // composer: WinUI brings a focused TextBox into view, which a focused
        // Button is not, and Reactor exposes no ScrollViewer offset API.
        let end_anchor = TextBox::new()
            .element_ref(&self.end_anchor)
            .text(self.composer_text())
            .placeholder_text(match self.convo.as_ref() {
                Some(_) if self.items.is_empty() => {
                    "No retained turns in coverage yet. Reply here…"
                }
                Some(_) => "End of conversation. Reply here…",
                None => "",
            })
            .automation_name("End of conversation reply")
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .on_text_changed(context.callback(Message::ComposerChanged));
        let transcript = ScrollViewer::new()
            .grid_row(2)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .content(
                StackPanel::new()
                    .max_width(theme::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .children((ItemsRepeater::new().virtual_source(source), end_anchor)),
            );

        let pending_rows: Vec<KeyedView> = self
            .detail
            .as_ref()
            .map(|detail| detail.pending.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|request| {
                let key = request.binding.request.clone().unwrap_or_default();
                let answer_box = TextBox::new()
                    .text(self.answers.get(&key).cloned().unwrap_or_default())
                    .placeholder_text("{\"question\": {\"answers\": [\"…\"]}}")
                    .automation_name("Answer JSON")
                    .on_text_changed({
                        let key = key.clone();
                        context
                            .callback(move |text: String| Message::AnswerChanged(key.clone(), text))
                    });
                KeyedView::new(
                    key,
                    Border::new()
                        .background(theme::winui::surface())
                        .border_brush(theme::winui::text_critical())
                        .border_thickness(1.0)
                        .corner_radius(theme::radius::MD)
                        .padding(Thickness::uniform(theme::spacing::SM))
                        .content(
                            StackPanel::new().spacing(theme::spacing::XS).children((
                                TextBlock::new()
                                    .text(format!(
                                        "Pending {} · turn {}",
                                        request.kind, request.turn_id
                                    ))
                                    .font_weight(FontWeight::SEMI_BOLD)
                                    .foreground(theme::winui::text_critical()),
                                TextBlock::new()
                                    .text(request.summary.clone())
                                    .text_wrapping(TextWrapping::Wrap)
                                    .is_text_selection_enabled(true),
                                answer_box,
                                StackPanel::new()
                                    .orientation(Orientation::Horizontal)
                                    .spacing(theme::spacing::XS)
                                    .children((
                                        Button::new()
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::ApprovalRequested(
                                                request.clone(),
                                                true,
                                            )))
                                            .content(TextBlock::new().text("Approve")),
                                        Button::new()
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::ApprovalRequested(
                                                request.clone(),
                                                false,
                                            )))
                                            .content(TextBlock::new().text("Deny")),
                                        Button::new()
                                            .is_enabled(!self.submitting())
                                            .on_click(
                                                context.message(Message::AnswerRequested(
                                                    request.clone(),
                                                )),
                                            )
                                            .content(TextBlock::new().text("Answer")),
                                    )),
                            )),
                        ),
                )
            })
            .collect();
        let pending = StackPanel::new()
            .grid_row(3)
            .spacing(theme::spacing::XS)
            .keyed_children(pending_rows);

        let composer = TextBox::new()
            .text(self.composer_text())
            .placeholder_text(if self.selected_agent_id().is_some() {
                "Message the selected Agent… (Ctrl+Enter sends)"
            } else {
                "First message for a new Agent…"
            })
            .automation_name("Message")
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .height(theme::layout::COMPOSER_HEIGHT)
            .on_text_changed(context.callback(Message::ComposerChanged));

        let has_agent = self.selected_agent_id().is_some();
        let busy_turn = self
            .detail
            .as_ref()
            .and_then(|detail| detail.active_turn.as_ref())
            .is_some();
        let primary = if has_agent {
            Button::new()
                .resource_overrides(theme::winui::primary_button(self.dark_accent))
                .is_enabled(!self.submitting())
                .key_accelerators(KeyAccelerators::new([KeyAccelerator::new(
                    AcceleratorKey::Enter,
                    AcceleratorModifiers::Control,
                    context.message(Message::SendRequested),
                )]))
                .on_click(context.message(Message::SendRequested))
                .content(TextBlock::new().text(if busy_turn { "Queue" } else { "Send" }))
        } else {
            Button::new()
                .resource_overrides(theme::winui::primary_button(self.dark_accent))
                .is_enabled(!self.submitting())
                .on_click(context.message(Message::LaunchRequested))
                .content(TextBlock::new().text("Launch Agent"))
        };
        let interrupt = Button::new()
            .is_enabled(!self.submitting() && busy_turn)
            .automation_name("Interrupt the active turn")
            .on_click(context.message(Message::InterruptRequested))
            .content(TextBlock::new().text("Interrupt"));
        let jump = Button::new()
            .is_enabled(!self.items.is_empty())
            .on_click(context.message(Message::JumpToLatest))
            .content(TextBlock::new().text(if self.unseen > 0 {
                format!("Jump to latest ({} new)", self.unseen)
            } else {
                "Jump to latest".to_owned()
            }));
        let actions = StackPanel::new()
            .grid_row(5)
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::SM)
            .children((primary, interrupt, jump));

        Grid::new()
            .grid_column(1)
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .row_spacing(theme::spacing::SM)
            .children((
                TextBlock::new()
                    .text(detail_text)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .grid_row(0),
                coverage,
                transcript,
                pending,
                composer.max_width(theme::layout::READING_WIDTH).grid_row(4),
                actions,
            ))
    }
}

/// Run the single desktop window. Blocks while the WinUI message loop runs;
/// closing the window disconnects the UI, never the Agent.
pub fn run() -> Result<(), String> {
    App::run_component::<DesktopApp>(()).map_err(|error| error.to_string())
}
