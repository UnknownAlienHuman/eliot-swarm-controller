//! Styled real-chat screen: one native WinUI 3 window through Reactor.
//!
//! Donor: `windows-reactor` 0.100.0 (Microsoft windows-rs,
//! https://github.com/microsoft/windows-rs), MIT OR Apache-2.0, rust-version
//! 1.95. Used as a direct dependency; no donor source is copied. Native text
//! (TextBlock), lists (ComboBox/ListView), Button and ScrollViewer are the
//! only controls; Markdown rendering, virtualization tuning and the monitor
//! are later slices and are not claimed here.
//!
//! Data path: every load and mutation runs on a Reactor background worker and
//! calls [`crate::client`], which uses the existing typed daemon RPC. The UI
//! thread never performs RPC. Each control binds its Agent, generation and
//! turn/request at click time (see [`crate::state::BoundAction`]); later
//! selection changes cannot retarget an in-flight submission.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use windows_reactor::*;

use crate::chat::{self, ChatMessage, Role, Transcript};
use crate::client;
use crate::state::{BoundAction, DraftStore, MutationLedger, Selection};
use crate::theme;

/// UI copy of one project registration: picker id, launch profiles and the
/// checkout used as the default launch workspace.
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
    project: String,
    state: String,
    active_turn: Option<String>,
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
    effective: String,
    state: String,
}

/// Outcome of a settled send: which draft revision it submitted and the real
/// final text from result/turn readback, if any yet.
#[derive(Clone, Debug)]
struct SendOutcome {
    command_id: String,
    draft_rev: u64,
    final_turn: Option<String>,
    final_text: Option<String>,
}

/// Outcome of a settled launch: the new Agent plus its linked input result.
#[derive(Clone, Debug)]
struct LaunchOutcome {
    agent: String,
    command_id: String,
    final_turn: Option<String>,
    final_text: Option<String>,
}

type Fallible<T> = Result<T, String>;

/// Messages are plain owned data so background workers can return them.
#[derive(Clone, Debug)]
enum Message {
    ComposerChanged(String),
    WorkspaceChanged(String),
    AnswerChanged(String, String),
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    AgentsLoaded(Fallible<Vec<UiAgent>>),
    DetailLoaded(Fallible<UiDetail>),
    TranscriptLoaded(String, Fallible<Vec<(String, Role, String)>>),
    ProjectSelected(Option<usize>),
    AgentSelected(Option<usize>),
    ProfileSelected(Option<usize>),
    RefreshRequested,
    LaunchRequested,
    LaunchSettled(Fallible<LaunchOutcome>),
    SendRequested,
    SendSettled(Fallible<SendOutcome>),
    InterruptRequested,
    InterruptSettled(Fallible<String>),
    ApprovalRequested(UiPending, bool),
    ApprovalSettled(Fallible<String>),
    AnswerRequested(UiPending),
    AnswerSettled(Fallible<String>),
}

struct DesktopApp {
    data_dir: Fallible<PathBuf>,
    status: String,
    busy: bool,
    dark_accent: bool,
    selection: Selection,
    projects: Vec<UiProject>,
    project_idx: Option<usize>,
    agents: Vec<UiAgent>,
    agent_idx: Option<usize>,
    profiles: Vec<String>,
    profile_idx: Option<usize>,
    workspace: String,
    launch_text: String,
    detail: Option<UiDetail>,
    transcripts: HashMap<String, Transcript>,
    drafts: DraftStore,
    ledger: MutationLedger,
    /// Ledger key of the single in-flight mutation; completed on success so
    /// a later action mints fresh, retried only while unsettled.
    in_flight_key: Option<String>,
    answers: HashMap<String, String>,
}

impl DesktopApp {
    fn selected_project(&self) -> Option<&UiProject> {
        self.project_idx.and_then(|index| self.projects.get(index))
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

    fn set_status(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    fn daemon_dir(&self) -> Fallible<PathBuf> {
        self.data_dir.clone()
    }

    /// Release the in-flight ledger key after a successful settle; failures
    /// keep it so a retry reuses the same mutation id.
    fn settle_mutation(&mut self, ok: bool) {
        if ok && let Some(key) = self.in_flight_key.take() {
            self.ledger.complete(&key);
        }
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

/// Load Agent rows for one project on a worker.
fn load_agents_blocking(data_dir: &Path, project: &str) -> Fallible<Vec<UiAgent>> {
    client::block_on(async {
        let summaries = client::list_agents(data_dir, Some(project))
            .await
            .map_err(|error| error.to_string())?;
        Ok(summaries
            .into_iter()
            .map(|summary| UiAgent {
                id: summary.id.clone(),
                generation: summary.generation,
                project: summary.project.clone(),
                state: format!("{:?}", summary.state),
                active_turn: summary.active_turn.clone(),
            })
            .collect())
    })
}

/// Full readback for one Agent on a worker: generation, active turn,
/// pending requests, profile and effective settings.
fn load_detail_blocking(data_dir: &Path, agent: &str) -> Fallible<UiDetail> {
    client::block_on(async {
        let reply = client::get_agent(data_dir, agent)
            .await
            .map_err(|error| error.to_string())?;
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
        Ok(UiDetail {
            generation: reply.agent.generation,
            active_turn: reply.agent.active_turn.clone(),
            pending,
            profile: reply.agent.profile.clone(),
            effective: effective_summary(&reply),
            state: format!("{:?}", reply.agent.state),
        })
    })
}

fn request_summary(details: &serde_json::Value) -> String {
    let text = details.to_string();
    if text.len() <= 160 {
        text
    } else {
        format!("{}…", &text[..160])
    }
}

fn effective_summary(reply: &eliot_core::api::AgentGetResponse) -> String {
    match reply.config.as_ref() {
        Some(config) => format!(
            "rev {} model={} effort={}",
            config.revision,
            config.model.as_deref().unwrap_or("profile default"),
            config.effort.as_deref().unwrap_or("profile default"),
        ),
        None => "profile defaults".to_owned(),
    }
}

impl Component for DesktopApp {
    type Input = ();
    type Message = Message;

    fn create(_input: &(), context: &ComponentContext<Self>) -> Self {
        let data_dir = client::data_dir().map_err(|error| error.to_string());
        if let Ok(data_dir) = data_dir.clone() {
            let _task = context.spawn_background(move |_| {
                Message::ProjectsLoaded(load_projects_blocking(&data_dir))
            });
        }
        Self {
            data_dir,
            status: "Connecting to the daemon…".to_owned(),
            busy: false,
            dark_accent: false,
            selection: Selection::default(),
            projects: Vec::new(),
            project_idx: None,
            agents: Vec::new(),
            agent_idx: None,
            profiles: Vec::new(),
            profile_idx: None,
            workspace: String::new(),
            launch_text: String::new(),
            detail: None,
            transcripts: HashMap::new(),
            drafts: DraftStore::default(),
            ledger: MutationLedger::default(),
            in_flight_key: None,
            answers: HashMap::new(),
        }
    }

    fn update(&mut self, message: Message, context: &ComponentContext<Self>) {
        match message {
            Message::ComposerChanged(text) => {
                if let Some(agent) = self.selected_agent_id() {
                    self.drafts.set(&agent, text);
                } else {
                    self.launch_text = text;
                }
            }
            Message::WorkspaceChanged(text) => {
                self.workspace = text;
            }
            Message::AnswerChanged(request, text) => {
                self.answers.insert(request, text);
            }
            Message::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
                    self.projects = projects;
                    self.project_idx = None;
                    self.set_status("Select a project to list its Agents.".to_owned());
                }
                Err(error) => {
                    self.set_status(format!("project.list failed: {error}"));
                }
            },
            Message::AgentsLoaded(agents) => {
                self.busy = false;
                match agents {
                    Ok(agents) => {
                        self.agents = agents;
                        self.agent_idx = None;
                        self.detail = None;
                        self.set_status("Select an Agent to read its conversation.".to_owned());
                    }
                    Err(error) => {
                        self.set_status(format!("agent.list failed: {error}"));
                    }
                }
            }
            Message::DetailLoaded(detail) => {
                self.busy = false;
                match detail {
                    Ok(detail) => {
                        self.detail = Some(detail);
                        self.set_status("Readback current.".to_owned());
                    }
                    Err(error) => {
                        self.detail = None;
                        self.set_status(format!("agent.get failed: {error}"));
                    }
                }
            }
            Message::TranscriptLoaded(agent, rows) => match rows {
                Ok(rows) => {
                    let transcript = self.transcripts.entry(agent).or_default();
                    for (turn, role, text) in rows {
                        transcript.upsert(&turn, 1, role, text);
                    }
                }
                Err(error) => {
                    self.set_status(format!("result readback failed: {error}"));
                }
            },
            Message::ProjectSelected(index) => {
                self.project_idx = index;
                self.agents.clear();
                self.agent_idx = None;
                self.detail = None;
                self.selection.select_agent(None);
                let project = self.selected_project().cloned();
                match project {
                    Some(project) => {
                        self.profiles = project.profiles.clone();
                        self.profile_idx = None;
                        self.workspace = project.workspace.clone();
                        self.selection.project = Some(project.id.clone());
                        self.selection.profile = None;
                        self.set_status(format!("Loading Agents for {}…", project.id));
                        if let Ok(data_dir) = self.daemon_dir() {
                            let id = project.id.clone();
                            self.busy = true;
                            let _task = context.spawn_background(move |_| {
                                Message::AgentsLoaded(load_agents_blocking(&data_dir, &id))
                            });
                        }
                    }
                    None => {
                        self.profiles.clear();
                        self.profile_idx = None;
                        self.selection.project = None;
                    }
                }
            }
            Message::AgentSelected(index) => {
                self.agent_idx = index;
                self.detail = None;
                match self.selected_agent().cloned() {
                    Some(agent) => {
                        self.selection.select_agent(Some(agent.id.clone()));
                        self.set_status(format!("Reading {}…", agent.id));
                        if let Ok(data_dir) = self.daemon_dir() {
                            self.busy = true;
                            let id = agent.id.clone();
                            let detail_dir = data_dir.clone();
                            let _task = context.spawn_background(move |_| {
                                Message::DetailLoaded(load_detail_blocking(&detail_dir, &id))
                            });
                            let transcript_id = agent.id.clone();
                            let _task = context.spawn_background(move |_| {
                                let rows = client::block_on(async {
                                    client::latest_final_text(&data_dir, &transcript_id)
                                        .await
                                        .map_err(|error| error.to_string())
                                        .map(|final_text| {
                                            final_text
                                                .map(|(turn, text)| {
                                                    vec![(
                                                        format!("turn:{turn}"),
                                                        Role::Assistant,
                                                        text,
                                                    )]
                                                })
                                                .unwrap_or_default()
                                        })
                                });
                                Message::TranscriptLoaded(transcript_id, rows)
                            });
                        }
                    }
                    None => {
                        self.selection.select_agent(None);
                    }
                }
            }
            Message::ProfileSelected(index) => {
                self.profile_idx = index;
                self.selection.profile = index.and_then(|index| self.profiles.get(index)).cloned();
            }
            Message::RefreshRequested => {
                if self.busy {
                    return;
                }
                if let Ok(data_dir) = self.daemon_dir() {
                    self.busy = true;
                    self.set_status("Reloading projects…".to_owned());
                    let _task = context.spawn_background(move |_| {
                        Message::ProjectsLoaded(load_projects_blocking(&data_dir))
                    });
                }
            }
            Message::LaunchRequested => {
                if self.busy {
                    return;
                }
                let ready = match (self.daemon_dir(), self.selected_project().cloned()) {
                    (Ok(data_dir), Some(project)) => {
                        let profile = self
                            .profile_idx
                            .and_then(|index| self.profiles.get(index))
                            .cloned();
                        let text = self.launch_text.clone();
                        if text.trim().is_empty() {
                            self.set_status("Type the first message before launching.".to_owned());
                            None
                        } else {
                            let key = format!(
                                "launch:{}:{}:{}",
                                project.id,
                                profile.clone().unwrap_or_default(),
                                self.workspace
                            );
                            let mutation_id = self.ledger.id_for(&key);
                            self.in_flight_key = Some(key);
                            Some((data_dir, profile, text, mutation_id))
                        }
                    }
                    _ => {
                        self.set_status("Select a project and profile first.".to_owned());
                        None
                    }
                };
                if let Some((data_dir, profile, text, mutation_id)) = ready {
                    let workspace = self.workspace.clone();
                    self.busy = true;
                    self.set_status("Launching…".to_owned());
                    let _task = context.spawn_background(move |_| {
                        Message::LaunchSettled(client::block_on(async {
                            let reply = client::submit_launch(
                                &data_dir,
                                mutation_id,
                                profile,
                                &workspace,
                                Some(text),
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                            let agent = reply
                                .agent
                                .clone()
                                .ok_or_else(|| "launch admitted no agent".to_owned())?;
                            let command =
                                client::wait_command_settled(&data_dir, &reply.command_id)
                                    .await
                                    .map_err(|error| error.to_string())?;
                            let final_text = client::command_final_text(
                                &data_dir,
                                &command,
                                reply.linked_command.as_deref(),
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                            Ok(LaunchOutcome {
                                agent,
                                command_id: reply.command_id.clone(),
                                final_turn: final_text.as_ref().map(|(turn, _)| turn.clone()),
                                final_text: final_text.map(|(_, text)| text),
                            })
                        }))
                    });
                }
            }
            Message::LaunchSettled(outcome) => {
                self.busy = false;
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(outcome) => {
                        let sent = std::mem::take(&mut self.launch_text);
                        self.transcripts
                            .entry(outcome.agent.clone())
                            .or_default()
                            .upsert(&format!("cmd:{}", outcome.command_id), 1, Role::User, sent);
                        self.set_status(format!(
                            "Launched {} ({}).",
                            outcome.agent, outcome.command_id
                        ));
                        if let Some((turn, text)) = outcome.final_turn.zip(outcome.final_text) {
                            self.transcripts
                                .entry(outcome.agent.clone())
                                .or_default()
                                .upsert(&format!("turn:{turn}"), 1, Role::Assistant, text);
                        }
                        if let Ok(data_dir) = self.daemon_dir()
                            && let Some(project) = self.selection.project.clone()
                        {
                            self.busy = true;
                            let _task = context.spawn_background(move |_| {
                                Message::AgentsLoaded(load_agents_blocking(&data_dir, &project))
                            });
                        }
                    }
                    Err(error) => {
                        self.set_status(format!("launch failed (retry reuses its id): {error}"));
                    }
                }
            }
            Message::SendRequested => {
                if self.busy {
                    return;
                }
                let ready = match (self.daemon_dir(), self.selected_agent().cloned()) {
                    (Ok(data_dir), Some(agent)) => {
                        let text = self.drafts.get(&agent.id).to_owned();
                        if text.trim().is_empty() {
                            self.set_status("Type a message before sending.".to_owned());
                            None
                        } else {
                            let revision = self.drafts.revision(&agent.id);
                            let key = format!("send:{}:{revision}", agent.id);
                            let mutation_id = self.ledger.id_for(&key);
                            self.in_flight_key = Some(key);
                            Some((data_dir, agent, text, revision, mutation_id))
                        }
                    }
                    _ => {
                        self.set_status("Select an Agent first.".to_owned());
                        None
                    }
                };
                if let Some((data_dir, agent, text, revision, mutation_id)) = ready {
                    let agent_id = agent.id.clone();
                    self.busy = true;
                    self.set_status("Sending…".to_owned());
                    let _task = context.spawn_background(move |_| {
                        Message::SendSettled(client::block_on(async {
                            let reply =
                                client::submit_send(&data_dir, mutation_id, &agent_id, &text)
                                    .await
                                    .map_err(|error| error.to_string())?;
                            let command =
                                client::wait_command_settled(&data_dir, &reply.command_id)
                                    .await
                                    .map_err(|error| error.to_string())?;
                            let final_text = client::command_final_text(
                                &data_dir,
                                &command,
                                reply.linked_command.as_deref(),
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                            Ok(SendOutcome {
                                command_id: reply.command_id.clone(),
                                draft_rev: revision,
                                final_turn: final_text.as_ref().map(|(turn, _)| turn.clone()),
                                final_text: final_text.map(|(_, text)| text),
                            })
                        }))
                    });
                }
            }
            Message::SendSettled(outcome) => {
                self.busy = false;
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(outcome) => {
                        if let Some(agent) = self.selected_agent_id() {
                            let sent = self.drafts.get(&agent).to_owned();
                            self.transcripts.entry(agent.clone()).or_default().upsert(
                                &format!("cmd:{}", outcome.command_id),
                                1,
                                Role::User,
                                sent,
                            );
                            self.drafts.ack(&agent, outcome.draft_rev);
                            if let Some((turn, text)) = outcome.final_turn.zip(outcome.final_text) {
                                self.transcripts.entry(agent.clone()).or_default().upsert(
                                    &format!("turn:{turn}"),
                                    1,
                                    Role::Assistant,
                                    text,
                                );
                            }
                        }
                        self.set_status(format!("Sent ({}).", outcome.command_id));
                    }
                    Err(error) => {
                        self.set_status(format!("send failed (retry reuses its id): {error}"));
                    }
                }
            }
            Message::InterruptRequested => {
                if self.busy {
                    return;
                }
                let ready = match (self.daemon_dir(), self.selected_agent().cloned()) {
                    (Ok(data_dir), Some(agent)) => match self
                        .detail
                        .as_ref()
                        .and_then(|detail| detail.active_turn.clone())
                    {
                        Some(turn) => {
                            let bound =
                                BoundAction::turn_control(&agent.id, agent.generation, &turn);
                            let key = format!("interrupt:{}:{}:{turn}", agent.id, agent.generation);
                            let mutation_id = self.ledger.id_for(&key);
                            self.in_flight_key = Some(key);
                            Some((data_dir, bound, mutation_id))
                        }
                        None => {
                            self.set_status("No active turn to interrupt.".to_owned());
                            None
                        }
                    },
                    _ => {
                        self.set_status("Select an Agent first.".to_owned());
                        None
                    }
                };
                if let Some((data_dir, bound, mutation_id)) = ready {
                    self.busy = true;
                    self.set_status("Interrupting that exact turn…".to_owned());
                    let _task = context.spawn_background(move |_| {
                        Message::InterruptSettled(client::block_on(async {
                            client::submit_interrupt(
                                &data_dir,
                                mutation_id,
                                &bound.agent,
                                Some(bound.generation),
                                bound.turn.clone(),
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                            Ok("Interrupted.".to_owned())
                        }))
                    });
                }
            }
            Message::InterruptSettled(outcome) => {
                self.busy = false;
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(note) => self.set_status(note),
                    Err(error) => self.set_status(format!("interrupt failed: {error}")),
                }
            }
            Message::ApprovalRequested(request, approve) => {
                if self.busy {
                    return;
                }
                if let Ok(data_dir) = self.daemon_dir() {
                    let key = format!(
                        "{}:{}:{}",
                        if approve { "approve" } else { "deny" },
                        request.binding.agent,
                        request.binding.request.clone().unwrap_or_default()
                    );
                    let mutation_id = self.ledger.id_for(&key);
                    self.in_flight_key = Some(key);
                    self.busy = true;
                    self.set_status("Answering that exact request…".to_owned());
                    let _task = context.spawn_background(move |_| {
                        Message::ApprovalSettled(client::block_on(async {
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
                            .map_err(|error| error.to_string())?;
                            Ok("Request answered.".to_owned())
                        }))
                    });
                }
            }
            Message::ApprovalSettled(outcome) => {
                self.busy = false;
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(note) => self.set_status(note),
                    Err(error) => self.set_status(format!("request reply failed: {error}")),
                }
            }
            Message::AnswerRequested(request) => {
                if self.busy {
                    return;
                }
                let answer = self
                    .answers
                    .get(request.binding.request.as_deref().unwrap_or_default())
                    .cloned()
                    .unwrap_or_default();
                let answers: Result<serde_json::Value, _> = serde_json::from_str(&answer);
                match answers {
                    Ok(answers) if answers.is_object() => {
                        if let Ok(data_dir) = self.daemon_dir() {
                            let key = format!(
                                "answer:{}:{}",
                                request.binding.agent,
                                request.binding.request.clone().unwrap_or_default()
                            );
                            let mutation_id = self.ledger.id_for(&key);
                            self.in_flight_key = Some(key);
                            self.busy = true;
                            self.set_status("Answering that exact request…".to_owned());
                            let _task = context.spawn_background(move |_| {
                                Message::AnswerSettled(client::block_on(async {
                                    client::submit_answer(
                                        &data_dir,
                                        mutation_id,
                                        &request.binding.agent,
                                        request.binding.generation,
                                        request.binding.request.as_deref().unwrap_or_default(),
                                        answers,
                                    )
                                    .await
                                    .map_err(|error| error.to_string())?;
                                    Ok("Answer submitted.".to_owned())
                                }))
                            });
                        }
                    }
                    _ => {
                        self.set_status(
                            "Answers must be a JSON object keyed by question id.".to_owned(),
                        );
                    }
                }
            }
            Message::AnswerSettled(outcome) => {
                self.busy = false;
                self.settle_mutation(outcome.is_ok());
                match outcome {
                    Ok(note) => self.set_status(note),
                    Err(error) => self.set_status(format!("answer failed: {error}")),
                }
            }
        }
    }

    fn view(&self, _input: &(), context: &mut ViewContext<Self>) -> View {
        context.window_title("Swarm Desktop");
        let status = TextBlock::new().text(self.status.clone());
        let header = TextBlock::new()
            .text("Swarm Desktop — styled real chat")
            .font_size(theme::type_scale::TITLE)
            .font_weight(FontWeight::BOLD);

        let project_names: Vec<String> = self
            .projects
            .iter()
            .map(|project| project.id.clone())
            .collect();
        let project_picker = ComboBox::new()
            .items_source(project_names)
            .selected_index(self.project_idx)
            .placeholder_text("Project")
            .on_selection_changed(context.callback(Message::ProjectSelected));

        let agent_items: Vec<KeyedView> = self
            .agents
            .iter()
            .map(|agent| {
                KeyedView::new(
                    agent.id.clone(),
                    ListViewItem::new()
                        .content(TextBlock::new().text(format!("{} · {}", agent.id, agent.state))),
                )
            })
            .collect();
        let agent_list = ListView::new()
            .height(theme::layout::AGENT_LIST_HEIGHT)
            .selected_index(self.agent_idx)
            .on_selection_changed(context.callback(Message::AgentSelected))
            .collection_slot(ListViewSlot::Items, agent_items);

        let profile_picker = ComboBox::new()
            .items_source(self.profiles.clone())
            .selected_index(self.profile_idx)
            .placeholder_text("Profile")
            .on_selection_changed(context.callback(Message::ProfileSelected));

        let workspace_box = TextBox::new()
            .text(self.workspace.clone())
            .placeholder_text("Workspace path")
            .on_text_changed(context.callback(Message::WorkspaceChanged));

        let refresh = Button::new()
            .is_enabled(!self.busy)
            .on_click(context.message(Message::RefreshRequested))
            .content(TextBlock::new().text("Refresh"));

        let launch = Button::new()
            .resource_overrides(theme::winui::primary_button(self.dark_accent))
            .is_enabled(!self.busy && self.project_idx.is_some() && self.profile_idx.is_some())
            .on_click(context.message(Message::LaunchRequested))
            .content(TextBlock::new().text("Launch agent"));

        let nav = StackPanel::new()
            .width(theme::layout::NAV_WIDTH)
            .spacing(theme::spacing::SM)
            .children((
                TextBlock::new()
                    .text("Project")
                    .font_size(theme::type_scale::LABEL),
                project_picker,
                TextBlock::new()
                    .text("Agents")
                    .font_size(theme::type_scale::LABEL),
                agent_list,
                TextBlock::new()
                    .text("Profile")
                    .font_size(theme::type_scale::LABEL),
                profile_picker,
                TextBlock::new()
                    .text("Workspace")
                    .font_size(theme::type_scale::LABEL),
                workspace_box,
                refresh,
                launch,
            ));

        let detail_text = match (&self.detail, self.selected_agent()) {
            (Some(detail), _) => format!(
                "{} · {} · gen {} · {} · profile {} · {} · turn {}",
                self.selection.agent.clone().unwrap_or_default(),
                self.selected_agent()
                    .map(|agent| agent.project.as_str())
                    .unwrap_or("?"),
                detail.generation,
                detail.state,
                detail.profile,
                detail.effective,
                detail
                    .active_turn
                    .clone()
                    .unwrap_or_else(|| "none".to_owned()),
            ),
            (None, Some(agent)) => format!(
                "{} · {} · gen {} · {} · turn {}",
                agent.id,
                agent.project,
                agent.generation,
                agent.state,
                agent
                    .active_turn
                    .clone()
                    .unwrap_or_else(|| "none".to_owned()),
            ),
            (None, None) => "No Agent selected.".to_owned(),
        };
        let detail_line = TextBlock::new().text(detail_text);

        let rows: Vec<KeyedView> = self
            .selected_agent_id()
            .and_then(|agent| self.transcripts.get(&agent))
            .map(|transcript| transcript.messages())
            .unwrap_or_default()
            .iter()
            .map(|message: &ChatMessage| {
                let role = TextBlock::new()
                    .text(message.role.label().to_owned())
                    .font_weight(FontWeight::BOLD);
                let body = TextBlock::new()
                    .text(chat::preview(&message.text))
                    .is_text_selection_enabled(true);
                KeyedView::new(
                    message.key.clone(),
                    StackPanel::new().spacing(2.0).children((
                        role,
                        body.font_size(theme::type_scale::BODY)
                            .foreground(theme::winui::text_primary()),
                    )),
                )
            })
            .collect();
        let transcript = ScrollViewer::new()
            .height(theme::layout::TRANSCRIPT_HEIGHT)
            .content(
                StackPanel::new()
                    .spacing(theme::spacing::SM)
                    .keyed_children(rows),
            );

        let pending_rows: Vec<KeyedView> = self
            .detail
            .as_ref()
            .map(|detail| detail.pending.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|request| {
                let label = TextBlock::new()
                    .text(format!("{} · turn {}", request.kind, request.turn_id))
                    .foreground(theme::winui::text_critical());
                let approve = Button::new()
                    .on_click(context.message(Message::ApprovalRequested(request.clone(), true)))
                    .content(TextBlock::new().text("Approve"));
                let deny = Button::new()
                    .on_click(context.message(Message::ApprovalRequested(request.clone(), false)))
                    .content(TextBlock::new().text("Deny"));
                let answer_box = TextBox::new()
                    .text(
                        self.answers
                            .get(request.binding.request.as_deref().unwrap_or_default())
                            .cloned()
                            .unwrap_or_default(),
                    )
                    .placeholder_text("{\"question\": {\"answers\": [\"…\"]}}")
                    .on_text_changed({
                        let key = request.binding.request.clone().unwrap_or_default();
                        context
                            .callback(move |text: String| Message::AnswerChanged(key.clone(), text))
                    });
                let answer = Button::new()
                    .on_click(context.message(Message::AnswerRequested(request.clone())))
                    .content(TextBlock::new().text("Answer"));
                KeyedView::new(
                    request.binding.request.clone().unwrap_or_default(),
                    StackPanel::new().spacing(theme::spacing::XS).children((
                        label,
                        TextBlock::new().text(request.summary.clone()),
                        answer_box,
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(theme::spacing::XS)
                            .children((approve, deny, answer)),
                    )),
                )
            })
            .collect();
        let pending = StackPanel::new()
            .spacing(theme::spacing::XS)
            .keyed_children(pending_rows);

        let composer = TextBox::new()
            .text(self.composer_text())
            .placeholder_text("Message the selected Agent…")
            .accepts_return(true)
            .on_text_changed(context.callback(Message::ComposerChanged));

        let send = Button::new()
            .resource_overrides(theme::winui::primary_button(self.dark_accent))
            .is_enabled(!self.busy && self.agent_idx.is_some())
            .on_click(context.message(Message::SendRequested))
            .content(TextBlock::new().text("Send"));
        let interrupt = Button::new()
            .is_enabled(
                !self.busy
                    && self
                        .detail
                        .as_ref()
                        .and_then(|detail| detail.active_turn.clone())
                        .is_some(),
            )
            .on_click(context.message(Message::InterruptRequested))
            .content(TextBlock::new().text("Interrupt"));
        let actions = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::SM)
            .children((send, interrupt));

        let resident = self
            .selected_agent_id()
            .and_then(|agent| self.transcripts.get(&agent))
            .map(|transcript| transcript.len())
            .unwrap_or(0);
        let content = StackPanel::new()
            .width(theme::layout::CONTENT_WIDTH)
            .spacing(theme::spacing::SM)
            .children((
                TextBlock::new()
                    .text(format!("Conversation ({resident})"))
                    .font_size(theme::type_scale::LABEL),
                detail_line,
                transcript,
                TextBlock::new()
                    .text("Pending requests")
                    .font_size(theme::type_scale::LABEL),
                pending,
                composer,
                actions,
            ));

        let middle = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::MD)
            .children((nav, content));

        StackPanel::new()
            .spacing(theme::spacing::MD)
            .children((header, status, middle))
    }
}

/// Run the single desktop window. Blocks while the WinUI message loop runs;
/// closing the window disconnects the UI, never the Agent.
pub fn run() -> Result<(), String> {
    App::run_component::<DesktopApp>(()).map_err(|error| error.to_string())
}
