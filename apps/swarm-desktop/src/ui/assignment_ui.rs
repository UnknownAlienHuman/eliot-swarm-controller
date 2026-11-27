//! The project page's summary group (brand book §12.1): the manager, the
//! assignment and transfer state, the team and capacity; manager
//! assignment and change (§12.2, §18.2 "Замена manager") and Note to
//! project agents (§12.3).
//!
//! Reads: `chat.organization`'s group header (manager, assignment,
//! `needs_assignment`, `conflicting_roots`, `transfer`, team `totals`,
//! `capacity`) and `project.assignment.get` with each project read, whose
//! revision and epoch every mutation carries back. Mutations, each with
//! one mutation id kept until a settled reply (an uncertain outcome
//! chosen again reuses it and is never a second effect; a settled one
//! leaves the next press a fresh check, as `project.transfer` asks):
//! `project.assignment.claim` names one live root at the revision shown,
//! `.launch` starts a manager on a profile the project approves,
//! `.release` revokes the assignment at the epoch shown after the §18.1
//! confirmation; `project.transfer` begins (with the brief as saved
//! continuation context), launches a successor, commits to an exact
//! successor generation or cancels, at the assignment revision shown
//! (coordination.md "Project transfer"); `project.broadcast` queues the
//! note and returns each recipient's insertion, which is not a read.
//! Nothing here picks a manager: competing roots stay a conflict until
//! the Owner chooses one, and the daemon checks every condition again.

use super::project_ui::ProjectView;
use super::*;
use eliot_core::AgentState;
use eliot_core::api::{
    OrgGroupView, ProjectAssignment, ProjectAssignmentClaimRequest, ProjectAssignmentLaunchRequest,
    ProjectAssignmentOutcome, ProjectAssignmentReleaseRequest, ProjectAssignmentResult,
    ProjectAssignmentState, ProjectBroadcastRequest, ProjectBroadcastResponse,
    ProjectManagerIdentity, ProjectTransferAction, ProjectTransferOutcome, ProjectTransferRequest,
    ProjectTransferResult,
};

/// Where the transfer goes: an existing idle root of the project at the
/// generation shown, or a new manager launched on an approved profile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Successor {
    Existing { agent: String, generation: i64 },
    Launch(String),
}

/// One `project.transfer` action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Step {
    Begin,
    Launch,
    Commit,
    Cancel,
}

impl Step {
    fn words(self) -> &'static str {
        match self {
            Self::Begin => "Begin transfer",
            Self::Launch => "Launch successor",
            Self::Commit => "Commit transfer",
            Self::Cancel => "Cancel transfer",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum AssignMsg {
    /// `project.assignment.get` for a project.
    Loaded(String, Box<Fallible<ProjectAssignment>>),
    /// Name this root (its id and generation shown) the project's manager.
    Claim(String, i64),
    LaunchOpened,
    ProfilePicked(Option<usize>),
    LaunchConfirmed,
    ReleaseOpened,
    ReleaseConfirmed,
    /// An assignment mutation settled: the project, its ledger key, words.
    Settled(
        String,
        String,
        String,
        Box<MutationResult<ProjectAssignmentResult>>,
    ),
    TransferOpened,
    SuccessorPicked(Option<usize>),
    BriefChanged(String),
    Transfer(Step),
    /// A transfer action settled: the project, its ledger key, the step.
    TransferSettled(
        String,
        String,
        Step,
        Box<MutationResult<ProjectTransferResult>>,
    ),
    NoteOpened,
    NoteChanged(String),
    NoteSend,
    /// The note settled: the project, its ledger key.
    NoteSent(
        String,
        String,
        Box<MutationResult<ProjectBroadcastResponse>>,
    ),
    Closed,
}

/// The open project dialog.
pub(super) enum Dialog {
    Launch {
        profile: Option<usize>,
    },
    Release,
    Transfer {
        successor: Option<usize>,
        brief: String,
    },
    Note {
        text: String,
    },
}

#[derive(Default)]
pub(super) struct AssignState {
    /// `project.assignment.get` by project, as last read.
    read: HashMap<String, Fallible<ProjectAssignment>>,
    reading: HashSet<String>,
    pub(super) dialog: Option<Dialog>,
    /// One mutation id per action, kept after an uncertain outcome.
    ids: HashMap<String, String>,
    /// Actions running (their ledger keys).
    running: HashSet<String>,
    /// The last assignment or transfer outcome by project (words, error).
    outcomes: HashMap<String, (String, bool)>,
    /// The last transfer result by project.
    transfers: HashMap<String, ProjectTransferResult>,
    /// The last note result by project, or its error.
    notes: HashMap<String, Result<ProjectBroadcastResponse, String>>,
}

/// The project page's summary group, read by the page.
#[derive(Clone, PartialEq)]
pub(super) struct ManagerView {
    agent: Option<ManagerLine>,
    /// A remote manager's identity in words.
    remote: Option<String>,
    /// The assignment's state when it is not simply active.
    state: Option<String>,
    start_command: Option<String>,
    needs: bool,
    /// The open transfer, its stage and blockers in words.
    transfer: Option<String>,
    blockers: Vec<String>,
    competing: Vec<String>,
    candidates: Vec<Candidate>,
    can_launch: bool,
    can_release: bool,
    can_transfer: bool,
    busy: bool,
    outcome: Option<(String, bool)>,
    team: Option<String>,
    capacity: Option<CapacityView>,
    read_error: Option<String>,
}

#[derive(Clone, PartialEq)]
struct ManagerLine {
    agent: Option<UiAgent>,
    title: String,
    state: AgentState,
    words: String,
}

/// A root the Owner can name the manager.
#[derive(Clone, PartialEq)]
struct Candidate {
    agent: String,
    generation: i64,
    title: String,
    words: String,
}

#[derive(Clone, PartialEq)]
struct CapacityView {
    scope: String,
    usage: usize,
    limit: Option<u64>,
    live: String,
}

/// A transfer outcome in words, with its successor's title.
fn stage_words(result: &ProjectTransferResult, title: impl Fn(&str) -> String) -> String {
    let destination = result.destination.as_ref().map(|identity| match identity {
        ProjectManagerIdentity::Local { agent, generation } => {
            format!("{} (run {generation})", title(agent))
        }
        ProjectManagerIdentity::Remote {
            instance,
            grant_revision,
        } => format!("remote manager {instance} (grant revision {grant_revision})"),
    });
    match (result.outcome, destination) {
        (ProjectTransferOutcome::Draining, _) => "Draining the current manager".to_owned(),
        (ProjectTransferOutcome::LaunchAdmitted, _) => match &result.launch_command {
            Some(command) => format!("Successor launch admitted ({command})"),
            None => "Successor launch admitted".to_owned(),
        },
        (ProjectTransferOutcome::DestinationReady, Some(to)) => format!("Successor ready: {to}"),
        (ProjectTransferOutcome::DestinationReady, None) => "Successor ready".to_owned(),
        (ProjectTransferOutcome::WaitingCapacity, _) => "Waiting for capacity".to_owned(),
        (ProjectTransferOutcome::Blocked, _) => "Blocked".to_owned(),
        (ProjectTransferOutcome::Transferred, Some(to)) => format!("Transferred to {to}"),
        (ProjectTransferOutcome::Transferred, None) => "Transferred".to_owned(),
        (ProjectTransferOutcome::Cancelled, _) => "Transfer cancelled".to_owned(),
    }
}

/// Whether a transfer outcome leaves no open intent.
fn finished(outcome: ProjectTransferOutcome) -> bool {
    matches!(
        outcome,
        ProjectTransferOutcome::Transferred | ProjectTransferOutcome::Cancelled
    )
}

/// One identity in words.
fn identity_words(identity: &ProjectManagerIdentity, title: impl Fn(&str) -> String) -> String {
    match identity {
        ProjectManagerIdentity::Local { agent, generation } => {
            format!("{} (run {generation})", title(agent))
        }
        ProjectManagerIdentity::Remote {
            instance,
            grant_revision,
        } => format!("Remote manager {instance} (grant revision {grant_revision})"),
    }
}

/// An assignment outcome in words.
fn assignment_words(result: &ProjectAssignmentResult) -> (String, bool) {
    match result.outcome {
        ProjectAssignmentOutcome::Claimed => ("Manager assigned.".to_owned(), false),
        ProjectAssignmentOutcome::LaunchAdmitted => (
            match &result.command_id {
                Some(command) => format!("Manager launch admitted ({command})."),
                None => "Manager launch admitted.".to_owned(),
            },
            false,
        ),
        ProjectAssignmentOutcome::Released => ("Manager released.".to_owned(), false),
        ProjectAssignmentOutcome::Occupied => (
            "Another manager holds this project now; the page shows it.".to_owned(),
            true,
        ),
        ProjectAssignmentOutcome::StaleEpoch => (
            "The assignment changed since it was read; the page reads it again.".to_owned(),
            true,
        ),
        ProjectAssignmentOutcome::Vacant => ("This project has no manager.".to_owned(), true),
        ProjectAssignmentOutcome::StartupUncertain => (
            "The manager's start is not confirmed; its command stays readable.".to_owned(),
            true,
        ),
    }
}

impl DesktopApp {
    pub(super) fn assign_update(&mut self, msg: AssignMsg, context: &ComponentContext<Self>) {
        let project = self.project.id.clone();
        match msg {
            AssignMsg::Loaded(id, read) => {
                self.assignment.reading.remove(&id);
                self.assignment.read.insert(id, *read);
            }
            AssignMsg::Claim(agent, generation) => {
                let revision = self.assignment_revision(&project).unwrap_or(0);
                let key = format!("{project}:claim:{agent}:{generation}:{revision}");
                let request = |id: String| {
                    client::AssignmentChange::Claim(ProjectAssignmentClaimRequest {
                        id,
                        project: project.clone(),
                        manager: Some(ProjectManagerIdentity::Local {
                            agent: agent.clone(),
                            generation,
                        }),
                        expected_revision: Some(revision),
                    })
                };
                let words = format!("Assign {}", self.agent_title(&agent));
                self.run_assignment(key, words, request, context);
            }
            AssignMsg::LaunchOpened => {
                self.assignment.dialog = Some(Dialog::Launch { profile: None });
            }
            AssignMsg::ProfilePicked(index) => {
                if let Some(Dialog::Launch { profile }) = &mut self.assignment.dialog {
                    *profile = index;
                }
            }
            AssignMsg::LaunchConfirmed => {
                let profiles = self.project_profiles(&project);
                let Some(Dialog::Launch {
                    profile: Some(index),
                }) = self.assignment.dialog
                else {
                    return;
                };
                let Some(profile) = profiles.get(index).cloned() else {
                    return;
                };
                self.assignment.dialog = None;
                let key = format!("{project}:launch:{profile}");
                let request = |id: String| {
                    client::AssignmentChange::Launch(ProjectAssignmentLaunchRequest {
                        id,
                        project: project.clone(),
                        profile: profile.clone(),
                        adopt_worktree: None,
                        no_policy: None,
                        goal: None,
                    })
                };
                self.run_assignment(
                    key,
                    format!("Launch manager on {profile}"),
                    request,
                    context,
                );
            }
            AssignMsg::ReleaseOpened => self.assignment.dialog = Some(Dialog::Release),
            AssignMsg::ReleaseConfirmed => {
                self.assignment.dialog = None;
                let Some(epoch) = self.assignment_epoch(&project) else {
                    return;
                };
                let key = format!("{project}:release:{epoch}");
                let request = |id: String| {
                    client::AssignmentChange::Release(ProjectAssignmentReleaseRequest {
                        id,
                        project: project.clone(),
                        expected_epoch: epoch,
                    })
                };
                self.run_assignment(key, "Release".to_owned(), request, context);
            }
            AssignMsg::Settled(id, key, words, result) => {
                self.assignment.running.remove(&key);
                let outcome = match *result {
                    Ok(reply) => {
                        self.assignment.ids.remove(&key);
                        self.assignment
                            .read
                            .insert(id.clone(), Ok(reply.assignment.clone()));
                        assignment_words(&reply)
                    }
                    Err(error) if crate::state::uncertain(&error) => (
                        format!(
                            "{words}: not confirmed ({}). Choosing it again retries the same request.",
                            error.user_message()
                        ),
                        true,
                    ),
                    Err(error) => {
                        self.assignment.ids.remove(&key);
                        (format!("{words}: {}", error.user_message()), true)
                    }
                };
                self.assignment.outcomes.insert(id, outcome);
                self.reread_assignment(context);
            }
            AssignMsg::TransferOpened => {
                self.assignment.dialog = Some(Dialog::Transfer {
                    successor: None,
                    brief: String::new(),
                });
            }
            AssignMsg::SuccessorPicked(index) => {
                if let Some(Dialog::Transfer { successor, .. }) = &mut self.assignment.dialog {
                    *successor = index;
                }
            }
            AssignMsg::BriefChanged(text) => {
                if let Some(Dialog::Transfer { brief, .. }) = &mut self.assignment.dialog
                    && *brief != text
                {
                    *brief = text;
                }
            }
            AssignMsg::Transfer(step) => self.run_transfer(step, context),
            AssignMsg::TransferSettled(id, key, step, result) => {
                self.assignment.running.remove(&key);
                let outcome = match *result {
                    Ok(reply) => {
                        self.assignment.ids.remove(&key);
                        let words = stage_words(&reply, |agent| self.agent_title(agent));
                        self.assignment
                            .read
                            .insert(id.clone(), Ok(reply.assignment.clone()));
                        self.assignment.transfers.insert(id.clone(), reply);
                        (format!("{}: {words}", step.words()), false)
                    }
                    Err(error) if crate::state::uncertain(&error) => (
                        format!(
                            "{}: not confirmed ({}). Choosing it again retries the same request.",
                            step.words(),
                            error.user_message()
                        ),
                        true,
                    ),
                    Err(error) => {
                        self.assignment.ids.remove(&key);
                        (format!("{}: {}", step.words(), error.user_message()), true)
                    }
                };
                self.assignment.outcomes.insert(id, outcome);
                self.reread_assignment(context);
            }
            AssignMsg::NoteOpened => {
                self.assignment.notes.remove(&project);
                self.assignment.dialog = Some(Dialog::Note {
                    text: String::new(),
                });
            }
            AssignMsg::NoteChanged(value) => {
                if let Some(Dialog::Note { text }) = &mut self.assignment.dialog
                    && *text != value
                {
                    *text = value;
                }
            }
            AssignMsg::NoteSend => {
                let Some(Dialog::Note { text }) = &self.assignment.dialog else {
                    return;
                };
                let text = text.clone();
                if text.trim().is_empty() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let key = format!("{project}:note:{}", note_digest(&text));
                if !self.assignment.running.insert(key.clone()) {
                    return;
                }
                let id = self
                    .assignment
                    .ids
                    .entry(key.clone())
                    .or_insert_with(client::new_mutation_id)
                    .clone();
                self.assignment.notes.remove(&project);
                let _task = context.spawn_background(move |_| {
                    let request = ProjectBroadcastRequest {
                        id,
                        project: project.clone(),
                        text,
                    };
                    let result = client::block_on(client::project_broadcast(&data_dir, &request));
                    Message::Project(ProjectMsg::Assign(AssignMsg::NoteSent(
                        project,
                        key,
                        Box::new(result),
                    )))
                });
            }
            AssignMsg::NoteSent(id, key, result) => {
                self.assignment.running.remove(&key);
                let note = match *result {
                    Ok(reply) => {
                        self.assignment.ids.remove(&key);
                        Ok(reply)
                    }
                    Err(error) if crate::state::uncertain(&error) => Err(format!(
                        "Not confirmed ({}). Sending it again retries the same note.",
                        error.user_message()
                    )),
                    Err(error) => {
                        self.assignment.ids.remove(&key);
                        Err(error.user_message())
                    }
                };
                self.assignment.notes.insert(id, note);
            }
            AssignMsg::Closed => self.assignment.dialog = None,
        }
    }

    /// Read the open project's assignment (`project.assignment.get`).
    pub(super) fn read_assignment(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let project = self.project.id.clone();
        if project.is_empty() || !self.assignment.reading.insert(project.clone()) {
            return;
        }
        let _task = context.spawn_background(move |_| {
            let read = client::block_on(client::assignment_get(&data_dir, &project))
                .map_err(|error| error.user_message());
            Message::Project(ProjectMsg::Assign(AssignMsg::Loaded(
                project,
                Box::new(read),
            )))
        });
    }

    /// After a mutation: the assignment, the project's groups and the
    /// sidebar's organization (its header carries the transfer and team).
    fn reread_assignment(&mut self, context: &ComponentContext<Self>) {
        self.read_assignment(context);
        self.read_project(context);
        self.load_organization(context);
    }

    /// The project's organization group (its `chat.organization` header).
    fn org_group(&self, project: &str) -> Option<&OrgGroupView> {
        self.nav
            .organization
            .groups
            .iter()
            .find(|group| group.project.as_deref() == Some(project))
    }

    /// The assignment as last read: `project.assignment.get`, else the
    /// organization header's.
    fn assignment_of(&self, project: &str) -> Option<&ProjectAssignment> {
        match self.assignment.read.get(project) {
            Some(Ok(assignment)) => Some(assignment),
            _ => self
                .org_group(project)
                .and_then(|group| group.assignment.as_ref()),
        }
    }

    fn assignment_revision(&self, project: &str) -> Option<i64> {
        self.assignment_of(project)
            .map(|assignment| assignment.revision)
    }

    fn assignment_epoch(&self, project: &str) -> Option<i64> {
        self.assignment_of(project)
            .map(|assignment| assignment.epoch)
    }

    /// The profiles the project approves (its registration's allowlist).
    fn project_profiles(&self, project: &str) -> Vec<String> {
        self.nav
            .projects
            .iter()
            .find(|registered| registered.id == project)
            .map(|registered| registered.profiles.clone())
            .unwrap_or_default()
    }

    /// The open transfer intent: the header's, else the last result's
    /// while it is not finished.
    fn open_intent(&self, project: &str) -> Option<String> {
        self.org_group(project)
            .and_then(|group| group.transfer.clone())
            .or_else(|| {
                self.assignment
                    .transfers
                    .get(project)
                    .filter(|result| !finished(result.outcome))
                    .map(|result| result.intent.clone())
            })
    }

    /// Root Agents of the project the daemon may accept as its manager:
    /// listed, controlled, unarchived, not a chat, in a live state
    /// (`manager_live`), with no parent.
    fn manager_candidates(&self, project: &str, states: &[AgentState]) -> Vec<&UiAgent> {
        let mut seen = HashSet::new();
        self.project_task_rows()
            .chain(self.nav.agents.iter())
            .filter(|agent| seen.insert(agent.id.as_str()))
            .filter(|agent| {
                agent.project_id == project
                    && agent.parent.is_none()
                    && !agent.native_child
                    && !agent.chat
                    && !agent.archived
                    && !agent.pending
                    && states.contains(&agent.state)
            })
            .collect()
    }

    /// The successors the transfer dialog offers: idle roots other than
    /// the current manager, then a new manager on each approved profile.
    fn successors(&self, project: &str) -> Vec<(String, Successor)> {
        let source = match self.assignment_of(project).and_then(|a| a.manager.as_ref()) {
            Some(ProjectManagerIdentity::Local { agent, .. }) => Some(agent.clone()),
            _ => None,
        };
        let mut options: Vec<(String, Successor)> = self
            .manager_candidates(project, &[AgentState::Idle])
            .into_iter()
            .filter(|agent| Some(&agent.id) != source.as_ref())
            .map(|agent| {
                (
                    format!(
                        "{} · {}",
                        self.agent_title(&agent.id),
                        crate::picker::harness_model_label(&agent.harness, agent.model.as_deref())
                    ),
                    Successor::Existing {
                        agent: agent.id.clone(),
                        generation: agent.generation,
                    },
                )
            })
            .collect();
        options.extend(self.project_profiles(project).into_iter().map(|profile| {
            (
                format!("Launch a new manager on {profile}"),
                Successor::Launch(profile),
            )
        }));
        options
    }

    /// Run one assignment mutation under its ledger key.
    fn run_assignment(
        &mut self,
        key: String,
        words: String,
        request: impl FnOnce(String) -> client::AssignmentChange,
        context: &ComponentContext<Self>,
    ) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if !self.assignment.running.insert(key.clone()) {
            return;
        }
        let project = self.project.id.clone();
        self.assignment.outcomes.remove(&project);
        let id = self
            .assignment
            .ids
            .entry(key.clone())
            .or_insert_with(client::new_mutation_id)
            .clone();
        let change = request(id);
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(client::assignment_change(&data_dir, &change));
            Message::Project(ProjectMsg::Assign(AssignMsg::Settled(
                project,
                key,
                words,
                Box::new(result),
            )))
        });
    }

    /// Run one transfer action from the dialog's choices.
    fn run_transfer(&mut self, step: Step, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let project = self.project.id.clone();
        let Some(Dialog::Transfer { successor, brief }) = &self.assignment.dialog else {
            return;
        };
        let options = self.successors(&project);
        let chosen = successor.and_then(|index| options.get(index).map(|(_, s)| s.clone()));
        let brief = brief.trim().to_owned();
        let Some(revision) = self.assignment_revision(&project).filter(|r| *r > 0) else {
            return;
        };
        let intent = self.open_intent(&project);
        let ready = self
            .assignment
            .transfers
            .get(&project)
            .filter(|result| result.outcome == ProjectTransferOutcome::DestinationReady)
            .filter(|result| Some(&result.intent) == intent.as_ref())
            .and_then(|result| result.destination.clone());
        let (action, detail) = match (step, intent, chosen) {
            (Step::Begin, None, Some(_)) => (
                ProjectTransferAction::Begin {
                    saved_context: (!brief.is_empty()).then(|| brief.clone()),
                },
                note_digest(&brief).to_string(),
            ),
            (Step::Launch, Some(intent), Some(Successor::Launch(profile))) => (
                ProjectTransferAction::Launch {
                    intent: intent.clone(),
                    profile: profile.clone(),
                },
                format!("{intent}:{profile}"),
            ),
            (Step::Commit, Some(intent), Some(Successor::Existing { agent, generation })) => (
                ProjectTransferAction::Commit {
                    intent: intent.clone(),
                    destination: ProjectManagerIdentity::Local {
                        agent: agent.clone(),
                        generation,
                    },
                    task_command: None,
                    commands: Vec::new(),
                },
                format!("{intent}:{agent}:{generation}"),
            ),
            (Step::Commit, Some(intent), Some(Successor::Launch(_))) => {
                let Some(destination) = ready else {
                    return;
                };
                let detail = format!("{intent}:{destination:?}");
                (
                    ProjectTransferAction::Commit {
                        intent,
                        destination,
                        task_command: None,
                        commands: Vec::new(),
                    },
                    detail,
                )
            }
            (Step::Cancel, Some(intent), _) => (
                ProjectTransferAction::Cancel {
                    intent: intent.clone(),
                },
                intent,
            ),
            _ => return,
        };
        let key = format!("{project}:transfer:{step:?}:{revision}:{detail}");
        if !self.assignment.running.insert(key.clone()) {
            return;
        }
        self.assignment.outcomes.remove(&project);
        let id = self
            .assignment
            .ids
            .entry(key.clone())
            .or_insert_with(client::new_mutation_id)
            .clone();
        let request = ProjectTransferRequest {
            id,
            project: project.clone(),
            expected_revision: revision,
            action,
        };
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(client::project_transfer(&data_dir, &request));
            Message::Project(ProjectMsg::Assign(AssignMsg::TransferSettled(
                project,
                key,
                step,
                Box::new(result),
            )))
        });
    }

    /// Whether any assignment or transfer action of `project` runs.
    fn assignment_busy(&self, project: &str) -> bool {
        let prefix = format!("{project}:");
        self.assignment
            .running
            .iter()
            .any(|key| key.starts_with(&prefix) && !key[prefix.len()..].starts_with("note:"))
    }

    /// The summary group the project page shows (§12.1): the manager or
    /// "Choose a manager" with its candidates, the assignment and transfer
    /// state, the team and capacity.
    pub(super) fn manager_view(&self, live: Vec<String>) -> Option<ManagerView> {
        let project = self.project.id.as_str();
        let group = self.org_group(project)?;
        let title = |agent: &str| self.agent_title(agent);
        let assignment = self.assignment_of(project);
        let agent = group.manager.as_ref().map(|manager| {
            let row = self.find_agent(&manager.agent);
            ManagerLine {
                agent: row.cloned(),
                title: self.agent_title(&manager.agent),
                state: manager.state,
                words: [
                    row.map(|row| {
                        crate::picker::harness_model_label(&row.harness, row.model.as_deref())
                    }),
                    manager.branch.clone(),
                    Some(crate::monitor::state_words(manager.state).to_owned()),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" · "),
            }
        });
        let remote = assignment
            .and_then(|assignment| assignment.manager.as_ref())
            .filter(|identity| matches!(identity, ProjectManagerIdentity::Remote { .. }))
            .map(|identity| identity_words(identity, title));
        let state = assignment.and_then(|assignment| match assignment.state {
            ProjectAssignmentState::Active | ProjectAssignmentState::Vacant => None,
            ProjectAssignmentState::Starting => Some("Manager starting".to_owned()),
            ProjectAssignmentState::Blocked => {
                Some("Manager preparation outcome unknown".to_owned())
            }
        });
        let occupied = assignment.is_some_and(|assignment| assignment.manager.is_some())
            || group.manager.is_some();
        let intent = self.open_intent(project);
        let last = self.assignment.transfers.get(project);
        let shown = last.filter(|result| {
            Some(&result.intent) == intent.as_ref()
                || (intent.is_none() && finished(result.outcome))
        });
        let transfer = match (&intent, shown) {
            (_, Some(result)) => Some(stage_words(result, title)),
            (Some(_), None) => Some("Transfer in progress".to_owned()),
            (None, None) => None,
        };
        let blockers = shown
            .map(|result| {
                result
                    .blockers
                    .iter()
                    .map(|blocker| match &blocker.subject {
                        Some(subject) if subject.starts_with('a') => format!(
                            "{} · {}",
                            eliot_core::reason_words(&blocker.code),
                            self.agent_title(subject)
                        ),
                        Some(subject) => {
                            format!("{} · {subject}", eliot_core::reason_words(&blocker.code))
                        }
                        None => eliot_core::reason_words(&blocker.code),
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Competing roots: with a manager they compete with it; without one
        // they are the candidates, and the Owner chooses (§12.2).
        let candidates: Vec<Candidate> = if occupied || intent.is_some() {
            Vec::new()
        } else if !group.conflicting_roots.is_empty() {
            group
                .conflicting_roots
                .iter()
                .filter_map(|id| {
                    let row = self.find_agent(id)?;
                    Some(Candidate {
                        agent: id.clone(),
                        generation: row.generation,
                        title: self.agent_title(id),
                        words: crate::monitor::state_words(row.state).to_owned(),
                    })
                })
                .collect()
        } else {
            self.manager_candidates(
                project,
                &[AgentState::Idle, AgentState::Busy, AgentState::Waiting],
            )
            .into_iter()
            .map(|agent| Candidate {
                agent: agent.id.clone(),
                generation: agent.generation,
                title: self.agent_title(&agent.id),
                words: format!(
                    "{} · {}",
                    crate::picker::harness_model_label(&agent.harness, agent.model.as_deref()),
                    crate::monitor::state_words(agent.state)
                ),
            })
            .collect()
        };
        let team = group.totals.as_ref().map(|totals| {
            [
                eliot_core::count_words(totals.live as u64, "live Agent", "live Agents"),
                eliot_core::count_words(
                    totals.active_turns as u64,
                    "running turn",
                    "running turns",
                ),
                eliot_core::count_words(totals.queued as u64, "queued command", "queued commands"),
                format!(
                    "{} retained after lost, stopped or failed",
                    totals.retained_unknown
                ),
            ]
            .join(" · ")
        });
        let capacity = group.capacity.as_ref().map(|scope| CapacityView {
            scope: scope.scope.clone(),
            usage: scope.usage,
            limit: scope.limit,
            live: live.join(", "),
        });
        let busy = self.assignment_busy(project);
        let active =
            assignment.is_some_and(|assignment| assignment.state == ProjectAssignmentState::Active);
        Some(ManagerView {
            agent,
            remote,
            state,
            start_command: assignment
                .filter(|assignment| assignment.state != ProjectAssignmentState::Active)
                .and_then(|assignment| assignment.start_command_id.clone()),
            needs: group.needs_assignment,
            transfer,
            blockers,
            competing: if occupied {
                group
                    .conflicting_roots
                    .iter()
                    .map(|id| self.agent_title(id))
                    .collect()
            } else {
                Vec::new()
            },
            candidates,
            can_launch: !occupied
                && intent.is_none()
                && group.conflicting_roots.is_empty()
                && !self.project_profiles(project).is_empty(),
            can_release: occupied && intent.is_none(),
            can_transfer: occupied && (active || intent.is_some()),
            busy,
            outcome: self.assignment.outcomes.get(project).cloned(),
            team,
            capacity,
            read_error: match self.assignment.read.get(project) {
                Some(Err(error)) => Some(error.clone()),
                _ => None,
            },
        })
    }

    /// The open project dialog (§18.1).
    pub(super) fn assignment_dialog(
        &self,
        context: &mut ViewContext<Self>,
    ) -> Option<ContentDialog> {
        let dialog = self.assignment.dialog.as_ref()?;
        let project = self.project.id.as_str();
        let name = self.place_title(&self.project.checkout);
        let busy = self.assignment_busy(project);
        let close = |label: &str, context: &mut ViewContext<Self>| -> View {
            controls::text_button()
                .element_ref(&self.dialog.close)
                .automation_name(label)
                .on_click(context.message(Message::Project(ProjectMsg::Assign(AssignMsg::Closed))))
                .content(controls::label(label))
                .into()
        };
        let (title, body, footer, width): (String, View, View, f64) = match dialog {
            Dialog::Launch { profile } => {
                let profiles = self.project_profiles(project);
                let body = StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(vec![
                        KeyedView::new(
                            "what",
                            controls::body(
                                "Swarm reserves the project's manager assignment, prepares a fresh worktree from the project's local HEAD and starts the manager there.",
                            )
                            .text_wrapping(TextWrapping::Wrap),
                        ),
                        KeyedView::new(
                            format!("profiles:{}", profiles.join("\u{1f}")),
                            ComboBox::new()
                                .items_source(profiles.clone())
                                .selected_index(*profile)
                                .placeholder_text("Profile")
                                .automation_name("Manager profile")
                                .horizontal_alignment(HorizontalAlignment::Stretch)
                                .on_selection_changed(context.callback(|index| {
                                    Message::Project(ProjectMsg::Assign(AssignMsg::ProfilePicked(
                                        index,
                                    )))
                                })),
                        ),
                        KeyedView::new(
                            "approved",
                            controls::secondary(
                                "Only profiles this project approves are listed (Settings › Projects).",
                            )
                            .text_wrapping(TextWrapping::Wrap),
                        ),
                    ])
                    .into();
                let footer = dialog_ui::actions(
                    close("Cancel", context),
                    View::from(
                        controls::filled()
                            .element_ref(&self.dialog.primary)
                            .is_enabled(profile.is_some() && !busy)
                            .automation_name("Launch manager")
                            .on_click(context.message(Message::Project(ProjectMsg::Assign(
                                AssignMsg::LaunchConfirmed,
                            ))))
                            .content(controls::label("Launch manager")),
                    ),
                );
                (
                    format!("Launch a manager for {name}"),
                    body,
                    footer,
                    tokens::layout::DIALOG_WIDTH,
                )
            }
            Dialog::Release => {
                let manager = self
                    .assignment_of(project)
                    .and_then(|assignment| assignment.manager.as_ref())
                    .map(|identity| identity_words(identity, |agent| self.agent_title(agent)))
                    .unwrap_or_else(|| "The manager".to_owned());
                let body = StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .children((
                        controls::body(format!("{manager} stops being the manager of {name}."))
                            .text_wrapping(TextWrapping::Wrap),
                        controls::body(
                            "Swarm authority over its work ends; the running session keeps its own lifecycle. A start that is not settled refuses the release.",
                        )
                        .text_wrapping(TextWrapping::Wrap),
                    ))
                    .into();
                let footer = dialog_ui::actions(
                    close("Cancel", context),
                    View::from(
                        controls::destructive()
                            .element_ref(&self.dialog.primary)
                            .is_enabled(!busy)
                            .automation_name(format!("Release {manager}"))
                            .on_click(context.message(Message::Project(ProjectMsg::Assign(
                                AssignMsg::ReleaseConfirmed,
                            ))))
                            .content(controls::label("Release")),
                    ),
                );
                (
                    "Release the manager?".to_owned(),
                    body,
                    footer,
                    tokens::layout::DIALOG_WIDTH,
                )
            }
            Dialog::Transfer { successor, brief } => {
                return Some(self.transfer_dialog(*successor, brief, busy, context));
            }
            Dialog::Note { text } => {
                let key = format!("{project}:note:{}", note_digest(text));
                let sending = self.assignment.running.contains(&key);
                let working: Vec<String> = self
                    .project_task_rows()
                    .filter(|agent| {
                        matches!(
                            agent.state,
                            AgentState::Busy | AgentState::Starting | AgentState::Created
                        ) && !agent.native_child
                    })
                    .map(|agent| self.agent_title(&agent.id))
                    .collect();
                let mut rows = vec![
                    KeyedView::new(
                        "recipients",
                        page_ui::kv_text(
                            "To",
                            if working.is_empty() {
                                "The project's other working Agents when you send (none listed now)"
                                    .to_owned()
                            } else {
                                format!(
                                    "The project's other working Agents when you send · now {}",
                                    working.join(", ")
                                )
                            },
                        ),
                    ),
                    KeyedView::new(
                        "text",
                        controls::field()
                            .text(text.clone())
                            .accepts_return(true)
                            .text_wrapping(TextWrapping::Wrap)
                            .min_height(tokens::layout::GOAL_TEXT_MIN_HEIGHT)
                            .is_enabled(!sending)
                            .placeholder_text("Note")
                            .automation_name("Note to project agents")
                            .on_text_changed(crate::views::text_edits(context.callback(|text| {
                                Message::Project(ProjectMsg::Assign(AssignMsg::NoteChanged(text)))
                            }))),
                    ),
                ];
                match self.assignment.notes.get(project) {
                    Some(Ok(reply)) => {
                        rows.push(KeyedView::new(
                            "sent",
                            controls::body(if reply.recipients.is_empty() {
                                "No other Agent was working: nothing was queued.".to_owned()
                            } else {
                                format!(
                                    "Queued for {}. Queued is not read: each Agent takes it at its next step.",
                                    eliot_core::count_words(
                                        reply.recipients.len() as u64,
                                        "Agent",
                                        "Agents"
                                    )
                                )
                            })
                            .text_wrapping(TextWrapping::Wrap),
                        ));
                        for recipient in &reply.recipients {
                            rows.push(KeyedView::new(
                                format!("to:{}", recipient.agent),
                                controls::secondary(format!(
                                    "{} (run {}) · {} · {}",
                                    self.agent_title(&recipient.agent),
                                    recipient.generation,
                                    eliot_core::reason_words(
                                        &format!("{:?}", recipient.state).to_lowercase()
                                    ),
                                    recipient.command_id
                                ))
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true),
                            ));
                        }
                    }
                    Some(Err(error)) => rows.push(KeyedView::new(
                        "error",
                        controls::body(error.clone())
                            .foreground(ThemeBrush::SystemCritical)
                            .text_wrapping(TextWrapping::Wrap),
                    )),
                    None => {}
                }
                let sent = matches!(self.assignment.notes.get(project), Some(Ok(_)));
                let footer = dialog_ui::actions(
                    close(if sent { "Close" } else { "Cancel" }, context),
                    (!sent).then(|| {
                        View::from(
                            controls::filled()
                                .element_ref(&self.dialog.primary)
                                .is_enabled(!sending && !text.trim().is_empty())
                                .automation_name("Send note")
                                .on_click(context.message(Message::Project(ProjectMsg::Assign(
                                    AssignMsg::NoteSend,
                                ))))
                                .content(controls::label(if sending {
                                    "Sending…"
                                } else {
                                    "Send"
                                })),
                        )
                    }),
                );
                (
                    "Note to project agents".to_owned(),
                    StackPanel::new()
                        .spacing(tokens::spacing::SM)
                        .keyed_children(rows)
                        .into(),
                    footer,
                    tokens::layout::GOAL_DIALOG_WIDTH,
                )
            }
        };
        Some(dialog_ui::surface(
            context,
            self.dialog.epoch,
            title,
            body,
            footer,
            width,
        ))
    }

    /// Transfer (§12.2): source, successor, workspace, brief and the
    /// conditions; once begun, the stage and blockers the daemon returned
    /// and the next action for the chosen successor.
    fn transfer_dialog(
        &self,
        successor: Option<usize>,
        brief: &str,
        busy: bool,
        context: &mut ViewContext<Self>,
    ) -> ContentDialog {
        let project = self.project.id.as_str();
        let name = self.place_title(&self.project.checkout);
        let assignment = self.assignment_of(project);
        let source = assignment
            .and_then(|assignment| assignment.manager.as_ref())
            .map(|identity| identity_words(identity, |agent| self.agent_title(agent)))
            .unwrap_or_else(|| "No manager".to_owned());
        let workspace = assignment
            .and_then(|assignment| assignment.workspace.clone())
            .or_else(|| {
                self.org_group(project)
                    .and_then(|group| group.manager.as_ref())
                    .and_then(|manager| manager.workspace.clone())
            });
        let branch = self
            .org_group(project)
            .and_then(|group| group.manager.as_ref())
            .and_then(|manager| manager.branch.clone());
        let options = self.successors(project);
        let chosen = successor.and_then(|index| options.get(index).map(|(_, s)| s.clone()));
        let intent = self.open_intent(project);
        let last = self.assignment.transfers.get(project).filter(|result| {
            Some(&result.intent) == intent.as_ref()
                || (intent.is_none() && finished(result.outcome))
        });
        let done = intent.is_none() && last.is_some();
        let mut rows: Vec<KeyedView> = vec![
            KeyedView::new("from", page_ui::kv_text("From", source)),
            KeyedView::new(
                "workspace",
                page_ui::kv_text(
                    "Workspace",
                    match (&workspace, &branch) {
                        (Some(path), Some(branch)) => {
                            format!("{branch} · {}", nav::display_path(path))
                        }
                        (Some(path), None) => nav::display_path(path),
                        _ => "Not bound".to_owned(),
                    },
                ),
            ),
        ];
        if !done {
            let labels: Vec<String> = options.iter().map(|(label, _)| label.clone()).collect();
            rows.push(KeyedView::new(
                format!("successor:{}", labels.join("\u{1f}")),
                ComboBox::new()
                    .items_source(labels)
                    .selected_index(successor)
                    .is_enabled(!busy)
                    .placeholder_text("Successor")
                    .automation_name("Successor")
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .on_selection_changed(context.callback(|index| {
                        Message::Project(ProjectMsg::Assign(AssignMsg::SuccessorPicked(index)))
                    })),
            ));
            if options.is_empty() {
                rows.push(KeyedView::new(
                    "no-successor",
                    controls::secondary(
                        "No idle root Agent of this project and no approved profile to launch one.",
                    )
                    .text_wrapping(TextWrapping::Wrap),
                ));
            }
        }
        if intent.is_none() && !done {
            rows.push(KeyedView::new(
                "brief",
                controls::field()
                    .text(brief)
                    .accepts_return(true)
                    .text_wrapping(TextWrapping::Wrap)
                    .min_height(tokens::layout::GOAL_TEXT_MIN_HEIGHT)
                    .is_enabled(!busy)
                    .placeholder_text("Brief for the successor")
                    .automation_name("Brief for the successor")
                    .on_text_changed(crate::views::text_edits(context.callback(|text| {
                        Message::Project(ProjectMsg::Assign(AssignMsg::BriefChanged(text)))
                    }))),
            ));
            rows.push(KeyedView::new(
                "conditions",
                controls::secondary(
                    "Begin drains the manager: new controls, queued launches and automated work for it wait, while replies, interrupt and stop stay available. A new manager needs the brief and the project's launch action, and starts only after the current manager's processes have exited. Commit moves the workspace binding, workers, claims and a paused Goal to the successor in one step; blockers stop it and are listed here.",
                )
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        if let Some(result) = last {
            rows.push(KeyedView::new(
                format!("stage:{:?}", result.outcome),
                page_ui::kv_text(
                    "Stage",
                    stage_words(result, |agent| self.agent_title(agent)),
                ),
            ));
            if let Some(context_text) = &result.saved_context {
                rows.push(KeyedView::new(
                    "saved",
                    page_ui::kv_text("Brief", context_text.clone()),
                ));
            }
            for (index, blocker) in result.blockers.iter().enumerate() {
                let subject = blocker
                    .subject
                    .as_deref()
                    .map(|subject| {
                        if subject.starts_with('a') {
                            format!(" · {}", self.agent_title(subject))
                        } else {
                            format!(" · {subject}")
                        }
                    })
                    .unwrap_or_default();
                rows.push(KeyedView::new(
                    format!("blocker:{index}"),
                    controls::secondary(format!(
                        "Blocked by: {}{subject}",
                        eliot_core::reason_words(&blocker.code)
                    ))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                ));
            }
            if result.outcome == ProjectTransferOutcome::Transferred
                && let Some(handoff) = &result.handoff
            {
                rows.push(KeyedView::new(
                    "handoff",
                    controls::secondary(format!(
                        "Brief handed over by {}; its delivery shows in the successor's conversation.",
                        handoff.brief_command
                    ))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                ));
            }
        } else if intent.is_some() {
            rows.push(KeyedView::new(
                "open",
                controls::secondary(
                    "A transfer is open for this project. Choose the successor to continue it, or cancel it.",
                )
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        if let Some((text, error)) = self.assignment.outcomes.get(project) {
            let line = controls::secondary(text.clone())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true);
            rows.push(KeyedView::new(
                "outcome",
                if *error {
                    line.foreground(ThemeBrush::SystemCritical)
                } else {
                    line
                },
            ));
        }
        // The next action for the chosen successor: Begin first; then a
        // launch until the daemon reports its successor ready, then Commit.
        let ready = last.is_some_and(|result| {
            result.outcome == ProjectTransferOutcome::DestinationReady
                && result.destination.is_some()
        });
        let next = match (&intent, &chosen) {
            _ if done => None,
            (None, Some(Successor::Launch(_))) if brief.trim().is_empty() => None,
            (None, Some(_)) => Some(Step::Begin),
            (Some(_), Some(Successor::Existing { .. })) => Some(Step::Commit),
            (Some(_), Some(Successor::Launch(_))) if ready => Some(Step::Commit),
            (Some(_), Some(Successor::Launch(_))) => Some(Step::Launch),
            _ => None,
        };
        let primary_label = next.map_or(
            if intent.is_some() {
                Step::Commit.words()
            } else {
                Step::Begin.words()
            },
            Step::words,
        );
        let mut buttons: Vec<KeyedView> = vec![KeyedView::new(
            "close",
            controls::text_button()
                .element_ref(&self.dialog.close)
                .automation_name("Close")
                .on_click(context.message(Message::Project(ProjectMsg::Assign(AssignMsg::Closed))))
                .content(controls::label("Close")),
        )];
        if intent.is_some() {
            buttons.push(KeyedView::new(
                "cancel",
                controls::outlined()
                    .is_enabled(!busy)
                    .automation_name("Cancel transfer")
                    .on_click(context.message(Message::Project(ProjectMsg::Assign(
                        AssignMsg::Transfer(Step::Cancel),
                    ))))
                    .content(controls::label(Step::Cancel.words())),
            ));
        }
        if !done {
            buttons.push(KeyedView::new(
                format!("next:{primary_label}"),
                controls::filled()
                    .element_ref(&self.dialog.primary)
                    .is_enabled(next.is_some() && !busy)
                    .automation_name(primary_label)
                    .on_click(match next {
                        Some(step) => context.message(Message::Project(ProjectMsg::Assign(
                            AssignMsg::Transfer(step),
                        ))),
                        None => context.message(Message::Menu(MenuMsg::Ignored)),
                    })
                    .content(controls::label(primary_label)),
            ));
        }
        let footer = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .horizontal_alignment(HorizontalAlignment::Right)
            .spacing(tokens::spacing::SM)
            .keyed_children(buttons)
            .into();
        dialog_ui::surface(
            context,
            self.dialog.epoch,
            format!("Transfer {name}"),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(rows)
                .into(),
            footer,
            tokens::layout::HANDOFF_DIALOG_WIDTH,
        )
    }
}

/// A stable digest of a text, for a ledger key.
fn note_digest(text: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

/// The summary group (§12.1): the manager row or "Choose a manager", the
/// assignment and transfer state with their actions, team and capacity.
pub(super) fn summary_panel(
    manager: Option<&ManagerView>,
    context: &mut ViewContext<ProjectView>,
) -> View {
    let mut rows: Vec<KeyedView> =
        vec![KeyedView::new("head", page_ui::panel_head("Manager", None))];
    let Some(manager) = manager else {
        rows.push(KeyedView::new("reading", page_ui::quiet("Reading…")));
        return page_ui::panel(tokens::spacing::SM, rows);
    };
    let assign = |msg: AssignMsg| Message::Project(ProjectMsg::Assign(msg));
    match (&manager.agent, &manager.remote) {
        (Some(line), _) => {
            let content = Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(tokens::spacing::INSET)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .maybe_content(page_ui::state_mark(line.state)),
                    Border::new()
                        .grid_column(1)
                        .content(page_ui::two_lines(&line.title, &line.words)),
                ));
            let agent: View = match &line.agent {
                Some(agent) => controls::row(false)
                    .min_height(tokens::height::TWO_LINE_ROW)
                    .automation_name(format!("Open the manager {}", line.title))
                    .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
                    .content(content)
                    .tooltip("Open task"),
                None => content.into(),
            };
            rows.push(KeyedView::new("agent", agent));
        }
        (None, Some(remote)) => rows.push(KeyedView::new("remote", controls::body(remote.clone()))),
        (None, None) => {
            // §12.2: one useful line with the existing candidates and
            // Launch manager; competing roots are a conflict the Owner
            // settles by choosing.
            let line = if manager.needs && manager.candidates.len() > 1 {
                format!(
                    "Choose a manager · {} compete for this project",
                    eliot_core::count_words(
                        manager.candidates.len() as u64,
                        "top-level task",
                        "top-level tasks"
                    )
                )
            } else {
                "Choose a manager".to_owned()
            };
            let launch: Option<View> = manager.can_launch.then(|| {
                controls::outlined()
                    .is_enabled(!manager.busy)
                    .automation_name("Launch manager")
                    .on_click(context.message(assign(AssignMsg::LaunchOpened)))
                    .content(controls::label("Launch manager"))
                    .into()
            });
            rows.push(KeyedView::new(
                format!("choose:{}", manager.busy),
                Grid::new()
                    .min_height(tokens::height::CONTROL)
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((
                        controls::body(line)
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_wrapping(TextWrapping::Wrap),
                        crate::views::MaybeContent::maybe_content(
                            Border::new().grid_column(1),
                            launch,
                        ),
                    )),
            ));
            for candidate in &manager.candidates {
                rows.push(KeyedView::new(
                    format!(
                        "candidate:{}:{}:{}",
                        candidate.agent, candidate.generation, manager.busy
                    ),
                    Grid::new()
                        .min_height(tokens::height::TWO_LINE_ROW)
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            page_ui::two_lines(&candidate.title, &candidate.words),
                            controls::outlined()
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .is_enabled(!manager.busy)
                                .automation_name(format!("Assign {} as manager", candidate.title))
                                .on_click(context.message(assign(AssignMsg::Claim(
                                    candidate.agent.clone(),
                                    candidate.generation,
                                ))))
                                .content(controls::label("Assign")),
                        )),
                ));
            }
            if manager.candidates.is_empty() && !manager.can_launch {
                rows.push(KeyedView::new(
                    "none",
                    page_ui::quiet(
                        "No live top-level task to assign, and no profile this project approves for a manager launch.",
                    ),
                ));
            }
        }
    }
    if let Some(state) = &manager.state {
        let text = match &manager.start_command {
            Some(command) => format!("{state} · start command {command}"),
            None => state.clone(),
        };
        rows.push(KeyedView::new(
            "state",
            controls::secondary(text)
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        ));
    }
    if let Some(transfer) = &manager.transfer {
        rows.push(KeyedView::new(
            format!("transfer:{transfer}"),
            controls::body(format!("Transfer: {transfer}")).text_wrapping(TextWrapping::Wrap),
        ));
        for (index, blocker) in manager.blockers.iter().enumerate() {
            rows.push(KeyedView::new(
                format!("blocker:{index}"),
                controls::secondary(format!("Blocked by: {blocker}"))
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
    }
    if !manager.competing.is_empty() {
        rows.push(KeyedView::new(
            "competing",
            controls::secondary(format!(
                "Competing with the manager: {}",
                manager.competing.join(", ")
            ))
            .text_wrapping(TextWrapping::Wrap),
        ));
    }
    let mut actions: Vec<KeyedView> = Vec::new();
    if manager.can_transfer {
        let label = if manager.transfer.is_some() {
            "Continue transfer…"
        } else {
            "Transfer…"
        };
        actions.push(KeyedView::new(
            format!("transfer:{label}:{}", manager.busy),
            controls::outlined()
                .automation_name(label)
                .on_click(context.message(assign(AssignMsg::TransferOpened)))
                .content(controls::label(label)),
        ));
    }
    if manager.can_release {
        actions.push(KeyedView::new(
            format!("release:{}", manager.busy),
            controls::text_button()
                .is_enabled(!manager.busy)
                .automation_name("Release the manager")
                .on_click(context.message(assign(AssignMsg::ReleaseOpened)))
                .content(controls::label("Release…")),
        ));
    }
    if !actions.is_empty() {
        rows.push(KeyedView::new(
            "actions",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .keyed_children(actions),
        ));
    }
    if manager.busy {
        rows.push(KeyedView::new("busy", page_ui::quiet("Sending…")));
    }
    if let Some((text, error)) = &manager.outcome {
        let line = controls::secondary(text.clone())
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true);
        rows.push(KeyedView::new(
            format!("outcome:{text}"),
            if *error {
                line.foreground(ThemeBrush::SystemCritical)
            } else {
                line
            },
        ));
    }
    if let Some(error) = &manager.read_error {
        rows.push(KeyedView::new(
            "read-error",
            page_ui::error_line(format!("Assignment not read: {error}")),
        ));
    }
    if let Some(team) = &manager.team {
        rows.push(KeyedView::new(
            "team",
            page_ui::kv_text("Team", team.clone()),
        ));
    }
    match &manager.capacity {
        None => rows.push(KeyedView::new(
            "capacity",
            page_ui::kv_text("At once", "Not read yet"),
        )),
        Some(capacity) => {
            let at_once = match capacity.limit {
                Some(0) => "Closed: new launches wait".to_owned(),
                Some(limit) => format!("{} of {limit}", capacity.usage),
                None => format!("{} · no limit", capacity.usage),
            };
            let at_once = if capacity.live.is_empty() {
                at_once
            } else {
                format!("{at_once} · {}", capacity.live)
            };
            let edit: View = controls::text_button()
                .automation_name("Edit capacity in config.toml")
                .on_click(context.message(Message::Settings(SettingsMsg::OpenConfig)))
                .content(controls::link_content("Edit"))
                .tooltip("Capacity is set in config.toml ([capacity])");
            rows.push(KeyedView::new(
                "capacity",
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((
                        page_ui::kv_text("At once", at_once),
                        Border::new().grid_column(1).content(edit),
                    )),
            ));
            rows.push(KeyedView::new(
                "scope",
                page_ui::quiet(format!("Capacity scope {}", capacity.scope)),
            ));
            if let Some(limit) = capacity.limit.filter(|limit| *limit > 0) {
                let share = capacity.usage as f64 * 100.0 / limit as f64;
                rows.push(KeyedView::new(
                    format!("meter:{}", share.round()),
                    controls::meter(share, false),
                ));
            }
        }
    }
    page_ui::panel(tokens::spacing::SM, rows)
}
