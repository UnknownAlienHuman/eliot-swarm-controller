//! The sidebar's right-click menus (owner review 2026-11-16): a project's
//! New task here, Open folder, Rename, Collapse or Expand and Remove
//! project; a chat's or task's Rename, Stop (while it is live, idle
//! included), Move under another task (k113, [`super::move_ui`]), Resume
//! (a lost one), Archive or Unarchive, Delete, Copy Agent ID and Open
//! folder.
//!
//! Delete follows the Store's rule (`docs/spec/core.md`, explicit Agent
//! tree removal): only a stopped or failed tree, once drained. It asks
//! once, naming the chat. A live chat's Delete is "Stop and delete" (k98):
//! stop, wait until the Agent reads stopped or failed, then `agent.delete`;
//! the dialog shows each step and a refused step's typed error. A lost
//! task cannot be deleted: its menu offers Resume or Archive instead.
//! Archiving the open chat returns to New task with an "Archived · Undo"
//! notice (the Codex app's archive).
//!
//! Reactor 0.100.0 attaches a `Menu` only as a Button's left-click flyout
//! and has no `ContextFlyout`, so the menu is drawn in the window, at the
//! pointer, over a transparent layer that closes it on any press. All modal
//! confirmations use the common M3 body/footer and native focus host in
//! [`super::dialog_ui`].
//! A row's right press arrives through its `Border` (`PointerPressed` with
//! the right button, which a Button leaves unhandled).
//!
//! Rename writes an owner display name (`[desktop] display_names`); it
//! renames what the sidebar shows, not a registration or an Agent record.
//! Archive and Delete call `agent.archive` / `agent.delete` and Remove
//! project the settings writer's `projects` section (k92 contract §2).

use super::*;
use crate::theme::winui::Palette;

/// What a right-click menu acts on, captured when it opened.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Target {
    Project {
        /// The sidebar section key (`repo:<common dir>`).
        key: String,
        name: String,
        /// Where a new task starts.
        checkout: String,
        checkouts: Vec<String>,
        collapsed: bool,
    },
    Agent(UiAgent),
}

impl Target {
    /// The `[desktop] display_names` key of the target.
    fn name_key(&self) -> String {
        match self {
            Self::Project { key, .. } => format!("project:{key}"),
            Self::Agent(agent) => format!("agent:{}", agent.id),
        }
    }

    fn title(&self) -> &str {
        match self {
            Self::Project { name, .. } => name,
            Self::Agent(agent) => &agent.title,
        }
    }
}

/// One menu entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Action {
    NewTaskHere,
    OpenFolder,
    Rename,
    ToggleCollapse,
    RemoveProject,
    Stop,
    /// "Move under another task…": open the picker.
    MoveUnder,
    /// Resume a lost task in its conversation.
    Resume,
    Archive(bool),
    Delete,
    CopyId,
    More,
}

#[derive(Clone, Debug)]
pub(super) enum MenuMsg {
    /// A row's right press: its target and the pointer in window DIPs.
    Requested(Target, f64, f64),
    Closed,
    Chosen(Action),
    Quick(UiAgent, Action),
    TaskOpened(UiAgent),
    Hovered(Option<String>),
    RenameChanged(String),
    RenameCommitted,
    RenameCancelled,
    /// The confirmation's answer (true: go ahead).
    Confirmed(bool),
    /// Show archived Agents in the sidebar, or not.
    ArchivedToggled,
    /// An Agent lifecycle call settled (what, the outcome).
    Done(String, MutationResult<String>),
    /// Put the keyboard in the rename field (tries left).
    FocusRename(u8),
    /// Stop and delete: the stop was submitted (its outcome).
    RemovalStopped(MutationResult<String>),
    /// Stop and delete: the Agent reads stopped (its state), or not.
    RemovalSettled(MutationResult<String>),
    /// Stop and delete: `agent.delete` settled.
    RemovalDeleted(MutationResult<String>),
    /// Close the Stop and delete dialog after a refused step.
    RemovalClosed,
    /// Stop and delete: a second passed; the progress line's seconds move.
    RemovalTick,
    /// `agent.archive` settled (the Agent, archived or not, whether to
    /// reopen its conversation, the outcome).
    Archived(UiAgent, bool, bool, MutationResult<usize>),
    /// The "Archived · Undo" notice's Undo.
    UndoArchive,
    /// The notice was closed, or its time ran out (its epoch).
    ArchivedNoticeClosed(Option<u64>),
    /// A left press on a row: nothing to do here (its button acts).
    Ignored,
}

/// The open menu, the rename in progress and the pending confirmation.
#[derive(Default)]
pub(super) struct MenuState {
    pub(super) open: Option<(Target, f64, f64)>,
    pub(super) more: bool,
    pub(super) hovered: Option<String>,
    /// The target being renamed and the text typed so far.
    pub(super) renaming: Option<(Target, String)>,
    pub(super) confirm: Option<(Target, Action)>,
    pub(super) rename_ref: ElementRef<TextBox>,
    /// A live Agent's Stop and delete in progress (its dialog stays open).
    pub(super) removal: Option<Removal>,
    /// "Move under another task…" is open.
    pub(super) moving: Option<super::move_ui::MoveDialog>,
    /// "Archived · Undo" after an archive: the Agent, whether it was the
    /// open conversation, and the notice's epoch (it expires).
    pub(super) archived: Option<(UiAgent, bool, u64)>,
    pub(super) archived_epoch: u64,
}

/// A live Agent's Stop and delete: the step it is at, and a refused step's
/// typed error.
#[derive(Clone, Debug)]
pub(super) struct Removal {
    pub(super) agent: UiAgent,
    /// The stop command's id: `agent.delete` refuses while it is open.
    pub(super) command: String,
    pub(super) step: RemovalStep,
    pub(super) error: Option<String>,
    /// When Stop was clicked: the progress line counts from here.
    pub(super) started: std::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RemovalStep {
    Stopping,
    Waiting,
    Deleting,
}

impl RemovalStep {
    /// The dialog's honest progress line: what is happening, whose
    /// shutdown it waits for, and for how long ("Stopping Codex… 2 s").
    fn progress(self, harness: &str, elapsed: std::time::Duration) -> String {
        let harness = crate::picker::harness_label(harness);
        let seconds = elapsed.as_secs();
        match self {
            Self::Stopping => format!("Stopping {harness}… {seconds} s"),
            Self::Waiting => {
                format!("Waiting for {harness} to exit and release its files… {seconds} s")
            }
            Self::Deleting => format!("Deleting… {seconds} s"),
        }
    }

    fn what(self) -> &'static str {
        match self {
            Self::Stopping => "Stop",
            Self::Waiting => "Waiting for the stop",
            Self::Deleting => "Delete",
        }
    }
}

/// How long Stop and delete waits for the Agent to read stopped.
const REMOVAL_WAIT_MS: u64 = 60_000;
/// Between two reads of the stopping Agent.
const REMOVAL_POLL_MS: u64 = 400;
/// How long the "Archived · Undo" notice stays.
const ARCHIVED_NOTICE_MS: u64 = 10_000;

/// The menu's width.
const MENU_WIDTH: f64 = 248.0;

impl DesktopApp {
    pub(super) fn menu_update(&mut self, msg: MenuMsg, context: &ComponentContext<Self>) {
        match msg {
            MenuMsg::Requested(target, x, y) => {
                self.close_popover();
                self.menu.more = false;
                self.menu.open = Some((target, x, y));
                crate::shortcuts::set_popover(crate::shortcuts::Popover::Menu);
            }
            MenuMsg::Closed => self.close_menu(),
            MenuMsg::TaskOpened(agent) => {
                let (x, y) = crate::shortcuts::pointer_position();
                self.menu_update(MenuMsg::Requested(Target::Agent(agent), x, y), context);
            }
            MenuMsg::Hovered(agent) => self.menu.hovered = agent,
            MenuMsg::Quick(agent, action) => {
                self.menu_action(Target::Agent(agent), action, context)
            }
            MenuMsg::Chosen(Action::More) => self.menu.more = !self.menu.more,
            MenuMsg::Chosen(action) => {
                let Some((target, _, _)) = self.menu.open.take() else {
                    return;
                };
                crate::shortcuts::set_popover(crate::shortcuts::Popover::None);
                self.menu_action(target, action, context);
            }
            MenuMsg::RenameChanged(text) => {
                if let Some((_, typed)) = self.menu.renaming.as_mut() {
                    *typed = text;
                }
            }
            MenuMsg::RenameCommitted => {
                if let Some((target, typed)) = self.menu.renaming.take() {
                    if self.prefs.rename(&target.name_key(), &typed) {
                        self.prefs_rev += 1;
                        self.save_preferences(context);
                    } else {
                        self.alert("That name cannot be kept: too many renamed items.");
                    }
                }
            }
            MenuMsg::RenameCancelled => self.menu.renaming = None,
            MenuMsg::Confirmed(yes) => {
                let Some((target, action)) = self.menu.confirm.take() else {
                    return;
                };
                if yes {
                    self.confirmed_action(target, action, context);
                }
            }
            MenuMsg::ArchivedToggled => {
                self.nav.show_archived = !self.nav.show_archived;
                self.load_agents(context, None);
            }
            MenuMsg::Done(what, outcome) => {
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                match outcome {
                    Ok(note) => self.set_status(format!("{what}: {note}.")),
                    Err(error) if crate::state::uncertain(&error) => self.alert(format!(
                        "{what}: not confirmed (trying again reuses its id): {error}"
                    )),
                    Err(error) => self.alert(format!("{what} refused: {error}")),
                }
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
            MenuMsg::FocusRename(tries) => {
                if self.menu.renaming.is_some()
                    && !self.menu.rename_ref.request_focus()
                    && tries > 0
                {
                    self.focus_rename(tries - 1, context);
                }
            }
            MenuMsg::RemovalStopped(outcome) => self.removal_stopped(outcome, context),
            MenuMsg::RemovalSettled(outcome) => self.removal_settled(outcome, context),
            MenuMsg::RemovalDeleted(outcome) => {
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                match outcome {
                    Ok(note) => {
                        let title = self
                            .menu
                            .removal
                            .take()
                            .map(|removal| self.agent_label(&removal.agent))
                            .unwrap_or_default();
                        self.set_status(format!("Stopped and deleted “{title}”: {note}."));
                    }
                    Err(error) => self.removal_failed(error),
                }
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
            MenuMsg::RemovalTick => {
                if self
                    .menu
                    .removal
                    .as_ref()
                    .is_some_and(|removal| removal.error.is_none())
                {
                    self.post_removal_tick(context);
                }
            }
            MenuMsg::RemovalClosed => {
                self.menu.removal = None;
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
            MenuMsg::Archived(agent, archived, reopen, outcome) => {
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                let what = if archived { "Archive" } else { "Unarchive" };
                match outcome {
                    Ok(_) if archived => {
                        let open = self.selected_agent_id().as_deref() == Some(agent.id.as_str());
                        if open {
                            // The Codex app's archive: back to a new task.
                            self.new_task(context);
                        }
                        self.menu.archived_epoch += 1;
                        let epoch = self.menu.archived_epoch;
                        self.set_status(format!("Archived “{}”.", self.agent_label(&agent)));
                        self.menu.archived = Some((agent, open, epoch));
                        let _task = context.spawn_background(move |cancel| {
                            let cancelled = || cancel.is_cancelled();
                            let _ = feed::backoff(ARCHIVED_NOTICE_MS, &cancelled);
                            Message::Menu(MenuMsg::ArchivedNoticeClosed(Some(epoch)))
                        });
                    }
                    Ok(_) => {
                        self.menu.archived = None;
                        self.set_status(format!("Unarchived “{}”.", self.agent_label(&agent)));
                        if reopen {
                            let mut agent = agent;
                            agent.archived = false;
                            self.shell_update(ShellMsg::AgentOpened(agent), context);
                        }
                    }
                    Err(error) if crate::state::uncertain(&error) => self.alert(format!(
                        "{what}: not confirmed (trying again reuses its id): {error}"
                    )),
                    Err(error) => self.alert(format!("{what} refused: {error}")),
                }
                self.shell_update(ShellMsg::RefreshRequested, context);
            }
            MenuMsg::UndoArchive => {
                if let Some((agent, open, _)) = self.menu.archived.take() {
                    self.submit_archive(agent, false, open, context);
                }
            }
            MenuMsg::ArchivedNoticeClosed(epoch) => {
                if epoch.is_none()
                    || self
                        .menu
                        .archived
                        .as_ref()
                        .is_some_and(|(_, _, shown)| Some(*shown) == epoch)
                {
                    self.menu.archived = None;
                }
            }
            MenuMsg::Ignored => {}
        }
    }

    /// Stop and delete of a live Agent, step one: submit its stop.
    fn start_removal(&mut self, agent: UiAgent, context: &ComponentContext<Self>) {
        if self.submitting() {
            self.alert("Waiting for the last action to settle.");
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let bound = BoundAction::agent_control(&agent.id, agent.generation);
        let key = format!("stop:{}:{}:false", bound.agent, bound.generation);
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.set_status(format!("Stop ({mutation_id})…"));
        self.menu.removal = Some(Removal {
            agent,
            command: mutation_id.clone(),
            step: RemovalStep::Stopping,
            error: None,
            started: std::time::Instant::now(),
        });
        self.post_removal_tick(context);
        let _task = context.spawn_background(move |_| {
            Message::Menu(MenuMsg::RemovalStopped(
                client::block_on(client::submit_stop(
                    &data_dir,
                    mutation_id,
                    &bound.agent,
                    bound.generation,
                    false,
                ))
                .map(|reply| submit_note(&reply)),
            ))
        });
    }

    /// Re-render the progress line's seconds once a second while the
    /// Stop and delete dialog shows progress.
    fn post_removal_tick(&self, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(1_000, &cancelled);
            Message::Menu(MenuMsg::RemovalTick)
        });
    }

    /// Step two: wait until the stop command has settled and the Agent
    /// reads stopped (bounded): `agent.delete` refuses `busy` while either
    /// is still open.
    fn removal_stopped(
        &mut self,
        outcome: MutationResult<String>,
        context: &ComponentContext<Self>,
    ) {
        self.settle_mutation(
            outcome
                .as_ref()
                .err()
                .is_none_or(|error| !crate::state::uncertain(error)),
        );
        if let Err(error) = outcome {
            self.removal_failed(error);
            return;
        }
        let (Some(removal), Ok(data_dir)) = (self.menu.removal.as_mut(), self.data_dir.clone())
        else {
            return;
        };
        removal.step = RemovalStep::Waiting;
        let id = removal.agent.id.clone();
        let command = removal.command.clone();
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let mut waited = 0;
            let outcome = loop {
                let read = client::block_on(async {
                    let stop = client::read_command(&data_dir, &command).await?;
                    let agent = client::get_agent(&data_dir, &id).await?;
                    Ok::<_, eliot_core::Error>((stop.state, agent.agent.state))
                });
                match read {
                    Ok((stop, state)) => {
                        use eliot_core::{AgentState, CommandState};
                        if matches!(stop, CommandState::Rejected | CommandState::Failed) {
                            break Err(eliot_core::Error::new(
                                format!("stop_{}", format!("{stop:?}").to_lowercase()),
                                "the stop did not apply; nothing was deleted",
                            ));
                        }
                        // The Store deletes only a stopped or failed tree;
                        // one lost on the way out stays (Resume or Archive).
                        if stop.is_terminal() && state == AgentState::Lost {
                            break Err(eliot_core::Error::new(
                                "lost",
                                "it was lost while stopping, so Swarm cannot delete it; nothing was deleted. Resume it, or archive it to hide it",
                            ));
                        }
                        let stopped = matches!(state, AgentState::Stopped | AgentState::Failed);
                        if stop.is_terminal() && stopped {
                            break Ok(format!("{state:?}").to_lowercase());
                        }
                        if waited >= REMOVAL_WAIT_MS {
                            break Err(eliot_core::Error::new(
                                "timeout",
                                format!(
                                    "Agent {}, stop {} after {} s; nothing was deleted",
                                    format!("{state:?}").to_lowercase(),
                                    format!("{stop:?}").to_lowercase(),
                                    REMOVAL_WAIT_MS / 1000
                                ),
                            ));
                        }
                    }
                    Err(error) => break Err(error),
                }
                if !feed::backoff(REMOVAL_POLL_MS, &cancelled) {
                    break Err(eliot_core::Error::new(
                        "cancelled",
                        "the window stopped waiting",
                    ));
                }
                waited += REMOVAL_POLL_MS;
            };
            Message::Menu(MenuMsg::RemovalSettled(outcome))
        });
    }

    /// Step three: the Agent is stopped, so `agent.delete` it. The Store
    /// refuses `busy` until the stopped generation's processes and sources
    /// are released; such a definite refusal records nothing, so the same
    /// mutation id asks again (bounded) and any other outcome is final.
    fn removal_settled(
        &mut self,
        outcome: MutationResult<String>,
        context: &ComponentContext<Self>,
    ) {
        if let Err(error) = outcome {
            self.removal_failed(error);
            return;
        }
        let Some(agent) = self
            .menu
            .removal
            .as_ref()
            .map(|removal| removal.agent.clone())
        else {
            return;
        };
        if self.submitting() {
            self.removal_failed(eliot_core::Error::new(
                "busy",
                "waiting for the last action to settle",
            ));
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.selected_agent_id().as_deref() == Some(agent.id.as_str()) {
            self.new_task(context);
        }
        if let Some(removal) = self.menu.removal.as_mut() {
            removal.step = RemovalStep::Deleting;
        }
        let key = format!("delete:{}", agent.id);
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        let id = agent.id;
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let mut waited = 0;
            let outcome = loop {
                match client::block_on(client::delete_agent(&data_dir, mutation_id.clone(), &id)) {
                    Ok(count) => break Ok(format!("{count} record tree(s) removed")),
                    Err(error) if error.code == "busy" && waited < REMOVAL_WAIT_MS => {}
                    Err(error) => break Err(error),
                }
                if !feed::backoff(REMOVAL_POLL_MS, &cancelled) {
                    break Err(eliot_core::Error::new(
                        "cancelled",
                        "the window stopped waiting",
                    ));
                }
                waited += REMOVAL_POLL_MS;
            };
            Message::Menu(MenuMsg::RemovalDeleted(outcome))
        });
    }

    /// A refused (or unconfirmed) step: its typed error, in the dialog.
    fn removal_failed(&mut self, error: eliot_core::Error) {
        let Some(removal) = self.menu.removal.as_mut() else {
            self.alert(error.to_string());
            return;
        };
        let what = removal.step.what();
        removal.error = Some(if crate::state::uncertain(&error) {
            format!("{what}: not confirmed (trying again reuses its id): {error}")
        } else {
            format!("{what} refused: {error}")
        });
        let line = removal.error.clone().unwrap_or_default();
        self.set_status(line);
    }

    /// `agent.archive` (or unarchive) with a ledger-held mutation id.
    fn submit_archive(
        &mut self,
        agent: UiAgent,
        archived: bool,
        reopen: bool,
        context: &ComponentContext<Self>,
    ) {
        if self.submitting() {
            self.alert("Waiting for the last action to settle.");
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let key = format!("archive:{}:{archived}", agent.id);
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        let what = if archived { "Archive" } else { "Unarchive" };
        self.set_status(format!("{what} ({mutation_id})…"));
        let _task = context.spawn_background(move |_| {
            let outcome = client::block_on(client::archive_agent(
                &data_dir,
                mutation_id,
                &agent.id,
                archived,
            ));
            Message::Menu(MenuMsg::Archived(agent, archived, reopen, outcome))
        });
    }

    pub(super) fn close_menu(&mut self) {
        if self.menu.open.take().is_some() {
            crate::shortcuts::set_popover(crate::shortcuts::Popover::None);
        }
    }

    pub(super) fn menu_action(
        &mut self,
        target: Target,
        action: Action,
        context: &ComponentContext<Self>,
    ) {
        match (action, &target) {
            (Action::NewTaskHere, Target::Project { checkout, .. }) => {
                self.shell_update(ShellMsg::ProjectPicked(checkout.clone()), context);
            }
            (Action::OpenFolder, Target::Project { checkout, .. }) => open_folder(checkout),
            (Action::OpenFolder, Target::Agent(agent)) => open_folder(&agent.project),
            (Action::ToggleCollapse, Target::Project { key, .. }) => {
                self.shell_update(ShellMsg::ProjectToggled(key.clone()), context);
            }
            (Action::Rename, _) => {
                let current = self
                    .prefs
                    .display_name(&target.name_key())
                    .unwrap_or(target.title())
                    .to_owned();
                self.menu.renaming = Some((target, current));
                self.focus_rename(5, context);
            }
            (Action::RemoveProject | Action::Delete, _) => {
                self.menu.confirm = Some((target, action));
            }
            (Action::CopyId, Target::Agent(agent)) => match crate::clipboard::set_text(&agent.id) {
                Ok(()) => self.set_status(format!("Copied {}.", agent.id)),
                Err(error) => self.alert(format!("Copy failed: {error}")),
            },
            (Action::Stop, Target::Agent(agent)) => {
                let bound = BoundAction::agent_control(&agent.id, agent.generation);
                let key = format!("stop:{}:{}:false", bound.agent, bound.generation);
                self.submit_lifecycle("Stop", key, context, move |data_dir, id| {
                    client::block_on(client::submit_stop(
                        data_dir,
                        id,
                        &bound.agent,
                        bound.generation,
                        false,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            (Action::Archive(archived), Target::Agent(agent)) => {
                self.submit_archive(agent.clone(), archived, false, context);
            }
            (Action::MoveUnder, Target::Agent(agent)) => {
                self.move_update(super::move_ui::MoveMsg::Open(agent.clone()), context);
            }
            (Action::Resume, Target::Agent(agent)) => {
                if self.submitting() {
                    self.alert("Waiting for the last action to settle.");
                    return;
                }
                // Resume in its conversation, bound to the run shown.
                self.shell_update(ShellMsg::AgentOpened(agent.clone()), context);
                self.chat_update(
                    ChatMsg::DraftRequested(super::chat_ui::DraftAction::Resume {
                        agent: agent.id.clone(),
                        generation: agent.generation,
                    }),
                    context,
                );
            }
            _ => {}
        }
    }

    fn confirmed_action(
        &mut self,
        target: Target,
        action: Action,
        context: &ComponentContext<Self>,
    ) {
        match (action, target) {
            (Action::Delete, Target::Agent(agent)) if agent.live() => {
                self.start_removal(agent, context);
            }
            (Action::Delete, Target::Agent(agent)) => {
                if self.selected_agent_id().as_deref() == Some(agent.id.as_str()) {
                    self.new_task(context);
                }
                let id = agent.id.clone();
                let key = format!("delete:{id}");
                self.submit_lifecycle("Delete", key, context, move |data_dir, mutation| {
                    client::block_on(client::delete_agent(data_dir, mutation, &id))
                        .map(|count| format!("{count} record tree(s) removed"))
                });
            }
            (
                Action::RemoveProject,
                Target::Project {
                    checkouts, name, ..
                },
            ) => {
                let ids: Vec<String> = self
                    .nav
                    .projects
                    .iter()
                    .filter(|project| {
                        checkouts
                            .iter()
                            .any(|path| nav::path_key(path) == nav::path_key(&project.workspace))
                    })
                    .map(|project| project.id.clone())
                    .collect();
                if ids.is_empty() {
                    self.alert(format!("{name} is not a registered project."));
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let _task = context.spawn_background(move |_| {
                    Message::Menu(MenuMsg::Done(
                        format!("Remove project {name}"),
                        client::block_on(config_client::remove_projects(&data_dir, &ids))
                            .map(|_| "unregistered; its folder and tasks stay".to_owned()),
                    ))
                });
            }
            _ => {}
        }
    }

    /// One Agent lifecycle call with a ledger-held mutation id.
    fn submit_lifecycle(
        &mut self,
        what: &str,
        key: String,
        context: &ComponentContext<Self>,
        work: impl FnOnce(&Path, String) -> MutationResult<String> + Send + 'static,
    ) {
        if self.submitting() {
            self.alert("Waiting for the last action to settle.");
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.set_status(format!("{what} ({mutation_id})…"));
        let what = what.to_owned();
        let _task = context.spawn_background(move |_| {
            Message::Menu(MenuMsg::Done(what, work(&data_dir, mutation_id)))
        });
    }

    /// Focus the rename field once it exists (after the next render).
    fn focus_rename(&self, tries: u8, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(60, &cancelled);
            Message::Menu(MenuMsg::FocusRename(tries))
        });
    }

    /// The menu's entries for a target, in order: (action, label, glyph,
    /// destructive).
    pub(super) fn menu_entries(
        &self,
        target: &Target,
    ) -> Vec<(Action, String, &'static str, bool)> {
        match target {
            Target::Project { collapsed, .. } => vec![
                (
                    Action::NewTaskHere,
                    "New task here".to_owned(),
                    m3::glyph::NEW_TASK,
                    false,
                ),
                (
                    Action::OpenFolder,
                    "Open folder".to_owned(),
                    m3::glyph::OPEN_FOLDER,
                    false,
                ),
                (
                    Action::Rename,
                    "Rename".to_owned(),
                    m3::glyph::RENAME,
                    false,
                ),
                (
                    Action::ToggleCollapse,
                    if *collapsed { "Expand" } else { "Collapse" }.to_owned(),
                    if *collapsed {
                        m3::glyph::CHEVRON_DOWN
                    } else {
                        m3::glyph::CHEVRON_RIGHT
                    },
                    false,
                ),
                (
                    Action::RemoveProject,
                    "Remove project".to_owned(),
                    m3::glyph::DELETE,
                    true,
                ),
            ],
            Target::Agent(agent) => {
                if self.menu.more && self.menu.open.is_some() {
                    return std::iter::once((
                        Action::More,
                        "Back".to_owned(),
                        m3::glyph::BACK,
                        false,
                    ))
                    .chain(self.developer_entries(target))
                    .collect();
                }
                let mut entries = vec![(
                    Action::Rename,
                    "Rename".to_owned(),
                    m3::glyph::RENAME,
                    false,
                )];
                if !agent.native_child {
                    entries.push((
                        Action::MoveUnder,
                        "Move under another task…".to_owned(),
                        m3::glyph::MOVE,
                        false,
                    ));
                }
                if !agent.native_child {
                    entries.push(if agent.archived {
                        (
                            Action::Archive(false),
                            "Unarchive".to_owned(),
                            m3::glyph::ARCHIVE,
                            false,
                        )
                    } else {
                        (
                            Action::Archive(true),
                            "Archive".to_owned(),
                            m3::glyph::ARCHIVE,
                            false,
                        )
                    });
                }
                // A lost task cannot be deleted (the Store keeps it):
                // resume it, or archive it to hide it.
                if matches!(
                    agent.state,
                    eliot_core::AgentState::Lost
                        | eliot_core::AgentState::Stopped
                        | eliot_core::AgentState::Failed
                ) && !agent.native_child
                {
                    entries.push((
                        Action::Resume,
                        "Resume".to_owned(),
                        m3::glyph::RESUME,
                        false,
                    ));
                }
                if agent.live() && !agent.native_child {
                    entries.push((Action::Stop, "Stop".to_owned(), m3::glyph::STOP, false));
                }
                if !agent.native_child && (agent.live() || agent.deletable()) {
                    entries.push((
                        Action::Delete,
                        if agent.live() {
                            "Stop and delete…"
                        } else {
                            "Delete…"
                        }
                        .to_owned(),
                        m3::glyph::DELETE,
                        true,
                    ));
                }
                entries.push((
                    Action::More,
                    "More".to_owned(),
                    m3::glyph::CHEVRON_RIGHT,
                    false,
                ));
                entries
            }
        }
    }

    pub(super) fn developer_entries(
        &self,
        target: &Target,
    ) -> Vec<(Action, String, &'static str, bool)> {
        let Target::Agent(agent) = target else {
            return Vec::new();
        };
        let mut entries = vec![(
            Action::CopyId,
            "Copy Agent ID".to_owned(),
            m3::glyph::COPY,
            false,
        )];
        if !agent.project.is_empty() {
            entries.push((
                Action::OpenFolder,
                "Open folder".to_owned(),
                m3::glyph::OPEN_FOLDER,
                false,
            ));
        }
        entries
    }

    pub(super) fn task_dialog(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        if let Some(removal) = &self.menu.removal {
            return self.removal_dialog(removal, context, palette);
        }
        if let Some((target, action)) = &self.menu.confirm {
            return self.confirm_dialog(target, *action, context, palette);
        }
        if let Some(dialog) = &self.menu.moving {
            return self.move_dialog(dialog, context, palette);
        }
        let epoch = self.dialog.epoch;
        ContentDialog::new()
            .is_open(false)
            .on_closed(context.callback(move |_| Message::DialogClosed(epoch)))
            .content(View::empty())
    }

    /// The window-wide layer: the open menu at the pointer (a press outside
    /// closes it) or the confirmation dialog over a scrim.
    pub(super) fn menu_overlay(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let Some((target, x, y)) = &self.menu.open else {
            return View::empty();
        };
        let width = self.window_width.unwrap_or(tokens::layout::WINDOW_WIDTH);
        let left = x.min(width - MENU_WIDTH - tokens::spacing::SM).max(0.0);
        let rows: Vec<KeyedView> = self
            .menu_entries(target)
            .into_iter()
            .map(|(action, label, glyph, destructive)| {
                let text = if destructive {
                    m3::body(label.clone()).foreground(palette.error())
                } else {
                    m3::body(label.clone())
                };
                KeyedView::new(
                    label.clone(),
                    m3::row(palette, false)
                        .automation_name(label)
                        .on_click(context.message(Message::Menu(MenuMsg::Chosen(action))))
                        .content(
                            StackPanel::new()
                                .orientation(Orientation::Horizontal)
                                .spacing(tokens::spacing::SM)
                                .children((m3::icon(glyph), text)),
                        ),
                )
            })
            .collect();
        Grid::new().children((
            Border::new()
                .background(Brush::Solid(Color::transparent()))
                .on_pointer_pressed(
                    context.callback(|_: PointerEventInfo| Message::Menu(MenuMsg::Closed)),
                ),
            m3::popover(palette)
                .width(MENU_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Left)
                .vertical_alignment(VerticalAlignment::Top)
                .margin(Thickness::new(left, *y, 0.0, 0.0))
                .automation_name(format!("Actions for {}", target.title()))
                .content(StackPanel::new().keyed_children(rows)),
        ))
    }

    fn confirm_dialog(
        &self,
        target: &Target,
        action: Action,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let live = matches!(target, Target::Agent(agent) if agent.live());
        let (title, body, button) = match action {
            Action::Delete if live => (
                format!("Stop and delete “{}”?", target.title()),
                "Swarm stops it, waits until it has stopped, then removes its conversation and records for good. Its folder, files and branches stay.",
                "Stop and delete",
            ),
            Action::Delete => (
                format!("Delete “{}”?", target.title()),
                "Its conversation and records leave Swarm for good. Its folder, files and branches stay.",
                "Delete",
            ),
            _ => (
                format!("Remove project “{}”?", target.title()),
                "Swarm forgets this project. Its folder, files and tasks stay; you can add it again in Settings › Projects.",
                "Remove project",
            ),
        };
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            title,
            m3::secondary(body).text_wrapping(TextWrapping::Wrap).into(),
            super::dialog_ui::actions(
                m3::text_button(palette)
                    .element_ref(&self.dialog.close)
                    .automation_name("Cancel")
                    .on_click(context.message(Message::Menu(MenuMsg::Confirmed(false))))
                    .content(m3::label("Cancel")),
                m3::destructive(palette)
                    .element_ref(&self.dialog.primary)
                    .automation_name(format!("{button}: {}", target.title()))
                    .on_click(context.message(Message::Menu(MenuMsg::Confirmed(true))))
                    .content(m3::label(button)),
            ),
            420.0,
        )
    }

    /// Stop and delete in progress: the step it is at, or a refused step's
    /// typed error and Close.
    fn removal_dialog(
        &self,
        removal: &Removal,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let title = format!("Stop and delete “{}”", self.agent_label(&removal.agent));
        let status: View = match &removal.error {
            Some(error) => m3::body(error.clone())
                .foreground(palette.error())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into(),
            None => StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((
                    ProgressRing::new()
                        .is_indeterminate(true)
                        .width(16.0)
                        .height(16.0)
                        .vertical_alignment(VerticalAlignment::Center),
                    m3::secondary(
                        removal
                            .step
                            .progress(&removal.agent.harness, removal.started.elapsed()),
                    )
                    .vertical_alignment(VerticalAlignment::Center),
                )),
        };
        let close: View = if removal.error.is_some() {
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .horizontal_alignment(HorizontalAlignment::Right)
                .children((m3::text_button(palette)
                    .element_ref(&self.dialog.close)
                    .automation_name("Close")
                    .on_click(context.message(Message::Menu(MenuMsg::RemovalClosed)))
                    .content(m3::label("Close")),))
        } else {
            View::empty()
        };
        super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            title,
            status,
            close,
            420.0,
        )
    }

    /// "Archived “title”" with Undo, above the composer.
    pub(super) fn archived_notice(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Option<View> {
        let (agent, _, _) = self.menu.archived.as_ref()?;
        let text = format!("Archived “{}”", self.agent_label(agent));
        Some(
            Border::new()
                .grid_row(4)
                .width(column)
                .max_width(tokens::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                .background(palette.card())
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::new(
                    tokens::spacing::MD,
                    tokens::spacing::XS,
                    tokens::spacing::XS,
                    tokens::spacing::XS,
                ))
                .automation_name(text.clone())
                .content(
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            m3::body(text)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            m3::text_button(palette)
                                .grid_column(1)
                                .automation_name("Undo archive")
                                .on_click(context.message(Message::Menu(MenuMsg::UndoArchive)))
                                .content(m3::label("Undo")),
                            m3::icon_button(palette)
                                .grid_column(2)
                                .automation_name("Dismiss")
                                .on_click(
                                    context.message(Message::Menu(MenuMsg::ArchivedNoticeClosed(
                                        None,
                                    ))),
                                )
                                .content(m3::icon(m3::glyph::CLOSE)),
                        )),
                ),
        )
    }

    /// The inline rename editor in place of a row's title, with Save and
    /// Cancel (Enter saves through the window's keyboard hook).
    pub(super) fn rename_row(
        &self,
        typed: &str,
        indent: f64,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        Grid::new()
            .margin(Thickness::new(
                indent,
                tokens::spacing::XS,
                0.0,
                tokens::spacing::XS,
            ))
            .columns([GridLength::STAR, GridLength::Auto, GridLength::Auto])
            .column_spacing(tokens::spacing::XS)
            .children((
                m3::field()
                    .element_ref(&self.menu.rename_ref)
                    .text(typed.to_owned())
                    .placeholder_text("Name")
                    .automation_name("Rename")
                    .on_text_changed(
                        context.callback(|text| Message::Menu(MenuMsg::RenameChanged(text))),
                    ),
                m3::icon_button(palette)
                    .grid_column(1)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .automation_name("Save name")
                    .on_click(context.message(Message::Menu(MenuMsg::RenameCommitted)))
                    .content(m3::icon(m3::glyph::CHECK)),
                m3::icon_button(palette)
                    .grid_column(2)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .automation_name("Cancel rename")
                    .on_click(context.message(Message::Menu(MenuMsg::RenameCancelled)))
                    .content(m3::icon(m3::glyph::CLOSE)),
            ))
    }

    /// The owner's name for an Agent, else its title.
    pub(super) fn agent_label(&self, agent: &UiAgent) -> String {
        self.prefs
            .display_name(&format!("agent:{}", agent.id))
            .map_or_else(|| agent.title.clone(), str::to_owned)
    }
}

/// A row wrapped so its right press opens the menu for `target`.
pub(super) fn with_menu(row: View, target: Target, context: &mut ViewContext<DesktopApp>) -> View {
    Border::new()
        .on_pointer_pressed(context.callback(move |info: PointerEventInfo| {
            if info.is_right_button_pressed {
                Message::Menu(MenuMsg::Requested(
                    target.clone(),
                    info.window_x,
                    info.window_y,
                ))
            } else {
                Message::Menu(MenuMsg::Ignored)
            }
        }))
        .content(row)
}

/// Open a folder in Explorer (the verbatim `\\?\` prefix dropped).
fn open_folder(path: &str) {
    let path = nav::display_path(path);
    if path.trim().is_empty() {
        return;
    }
    let _ = std::process::Command::new("explorer.exe").arg(path).spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_removal_line_names_the_harness_and_the_seconds() {
        let elapsed = std::time::Duration::from_millis(12_400);
        assert_eq!(
            RemovalStep::Stopping.progress("codex", elapsed),
            "Stopping Codex… 12 s"
        );
        assert_eq!(
            RemovalStep::Waiting.progress("claude", elapsed),
            "Waiting for Claude Code to exit and release its files… 12 s"
        );
        assert_eq!(
            RemovalStep::Deleting.progress("codex", elapsed),
            "Deleting… 12 s"
        );
    }
}
