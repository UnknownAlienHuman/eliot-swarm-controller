//! The sidebar's right-click menus (owner review 2026-11-16): a project's
//! or subproject's New task here, Open folder, Rename, Collapse or Expand,
//! Archive finished tasks, Delete stopped tasks and Remove project (k142,
//! through [`super::bulk_ui`]); a chat's or task's Rename, Stop (while it is live, idle
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
//! Attach to project, Detach from project (chats) and Tags (any task) call
//! `chat.attach`, `chat.detach` and `chat.tag` (#657 step 5): placement
//! and tags only, never execution authority; the sidebar reads the result
//! back from `chat.organization` and the listed rows.
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
    /// The inline tag editor of an Agent (`chat.tag`), in the rename
    /// field's place.
    Tags(UiAgent),
}

impl Target {
    /// The `[desktop] display_names` key of the target.
    fn name_key(&self) -> String {
        match self {
            Self::Project { key, .. } => format!("project:{key}"),
            Self::Agent(agent) | Self::Tags(agent) => format!("agent:{}", agent.id),
        }
    }

    fn title(&self) -> &str {
        match self {
            Self::Project { name, .. } => name,
            Self::Agent(agent) | Self::Tags(agent) => &agent.title,
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
    /// Archive every finished task of the project or subproject.
    ArchiveFinished,
    /// Delete every stopped task of the project or subproject.
    DeleteStopped,
    Stop,
    /// "Move under another task…": open the picker.
    MoveUnder,
    /// Resume a lost task in its conversation.
    Resume,
    Archive(bool),
    Delete,
    CopyId,
    More,
    /// "Attach to project…": open the registered projects (a chat).
    AttachMenu,
    /// Attach the chat to the opened list's project at this index.
    AttachTo(usize),
    /// Return the chat to the no-project bucket.
    Detach,
    /// Edit the Agent's tags inline.
    EditTags,
}

#[derive(Clone, Debug)]
pub(super) enum MenuMsg {
    /// A row's right press: its target and the pointer in window DIPs.
    Requested(Target, f64, f64),
    Closed,
    /// Move the keyboard highlight by `step` rows (wrapping).
    HighlightMove(i32),
    /// Choose the keyboard-highlighted row, if any.
    HighlightActivate,
    /// Bring the keyboard-highlighted row into view after its render
    /// (the highlight move that needs it).
    Reveal(u64),
    Chosen(Action),
    /// A row's '…': its menu at the pointer.
    Opened(Target),
    Hovered(Option<String>),
    RenameChanged(String),
    /// The tag editor's field at this index changed (the index past the
    /// last tag is the new-tag field).
    TagChanged(usize, String),
    /// The tag editor's remove button at this index.
    TagRemoved(usize),
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
    /// The keyboard-highlighted row (arrow keys, Enter activates).
    pub(super) highlight: Option<usize>,
    pub(super) more: bool,
    /// "Attach to project…" is open: the registered projects as it
    /// captured them, `(project, name)`.
    pub(super) attach: Option<Vec<(String, String)>>,
    pub(super) hovered: Option<String>,
    /// The target being renamed and the text typed so far.
    pub(super) renaming: Option<(Target, String)>,
    /// The tag editor's fields, one per tag (while `renaming` holds
    /// [`Target::Tags`]): an existing tag round-trips unchanged.
    pub(super) tag_fields: Vec<String>,
    pub(super) confirm: Option<(Target, Action)>,
    pub(super) rename_ref: ElementRef<TextBox>,
    /// The keyboard-highlighted row, bound when it renders: focusing
    /// it scrolls it into view (WinUI brings a focused element in).
    row_ref: ElementRef<Button>,
    /// Highlight moves handed out by `reveal_highlight`: a reveal for
    /// an older move is ignored.
    reveal_epoch: u64,
    /// Renders composed when the latest highlight move ran: the reveal
    /// waits for the render that binds the row.
    reveal_after: u64,
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

/// Between bringing the highlighted row into view and retrying it:
/// one frame later (see `reveal_highlight`).
const MENU_REVEAL_RETRY_MS: u64 = 16;

impl DesktopApp {
    pub(super) fn menu_update(&mut self, msg: MenuMsg, context: &ComponentContext<Self>) {
        match msg {
            MenuMsg::Requested(target, x, y) => {
                self.close_popover();
                self.menu.more = false;
                self.menu.attach = None;
                self.menu.highlight = None;
                self.menu.open = Some((target, x, y));
                crate::shortcuts::set_popover(crate::shortcuts::Popover::Menu);
            }
            MenuMsg::HighlightMove(step) => {
                let Some((target, _, _)) = &self.menu.open else {
                    return;
                };
                // The rename field and the confirmation own the keyboard.
                if self.menu.renaming.is_some() || self.menu.confirm.is_some() {
                    return;
                }
                let count = self.menu_entries(target).len();
                self.menu.highlight = highlight_step(self.menu.highlight, step, count);
                if self.menu.highlight.is_some() {
                    self.reveal_highlight(context);
                }
            }
            MenuMsg::Reveal(epoch) => {
                if epoch != self.menu.reveal_epoch || self.menu.open.is_none() {
                    return;
                }
                // Only a highlighted row binds the reference.
                if self.menu.highlight.is_none() {
                    return;
                }
                // The highlighted row's reference binds when it renders.
                if self.renders.get() <= self.menu.reveal_after {
                    self.post_reveal_highlight(epoch, MENU_REVEAL_RETRY_MS, context);
                    return;
                }
                // Focusing the row brings it into view; the thread hook
                // keeps the keyboard while the menu is open, so arrows
                // and Enter keep working.
                if !self.menu.row_ref.request_focus() {
                    self.post_reveal_highlight(epoch, MENU_REVEAL_RETRY_MS, context);
                }
            }
            MenuMsg::HighlightActivate => {
                let Some((target, _, _)) = &self.menu.open else {
                    return;
                };
                if self.menu.renaming.is_some() || self.menu.confirm.is_some() {
                    return;
                }
                if let Some(index) = self.menu.highlight
                    && let Some((action, _, _, _)) = self.menu_entries(target).get(index)
                {
                    let action = *action;
                    self.menu_update(MenuMsg::Chosen(action), context);
                }
            }
            MenuMsg::Closed => self.close_menu(),
            MenuMsg::Opened(target) => {
                let (x, y) = crate::shortcuts::pointer_position();
                self.menu_update(MenuMsg::Requested(target, x, y), context);
            }
            MenuMsg::Hovered(agent) => self.menu.hovered = agent,
            MenuMsg::Chosen(Action::More) => {
                self.menu.more = !self.menu.more;
                self.menu.highlight = None;
            }
            MenuMsg::Chosen(Action::AttachMenu) => {
                self.menu.attach = match self.menu.attach.take() {
                    Some(_) => None,
                    None => Some(self.attach_choices()),
                };
                self.menu.highlight = None;
            }
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
            MenuMsg::TagChanged(index, text) => {
                let fields = &mut self.menu.tag_fields;
                if let Some(field) = fields.get_mut(index) {
                    *field = text;
                } else if index == fields.len() && !text.is_empty() {
                    fields.push(text);
                }
            }
            MenuMsg::TagRemoved(index) => {
                if index < self.menu.tag_fields.len() {
                    self.menu.tag_fields.remove(index);
                }
            }
            MenuMsg::RenameCommitted => {
                if let Some((Target::Tags(agent), _)) = self.menu.renaming.clone() {
                    self.menu.renaming = None;
                    let tags = nav::field_tags(&std::mem::take(&mut self.menu.tag_fields));
                    self.submit_organize(
                        "Tags".to_owned(),
                        client::Organize::Tag {
                            agent: agent.id.clone(),
                            tags,
                            revision: agent.organization_revision.unwrap_or(0),
                        },
                        context,
                    );
                } else if let Some((target, typed)) = self.menu.renaming.take() {
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
                // The group totals follow the archived filter too.
                self.load_organization(context);
                // An open search reads its matches again under the filter.
                if !self.nav.search.query.trim().is_empty() {
                    self.load_search(context, None);
                }
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
                        let removed = self.menu.removal.take();
                        if let Some(removal) = removed.as_ref() {
                            // The Agent is gone: its unresolved snapshot can
                            // never be retried, so release its ledger key too.
                            super::chat_ui::abandon_retry(
                                &mut self.chat.pending_retry,
                                &mut self.ledger,
                                &removal.agent.id,
                            );
                        }
                        let title = removed
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
        self.menu.highlight = None;
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
            (Action::OpenFolder, Target::Project { checkout, .. }) => {
                let checkout = checkout.clone();
                self.open_folder(&checkout);
            }
            (Action::OpenFolder, Target::Agent(agent)) => {
                let folder = agent
                    .workspace
                    .clone()
                    .unwrap_or_else(|| agent.project_id.clone());
                self.open_folder(&folder);
            }
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
            (Action::Delete, _) => {
                self.menu.confirm = Some((target, action));
            }
            (
                Action::ArchiveFinished | Action::DeleteStopped,
                Target::Project { key, name, .. },
            ) => {
                let op = if action == Action::ArchiveFinished {
                    super::bulk_ui::BulkOp::ArchiveFinished
                } else {
                    super::bulk_ui::BulkOp::DeleteStopped
                };
                self.start_bulk(
                    op,
                    super::bulk_ui::Pick::Tasks(Some(key.clone())),
                    name.clone(),
                    context,
                );
            }
            (
                Action::RemoveProject,
                Target::Project {
                    checkouts, name, ..
                },
            ) => {
                let ids = self.registrations_in(checkouts);
                if ids.is_empty() {
                    self.alert(format!("{name} is not a registered project."));
                    return;
                }
                // One confirmation naming what stays; a project with a
                // live task is kept, never forced.
                self.start_bulk(
                    super::bulk_ui::BulkOp::RemoveProjects,
                    super::bulk_ui::Pick::Selected(ids),
                    name.clone(),
                    context,
                );
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
            (Action::AttachTo(index), Target::Agent(agent)) => {
                let chosen = self
                    .menu
                    .attach
                    .take()
                    .and_then(|choices| choices.into_iter().nth(index));
                if let Some((project, name)) = chosen {
                    self.submit_organize(
                        format!("Attach to {name}"),
                        client::Organize::Attach {
                            agent: agent.id.clone(),
                            project,
                            revision: agent.organization_revision.unwrap_or(0),
                        },
                        context,
                    );
                }
            }
            (Action::Detach, Target::Agent(agent)) => {
                self.submit_organize(
                    "Detach from project".to_owned(),
                    client::Organize::Detach {
                        agent: agent.id.clone(),
                        revision: agent.organization_revision.unwrap_or(0),
                    },
                    context,
                );
            }
            (Action::EditTags, Target::Agent(agent)) => {
                // One field per tag, so a tag such as `ops, EU` saves back
                // unchanged.
                self.menu.tag_fields = agent.tags.clone();
                self.menu.renaming = Some((Target::Tags(agent.clone()), String::new()));
                self.focus_rename(5, context);
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
            _ => {}
        }
    }

    /// The registration IDs whose checkout is one of `checkouts`.
    pub(super) fn registrations_in(&self, checkouts: &[String]) -> Vec<String> {
        self.nav
            .projects
            .iter()
            .filter(|project| {
                checkouts
                    .iter()
                    .any(|path| nav::path_key(path) == nav::path_key(&project.workspace))
            })
            .map(|project| project.id.clone())
            .collect()
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

    /// One organization mutation (`chat.attach`, `chat.detach`,
    /// `chat.tag`) with a ledger-held id; the refresh after it reads the
    /// committed placement back.
    fn submit_organize(
        &mut self,
        what: String,
        change: client::Organize,
        context: &ComponentContext<Self>,
    ) {
        let key = format!("organize:{change:?}");
        self.submit_lifecycle(&what, key, context, move |data_dir, id| {
            client::block_on(client::organize(data_dir, id, change)).map(|reply| {
                reply
                    .links
                    .iter()
                    .map(|link| {
                        let place = link.project_link.as_deref().unwrap_or("no project");
                        if link.tags.is_empty() {
                            format!("{} in {place}", link.agent)
                        } else {
                            format!("{} in {place}, tags {}", link.agent, link.tags.join(", "))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            })
        });
    }

    /// The registered projects a chat can be attached to, as the sidebar
    /// names them, in the daemon's order.
    fn attach_choices(&self) -> Vec<(String, String)> {
        let checkouts = self.checkouts();
        self.nav
            .organization
            .registered()
            .filter_map(|group| {
                let project = group.project.clone()?;
                let checkout = group.registration.as_ref()?.checkout.clone();
                let name = self
                    .prefs
                    .display_name(&format!("project:{}", nav::group_key(Some(&project))))
                    .map_or_else(|| nav::place_label(&checkout, &checkouts), str::to_owned);
                Some((project, name))
            })
            .collect()
    }

    /// Focus the rename field once it exists (after the next render).
    fn focus_rename(&self, tries: u8, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(60, &cancelled);
            Message::Menu(MenuMsg::FocusRename(tries))
        });
    }

    /// After the next render, bring the highlighted menu row into view.
    fn reveal_highlight(&mut self, context: &ComponentContext<Self>) {
        self.menu.reveal_epoch += 1;
        self.menu.reveal_after = self.renders.get();
        self.post_reveal_highlight(self.menu.reveal_epoch, 0, context);
    }

    fn post_reveal_highlight(&self, epoch: u64, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Menu(MenuMsg::Reveal(epoch))
        });
    }

    /// The menu's entries for a target, in order: (action, label, glyph,
    /// destructive).
    pub(super) fn menu_entries(
        &self,
        target: &Target,
    ) -> Vec<(Action, String, &'static str, bool)> {
        match target {
            Target::Project {
                collapsed,
                checkouts,
                checkout,
                ..
            } => {
                let mut entries = vec![
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
                        Action::ArchiveFinished,
                        "Archive finished tasks…".to_owned(),
                        m3::glyph::ARCHIVE,
                        false,
                    ),
                    (
                        Action::DeleteStopped,
                        "Delete stopped tasks…".to_owned(),
                        m3::glyph::DELETE,
                        true,
                    ),
                ];
                if self.can_launch_here(checkout) {
                    entries.insert(
                        0,
                        (
                            Action::NewTaskHere,
                            "New task here".to_owned(),
                            m3::glyph::NEW_TASK,
                            false,
                        ),
                    );
                }
                // Only a registered checkout can be forgotten.
                if !self.registrations_in(checkouts).is_empty() {
                    entries.push((
                        Action::RemoveProject,
                        "Remove project…".to_owned(),
                        m3::glyph::DELETE,
                        true,
                    ));
                }
                entries
            }
            Target::Tags(_) => Vec::new(),
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
                if let Some(choices) = self
                    .menu
                    .attach
                    .as_ref()
                    .filter(|_| self.menu.open.is_some())
                {
                    return std::iter::once((
                        Action::AttachMenu,
                        "Back".to_owned(),
                        m3::glyph::BACK,
                        false,
                    ))
                    .chain(choices.iter().enumerate().map(|(index, (_, name))| {
                        (
                            Action::AttachTo(index),
                            name.clone(),
                            m3::glyph::FOLDER,
                            false,
                        )
                    }))
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
                // Placement and tags (`chat.attach`/`chat.detach`/`chat.tag`):
                // attach and detach place chats; tags group any task.
                if agent.chat
                    && !agent.pending
                    && self.nav.organization.registered().next().is_some()
                {
                    entries.push((
                        Action::AttachMenu,
                        "Attach to project…".to_owned(),
                        m3::glyph::FOLDER,
                        false,
                    ));
                }
                if agent.chat && agent.organization.is_some() {
                    entries.push((
                        Action::Detach,
                        "Detach from project".to_owned(),
                        m3::glyph::FOLDER,
                        false,
                    ));
                }
                if !agent.native_child && !agent.pending {
                    entries.push((
                        Action::EditTags,
                        if agent.tags.is_empty() {
                            "Tags…".to_owned()
                        } else {
                            format!("Tags: {}…", agent.tags.join(", "))
                        },
                        m3::glyph::TAG,
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
        // No-project chats never had Open folder: their workspace is a
        // private daemon directory, not a place to open. A chat with a
        // project keeps its old entry, as does any task with a folder.
        let place = agent.workspace.as_deref().unwrap_or(&agent.project_id);
        if !agent.project_id.is_empty() || (!agent.chat && !place.is_empty()) {
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
        let height = self.window_height.unwrap_or(tokens::layout::WINDOW_HEIGHT);
        let margin = tokens::spacing::SM;
        // A narrow client caps the outer menu (its padding included)
        // instead of running past the client edge (#683).
        let menu_width = menu_width(width);
        let left = x.min(width - menu_width - margin).max(0.0);
        let entries = self.menu_entries(target);
        let (top, max_height) = menu_placement(*y, entries.len(), height);
        let highlight = self.menu.highlight;
        let rows: Vec<KeyedView> = entries
            .into_iter()
            .enumerate()
            .map(|(index, (action, label, glyph, destructive))| {
                let selected = highlight == Some(index);
                let entry = if destructive {
                    m3::destructive_row(palette, selected)
                } else {
                    m3::row(palette, selected)
                };
                let mut button = entry
                    .automation_name(label.clone())
                    .on_click(context.message(Message::Menu(MenuMsg::Chosen(action))));
                if highlight == Some(index) {
                    // Bound when it renders; focusing it scrolls it into
                    // view (#683).
                    button = button.element_ref(&self.menu.row_ref);
                }
                let title = m3::body(label.clone());
                KeyedView::new(
                    label,
                    button.content(
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::ROW)
                            .children((m3::icon(glyph), title)),
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
            m3::lifted(
                palette,
                crate::theme::elevation::Level::Two,
                tokens::radius::CONTAINER,
                Grid::new()
                    .width(menu_width)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .vertical_alignment(VerticalAlignment::Top)
                    .margin(Thickness::new(left, top, 0.0, 0.0)),
                m3::popover(palette)
                    .automation_name(format!("Actions for {}", target.title()))
                    .content(
                        ScrollViewer::new()
                            .max_height(max_height)
                            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                            .automation_name(format!("Actions for {}", target.title()))
                            .content(StackPanel::new().keyed_children(rows)),
                    ),
            ),
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
            tokens::layout::DIALOG_WIDTH,
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
                .foreground(palette.attention())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .into(),
            None => StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((
                    ProgressRing::new()
                        .is_indeterminate(true)
                        .width(tokens::layout::RING)
                        .height(tokens::layout::RING)
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
            tokens::layout::DIALOG_WIDTH,
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
                .background(palette.tonal())
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

    /// The owner's name for an Agent, else its title.
    pub(super) fn agent_label(&self, agent: &UiAgent) -> String {
        self.prefs
            .display_name(&format!("agent:{}", agent.id))
            .map_or_else(|| agent.title.clone(), str::to_owned)
    }
}

impl DesktopApp {
    /// The menu's Open folder: a folder that is gone says so above the
    /// composer (the menu has closed).
    fn open_folder(&mut self, path: &str) {
        if let Err(note) = super::reveal::reveal(path) {
            self.alert(note);
        }
    }
}

/// A menu row's height: a default button row.
const MENU_ROW_HEIGHT: f64 = tokens::height::CONTROL;
/// The popover's vertical insets around the rows.
const MENU_VERTICAL_PADDING: f64 = 2.0 * tokens::spacing::XS;

/// The menu's width in window DIPs: the fixed width, capped
/// to a narrow client (the same margin both sides) so the outer menu,
/// its padding included, never runs past the client edge (#683). It
/// never collapses below one row's height.
fn menu_width(client_width: f64) -> f64 {
    tokens::layout::CONTEXT_MENU_WIDTH
        .min((client_width - 2.0 * tokens::spacing::SM).max(MENU_ROW_HEIGHT))
}

/// The menu's top edge in window DIPs and its visible height cap: the
/// pointer, moved up when the menu would run past the client bottom. An
/// oversized menu keeps the top margin and scrolls instead of placing
/// actions out of reach (#683).
fn menu_placement(y: f64, rows: usize, height: f64) -> (f64, f64) {
    let margin = tokens::spacing::SM;
    let estimated = rows as f64 * MENU_ROW_HEIGHT + MENU_VERTICAL_PADDING;
    let top = y.min(height - estimated - margin).max(margin);
    let max_height = (height - top - margin).max(MENU_ROW_HEIGHT + MENU_VERTICAL_PADDING);
    (top, max_height)
}

/// The keyboard highlight after moving `step` rows from `current`
/// (wrapping around both ends).
fn highlight_step(current: Option<usize>, step: i32, count: usize) -> Option<usize> {
    if count == 0 {
        return None;
    }
    match current {
        None if step >= 0 => Some(0),
        None => Some(count - 1),
        Some(at) => Some((at as i32 + step).rem_euclid(count as i32) as usize),
    }
}
