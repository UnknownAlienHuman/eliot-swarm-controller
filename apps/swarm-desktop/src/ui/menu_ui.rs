//! The sidebar's right-click menus (owner review 2026-11-16): a project's
//! or subproject's New task here, Open folder, Rename, Collapse or Expand,
//! Archive finished tasks, Delete stopped tasks and Remove from Swarm (k142,
//! through [`super::bulk_ui`]); a chat's or task's Rename, Stop (while it is live, idle
//! included), Move under another task (k113, [`super::move_ui`]), Resume
//! (a lost one), Archive or Unarchive, Delete, Copy Agent ID and Open folder.
//!
//! Delete follows the Store's rule (`docs/spec/core.md`, explicit Agent
//! tree removal): only a stopped or failed tree, once drained. Its
//! confirmation names the records, worktrees and native chats in scope.
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
    /// The sidebar row holding keyboard focus (`crate::motion`).
    pub(super) focused: Option<String>,
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
    /// "Move under another task…" is open.
    pub(super) moving: Option<super::move_ui::MoveDialog>,
    /// "Archived · Undo" after an archive: the Agent, whether it was the
    /// open conversation, and the notice's epoch (it expires).
    pub(super) archived: Option<(UiAgent, bool, u64)>,
    pub(super) archived_epoch: u64,
}

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
                if let Some(folder) = agent_folder(agent) {
                    self.open_folder(folder);
                }
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
            (Action::Delete, Target::Agent(agent)) if !agent.native_child && agent.deletable() => {
                self.menu.confirm = Some((target, action));
            }
            (Action::Delete, Target::Agent(_)) => {
                self.alert("Delete is available only for stopped or failed task trees.");
                self.shell_update(ShellMsg::RefreshRequested, context);
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
                self.stop_agent(&agent.id, agent.generation, context);
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
        if let (Action::Delete, Target::Agent(agent)) = (action, target) {
            if self.selected_agent_id().as_deref() == Some(agent.id.as_str()) {
                self.new_task(context);
            }
            let id = agent.id;
            let key = format!("delete:{id}");
            self.submit_lifecycle("Delete", key, context, move |data_dir, mutation| {
                client::block_on(client::delete_agent(data_dir, mutation, &id)).map(|count| {
                    format!(
                        "{} removed",
                        eliot_core::count_words(count as u64, "task", "tasks")
                    )
                })
            });
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
                        "Remove from Swarm…".to_owned(),
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
                entries.push((
                    Action::More,
                    "More".to_owned(),
                    m3::glyph::CHEVRON_RIGHT,
                    false,
                ));
                // The destructive action ends the menu.
                if !agent.native_child && agent.deletable() {
                    entries.push((
                        Action::Delete,
                        "Delete…".to_owned(),
                        m3::glyph::DELETE,
                        true,
                    ));
                }
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
        // No-project chats have only a private daemon workspace. Other
        // tasks need a real workspace or an absolute legacy project path.
        if agent_folder(agent).is_some() {
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
        let (title, body, button) = match target {
            Target::Agent(agent) if action == Action::Delete => {
                let history = match agent.harness.as_str() {
                    "claude" => "Claude Code keeps its own chat history.",
                    "muse" => "Muse keeps its own chat history.",
                    "antigravity" => "Antigravity keeps its own chat history.",
                    _ => "Claude Code, Muse and Antigravity keep their own chat history.",
                };
                let body = format!(
                    "Swarm removes this task tree, its records and clean, merged worktrees it created. Project checkouts and Git branches stay on disk. Swarm archives eligible Codex chats and deletes eligible OpenCode chats it created. Attached and observed chats stay with their app. {history}"
                );
                (
                    format!("Delete “{}” and its task tree?", target.title()),
                    body,
                    "Delete",
                )
            }
            _ => (
                format!("Remove “{}” from Swarm?", target.title()),
                "Swarm removes this project registration. The checkout stays on disk.".to_owned(),
                "Remove from Swarm",
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

/// The filesystem location Open folder may reveal. A registered project
/// identifier by itself is not a path; old rows may retain an absolute path.
fn agent_folder(agent: &UiAgent) -> Option<&str> {
    if agent.chat && agent.project_id.is_empty() {
        return None;
    }
    agent
        .workspace
        .as_deref()
        .filter(|workspace| !workspace.is_empty())
        .or_else(|| {
            let project = agent.project_id.as_str();
            std::path::Path::new(project)
                .is_absolute()
                .then_some(project)
        })
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
