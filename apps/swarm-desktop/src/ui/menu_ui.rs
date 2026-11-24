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
//! Each menu is a real WinUI `MenuFlyout`. Reactor 0.100.0 attaches a
//! `Menu` only as a Button's left-click flyout and has no `ContextFlyout`,
//! so the window keeps one row-menu button that is never shown
//! ([`DesktopApp::menu_anchor`], automation id `crate::flyout::MENU_ANCHOR`)
//! carrying the menu of the row last asked for, and `crate::flyout` opens
//! it at a point once the render that attached its items committed: under
//! the row's "…", at the pointer for a right press (it arrives through the
//! row's `Border`, `PointerPressed` with the right button, which a Button
//! leaves unhandled), or under the focused row for Shift+F10 and the
//! context-menu key (`crate::shortcuts`). WinUI gives the menu its keyboard
//! (arrows, Enter, Esc), light dismiss and placement. Reactor's `MenuItem`
//! is text only: before the menu opens, `crate::flyout` gives each native
//! item its glyph (and the danger items the error color) and the menu its
//! accessible name. Every item, here and in the conversation header's "…",
//! is a typed [`Action`] with one fixed label; Reactor reports a chosen
//! item only by that label, the labels are proven distinct at compile time,
//! and the label maps back to its action, which runs only if the opened
//! menu offered it. No project or task name is ever an item (the tags item
//! only shows the current tags). More is a submenu. "Attach to project…" opens its own chooser, a WinUI
//! `Flyout` at the row ([`DesktopApp::attach_anchor`]) whose rows are keyed
//! by the project's id and checkout, never by its name. All modal
//! confirmations use the common M3 body/footer and native focus host in
//! [`super::dialog_ui`].
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

/// Declares [`Action`] from one list, so every action has exactly one label,
/// glyph and danger flag, and [`Action::ALL`] lists every variant.
macro_rules! actions {
    ($($(#[$doc:meta])* $variant:ident => ($label:expr, $glyph:expr, $danger:expr),)*) => {
        /// One menu command of a row menu or the conversation header's "…".
        ///
        /// Reactor 0.100.0's `Menu` reports a chosen item only by its label
        /// (its `on_click` is a `Callback<String>`; each native
        /// `MenuFlyoutItem.Click` enqueues the item's label, never its key),
        /// so the label is the only payload a menu can carry. Each action
        /// owns one fixed label, the labels are proven distinct at compile
        /// time (the assertion below), menus are built from actions and a
        /// chosen label maps back through [`Action::from_label`]: no label
        /// can stand for two actions, in any build.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(super) enum Action {
            $($(#[$doc])* $variant,)*
        }

        impl Action {
            /// Every action, in declaration order.
            pub(super) const ALL: &'static [Action] = &[$(Action::$variant,)*];

            /// The text a menu shows for it and Reactor reports back.
            pub(super) const fn label(self) -> &'static str {
                match self {
                    $(Action::$variant => $label,)*
                }
            }

            /// Its icon in a row menu.
            pub(super) const fn glyph(self) -> &'static str {
                match self {
                    $(Action::$variant => $glyph,)*
                }
            }

            /// Deletes, removes or forces (the error color; a row menu ends
            /// with it).
            pub(super) const fn danger(self) -> bool {
                match self {
                    $(Action::$variant => $danger,)*
                }
            }
        }
    };
}

actions! {
    NewTaskHere => ("New task here", m3::glyph::NEW_TASK, false),
    OpenFolder => ("Open folder", m3::glyph::OPEN_FOLDER, false),
    Rename => ("Rename", m3::glyph::RENAME, false),
    /// Expand a collapsed project.
    Expand => ("Expand", m3::glyph::CHEVRON_DOWN, false),
    /// Collapse a project.
    Collapse => ("Collapse", m3::glyph::CHEVRON_RIGHT, false),
    /// Archive every finished task of the project or subproject.
    ArchiveFinished => ("Archive finished tasks…", m3::glyph::ARCHIVE, false),
    /// Delete every stopped task of the project or subproject.
    DeleteStopped => ("Delete stopped tasks…", m3::glyph::DELETE, true),
    RemoveProject => ("Remove from Swarm…", m3::glyph::DELETE, true),
    /// "Move under another task…": open the picker.
    MoveUnder => ("Move under another task…", m3::glyph::MOVE, false),
    /// "Attach to project…": open the project chooser at the row.
    AttachMenu => ("Attach to project…", m3::glyph::FOLDER, false),
    /// Return the chat to the no-project bucket.
    Detach => ("Detach from project", m3::glyph::FOLDER, false),
    /// Edit the Agent's tags inline (the item shows the current tags).
    EditTags => ("Tags…", m3::glyph::TAG, false),
    Archive => ("Archive", m3::glyph::ARCHIVE, false),
    Unarchive => ("Unarchive", m3::glyph::ARCHIVE, false),
    /// Resume an ended task in its conversation.
    Resume => ("Resume", m3::glyph::RESUME, false),
    Stop => ("Stop", m3::glyph::STOP, false),
    /// The More submenu (never chosen itself).
    More => ("More", m3::glyph::MORE, false),
    Delete => ("Delete…", m3::glyph::DELETE, true),
    CopyId => ("Copy Agent ID", m3::glyph::COPY, false),
    /// The conversation header's Agent details.
    Details => ("Agent details", m3::glyph::INFO, false),
    /// The conversation header's handoff or switch.
    Handoff => ("Hand off or switch…", m3::glyph::FORWARD, false),
    /// Detach Eliot's observer from an observed Agent (header only).
    DetachObserver => ("Detach", m3::glyph::EXTERNAL, false),
    /// The conversation header's Force stop (it asks first).
    ForceStop => ("Force stop…", m3::glyph::STOP, true),
}

/// Whether `a` and `b` hold the same text, in a constant context.
const fn same_text(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut at = 0;
    while at < a.len() {
        if a[at] != b[at] {
            return false;
        }
        at += 1;
    }
    true
}

// Compile time: no two actions share a label, so a reported label names
// exactly one action.
const _: () = {
    let all = Action::ALL;
    let mut first = 0;
    while first < all.len() {
        let mut second = first + 1;
        while second < all.len() {
            assert!(
                !same_text(all[first].label(), all[second].label()),
                "two menu actions share a label"
            );
            second += 1;
        }
        first += 1;
    }
};

impl Action {
    /// The action whose label Reactor reported, by an exhaustive search of
    /// [`Action::ALL`].
    pub(super) fn from_label(label: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|action| action.label() == label)
    }
}

/// One item of a row menu: a command, or a submenu of commands.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Entry {
    Item {
        action: Action,
        /// What the item shows in place of its label (Tags lists the
        /// current tags); display only: the native item's text is replaced
        /// as the menu opens, and a choice still reports the label.
        display: Option<String>,
    },
    Submenu {
        action: Action,
        items: Vec<Entry>,
    },
}

impl Entry {
    pub(super) fn item(action: Action) -> Self {
        Self::Item {
            action,
            display: None,
        }
    }

    fn action(&self) -> Action {
        match self {
            Self::Item { action, .. } | Self::Submenu { action, .. } => *action,
        }
    }

    /// Whether it deletes or removes.
    pub(super) fn is_destructive(&self) -> bool {
        matches!(self, Self::Item { action, .. } if action.danger())
    }

    /// Whether it is the More submenu (the conversation header has its
    /// own).
    pub(super) fn is_more(&self) -> bool {
        matches!(
            self,
            Self::Submenu {
                action: Action::More,
                ..
            }
        )
    }
}

/// The automation id prefix of a sidebar row's "…" button, followed by the
/// row's key (`agent:<id>`, `project:<key>`); its menu opens under it.
pub(super) const MORE_ID_PREFIX: &str = "eliot-more:";

/// Reactor's menu items for `entries`, each keyed and labelled by its
/// action's label.
pub(super) fn menu_items(entries: &[Entry]) -> Vec<MenuItem> {
    entries
        .iter()
        .map(|entry| {
            let label = entry.action().label();
            match entry {
                Entry::Item { .. } => MenuItem::item(label, label),
                Entry::Submenu { items, .. } => MenuItem::submenu(label, label, menu_items(items)),
            }
        })
        .collect()
}

/// Whether the menu `entries` offers `action`, at any depth: a choice runs
/// only in the menu that offered it.
pub(super) fn offers(entries: &[Entry], action: Action) -> bool {
    entries.iter().any(|entry| match entry {
        Entry::Item { action: item, .. } => *item == action,
        Entry::Submenu { items, .. } => offers(items, action),
    })
}

/// How each command of `entries` looks natively: its glyph, danger tint and
/// display text, by label (`crate::flyout::MenuDress`).
fn entry_looks(entries: &[Entry], looks: &mut Vec<crate::flyout::MenuLook>) {
    for entry in entries {
        let action = entry.action();
        let display = match entry {
            Entry::Item { display, .. } => display.clone(),
            Entry::Submenu { items, .. } => {
                entry_looks(items, looks);
                None
            }
        };
        looks.push(crate::flyout::MenuLook {
            label: action.label(),
            display,
            glyph: action.glyph(),
            danger: action.danger(),
        });
    }
}

#[derive(Clone, Debug)]
pub(super) enum MenuMsg {
    /// Open the menu of a row at a point in window DIPs (a right press,
    /// Shift+F10 or the context-menu key); true when the row itself asked
    /// (the keyboard or its "…"), so focus goes back to it if the menu
    /// closes without a choice.
    Requested(Target, f64, f64, bool),
    /// A row's "…": its menu under the button.
    Opened(Target),
    /// Show the menu or attach chooser asked for with this epoch once its
    /// render committed (tries left).
    Show(u64, u8),
    /// A menu item was chosen (mapped from the label Reactor reports by
    /// [`Action::from_label`]).
    Picked(Action),
    /// A row of the attach chooser was chosen: the project's id and
    /// checkout.
    AttachPicked(String, String),
    /// A menu or attach chooser shown with this epoch closed.
    Closed(u64),
    /// The row menu of this epoch closed without a choice: give its row
    /// the keyboard back.
    ReturnFocus(u64),
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

/// The row menu the window's menu button carries: what it acts on, its
/// items as they were built, and where it opens (window DIPs).
pub(super) struct OpenMenu {
    pub(super) target: Target,
    pub(super) entries: Vec<Entry>,
    x: f64,
    y: f64,
    /// Opened from the row itself (Shift+F10, the context-menu key or its
    /// "…"): closed without a choice, it gives keyboard focus back to the
    /// row (the "…" leaves the tree as the menu takes focus).
    return_focus: bool,
}

/// One row of the attach chooser: a registered project, by its id and
/// checkout; `name` is only shown.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct AttachChoice {
    project: String,
    checkout: String,
    name: String,
}

/// The open "Attach to project…" chooser: the chat it places, its rows,
/// where it opens (window DIPs) and its show epoch.
pub(super) struct AttachChooser {
    agent: UiAgent,
    choices: Vec<AttachChoice>,
    x: f64,
    y: f64,
    epoch: u64,
    /// Renders composed when it was asked for: it opens after the render
    /// that attaches its flyout.
    show_after: u64,
}

/// The row menu last asked for, the rename in progress and the pending
/// confirmation.
#[derive(Default)]
pub(super) struct MenuState {
    /// Kept after it closes, so a chosen item still finds its action; the
    /// next row's menu replaces it.
    pub(super) open: Option<OpenMenu>,
    /// The attach chooser, while it is open.
    pub(super) attach: Option<AttachChooser>,
    /// The Enter presses counted when the rename or tag edit started
    /// (`crate::shortcuts::enter_presses`): the Enter that chose it in the
    /// menu must not commit it.
    pub(super) rename_enter: u64,
    /// The epoch of the last show (`crate::flyout`).
    epoch: u64,
    /// Renders composed when the menu was asked for: it opens after the
    /// render that attaches its items.
    show_after: u64,
    /// The menu is open (shown, its `Closed` not seen yet).
    shown: bool,
    /// An item of the last menu asked for was chosen.
    picked: bool,
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
    /// "Move under another task…" is open.
    pub(super) moving: Option<super::move_ui::MoveDialog>,
    /// "Archived · Undo" after an archive: the Agent, whether it was the
    /// open conversation, and the notice's epoch (it expires).
    pub(super) archived: Option<(UiAgent, bool, u64)>,
    pub(super) archived_epoch: u64,
}

/// How long the "Archived · Undo" notice stays.
const ARCHIVED_NOTICE_MS: u64 = 10_000;

/// Tries to open a row menu after its render, one frame apart.
const MENU_SHOW_TRIES: u8 = 10;
const MENU_SHOW_RETRY_MS: u64 = 16;
/// How long a closed row menu waits for its chosen item's click before it
/// gives focus back to its row.
const RETURN_FOCUS_MS: u64 = 60;

impl DesktopApp {
    pub(super) fn menu_update(&mut self, msg: MenuMsg, context: &ComponentContext<Self>) {
        match msg {
            MenuMsg::Requested(target, x, y, return_focus) => {
                self.close_popover();
                // A menu shown now closes first (Shift+F10 twice, or "…"
                // then a right press): its watch ends with it, and Reactor
                // never swaps the native menu under an open one, whose
                // clicks would be lost.
                self.close_menu();
                let entries = self.menu_entries(&target);
                self.menu.open = Some(OpenMenu {
                    target,
                    entries,
                    x,
                    y,
                    return_focus,
                });
                self.menu.picked = false;
                self.menu.epoch = crate::flyout::next_epoch();
                self.menu.show_after = self.renders.get();
                self.post_menu_show(self.menu.epoch, MENU_SHOW_TRIES, 0, context);
            }
            MenuMsg::Opened(target) => {
                // Under the "…" just pressed or invoked; at the pointer
                // when it cannot be placed.
                let (x, y) = crate::motion::element_bottom_left(&format!(
                    "{MORE_ID_PREFIX}{}",
                    target.name_key()
                ))
                .unwrap_or_else(crate::shortcuts::pointer_position);
                self.menu_update(MenuMsg::Requested(target, x, y, true), context);
            }
            MenuMsg::Show(epoch, tries)
                if self
                    .menu
                    .attach
                    .as_ref()
                    .is_some_and(|chooser| chooser.epoch == epoch) =>
            {
                self.show_attach(epoch, tries, context);
            }
            MenuMsg::Show(epoch, tries) => {
                let Some(open) = self.menu.open.as_ref().filter(|_| epoch == self.menu.epoch)
                else {
                    return;
                };
                let left = tries.saturating_sub(1);
                // The render that attaches the menu's items comes first.
                let shown = if self.renders.get() <= self.menu.show_after {
                    Err("its render has not committed".to_owned())
                } else {
                    let mut items = Vec::new();
                    entry_looks(&open.entries, &mut items);
                    let name = format!("Actions for {}", open.target.title());
                    let dress = crate::flyout::MenuDress {
                        name: &name,
                        items: &items,
                        danger: self.palette().roles().map(|roles| {
                            (
                                roles.attention_text.r,
                                roles.attention_text.g,
                                roles.attention_text.b,
                            )
                        }),
                    };
                    crate::flyout::show_menu_at(
                        crate::flyout::MENU_ANCHOR,
                        open.x,
                        open.y,
                        epoch,
                        &dress,
                    )
                };
                match shown {
                    Ok(()) => self.menu.shown = true,
                    Err(_) if left > 0 => {
                        self.post_menu_show(epoch, left, MENU_SHOW_RETRY_MS, context);
                    }
                    Err(error) => crate::startup::note(&format!("row menu not shown: {error}")),
                }
            }
            MenuMsg::Picked(action) => {
                let Some(open) = &self.menu.open else {
                    return;
                };
                self.menu.picked = true;
                // Only an action the opened menu offered runs.
                if offers(&open.entries, action) {
                    let target = open.target.clone();
                    self.menu_action(target, action, context);
                }
            }
            MenuMsg::AttachPicked(project, checkout) => {
                let Some(chooser) = self.menu.attach.take() else {
                    return;
                };
                crate::flyout::hide(crate::flyout::ATTACH_ANCHOR);
                let Some(choice) = chooser
                    .choices
                    .into_iter()
                    .find(|choice| choice.project == project && choice.checkout == checkout)
                else {
                    return;
                };
                let agent = chooser.agent;
                self.submit_organize(
                    format!("Attach to {}", choice.name),
                    client::Organize::Attach {
                        agent: agent.id.clone(),
                        project: choice.project,
                        revision: agent.organization_revision.unwrap_or(0),
                    },
                    context,
                );
            }
            MenuMsg::Closed(epoch)
                if self
                    .menu
                    .attach
                    .as_ref()
                    .is_some_and(|chooser| chooser.epoch == epoch) =>
            {
                // Esc or a press outside it.
                self.menu.attach = None;
            }
            MenuMsg::Closed(epoch) => {
                if epoch != self.menu.epoch {
                    return;
                }
                self.menu.shown = false;
                // WinUI puts focus back where it was as the menu closes:
                // a rename or tag edit the menu started takes it now.
                if self.menu.renaming.is_some() {
                    self.focus_rename(5, context);
                } else if self
                    .menu
                    .open
                    .as_ref()
                    .is_some_and(|open| open.return_focus)
                {
                    // The chosen item's click may still be on its way.
                    let _task = context.spawn_background(move |cancel| {
                        let _ = feed::backoff(RETURN_FOCUS_MS, &|| cancel.is_cancelled());
                        Message::Menu(MenuMsg::ReturnFocus(epoch))
                    });
                }
            }
            MenuMsg::ReturnFocus(epoch) => {
                // Closed without a choice (Esc, a press outside): the row
                // the menu came from takes the keyboard back, unless
                // something else took it since.
                let Some(open) = self.menu.open.as_ref() else {
                    return;
                };
                if epoch == self.menu.epoch
                    && !self.menu.picked
                    && !self.menu.shown
                    && self.menu.renaming.is_none()
                    && self.menu.attach.is_none()
                    && self.dialog_kind() == 0
                    && !crate::motion::focus_row(&open.target.name_key())
                {
                    crate::startup::note("row menu closed: its row could not take focus back");
                }
            }
            MenuMsg::Hovered(agent) => self.menu.hovered = agent,
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

    /// Close the row menu and the attach chooser if they are open (a dialog
    /// takes over, another menu replaces it). A show still pending is
    /// cancelled: it carries the epoch this replaces.
    pub(super) fn close_menu(&mut self) {
        self.menu.epoch = crate::flyout::next_epoch();
        if std::mem::take(&mut self.menu.shown) {
            crate::flyout::hide(crate::flyout::MENU_ANCHOR);
        }
        if self.menu.attach.take().is_some() {
            crate::flyout::hide(crate::flyout::ATTACH_ANCHOR);
        }
    }

    /// Open the attach chooser for `agent` under its sidebar row (where its
    /// menu opened when the row is not in view, else at the pointer).
    fn open_attach(&mut self, agent: UiAgent, context: &ComponentContext<Self>) {
        let choices = self.attach_choices();
        if choices.is_empty() {
            self.alert("No registered project to attach this chat to.");
            return;
        }
        let row = format!("{}agent:{}", crate::motion::ROW_ID_PREFIX, agent.id);
        let (x, y) = crate::motion::element_bottom_left(&row)
            .or_else(|| {
                self.menu
                    .open
                    .as_ref()
                    .filter(|open| matches!(&open.target, Target::Agent(row) if row.id == agent.id))
                    .map(|open| (open.x, open.y))
            })
            .unwrap_or_else(crate::shortcuts::pointer_position);
        let epoch = crate::flyout::next_epoch();
        self.menu.attach = Some(AttachChooser {
            agent,
            choices,
            x,
            y,
            epoch,
            show_after: self.renders.get(),
        });
        self.post_menu_show(epoch, MENU_SHOW_TRIES, 0, context);
    }

    /// Show the attach chooser of `epoch` once the render that attached its
    /// flyout committed; out of tries, it closes and says why in the log.
    fn show_attach(&mut self, epoch: u64, tries: u8, context: &ComponentContext<Self>) {
        let Some(chooser) = &self.menu.attach else {
            return;
        };
        let left = tries.saturating_sub(1);
        let shown = if self.renders.get() <= chooser.show_after {
            Err("its render has not committed".to_owned())
        } else {
            crate::flyout::show(crate::flyout::ATTACH_ANCHOR, epoch)
        };
        match shown {
            Ok(()) => {}
            Err(_) if left > 0 => self.post_menu_show(epoch, left, MENU_SHOW_RETRY_MS, context),
            Err(error) => {
                crate::startup::note(&format!("attach chooser not shown: {error}"));
                self.menu.attach = None;
            }
        }
    }

    /// After `delay_ms`, try to show the row menu of `epoch`.
    fn post_menu_show(
        &self,
        epoch: u64,
        tries: u8,
        delay_ms: u64,
        context: &ComponentContext<Self>,
    ) {
        if tries == 0 {
            return;
        }
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Menu(MenuMsg::Show(epoch, tries))
        });
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
            (Action::Expand | Action::Collapse, Target::Project { key, .. }) => {
                self.shell_update(ShellMsg::ProjectToggled(key.clone()), context);
            }
            (Action::Rename, _) => {
                let current = self
                    .prefs
                    .display_name(&target.name_key())
                    .unwrap_or(target.title())
                    .to_owned();
                self.menu.renaming = Some((target, current));
                self.menu.rename_enter = crate::shortcuts::enter_presses();
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
            (Action::Archive | Action::Unarchive, Target::Agent(agent)) => {
                self.submit_archive(agent.clone(), action == Action::Archive, false, context);
            }
            (Action::MoveUnder, Target::Agent(agent)) => {
                self.move_update(super::move_ui::MoveMsg::Open(agent.clone()), context);
            }
            (Action::AttachMenu, Target::Agent(agent)) => {
                self.open_attach(agent.clone(), context);
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
                self.menu.rename_enter = crate::shortcuts::enter_presses();
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

    /// The registered projects a chat can be attached to, in the daemon's
    /// order: each by its id and checkout, with the name the sidebar shows.
    fn attach_choices(&self) -> Vec<AttachChoice> {
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
                Some(AttachChoice {
                    project,
                    checkout,
                    name,
                })
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

    /// The menu's entries for a target, in order: the destructive one last.
    /// Every item is an [`Action`] with its fixed label (unique at compile
    /// time); the tags item only shows the current tags.
    pub(super) fn menu_entries(&self, target: &Target) -> Vec<Entry> {
        match target {
            Target::Project {
                collapsed,
                checkouts,
                checkout,
                ..
            } => {
                let mut entries = Vec::new();
                if self.can_launch_here(checkout) {
                    entries.push(Entry::item(Action::NewTaskHere));
                }
                entries.extend([
                    Entry::item(Action::OpenFolder),
                    Entry::item(Action::Rename),
                    Entry::item(if *collapsed {
                        Action::Expand
                    } else {
                        Action::Collapse
                    }),
                    Entry::item(Action::ArchiveFinished),
                    Entry::item(Action::DeleteStopped),
                ]);
                // Only a registered checkout can be forgotten.
                if !self.registrations_in(checkouts).is_empty() {
                    entries.push(Entry::item(Action::RemoveProject));
                }
                entries
            }
            Target::Tags(_) => Vec::new(),
            Target::Agent(agent) => {
                let mut entries = vec![Entry::item(Action::Rename)];
                if !agent.native_child {
                    entries.push(Entry::item(Action::MoveUnder));
                }
                // Placement and tags (`chat.attach`/`chat.detach`/`chat.tag`):
                // attach and detach place chats; tags group any task. The
                // projects are rows of the attach chooser, never items here.
                if agent.chat
                    && !agent.pending
                    && self.nav.organization.registered().next().is_some()
                {
                    entries.push(Entry::item(Action::AttachMenu));
                }
                if agent.chat && agent.organization.is_some() {
                    entries.push(Entry::item(Action::Detach));
                }
                if !agent.native_child && !agent.pending {
                    entries.push(Entry::Item {
                        action: Action::EditTags,
                        display: (!agent.tags.is_empty())
                            .then(|| format!("Tags: {}…", agent.tags.join(", "))),
                    });
                }
                if !agent.native_child {
                    entries.push(Entry::item(if agent.archived {
                        Action::Unarchive
                    } else {
                        Action::Archive
                    }));
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
                    entries.push(Entry::item(Action::Resume));
                }
                if agent.live() && !agent.native_child {
                    entries.push(Entry::item(Action::Stop));
                }
                entries.push(Entry::Submenu {
                    action: Action::More,
                    items: self.developer_entries(target),
                });
                // The destructive action ends the menu.
                if !agent.native_child && agent.deletable() {
                    entries.push(Entry::item(Action::Delete));
                }
                entries
            }
        }
    }

    /// The developer commands of a task (its menu's More): Copy Agent ID
    /// and Open folder.
    pub(super) fn developer_entries(&self, target: &Target) -> Vec<Entry> {
        let Target::Agent(agent) = target else {
            return Vec::new();
        };
        let mut entries = vec![Entry::item(Action::CopyId)];
        // No-project chats have only a private daemon workspace. Other
        // tasks need a real workspace or an absolute legacy project path.
        if agent_folder(agent).is_some() {
            entries.push(Entry::item(Action::OpenFolder));
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

    /// The window's row-menu button. It is never shown (no size, not
    /// enabled, so never a tab stop): it carries the row menu last asked
    /// for as its `MenuFlyout` (`crate::flyout` opens it at a point) and,
    /// like the menu, is named "Actions for {title}". The items and the
    /// capture-free callback compare equal from one render to the next, so
    /// Reactor keeps the native menu while it is open (a replaced menu
    /// drops the click on the open one).
    pub(super) fn menu_anchor(&self, context: &mut ViewContext<Self>) -> View {
        let items = self
            .menu
            .open
            .as_ref()
            .map(|open| menu_items(&open.entries))
            .unwrap_or_default();
        let name = self.menu.open.as_ref().map_or_else(
            || "Actions".to_owned(),
            |open| format!("Actions for {}", open.target.title()),
        );
        Button::new()
            .automation_id(crate::flyout::MENU_ANCHOR)
            .automation_name(name)
            .is_enabled(false)
            .width(0.0)
            .height(0.0)
            .min_width(0.0)
            .min_height(0.0)
            .horizontal_alignment(HorizontalAlignment::Left)
            .vertical_alignment(VerticalAlignment::Top)
            .menu(Menu::new(
                items,
                context.callback(|label: String| {
                    Action::from_label(&label).map_or(Message::Menu(MenuMsg::Ignored), |action| {
                        Message::Menu(MenuMsg::Picked(action))
                    })
                }),
            ))
    }

    /// The window's attach-chooser button: no size, at the point under the
    /// row where the chooser opens. While the chooser is open it carries it
    /// as its `Flyout` (`crate::flyout` opens it at the button), one row
    /// per registered project keyed by the project's id and checkout; the
    /// name is only shown ([`AttachRows`]). Enabled only then (a flyout
    /// opened at a disabled target would disable its content); it is out of
    /// the tab order otherwise.
    pub(super) fn attach_anchor(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let button = Button::new()
            .automation_id(crate::flyout::ATTACH_ANCHOR)
            .width(0.0)
            .height(0.0)
            .min_width(0.0)
            .min_height(0.0)
            .horizontal_alignment(HorizontalAlignment::Left)
            .vertical_alignment(VerticalAlignment::Top);
        let Some(chooser) = &self.menu.attach else {
            return button.is_enabled(false).into();
        };
        // A component: its rows render again only when the projects, the
        // chat's placement or the palette change, not on every window render.
        let content = View::component::<AttachRows>(AttachRowsInput {
            palette,
            title: self.agent_label(&chooser.agent),
            current: chooser.agent.organization.clone(),
            choices: chooser.choices.clone(),
            send: context.forward(),
        });
        button
            .margin(Thickness::new(chooser.x, chooser.y, 0.0, 0.0))
            .flyout_with(
                Flyout::rich(Border::new().content(content))
                    .placement(FlyoutPlacement::BottomEdgeAlignedLeft),
            )
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

/// What the attach chooser shows: its rows render again only when this
/// changes.
#[derive(Clone, PartialEq)]
pub(super) struct AttachRowsInput {
    palette: Palette,
    /// The chat's name.
    title: String,
    /// The project the chat is placed in now.
    current: Option<String>,
    choices: Vec<AttachChoice>,
    send: Callback<Message>,
}

/// The attach chooser's content: a caption and one row per registered
/// project, keyed and chosen by the project's id and checkout.
pub(super) struct AttachRows {
    send: Callback<Message>,
}

impl Component for AttachRows {
    type Input = AttachRowsInput;
    type Message = Message;

    fn create(input: &AttachRowsInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            send: input.send.clone(),
        }
    }

    fn input_changed(&mut self, input: &AttachRowsInput, _context: &ComponentContext<Self>) {
        self.send = input.send.clone();
    }

    fn update(&mut self, message: Message, _context: &ComponentContext<Self>) {
        let _ = self.send.call(message);
    }

    fn view(&self, input: &AttachRowsInput, context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
        let rows: Vec<KeyedView> = input
            .choices
            .iter()
            .map(|choice| {
                let path = nav::display_path(&choice.checkout);
                let current = input.current.as_deref() == Some(choice.project.as_str());
                KeyedView::new(
                    format!("{}\u{1f}{}", choice.project, choice.checkout),
                    m3::row(palette, false)
                        .automation_name(format!("{}, {path}", choice.name))
                        .on_click(context.message(Message::Menu(MenuMsg::AttachPicked(
                            choice.project.clone(),
                            choice.checkout.clone(),
                        ))))
                        .content(m3::row_content(
                            Some(m3::glyph::FOLDER),
                            &choice.name,
                            &path,
                            if current {
                                m3::icon(m3::glyph::CHECK)
                            } else {
                                View::empty()
                            },
                        )),
                )
            })
            .collect();
        StackPanel::new()
            .width(tokens::layout::MENU_WIDTH)
            .spacing(tokens::spacing::XS)
            .automation_name(format!("Attach {} to a project", input.title))
            .children((
                m3::secondary("Attach to project")
                    .margin(Thickness::xy(tokens::spacing::INSET, tokens::spacing::XS)),
                ScrollViewer::new()
                    .max_height(tokens::layout::POPOVER_LIST_HEIGHT)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .content(StackPanel::new().keyed_children(rows)),
            ))
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
