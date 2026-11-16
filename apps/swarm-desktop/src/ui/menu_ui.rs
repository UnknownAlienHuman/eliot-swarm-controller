//! The sidebar's right-click menus (owner review 2026-11-16): a project's
//! New task here, Open folder, Rename, Collapse or Expand and Remove
//! project; a chat's or task's Rename, Stop (while it runs), Archive or
//! Unarchive, Delete (stopped only, after a confirmation naming it), Copy
//! Agent ID and Open folder.
//!
//! Reactor 0.100.0 attaches a `Menu` only as a Button's left-click flyout
//! and has no `ContextFlyout`, so the menu is drawn in the window, at the
//! pointer, over a transparent layer that closes it on any press (the
//! composer popovers' pattern); the confirmation is an in-window dialog in
//! the window's own theme (a `ContentDialog` follows the Windows theme).
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
    Archive(bool),
    Delete,
    CopyId,
}

#[derive(Clone, Debug)]
pub(super) enum MenuMsg {
    /// A row's right press: its target and the pointer in window DIPs.
    Requested(Target, f64, f64),
    Closed,
    Chosen(Action),
    RenameChanged(String),
    RenameCommitted,
    RenameCancelled,
    /// The confirmation's answer (true: go ahead).
    Confirmed(bool),
    /// Show archived Agents in the sidebar, or not.
    ArchivedToggled,
    /// An Agent lifecycle call settled (what, the outcome).
    Done(String, Fallible<String>),
    /// Put the keyboard in the rename field (tries left).
    FocusRename(u8),
    /// A left press on a row: nothing to do here (its button acts).
    Ignored,
}

/// The open menu, the rename in progress and the pending confirmation.
#[derive(Default)]
pub(super) struct MenuState {
    pub(super) open: Option<(Target, f64, f64)>,
    /// The target being renamed and the text typed so far.
    pub(super) renaming: Option<(Target, String)>,
    pub(super) confirm: Option<(Target, Action)>,
    pub(super) rename_ref: ElementRef<TextBox>,
}

/// The menu's width.
const MENU_WIDTH: f64 = 220.0;

impl DesktopApp {
    pub(super) fn menu_update(&mut self, msg: MenuMsg, context: &ComponentContext<Self>) {
        match msg {
            MenuMsg::Requested(target, x, y) => {
                self.close_popover();
                self.menu.open = Some((target, x, y));
                crate::shortcuts::set_popover(crate::shortcuts::Popover::Menu);
            }
            MenuMsg::Closed => self.close_menu(),
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
            MenuMsg::Ignored => {}
        }
    }

    pub(super) fn close_menu(&mut self) {
        if self.menu.open.take().is_some() {
            crate::shortcuts::set_popover(crate::shortcuts::Popover::None);
        }
    }

    fn menu_action(&mut self, target: Target, action: Action, context: &ComponentContext<Self>) {
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
                    .map_err(|error| error.to_string())
                });
            }
            (Action::Archive(archived), Target::Agent(agent)) => {
                let what = if archived { "Archive" } else { "Unarchive" };
                let agent = agent.id.clone();
                let key = format!("archive:{agent}:{archived}");
                self.submit_lifecycle(what, key, context, move |data_dir, id| {
                    client::block_on(client::archive_agent(data_dir, id, &agent, archived))
                        .map(|count| format!("{count} Agent(s)"))
                        .map_err(|error| error.to_string())
                });
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
            (Action::Delete, Target::Agent(agent)) => {
                if self.selected_agent_id().as_deref() == Some(agent.id.as_str()) {
                    self.new_task(context);
                }
                let id = agent.id.clone();
                let key = format!("delete:{id}");
                self.submit_lifecycle("Delete", key, context, move |data_dir, mutation| {
                    client::block_on(client::delete_agent(data_dir, mutation, &id))
                        .map(|count| format!("{count} record tree(s) removed"))
                        .map_err(|error| error.to_string())
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
                            .map(|_| "unregistered; its folder and tasks stay".to_owned())
                            .map_err(|error| error.to_string()),
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
        work: impl FnOnce(&Path, String) -> Fallible<String> + Send + 'static,
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
    fn menu_entries(&self, target: &Target) -> Vec<(Action, String, &'static str, bool)> {
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
                let mut entries = vec![(
                    Action::Rename,
                    "Rename".to_owned(),
                    m3::glyph::RENAME,
                    false,
                )];
                if agent.running() && !agent.native_child {
                    entries.push((Action::Stop, "Stop".to_owned(), m3::glyph::STOP, false));
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
                if agent.state == "stopped" && !agent.native_child {
                    entries.push((
                        Action::Delete,
                        "Delete…".to_owned(),
                        m3::glyph::DELETE,
                        true,
                    ));
                }
                entries.push((
                    Action::CopyId,
                    "Copy Agent ID".to_owned(),
                    m3::glyph::COPY,
                    false,
                ));
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
        }
    }

    /// The window-wide layer: the open menu at the pointer (a press outside
    /// closes it) or the confirmation dialog over a scrim.
    pub(super) fn menu_overlay(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        if let Some((target, action)) = &self.menu.confirm {
            return self.confirm_dialog(target, *action, context, palette);
        }
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
        let (title, body, button) = match action {
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
        Grid::new().children((
            Border::new().background(palette.scrim()),
            m3::card_high(palette)
                .width(420.0)
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Center)
                .automation_name(title.clone())
                .content(
                    StackPanel::new().spacing(tokens::spacing::MD).children((
                        m3::section_title(title.clone()).text_wrapping(TextWrapping::Wrap),
                        m3::secondary(body).text_wrapping(TextWrapping::Wrap),
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .horizontal_alignment(HorizontalAlignment::Right)
                            .spacing(tokens::spacing::SM)
                            .children((
                                m3::text_button(palette)
                                    .automation_name("Cancel")
                                    .on_click(
                                        context.message(Message::Menu(MenuMsg::Confirmed(false))),
                                    )
                                    .content(m3::label("Cancel")),
                                m3::destructive(palette)
                                    .automation_name(format!("{button}: {}", target.title()))
                                    .on_click(
                                        context.message(Message::Menu(MenuMsg::Confirmed(true))),
                                    )
                                    .content(m3::label(button)),
                            )),
                    )),
                ),
        ))
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
