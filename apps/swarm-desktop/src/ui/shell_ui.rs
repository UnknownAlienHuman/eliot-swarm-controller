//! The one shell: the sidebar (Search tasks, New task, collapsible
//! projects with their Agents nested, Recents, the fleet status and
//! Settings), the main area (one conversation or the Settings page) and the
//! optional right panel (the monitor or the open Agent's details).

use std::sync::mpsc::{Receiver, RecvTimeoutError};

use super::menu_ui::{Target, with_menu};
use super::*;
use crate::nav::{Branch, Checkout, Fleet, Row, Section, Shown};
use crate::settings::{Startup, ThemeChoice};
use crate::shortcuts::{Input, Shortcut};
use crate::theme::StatusTone;
use crate::theme::winui::Palette;

/// What the main area shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Main {
    Conversation,
    Settings,
}

/// The optional right panel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Panel {
    Closed,
    /// Every Agent as a simple card.
    Monitor,
    /// The open conversation's Agent: settings, coverage, resources, usage
    /// and Change manager.
    Details,
}

#[derive(Clone, Debug)]
pub(super) enum ShellMsg {
    /// The daemon runs (started when needed), the opened checkout was
    /// discovered, and the settings were read.
    DaemonReady(Fallible<(Option<String>, eliot_core::api::SettingsReadResponse)>),
    /// The owner chose a fresh private data directory (unsafe default).
    UseFreshDirectory,
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    AgentsLoaded(u64, bool, Fallible<AgentPage>),
    /// A sidebar row: open this Agent's conversation.
    AgentOpened(UiAgent),
    /// A project's new-task button: a new task in this checkout.
    ProjectPicked(String),
    /// A project row: collapse or expand the project (its section key).
    ProjectToggled(String),
    /// A project's "Show N older" row: unfold or fold its older tasks.
    OlderToggled(String),
    /// The attention row: list only the Agents that need attention, or all.
    /// The search box's text (typed, or a suggestion WinUI wrote into it).
    SearchChanged(String),
    /// A search suggestion chosen (clicked, or reached with the arrows).
    SearchChosen(String),
    /// A chord, a popover key or Enter from the keyboard hook.
    Shortcut(Shortcut),
    /// Try focusing the search box again (tries left): WinUI refused it,
    /// as it does right after the window is activated, or the box did not
    /// exist yet (Settings were open).
    FocusSearch(u8),
    /// Wait, then send [`ShellMsg::FocusSearch`] with these tries.
    FocusSearchLater(u8),
    /// The shortcut watcher stopped (window closing): nothing to do.
    ShortcutsEnded,
    RefreshRequested,
    NewTask,
    MoreAgentsRequested,
    SettingsOpened,
    /// Open Settings at one page (the sidebar's Automations and Usage).
    SettingsPage(settings_ui::Page),
    /// The Settings navigation's search field.
    SettingsSearch(String),
    /// The Settings navigation's Back row: the conversation again.
    SettingsClosed,
    PanelToggled(Panel),
    /// The window's actual Light/Dark scheme (Reactor `on_color_scheme`).
    SchemeObserved(ColorScheme),
    /// The window's client size (Reactor `on_window_size`).
    WindowResized(WindowSize),
    ThemeChosen(ThemeChoice),
    StartupChosen(Startup),
    /// The re-created editors of this re-prime exist: give them their text.
    FieldsPrimed(u64),
    /// The periodic High Contrast reading.
    HighContrastRead(bool),
    /// Expand (true) or collapse an Agent's children.
    ExpandToggled(String, bool),
    /// Tree epoch, manager id and its direct-children page.
    BranchLoaded(u64, String, Fallible<AgentPage>),
    /// The preferences and window size written to `[desktop]` (or not).
    PreferencesSaved(
        crate::settings::Preferences,
        Option<(u32, u32)>,
        Fallible<eliot_core::api::SettingsSaveResponse>,
    ),
    AlertDismissed,
    /// Re-read the listed Agents (states, Recents order).
    ListTick,
}

/// Reactor 0.100.0 reports the window's Light/Dark scheme only, and a High
/// Contrast theme can keep the same base scheme, so High Contrast is read
/// at the monitor's cadence.
const HIGH_CONTRAST_POLL_MS: u64 = 2_000;
/// Re-check for the empty editor render one frame later.
const PRIME_RETRY_MS: u64 = 16;
/// The sidebar's padding around its rows.
const SIDEBAR_PADDING: f64 = tokens::spacing::SM;
/// Rows in Recents.
const RECENTS: usize = 8;
/// Focus attempts for Ctrl+K, and the wait between them: right after a
/// cold start WinUI refuses focus until the window's content is ready.
const SEARCH_FOCUS_TRIES: u8 = 20;
const SEARCH_FOCUS_RETRY_MS: u64 = 100;
/// A suggestion WinUI reports as chosen this soon after ↑/↓ was reached
/// with the arrows: it is only highlighted.
const ARROW_CHOICE_MS: u64 = 500;
/// How long the shortcut watcher waits before checking for cancellation.
const SHORTCUT_POLL_MS: u64 = 500;
/// The sidebar's states and Recents follow the daemon at this cadence.
const LIST_REFRESH_MS: u64 = 10_000;

fn window_theme(choice: ThemeChoice) -> WindowTheme {
    match choice {
        ThemeChoice::System => WindowTheme::System,
        ThemeChoice::Light => WindowTheme::Light,
        ThemeChoice::Dark => WindowTheme::Dark,
    }
}

/// The scheme the window actually shows, and High Contrast.
pub(super) struct Appearance {
    pub(super) dark: bool,
    pub(super) high_contrast: bool,
}

impl Appearance {
    pub(super) fn new() -> Self {
        Self {
            dark: false,
            high_contrast: crate::accessibility::high_contrast_on(),
        }
    }

    pub(super) fn palette(&self) -> theme::winui::Palette {
        theme::winui::Palette::new(self.dark, self.high_contrast)
    }
}

/// Projects, the listed Agents, expanded branches, titles and the selection.
pub(super) struct ShellState {
    pub(super) selection: Selection,
    pub(super) projects: Vec<UiProject>,
    /// Listed pages, in daemon order.
    pub(super) agents: Vec<UiAgent>,
    /// Direct-children reads of expanded managers.
    pub(super) branches: HashMap<String, Branch>,
    pub(super) expanded: HashSet<String>,
    /// Tags branch reads; a reply from before a refresh is dropped.
    pub(super) tree_epoch: u64,
    pub(super) agents_next: Option<String>,
    pub(super) agents_total: usize,
    /// Archived Agents the list leaves out (the fleet line counts the
    /// live ones); empty while Show archived lists them.
    pub(super) archived: nav::Archived,
    pub(super) agents_loading: bool,
    /// Tags agent-list loads; a reply for an older request is dropped.
    pub(super) list_epoch: u64,
    pub(super) pending_select: Option<UiAgent>,
    /// Section keys of collapsed projects (`[desktop] collapsed_projects`,
    /// written from [`DesktopApp::collapsed_projects_changed`]).
    pub(super) collapsed: HashSet<String>,
    /// Section keys whose older stopped tasks are unfolded.
    pub(super) unfolded: HashSet<String>,
    /// List archived Agents too (`agent.list` `archived: include`, k92).
    pub(super) show_archived: bool,
    pub(super) search: Search,
    pub(super) search_ref: ElementRef<AutoSuggestBox>,
    /// Ctrl+N and Ctrl+K from the window's keyboard hook.
    pub(super) shortcuts: Option<Arc<Mutex<Receiver<Shortcut>>>>,
}

/// The sidebar search: what the box shows, what was typed and the
/// suggestions it offers.
#[derive(Clone, Debug, Default)]
pub(super) struct Search {
    /// The box's text as WinUI last reported it (typed, or a suggestion it
    /// wrote in while the arrows move). The box is controlled: a render
    /// that left its text unset cleared it (observed live).
    pub(super) text: String,
    /// The typed query the list and the suggestions follow.
    pub(super) query: String,
    /// `(label, Agent id)` per suggestion ([`nav::search`]).
    pub(super) results: Vec<(String, String)>,
    /// The suggestion reached with ↑/↓: its label, Agent id and the input
    /// epoch then. Enter opens it while no other input came between.
    pub(super) highlighted: Option<(String, String, u64)>,
}

impl DesktopApp {
    pub(super) fn shell_update(&mut self, msg: ShellMsg, context: &ComponentContext<Self>) {
        match msg {
            ShellMsg::DaemonReady(result) => match result {
                Ok((discovered, read)) => {
                    self.startup_error = false;
                    self.prefs = crate::settings::Preferences::from_reply(&read);
                    self.nav.collapsed = self.prefs.collapsed.iter().cloned().collect();
                    self.launch_preferences_loaded();
                    self.preferred_window = crate::settings::window(&read);
                    // The opened checkout, registered by discovery, is where
                    // a new task starts.
                    if let Some(checkout) = discovered {
                        self.launch.workspace = checkout;
                    }
                    let first_connection = !self.preferences_loaded;
                    self.preferences_loaded = true;
                    self.load_agents(context, None);
                    self.load_profile_list(context);
                    self.load_routing_preview(context);
                    // The picker opens on a read catalogue (the daemon caches it).
                    self.load_catalog(context);
                    if let Ok(data_dir) = self.data_dir.clone() {
                        let _task = context.spawn_background(move |_| {
                            Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(
                                &data_dir,
                            )))
                        });
                    }
                    if first_connection {
                        self.watch_list(context);
                    }
                }
                Err(error) => {
                    self.startup_error = true;
                    self.alert(format!("Cannot start Swarm: {error}"));
                }
            },
            ShellMsg::UseFreshDirectory => {
                if let Ok(original) = client::data_dir() {
                    let fresh = original
                        .with_file_name(format!("Eliot-private-{}", client::new_mutation_id()));
                    match eliot_daemon::bootstrap::ensure_private_data_dir(&fresh).and_then(
                        |selected| {
                            eliot_daemon::persist_selected_root(&selected).map(|()| selected)
                        },
                    ) {
                        Ok(selected) => {
                            self.data_dir = Ok(selected.clone());
                            self.startup_error = false;
                            self.note_window_size();
                            self.alert = None;
                            self.set_status(format!(
                                "Starting in fresh private directory {}",
                                selected.display()
                            ));
                            self.start_connection(selected, context);
                        }
                        Err(error) => {
                            self.alert(format!("Cannot select fresh private directory: {error}"))
                        }
                    }
                }
            }
            ShellMsg::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
                    self.nav.projects = projects;
                    let workspace = self.launch.workspace.clone();
                    if let Some(project) = self
                        .nav
                        .projects
                        .iter()
                        .find(|project| project.workspace == workspace)
                        .cloned()
                    {
                        // The opened checkout is a registered project.
                        self.pick_project(&project.workspace);
                    } else if !self.launch.custom_workspace
                        && let Some(first) = self.nav.projects.first().cloned()
                    {
                        // A new task starts in the first project until one is picked.
                        self.pick_project(&first.workspace);
                    }
                }
                Err(error) => self.alert(format!("Projects not read: {error}")),
            },
            ShellMsg::AgentsLoaded(list_epoch, append, page) => {
                // The composer's usage indicator reads every harness's quota
                // once the daemon answers (Usage's Refresh reads again).
                if self.settings.quotas.is_empty() {
                    self.load_quotas(context);
                }
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                match page {
                    Ok(page) => {
                        if append {
                            self.nav.agents.extend(page.rows);
                        } else {
                            self.nav.agents = page.rows;
                        }
                        self.nav.agents_next = page.next;
                        self.nav.agents_total = page.total;
                        if !append {
                            self.nav.archived = page.archived;
                        }
                        // A just-launched Agent may not be on this page yet;
                        // its id is already the selection.
                        if let Some(wanted) = self.nav.pending_select.take()
                            && !self.nav.agents.iter().any(|agent| agent.id == wanted.id)
                        {
                            self.nav.agents.push(wanted);
                        }
                        let selected = self.nav.selection.agent.clone();
                        if let Some(agent) = selected
                            .and_then(|id| self.nav.agents.iter().find(|agent| agent.id == id))
                            .cloned()
                            && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                        {
                            self.open_conversation(&agent, context);
                        }
                        self.open_startup_conversation(context);
                        self.refresh_search();
                    }
                    Err(error) => {
                        // A daemon without the archived filter: the toggle
                        // goes back off, so the refresh does not repeat it.
                        if self.nav.show_archived && error.contains("(k92)") {
                            self.nav.show_archived = false;
                        }
                        self.alert(format!("Agents not read: {error}"));
                    }
                }
            }
            ShellMsg::AgentOpened(agent) => {
                self.close_popover();
                self.chat.opened_from_search = false;
                if self.main != Main::Conversation {
                    self.main = Main::Conversation;
                    self.reprime_fields(context);
                }
                if self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id) {
                    self.open_conversation(&agent, context);
                }
                if self.panel == Panel::Details {
                    self.show_details(&agent.id, context);
                }
            }
            ShellMsg::ProjectPicked(checkout) => {
                self.pick_project(&checkout);
                self.new_task(context);
            }
            ShellMsg::ExpandToggled(id, expand) => {
                if expand {
                    self.nav.expanded.insert(id.clone());
                    if !matches!(self.nav.branches.get(&id), Some(Branch::Loaded { .. })) {
                        self.load_branch(&id, context);
                    }
                } else {
                    self.nav.expanded.remove(&id);
                }
            }
            ShellMsg::BranchLoaded(epoch, id, page) => {
                if epoch != self.nav.tree_epoch {
                    return;
                }
                let branch = match page {
                    Ok(page) => Branch::Loaded {
                        more: page.next.is_some() && page.rows.len() < page.total,
                        children: page.rows,
                    },
                    Err(error) => Branch::Failed(error),
                };
                self.nav.branches.insert(id, branch);
            }
            ShellMsg::RefreshRequested => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let _task = context.spawn_background(move |_| {
                        Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
                    });
                }
                self.load_agents(context, None);
                // Expanded branches are read again; their rows stay until then.
                let expanded: Vec<String> = self.nav.expanded.iter().cloned().collect();
                for id in expanded {
                    self.load_branch(&id, context);
                }
                self.load_profile_list(context);
                if self.chat.convo.is_some() {
                    self.chat.failures = 0;
                    self.start_snapshot(context);
                }
            }
            ShellMsg::NewTask => self.new_task(context),
            ShellMsg::MoreAgentsRequested => {
                if let Some(after) = self.nav.agents_next.clone() {
                    self.load_agents(context, Some(after));
                }
            }
            ShellMsg::SettingsOpened => self.open_settings(None, context),
            ShellMsg::SettingsPage(page) => self.open_settings(Some(page), context),
            ShellMsg::SettingsSearch(text) => self.settings.query = text,
            ShellMsg::SettingsClosed => {
                // A preset's picker does not follow the owner out.
                self.close_popover();
                if self.main != Main::Conversation {
                    self.main = Main::Conversation;
                    self.reprime_fields(context);
                }
            }
            ShellMsg::PanelToggled(panel) => match panel {
                Panel::Details => match self.selected_agent_id() {
                    Some(agent) if self.panel != Panel::Details => {
                        self.show_details(&agent, context);
                    }
                    _ => self.close_panel(),
                },
                Panel::Monitor if self.panel != Panel::Monitor => {
                    self.panel = Panel::Monitor;
                    self.start_monitor(context);
                }
                _ => self.close_panel(),
            },
            ShellMsg::SchemeObserved(scheme) => {
                let before = self.palette();
                self.appearance.dark = scheme == ColorScheme::Dark;
                self.appearance.high_contrast = crate::accessibility::high_contrast_on();
                if self.palette() != before {
                    // The palette-keyed tree is rebuilt with new controls
                    // (an empty search box among them).
                    self.nav.search = Search::default();
                    self.apply_scheme();
                    self.reprime_fields(context);
                }
                // The first observation colors the title bar even when the
                // palette is the starting one.
                self.sync_title_bar();
            }
            ShellMsg::WindowResized(size) => {
                self.window_width = Some(size.width);
                self.sync_title_bar();
                if size.width >= 640.0 && size.height >= 480.0 {
                    let window = (size.width as u32, size.height as u32);
                    if self.window_size != Some(window) {
                        self.window_size = Some(window);
                        // No write here: the size persists once, at close.
                        self.note_window_size();
                    }
                }
            }
            ShellMsg::HighContrastRead(on) => {
                if on != self.appearance.high_contrast {
                    self.appearance.high_contrast = on;
                    self.apply_scheme();
                    self.reprime_fields(context);
                }
                self.watch_high_contrast(context);
            }
            ShellMsg::ThemeChosen(theme) => {
                self.prefs.theme = theme;
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::StartupChosen(startup) => {
                self.prefs.startup = startup;
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::PreferencesSaved(saved, window, result) => {
                self.prefs_saving = false;
                if let Err(error) = &result {
                    self.alert(format!("Preferences not saved: {error}"));
                }
                // A choice made on the page reports there; window-size
                // and composer writes stay quiet.
                if self.main == Main::Settings || result.is_err() {
                    self.settings.notes.insert(
                        self.settings_page,
                        crate::settings_model::save_note(false, &result),
                    );
                }
                // A newer change arrived while this one was written.
                if saved != self.prefs || window != self.window_size {
                    self.save_preferences(context);
                } else if self.settings.loaded && result.is_ok() {
                    // The revision moved: the page's drafts save against it.
                    self.load_settings(context);
                }
            }
            ShellMsg::ProjectToggled(key) => {
                if !self.nav.collapsed.remove(&key) {
                    self.nav.collapsed.insert(key);
                }
                self.collapsed_projects_changed(context);
            }
            ShellMsg::OlderToggled(key) => {
                if !self.nav.unfolded.remove(&key) {
                    self.nav.unfolded.insert(key);
                }
            }
            ShellMsg::SearchChanged(text) => {
                self.nav.search.text = text.clone();
                // WinUI writes the suggestion reached with the arrows into
                // the box: that is not a new query.
                if !self
                    .nav
                    .search
                    .results
                    .iter()
                    .any(|(label, _)| *label == text)
                {
                    self.nav.search.query = text;
                    self.refresh_search();
                }
            }
            ShellMsg::SearchChosen(label) => {
                let chosen = self
                    .nav
                    .search
                    .results
                    .iter()
                    .find(|(shown, _)| *shown == label)
                    .map(|(_, id)| id.clone());
                // WinUI reports every suggestion reached with the arrows as
                // chosen (Reactor 0.100.0 exposes no QuerySubmitted): that
                // only highlights it. Enter (`Shortcut::Enter`) or a click
                // opens it.
                let (input, age, epoch) = crate::shortcuts::last_input();
                if input == Input::Arrow && age <= ARROW_CHOICE_MS {
                    self.nav.search.highlighted = chosen.map(|id| (label, id, epoch));
                    return;
                }
                self.nav.search.highlighted = None;
                if let Some(id) = chosen {
                    self.open_found(&id, context);
                }
            }
            ShellMsg::Shortcut(shortcut) => {
                match shortcut {
                    Shortcut::NewTask => self.new_task(context),
                    Shortcut::Search => self.focus_search(SEARCH_FOCUS_TRIES, context),
                    Shortcut::Settings => self.open_settings(None, context),
                    Shortcut::ModelPicker => {
                        if self.launch.open != Some(launch_ui::Pop::Model) {
                            self.launch_update(LaunchMsg::Toggle(launch_ui::Pop::Model), context);
                        }
                    }
                    Shortcut::Popover(key) if self.menu.open.is_some() => {
                        if key == crate::shortcuts::PopoverKey::Escape {
                            self.close_menu();
                        }
                    }
                    Shortcut::Popover(key) => self.launch_update(LaunchMsg::Key(key), context),
                    Shortcut::Enter(_) if self.menu.renaming.is_some() => {
                        self.menu_update(MenuMsg::RenameCommitted, context);
                    }
                    Shortcut::Enter(epoch) => {
                        // Enter on a suggestion reached with the arrows, with
                        // no other input since and the box still showing it.
                        if let Some((label, id, at)) = self.nav.search.highlighted.take()
                            && at == epoch
                            && self.nav.search.text == label
                        {
                            self.open_found(&id, context);
                        }
                    }
                }
                self.watch_shortcuts(context);
            }
            ShellMsg::FocusSearch(tries) => self.focus_search(tries, context),
            ShellMsg::FocusSearchLater(tries) => self.focus_search_later(tries, context),
            ShellMsg::ShortcutsEnded => {}
            ShellMsg::AlertDismissed => self.alert = None,
            ShellMsg::ListTick => {
                // Only the first page is re-read; pages added with "Show
                // more" stay until a manual refresh.
                if self.preferences_loaded
                    && !self.nav.agents_loading
                    && self.nav.agents.len() <= AGENT_PAGE
                {
                    self.load_agents(context, None);
                }
                self.watch_list(context);
            }
            ShellMsg::FieldsPrimed(epoch) => {
                if epoch != self.prime_epoch {
                    return;
                }
                // Reactor drains queued messages before it composes, so this
                // can arrive before the empty render: wait for one.
                if self.renders.get() > self.prime_after {
                    self.fields_primed = true;
                } else {
                    self.post_fields_primed(epoch, PRIME_RETRY_MS, context);
                }
            }
        }
    }

    /// Write the shown scheme's M3 roles at the application root (removed
    /// under High Contrast), before the palette-keyed tree is rebuilt.
    fn apply_scheme(&mut self) {
        if let Err(error) = crate::app_resources::apply_scheme(self.palette().scheme()) {
            self.set_status(format!("WinUI M3 resources not applied: {error}"));
        }
        self.sync_title_bar();
    }

    /// Color the title bar in the shown palette once the window exists and
    /// whenever the palette changes (a runtime theme switch, High Contrast).
    /// Until the window is shown the colors stay pending and the next scheme
    /// or size observation tries again.
    fn sync_title_bar(&mut self) {
        let palette = self.palette();
        if self.title_bar == Some(palette) {
            return;
        }
        match crate::app_resources::apply_title_bar(palette.scheme()) {
            Ok(()) => self.title_bar = Some(palette),
            Err(error) => self.set_status(format!("Title bar colors not applied: {error}")),
        }
    }

    /// Open Settings, at `page` when given.
    fn open_settings(&mut self, page: Option<settings_ui::Page>, context: &ComponentContext<Self>) {
        self.close_popover();
        let page_changed = page.is_some_and(|page| page != self.settings_page);
        if let Some(page) = page {
            self.settings_page = page;
        }
        if self.main != Main::Settings || page_changed {
            self.main = Main::Settings;
            self.reprime_fields(context);
            self.settings_entered(context);
        }
    }

    /// Open the Agent a search found.
    fn open_found(&mut self, id: &str, context: &ComponentContext<Self>) {
        if let Some(agent) = self.find_agent(id).cloned() {
            self.nav.selection.select_agent(Some(agent.id.clone()));
            self.shell_update(ShellMsg::AgentOpened(agent), context);
            self.chat.opened_from_search = true;
        }
    }

    /// Leave the open conversation locally (the Agent keeps running) and
    /// show the empty new-task view.
    pub(super) fn new_task(&mut self, context: &ComponentContext<Self>) {
        self.close_popover();
        if self.main != Main::Conversation {
            self.main = Main::Conversation;
            self.reprime_fields(context);
        }
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        let _ = self.chat.composer_ref.request_focus();
        self.chat.epoch += 1;
        self.chat.inputs_checking = false;
        if let Some(task) = self.chat.live_task.take() {
            task.cancel();
        }
        self.nav.selection.select_agent(None);
        self.chat.convo = None;
        self.chat.detail = None;
        self.chat.live = LiveStatus::Idle;
        self.chat.items = Rc::new(Vec::new());
        self.chat.items_revision = 0;
        if self.panel == Panel::Details {
            self.close_panel();
        }
        self.load_routing_preview(context);
        self.set_status("New task.");
    }

    /// The project a new task starts in: its checkout, and its profile
    /// allowlist when it is registered.
    pub(super) fn pick_project(&mut self, checkout: &str) {
        let registered = self
            .nav
            .projects
            .iter()
            .find(|project| project.workspace == checkout)
            .cloned();
        self.launch.workspace = checkout.to_owned();
        self.launch.custom_workspace = false;
        self.launch.chat = false;
        match registered {
            Some(project) => {
                self.launch.profiles = project.profiles.clone();
                self.nav.selection.project = Some(project.id.clone());
            }
            None => {
                self.launch.profiles.clear();
                self.nav.selection.project = None;
            }
        }
        self.launch.reconcile_choice();
    }

    /// What the owner reads for a registered project: the display name they
    /// gave its sidebar project (`[desktop] display_names`), else its name.
    pub(super) fn project_label(&self, project: &UiProject) -> String {
        let key = format!("project:repo:{}", nav::path_key(&project.common_dir));
        self.prefs
            .display_name(&key)
            .map_or_else(|| project.name.clone(), str::to_owned)
    }

    /// Open the most recently active Agent's conversation once at start,
    /// when the preference asks.
    fn open_startup_conversation(&mut self, context: &ComponentContext<Self>) {
        if self.startup_done {
            return;
        }
        self.startup_done = true;
        if self.prefs.startup != Startup::LastConversation || self.chat.convo.is_some() {
            return;
        }
        if let Some(agent) = nav::recents(&self.nav.agents, 1, &HashSet::new())
            .first()
            .cloned()
            .cloned()
        {
            self.nav.selection.select_agent(Some(agent.id.clone()));
            self.open_conversation(&agent, context);
        }
    }

    /// The registered checkouts with their repositories, for grouping.
    pub(super) fn checkouts(&self) -> Vec<Checkout> {
        self.nav
            .projects
            .iter()
            .map(|project| Checkout {
                path: project.workspace.clone(),
                common_dir: project.common_dir.clone(),
            })
            .collect()
    }

    /// The listed or fetched Agent with this id.
    fn find_agent(&self, id: &str) -> Option<&UiAgent> {
        let branch_children = self.nav.branches.values().flat_map(|branch| match branch {
            Branch::Loaded { children, .. } => children.as_slice(),
            _ => &[],
        });
        self.nav
            .agents
            .iter()
            .chain(branch_children)
            .find(|agent| agent.id == id)
    }

    /// The title of `agent` as its summary gave it, else the monitor's,
    /// else its id (an Agent on no page read yet).
    pub(super) fn agent_title(&self, agent: &str) -> String {
        if let Some(name) = self.prefs.display_name(&format!("agent:{agent}")) {
            return name.to_owned();
        }
        if let Some(found) = self.find_agent(agent) {
            return found.title.clone();
        }
        self.monitor
            .data
            .rows
            .iter()
            .find(|row| row.id == agent)
            .map_or_else(|| agent.to_owned(), |row| row.title.clone())
    }

    /// Recompute the search suggestions from the listed Agents.
    fn refresh_search(&mut self) {
        self.nav.search.results =
            nav::search(&self.nav.agents, &self.checkouts(), &self.nav.search.query);
    }

    /// Put the keyboard in the search box; from Settings, close them first.
    /// A refused or unbound focus is tried again, `tries` times in all.
    fn focus_search(&mut self, tries: u8, context: &ComponentContext<Self>) {
        if self.main != Main::Conversation {
            self.main = Main::Conversation;
            self.reprime_fields(context);
        }
        let Some(left) = tries.checked_sub(1) else {
            return;
        };
        let later = context.sender();
        let accepted = self.nav.search_ref.request_focus_result(move |result| {
            if !matches!(result, Ok(true)) && left > 0 {
                let _ = later.send(Message::Shell(ShellMsg::FocusSearchLater(left)));
            }
        });
        if !accepted && left > 0 {
            self.focus_search_later(left, context);
        }
    }

    fn focus_search_later(&self, tries: u8, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(SEARCH_FOCUS_RETRY_MS, &cancelled);
            Message::Shell(ShellMsg::FocusSearch(tries))
        });
    }

    /// Wait for the next Ctrl+N or Ctrl+K on a worker.
    pub(super) fn watch_shortcuts(&self, context: &ComponentContext<Self>) {
        let Some(chords) = self.nav.shortcuts.clone() else {
            return;
        };
        let _task = context.spawn_background(move |cancel| {
            let Ok(chords) = chords.lock() else {
                return Message::Shell(ShellMsg::ShortcutsEnded);
            };
            loop {
                match chords.recv_timeout(std::time::Duration::from_millis(SHORTCUT_POLL_MS)) {
                    Ok(chord) => return Message::Shell(ShellMsg::Shortcut(chord)),
                    Err(RecvTimeoutError::Timeout) if !cancel.is_cancelled() => {}
                    // Window closing or hook gone: nothing more to deliver.
                    Err(_) => return Message::Shell(ShellMsg::ShortcutsEnded),
                }
            }
        });
    }

    /// The collapsed projects changed: write `[desktop] collapsed_projects`
    /// through the settings writer, so the state survives a restart.
    fn collapsed_projects_changed(&mut self, context: &ComponentContext<Self>) {
        let mut collapsed: Vec<String> = self.nav.collapsed.iter().cloned().collect();
        collapsed.sort();
        if collapsed != self.prefs.collapsed {
            self.prefs.collapsed = collapsed;
            self.prefs_rev += 1;
            self.save_preferences(context);
        }
    }

    /// Reactor 0.100.0 creates a TextBox with `Text` before `AcceptsReturn`,
    /// and a TextBox that is still single-line keeps only the first line of
    /// the text it is given. Whenever the main tree is re-created the
    /// multi-line editors (composer, profile and script sources) therefore
    /// render one turn empty and get their text on the next message, once
    /// they already accept returns: the same path as a first load.
    pub(super) fn reprime_fields(&mut self, context: &ComponentContext<Self>) {
        self.fields_primed = false;
        self.prime_epoch += 1;
        self.prime_after = self.renders.get();
        self.post_fields_primed(self.prime_epoch, 0, context);
    }

    fn post_fields_primed(&self, epoch: u64, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Shell(ShellMsg::FieldsPrimed(epoch))
        });
    }

    /// The next sidebar refresh, one cadence from now.
    pub(super) fn watch_list(&self, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(|cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(LIST_REFRESH_MS, &cancelled);
            Message::Shell(ShellMsg::ListTick)
        });
    }

    /// The open Agent's row takes the state its detail just read.
    pub(super) fn sync_open_agent(&mut self) {
        let (Some(id), Some(detail)) = (self.selected_agent_id(), self.chat.detail.as_ref()) else {
            return;
        };
        let state = detail.state;
        for agent in self.nav.agents.iter_mut().filter(|agent| agent.id == id) {
            agent.state = state;
        }
        for branch in self.nav.branches.values_mut() {
            if let Branch::Loaded { children, .. } = branch {
                for agent in children.iter_mut().filter(|agent| agent.id == id) {
                    agent.state = state;
                }
            }
        }
    }

    /// The next High Contrast reading, one cadence from now.
    pub(super) fn watch_high_contrast(&self, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(|cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(HIGH_CONTRAST_POLL_MS, &cancelled);
            Message::Shell(ShellMsg::HighContrastRead(
                crate::accessibility::high_contrast_on(),
            ))
        });
    }

    /// The main area's width from the window width, the sidebar (or the
    /// Settings navigation in its place) and the open panel, or None until
    /// the first size observation.
    pub(super) fn main_width(&self) -> Option<f64> {
        let panel = if self.panel_shown() {
            tokens::layout::PANEL_WIDTH
        } else {
            0.0
        };
        self.window_width
            .map(|width| width - tokens::layout::SIDEBAR_WIDTH - panel)
    }

    /// The right panel belongs to the conversation: Settings shows two
    /// columns (its navigation and the page), as in the Codex app.
    fn panel_shown(&self) -> bool {
        self.panel != Panel::Closed && self.main == Main::Conversation && !self.startup_error
    }

    /// Write an explicit theme or startup choice into `[desktop]` at once,
    /// carrying the current window size; one write at a time (a newer state
    /// is written when the running write reports back). A bare resize never
    /// writes: the size persists once, at close (see
    /// [`crate::settings::save_window_on_close`]).
    pub(super) fn save_preferences(&mut self, context: &ComponentContext<Self>) {
        if self.prefs_saving {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.prefs_saving = true;
        let (preferences, window) = (self.prefs.clone(), self.window_size);
        let _task = context.spawn_background(move |_| {
            let saved = client::block_on(config_client::save_preferences(
                &data_dir,
                preferences.clone(),
                window,
            ))
            .map_err(|error| error.to_string());
            Message::Shell(ShellMsg::PreferencesSaved(preferences, window, saved))
        });
    }

    /// Remember the last observed size (and its data directory) for the one
    /// `[desktop]` write after the window closes.
    pub(super) fn note_window_size(&mut self) {
        if let (Ok(data_dir), Some(window)) = (self.data_dir.clone(), self.window_size) {
            self.close_window.store(data_dir, window);
        }
    }

    /// Start the daemon when needed (Owner only, the sibling `swarm.exe`),
    /// register the opened checkout, then read the settings.
    pub(super) fn start_connection(&self, data_dir: PathBuf, context: &ComponentContext<Self>) {
        let workspace = self.opened_workspace.clone();
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(async {
                let executable = std::env::current_exe()
                    .map_err(|_| {
                        eliot_core::Error::new("platform_io", "cannot locate desktop executable")
                    })?
                    .with_file_name("swarm.exe");
                eliot_daemon::startup::ensure_running(&data_dir, &executable).await?;
                let request = eliot_core::api::ProjectDiscoverRequest {
                    workspace: workspace.to_string_lossy().into_owned(),
                };
                request.validate()?;
                let reply: eliot_core::api::ProjectDiscoverResponse =
                    eliot_daemon::client_call_typed(&data_dir, "project.discover", &request)
                        .await?;
                request.validate_response(&reply)?;
                let read = config_client::read_settings(&data_dir).await?;
                Ok((reply.project.map(|project| project.checkout), read))
            })
            .map_err(|error: eliot_core::Error| error.to_string());
            Message::Shell(ShellMsg::DaemonReady(result))
        });
    }

    /// `text` once the editors are primed, empty during the priming render.
    pub(super) fn primed<'a>(&self, text: &'a str) -> &'a str {
        if self.fields_primed { text } else { "" }
    }

    /// The registered project a new task starts in, if any.
    pub(super) fn selected_project(&self) -> Option<&UiProject> {
        self.nav
            .projects
            .iter()
            .find(|project| project.workspace == self.launch.workspace)
    }

    /// Read one manager's direct children (`agent.list` with `parent`).
    fn load_branch(&mut self, id: &str, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if !matches!(self.nav.branches.get(id), Some(Branch::Loaded { .. })) {
            self.nav.branches.insert(id.to_owned(), Branch::Loading);
        }
        let request = AgentListRequest {
            limit: AGENT_PAGE,
            parent: Some(id.to_owned()),
            ..Default::default()
        };
        let (epoch, id) = (self.nav.tree_epoch, id.to_owned());
        let _task = context.spawn_background(move |_| {
            let page = load_agents_blocking(&data_dir, &request, None);
            Message::Shell(ShellMsg::BranchLoaded(epoch, id, page))
        });
    }

    pub(super) fn selected_agent_id(&self) -> Option<String> {
        self.nav.selection.agent.clone()
    }

    pub(super) fn load_agents(&mut self, context: &ComponentContext<Self>, after: Option<String>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let append = after.is_some();
        self.nav.list_epoch += 1;
        let list_epoch = self.nav.list_epoch;
        let request = AgentListRequest {
            after,
            limit: AGENT_PAGE,
            ..Default::default()
        };
        self.nav.agents_loading = true;
        let archived = self.nav.show_archived.then_some("include");
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::AgentsLoaded(
                list_epoch,
                append,
                load_agents_blocking(&data_dir, &request, archived),
            ))
        });
    }

    /// The window: sidebar, main area and the optional right panel.
    pub(super) fn shell_view(&self, context: &mut ViewContext<Self>) -> View {
        context.window_title("Eliot Swarm");
        // A bounded first window instead of WinUI's share of the monitor
        // (3826 px wide on a 5120 px display). Reactor applies only visuals
        // that differ from the default, so this declaration also carries the
        // title-bar theme.
        context.window_visuals(
            WindowVisuals::new()
                .client_size(
                    self.preferred_window
                        .map_or(tokens::layout::WINDOW_WIDTH, |w| f64::from(w.0)),
                    self.preferred_window
                        .map_or(tokens::layout::WINDOW_HEIGHT, |w| f64::from(w.1)),
                )
                .theme(window_theme(self.prefs.theme)),
        );
        context.on_color_scheme(
            context.callback(|scheme| Message::Shell(ShellMsg::SchemeObserved(scheme))),
        );
        context
            .on_window_size(context.callback(|size| Message::Shell(ShellMsg::WindowResized(size))));
        let palette = self.palette();

        let main: View = match self.main {
            _ if self.startup_error => self.startup_error_view(context, palette),
            Main::Conversation => self.chat_view(context, palette),
            Main::Settings => self.settings_view(context, palette),
        };
        let mut columns = vec![
            GridLength::Pixel(tokens::layout::SIDEBAR_WIDTH),
            GridLength::STAR,
        ];
        let panel: View = if !self.panel_shown() {
            View::empty()
        } else {
            columns.push(GridLength::Pixel(tokens::layout::PANEL_WIDTH));
            Border::new()
                .grid_column(2)
                .background(palette.sidebar())
                .content(self.panel_view(context, palette))
        };
        // Settings replaces the sidebar with its own navigation.
        let side: View = match self.main {
            Main::Settings if !self.startup_error => self.settings_nav(context, palette),
            _ => self.sidebar_view(context, palette),
        };
        let columns_grid = Grid::new()
            .background(palette.surface())
            .columns(columns)
            .children((
                Border::new()
                    .grid_column(0)
                    .background(palette.sidebar())
                    .content(side),
                Border::new().grid_column(1).content(main),
                panel,
            ));
        // One cell: the window, and over it the right-click menu or the
        // confirmation dialog.
        let window = Grid::new().children((columns_grid, self.menu_overlay(context, palette)));
        // Reactor updates a Button's resource overrides in place, but WinUI
        // resolves a template's ThemeResource values only when the template
        // is applied or the theme changes, and the theme change arrives before
        // the new palette. Keying the tree by palette rebuilds the native
        // controls on a scheme switch (rare) so every button takes the new
        // overrides; all view state is model-bound and is redrawn as it was.
        let palette_key = match palette.scheme() {
            None => "high-contrast",
            Some(scheme) if scheme.dark => "dark",
            Some(_) => "light",
        };
        Grid::new().keyed_children(vec![KeyedView::new(palette_key.to_owned(), window)])
    }

    /// The sidebar at Codex density (owner review 2026-11-16, the Codex
    /// and OpenCode sidebars as references): the app name, Search tasks
    /// (Ctrl+K), the one tonal New task row (Ctrl+N), the projects
    /// (collapsible 32-DIP rows) with their tasks nested (28-DIP rows
    /// indented 24 DIPs), Recents, then the fleet line, the attention row
    /// and Settings at the bottom. Section labels are secondary text.
    fn sidebar_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        let header = m3::section_title("Eliot Swarm")
            .height(tokens::height::CONTROL)
            .margin(Thickness::new(
                pad + tokens::spacing::INSET,
                tokens::spacing::SM,
                pad,
                0.0,
            ))
            .vertical_alignment(VerticalAlignment::Center);
        let search = AutoSuggestBox::new()
            .element_ref(&self.nav.search_ref)
            .placeholder_text("Search tasks (Ctrl+K)")
            .text(self.nav.search.text.clone())
            .items_source(
                self.nav
                    .search
                    .results
                    .iter()
                    .map(|(label, _)| label.clone()),
            )
            .on_text_changed(context.callback(|text| Message::Shell(ShellMsg::SearchChanged(text))))
            .on_suggestion_chosen(
                context.callback(|label| Message::Shell(ShellMsg::SearchChosen(label))),
            )
            .automation_name("Search tasks")
            .height(tokens::height::CONTROL)
            .grid_row(2)
            .margin(Thickness::new(pad, tokens::spacing::XS, pad, 0.0));
        let new_task = m3::tonal_row(palette)
            .grid_row(1)
            .margin(Thickness::new(pad, tokens::spacing::XS, pad, 0.0))
            .automation_name("New task (Ctrl+N)")
            .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
            .content(m3::row_content(
                Some(m3::glyph::NEW_TASK),
                "New task",
                "",
                m3::secondary("Ctrl+N")
                    .vertical_alignment(VerticalAlignment::Center)
                    .into(),
            ));
        // Then the two destinations, as in Antigravity's sidebar.
        let destination = |page: settings_ui::Page, glyph: &str, label: &str| {
            let shown = self.main == Main::Settings && self.settings_page == page;
            KeyedView::new(
                format!("{label}:{shown}"),
                m3::row(palette, shown)
                    .automation_name(label.to_owned())
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(page))))
                    .content(m3::icon_label(glyph, label)),
            )
        };
        let destinations = StackPanel::new()
            .grid_row(3)
            .margin(Thickness::new(pad, tokens::spacing::XS, pad, 0.0))
            .keyed_children(vec![
                destination(
                    settings_ui::Page::Automations,
                    m3::glyph::AUTOMATION,
                    "Automations",
                ),
                destination(settings_ui::Page::Usage, m3::glyph::USAGE, "Usage"),
            ]);

        let fleet = Fleet::of(&self.nav.agents);
        let query = self.nav.search.query.trim().to_owned();
        let checkouts = self.checkouts();
        let keep = |agent: &UiAgent| {
            query.is_empty()
                || nav::matches(agent, &nav::project_of(&agent.project, &checkouts), &query)
        };
        let filtered = !query.is_empty();
        let shown = Shown {
            branches: &self.nav.branches,
            expanded: &self.nav.expanded,
            collapsed: &self.nav.collapsed,
            unfolded: &self.nav.unfolded,
            today_start: today_start_ms(),
            only: filtered.then_some(&keep as &dyn Fn(&UiAgent) -> bool),
        };
        let sections = nav::sections(&self.nav.agents, &checkouts, &shown);

        let caption = if filtered {
            "Matching tasks"
        } else {
            "Projects"
        };
        let mut items: Vec<KeyedView> = vec![KeyedView::new(
            format!("caption:{caption}"),
            section_label(caption),
        )];
        if sections.is_empty() {
            let empty = if self.nav.agents_loading {
                "Loading…"
            } else if filtered {
                "No matching tasks. Clear the search to see all."
            } else {
                "No projects yet. Start a task with New task, or add one in Settings › Projects."
            };
            items.push(KeyedView::new(
                "no-projects",
                m3::secondary(empty)
                    .text_wrapping(TextWrapping::Wrap)
                    .margin(Thickness::new(
                        tokens::spacing::INSET,
                        tokens::spacing::XS,
                        tokens::spacing::SM,
                        0.0,
                    )),
            ));
        }
        let new_task_selected =
            self.main == Main::Conversation && self.chat.convo.is_none() && !self.launch.chat;
        for section in &sections {
            let selected = new_task_selected && section.holds(&self.launch.workspace);
            let name_key = format!("project:{}", section.key);
            let renaming = self
                .menu
                .renaming
                .as_ref()
                .filter(|(target, _)| matches!(target, Target::Project { key, .. } if *key == section.key))
                .map(|(_, typed)| typed.clone());
            let name = self
                .prefs
                .display_name(&name_key)
                .map_or_else(|| section.name.clone(), str::to_owned);
            let row = match renaming {
                Some(typed) => self.rename_row(&typed, 0.0, context, palette),
                None => with_menu(
                    self.project_row(section, &name, selected, filtered, context, palette),
                    Target::Project {
                        key: section.key.clone(),
                        name: name.clone(),
                        checkout: section.checkout.clone(),
                        checkouts: section.checkouts.clone(),
                        collapsed: section.collapsed,
                    },
                    context,
                ),
            };
            items.push(KeyedView::new(
                format!(
                    "project:{}:{selected}:{}:{name}:{}",
                    section.key,
                    section.collapsed,
                    self.menu.renaming.is_some()
                ),
                row,
            ));
            if section.collapsed {
                continue;
            }
            if section.rows.is_empty() {
                // Older tasks fold below; "No tasks yet" only when there
                // are none at all.
                items.push(KeyedView::new(
                    format!("empty:{}", section.key),
                    quiet_row(
                        if section.older > 0 {
                            "No recent tasks"
                        } else {
                            "No tasks yet: start one with New task"
                        },
                        tokens::layout::TREE_INDENT,
                    ),
                ));
            }
            for row in &section.rows {
                items.push(self.task_item(row, context, palette));
            }
            if section.older > 0 {
                let label = if section.older_shown {
                    "Hide older".to_owned()
                } else {
                    format!("Show {} older", section.older)
                };
                items.push(KeyedView::new(
                    format!("older:{}:{}", section.key, section.older_shown),
                    m3::sub_row(palette, false)
                        .margin(Thickness::new(tokens::layout::TREE_INDENT, 0.0, 0.0, 0.0))
                        .automation_name(format!("{label} in {}", section.name))
                        .on_click(
                            context.message(Message::Shell(ShellMsg::OlderToggled(
                                section.key.clone(),
                            ))),
                        )
                        .content(m3::secondary(label)),
                ));
            }
        }
        if !filtered
            && self.nav.agents.len() < self.nav.agents_total
            && self.nav.agents_next.is_some()
        {
            items.push(KeyedView::new(
                "more",
                m3::sub_row(palette, false)
                    .is_enabled(!self.nav.agents_loading)
                    .margin(Thickness::new(tokens::layout::TREE_INDENT, 0.0, 0.0, 0.0))
                    .on_click(context.message(Message::Shell(ShellMsg::MoreAgentsRequested)))
                    .content(m3::secondary(format!(
                        "Show more ({} of {})",
                        self.nav.agents.len(),
                        self.nav.agents_total
                    ))),
            ));
        }
        // Chats without a project (`launch {chat: true}`), as tasks are
        // under their project: the open one selected, children nested.
        let chats: Vec<&UiAgent> = nav::chats(&self.nav.agents)
            .into_iter()
            .filter(|agent| keep(agent))
            .collect();
        if !chats.is_empty() || (self.launch.chat && !filtered) {
            items.push(KeyedView::new("chats-caption", section_label("Chats")));
            if chats.is_empty() {
                items.push(KeyedView::new(
                    "chats-empty",
                    quiet_row("No chats yet: Send starts one", tokens::layout::TREE_INDENT),
                ));
            }
            for agent in chats {
                let row = Row {
                    agent: agent.clone(),
                    depth: 0,
                    children: self
                        .nav
                        .agents
                        .iter()
                        .filter(|child| child.parent.as_deref() == Some(agent.id.as_str()))
                        .count(),
                    expanded: self.nav.expanded.contains(&agent.id),
                    note: None,
                    place: None,
                };
                items.push(self.task_item(&row, context, palette));
                if row.expanded {
                    for child in self
                        .nav
                        .agents
                        .iter()
                        .filter(|child| child.parent.as_deref() == Some(agent.id.as_str()))
                    {
                        let child_row = Row {
                            agent: child.clone(),
                            depth: 1,
                            children: 0,
                            expanded: false,
                            note: None,
                            place: None,
                        };
                        items.push(self.task_item(&child_row, context, palette));
                    }
                }
            }
        }
        // Recents lists only what the list above does not show now (a
        // collapsed project's tasks, older ones folded away).
        let mut listed: HashSet<&str> = sections
            .iter()
            .filter(|section| !section.collapsed)
            .flat_map(|section| section.rows.iter().map(|row| row.agent.id.as_str()))
            .collect();
        for chat in nav::chats(&self.nav.agents) {
            listed.insert(chat.id.as_str());
            if self.nav.expanded.contains(&chat.id) {
                listed.extend(
                    self.nav
                        .agents
                        .iter()
                        .filter(|child| child.parent.as_deref() == Some(chat.id.as_str()))
                        .map(|child| child.id.as_str()),
                );
            }
        }
        let recents = nav::recents(&self.nav.agents, RECENTS, &listed);
        if !filtered && !recents.is_empty() {
            items.push(KeyedView::new("recents-caption", section_label("Recents")));
            for agent in recents {
                items.push(KeyedView::new(
                    format!("recent:{}:{}", agent.id, self.agent_label(agent)),
                    with_menu(
                        self.recent_row(agent, context, palette),
                        Target::Agent(agent.clone()),
                        context,
                    ),
                ));
            }
        }
        items.push(KeyedView::new(
            format!("archived:{}", self.nav.show_archived),
            m3::sub_row(palette, self.nav.show_archived)
                .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                .automation_name(if self.nav.show_archived {
                    "Hide archived (showing archived)"
                } else {
                    "Show archived"
                })
                .on_click(context.message(Message::Menu(MenuMsg::ArchivedToggled)))
                .content(m3::row_content(
                    Some(m3::glyph::ARCHIVE),
                    if self.nav.show_archived {
                        "Hide archived"
                    } else {
                        "Show archived"
                    },
                    "",
                    View::empty(),
                )),
        ));
        let list = ScrollViewer::new()
            .grid_row(4)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Projects and Agents")
            .content(
                StackPanel::new()
                    .margin(Thickness::new(pad, 0.0, pad, pad))
                    .keyed_children(items),
            );

        let monitor_open = self.panel == Panel::Monitor;
        let mut bottom = vec![
            KeyedView::new(
                "divider",
                m3::divider(palette).margin(Thickness::new(
                    tokens::spacing::INSET,
                    0.0,
                    tokens::spacing::INSET,
                    tokens::spacing::SM,
                )),
            ),
            KeyedView::new(
                format!("fleet:{monitor_open}"),
                self.fleet_row(fleet, context, palette),
            ),
        ];
        bottom.push(KeyedView::new(
            "settings",
            m3::row(palette, false)
                .automation_name("Settings")
                .on_click(context.message(Message::Shell(ShellMsg::SettingsOpened)))
                .content(m3::icon_label(m3::glyph::SETTINGS, "Settings")),
        ));
        let bottom = StackPanel::new()
            .grid_row(5)
            .margin(Thickness::new(pad, 0.0, pad, pad))
            .keyed_children(bottom);

        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .children((header, new_task, search, destinations, list, bottom))
    }

    /// A project row: its chevron, folder and repository name collapse or
    /// expand it; a collapsed project keeps its running tasks as a count.
    /// The trailing button starts a new task in it.
    fn project_row(
        &self,
        section: &Section,
        name: &str,
        selected: bool,
        filtered: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let badge: View = if section.collapsed && section.running > 0 {
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::XS)
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    palette
                        .dot(StatusTone::Active)
                        .map_or_else(View::empty, m3::dot),
                    m3::secondary(section.running.to_string())
                        .vertical_alignment(VerticalAlignment::Center),
                ))
        } else {
            View::empty()
        };
        let (chevron, state) = if section.collapsed {
            (m3::glyph::CHEVRON_RIGHT, "collapsed")
        } else {
            (m3::glyph::CHEVRON_DOWN, "expanded")
        };
        let mut label = format!("{name}, project, {state}");
        if section.collapsed && section.running > 0 {
            label.push_str(&format!(", {} running", section.running));
        }
        let toggle = m3::row(palette, selected)
            .automation_name(label)
            .is_enabled(!filtered)
            .on_click(context.message(Message::Shell(ShellMsg::ProjectToggled(
                section.key.clone(),
            ))))
            .content(
                Grid::new()
                    .columns([GridLength::Auto, GridLength::STAR])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        m3::icon(chevron),
                        Border::new().grid_column(1).content(m3::row_content(
                            Some(m3::glyph::FOLDER),
                            name,
                            "",
                            badge,
                        )),
                    )),
            );
        let new_task = m3::icon_button(palette)
            .grid_column(1)
            .width(tokens::height::CONTROL)
            .height(tokens::height::CONTROL)
            .automation_name(format!("New task in {name}"))
            .on_click(context.message(Message::Shell(ShellMsg::ProjectPicked(
                section.checkout.clone(),
            ))))
            .content(m3::icon(m3::glyph::NEW_TASK));
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .children((toggle, new_task))
    }

    /// A task row under its project: indented 24 DIPs per level, so its
    /// title lines up with the project's label. One line: the title, the
    /// worktree where the project spans several, and a state dot only for
    /// a task that runs or needs attention. A manager with workers carries
    /// its expand chevron in that indent.
    fn agent_row(
        &self,
        row: &Row,
        selected: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let agent = &row.agent;
        let label = self.agent_label(agent);
        let title = label.as_str();
        let indent = tokens::layout::TREE_INDENT;
        let chevron: View = if row.expandable() {
            let (glyph, name) = if row.expanded {
                (
                    m3::glyph::CHEVRON_DOWN,
                    format!("Collapse workers of {title}"),
                )
            } else {
                (
                    m3::glyph::CHEVRON_RIGHT,
                    format!("Show {} worker(s) of {title}", row.children),
                )
            };
            m3::icon_button(palette)
                .width(indent)
                .height(tokens::height::SUB_ROW)
                .automation_name(name)
                .on_click(context.message(Message::Shell(ShellMsg::ExpandToggled(
                    agent.id.clone(),
                    !row.expanded,
                ))))
                .content(m3::icon(glyph))
        } else {
            View::empty()
        };
        let mut name = format!(
            "{title}, {}, Agent {}",
            eliot_core::api::agent_state_name(agent.state),
            agent.id
        );
        if let Some(place) = &row.place {
            name.push_str(&format!(", in {place}"));
        }
        if row.depth > 0 {
            name.push_str(&format!(", worker level {}", row.depth));
        }
        let note: View = match &row.note {
            Some(note) => m3::secondary(note.clone())
                .text_trimming(TextTrimming::CharacterEllipsis)
                .margin(Thickness::new(
                    tokens::spacing::INSET,
                    0.0,
                    0.0,
                    tokens::spacing::XS,
                ))
                .grid_row(1)
                .grid_column(1)
                .into(),
            None => View::empty(),
        };
        Grid::new()
            .margin(Thickness::new(row.depth as f64 * indent, 0.0, 0.0, 0.0))
            .columns([GridLength::Pixel(indent), GridLength::STAR])
            .rows([GridLength::Auto, GridLength::Auto])
            .children((
                Border::new()
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(chevron),
                m3::sub_row(palette, selected)
                    .grid_column(1)
                    .automation_name(name)
                    .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
                    .content(m3::row_content(
                        None,
                        title,
                        if agent.archived {
                            "archived"
                        } else {
                            row.place.as_deref().unwrap_or("")
                        },
                        task_dot(palette, agent),
                    )),
                note,
            ))
    }

    /// One task or chat row: its right-click menu, or the rename editor.
    fn task_item(&self, row: &Row, context: &mut ViewContext<Self>, palette: Palette) -> KeyedView {
        let selected = self.main == Main::Conversation
            && self.nav.selection.agent.as_deref() == Some(row.agent.id.as_str());
        let renaming = self
            .menu
            .renaming
            .as_ref()
            .filter(
                |(target, _)| matches!(target, Target::Agent(agent) if agent.id == row.agent.id),
            )
            .map(|(_, typed)| typed.clone());
        let label = self.agent_label(&row.agent);
        let view = match renaming {
            Some(typed) => self.rename_row(
                &typed,
                (row.depth as f64 + 1.0) * tokens::layout::TREE_INDENT,
                context,
                palette,
            ),
            None => with_menu(
                self.agent_row(row, selected, context, palette),
                Target::Agent(row.agent.clone()),
                context,
            ),
        };
        KeyedView::new(
            format!(
                "agent:{}:{selected}:{}:{label}:{}:{}",
                row.agent.id,
                row.expanded,
                row.agent.archived,
                self.menu.renaming.is_some()
            ),
            view,
        )
    }

    /// A Recents row: the title and its dot, no tree.
    fn recent_row(
        &self,
        agent: &UiAgent,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let label = self.agent_label(agent);
        m3::sub_row(palette, false)
            .automation_name(format!(
                "Recent: {label}, {}",
                eliot_core::api::agent_state_name(agent.state)
            ))
            .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
            .content(m3::row_content(None, &label, "", task_dot(palette, agent)))
    }

    /// The fleet in one calm row ("6 running", "All quiet · 40 Agents");
    /// it opens the monitor.
    fn fleet_row(&self, fleet: Fleet, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let summary = fleet.summary(self.nav.agents_total, self.nav.archived);
        // One row: it opens the Monitor, whose filter lists only the
        // Agents that need attention; the dot says the most urgent tone.
        let tone = if fleet.attention > 0 {
            Some(StatusTone::Attention)
        } else if fleet.running > 0 {
            Some(StatusTone::Active)
        } else {
            None
        };
        let dot = tone
            .and_then(|tone| palette.dot(tone))
            .map_or_else(View::empty, m3::dot);
        let open = self.panel == Panel::Monitor;
        m3::row(palette, open)
            .automation_name(format!(
                "Monitor: {summary} ({})",
                if open { "open" } else { "closed" }
            ))
            .on_click(context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Monitor))))
            .content(m3::row_content(Some(m3::glyph::PULSE), &summary, "", dot))
    }

    /// Settings in the sidebar's place (the Codex settings navigation): a
    /// Back row, a Search field that finds settings across the pages, then
    /// the pages in their groups.
    fn settings_nav(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        let top = StackPanel::new()
            .margin(Thickness::new(pad, tokens::spacing::SM, pad, 0.0))
            .spacing(tokens::spacing::XS)
            .children((
                m3::row(palette, false)
                    .automation_name("Back to tasks")
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsClosed)))
                    .content(m3::icon_label(m3::glyph::BACK, "Back to tasks")),
                m3::field()
                    .text(self.settings.query.clone())
                    .placeholder_text("Search settings")
                    .automation_name("Search settings")
                    .on_text_changed(
                        context.callback(|text| Message::Shell(ShellMsg::SettingsSearch(text))),
                    ),
            ));
        let rows = self.settings_rows(context, palette);
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                top,
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .content(
                        StackPanel::new()
                            .margin(Thickness::new(pad, 0.0, pad, pad))
                            .keyed_children(rows),
                    ),
            ))
    }

    /// Show a short notice above the composer (refusals, uncertain
    /// outcomes, failed reads); it stays until dismissed or replaced.
    pub(super) fn alert(&mut self, text: impl Into<String>) {
        let text = text.into();
        self.set_status(text.clone());
        self.alert = Some(text);
    }

    /// The data directory could not be opened (for example, the default
    /// root is owned by another principal). Nothing is moved; the owner may
    /// select a fresh private directory, which is remembered.
    fn startup_error_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let error = self.alert.clone().unwrap_or_default();
        StackPanel::new()
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .spacing(tokens::spacing::MD)
            .margin(Thickness::uniform(tokens::spacing::LG))
            .children((
                m3::page_title("Swarm could not start here"),
                m3::text(
                    "Swarm could not open this data directory. Existing settings have not been moved.",
                    tokens::text::BODY,
                )
                .text_wrapping(TextWrapping::Wrap),
                m3::secondary(error)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                m3::filled(palette)
                    .automation_name("Use fresh private directory")
                    .on_click(context.message(Message::Shell(ShellMsg::UseFreshDirectory)))
                    .content(m3::label("Use a fresh private directory")),
            ))
    }

    /// The right panel: the monitor or the open Agent's details.
    fn panel_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        match self.panel {
            Panel::Monitor => self.monitor_view(context, palette),
            Panel::Details => self.details_view(context, palette),
            Panel::Closed => View::empty(),
        }
    }
}

/// A sidebar section label: secondary text at the rows' content inset, 16
/// DIPs below the group above it.
fn section_label(text: &str) -> TextBlock {
    m3::section_label(text).margin(Thickness::new(
        tokens::spacing::INSET,
        tokens::spacing::MD,
        0.0,
        tokens::spacing::XS,
    ))
}

/// A muted sub-row that is not a control ("No tasks"), its text where an
/// indented task row's title starts.
fn quiet_row(text: &str, indent: f64) -> View {
    m3::secondary(text.to_owned())
        .height(tokens::height::SUB_ROW)
        .text_trimming(TextTrimming::CharacterEllipsis)
        .margin(Thickness::new(
            indent + tokens::spacing::INSET,
            0.0,
            0.0,
            0.0,
        ))
        .into()
}

/// A task's state dot, only while it runs or needs attention: a calm
/// list carries no marks.
fn task_dot(palette: Palette, agent: &UiAgent) -> View {
    if agent.running() || agent.needs_attention() {
        m3::list_dot(palette, agent.state)
    } else {
        View::empty()
    }
}

/// The start of the local day, in ms since the epoch.
fn today_start_ms() -> i64 {
    use chrono::{Local, TimeZone};
    Local::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .and_then(|midnight| Local.from_local_datetime(&midnight).earliest())
        .map_or(0, |start| start.timestamp_millis())
}
